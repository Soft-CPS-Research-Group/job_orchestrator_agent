# app/services/job_service.py
import os, re, json, yaml, time, logging, shutil, fcntl, copy
from uuid import uuid4
from typing import Any, Generator, Optional
from pathlib import Path
from datetime import datetime, timezone
from collections import deque
from contextlib import contextmanager
from fastapi import HTTPException
from urllib import request as urllib_request
from urllib import parse as urllib_parse
from urllib import error as urllib_error

from app.config import settings
from app.models.job import JobLaunchRequest
from app.services import email_notification_service
from app.utils import job_utils, file_utils
from app.status import JobStatus, can_transition

# In-memory cache of tracked jobs for fast access and testability
jobs = job_utils.load_jobs()
host_heartbeats: dict[str, dict] = {}
HEARTBEAT_TTL = settings.HOST_HEARTBEAT_TTL  # backward compatibility for tests

_LOGGER = logging.getLogger(__name__)

CAPACITY_COUNT_STATUSES = {
    JobStatus.DISPATCHED.value,
    JobStatus.RUNNING.value,
    JobStatus.STOP_REQUESTED.value,
}

ACTIVE_JOB_STATUSES = {
    JobStatus.DISPATCHED.value,
    JobStatus.RUNNING.value,
    JobStatus.STOP_REQUESTED.value,
}

TERMINAL_JOB_STATUSES = {
    JobStatus.FINISHED.value,
    JobStatus.FAILED.value,
    JobStatus.STOPPED.value,
    JobStatus.CANCELED.value,
}

DEFAULT_JOB_CLEANUP_KEEP = {
    "sample_job",
    "running_job",
    "failed_job",
    "queued_job",
}

RUNTIME_RESET_FIELDS = {
    "container_id",
    "container_name",
    "exit_code",
    "error",
    "details",
    "stop_requested",
    "stop_requested_at",
    "worker_id",
    "last_host",
}

JOB_INFO_RUNTIME_RESET_FIELDS = {
    "container_id",
    "container_name",
    "exit_code",
    "error",
    "details",
    "target_host",
}

DEUCALION_SLURM_ACTIVE_STATES = {
    "PENDING",
    "CONFIGURING",
    "COMPLETING",
    "RUNNING",
    "STAGE_OUT",
    "RESIZING",
    "SUSPENDED",
}

_image_versions_cache: dict[str, dict] = {}
_LOG_CHUNK_DEFAULT_TAIL_LINES = 200
_LOG_CHUNK_DEFAULT_MAX_BYTES = 256 * 1024
_LOG_CHUNK_MAX_BYTES_LIMIT = 2 * 1024 * 1024
CONTAINER_DATA_ROOT = "/data"


def _parse_timestamp(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            pass
        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(iso_text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    return None


def _ensure_float(value) -> float | None:
    return _parse_timestamp(value)


def _is_yaml_filename(value: str) -> bool:
    lower = str(value).lower()
    return lower.endswith(".yaml") or lower.endswith(".yml")

def _refresh_jobs():
    """Reload the job registry from disk to keep multiple workers in sync."""
    try:
        disk_jobs = job_utils.load_jobs()
        if isinstance(disk_jobs, dict):
            jobs.clear()
            jobs.update(disk_jobs)
    except Exception:
        _LOGGER.warning("Failed to refresh jobs registry from disk", exc_info=True)


def _persist_job(job_id: str, metadata: dict):
    """Persist job metadata to disk and mirror it in the in-memory cache."""
    _LOGGER.debug("Persisting job %s (status=%s)", job_id, metadata.get("status"))
    job_utils.save_job(job_id, metadata)
    jobs[job_id] = metadata

def _job_exists(job_id: str) -> bool:
    if job_id in jobs:
        return True
    try:
        return job_id in job_utils.load_jobs()
    except Exception:
        return False

# ---------- helpers ----------
def _slug(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', s)

@contextmanager
def _dispatch_lock(worker_id: str):
    """Serialize queue dispatch per worker across API processes."""
    lock_dir = settings.QUEUE_DIR or settings.VM_SHARED_DATA
    os.makedirs(lock_dir, exist_ok=True)
    lock_path = os.path.join(lock_dir, f".dispatch.{_slug(worker_id)}.lock")
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)

def _job_dir(job_id: str) -> str:
    return os.path.join(settings.JOBS_DIR, job_id)

def _status_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "status.json")

def _info_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "job_info.json")

def _log_dir(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "logs")

def _log_path(job_id: str) -> str:
    return os.path.join(_log_dir(job_id), f"{job_id}.log")

def _resolved_config_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "config.resolved.yaml")

def _resolved_config_container_path(job_id: str) -> str:
    return f"jobs/{job_id}/config.resolved.yaml"


def _read_job_info_payload(job_id: str) -> dict:
    info_path = _info_path(job_id)
    if not os.path.exists(info_path):
        return {}
    try:
        with open(info_path) as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _resolve_log_path(job_id: str) -> Optional[str]:
    logs_dir = _log_dir(job_id)
    if not os.path.isdir(logs_dir):
        return None

    canonical_log = Path(_log_path(job_id))
    if canonical_log.is_file() and canonical_log.stat().st_size > 0:
        # Canonical merged stream must win over run-id specific files.
        return str(canonical_log)

    info = _read_job_info_payload(job_id)
    # Prefer the canonical merged stream written by workers into <job_id>.log.
    candidate_names: list[str] = [f"{job_id}.log"]
    for key in ("run_id", "mlflow_run_id"):
        value = info.get(key)
        if isinstance(value, str) and value.strip():
            candidate_names.append(f"{value.strip()}.log")

    existing_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate_name in candidate_names:
        if candidate_name in seen:
            continue
        seen.add(candidate_name)
        candidate_path = os.path.join(logs_dir, candidate_name)
        if os.path.isfile(candidate_path):
            existing_candidates.append(Path(candidate_path))

    log_candidates = [
        entry for entry in Path(logs_dir).glob("*.log")
        if entry.is_file()
    ]
    if not log_candidates:
        return None

    if existing_candidates:
        # Prefer known candidate names if any of them already has content.
        non_empty_known = [entry for entry in existing_candidates if entry.stat().st_size > 0]
        if non_empty_known:
            return str(max(non_empty_known, key=lambda entry: entry.stat().st_mtime))

    # Otherwise pick the most recent non-empty log across every .log file
    # (e.g. runtime.log while canonical <job_id>.log is still empty).
    non_empty_any = [entry for entry in log_candidates if entry.stat().st_size > 0]
    if non_empty_any:
        newest_non_empty = max(non_empty_any, key=lambda entry: entry.stat().st_mtime)
        return str(newest_non_empty)

    if existing_candidates:
        return str(existing_candidates[0])

    newest = max(log_candidates, key=lambda entry: entry.stat().st_mtime)
    return str(newest)

def _container_name(job_id: str, job_name: str) -> str:
    safe_name = _slug(job_name)[:40]
    return f"{settings.CONTAINER_NAME_PREFIX}_{safe_name}_{job_id[:8]}"

def _read_status_payload(job_id: str) -> Optional[dict]:
    path = _status_path(job_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("job_id", job_id)
            return data
    except Exception:
        return None
    return None


def _read_status_file(job_id: str) -> Optional[str]:
    payload = _read_status_payload(job_id)
    if payload:
        return payload.get("status")
    return None


def _status_last_update(job_id: str) -> Optional[float]:
    payload = _read_status_payload(job_id)
    if payload:
        ts = payload.get("status_updated_at")
        if isinstance(ts, (int, float)):
            return float(ts)
    path = _status_path(job_id)
    if os.path.exists(path):
        try:
            return os.path.getmtime(path)
        except OSError:
            return None
    return None


def _apply_lifecycle_metadata(meta: dict, *, prev_status: str | None, status: str, status_ts: float) -> None:
    meta["last_status_at"] = status_ts

    if "submitted_at" not in meta:
        meta["submitted_at"] = status_ts

    if status == JobStatus.QUEUED.value:
        if prev_status in (None, JobStatus.LAUNCHING.value):
            meta.setdefault("queued_at", status_ts)
        elif prev_status != JobStatus.QUEUED.value:
            meta["requeue_count"] = int(meta.get("requeue_count", 0) or 0) + 1
            meta["queued_at"] = status_ts
    elif status == JobStatus.DISPATCHED.value:
        if prev_status != JobStatus.DISPATCHED.value:
            meta["attempt_number"] = int(meta.get("attempt_number", 0) or 0) + 1
        meta.setdefault("queued_at", status_ts)
        meta["dispatched_at"] = status_ts
    elif status == JobStatus.RUNNING.value:
        meta.setdefault("queued_at", status_ts)
        meta.setdefault("started_at", status_ts)
    elif status == JobStatus.STOP_REQUESTED.value:
        meta["stop_requested_at"] = status_ts

    if status in TERMINAL_JOB_STATUSES:
        meta["finished_at"] = status_ts


def _apply_detail_timestamps(meta: dict, details: dict, *, status_ts: float) -> None:
    if not isinstance(details, dict):
        return

    queued_candidates = (
        details.get("queued_at"),
    )
    started_candidates = (
        details.get("started_at"),
    )

    queued_ts = next((candidate for candidate in (_parse_timestamp(v) for v in queued_candidates) if candidate is not None), None)
    started_ts = next((candidate for candidate in (_parse_timestamp(v) for v in started_candidates) if candidate is not None), None)

    current_queued = _ensure_float(meta.get("queued_at"))
    if queued_ts is not None:
        meta["queued_at"] = min(current_queued, queued_ts) if current_queued is not None else queued_ts

    current_started = _ensure_float(meta.get("started_at"))
    if started_ts is not None:
        meta["started_at"] = min(current_started, started_ts) if current_started is not None else started_ts

    # Queue timing is backend lifecycle timing: queue ends when leaving dispatched/running path.
    if meta.get("status") in TERMINAL_JOB_STATUSES and _ensure_float(meta.get("started_at")) is None:
        meta["started_at"] = status_ts

    if _ensure_float(meta.get("queued_at")) is None:
        fallback_queued = _ensure_float(meta.get("submitted_at"))
        if fallback_queued is not None:
            meta["queued_at"] = fallback_queued


def _compute_job_durations(meta: dict, now_ts: float | None = None) -> dict:
    now = now_ts or time.time()
    submitted_at = _ensure_float(meta.get("submitted_at"))
    queued_at = _ensure_float(meta.get("queued_at"))
    started_at = _ensure_float(meta.get("started_at"))
    finished_at = _ensure_float(meta.get("finished_at"))

    queue_wait_seconds = None
    if queued_at and started_at:
        queue_wait_seconds = max(0.0, started_at - queued_at)

    run_duration_seconds = None
    if started_at:
        run_end = finished_at or now
        run_duration_seconds = max(0.0, run_end - started_at)

    total_duration_seconds = None
    if submitted_at:
        total_end = finished_at or now
        total_duration_seconds = max(0.0, total_end - submitted_at)

    return {
        "queue_wait_seconds": queue_wait_seconds,
        "run_duration_seconds": run_duration_seconds,
        "total_duration_seconds": total_duration_seconds,
    }


def _status_notification_meta(job_id: str, status: str) -> dict[str, Any]:
    meta = dict(jobs.get(job_id) or job_utils.load_jobs().get(job_id, {}) or {})
    status_payload = _read_status_payload(job_id) or {}
    meta.update(status_payload)
    meta["job_id"] = job_id
    meta["status"] = status
    meta.update(_compute_job_durations(meta))
    return meta


def _notify_status_change(job_id: str, previous_status: str | None, status: str) -> None:
    email_notification_service.notify_job_status_change(
        job_id=job_id,
        previous_status=previous_status,
        status=status,
        job=_status_notification_meta(job_id, status),
    )


def _write_status(job_id: str, status: str, extra: dict | None = None):
    """Persist status to disk and update the in-memory jobs cache."""
    prev = _read_status_file(job_id)
    if prev and prev != status and not can_transition(prev, status):
        _LOGGER.error("Invalid status transition for job %s: %s -> %s", job_id, prev, status)
        raise ValueError(f"Invalid status transition {prev} -> {status}")
    _LOGGER.info(
        "Job %s status change %s -> %s (extras=%s)",
        job_id,
        prev,
        status,
        sorted((extra or {}).keys()),
    )
    status_ts = time.time()
    extra_payload = dict(extra or {})
    extra_payload.setdefault("status_updated_at", status_ts)
    extra_payload.setdefault("last_status_at", status_ts)
    job_utils.write_status_file(job_id, status, extra_payload)
    if job_id in jobs:
        prev_status = jobs[job_id].get("status")
        jobs[job_id]["status"] = status
        _apply_lifecycle_metadata(jobs[job_id], prev_status=prev_status, status=status, status_ts=status_ts)
        if extra_payload:
            jobs[job_id].update(extra_payload)
        _apply_detail_timestamps(
            jobs[job_id],
            extra_payload.get("details") if isinstance(extra_payload.get("details"), dict) else {},
            status_ts=status_ts,
        )
        job_utils.save_job(job_id, jobs[job_id])
    _notify_status_change(job_id, prev, status)


def _force_status(job_id: str, status: str, extra: dict | None = None) -> None:
    """Write status without enforcing state transitions (ops override)."""
    prev = _read_status_file(job_id)
    status_ts = time.time()
    extra_payload = dict(extra or {})
    extra_payload.setdefault("status_updated_at", status_ts)
    extra_payload.setdefault("last_status_at", status_ts)
    job_utils.write_status_file(job_id, status, extra_payload)
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    if meta:
        prev_status = meta.get("status")
        meta["status"] = status
        _apply_lifecycle_metadata(meta, prev_status=prev_status, status=status, status_ts=status_ts)
        meta.update(extra_payload)
        _apply_detail_timestamps(
            meta,
            extra_payload.get("details") if isinstance(extra_payload.get("details"), dict) else {},
            status_ts=status_ts,
        )
        job_utils.save_job(job_id, meta)
        jobs[job_id] = meta
    _notify_status_change(job_id, prev, status)

# ---------- API: launch ----------
def _as_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float) and value.is_integer():
        parsed = int(value)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed > 0 else None


def _is_gpu_like_partition(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip().lower()
    if not normalized:
        return False
    return "gpu" in normalized or "a100" in normalized or "h100" in normalized


def _payload_indicates_gpu(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ("gpus", "gpu_count", "gpus_per_task", "slurm_gpus"):
        if _as_positive_int(payload.get(key)) is not None:
            return True
    for key in ("partition", "slurm_partition"):
        if _is_gpu_like_partition(payload.get(key)):
            return True
    return False


def _deucalion_job_profile(job_id: str, meta: dict | None = None) -> str:
    metadata = meta or jobs.get(job_id) or {}
    info = _read_job_info_payload(job_id)
    status_payload = _read_status_payload(job_id) or {}

    candidates = [
        metadata.get("deucalion_options") if isinstance(metadata, dict) else None,
        metadata.get("details") if isinstance(metadata, dict) else None,
        info.get("deucalion_options"),
        info.get("details"),
        status_payload.get("details"),
    ]
    return "gpu" if any(_payload_indicates_gpu(candidate) for candidate in candidates) else "cpu"


def _deucalion_profile_limits() -> dict[str, int]:
    return {
        "cpu": max(0, int(settings.DEUCALION_MAX_ACTIVE_CPU_JOBS)),
        "gpu": max(0, int(settings.DEUCALION_MAX_ACTIVE_GPU_JOBS)),
    }


def _host_active_count(host: str) -> int:
    total = 0
    for job in jobs.values():
        if job.get("target_host") != host:
            continue
        if job.get("status") in CAPACITY_COUNT_STATUSES:
            total += 1
    return total


def _active_job_ids_for_host(host: str) -> list[str]:
    active: list[tuple[str, float]] = []
    for job_id, meta in jobs.items():
        if meta.get("target_host") != host:
            continue
        if meta.get("status") not in ACTIVE_JOB_STATUSES:
            continue
        updated = _ensure_float(meta.get("last_status_at")) or _ensure_float(meta.get("status_updated_at")) or 0.0
        active.append((job_id, updated))
    active.sort(key=lambda item: item[1], reverse=True)
    return [job_id for job_id, _ in active]


def _deucalion_active_job_ids_by_profile() -> dict[str, list[str]]:
    active: dict[str, list[tuple[str, float]]] = {"cpu": [], "gpu": []}
    for job_id, meta in jobs.items():
        host = meta.get("target_host") or meta.get("preferred_host")
        if host != "deucalion":
            continue
        if meta.get("status") not in ACTIVE_JOB_STATUSES:
            continue
        profile = _deucalion_job_profile(job_id, meta)
        updated = _ensure_float(meta.get("last_status_at")) or _ensure_float(meta.get("status_updated_at")) or 0.0
        active.setdefault(profile, []).append((job_id, updated))

    return {
        profile: [job_id for job_id, _ in sorted(entries, key=lambda item: item[1], reverse=True)]
        for profile, entries in active.items()
    }


def _deucalion_active_counts_by_profile() -> dict[str, int]:
    active = _deucalion_active_job_ids_by_profile()
    return {profile: len(job_ids) for profile, job_ids in active.items()}


def _can_dispatch_to_deucalion(queue_payload: dict, active_counts: dict[str, int] | None = None) -> bool:
    job_id = queue_payload.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        return False
    meta = jobs.get(job_id)
    if not meta:
        return False
    profile = _deucalion_job_profile(job_id, meta)
    limits = _deucalion_profile_limits()
    counts = active_counts or _deucalion_active_counts_by_profile()
    allowed = counts.get(profile, 0) < limits.get(profile, 0)
    if not allowed:
        _LOGGER.info(
            "Deucalion %s dispatch slot full; keeping job %s queued (%s/%s)",
            profile.upper(),
            job_id,
            counts.get(profile, 0),
            limits.get(profile, 0),
        )
    return allowed


def _preferred_host(requested: Optional[str]) -> Optional[str]:
    if not requested:
        return None
    if not job_utils.is_valid_host(requested):
        raise HTTPException(400, f"Unknown host '{requested}'. Allowed: {settings.AVAILABLE_HOSTS}")
    return requested


def _safe_filename(value: str) -> str:
    cleaned = os.path.normpath(value).lstrip(os.sep)
    if cleaned.startswith("..") or os.path.isabs(value) or os.sep in cleaned:
        raise HTTPException(400, "Invalid file name")
    return cleaned


def _normalize_job_image(value: Optional[str]) -> str:
    if value is None:
        return settings.DEFAULT_JOB_IMAGE
    image = str(value).strip()
    if not image:
        return settings.DEFAULT_JOB_IMAGE
    if len(image) > 512:
        raise HTTPException(400, "Job image is too long")
    if any(ch.isspace() for ch in image):
        raise HTTPException(400, "Invalid job image")
    return image


def _normalize_image_tag(value: Optional[str]) -> str:
    if value is None:
        return "latest"
    tag = str(value).strip()
    if not tag:
        return "latest"
    if len(tag) > 128:
        raise HTTPException(400, "Image tag is too long")
    if any(ch.isspace() for ch in tag):
        raise HTTPException(400, "Invalid image tag")
    if "/" in tag or "@" in tag or ":" in tag:
        raise HTTPException(400, "Image tag must not contain '/', ':' or '@'")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", tag):
        raise HTTPException(400, "Invalid image tag format")
    return tag


def _resolve_job_image_from_tag(image_tag: str) -> str:
    repository = _normalize_image_repository(settings.JOB_IMAGE_REPOSITORY)
    return f"{repository}:{image_tag}"


def _normalize_deucalion_options(value: Any) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise HTTPException(400, "deucalion_options must be an object")
    normalized = {}
    list_fields = {"modules", "datasets", "required_paths"}
    for key, raw in value.items():
        if raw is None:
            continue
        if key in list_fields:
            if not isinstance(raw, list):
                raise HTTPException(400, f"deucalion_options.{key} must be a list")
            values = [str(item).strip() for item in raw if str(item).strip()]
            if values:
                normalized[key] = values
            continue
        if key in {"cpus_per_task", "mem_gb", "gpus"}:
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                raise HTTPException(400, f"deucalion_options.{key} must be an integer")
            if parsed < 0:
                raise HTTPException(400, f"deucalion_options.{key} must be >= 0")
            normalized[key] = parsed
            continue
        text = str(raw).strip()
        if text:
            normalized[key] = text
    return normalized or None


def _validate_executor_agnostic_config(config: dict) -> None:
    execution = config.get("execution")
    if not isinstance(execution, dict):
        return
    blocked = {"deucalion", "docker", "executor", "host", "runtime", "server", "local"}
    invalid = sorted(key for key in execution.keys() if key in blocked)
    if invalid:
        joined = ", ".join(invalid)
        raise HTTPException(
            400,
            (
                "Config contains executor-specific fields under 'execution' "
                f"({joined}). Move execution options to run-simulation payload."
            ),
        )


def _container_dataset_schema_path(dataset_name: str) -> str:
    return f"{CONTAINER_DATA_ROOT}/datasets/{dataset_name}/schema.json"


def _normalize_path_for_container(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None

    normalized = raw.replace("\\", "/")
    container_root = CONTAINER_DATA_ROOT.rstrip("/")
    if normalized == container_root or normalized.startswith(f"{container_root}/"):
        return raw

    host_roots = [
        (str(Path(settings.DATASETS_DIR).as_posix()).rstrip("/"), f"{CONTAINER_DATA_ROOT}/datasets"),
        (str(Path(settings.VM_SHARED_DATA).as_posix()).rstrip("/"), CONTAINER_DATA_ROOT),
    ]
    for host_root, mapped_root in host_roots:
        if not host_root:
            continue
        if normalized == host_root:
            return mapped_root
        if normalized.startswith(f"{host_root}/"):
            return f"{mapped_root}/{normalized[len(host_root):].lstrip('/')}"

    relative = normalized[2:] if normalized.startswith("./") else normalized
    relative = relative.lstrip("/")
    if relative == "datasets" or relative.startswith("datasets/"):
        return f"{CONTAINER_DATA_ROOT}/{relative}"

    return raw


def _resolve_runtime_config(config: dict) -> tuple[dict, bool]:
    resolved = copy.deepcopy(config)
    changed = False

    simulator = resolved.get("simulator")
    if not isinstance(simulator, dict):
        return resolved, changed

    dataset_name = simulator.get("dataset_name")
    dataset_name = dataset_name.strip() if isinstance(dataset_name, str) else ""
    dataset_path = simulator.get("dataset_path")

    if isinstance(dataset_path, str) and dataset_path.strip():
        normalized_path = _normalize_path_for_container(dataset_path)
        if normalized_path and normalized_path != dataset_path:
            simulator["dataset_path"] = normalized_path
            changed = True
    elif dataset_name:
        simulator["dataset_path"] = _container_dataset_schema_path(dataset_name)
        changed = True

    return resolved, changed


def _write_resolved_config(job_id: str, config: dict) -> None:
    os.makedirs(_job_dir(job_id), exist_ok=True)
    with open(_resolved_config_path(job_id), "w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)


def _normalize_image_repository(value: Optional[str]) -> str:
    repo = (value or settings.JOB_IMAGE_REPOSITORY or "").strip().strip("/")
    if not repo:
        raise HTTPException(400, "Image repository is required")
    parts = [part for part in repo.split("/") if part]
    if len(parts) == 1:
        namespace, name = "library", parts[0]
    elif len(parts) == 2:
        namespace, name = parts
    else:
        raise HTTPException(400, "Repository must be '<namespace>/<name>'")
    token = re.compile(r"^[a-z0-9]+([._-][a-z0-9]+)*$")
    if not token.fullmatch(namespace) or not token.fullmatch(name):
        raise HTTPException(400, "Invalid Docker Hub repository format")
    return f"{namespace}/{name}"


def _dockerhub_tag_digest(raw: dict) -> str | None:
    if not isinstance(raw, dict):
        return None
    images = raw.get("images")
    if not isinstance(images, list):
        return None
    for item in images:
        if isinstance(item, dict):
            digest = item.get("digest")
            if isinstance(digest, str) and digest:
                return digest
    return None


def _fetch_dockerhub_tags(repository: str, max_tags: int) -> tuple[list[dict], bool, float]:
    repo = _normalize_image_repository(repository)
    cache_key = f"{repo}:{max_tags}"
    now = time.time()
    ttl = max(0, int(settings.JOB_IMAGE_CATALOG_TTL_SECONDS))

    cached = _image_versions_cache.get(cache_key)
    if cached and (now - cached.get("fetched_at", 0.0)) < ttl:
        return cached["tags"], True, cached["fetched_at"]

    namespace, name = repo.split("/", 1)
    next_url = (
        "https://hub.docker.com/v2/repositories/"
        f"{urllib_parse.quote(namespace)}/{urllib_parse.quote(name)}"
        f"/tags?page_size={min(max_tags, 100)}"
    )
    tags: list[dict] = []
    timeout_seconds = max(1, int(settings.JOB_IMAGE_CATALOG_TIMEOUT_SECONDS))
    while next_url and len(tags) < max_tags:
        req = urllib_request.Request(next_url, headers={"Accept": "application/json"})
        try:
            with urllib_request.urlopen(req, timeout=timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            if exc.code == 404:
                raise HTTPException(404, f"Docker Hub repository not found: {repo}")
            raise HTTPException(502, f"Failed to fetch Docker Hub tags for {repo} (status={exc.code})")
        except Exception as exc:
            raise HTTPException(502, f"Failed to fetch Docker Hub tags for {repo}: {exc}")

        results = payload.get("results")
        if not isinstance(results, list):
            break
        for item in results:
            if not isinstance(item, dict):
                continue
            tag_name = item.get("name")
            if not isinstance(tag_name, str) or not tag_name:
                continue
            tags.append(
                {
                    "name": tag_name,
                    "last_updated": item.get("last_updated"),
                    "digest": _dockerhub_tag_digest(item),
                }
            )
            if len(tags) >= max_tags:
                break

        raw_next = payload.get("next")
        next_url = raw_next if isinstance(raw_next, str) and raw_next else None

    fetched_at = time.time()
    _image_versions_cache[cache_key] = {"tags": tags, "fetched_at": fetched_at}
    return tags, False, fetched_at


def list_job_image_versions(repository: Optional[str] = None, limit: Optional[int] = None) -> dict:
    repo = _normalize_image_repository(repository)
    sif_repo = _normalize_image_repository(settings.JOB_SIF_REPOSITORY)
    max_tags = int(limit or settings.JOB_IMAGE_TAGS_LIMIT)
    max_tags = max(1, min(max_tags, 200))

    image_tags, image_cached, image_fetched_at = _fetch_dockerhub_tags(repo, max_tags)
    sif_tags, sif_cached, sif_fetched_at = _fetch_dockerhub_tags(sif_repo, max_tags)
    sif_tag_names = {
        str(tag.get("name"))
        for tag in sif_tags
        if isinstance(tag, dict) and isinstance(tag.get("name"), str)
    }

    tags_with_readiness: list[dict] = []
    for tag in image_tags:
        if not isinstance(tag, dict):
            continue
        tag_name = tag.get("name")
        if not isinstance(tag_name, str):
            continue
        merged = dict(tag)
        merged["deucalion_ready"] = tag_name in sif_tag_names
        tags_with_readiness.append(merged)

    return {
        "repository": repo,
        "sif_repository": sif_repo,
        "tags": tags_with_readiness,
        "count": len(tags_with_readiness),
        "cached": bool(image_cached and sif_cached),
        "fetched_at": max(image_fetched_at, sif_fetched_at),
    }


def _status_stale_ttl(meta: dict, status: str) -> int:
    ttl = int(settings.JOB_STATUS_TTL)
    host = str(meta.get("target_host") or meta.get("preferred_host") or "")
    if status == JobStatus.DISPATCHED.value and host == "deucalion":
        ttl = max(ttl, int(settings.DEUCALION_DISPATCH_STATUS_TTL))
    return ttl


def _should_preserve_deucalion_active_status(job_id: str, meta: dict, now_ts: float) -> bool:
    host = str(meta.get("target_host") or meta.get("preferred_host") or "")
    if host != "deucalion":
        return False

    payload = _read_status_payload(job_id) or {}
    details = payload.get("details")
    if not isinstance(details, dict):
        details = {}

    hb = host_heartbeats.get("deucalion")
    if not hb:
        return False
    last_seen = hb.get("last_seen")
    if not isinstance(last_seen, (int, float)):
        return False
    cutoff = settings.HOST_HEARTBEAT_TTL + settings.WORKER_STALE_GRACE_SECONDS
    if (now_ts - float(last_seen)) > cutoff:
        return False

    info = hb.get("info")
    heartbeat_slurm_state = None
    if isinstance(info, dict):
        active_jobs = _normalize_active_jobs_payload(info.get("active_jobs"))
        for row in active_jobs:
            if row.get("job_id") == job_id:
                heartbeat_slurm_state = row.get("slurm_state")
                break
        active_job_ids = info.get("active_job_ids")
        if isinstance(active_job_ids, list):
            normalized_ids = {str(item) for item in active_job_ids if isinstance(item, str)}
            if normalized_ids and job_id not in normalized_ids:
                return False
        else:
            active_job_id = info.get("active_job_id")
            if active_job_id and active_job_id != job_id:
                return False

    slurm_state = details.get("slurm_state") or heartbeat_slurm_state
    if not isinstance(slurm_state, str) or slurm_state.strip().upper() not in DEUCALION_SLURM_ACTIVE_STATES:
        return False

    return True


def _reset_runtime_metadata(job_id: str, meta: dict) -> dict:
    cleaned = dict(meta)
    for key in RUNTIME_RESET_FIELDS:
        cleaned.pop(key, None)

    info = _read_job_info_payload(job_id)
    if info:
        changed = False
        for key in JOB_INFO_RUNTIME_RESET_FIELDS:
            if key in info:
                info.pop(key, None)
                changed = True
        if changed:
            try:
                with open(_info_path(job_id), "w") as f:
                    json.dump(info, f, indent=2)
            except Exception:
                _LOGGER.warning("Failed to reset job_info runtime metadata for %s", job_id, exc_info=True)

    return cleaned


def _resolve_experiment_identity(config: dict) -> tuple[str, str]:
    metadata = config.get("metadata", {}) if isinstance(config, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}

    experiment_name = str(metadata.get("experiment_name", "")).strip()
    run_name = str(metadata.get("run_name", "")).strip()

    if not experiment_name or not run_name:
        legacy = config.get("experiment", {}) if isinstance(config, dict) else {}
        if not isinstance(legacy, dict):
            legacy = {}
        if not experiment_name:
            experiment_name = str(legacy.get("name", "")).strip()
        if not run_name:
            run_name = str(legacy.get("run_name", "")).strip()

    if not experiment_name:
        experiment_name = "UnnamedExperiment"
    if not run_name:
        run_name = "UnnamedRun"

    return experiment_name, run_name


def _build_mlflow_run_url(
    *,
    base_url: Optional[str],
    experiment_id: Optional[str],
    run_id: Optional[str],
) -> Optional[str]:
    if not base_url or not experiment_id or not run_id:
        return None
    normalized = base_url.rstrip("/")
    return f"{normalized}/#/experiments/{experiment_id}/runs/{run_id}"


def _resolve_mlflow_base_url(info: dict) -> Optional[str]:
    candidates: list[Optional[str]] = [
        settings.MLFLOW_UI_BASE_URL,
        info.get("mlflow_ui_base_url") if isinstance(info, dict) else None,
        info.get("tracking_ui_base_url") if isinstance(info, dict) else None,
        info.get("tracking_uri") if isinstance(info, dict) else None,
        info.get("mlflow_uri") if isinstance(info, dict) else None,
    ]
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        text = candidate.strip()
        if not text:
            continue
        if text.startswith("http://") or text.startswith("https://"):
            return text
    return None


def _queue_payload(
    *,
    job_id: str,
    preferred_host: str | None,
    require_host: bool,
    submitted_by: str | None = None,
) -> dict:
    payload: dict = {
        "job_id": job_id,
        "preferred_host": preferred_host,
        "require_host": require_host,
    }
    if submitted_by:
        payload["submitted_by"] = submitted_by
    return payload


def _enrich_job_info_with_mlflow_links(info: dict) -> dict:
    if not isinstance(info, dict):
        return info

    enriched = dict(info)
    run_id = enriched.get("mlflow_run_id") or enriched.get("run_id")
    experiment_id = enriched.get("mlflow_experiment_id") or enriched.get("experiment_id")

    if run_id is not None and "mlflow_run_id" not in enriched:
        enriched["mlflow_run_id"] = run_id
    if experiment_id is not None and "mlflow_experiment_id" not in enriched:
        enriched["mlflow_experiment_id"] = experiment_id

    if "mlflow_run_url" not in enriched or not enriched.get("mlflow_run_url"):
        derived_url = _build_mlflow_run_url(
            base_url=_resolve_mlflow_base_url(enriched),
            experiment_id=str(experiment_id) if experiment_id is not None else None,
            run_id=str(run_id) if run_id is not None else None,
        )
        if derived_url:
            enriched["mlflow_run_url"] = derived_url

    return enriched


def _normalize_active_jobs_payload(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        job_id = entry.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            continue
        row: dict[str, Any] = {"job_id": job_id}
        for key in (
            "job_name",
            "status",
            "phase",
            "slurm_job_id",
            "slurm_state",
            "slurm_partition",
            "slurm_nodes",
            "slurm_cpus",
            "slurm_gpus",
            "queue_pos",
            "ahead",
            "updated_at",
        ):
            if key in entry and entry[key] is not None:
                row[key] = entry[key]
        normalized.append(row)
    return normalized


def record_host_heartbeat(worker_id: str, info: dict | None = None) -> None:
    if not job_utils.is_valid_host(worker_id):
        raise HTTPException(400, f"Unknown worker_id '{worker_id}'. Allowed: {settings.AVAILABLE_HOSTS}")
    _LOGGER.debug("Heartbeat received from %s (info keys=%s)", worker_id, sorted((info or {}).keys()))
    host_heartbeats[worker_id] = {
        "last_seen": time.time(),
        "info": info or {},
    }


def _host_status_snapshot() -> dict[str, dict]:
    now = time.time()
    known_hosts = set(settings.AVAILABLE_HOSTS) | set(host_heartbeats.keys())
    snapshot: dict[str, dict] = {}
    for host in sorted(known_hosts):
        hb = host_heartbeats.get(host)
        online = bool(hb and (now - hb["last_seen"]) <= settings.HOST_HEARTBEAT_TTL)
        # Consider hosts with active jobs as online to avoid marking long runs offline
        if not online:
            active = any(
                (job.get("target_host") == host)
                and job.get("status")
                in ACTIVE_JOB_STATUSES
                for job in jobs.values()
            )
            if active:
                online = True
        active_job_ids = _active_job_ids_for_host(host)
        current_job_id = active_job_ids[0] if active_job_ids else None
        current_job_status = jobs.get(current_job_id, {}).get("status") if current_job_id else None
        raw_info = hb["info"] if hb else {}
        if not isinstance(raw_info, dict):
            raw_info = {}
        normalized_info = dict(raw_info)
        normalized_info["executor"] = raw_info.get("executor")
        normalized_info["worker_version"] = raw_info.get("worker_version") or raw_info.get("version")
        normalized_active_jobs = _normalize_active_jobs_payload(raw_info.get("active_jobs"))
        active_ids_from_info = [
            str(item) for item in (raw_info.get("active_job_ids") or []) if isinstance(item, str)
        ]
        merged_active_ids = active_ids_from_info or [job.get("job_id") for job in normalized_active_jobs]
        if not merged_active_ids:
            merged_active_ids = active_job_ids
        if host == "deucalion":
            merged_active_ids = active_job_ids

        normalized_info["active_job_id"] = raw_info.get("active_job_id") or current_job_id
        normalized_info["active_job_count"] = raw_info.get("active_job_count")
        normalized_info["active_job_ids"] = merged_active_ids
        normalized_info["active_jobs"] = normalized_active_jobs
        normalized_info["last_job_id"] = raw_info.get("last_job_id")
        normalized_info["last_terminal_status"] = raw_info.get("last_terminal_status")
        normalized_info["last_status"] = raw_info.get("last_status")
        normalized_info["last_status_job_id"] = raw_info.get("last_status_job_id")
        normalized_info["last_status_at"] = raw_info.get("last_status_at")
        normalized_info["last_status_worker_version"] = (
            raw_info.get("last_status_worker_version") or normalized_info.get("worker_version")
        )
        normalized_info["budget"] = raw_info.get("budget")
        normalized_info["budget_refreshed_at"] = raw_info.get("budget_refreshed_at")
        if host == "deucalion":
            profile_limits = _deucalion_profile_limits()
            profile_active_ids = _deucalion_active_job_ids_by_profile()
            profile_counts = {
                profile: len(profile_active_ids.get(profile, []))
                for profile in profile_limits.keys()
            }
            normalized_info["max_active_jobs"] = sum(profile_limits.values())
            normalized_info["max_active_jobs_by_profile"] = profile_limits
            normalized_info["active_job_count_by_profile"] = profile_counts
            normalized_info["active_job_ids_by_profile"] = profile_active_ids
            normalized_info["active_job_count"] = sum(profile_counts.values())
        if normalized_info["active_job_count"] is None:
            normalized_info["active_job_count"] = len(merged_active_ids)
        snapshot[host] = {
            "online": online,
            "last_seen": hb["last_seen"] if hb else None,
            "info": normalized_info,
            "running": _host_active_count(host),
            "active_job_ids": merged_active_ids,
            "current_job_id": current_job_id,
            "current_job_status": current_job_status,
        }
    return snapshot


def _job_results_root(job_id: str) -> Path:
    return Path(settings.JOBS_DIR) / job_id / "results"


def _simulation_data_root(job_id: str) -> Path:
    return _job_results_root(job_id) / "simulation_data"


def _latest_simulation_session_path(sim_root: Path) -> tuple[str | None, Path | None]:
    if not sim_root.exists() or not sim_root.is_dir():
        return None, None
    directories = [item for item in sim_root.iterdir() if item.is_dir()]
    if directories:
        latest = sorted(directories, key=lambda item: item.stat().st_mtime)[-1]
        return latest.name, latest
    return "root", sim_root


def _resolve_kpi_source(result_payload: dict, sim_session_path: Path | None) -> str:
    if sim_session_path and sim_session_path.exists():
        for candidate in sim_session_path.rglob("exported_kpis.csv"):
            if candidate.is_file():
                return "simulation_data/exported_kpis.csv"
    evaluation = result_payload.get("evaluation")
    if isinstance(evaluation, dict) and isinstance(evaluation.get("kpis"), dict):
        return "result.evaluation.kpis"
    if isinstance(result_payload.get("kpis"), dict):
        return "result.kpis"
    return "unknown"


def _simulation_data_metadata(job_id: str, result_payload: dict) -> dict:
    sim_root = _simulation_data_root(job_id)
    session_name, session_path = _latest_simulation_session_path(sim_root)
    simulation_data_available = bool(session_path and session_path.exists())
    kpi_source = _resolve_kpi_source(result_payload, session_path)
    return {
        "simulation_data_available": simulation_data_available,
        "simulation_data_session_default": session_name,
        "simulation_data_dir": str(session_path) if session_path else None,
        "kpi_source": kpi_source,
    }


def _mark_stale_jobs():
    """Detect jobs stuck on offline workers and requeue or fail them."""
    now = time.time()
    cutoff = settings.HOST_HEARTBEAT_TTL + settings.WORKER_STALE_GRACE_SECONDS
    for job_id, meta in list(jobs.items()):
        status = meta.get("status")
        host = meta.get("target_host")
        if status not in (JobStatus.DISPATCHED.value, JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            continue

        status_ttl = _status_stale_ttl(meta, status)
        last_update = _status_last_update(job_id)
        if last_update and (now - last_update) > status_ttl:
            if _should_preserve_deucalion_active_status(job_id, meta, now):
                _LOGGER.info(
                    "Keeping %s Deucalion job %s while heartbeat/Slurm state is active",
                    status,
                    job_id,
                )
            else:
                preferred = meta.get("preferred_host") or meta.get("target_host")
                require_host = bool(meta.get("require_host", bool(preferred)))
                if status == JobStatus.DISPATCHED.value:
                    job_utils.enqueue_job(
                        _queue_payload(
                            job_id=job_id,
                            preferred_host=preferred,
                            require_host=require_host,
                            submitted_by=meta.get("submitted_by"),
                        )
                    )
                    meta["status"] = JobStatus.QUEUED.value
                    _persist_job(job_id, meta)
                    _write_status(
                        job_id,
                        JobStatus.QUEUED.value,
                        {"requeued_from": host, "preferred_host": preferred, "stale_status": True},
                    )
                    _LOGGER.warning("Re-queued dispatched job %s due to stale status update", job_id)
                else:
                    _write_status(job_id, JobStatus.FAILED.value, {"error": "stale_status", "last_host": host})
                    meta["status"] = JobStatus.FAILED.value
                    _persist_job(job_id, meta)
                    _LOGGER.warning("Marked job %s as failed due to stale status update", job_id)
                continue

        if not host:
            continue
        hb = host_heartbeats.get(host)
        last_seen = hb["last_seen"] if hb else None
        if last_seen is None:
            continue  # no heartbeat recorded yet; give it a chance
        offline = (now - last_seen) > cutoff
        if not offline:
            continue
        preferred = meta.get("preferred_host") or meta.get("target_host")
        require_host = bool(meta.get("require_host", bool(preferred)))
        if status == JobStatus.DISPATCHED.value:
            # Put back in queue for another worker to pick up
            job_utils.enqueue_job(
                _queue_payload(
                    job_id=job_id,
                    preferred_host=preferred,
                    require_host=require_host,
                    submitted_by=meta.get("submitted_by"),
                )
            )
            meta["status"] = JobStatus.QUEUED.value
            _persist_job(job_id, meta)
            _write_status(job_id, JobStatus.QUEUED.value, {"requeued_from": host, "preferred_host": preferred})
            _LOGGER.warning("Re-queued stale dispatched job %s from offline host %s", job_id, host)
        elif status in (JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            _write_status(job_id, JobStatus.FAILED.value, {"error": "worker_offline", "last_host": host})
            meta["status"] = JobStatus.FAILED.value
            _persist_job(job_id, meta)
            _LOGGER.warning("Marked job %s as failed because host %s is offline", job_id, host)


async def launch_simulation(request: JobLaunchRequest):
    job_utils.ensure_directories()

    if not settings.AVAILABLE_HOSTS:
        raise HTTPException(503, "No hosts configured")

    preferred_host = _preferred_host(request.target_host)
    job_id = str(uuid4())

    # config
    if request.config_path:
        # Accept both "file.yaml" and "configs/file.yaml" style paths
        config_path = request.config_path.lstrip("/")
        relative_path = config_path[len("configs/"):] if config_path.startswith("configs/") else config_path
        relative_path = os.path.normpath(relative_path)
        if relative_path.startswith(".."):
            raise HTTPException(400, "Invalid config_path")
        with open(os.path.join(settings.CONFIGS_DIR, relative_path)) as f:
            config = yaml.safe_load(f)
        config_path = relative_path
    elif request.config:
        file_name = request.save_as or f"{job_id}.yaml"
        file_name = _safe_filename(file_name)
        config_path = file_utils.save_config_dict(request.config, file_name)
        config = request.config
    else:
        raise HTTPException(400, "Missing config or config_path")

    if not isinstance(config, dict):
        raise HTTPException(400, "Invalid config format")
    _validate_executor_agnostic_config(config)
    runtime_config, runtime_config_changed = _resolve_runtime_config(config)

    experiment_name, run_name = _resolve_experiment_identity(runtime_config)
    requested_job_name = (request.job_name or "").strip()
    job_name = requested_job_name or f"{experiment_name}-{run_name}"
    submitted_by = email_notification_service.normalize_submitted_by((request.submitted_by or "").strip() or None)
    image_tag = _normalize_image_tag(request.image_tag)
    job_image = _resolve_job_image_from_tag(image_tag)
    deucalion_options = _normalize_deucalion_options(
        request.deucalion_options.model_dump(exclude_none=True, by_alias=True)
        if request.deucalion_options is not None
        else None
    )
    if deucalion_options and preferred_host != "deucalion":
        raise HTTPException(400, "deucalion_options are only allowed when target_host is 'deucalion'")

    if not config_path.startswith("configs/"):
        config_path = f"configs/{config_path}"

    os.makedirs(_log_dir(job_id), exist_ok=True)
    if runtime_config_changed:
        _write_resolved_config(job_id, runtime_config)
    meta = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": preferred_host,
        "preferred_host": preferred_host,
        "experiment_name": experiment_name,
        "run_name": run_name,
        "status": JobStatus.LAUNCHING.value,
        "require_host": bool(preferred_host),
        "submitted_by": submitted_by,
        "image_tag": image_tag,
        "image": job_image,
        "deucalion_options": deucalion_options,
    }
    _persist_job(job_id, meta)
    job_utils.save_job_info(
        job_id,
        job_name,
        config_path,
        preferred_host or "",
        container_id="",
        container_name="",
        exp=experiment_name,
        run=run_name,
        submitted_by=submitted_by,
        image_tag=image_tag,
        image=job_image,
        deucalion_options=deucalion_options,
    )
    _write_status(job_id, JobStatus.LAUNCHING.value, {"preferred_host": preferred_host})

    # enqueue for agent (agent decides how to run the container)
    job_utils.enqueue_job(
        _queue_payload(
            job_id=job_id,
            preferred_host=preferred_host,
            require_host=bool(preferred_host),
            submitted_by=submitted_by,
        )
    )
    meta.update({"status": JobStatus.QUEUED.value})
    _persist_job(job_id, meta)
    _write_status(job_id, JobStatus.QUEUED.value, {"preferred_host": preferred_host})
    return {
        "job_id": job_id,
        "status": JobStatus.QUEUED.value,
        "host": preferred_host,
        "job_name": job_name,
        "image_tag": image_tag,
        "image": job_image,
    }

# ---------- API: status/result/progress/logs ----------

def get_status(job_id: str):
    """Return the current status payload for a given job."""
    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    payload = _read_status_payload(job_id)
    if payload:
        return payload

    status = job.get("status", JobStatus.UNKNOWN.value)
    return {"job_id": job_id, "status": status}


def get_result(job_id: str):
    payload = file_utils.collect_results(job_id)
    if not isinstance(payload, dict):
        payload = {"result": payload}
    payload.update(_simulation_data_metadata(job_id, payload))
    return payload

def get_progress(job_id: str):
    return file_utils.read_progress(job_id)

def get_job_resolved_config(job_id: str) -> str:
    path = _resolved_config_path(job_id)
    if not os.path.exists(path):
        raise HTTPException(404, "Resolved config not found")
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()

def _stream_file(path: str) -> Generator[str, None, None]:
    with open(path) as f:
        for line in f:
            yield line


def _read_log_tail(path: str, *, tail_lines: int, max_bytes: int) -> tuple[str, int, bool]:
    file_size = os.path.getsize(path)
    if file_size <= 0:
        return "", 0, False

    lines_limit = max(1, tail_lines)
    with open(path, "rb") as handle:
        selected = deque(maxlen=lines_limit)
        for raw_line in handle:
            selected.append(raw_line)
    payload = b"".join(selected)
    truncated = len(payload) > max_bytes
    if truncated:
        payload = payload[-max_bytes:]
    start_offset = max(0, file_size - len(payload))
    return payload.decode("utf-8", errors="replace"), file_size, truncated or start_offset > 0


def _read_log_delta(path: str, *, offset: int, max_bytes: int) -> tuple[str, int, bool]:
    file_size = os.path.getsize(path)
    safe_offset = max(0, min(offset, file_size))
    if file_size <= safe_offset:
        return "", file_size, False

    with open(path, "rb") as handle:
        handle.seek(safe_offset)
        payload = handle.read(max_bytes + 1)

    truncated = len(payload) > max_bytes
    if truncated:
        payload = payload[:max_bytes]
    next_offset = safe_offset + len(payload)
    return payload.decode("utf-8", errors="replace"), next_offset, truncated or next_offset < file_size

def get_file_logs(job_id: str):
    path = _resolve_log_path(job_id)
    if not path:
        raise HTTPException(404, "Log file not found")
    return _stream_file(path)

def get_logs(job_id: str):
    path = _resolve_log_path(job_id)
    if path and os.path.exists(path):
        return _stream_file(path)
    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Logs not available for this job")
    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)
    if status_now in {
        JobStatus.LAUNCHING.value,
        JobStatus.QUEUED.value,
        JobStatus.DISPATCHED.value,
        JobStatus.RUNNING.value,
        JobStatus.STOP_REQUESTED.value,
    }:
        def _msg():
            yield "Logs not available yet. Job is still initializing or running.\n"
        return _msg()
    raise HTTPException(404, "Logs not available for this job")


def get_logs_chunk(
    job_id: str,
    *,
    offset: int | None = None,
    tail_lines: int = _LOG_CHUNK_DEFAULT_TAIL_LINES,
    max_bytes: int = _LOG_CHUNK_DEFAULT_MAX_BYTES,
) -> dict:
    safe_tail_lines = max(1, int(tail_lines))
    safe_max_bytes = max(1024, min(int(max_bytes), _LOG_CHUNK_MAX_BYTES_LIMIT))

    path = _resolve_log_path(job_id)
    if path and os.path.exists(path):
        if offset is None:
            text, next_offset, truncated = _read_log_tail(
                path,
                tail_lines=safe_tail_lines,
                max_bytes=safe_max_bytes,
            )
        else:
            text, next_offset, truncated = _read_log_delta(
                path,
                offset=int(offset),
                max_bytes=safe_max_bytes,
            )

        return {
            "job_id": job_id,
            "text": text,
            "next_offset": next_offset,
            "truncated": truncated,
            "available": True,
            "message": None,
        }

    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Logs not available for this job")

    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)
    if status_now in {
        JobStatus.LAUNCHING.value,
        JobStatus.QUEUED.value,
        JobStatus.DISPATCHED.value,
        JobStatus.RUNNING.value,
        JobStatus.STOP_REQUESTED.value,
    }:
        return {
            "job_id": job_id,
            "text": "",
            "next_offset": max(0, int(offset or 0)),
            "truncated": False,
            "available": False,
            "message": "Logs not available yet. Job is still initializing or running.",
        }

    raise HTTPException(404, "Logs not available for this job")

# ---------- API: stop/list/info/delete/hosts ----------
def stop_job(job_id: str, reason: str = "stop_requested", requested_by_ops: bool = False):
    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)

    job_utils.remove_from_queue(job_id)

    if status_now in (JobStatus.LAUNCHING.value, JobStatus.QUEUED.value):
        extra = {"error": reason}
        if requested_by_ops:
            extra["canceled_by_ops"] = True
        _write_status(job_id, JobStatus.CANCELED.value, extra)
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.CANCELED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Job canceled from queue"}

    if status_now in (JobStatus.DISPATCHED.value, JobStatus.RUNNING.value):
        extra = {"stop_requested": True, "stop_reason": reason}
        if requested_by_ops:
            extra["stopped_by_ops"] = True
        _write_status(job_id, JobStatus.STOP_REQUESTED.value, extra)
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.STOP_REQUESTED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Stop requested; worker should terminate the job"}

    if status_now == JobStatus.STOP_REQUESTED.value:
        return {"message": "Stop already requested"}

    if status_now in (
        JobStatus.FINISHED.value,
        JobStatus.FAILED.value,
        JobStatus.STOPPED.value,
        JobStatus.CANCELED.value,
    ):
        return {"message": f"Job already finished ({status_now})"}

    return {"message": f"Job is {status_now}; nothing to stop"}

def list_jobs():
    _refresh_jobs()
    _mark_stale_jobs()
    result = []
    for job_id, job in jobs.items():
        merged = dict(job)
        merged["job_id"] = job_id
        info = {}
        ipath = _info_path(job_id)
        if os.path.exists(ipath):
            with open(ipath) as f:
                info = json.load(f)
            info = _enrich_job_info_with_mlflow_links(info)
        else:
            info = {}

        resolved_path = _resolved_config_path(job_id)
        info.setdefault("resolved_config_available", os.path.isfile(resolved_path))
        if info["resolved_config_available"]:
            info.setdefault("resolved_config_file", "config.resolved.yaml")
        info.setdefault("submitted_by", merged.get("submitted_by"))
        info.setdefault("job_name", merged.get("job_name"))
        info.setdefault("config_path", merged.get("config_path"))
        info.setdefault("target_host", merged.get("target_host"))
        info.setdefault("image_tag", merged.get("image_tag"))
        info.setdefault("deucalion_options", merged.get("deucalion_options"))
        info.setdefault("image", merged.get("image") or settings.DEFAULT_JOB_IMAGE)

        status_payload = get_status(job_id)
        status = status_payload["status"]
        merged["status"] = status
        if "config_path" in merged and not _is_yaml_filename(str(merged["config_path"])):
            # Keep backward compatibility but normalize config extension when legacy jobs exist.
            merged["config_path"] = str(merged["config_path"])
        durations = _compute_job_durations(merged)
        result.append(
            {
                "job_id": job_id,
                "status": status,
                "job_info": info,
                "submitted_at": merged.get("submitted_at"),
                "queued_at": merged.get("queued_at"),
                "dispatched_at": merged.get("dispatched_at"),
                "started_at": merged.get("started_at"),
                "stop_requested_at": merged.get("stop_requested_at"),
                "finished_at": merged.get("finished_at"),
                "last_status_at": merged.get("last_status_at") or status_payload.get("last_status_at"),
                "queue_wait_seconds": durations.get("queue_wait_seconds"),
                "run_duration_seconds": durations.get("run_duration_seconds"),
                "total_duration_seconds": durations.get("total_duration_seconds"),
                "requeue_count": int(merged.get("requeue_count", 0) or 0),
                "attempt_number": int(merged.get("attempt_number", 0) or 0),
                "job_meta": merged,
            }
        )
    return result


def list_queue():
    _mark_stale_jobs()
    entries = job_utils.list_queue()
    tracked = jobs if jobs else job_utils.load_jobs()
    for entry in entries:
        if entry.get("submitted_by"):
            continue
        job_id = entry.get("job_id")
        if not job_id:
            continue
        meta = tracked.get(job_id) or {}
        submitted_by = meta.get("submitted_by")
        if submitted_by:
            entry["submitted_by"] = submitted_by
    return entries

def get_job_info(job_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    p = _info_path(job_id)
    if not os.path.exists(p):
        raise HTTPException(404, "Job info not found")
    with open(p) as f:
        info = json.load(f)
    info = _enrich_job_info_with_mlflow_links(info)
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    resolved_path = _resolved_config_path(job_id)
    info.setdefault("resolved_config_available", os.path.isfile(resolved_path))
    if info["resolved_config_available"]:
        info.setdefault("resolved_config_file", "config.resolved.yaml")
    if not info.get("submitted_by"):
        submitted_by = meta.get("submitted_by")
        if submitted_by:
            info["submitted_by"] = submitted_by
    if not info.get("image"):
        info["image"] = meta.get("image") or settings.DEFAULT_JOB_IMAGE
    if not info.get("image_tag"):
        info["image_tag"] = meta.get("image_tag")
    if not info.get("deucalion_options"):
        info["deucalion_options"] = meta.get("deucalion_options")

    # Expose lifecycle timing in job details overview.
    durations = _compute_job_durations(meta) if meta else {}
    lifecycle_keys = (
        "submitted_at",
        "queued_at",
        "dispatched_at",
        "started_at",
        "stop_requested_at",
        "finished_at",
        "last_status_at",
    )
    for key in lifecycle_keys:
        if key not in info and key in meta:
            info[key] = meta.get(key)
    if durations:
        info.setdefault("queue_wait_seconds", durations.get("queue_wait_seconds"))
        info.setdefault("run_duration_seconds", durations.get("run_duration_seconds"))
        info.setdefault("total_duration_seconds", durations.get("total_duration_seconds"))

    return info

def delete_job(job_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    if job_id not in jobs:
        raise HTTPException(404, "Job not found or already deleted")
    ok = job_utils.delete_job_by_id(job_id)
    if not ok:
        raise HTTPException(500, "Failed to delete job")
    job_utils.remove_from_queue(job_id)
    jobs.pop(job_id, None)
    return {"message": f"Job {job_id} deleted successfully"}

def get_hosts():
    _refresh_jobs()
    _mark_stale_jobs()
    return {
        "available_hosts": settings.AVAILABLE_HOSTS,
        "hosts": _host_status_snapshot(),
    }


def ops_requeue_job(
    job_id: str,
    force: bool = False,
    preferred_host: Optional[str] = None,
    require_host: Optional[bool] = None,
):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if preferred_host:
        if not job_utils.is_valid_host(preferred_host):
            raise HTTPException(400, f"Unknown host '{preferred_host}'. Allowed: {settings.AVAILABLE_HOSTS}")
    preferred = preferred_host or meta.get("preferred_host") or meta.get("target_host")
    if require_host is None:
        require_host = bool(meta.get("require_host", bool(preferred)))

    if not force:
        if status_now == JobStatus.FINISHED.value:
            raise HTTPException(409, "Finished jobs require force to requeue")
        if status_now in (JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            raise HTTPException(409, f"Job is {status_now}; stop it first or use force to requeue")

    prev_host = meta.get("target_host")
    meta = _reset_runtime_metadata(job_id, meta)
    job_utils.remove_from_queue(job_id)
    job_utils.enqueue_job(
        _queue_payload(
            job_id=job_id,
            preferred_host=preferred,
            require_host=require_host,
            submitted_by=meta.get("submitted_by"),
        )
    )

    meta["status"] = JobStatus.QUEUED.value
    meta["preferred_host"] = preferred
    meta["require_host"] = require_host
    meta["target_host"] = preferred if require_host else None
    _persist_job(job_id, meta)

    extra = {
        "requeued_by_ops": True,
        "force": force,
        "requeued_from": prev_host,
        "preferred_host": preferred,
    }
    requires_forced_transition = force or status_now in {
        JobStatus.FINISHED.value,
        JobStatus.FAILED.value,
        JobStatus.STOPPED.value,
        JobStatus.CANCELED.value,
    }
    if requires_forced_transition:
        _force_status(job_id, JobStatus.QUEUED.value, extra)
    else:
        _write_status(job_id, JobStatus.QUEUED.value, extra)

    return {"message": "Job requeued", "job_id": job_id, "status": JobStatus.QUEUED.value}


def ops_fail_job(job_id: str, reason: str = "ops_failed", force: bool = False):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if not force:
        if status_now in (
            JobStatus.FINISHED.value,
            JobStatus.FAILED.value,
            JobStatus.STOPPED.value,
            JobStatus.CANCELED.value,
        ):
            raise HTTPException(409, f"Job already terminal ({status_now})")
        if status_now in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
            raise HTTPException(409, f"Job is {status_now}; use cancel or force to fail")

    job_utils.remove_from_queue(job_id)
    meta["status"] = JobStatus.FAILED.value
    meta["error"] = reason
    _persist_job(job_id, meta)

    extra = {
        "error": reason,
        "failed_by_ops": True,
        "force": force,
        "terminate_requested": status_now in ACTIVE_JOB_STATUSES,
    }
    if force:
        _force_status(job_id, JobStatus.FAILED.value, extra)
    else:
        _write_status(job_id, JobStatus.FAILED.value, extra)

    return {"message": "Job failed", "job_id": job_id, "status": JobStatus.FAILED.value}


def ops_cancel_job(job_id: str, reason: str = "ops_canceled", force: bool = False):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if not force and status_now in (
        JobStatus.FINISHED.value,
        JobStatus.FAILED.value,
        JobStatus.STOPPED.value,
        JobStatus.CANCELED.value,
    ):
        raise HTTPException(409, f"Job already terminal ({status_now})")

    job_utils.remove_from_queue(job_id)
    meta["status"] = JobStatus.CANCELED.value
    meta["error"] = reason
    _persist_job(job_id, meta)

    extra = {"error": reason, "canceled_by_ops": True, "force": force}
    if force:
        _force_status(job_id, JobStatus.CANCELED.value, extra)
    else:
        _write_status(job_id, JobStatus.CANCELED.value, extra)

    return {"message": "Job canceled", "job_id": job_id, "status": JobStatus.CANCELED.value}


def ops_stop_job(job_id: str, reason: str = "ops_stop"):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")

    response = stop_job(job_id, reason=reason, requested_by_ops=True)
    status_now = _read_status_file(job_id) or (jobs.get(job_id) or {}).get("status", JobStatus.UNKNOWN.value)

    response.update({"job_id": job_id, "status": status_now})
    return response


def ops_cleanup_queue(force: bool = False) -> dict:
    _refresh_jobs()
    removed: list[str] = []
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return {"removed": removed, "count": 0}
    if force:
        removed_set: set[str] = set()
        for fname in os.listdir(wdir):
            path = os.path.join(wdir, fname)
            if not os.path.isfile(path):
                continue
            if fname.endswith(".json"):
                job_id = fname[:-5]
            elif ".json.claim." in fname:
                job_id = fname.split(".json.claim.", 1)[0]
            else:
                job_id = fname
            try:
                os.remove(path)
                removed_set.add(job_id)
            except OSError:
                continue
        removed = sorted(removed_set)
        return {"removed": removed, "count": len(removed)}
    for fname in os.listdir(wdir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(wdir, fname)
        try:
            with open(path) as f:
                payload = json.load(f)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        job_id = payload.get("job_id") or fname.rsplit(".", 1)[0]
        meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
        status_now = _read_status_file(job_id) or meta.get("status")
        if not meta or status_now not in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
            try:
                os.remove(path)
                removed.append(job_id)
            except OSError:
                continue
    return {"removed": removed, "count": len(removed)}


def ops_cleanup_jobs(keep: list[str] | None = None) -> dict:
    """Remove job registry entries and clean corresponding/orphan job directories."""
    _refresh_jobs()
    keep_set = set(DEFAULT_JOB_CLEANUP_KEEP)
    if keep:
        keep_set.update(keep)

    tracked = job_utils.load_jobs()
    # Never remove active jobs by default; cleanup is intended for stale/terminal data.
    active_kept: set[str] = set()
    for job_id, tracked_meta in tracked.items():
        in_memory_meta = jobs.get(job_id) or {}
        status_now = _read_status_file(job_id) or in_memory_meta.get("status") or tracked_meta.get("status")
        if status_now in ACTIVE_JOB_STATUSES:
            active_kept.add(job_id)
    keep_set.update(active_kept)

    removed = [job_id for job_id in tracked.keys() if job_id not in keep_set]
    removed_dirs: list[str] = []
    orphan_removed: list[str] = []
    filesystem_errors: dict[str, str] = {}

    if removed:
        for job_id in removed:
            job_dir = Path(_job_dir(job_id))
            if not job_dir.exists():
                continue
            try:
                shutil.rmtree(job_dir)
                removed_dirs.append(job_id)
            except OSError as exc:
                filesystem_errors[job_id] = str(exc)
        job_utils.prune_jobs(keep_set)

    for job_id in removed:
        job_utils.remove_from_queue(job_id)
        jobs.pop(job_id, None)

    # Remove orphan directories not present in tracked jobs and not explicitly kept.
    tracked_after = set(job_utils.load_jobs().keys())
    protected = tracked_after | keep_set
    jobs_root = Path(settings.JOBS_DIR)
    if jobs_root.exists():
        for candidate in jobs_root.iterdir():
            if not candidate.is_dir():
                continue
            job_id = candidate.name
            if job_id in protected:
                continue
            candidate_status = _read_status_file(job_id)
            if candidate_status in ACTIVE_JOB_STATUSES:
                keep_set.add(job_id)
                continue
            # Avoid deleting unrelated directories accidentally.
            looks_like_job_dir = any(
                (candidate / marker).exists()
                for marker in (
                    "status.json",
                    "job_info.json",
                    "logs",
                    "progress",
                    "results",
                    "bundle",
                    "checkpoints",
                    "config.resolved.yaml",
                )
            )
            if not looks_like_job_dir:
                continue
            try:
                shutil.rmtree(candidate)
                orphan_removed.append(job_id)
            except OSError as exc:
                filesystem_errors[job_id] = str(exc)

    _refresh_jobs()
    kept = [job_id for job_id in jobs.keys() if job_id in keep_set]
    return {
        "removed": removed,
        "removed_dirs": removed_dirs,
        "orphan_removed": orphan_removed,
        "filesystem_errors": filesystem_errors,
        "kept": kept,
        "active_kept": sorted(active_kept),
        "count": len(removed),
    }

# ---------- hooks used by agent endpoints ----------
def agent_next_job(worker_id: str):
    with _dispatch_lock(worker_id):
        return _agent_next_job_locked(worker_id)


def _agent_next_job_locked(worker_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    deucalion_active_counts = _deucalion_active_counts_by_profile() if worker_id == "deucalion" else None
    job_queue_entry = job_utils.agent_pop_next_job(
        worker_id,
        can_accept=(
            (lambda payload: _can_dispatch_to_deucalion(payload, deucalion_active_counts))
            if worker_id == "deucalion"
            else None
        ),
    )
    if not job_queue_entry:
        _LOGGER.debug("Worker %s polled queue but no job was available", worker_id)
        return None

    job_id = job_queue_entry["job_id"]

    meta = jobs.get(job_id)
    if not meta:
        _LOGGER.warning("Queue entry for unknown job %s; skipping dispatch", job_id)
        return None

    status_now = _read_status_file(job_id) or meta.get("status")
    if status_now not in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
        _LOGGER.warning("Skipping job %s with status %s (queue entry likely stale)", job_id, status_now)
        return None

    config_path = meta.get("config_path")
    job_name = meta.get("job_name", job_id)

    if not config_path:
        info_path = _info_path(job_id)
        if os.path.exists(info_path):
            with open(info_path) as f:
                info_data = json.load(f)
            config_path = info_data.get("config_path")
            job_name = info_data.get("job_name", job_name)
        if not config_path:
            _write_status(job_id, JobStatus.FAILED.value, {"error": "missing_config"})
            _LOGGER.error("Missing config path for job %s; marked failed", job_id)
            return None

    runtime_config_path = (
        _resolved_config_container_path(job_id)
        if os.path.exists(_resolved_config_path(job_id))
        else config_path
    )
    container_name = _container_name(job_id, job_name)
    command = f"--config {CONTAINER_DATA_ROOT}/{runtime_config_path} --job_id {job_id}"

    response = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": runtime_config_path,
        "source_config_path": config_path,
        "preferred_host": job_queue_entry.get("preferred_host"),
        "image": _normalize_job_image(meta.get("image")),
        "image_tag": meta.get("image_tag"),
        "deucalion_options": meta.get("deucalion_options") if worker_id == "deucalion" else None,
        "command": command,
        "container_name": container_name,
        "volumes": [{
            "host": settings.VM_SHARED_DATA,
            "container": "/data",
            "mode": "rw",
        }],
        "env": {
            "OPEVA_JOB_NAME": str(job_name),
        },
    }
    if worker_id == "deucalion":
        deucalion_tracking_uri = str(settings.DEUCALION_MLFLOW_TRACKING_URI or "").strip()
        if deucalion_tracking_uri:
            response["env"]["MLFLOW_TRACKING_URI"] = deucalion_tracking_uri
    elif settings.MLFLOW_TRACKING_URI:
        response["env"]["MLFLOW_TRACKING_URI"] = str(settings.MLFLOW_TRACKING_URI)
    if settings.MLFLOW_UI_BASE_URL:
        response["env"]["MLFLOW_UI_BASE_URL"] = str(settings.MLFLOW_UI_BASE_URL)

    _LOGGER.info(
        "Dispatching job %s to worker %s (config=%s, preferred=%s)",
        job_id,
        worker_id,
        config_path,
        job_queue_entry.get("preferred_host"),
    )

    meta["status"] = JobStatus.DISPATCHED.value
    meta["target_host"] = worker_id
    _persist_job(job_id, meta)

    _write_status(job_id, JobStatus.DISPATCHED.value, {"worker_id": worker_id})

    info_path = _info_path(job_id)
    info = {}
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
    info["target_host"] = worker_id
    if "job_name" not in info:
        info["job_name"] = job_name
    if "config_path" not in info:
        info["config_path"] = config_path
    if "image" not in info:
        info["image"] = response["image"]
    if "image_tag" not in info and response.get("image_tag"):
        info["image_tag"] = response["image_tag"]
    if worker_id == "deucalion" and response.get("deucalion_options") and "deucalion_options" not in info:
        info["deucalion_options"] = response["deucalion_options"]
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)

    return response

def agent_update_status(job_id: str, status: str, extra: dict | None = None):
    _refresh_jobs()
    _mark_stale_jobs()
    extra = extra or {}
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    try:
        JobStatus(status)
    except ValueError:
        raise HTTPException(400, f"Unknown status '{status}'")
    _LOGGER.info(
        "Agent reported status for job %s: %s (extra keys=%s)",
        job_id,
        status,
        sorted(extra.keys()),
    )
    try:
        _write_status(job_id, status, extra)
    except ValueError as exc:
        raise HTTPException(409, str(exc))

    if status != JobStatus.QUEUED.value:
        job_utils.remove_from_queue(job_id)

    worker = extra.get("worker_id")
    if worker:
        try:
            current_info = {}
            existing_hb = host_heartbeats.get(worker)
            if isinstance(existing_hb, dict) and isinstance(existing_hb.get("info"), dict):
                current_info.update(existing_hb.get("info", {}))
            worker_version = extra.get("worker_version")
            if worker_version:
                current_info["worker_version"] = str(worker_version)
                current_info["last_status_worker_version"] = str(worker_version)
            status_payload = _read_status_payload(job_id) or {}
            current_info["last_status"] = status
            current_info["last_status_job_id"] = job_id
            current_info["last_status_at"] = (
                status_payload.get("last_status_at")
                or status_payload.get("status_updated_at")
                or time.time()
            )
            details_payload = extra.get("details")
            if isinstance(details_payload, dict):
                current_info["last_status_details"] = details_payload
            record_host_heartbeat(worker, current_info)
        except HTTPException:
            # If the worker is unknown, don't block status updates
            _LOGGER.warning("Ignoring heartbeat from unknown worker %s", worker)

    if worker and job_id in jobs:
        meta = jobs[job_id]
        if meta.get("target_host") != worker:
            meta["target_host"] = worker
            _LOGGER.debug("Updating job %s target host to %s", job_id, worker)
            _persist_job(job_id, meta)

    # If agent provided container info, persist to job_info.json and job_track.json
    if {"container_id", "container_name", "exit_code", "error", "details"} & extra.keys():
        info_path = _info_path(job_id)
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        if worker:
            info["target_host"] = worker
        if "container_id" in extra:
            info["container_id"] = extra["container_id"]
        if "container_name" in extra:
            info["container_name"] = extra["container_name"]
        if "exit_code" in extra:
            info["exit_code"] = extra["exit_code"]
        if "error" in extra:
            info["error"] = extra["error"]
        if "details" in extra and isinstance(extra["details"], dict):
            info["details"] = extra["details"]
        with open(info_path, "w") as f:
            json.dump(info, f, indent=2)
        _LOGGER.debug("Persisted container metadata for job %s", job_id)

        tracked = job_utils.load_jobs()
        if job_id in tracked:
            updated = tracked[job_id]
            if "container_id" in extra:
                updated["container_id"] = extra["container_id"]
            if "container_name" in extra:
                updated["container_name"] = extra["container_name"]
            if "exit_code" in extra:
                updated["exit_code"] = extra["exit_code"]
            if "error" in extra:
                updated["error"] = extra["error"]
            if "details" in extra and isinstance(extra["details"], dict):
                updated["details"] = extra["details"]
            _persist_job(job_id, updated)
            _LOGGER.debug("Updated tracked metadata for job %s", job_id)
        elif job_id in jobs:
            # fall back to the in-memory version if the track file is missing
            meta = jobs[job_id]
            if "container_id" in extra:
                meta["container_id"] = extra["container_id"]
            if "container_name" in extra:
                meta["container_name"] = extra["container_name"]
            if "exit_code" in extra:
                meta["exit_code"] = extra["exit_code"]
            if "error" in extra:
                meta["error"] = extra["error"]
            if "details" in extra and isinstance(extra["details"], dict):
                meta["details"] = extra["details"]
            _persist_job(job_id, meta)
            _LOGGER.debug("Updated in-memory metadata for job %s", job_id)

    return {"ok": True}
