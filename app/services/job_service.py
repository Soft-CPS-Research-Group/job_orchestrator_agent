# app/services/job_service.py
import os, re, json, yaml, time, logging, shutil, fcntl, copy, hashlib, hmac, secrets, threading, weakref
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
worker_commands: dict[str, dict] = {}
_worker_commands_lock = threading.Lock()
HEARTBEAT_TTL = settings.HOST_HEARTBEAT_TTL  # backward compatibility for tests

_LOGGER = logging.getLogger(__name__)
_job_state_locks_guard = threading.Lock()
_job_state_locks = weakref.WeakValueDictionary()


def request_worker_authentication(worker_id: str) -> dict:
    if worker_id not in settings.AVAILABLE_HOSTS:
        raise HTTPException(404, f"Unknown worker: {worker_id}")
    if worker_id != "union-inesctec":
        raise HTTPException(400, "Interactive authentication is only supported by the Union worker")
    command = {
        "action": "union_authenticate",
        "request_id": str(uuid4()),
        "requested_at": time.time(),
    }
    with _worker_commands_lock:
        worker_commands[worker_id] = command
    return {"worker_id": worker_id, **command}


def take_worker_command(worker_id: str) -> dict:
    with _worker_commands_lock:
        return worker_commands.pop(worker_id, {})

CAPACITY_COUNT_STATUSES = {
    JobStatus.DISPATCHED.value,
    JobStatus.SETUP.value,
    JobStatus.RUNNING.value,
    JobStatus.STOP_REQUESTED.value,
}

ACTIVE_JOB_STATUSES = {
    JobStatus.DISPATCHED.value,
    JobStatus.SETUP.value,
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
EMAIL_NOTIFICATION_HISTORY_LIMIT = 20
EMAIL_NOTIFICATION_METADATA_KEYS = {"last_email_notification", "email_notifications"}
ATTEMPT_FENCING_CAPABILITY = "attempt_fencing_v1"
INTERNAL_JOB_METADATA_KEYS = {"attempt_token_hash"}

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

ATTEMPT_LIFECYCLE_RESET_FIELDS = {
    "queued_at",
    "dispatched_at",
    "setup_at",
    "started_at",
    "stop_requested_at",
    "finished_at",
    "last_status_at",
    "status_updated_at",
    "queue_wait_seconds",
    "run_duration_seconds",
    "total_duration_seconds",
}

JOB_INFO_RUNTIME_RESET_FIELDS = {
    "container_id",
    "container_name",
    "exit_code",
    "error",
    "details",
    "target_host",
    *ATTEMPT_LIFECYCLE_RESET_FIELDS,
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

DEUCALION_PARTITION_LIMIT_SOURCE = "https://docs.deucalion.macc.fccn.pt/jobs/#partitions-on-deucalion"
DEUCALION_PARTITION_LIMITS = (
    {"partition": "dev-arm", "architecture": "aarch64", "max_nodes": 2, "time_limit_seconds": 4 * 60 * 60},
    {"partition": "normal-arm", "architecture": "aarch64", "max_nodes": 128, "time_limit_seconds": 48 * 60 * 60},
    {"partition": "large-arm", "architecture": "aarch64", "max_nodes": 512, "time_limit_seconds": 72 * 60 * 60},
    {"partition": "dev-x86", "architecture": "x86_64", "max_nodes": 2, "time_limit_seconds": 4 * 60 * 60},
    {"partition": "normal-x86", "architecture": "x86_64", "max_nodes": 64, "time_limit_seconds": 48 * 60 * 60},
    {"partition": "large-x86", "architecture": "x86_64", "max_nodes": 128, "time_limit_seconds": 72 * 60 * 60},
    {"partition": "dev-a100-40", "architecture": "x86_64", "max_nodes": 1, "time_limit_seconds": 4 * 60 * 60},
    {"partition": "normal-a100-40", "architecture": "x86_64", "max_nodes": 4, "time_limit_seconds": 48 * 60 * 60},
    {"partition": "dev-a100-80", "architecture": "x86_64", "max_nodes": 1, "time_limit_seconds": 4 * 60 * 60},
    {"partition": "normal-a100-80", "architecture": "x86_64", "max_nodes": 4, "time_limit_seconds": 48 * 60 * 60},
)
DEUCALION_PARTITION_LIMITS_BY_NAME = {
    row["partition"]: row for row in DEUCALION_PARTITION_LIMITS
}

_image_versions_cache: dict[str, dict] = {}
_progress_eta_cache: dict[str, dict[str, Any]] = {}
_PROGRESS_ETA_CACHE_MAX_ENTRIES = 512
_LOG_CHUNK_DEFAULT_TAIL_LINES = 200
_LOG_CHUNK_DEFAULT_MAX_BYTES = 256 * 1024
_LOG_CHUNK_MAX_BYTES_LIMIT = 2 * 1024 * 1024
CONTAINER_DATA_ROOT = "/data"

ERROR_METADATA_KEYS = ("error_code", "error_category", "error_hint")

ERROR_CLASSIFICATION_RULES = (
    {
        "code": "config_deprecated_algorithm",
        "category": "configuration",
        "hint": "Migrate the config from the deprecated top-level 'algorithm' key to a pipeline list.",
        "patterns": (
            "deprecated top-level 'algorithm' key",
            "migrate to a 'pipeline' list",
        ),
    },
    {
        "code": "deucalion_missing_job_image",
        "category": "worker_payload",
        "hint": "The Deucalion worker did not receive a tagged image for this job.",
        "patterns": ("missing job image in payload for deucalion executor",),
    },
    {
        "code": "deucalion_command_mode_invalid",
        "category": "configuration",
        "hint": "execution.deucalion.command_mode=exec requires an explicit executable command.",
        "patterns": ("command_mode=exec requires an explicit executable",),
    },
    {
        "code": "deucalion_preflight_failed",
        "category": "deucalion_preflight",
        "hint": "The job failed before Slurm submission while preparing remote paths, image, or datasets.",
        "patterns": (
            "submission/preflight failure",
            "preflight failure",
        ),
    },
    {
        "code": "deucalion_connectivity_timeout",
        "category": "deucalion_connectivity",
        "hint": "The worker lost reliable SSH/Slurm connectivity long enough to fail the job.",
        "patterns": (
            "deucalion_unreachable_timeout",
            "ssh command timed out",
            "connection timed out",
            "connectivity timeout reached",
        ),
    },
    {
        "code": "slurm_unknown_timeout",
        "category": "slurm",
        "hint": "Slurm kept returning an unknown state beyond the configured grace period.",
        "patterns": ("slurm_unknown_timeout",),
    },
    {
        "code": "artifact_sync_failed",
        "category": "artifact_sync",
        "hint": "The Slurm job completed, but result/log artifact synchronization failed.",
        "patterns": ("artifact_sync_failed",),
    },
    {
        "code": "slurm_out_of_memory",
        "category": "slurm",
        "hint": "Slurm reported an out-of-memory failure. Increase memory or reduce workload size.",
        "patterns": ("slurm_out_of_memory", "out_of_memory", "out of memory"),
    },
    {
        "code": "slurm_timeout",
        "category": "slurm",
        "hint": "Slurm terminated the job after it reached its walltime limit.",
        "patterns": ("slurm_timeout", "timed out", "time limit"),
    },
    {
        "code": "slurm_cancelled",
        "category": "slurm",
        "hint": "The Slurm job was cancelled before completion.",
        "patterns": ("slurm_cancelled", "cancelled", "canceled"),
    },
    {
        "code": "slurm_failed",
        "category": "slurm",
        "hint": "Slurm reported a failed terminal state. Check the job log for the application error.",
        "patterns": ("slurm_failed", "slurm_fail"),
    },
)


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


@contextmanager
def _job_state_lock(job_id: str):
    """Serialize state transitions for one job across threads and API processes."""
    with _job_state_locks_guard:
        thread_lock = _job_state_locks.setdefault(job_id, threading.RLock())
    with thread_lock:
        lock_dir = _job_dir(job_id)
        os.makedirs(lock_dir, exist_ok=True)
        lock_path = os.path.join(lock_dir, ".state.lock")
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


def _read_text_tail(path: str, max_bytes: int = 64 * 1024) -> str:
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            if size > max_bytes:
                handle.seek(size - max_bytes)
            return handle.read(max_bytes).decode("utf-8", errors="replace")
    except OSError:
        return ""


def _error_classification_text(job_id: str, error: Any, details: dict | None) -> str:
    parts: list[str] = []
    if error is not None:
        parts.append(str(error))
    if isinstance(details, dict):
        for key in (
            "error",
            "message",
            "stderr",
            "stdout",
            "executor_stage",
            "slurm_state",
            "slurm_reason",
            "connectivity",
        ):
            value = details.get(key)
            if value is not None:
                parts.append(str(value))
    log_path = _resolve_log_path(job_id)
    if log_path:
        tail = _read_text_tail(log_path)
        if tail:
            parts.append(tail)
    return "\n".join(parts).lower()


def _classify_job_error(job_id: str, status: str | None, extra: dict | None) -> dict[str, str] | None:
    payload = extra if isinstance(extra, dict) else {}
    explicit_code = payload.get("error_code")
    if isinstance(explicit_code, str) and explicit_code.strip():
        return {
            "code": explicit_code.strip(),
            "category": str(payload.get("error_category") or "unknown"),
            "hint": str(payload.get("error_hint") or ""),
        }

    details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
    has_error_signal = (
        payload.get("error") is not None
        or details.get("error") is not None
        or status == JobStatus.FAILED.value
    )
    if not has_error_signal:
        return None

    stage = details.get("executor_stage")
    if isinstance(stage, str) and stage.startswith("preflight:"):
        for rule in ERROR_CLASSIFICATION_RULES:
            if rule["code"] == "deucalion_preflight_failed":
                return {
                    "code": str(rule["code"]),
                    "category": str(rule["category"]),
                    "hint": str(rule["hint"]),
                }

    text = _error_classification_text(job_id, payload.get("error"), details)
    if not text:
        return None

    for rule in ERROR_CLASSIFICATION_RULES:
        if any(pattern in text for pattern in rule["patterns"]):
            return {
                "code": str(rule["code"]),
                "category": str(rule["category"]),
                "hint": str(rule["hint"]),
            }
    return None


def _enrich_error_metadata(job_id: str, status: str | None, extra: dict) -> None:
    classification = _classify_job_error(job_id, status, extra)
    if not classification:
        return

    extra.setdefault("error_code", classification["code"])
    extra.setdefault("error_category", classification["category"])
    if classification.get("hint"):
        extra.setdefault("error_hint", classification["hint"])

    details = extra.get("details")
    if isinstance(details, dict):
        details.setdefault("error_code", extra["error_code"])
        details.setdefault("error_category", extra["error_category"])
        if extra.get("error_hint"):
            details.setdefault("error_hint", extra["error_hint"])


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
    elif status == JobStatus.SETUP.value:
        meta.setdefault("queued_at", status_ts)
        meta.setdefault("dispatched_at", status_ts)
        meta["setup_at"] = status_ts
        meta.pop("finished_at", None)
    elif status == JobStatus.RUNNING.value:
        meta.setdefault("queued_at", status_ts)
        meta.pop("finished_at", None)
        current_started = _ensure_float(meta.get("started_at"))
        current_dispatched = _ensure_float(meta.get("dispatched_at"))
        has_stale_start = current_dispatched is not None and current_started is not None and current_started < current_dispatched
        if current_started is None or has_stale_start:
            if prev_status == JobStatus.RUNNING.value:
                meta.pop("started_at", None)
            else:
                meta["started_at"] = status_ts
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
    current_dispatched = _ensure_float(meta.get("dispatched_at"))
    if started_ts is not None and current_dispatched is not None and started_ts < current_dispatched:
        started_ts = None
    if started_ts is not None:
        if current_started is None or (current_dispatched is not None and current_started < current_dispatched):
            meta["started_at"] = started_ts
        else:
            meta["started_at"] = min(current_started, started_ts)

    # Queue timing is backend lifecycle timing: queue ends when leaving dispatched/running path.
    if meta.get("status") in TERMINAL_JOB_STATUSES and _ensure_float(meta.get("started_at")) is None:
        meta["started_at"] = status_ts

    if _ensure_float(meta.get("queued_at")) is None:
        fallback_queued = _ensure_float(meta.get("submitted_at"))
        if fallback_queued is not None:
            meta["queued_at"] = fallback_queued


def _email_notification_records(meta: dict) -> list[dict]:
    records: list[dict] = []
    history = meta.get("email_notifications")
    if isinstance(history, list):
        records.extend(item for item in history if isinstance(item, dict))
    last = meta.get("last_email_notification")
    if isinstance(last, dict):
        records.append(last)
    return records


def _current_attempt_running_at(meta: dict) -> float | None:
    dispatched_at = _ensure_float(meta.get("dispatched_at"))
    candidates = []
    for record in _email_notification_records(meta):
        if record.get("status") != JobStatus.RUNNING.value:
            continue
        attempted_at = _parse_timestamp(record.get("attempted_at"))
        if attempted_at is None:
            continue
        if dispatched_at is not None and attempted_at < dispatched_at:
            continue
        candidates.append(attempted_at)
    return min(candidates) if candidates else None


def _repair_active_lifecycle_metadata(meta: dict) -> bool:
    status = str(meta.get("status") or "")
    changed = False
    if status not in TERMINAL_JOB_STATUSES and "finished_at" in meta:
        meta.pop("finished_at", None)
        changed = True

    current_started = _ensure_float(meta.get("started_at"))
    current_dispatched = _ensure_float(meta.get("dispatched_at"))
    has_stale_start = (
        current_started is not None
        and current_dispatched is not None
        and current_started < current_dispatched
    )

    if status == JobStatus.RUNNING.value and (current_started is None or has_stale_start):
        repaired_started = _current_attempt_running_at(meta)
        if repaired_started is not None:
            if current_started != repaired_started:
                meta["started_at"] = repaired_started
                changed = True
        elif has_stale_start:
            meta.pop("started_at", None)
            changed = True
    elif status in {JobStatus.QUEUED.value, JobStatus.DISPATCHED.value, JobStatus.SETUP.value} and has_stale_start:
        meta.pop("started_at", None)
        changed = True

    return changed


def _normalized_job_meta(job_id: str, meta: dict, *, persist: bool = False) -> dict:
    normalized = dict(meta)
    if not normalized:
        return normalized
    if _repair_active_lifecycle_metadata(normalized) and persist:
        _persist_job(job_id, normalized)
    return normalized


def _compute_job_durations(meta: dict, now_ts: float | None = None) -> dict:
    now = now_ts or time.time()
    submitted_at = _ensure_float(meta.get("submitted_at"))
    queued_at = _ensure_float(meta.get("queued_at"))
    started_at = _ensure_float(meta.get("started_at"))
    dispatched_at = _ensure_float(meta.get("dispatched_at"))
    finished_at = _ensure_float(meta.get("finished_at"))
    status = str(meta.get("status") or "")

    if status not in TERMINAL_JOB_STATUSES:
        finished_at = None
    if started_at is not None and dispatched_at is not None and started_at < dispatched_at:
        started_at = None
    if started_at is not None and finished_at is None and started_at >= now:
        started_at = None

    queue_wait_seconds = None
    if queued_at is not None and started_at is not None:
        queue_wait_seconds = max(0.0, started_at - queued_at)

    run_duration_seconds = None
    if started_at is not None:
        run_end = finished_at or now
        run_duration_seconds = max(0.0, run_end - started_at)

    total_duration_seconds = None
    if submitted_at is not None:
        total_end = finished_at or now
        total_duration_seconds = max(0.0, total_end - submitted_at)

    return {
        "queue_wait_seconds": queue_wait_seconds,
        "run_duration_seconds": run_duration_seconds,
        "total_duration_seconds": total_duration_seconds,
    }


def _number_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def _progress_number(payload: dict, *keys: str) -> float | None:
    for key in keys:
        value = _number_value(payload.get(key))
        if value is not None:
            return value
    return None


def _progress_timestamp(payload: dict) -> float | None:
    for key in ("updated_at", "timestamp", "last_update", "last_updated_at", "time"):
        parsed = _parse_timestamp(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _load_eta_config(job_id: str, meta: dict) -> dict:
    candidate_paths = [_resolved_config_path(job_id)]
    config_path = meta.get("config_path")
    if isinstance(config_path, str) and config_path.strip():
        raw_config_path = config_path.strip()
        if os.path.isabs(raw_config_path):
            candidate_paths.append(raw_config_path)
        relative_path = os.path.normpath(raw_config_path.lstrip("/"))
        if relative_path and relative_path != "." and not relative_path.startswith(".."):
            if relative_path == "configs":
                candidate_paths.append(settings.CONFIGS_DIR)
            elif relative_path.startswith("configs/"):
                candidate_paths.append(os.path.join(settings.CONFIGS_DIR, relative_path[len("configs/") :]))
            else:
                candidate_paths.append(os.path.join(settings.CONFIGS_DIR, relative_path))
            candidate_paths.append(os.path.join(settings.VM_SHARED_DATA, relative_path))

    seen_paths: set[str] = set()
    for path in candidate_paths:
        if path in seen_paths:
            continue
        seen_paths.add(path)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
            return payload if isinstance(payload, dict) else {}
        except Exception:
            _LOGGER.debug("Unable to read config for ETA calculation: %s", path, exc_info=True)
    return {}


def _nested_mapping(payload: dict, key: str) -> dict:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _eta_total_work_from_config(config: dict) -> tuple[float | None, str | None]:
    simulator = _nested_mapping(config, "simulator")
    candidates = [simulator, config]

    episodes = next(
        (
            value
            for section in candidates
            for value in (_progress_number(section, "episodes", "episode_total", "num_episodes"),)
            if value is not None and value > 0
        ),
        None,
    )

    per_episode_steps = next(
        (
            value
            for section in candidates
            for value in (
                _progress_number(
                    section,
                    "episode_time_steps",
                    "time_steps",
                    "timesteps",
                    "steps",
                    "max_steps",
                    "step_total",
                ),
            )
            if value is not None and value > 0
        ),
        None,
    )

    if per_episode_steps is None:
        start = next(
            (
                value
                for section in candidates
                for value in (_progress_number(section, "simulation_start_time_step", "start_time_step"),)
                if value is not None
            ),
            None,
        )
        end = next(
            (
                value
                for section in candidates
                for value in (_progress_number(section, "simulation_end_time_step", "end_time_step"),)
                if value is not None
            ),
            None,
        )
        if start is not None and end is not None and end >= start:
            per_episode_steps = end - start + 1

    if episodes is not None and per_episode_steps is not None:
        return episodes * per_episode_steps, "config_simulator"
    if per_episode_steps is not None:
        return per_episode_steps, "config_steps"
    return None, None


def _progress_fraction(payload: dict, config_total_work: float | None = None) -> dict:
    percent_keys = ("progress_pct", "percent", "progress_percent", "completion")
    percent_value = None
    percent_key = None
    for key in percent_keys:
        value = _progress_number(payload, key)
        if value is not None:
            percent_value = value
            percent_key = key
            break
    if percent_value is None and isinstance(payload.get("progress"), dict):
        for key in ("percent", "value", "progress"):
            value = _progress_number(payload["progress"], key)
            if value is not None:
                percent_value = value
                percent_key = f"progress.{key}"
                break
    elif percent_value is None:
        raw_progress = payload.get("progress")
        if not isinstance(raw_progress, str):
            percent_value = _number_value(raw_progress)
            if percent_value is not None:
                percent_key = "progress"

    current = None
    total = None
    source = None
    unit = "work"

    for current_key, total_key, candidate_unit in (
        ("global_step_current", "global_step_total", "step"),
        ("global_step", "global_step_total", "step"),
        ("step_current", "step_total", "step"),
        ("step", "step_total", "step"),
        ("timestep_current", "timestep_total", "timestep"),
        ("time_step_current", "time_step_total", "timestep"),
        ("episode_current", "episode_total", "episode"),
        ("episode", "episode_total", "episode"),
    ):
        current_value = _progress_number(payload, current_key)
        total_value = _progress_number(payload, total_key)
        if current_value is None or total_value is None or total_value <= 0:
            continue
        current = current_value
        total = total_value
        source = f"{current_key}/{total_key}"
        unit = candidate_unit
        break

    if current is None:
        step_current = _progress_number(payload, "step_current", "step", "timestep_current", "time_step_current")
        episode_current = _progress_number(payload, "episode_current", "episode")
        episode_total = _progress_number(payload, "episode_total", "episodes")
        step_total = _progress_number(payload, "step_total", "episode_time_steps")
        if step_current is not None and step_total is not None and step_total > 0 and episode_total:
            episode_index = 0.0
            if episode_current is not None:
                # Prefer one-based episode_current when available; fall back to zero-based episode.
                episode_index = max(0.0, episode_current - 1 if "episode_current" in payload else episode_current)
            current = episode_index * step_total + step_current
            total = episode_total * step_total
            source = "episode_step"
            unit = "step"

    if current is None:
        current = _progress_number(payload, "current", "current_step", "completed", "completed_steps")
        total = _progress_number(payload, "total", "total_steps", "target")
        if total is None:
            total = config_total_work
        if current is not None and total is not None and total > 0:
            source = "current_total" if "total" in payload or "total_steps" in payload else "config_total"
            unit = "step"

    if current is not None and total is not None and total > 0:
        percent = max(0.0, min(100.0, (current / total) * 100.0))
    elif percent_value is not None:
        percent = (
            percent_value
            if percent_key in {"progress_pct", "progress_percent"}
            else percent_value * 100.0 if 0 <= percent_value <= 1 else percent_value
        )
        percent = max(0.0, min(100.0, percent))
        return {"percent": percent, "source": percent_key or "percent", "unit": "percent"}
    else:
        return {"percent": None, "source": None, "unit": None}

    return {
        "percent": percent,
        "current": current,
        "total": total,
        "source": source or "percent",
        "unit": unit,
    }


def _job_runtime_elapsed_seconds(tracked: dict, *, now_ts: float) -> float | None:
    elapsed = _ensure_float(_compute_job_durations(tracked, now_ts=now_ts).get("run_duration_seconds"))
    return max(0.0, elapsed) if elapsed is not None else None


def _progress_eta(job_id: str, payload: dict) -> dict:
    tracked = _normalized_job_meta(job_id, jobs.get(job_id) or job_utils.load_jobs().get(job_id, {}) or {}, persist=True)
    status_payload = _read_status_payload(job_id) or {}
    status = status_payload.get("status") or tracked.get("status")

    if status != JobStatus.RUNNING.value:
        _progress_eta_cache.pop(job_id, None)
        return {
            "available": False,
            "reason": "job_not_running",
            "source": None,
        }

    signature = (
        _ensure_float(tracked.get("started_at")),
        int(tracked.get("attempt_number", 0) or 0),
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
    )
    cached = _progress_eta_cache.get(job_id)
    if cached and cached.get("signature") == signature:
        return copy.deepcopy(cached["eta"])

    now_ts = time.time()
    elapsed = _job_runtime_elapsed_seconds(tracked, now_ts=now_ts)

    progress = _progress_fraction(payload)
    percent = progress.get("percent")
    config_source = None
    if percent is None and elapsed is not None:
        config_total_work, config_source = _eta_total_work_from_config(_load_eta_config(job_id, tracked))
        progress = _progress_fraction(payload, config_total_work=config_total_work)
        percent = progress.get("percent")
    if percent is None:
        eta = {"available": False, "reason": "progress_unavailable", "source": None}
        _cache_progress_eta(job_id, signature, eta)
        return eta

    if percent >= 100:
        eta = {
            "available": True,
            "state": "complete",
            "eta_seconds": 0,
            "estimated_finish_at": now_ts,
            "source": progress.get("source"),
            "confidence": "progress",
            "progress_percent": 100.0,
            "current": progress.get("current"),
            "total": progress.get("total"),
            "unit": progress.get("unit"),
        }
        _cache_progress_eta(job_id, signature, eta)
        return eta

    if percent <= 0:
        eta = {
            "available": False,
            "reason": "progress_not_started",
            "source": progress.get("source"),
            "progress_percent": percent,
            "current": progress.get("current"),
            "total": progress.get("total"),
            "unit": progress.get("unit"),
        }
        _cache_progress_eta(job_id, signature, eta)
        return eta

    if elapsed is None:
        eta = {
            "available": False,
            "reason": "runtime_unavailable",
            "source": progress.get("source"),
            "progress_percent": percent,
            "current": progress.get("current"),
            "total": progress.get("total"),
            "unit": progress.get("unit"),
        }
        _cache_progress_eta(job_id, signature, eta)
        return eta

    fraction = percent / 100.0
    eta_seconds = max(0.0, elapsed * ((1.0 - fraction) / fraction))
    updated_at = _progress_timestamp(payload)
    confidence = "progress_rate"
    if config_source and progress.get("source") in {"config_total", "current_total"}:
        confidence = "progress_rate_config_total"

    eta = {
        "available": True,
        "state": "running",
        "eta_seconds": eta_seconds,
        "estimated_finish_at": now_ts + eta_seconds,
        "elapsed_seconds": elapsed,
        "source": progress.get("source"),
        "confidence": confidence,
        "progress_percent": percent,
        "current": progress.get("current"),
        "total": progress.get("total"),
        "unit": progress.get("unit"),
        "updated_at": updated_at,
    }
    _cache_progress_eta(job_id, signature, eta)
    return eta


def _cache_progress_eta(job_id: str, signature: tuple, eta: dict) -> None:
    if len(_progress_eta_cache) >= _PROGRESS_ETA_CACHE_MAX_ENTRIES and job_id not in _progress_eta_cache:
        oldest_job_id = next(iter(_progress_eta_cache), None)
        if oldest_job_id is not None:
            _progress_eta_cache.pop(oldest_job_id, None)
    _progress_eta_cache[job_id] = {
        "signature": signature,
        "eta": copy.deepcopy(eta),
    }


def _enrich_progress_payload(job_id: str, payload: dict) -> dict:
    enriched = dict(payload)
    eta = _progress_eta(job_id, enriched)
    enriched["eta"] = eta
    if eta.get("available"):
        enriched["eta_seconds"] = eta.get("eta_seconds")
        enriched["estimated_finish_at"] = eta.get("estimated_finish_at")
        enriched["eta_source"] = eta.get("source")
        enriched["eta_confidence"] = eta.get("confidence")
    return enriched


def _queue_estimate_payload(
    *,
    available: bool,
    reason: str,
    target_host: str | None = None,
    profile: str | None = None,
    estimated_start_at: float | None = None,
    now_ts: float | None = None,
    blocking_job_id: str | None = None,
    queue_position: int | None = None,
) -> dict:
    payload = {
        "available": available,
        "kind": "estimated_start",
        "reason": reason,
        "source": "orchestrator_queue",
        "target_host": target_host,
        "profile": profile,
        "estimated_start_at": estimated_start_at if available else None,
        "estimated_start_seconds": (
            max(0.0, estimated_start_at - (now_ts or time.time()))
            if available and estimated_start_at is not None
            else None
        ),
    }
    if blocking_job_id:
        payload["blocking_job_id"] = blocking_job_id
    if queue_position is not None:
        payload["queue_position"] = queue_position
    return payload


def _queue_target_group(entry: dict, meta: dict) -> tuple[tuple[str, str], str, str | None] | None:
    job_id = entry.get("job_id")
    preferred = entry.get("preferred_host") or meta.get("preferred_host") or meta.get("target_host")
    require_host = bool(entry.get("require_host", meta.get("require_host", bool(preferred))))
    if not isinstance(job_id, str) or not job_id:
        return None
    if not isinstance(preferred, str) or not preferred.strip() or not require_host:
        return None
    host = preferred.strip()
    if host == "deucalion":
        profile = _deucalion_job_profile(job_id, meta)
        return (host, profile), host, profile
    return (host, "default"), host, None


def _queue_group_capacity(host: str, profile: str | None) -> int:
    if host == "deucalion":
        limits = _deucalion_profile_limits()
        return max(0, int(limits.get(profile or "cpu", 0)))

    hb = host_heartbeats.get(host)
    info = hb.get("info") if isinstance(hb, dict) and isinstance(hb.get("info"), dict) else {}
    configured = _as_positive_int(info.get("max_active_jobs"))
    return configured if configured is not None else 1


def _queue_group_online(host: str, active_job_ids: list[str], *, now_ts: float) -> bool:
    hb = host_heartbeats.get(host)
    if isinstance(hb, dict) and (now_ts - float(hb.get("last_seen", 0) or 0)) <= settings.HOST_HEARTBEAT_TTL:
        return True
    return bool(active_job_ids)


def _queue_group_active_job_ids(host: str, profile: str | None) -> list[str]:
    if host == "deucalion":
        return _deucalion_active_job_ids_by_profile().get(profile or "cpu", [])
    return _active_job_ids_for_host(host)


def _active_job_estimated_finish_at(job_id: str) -> float | None:
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status = _read_status_file(job_id) or meta.get("status")
    if status != JobStatus.RUNNING.value:
        return None
    payload = file_utils.read_progress(job_id)
    if not isinstance(payload, dict):
        payload = {"progress": payload}
    eta = _progress_eta(job_id, payload)
    if not eta.get("available"):
        return None
    estimated_finish_at = _ensure_float(eta.get("estimated_finish_at"))
    if estimated_finish_at is None:
        return None
    return max(time.time(), estimated_finish_at)


def _first_queued_start_estimate(
    *,
    host: str,
    profile: str | None,
    now_ts: float,
    queue_position: int,
) -> dict:
    capacity = _queue_group_capacity(host, profile)
    active_job_ids = _queue_group_active_job_ids(host, profile)
    if capacity <= 0:
        return _queue_estimate_payload(
            available=False,
            reason="no_capacity",
            target_host=host,
            profile=profile,
            now_ts=now_ts,
            queue_position=queue_position,
        )

    if len(active_job_ids) < capacity:
        if not _queue_group_online(host, active_job_ids, now_ts=now_ts):
            return _queue_estimate_payload(
                available=False,
                reason="host_unavailable",
                target_host=host,
                profile=profile,
                now_ts=now_ts,
                queue_position=queue_position,
            )
        return _queue_estimate_payload(
            available=True,
            reason="slot_available",
            target_host=host,
            profile=profile,
            estimated_start_at=now_ts,
            now_ts=now_ts,
            queue_position=queue_position,
        )

    finishes: list[tuple[str, float]] = []
    for active_job_id in active_job_ids:
        finish_at = _active_job_estimated_finish_at(active_job_id)
        if finish_at is None:
            return _queue_estimate_payload(
                available=False,
                reason="active_eta_unavailable",
                target_host=host,
                profile=profile,
                now_ts=now_ts,
                blocking_job_id=active_job_id,
                queue_position=queue_position,
            )
        finishes.append((active_job_id, finish_at))

    blocking_job_id, estimated_start_at = min(finishes, key=lambda item: item[1])
    return _queue_estimate_payload(
        available=True,
        reason="waiting_for_active_job",
        target_host=host,
        profile=profile,
        estimated_start_at=estimated_start_at,
        now_ts=now_ts,
        blocking_job_id=blocking_job_id,
        queue_position=queue_position,
    )


def _queued_start_estimates(queue_entries: list[dict] | None = None, *, now_ts: float | None = None) -> dict[str, dict]:
    now = now_ts or time.time()
    entries = queue_entries if queue_entries is not None else job_utils.list_queue()
    tracked = jobs if jobs else job_utils.load_jobs()
    group_counts: dict[tuple[str, str], int] = {}
    estimates: dict[str, dict] = {}

    for entry in entries:
        job_id = entry.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            continue
        meta = tracked.get(job_id) or {}
        status = _read_status_file(job_id) or meta.get("status")
        if status not in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
            estimates[job_id] = _queue_estimate_payload(
                available=False,
                reason="job_not_queued",
                now_ts=now,
            )
            continue

        target = _queue_target_group(entry, meta)
        if target is None:
            estimates[job_id] = _queue_estimate_payload(
                available=False,
                reason="target_ambiguous",
                now_ts=now,
            )
            continue

        group_key, host, profile = target
        queue_position = group_counts.get(group_key, 0) + 1
        group_counts[group_key] = queue_position

        if queue_position > 1:
            estimates[job_id] = _queue_estimate_payload(
                available=False,
                reason="queued_behind_job",
                target_host=host,
                profile=profile,
                now_ts=now,
                queue_position=queue_position,
            )
            continue

        estimates[job_id] = _first_queued_start_estimate(
            host=host,
            profile=profile,
            now_ts=now,
            queue_position=queue_position,
        )

    return estimates


def _without_email_notification_metadata(payload: dict) -> dict:
    return {key: value for key, value in payload.items() if key not in EMAIL_NOTIFICATION_METADATA_KEYS}


def _public_job_metadata(payload: dict) -> dict:
    sanitized = _without_email_notification_metadata(payload)
    for key in INTERNAL_JOB_METADATA_KEYS:
        sanitized.pop(key, None)
    return sanitized


def _status_notification_meta(job_id: str, status: str) -> dict[str, Any]:
    meta = dict(jobs.get(job_id) or job_utils.load_jobs().get(job_id, {}) or {})
    status_payload = _read_status_payload(job_id) or {}
    meta.update(status_payload)
    meta["job_id"] = job_id
    meta["status"] = status
    meta.update(_compute_job_durations(meta))
    return meta


def _append_email_notification_record(job_id: str, record: dict[str, Any] | None) -> None:
    if not record:
        return

    meta = dict(jobs.get(job_id) or job_utils.load_jobs().get(job_id, {}) or {})
    existing = meta.get("email_notifications")
    history = list(existing) if isinstance(existing, list) else []
    history.append(record)
    history = history[-EMAIL_NOTIFICATION_HISTORY_LIMIT:]

    if meta:
        meta["last_email_notification"] = record
        meta["email_notifications"] = history
        _persist_job(job_id, meta)

    status_payload = _read_status_payload(job_id)
    if status_payload:
        status = str(status_payload.get("status") or meta.get("status") or JobStatus.UNKNOWN.value)
        extra = {key: value for key, value in status_payload.items() if key not in {"job_id", "status"}}
        extra["last_email_notification"] = record
        extra["email_notifications"] = history
        job_utils.write_status_file(job_id, status, extra)

    info_path = _info_path(job_id)
    if os.path.exists(info_path):
        try:
            info = _read_job_info_payload(job_id)
            info["last_email_notification"] = record
            info["email_notifications"] = history
            with open(info_path, "w") as handle:
                json.dump(info, handle, indent=2)
        except Exception:
            _LOGGER.warning("Failed to persist email notification metadata for job %s", job_id, exc_info=True)


def _notify_status_change(job_id: str, previous_status: str | None, status: str) -> None:
    record = email_notification_service.notify_job_status_change(
        job_id=job_id,
        previous_status=previous_status,
        status=status,
        job=_status_notification_meta(job_id, status),
    )
    _append_email_notification_record(job_id, record)


def _attempt_token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _invalidate_attempt_fence(job_id: str, invalidated_at: float) -> None:
    meta = jobs.get(job_id)
    if not isinstance(meta, dict) or meta.get("attempt_fencing_enabled") is not True:
        return
    meta.pop("attempt_token_hash", None)
    meta["attempt_fence_invalidated_at"] = invalidated_at


def _validate_agent_attempt(job_id: str, meta: dict, extra: dict) -> None:
    provided_token = extra.pop("attempt_token", None)
    provided_attempt = extra.get("attempt_number")
    if meta.get("attempt_fencing_enabled") is not True:
        return

    expected_hash = meta.get("attempt_token_hash")
    expected_attempt = int(meta.get("attempt_number", 0) or 0)
    expected_worker = str(meta.get("target_host") or "").strip()
    provided_worker = str(extra.get("worker_id") or "").strip()
    valid_token = (
        isinstance(expected_hash, str)
        and bool(expected_hash)
        and isinstance(provided_token, str)
        and bool(provided_token)
        and hmac.compare_digest(expected_hash, _attempt_token_digest(provided_token))
    )
    valid_attempt = isinstance(provided_attempt, int) and not isinstance(provided_attempt, bool) and (
        provided_attempt == expected_attempt
    )
    valid_worker = bool(expected_worker) and provided_worker == expected_worker
    if valid_token and valid_attempt and valid_worker:
        return

    _LOGGER.warning(
        "Rejected stale or unfenced status update for job %s from worker %s (attempt=%r current_attempt=%d)",
        job_id,
        provided_worker or "unknown",
        provided_attempt,
        expected_attempt,
    )
    raise HTTPException(
        409,
        {
            "code": "stale_job_attempt",
            "message": "Status update does not belong to the current dispatched attempt",
        },
    )



def _write_status(job_id: str, status: str, extra: dict | None = None):
    """Persist status to disk and update the in-memory jobs cache."""
    previous_payload = _read_status_payload(job_id) or {}
    prev = previous_payload.get("status") or _read_status_file(job_id)
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
    if status == JobStatus.QUEUED.value and prev != JobStatus.QUEUED.value:
        _invalidate_attempt_fence(job_id, status_ts)
    extra_payload = dict(extra or {})
    for key in EMAIL_NOTIFICATION_METADATA_KEYS:
        if key not in extra_payload and key in previous_payload:
            extra_payload[key] = previous_payload[key]
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
        _repair_active_lifecycle_metadata(jobs[job_id])
        job_utils.save_job(job_id, jobs[job_id])
    _notify_status_change(job_id, prev, status)


def _force_status(job_id: str, status: str, extra: dict | None = None) -> None:
    """Write status without enforcing state transitions (ops override)."""
    previous_payload = _read_status_payload(job_id) or {}
    prev = previous_payload.get("status") or _read_status_file(job_id)
    status_ts = time.time()
    if status == JobStatus.QUEUED.value and prev != JobStatus.QUEUED.value:
        _invalidate_attempt_fence(job_id, status_ts)
    extra_payload = dict(extra or {})
    for key in EMAIL_NOTIFICATION_METADATA_KEYS:
        if key not in extra_payload and key in previous_payload:
            extra_payload[key] = previous_payload[key]
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
        _repair_active_lifecycle_metadata(meta)
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


def _format_duration_hours(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _format_duration_label(seconds: int) -> str:
    hours = seconds // 3600
    if seconds % 3600 == 0:
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    return _format_duration_hours(seconds)


def _parse_slurm_time_limit_seconds(value: Any) -> int:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError("time limit is empty")

    days = 0
    time_part = text
    has_day_prefix = "-" in text
    if has_day_prefix:
        day_part, time_part = text.split("-", 1)
        if not day_part.isdigit() or not time_part:
            raise ValueError("invalid Slurm time format")
        days = int(day_part)

    parts = time_part.split(":")
    if not 1 <= len(parts) <= 3 or any(not part.isdigit() for part in parts):
        raise ValueError("invalid Slurm time format")

    values = [int(part) for part in parts]
    if has_day_prefix:
        hours = values[0]
        minutes = values[1] if len(values) >= 2 else 0
        seconds = values[2] if len(values) >= 3 else 0
    elif len(values) == 1:
        hours = 0
        minutes = values[0]
        seconds = 0
    elif len(values) == 2:
        hours = 0
        minutes, seconds = values
    else:
        hours, minutes, seconds = values

    if minutes >= 60 or seconds >= 60:
        raise ValueError("minutes and seconds must be below 60")
    total = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("time limit must be greater than zero")
    return total


def get_deucalion_partition_limits() -> dict:
    partitions = []
    for row in DEUCALION_PARTITION_LIMITS:
        seconds = int(row["time_limit_seconds"])
        partitions.append(
            {
                **row,
                "time_limit": _format_duration_hours(seconds),
                "time_limit_label": _format_duration_label(seconds),
            }
        )
    return {
        "source": DEUCALION_PARTITION_LIMIT_SOURCE,
        "partitions": partitions,
    }


def _validate_deucalion_walltime_options(options: dict | None) -> None:
    if not options:
        return
    partition_raw = options.get("partition")
    partition = str(partition_raw).strip().lower() if partition_raw is not None else ""
    if partition:
        limit = DEUCALION_PARTITION_LIMITS_BY_NAME.get(partition)
        if limit is None:
            allowed = ", ".join(row["partition"] for row in DEUCALION_PARTITION_LIMITS)
            raise HTTPException(400, f"Unknown Deucalion partition '{partition_raw}'. Allowed: {allowed}")
        options["partition"] = partition
    else:
        limit = None

    requested_gpus = _as_positive_int(options.get("gpus"))
    if partition and _is_gpu_like_partition(partition):
        if "gpus" not in options:
            options["gpus"] = 1
        elif requested_gpus is None:
            raise HTTPException(400, f"deucalion_options.gpus must be > 0 for GPU partition '{partition}'")
    elif partition and requested_gpus is not None and not _is_gpu_like_partition(partition):
        raise HTTPException(400, f"deucalion_options.gpus requires a GPU partition, got '{partition}'")

    time_limit = options.get("time") or options.get("time_limit")
    if time_limit is None:
        return

    try:
        requested_seconds = _parse_slurm_time_limit_seconds(time_limit)
    except ValueError as exc:
        raise HTTPException(
            400,
            (
                "Invalid deucalion_options.time. Use Slurm time format such as "
                "04:00:00, 2-00:00:00, or minutes."
            ),
        ) from exc

    if limit is None:
        return

    max_seconds = int(limit["time_limit_seconds"])
    if requested_seconds > max_seconds:
        max_label = _format_duration_label(max_seconds)
        raise HTTPException(
            400,
            (
                f"deucalion_options.time exceeds the {max_label} walltime limit "
                f"for partition '{partition}'"
            ),
        )


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


def _truthy_config_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _config_value_requests_gpu(key: Any, value: Any) -> bool:
    key_text = str(key).strip().lower()
    if key_text in {
        "cuda_required",
        "require_cuda",
        "requires_cuda",
        "require_gpu",
        "requires_gpu",
        "gpu_required",
        "use_cuda",
        "use_gpu",
    }:
        return _truthy_config_value(value)
    if key_text in {"gpus", "gpu_count", "gpus_per_task"}:
        return _as_positive_int(value) is not None
    if key_text in {"device", "torch_device", "accelerator"} and isinstance(value, str):
        normalized = value.strip().lower()
        return "cuda" in normalized or normalized in {"gpu", "mps"}
    return False


def _config_requires_gpu(payload: Any) -> bool:
    if isinstance(payload, dict):
        if _payload_indicates_gpu(payload):
            return True
        for key, value in payload.items():
            if _config_value_requests_gpu(key, value):
                return True
            if isinstance(value, (dict, list)) and _config_requires_gpu(value):
                return True
    elif isinstance(payload, list):
        return any(_config_requires_gpu(item) for item in payload)
    return False


def _job_requires_gpu(job_id: str, meta: dict | None = None) -> bool:
    metadata = meta or jobs.get(job_id) or {}
    for key in ("cuda_required", "require_cuda", "requires_cuda", "require_gpu", "requires_gpu", "gpu_required"):
        if _truthy_config_value(metadata.get(key)):
            return True
    if _payload_indicates_gpu(metadata.get("deucalion_options")):
        return True
    return _config_requires_gpu(_load_eta_config(job_id, metadata))


def _worker_supports_gpu(worker_id: str) -> bool:
    if worker_id == "deucalion" or _is_jetson_worker(worker_id) or _is_union_worker(worker_id):
        return True
    hb = host_heartbeats.get(worker_id)
    info = hb.get("info") if isinstance(hb, dict) else None
    if not isinstance(info, dict):
        return False
    for key in ("gpu_enabled", "gpu_required", "has_gpu", "gpu", "cuda_available"):
        if _truthy_config_value(info.get(key)):
            return True
    return False


def _normalize_target_worker_profile(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HTTPException(400, "target_worker_profile must be 'cpu' or 'gpu'")
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized not in {"cpu", "gpu"}:
        raise HTTPException(400, "target_worker_profile must be 'cpu' or 'gpu'")
    return normalized


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
    target_profile = _normalize_target_worker_profile(metadata.get("target_worker_profile"))
    if target_profile:
        return target_profile
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


def _can_dispatch_to_worker(
    worker_id: str,
    queue_payload: dict,
    *,
    deucalion_active_counts: dict[str, int] | None = None,
    supports_attempt_fencing: bool = False,
) -> bool:
    job_id = queue_payload.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        return True

    meta = jobs.get(job_id) or {}
    if meta.get("attempt_fencing_enabled") is True and not supports_attempt_fencing:
        _LOGGER.info(
            "Legacy worker %s cannot accept fenced job %s; keeping it queued",
            worker_id,
            job_id,
        )
        return False
    recovery_host = _pending_persistent_worker_recovery_host(job_id, meta)
    if recovery_host and worker_id != recovery_host:
        _LOGGER.info(
            "Worker %s cannot accept job %s while persistent recovery belongs to %s",
            worker_id,
            job_id,
            recovery_host,
        )
        return False
    target_worker_profile = _normalize_target_worker_profile(
        queue_payload.get("target_worker_profile") or meta.get("target_worker_profile")
    )
    if target_worker_profile == "gpu" and not _worker_supports_gpu(worker_id):
        _LOGGER.info("Worker %s does not advertise GPU support; keeping GPU-targeted job %s queued", worker_id, job_id)
        return False
    if target_worker_profile == "cpu" and _worker_supports_gpu(worker_id) and worker_id != "deucalion":
        _LOGGER.info("Worker %s advertises GPU support; keeping CPU-targeted job %s queued", worker_id, job_id)
        return False
    if _job_requires_gpu(job_id, meta) and not _worker_supports_gpu(worker_id):
        _LOGGER.info("Worker %s does not advertise GPU support; keeping GPU job %s queued", worker_id, job_id)
        return False
    if worker_id == "deucalion":
        try:
            _validate_deucalion_sif_tag_available(_normalize_image_tag(meta.get("image_tag")))
        except HTTPException as exc:
            _LOGGER.info("Deucalion cannot accept job %s: %s", job_id, exc.detail)
            return False
        return _can_dispatch_to_deucalion(queue_payload, deucalion_active_counts)
    if _is_jetson_worker(worker_id):
        try:
            image_tag = _normalize_image_tag(meta.get("image_tag"))
            _validate_jetson_image_tag_available(image_tag)
        except HTTPException as exc:
            _LOGGER.info("Worker %s cannot accept job %s: %s", worker_id, job_id, exc.detail)
            return False
    if _is_union_worker(worker_id):
        try:
            image_tag = _normalize_image_tag(meta.get("image_tag"))
            _validate_union_image_tag_available(image_tag)
        except HTTPException as exc:
            _LOGGER.info("Worker %s cannot accept job %s: %s", worker_id, job_id, exc.detail)
            return False
    return True


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


def _jetson_worker_hosts() -> set[str]:
    return {str(host).strip() for host in (settings.JETSON_WORKER_HOSTS or []) if str(host).strip()}


def _is_jetson_worker(worker_id: str | None) -> bool:
    return str(worker_id or "").strip() in _jetson_worker_hosts()


def _jetson_image_tag(image_tag: str) -> str:
    tag = _normalize_image_tag(image_tag)
    suffix = str(settings.JETSON_IMAGE_TAG_SUFFIX or "").strip()
    if suffix and not tag.endswith(suffix):
        return f"{tag}{suffix}"
    return tag


def _union_worker_hosts() -> set[str]:
    return {str(host).strip() for host in (settings.UNION_WORKER_HOSTS or []) if str(host).strip()}


def _is_union_worker(worker_id: str | None) -> bool:
    return str(worker_id or "").strip() in _union_worker_hosts()


def _union_image_tag(image_tag: str) -> str:
    tag = _normalize_image_tag(image_tag)
    suffix = str(settings.UNION_IMAGE_TAG_SUFFIX or "").strip()
    if suffix and not tag.endswith(suffix):
        return f"{tag}{suffix}"
    return tag


def _effective_image_tag_for_worker(worker_id: str, image_tag: str) -> str:
    if _is_jetson_worker(worker_id):
        return _jetson_image_tag(image_tag)
    if _is_union_worker(worker_id):
        return _union_image_tag(image_tag)
    return _normalize_image_tag(image_tag)


def _resolve_job_image_for_worker(worker_id: str, image_tag: str) -> tuple[str, str]:
    effective_tag = _effective_image_tag_for_worker(worker_id, image_tag)
    return _resolve_job_image_from_tag(effective_tag), effective_tag


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


def _fetch_dockerhub_tag(repository: str, tag: str) -> tuple[dict | None, bool, float]:
    repo = _normalize_image_repository(repository)
    tag_name = str(tag or "").strip()
    if not tag_name:
        raise HTTPException(400, "Image tag is required")

    cache_key = f"{repo}:tag:{tag_name}"
    now = time.time()
    ttl = max(0, int(settings.JOB_IMAGE_CATALOG_TTL_SECONDS))
    cached = _image_versions_cache.get(cache_key)
    if cached and (now - cached.get("fetched_at", 0.0)) < ttl:
        return cached.get("tag"), True, cached["fetched_at"]

    namespace, name = repo.split("/", 1)
    url = (
        "https://hub.docker.com/v2/repositories/"
        f"{urllib_parse.quote(namespace)}/{urllib_parse.quote(name)}"
        f"/tags/{urllib_parse.quote(tag_name, safe='')}"
    )
    req = urllib_request.Request(url, headers={"Accept": "application/json"})
    timeout_seconds = max(1, int(settings.JOB_IMAGE_CATALOG_TIMEOUT_SECONDS))
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        if exc.code == 404:
            fetched_at = time.time()
            _image_versions_cache[cache_key] = {"tag": None, "fetched_at": fetched_at}
            return None, False, fetched_at
        raise HTTPException(502, f"Failed to fetch Docker Hub tag {repo}:{tag_name} (status={exc.code})")
    except Exception as exc:
        raise HTTPException(502, f"Failed to fetch Docker Hub tag {repo}:{tag_name}: {exc}")

    payload = raw if isinstance(raw, dict) else {}
    tag_payload = {
        "name": payload.get("name") or tag_name,
        "last_updated": payload.get("last_updated"),
        "digest": _dockerhub_tag_digest(payload),
    }
    fetched_at = time.time()
    _image_versions_cache[cache_key] = {"tag": tag_payload, "fetched_at": fetched_at}
    return tag_payload, False, fetched_at


def _validate_deucalion_sif_tag_available(image_tag: str) -> None:
    sif_repo = _normalize_image_repository(settings.JOB_SIF_REPOSITORY)
    tag_payload, _cached, _fetched_at = _fetch_dockerhub_tag(sif_repo, image_tag)
    if tag_payload is None:
        raise HTTPException(
            400,
            (
                f"Image tag '{image_tag}' is not Deucalion-ready: "
                f"SIF artifact '{sif_repo}:{image_tag}' was not found"
            ),
        )


def _validate_jetson_image_tag_available(image_tag: str) -> None:
    repo = _normalize_image_repository(settings.JOB_IMAGE_REPOSITORY)
    jetson_tag = _jetson_image_tag(image_tag)
    tag_payload, _cached, _fetched_at = _fetch_dockerhub_tag(repo, jetson_tag)
    if tag_payload is None:
        raise HTTPException(
            400,
            (
                f"Image tag '{image_tag}' is not Jetson-ready: "
                f"Docker image '{repo}:{jetson_tag}' was not found"
            ),
        )


def _validate_union_image_tag_available(image_tag: str) -> None:
    repo = _normalize_image_repository(settings.JOB_IMAGE_REPOSITORY)
    union_tag = _union_image_tag(image_tag)
    tag_payload, _cached, _fetched_at = _fetch_dockerhub_tag(repo, union_tag)
    if tag_payload is None:
        raise HTTPException(
            400,
            (
                f"Image tag '{image_tag}' is not Union-ready: "
                f"Docker image '{repo}:{union_tag}' was not found"
            ),
        )


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
    image_tag_names = {
        str(tag.get("name"))
        for tag in image_tags
        if isinstance(tag, dict) and isinstance(tag.get("name"), str)
    }

    tags_with_readiness: list[dict] = []
    variant_suffixes = {
        str(settings.JETSON_IMAGE_TAG_SUFFIX or ""),
        str(settings.UNION_IMAGE_TAG_SUFFIX or ""),
    } - {""}
    for tag in image_tags:
        if not isinstance(tag, dict):
            continue
        tag_name = tag.get("name")
        if not isinstance(tag_name, str):
            continue
        if (
            tag_name == "latest"
            or tag_name.startswith("buildcache")
            or re.fullmatch(r"sha-[0-9a-f]{7,40}", tag_name)
        ):
            continue
        if any(tag_name.endswith(suffix) for suffix in variant_suffixes):
            continue
        merged = dict(tag)
        merged["deucalion_ready"] = tag_name in sif_tag_names
        merged["jetson_ready"] = _jetson_image_tag(tag_name) in image_tag_names
        merged["union_ready"] = _union_image_tag(tag_name) in image_tag_names
        tags_with_readiness.append(merged)

    return {
        "repository": repo,
        "sif_repository": sif_repo,
        "jetson_tag_suffix": str(settings.JETSON_IMAGE_TAG_SUFFIX or ""),
        "union_tag_suffix": str(settings.UNION_IMAGE_TAG_SUFFIX or ""),
        "tags": tags_with_readiness,
        "count": len(tags_with_readiness),
        "cached": bool(image_cached and sif_cached),
        "fetched_at": max(image_fetched_at, sif_fetched_at),
    }


def _dockerhub_repository_diagnostic(repository: Optional[str], limit: int = 5) -> dict:
    repo_label = str(repository or "").strip().strip("/")
    try:
        repo = _normalize_image_repository(repository)
        tags, cached, fetched_at = _fetch_dockerhub_tags(repo, max(1, min(int(limit), 20)))
    except HTTPException as exc:
        return {
            "ok": False,
            "repository": repo_label,
            "status_code": exc.status_code,
            "error": str(exc.detail),
        }
    except Exception as exc:
        return {
            "ok": False,
            "repository": repo_label,
            "error": str(exc),
        }

    return {
        "ok": True,
        "repository": repo,
        "cached": cached,
        "fetched_at": fetched_at,
        "sample_count": len(tags),
        "sample_tags": [str(tag.get("name")) for tag in tags if isinstance(tag, dict) and tag.get("name")],
    }


def _status_stale_ttl(meta: dict, status: str) -> int:
    ttl = int(settings.JOB_STATUS_TTL)
    host = str(meta.get("target_host") or meta.get("preferred_host") or "")
    if status == JobStatus.DISPATCHED.value and host == "deucalion":
        ttl = max(ttl, int(settings.DEUCALION_DISPATCH_STATUS_TTL))
    return ttl


def _worker_stale_grace_seconds(host: str) -> int:
    remote_hosts = {str(item).strip() for item in getattr(settings, "REMOTE_WORKER_HOSTS", []) if str(item).strip()}
    if host in remote_hosts:
        return max(int(settings.WORKER_STALE_GRACE_SECONDS), int(settings.REMOTE_WORKER_STALE_GRACE_SECONDS))
    return int(settings.WORKER_STALE_GRACE_SECONDS)


def _worker_heartbeat_cutoff(host: str) -> int:
    return int(settings.HOST_HEARTBEAT_TTL) + _worker_stale_grace_seconds(host)


def _pending_persistent_worker_recovery_host(job_id: str, meta: dict) -> str | None:
    recoverable_hosts = {
        str(item).strip()
        for item in getattr(settings, "PERSISTENT_RECOVERY_WORKER_HOSTS", [])
        if str(item).strip()
    }
    state_path = Path(settings.JOBS_DIR) / job_id / ".worker" / "union.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(state, dict) or not state.get("run_name"):
        return None

    host = str(
        state.get("worker_id")
        or meta.get("target_host")
        or meta.get("preferred_host")
        or "union-inesctec"
    ).strip()
    if host not in recoverable_hosts:
        return None

    # A non-terminal execution must be reattached by its worker. A terminal
    # execution remains recoverable until its final status is acknowledged.
    if state.get("terminal") is not True or state.get("orchestrator_ack") is not True:
        return host
    return None


def _has_pending_persistent_worker_recovery(job_id: str, meta: dict) -> bool:
    return _pending_persistent_worker_recovery_host(job_id, meta) is not None


def _should_preserve_heartbeat_active_status(job_id: str, meta: dict, now_ts: float) -> bool:
    host = str(meta.get("target_host") or meta.get("preferred_host") or "")
    if not host:
        return False
    hb = host_heartbeats.get(host)
    if not hb:
        return False
    last_seen = hb.get("last_seen")
    if not isinstance(last_seen, (int, float)):
        return False
    cutoff = _worker_heartbeat_cutoff(host)
    if (now_ts - float(last_seen)) > cutoff:
        return False

    info = hb.get("info")
    if not isinstance(info, dict):
        return False

    active_jobs = _normalize_active_jobs_payload(info.get("active_jobs"))
    if any(row.get("job_id") == job_id for row in active_jobs):
        return True

    active_job_ids = info.get("active_job_ids")
    if isinstance(active_job_ids, list):
        return job_id in {str(item) for item in active_job_ids if item is not None}

    return info.get("active_job_id") == job_id


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
    cutoff = _worker_heartbeat_cutoff("deucalion")
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
    error_metadata_keys = set(ERROR_METADATA_KEYS)
    for key in RUNTIME_RESET_FIELDS | ATTEMPT_LIFECYCLE_RESET_FIELDS | error_metadata_keys:
        cleaned.pop(key, None)

    info = _read_job_info_payload(job_id)
    if info:
        changed = False
        for key in JOB_INFO_RUNTIME_RESET_FIELDS | error_metadata_keys:
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
    target_worker_profile: str | None = None,
) -> dict:
    payload: dict = {
        "job_id": job_id,
        "preferred_host": preferred_host,
        "require_host": require_host,
    }
    if submitted_by:
        payload["submitted_by"] = submitted_by
    if target_worker_profile:
        payload["target_worker_profile"] = target_worker_profile
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
        gpu_model = entry.get("gpu_model")
        if isinstance(gpu_model, str):
            normalized_gpu_model = " ".join(gpu_model.split())[:160]
            if normalized_gpu_model:
                row["gpu_model"] = normalized_gpu_model
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
        normalized_info["running_job_count"] = raw_info.get("running_job_count")
        normalized_info["provisioning_job_count"] = raw_info.get("provisioning_job_count")
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
            normalized_info["partition_limits"] = get_deucalion_partition_limits()
            normalized_info["active_job_count"] = sum(profile_counts.values())
        if normalized_info["active_job_count"] is None:
            normalized_info["active_job_count"] = len(merged_active_ids)
        if normalized_info["running_job_count"] is None:
            normalized_info["running_job_count"] = sum(
                row.get("status") == JobStatus.RUNNING.value for row in normalized_active_jobs
            )
        if normalized_info["provisioning_job_count"] is None:
            normalized_info["provisioning_job_count"] = sum(
                row.get("phase") == "union:provisioning" for row in normalized_active_jobs
            )
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
    _refresh_jobs()
    active_job_ids = [
        job_id
        for job_id, meta in jobs.items()
        if isinstance(meta, dict) and meta.get("status") in ACTIVE_JOB_STATUSES
    ]
    for job_id in active_job_ids:
        with _job_state_lock(job_id):
            _refresh_jobs()
            meta = jobs.get(job_id)
            if isinstance(meta, dict):
                _mark_stale_job_locked(job_id, meta, now)


def _mark_stale_job_locked(job_id: str, meta: dict, now: float) -> None:
    status = _read_status_file(job_id) or meta.get("status")
    host = meta.get("target_host")
    if status not in ACTIVE_JOB_STATUSES:
        return
    if _has_pending_persistent_worker_recovery(job_id, meta):
        _LOGGER.info(
            "Keeping %s job %s for persistent recovery by worker %s",
            status,
            job_id,
            host,
        )
        return

    status_ttl = _status_stale_ttl(meta, status)
    last_update = _status_last_update(job_id)
    if last_update and (now - last_update) > status_ttl:
        if _should_preserve_heartbeat_active_status(job_id, meta, now) or _should_preserve_deucalion_active_status(job_id, meta, now):
            _LOGGER.info(
                "Keeping %s job %s while worker heartbeat reports it active",
                status,
                job_id,
            )
        else:
            preferred = meta.get("preferred_host") or meta.get("target_host")
            require_host = bool(meta.get("require_host", bool(preferred)))
            if status in (JobStatus.DISPATCHED.value, JobStatus.SETUP.value):
                job_utils.enqueue_job(
                    _queue_payload(
                        job_id=job_id,
                        preferred_host=preferred,
                        require_host=require_host,
                        submitted_by=meta.get("submitted_by"),
                        target_worker_profile=meta.get("target_worker_profile"),
                    )
                )
                meta = _reset_runtime_metadata(job_id, meta)
                meta["preferred_host"] = preferred
                meta["require_host"] = require_host
                meta["target_host"] = preferred if require_host else None
                jobs[job_id] = meta
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
            return

    if not host:
        return
    hb = host_heartbeats.get(host)
    last_seen = hb["last_seen"] if hb else None
    if last_seen is None:
        return
    cutoff = _worker_heartbeat_cutoff(str(host))
    if (now - last_seen) <= cutoff:
        return
    preferred = meta.get("preferred_host") or meta.get("target_host")
    require_host = bool(meta.get("require_host", bool(preferred)))
    if status in (JobStatus.DISPATCHED.value, JobStatus.SETUP.value):
        job_utils.enqueue_job(
            _queue_payload(
                job_id=job_id,
                preferred_host=preferred,
                require_host=require_host,
                submitted_by=meta.get("submitted_by"),
                target_worker_profile=meta.get("target_worker_profile"),
            )
        )
        meta = _reset_runtime_metadata(job_id, meta)
        meta["preferred_host"] = preferred
        meta["require_host"] = require_host
        meta["target_host"] = preferred if require_host else None
        jobs[job_id] = meta
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
    target_worker_profile = _normalize_target_worker_profile(request.target_worker_profile)
    if preferred_host and target_worker_profile:
        raise HTTPException(400, "target_worker_profile is only allowed with automatic host selection")
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
    if target_worker_profile == "cpu" and _config_requires_gpu(runtime_config):
        raise HTTPException(400, "Config requires GPU; choose Any GPU or a GPU-capable host")
    if preferred_host and _config_requires_gpu(runtime_config) and not _worker_supports_gpu(preferred_host):
        raise HTTPException(400, f"Config requires GPU, but host '{preferred_host}' is not GPU-capable")

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
    _validate_deucalion_walltime_options(deucalion_options)
    if preferred_host == "deucalion":
        _validate_deucalion_sif_tag_available(image_tag)
    if _is_jetson_worker(preferred_host):
        _validate_jetson_image_tag_available(image_tag)
    if _is_union_worker(preferred_host):
        _validate_union_image_tag_available(image_tag)

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
        "target_worker_profile": target_worker_profile,
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
        target_worker_profile=target_worker_profile,
    )
    _write_status(
        job_id,
        JobStatus.LAUNCHING.value,
        {"preferred_host": preferred_host, "target_worker_profile": target_worker_profile},
    )

    # enqueue for agent (agent decides how to run the container)
    job_utils.enqueue_job(
        _queue_payload(
            job_id=job_id,
            preferred_host=preferred_host,
            require_host=bool(preferred_host),
            submitted_by=submitted_by,
            target_worker_profile=target_worker_profile,
        )
    )
    meta.update({"status": JobStatus.QUEUED.value})
    _persist_job(job_id, meta)
    _write_status(
        job_id,
        JobStatus.QUEUED.value,
        {"preferred_host": preferred_host, "target_worker_profile": target_worker_profile},
    )
    return {
        "job_id": job_id,
        "status": JobStatus.QUEUED.value,
        "host": preferred_host,
        "target_worker_profile": target_worker_profile,
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
    payload = file_utils.read_progress(job_id)
    if not isinstance(payload, dict):
        payload = {"progress": payload}
    return _enrich_progress_payload(job_id, payload)

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
        JobStatus.SETUP.value,
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
        JobStatus.SETUP.value,
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

    if status_now in (JobStatus.DISPATCHED.value, JobStatus.SETUP.value, JobStatus.RUNNING.value):
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
    queue_entries = job_utils.list_queue()
    queued_start_estimates = _queued_start_estimates(queue_entries)
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
        info.setdefault("target_worker_profile", merged.get("target_worker_profile"))
        info.setdefault("image_tag", merged.get("image_tag"))
        info.setdefault("deucalion_options", merged.get("deucalion_options"))
        info.setdefault("image", merged.get("image") or settings.DEFAULT_JOB_IMAGE)
        info = _without_email_notification_metadata(info)

        status_payload = get_status(job_id)
        status = status_payload["status"]
        merged["status"] = status
        merged = _normalized_job_meta(job_id, merged, persist=True)
        if "config_path" in merged and not _is_yaml_filename(str(merged["config_path"])):
            # Keep backward compatibility but normalize config extension when legacy jobs exist.
            merged["config_path"] = str(merged["config_path"])
        durations = _compute_job_durations(merged)
        entry = {
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
            "job_meta": _public_job_metadata(merged),
        }
        queued_start_estimate = queued_start_estimates.get(job_id)
        if queued_start_estimate:
            entry["queued_start_estimate"] = queued_start_estimate
            if queued_start_estimate.get("available"):
                entry["estimated_start_at"] = queued_start_estimate.get("estimated_start_at")
                entry["estimated_start_seconds"] = queued_start_estimate.get("estimated_start_seconds")
        result.append(entry)
    return result


def list_queue():
    _mark_stale_jobs()
    entries = job_utils.list_queue()
    queued_start_estimates = _queued_start_estimates(entries)
    tracked = jobs if jobs else job_utils.load_jobs()
    for entry in entries:
        job_id = entry.get("job_id")
        if not job_id:
            continue
        meta = tracked.get(job_id) or {}
        if not entry.get("submitted_by") and meta.get("submitted_by"):
            submitted_by = meta.get("submitted_by")
            entry["submitted_by"] = submitted_by
        if not entry.get("target_worker_profile") and meta.get("target_worker_profile"):
            entry["target_worker_profile"] = meta.get("target_worker_profile")
        queued_start_estimate = queued_start_estimates.get(job_id)
        if queued_start_estimate:
            entry["queued_start_estimate"] = queued_start_estimate
            if queued_start_estimate.get("available"):
                entry["estimated_start_at"] = queued_start_estimate.get("estimated_start_at")
                entry["estimated_start_seconds"] = queued_start_estimate.get("estimated_start_seconds")
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
    meta = _normalized_job_meta(job_id, jobs.get(job_id) or job_utils.load_jobs().get(job_id, {}) or {}, persist=True)
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
    if "last_email_notification" not in info and meta.get("last_email_notification"):
        info["last_email_notification"] = meta.get("last_email_notification")
    if "email_notifications" not in info and meta.get("email_notifications"):
        info["email_notifications"] = meta.get("email_notifications")

    # Expose lifecycle timing in job details overview.
    status_payload = _read_status_payload(job_id) or {}
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
        if key in meta:
            info[key] = meta.get(key)
        elif key in info:
            info.pop(key, None)
    if durations:
        info["queue_wait_seconds"] = durations.get("queue_wait_seconds")
        info["run_duration_seconds"] = durations.get("run_duration_seconds")
        info["total_duration_seconds"] = durations.get("total_duration_seconds")

    if not all(key in info for key in ERROR_METADATA_KEYS):
        details = {}
        if isinstance(info.get("details"), dict):
            details = info["details"]
        elif isinstance(status_payload.get("details"), dict):
            details = status_payload["details"]
        elif isinstance(meta.get("details"), dict):
            details = meta["details"]
        error_extra = {
            "error": info.get("error") or status_payload.get("error") or meta.get("error"),
            "details": details,
        }
        _enrich_error_metadata(
            job_id,
            info.get("status") or status_payload.get("status") or meta.get("status"),
            error_extra,
        )
        for key in ERROR_METADATA_KEYS:
            if key in error_extra:
                info.setdefault(key, error_extra[key])
        if isinstance(info.get("details"), dict) and isinstance(error_extra.get("details"), dict):
            for key in ERROR_METADATA_KEYS:
                if key in error_extra["details"]:
                    info["details"].setdefault(key, error_extra["details"][key])

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


def get_deucalion_diagnostics() -> dict:
    _refresh_jobs()
    _mark_stale_jobs()
    now = time.time()
    hosts = _host_status_snapshot()
    host = hosts.get("deucalion")
    hb = host_heartbeats.get("deucalion")
    heartbeat_age_seconds = (now - hb["last_seen"]) if hb else None

    profile_limits = _deucalion_profile_limits()
    active_ids_by_profile = _deucalion_active_job_ids_by_profile()
    active_jobs: list[dict] = []
    for profile, job_ids in active_ids_by_profile.items():
        for job_id in job_ids:
            meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
            status_payload = _read_status_payload(job_id) or {}
            details = status_payload.get("details") if isinstance(status_payload.get("details"), dict) else {}
            active_jobs.append(
                {
                    "job_id": job_id,
                    "profile": profile,
                    "status": status_payload.get("status") or meta.get("status"),
                    "job_name": meta.get("job_name"),
                    "image_tag": meta.get("image_tag"),
                    "slurm_job_id": details.get("slurm_job_id"),
                    "slurm_state": details.get("slurm_state"),
                    "slurm_reason": details.get("slurm_reason"),
                    "slurm_partition": details.get("slurm_partition"),
                    "queue_position": details.get("slurm_queue_position"),
                    "jobs_ahead": details.get("slurm_jobs_ahead"),
                    "executor_stage": details.get("executor_stage"),
                    "last_status_at": meta.get("last_status_at") or status_payload.get("status_updated_at"),
                }
            )

    tracked = jobs if jobs else job_utils.load_jobs()
    queue_entries: list[dict] = []
    for entry in job_utils.list_queue():
        job_id = entry.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            continue
        meta = tracked.get(job_id) or {}
        preferred = entry.get("preferred_host") or meta.get("preferred_host") or meta.get("target_host")
        if preferred != "deucalion":
            continue
        queue_entries.append(
            {
                "job_id": job_id,
                "job_name": meta.get("job_name"),
                "profile": _deucalion_job_profile(job_id, meta),
                "status": _read_status_file(job_id) or meta.get("status"),
                "image_tag": meta.get("image_tag"),
                "preferred_host": preferred,
                "require_host": entry.get("require_host", meta.get("require_host")),
                "enqueued_at": entry.get("enqueued_at"),
                "submitted_by": entry.get("submitted_by") or meta.get("submitted_by"),
            }
        )

    checks = {
        "configured": {
            "ok": "deucalion" in settings.AVAILABLE_HOSTS,
            "available_hosts": settings.AVAILABLE_HOSTS,
        },
        "heartbeat": {
            "ok": bool(host and host.get("online")),
            "last_seen": hb.get("last_seen") if hb else None,
            "age_seconds": heartbeat_age_seconds,
            "ttl_seconds": settings.HOST_HEARTBEAT_TTL,
        },
        "image_repository": _dockerhub_repository_diagnostic(settings.JOB_IMAGE_REPOSITORY),
        "sif_repository": _dockerhub_repository_diagnostic(settings.JOB_SIF_REPOSITORY),
    }

    raw_info = (hb.get("info") if hb else {}) if isinstance(hb, dict) else {}
    if not isinstance(raw_info, dict):
        raw_info = {}
    checks["budget"] = {
        "ok": bool(raw_info.get("budget")),
        "refreshed_at": raw_info.get("budget_refreshed_at"),
    }

    issues: list[str] = []
    if not checks["configured"]["ok"]:
        issues.append("deucalion_not_configured")
    if not checks["heartbeat"]["ok"]:
        issues.append("deucalion_worker_offline")
    if not checks["sif_repository"]["ok"]:
        issues.append("sif_repository_unreachable")
    if not checks["image_repository"]["ok"]:
        issues.append("image_repository_unreachable")

    return {
        "generated_at": now,
        "ok": not issues,
        "issues": issues,
        "checks": checks,
        "host": host,
        "worker_info": raw_info,
        "limits": {
            "max_active_jobs_by_profile": profile_limits,
            "partition_limits": get_deucalion_partition_limits(),
        },
        "active": {
            "count_by_profile": {profile: len(active_ids_by_profile.get(profile, [])) for profile in profile_limits},
            "job_ids_by_profile": active_ids_by_profile,
            "jobs": active_jobs,
        },
        "queue": {
            "count": len(queue_entries),
            "entries": queue_entries,
        },
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
    with _job_state_lock(job_id):
        return _ops_requeue_job_locked(job_id, force, preferred_host, require_host)


def _ops_requeue_job_locked(
    job_id: str,
    force: bool,
    preferred_host: Optional[str],
    require_host: Optional[bool],
):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if preferred_host:
        if not job_utils.is_valid_host(preferred_host):
            raise HTTPException(400, f"Unknown host '{preferred_host}'. Allowed: {settings.AVAILABLE_HOSTS}")
    if require_host is False and preferred_host is None:
        preferred = None
    else:
        preferred = preferred_host or meta.get("preferred_host") or meta.get("target_host")
    if require_host is None:
        require_host = bool(meta.get("require_host", bool(preferred)))
    if require_host and _is_jetson_worker(preferred):
        _validate_jetson_image_tag_available(_normalize_image_tag(meta.get("image_tag")))
    if require_host and _is_union_worker(preferred):
        _validate_union_image_tag_available(_normalize_image_tag(meta.get("image_tag")))

    if not force:
        if status_now == JobStatus.FINISHED.value:
            raise HTTPException(409, "Finished jobs require force to requeue")
        if status_now in (JobStatus.SETUP.value, JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
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
            target_worker_profile=meta.get("target_worker_profile"),
        )
    )

    meta["preferred_host"] = preferred
    meta["require_host"] = require_host
    meta["target_host"] = preferred if require_host else None
    jobs[job_id] = meta

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
def agent_next_job(worker_id: str, capabilities: list[str] | None = None):
    with _dispatch_lock(worker_id):
        return _agent_next_job_locked(worker_id, capabilities=capabilities)


def _agent_next_job_locked(worker_id: str, capabilities: list[str] | None = None):
    _refresh_jobs()
    _mark_stale_jobs()
    worker_capabilities = {
        str(capability).strip()
        for capability in (capabilities or [])
        if str(capability).strip()
    }
    supports_attempt_fencing = ATTEMPT_FENCING_CAPABILITY in worker_capabilities
    deucalion_active_counts = _deucalion_active_counts_by_profile() if worker_id == "deucalion" else None
    job_queue_entry = job_utils.agent_pop_next_job(
        worker_id,
        can_accept=lambda payload: _can_dispatch_to_worker(
            worker_id,
            payload,
            deucalion_active_counts=deucalion_active_counts,
            supports_attempt_fencing=supports_attempt_fencing,
        ),
    )
    if not job_queue_entry:
        _LOGGER.debug("Worker %s polled queue but no job was available", worker_id)
        return None

    job_id = job_queue_entry["job_id"]
    with _job_state_lock(job_id):
        return _dispatch_claimed_job(
            worker_id,
            job_queue_entry,
            supports_attempt_fencing=supports_attempt_fencing,
        )


def _dispatch_claimed_job(
    worker_id: str,
    job_queue_entry: dict,
    *,
    supports_attempt_fencing: bool,
):
    job_id = job_queue_entry["job_id"]
    _refresh_jobs()

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
    requested_image_tag = _normalize_image_tag(meta.get("image_tag"))
    dispatch_image, dispatch_image_tag = _resolve_job_image_for_worker(worker_id, requested_image_tag)

    next_attempt_number = int(meta.get("attempt_number", 0) or 0) + 1
    attempt_token = secrets.token_urlsafe(32) if supports_attempt_fencing else None
    response = {
        "job_id": job_id,
        "job_name": job_name,
        "attempt_number": next_attempt_number,
        "config_path": runtime_config_path,
        "source_config_path": config_path,
        "preferred_host": job_queue_entry.get("preferred_host"),
        "target_worker_profile": meta.get("target_worker_profile") or job_queue_entry.get("target_worker_profile"),
        "image": dispatch_image,
        "image_tag": dispatch_image_tag,
        "requested_image_tag": requested_image_tag,
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
    if attempt_token is not None:
        response["attempt_token"] = attempt_token
        response["attempt_protocol"] = ATTEMPT_FENCING_CAPABILITY
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

    meta["target_host"] = worker_id
    meta["dispatched_image"] = response["image"]
    meta["dispatched_image_tag"] = response["image_tag"]
    if attempt_token is not None:
        meta["attempt_fencing_enabled"] = True
        meta["attempt_protocol"] = ATTEMPT_FENCING_CAPABILITY
        meta["attempt_token_hash"] = _attempt_token_digest(attempt_token)
        meta.pop("attempt_fence_invalidated_at", None)
    jobs[job_id] = meta

    _write_status(
        job_id,
        JobStatus.DISPATCHED.value,
        {
            "worker_id": worker_id,
            "target_worker_profile": response.get("target_worker_profile"),
            "dispatched_image": response["image"],
            "dispatched_image_tag": response["image_tag"],
        },
    )

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
    info["image"] = response["image"]
    if response.get("image_tag"):
        info["image_tag"] = response["image_tag"]
    if requested_image_tag and requested_image_tag != response.get("image_tag"):
        info["requested_image_tag"] = requested_image_tag
        info["requested_image"] = _resolve_job_image_from_tag(requested_image_tag)
    if response.get("target_worker_profile") and "target_worker_profile" not in info:
        info["target_worker_profile"] = response["target_worker_profile"]
    if worker_id == "deucalion" and response.get("deucalion_options") and "deucalion_options" not in info:
        info["deucalion_options"] = response["deucalion_options"]
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)

    return response

def agent_update_status(job_id: str, status: str, extra: dict | None = None):
    _refresh_jobs()
    _mark_stale_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    with _job_state_lock(job_id):
        return _agent_update_status_locked(job_id, status, extra)


def _agent_update_status_locked(job_id: str, status: str, extra: dict | None = None):
    _refresh_jobs()
    extra = dict(extra or {})
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    try:
        JobStatus(status)
    except ValueError:
        raise HTTPException(400, f"Unknown status '{status}'")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    _validate_agent_attempt(job_id, meta, extra)
    _LOGGER.info(
        "Agent reported status for job %s: %s (extra keys=%s)",
        job_id,
        status,
        sorted(extra.keys()),
    )
    _enrich_error_metadata(job_id, status, extra)
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
    if {"container_id", "container_name", "exit_code", "error", "details", *ERROR_METADATA_KEYS} & extra.keys():
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
        for key in ERROR_METADATA_KEYS:
            if key in extra:
                info[key] = extra[key]
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
            for key in ERROR_METADATA_KEYS:
                if key in extra:
                    updated[key] = extra[key]
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
            for key in ERROR_METADATA_KEYS:
                if key in extra:
                    meta[key] = extra[key]
            if "details" in extra and isinstance(extra["details"], dict):
                meta["details"] = extra["details"]
            _persist_job(job_id, meta)
            _LOGGER.debug("Updated in-memory metadata for job %s", job_id)

    return {"ok": True}
