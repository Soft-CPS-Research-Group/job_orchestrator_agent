# app/utils/job_utils.py
import os, json, shutil, time, tempfile
import fcntl
from contextlib import contextmanager
from typing import Callable
from app.config import settings

@contextmanager
def _job_track_lock():
    lock_path = f"{settings.JOB_TRACK_FILE}.lock"
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _read_job_track_unlocked() -> dict:
    if not os.path.exists(settings.JOB_TRACK_FILE):
        return {}
    try:
        with open(settings.JOB_TRACK_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _write_job_track_unlocked(jobs: dict) -> None:
    os.makedirs(os.path.dirname(settings.JOB_TRACK_FILE), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(settings.JOB_TRACK_FILE),
        prefix="job_track.",
        suffix=".json",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(jobs, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, settings.JOB_TRACK_FILE)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def ensure_directories():
    os.makedirs(settings.CONFIGS_DIR, exist_ok=True)
    os.makedirs(settings.JOBS_DIR, exist_ok=True)
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)
    os.makedirs(settings.QUEUE_DIR, exist_ok=True)
    if not os.path.exists(settings.JOB_TRACK_FILE):
        with _job_track_lock():
            _write_job_track_unlocked({})

def load_jobs():
    if not os.path.exists(settings.JOB_TRACK_FILE):
        return {}
    with _job_track_lock():
        return _read_job_track_unlocked()

def save_job(job_id, metadata):
    with _job_track_lock():
        jobs = _read_job_track_unlocked()
        jobs[job_id] = metadata
        _write_job_track_unlocked(jobs)

def save_job_info(
    job_id,
    job_name,
    config_path,
    host,
    container_id,
    container_name,
    exp,
    run,
    ray_task_id=None,
    *,
    submitted_by: str | None = None,
    image_tag: str | None = None,
    image: str | None = None,
    deucalion_options: dict | None = None,
    target_worker_profile: str | None = None,
):
    job_dir = os.path.join(settings.JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    info = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": host,
        "container_id": container_id,
        "container_name": container_name,
        "experiment_name": exp,
        "run_name": run,
    }
    if ray_task_id is not None:
        info["ray_task_id"] = ray_task_id
    if submitted_by:
        info["submitted_by"] = submitted_by
    if image_tag:
        info["image_tag"] = image_tag
    if image:
        info["image"] = image
    if deucalion_options:
        info["deucalion_options"] = deucalion_options
    if target_worker_profile:
        info["target_worker_profile"] = target_worker_profile
    with open(os.path.join(job_dir, "job_info.json"), "w") as f:
        json.dump(info, f, indent=2)

def write_status_file(job_id: str, status: str, extra: dict | None = None):
    job_dir = os.path.join(settings.JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    payload = {"job_id": job_id, "status": status}
    if extra:
        payload.update(extra)
    status_path = os.path.join(job_dir, "status.json")
    fd, tmp_path = tempfile.mkstemp(dir=job_dir, prefix="status.", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, status_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass

def delete_job_by_id(job_id: str) -> bool:
    job_path = os.path.join(settings.JOBS_DIR, job_id)
    removed_fs = False
    if os.path.exists(job_path):
        try:
            shutil.rmtree(job_path)
            removed_fs = True
        except OSError:
            # Best effort permission normalization for root-owned or restrictive files.
            try:
                for root, dirs, files in os.walk(job_path):
                    for directory in dirs:
                        try:
                            os.chmod(os.path.join(root, directory), 0o777)
                        except OSError:
                            continue
                    for file_name in files:
                        try:
                            os.chmod(os.path.join(root, file_name), 0o666)
                        except OSError:
                            continue
                os.chmod(job_path, 0o777)
                shutil.rmtree(job_path)
                removed_fs = True
            except OSError:
                return False

    removed_registry = False
    with _job_track_lock():
        jobs = _read_job_track_unlocked()
        if job_id in jobs:
            del jobs[job_id]
            removed_registry = True
        _write_job_track_unlocked(jobs)
    return removed_fs or removed_registry or (not os.path.exists(job_path))


def prune_jobs(keep: set[str]) -> list[str]:
    """Remove all job records from the registry except those in keep."""
    removed: list[str] = []
    with _job_track_lock():
        jobs = _read_job_track_unlocked()
        for job_id in list(jobs.keys()):
            if job_id in keep:
                continue
            removed.append(job_id)
            jobs.pop(job_id, None)
        _write_job_track_unlocked(jobs)
    return removed

def get_job_log_path(job_id: str):
    return os.path.join(settings.JOBS_DIR, job_id, "logs", f"{job_id}.log")

def is_valid_host(name: str) -> bool:
    return name in settings.AVAILABLE_HOSTS

# -------- queue helpers (filesystem) --------
def _queue_path(job_id: str) -> str:
    return os.path.join(settings.QUEUE_DIR, f"{job_id}.json")


def enqueue_job(payload: dict):
    os.makedirs(settings.QUEUE_DIR, exist_ok=True)
    path = _queue_path(payload["job_id"])
    entry = {
        "job_id": payload["job_id"],
        "preferred_host": payload.get("preferred_host"),
        "require_host": payload.get("require_host", bool(payload.get("preferred_host"))),
        "enqueued_at": payload.get("enqueued_at") or time.time(),
    }
    if payload.get("target_worker_profile"):
        entry["target_worker_profile"] = payload["target_worker_profile"]
    if payload.get("submitted_by"):
        entry["submitted_by"] = payload["submitted_by"]
    with open(path, "w") as f:
        json.dump(entry, f, indent=2)

def _restore_stale_claims(ttl: int):
    """Return stale claim files back to the queue if they have exceeded the TTL."""
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return
    now = time.time()
    for fname in os.listdir(wdir):
        if ".claim." not in fname:
            continue
        path = os.path.join(wdir, fname)
        try:
            if (now - os.path.getmtime(path)) < ttl:
                continue
            original = path.rsplit(".claim.", 1)[0]
            os.replace(path, original)
        except Exception:
            # Best-effort; leave the claim in place if anything goes wrong
            continue


def agent_pop_next_job(worker_id: str, can_accept: Callable[[dict], bool] | None = None) -> dict | None:
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return None

    # Return stale claims to the queue so jobs don't get stuck if a worker dies mid-claim
    _restore_stale_claims(settings.QUEUE_CLAIM_TTL)

    files = sorted(
        [os.path.join(wdir, f) for f in os.listdir(wdir) if f.endswith(".json")],
        key=lambda p: os.path.getmtime(p)
    )
    for path in files:
        claim_path = f"{path}.claim.{worker_id}"
        try:
            os.replace(path, claim_path)
        except FileNotFoundError:
            continue
        except OSError:
            continue

        try:
            with open(claim_path) as f:
                payload = json.load(f)

            preferred = payload.get("preferred_host")
            require_host = payload.get("require_host", bool(preferred))

            # Deucalion requires an explicit target because its Slurm options,
            # account and budget are selected as part of that host-specific flow.
            if worker_id == "deucalion":
                if preferred != worker_id or not require_host:
                    os.replace(claim_path, path)
                    continue

            if preferred and preferred != worker_id and require_host:
                os.replace(claim_path, path)
                continue

            # Allow any worker to pick up the job when host is not required
            if preferred and preferred != worker_id and not require_host:
                payload["preferred_host"] = preferred

            if can_accept and not can_accept(payload):
                os.replace(claim_path, path)
                continue

            os.remove(claim_path)
            return payload
        except Exception:
            try:
                if os.path.exists(claim_path):
                    os.replace(claim_path, path)
            except Exception:
                pass
            continue
    return None


def list_queue() -> list[dict]:
    """Return pending queue entries ordered by enqueue time."""
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return []
    files = sorted(
        [os.path.join(wdir, f) for f in os.listdir(wdir) if f.endswith(".json")],
        key=lambda p: os.path.getmtime(p),
    )
    entries: list[dict] = []
    for path in files:
        try:
            with open(path) as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                payload.setdefault("job_id", os.path.basename(path).rsplit(".", 1)[0])
                payload.setdefault("enqueued_at", os.path.getmtime(path))
                entries.append(payload)
        except Exception:
            continue
    return entries


def remove_from_queue(job_id: str) -> bool:
    removed = False
    path = _queue_path(job_id)
    if os.path.exists(path):
        os.remove(path)
        removed = True
    wdir = settings.QUEUE_DIR
    if os.path.isdir(wdir):
        prefix = f"{job_id}.json.claim."
        for fname in os.listdir(wdir):
            if not fname.startswith(prefix):
                continue
            try:
                os.remove(os.path.join(wdir, fname))
                removed = True
            except OSError:
                continue
    return removed


enqueue_job_for_agent = enqueue_job
