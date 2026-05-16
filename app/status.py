from __future__ import annotations
from enum import Enum

class JobStatus(str, Enum):
    # creation/dispatch
    LAUNCHING = "launching"      # created on server; preparing metadata
    QUEUED = "queued"            # written to /queue/<worker_id> but not yet taken
    DISPATCHED = "dispatched"    # agent popped from queue but not started yet

    # active
    RUNNING = "running"          # container started

    # terminal
    FINISHED = "finished"        # exit code 0
    FAILED = "failed"            # non-zero exit code
    STOP_REQUESTED = "stop_requested"  # stop requested by API; waiting on worker
    STOPPED = "stopped"          # intentionally stopped by worker

    # utility
    NOT_FOUND = "not_found"      # container/file missing
    UNKNOWN = "unknown"          # fallback
    CANCELED = "canceled"        # (optional) queued/dispatched canceled before start


# Allowed state transitions for jobs
ALLOWED_TRANSITIONS = {
    JobStatus.LAUNCHING: {JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.CANCELED},
    JobStatus.QUEUED: {JobStatus.DISPATCHED, JobStatus.CANCELED},
    JobStatus.DISPATCHED: {JobStatus.RUNNING, JobStatus.FAILED, JobStatus.CANCELED, JobStatus.STOP_REQUESTED, JobStatus.QUEUED},
    JobStatus.RUNNING: {JobStatus.FINISHED, JobStatus.FAILED, JobStatus.STOP_REQUESTED, JobStatus.STOPPED, JobStatus.CANCELED},
    JobStatus.STOP_REQUESTED: {JobStatus.STOPPED, JobStatus.FAILED, JobStatus.CANCELED},
    JobStatus.FINISHED: set(),
    JobStatus.FAILED: set(),
    JobStatus.STOPPED: set(),
    JobStatus.CANCELED: set(),
}


def can_transition(current: str | "JobStatus", new: str | "JobStatus") -> bool:
    """Check if a status change is permitted."""
    try:
        cur = JobStatus(current)
        nxt = JobStatus(new)
    except ValueError:
        return False

    if nxt in (JobStatus.NOT_FOUND, JobStatus.UNKNOWN):
        return True
    return nxt in ALLOWED_TRANSITIONS.get(cur, set())
