from fastapi import HTTPException
from app.services import job_service


def requeue_job(job_id: str, force: bool = False, preferred_host: str | None = None, require_host: bool | None = None):
    try:
        return job_service.ops_requeue_job(job_id, force, preferred_host, require_host)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def fail_job(job_id: str, reason: str = "ops_failed", force: bool = False):
    try:
        return job_service.ops_fail_job(job_id, reason, force)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def cancel_job(job_id: str, reason: str = "ops_canceled", force: bool = False):
    try:
        return job_service.ops_cancel_job(job_id, reason, force)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def stop_job(job_id: str, reason: str = "ops_stop"):
    try:
        return job_service.ops_stop_job(job_id, reason)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def cleanup_queue(force: bool = False):
    return job_service.ops_cleanup_queue(force)


def cleanup_jobs(keep: list[str] | None = None):
    return job_service.ops_cleanup_jobs(keep)
