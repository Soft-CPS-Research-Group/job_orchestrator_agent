from fastapi import APIRouter, Body
from app.controllers import ops_controller

router = APIRouter()


@router.post("/ops/jobs/{job_id}/requeue")
def requeue_job(
    job_id: str,
    force: bool = Body(False),
    preferred_host: str | None = Body(None),
    require_host: bool | None = Body(None),
):
    return ops_controller.requeue_job(job_id, force, preferred_host, require_host)


@router.post("/ops/jobs/{job_id}/fail")
def fail_job(job_id: str, reason: str = Body("ops_failed"), force: bool = Body(False)):
    return ops_controller.fail_job(job_id, reason, force)


@router.post("/ops/jobs/{job_id}/cancel")
def cancel_job(job_id: str, reason: str = Body("ops_canceled"), force: bool = Body(False)):
    return ops_controller.cancel_job(job_id, reason, force)


@router.post("/ops/jobs/{job_id}/stop")
def stop_job(job_id: str, reason: str = Body("ops_stop", embed=True)):
    return ops_controller.stop_job(job_id, reason)


@router.post("/ops/queue/cleanup")
def cleanup_queue(force: bool = Body(False, embed=True)):
    return ops_controller.cleanup_queue(force)


@router.post("/ops/jobs/cleanup")
def cleanup_jobs(keep: list[str] | None = Body(None, embed=True)):
    return ops_controller.cleanup_jobs(keep)
