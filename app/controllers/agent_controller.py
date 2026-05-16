# app/controllers/agent_controller.py
import logging
from fastapi import HTTPException
from app.services import job_service
from app.models.agent import NextJobRequest, StatusRequest, HeartbeatRequest

_LOGGER = logging.getLogger(__name__)

def next_job(req: NextJobRequest):
    job = job_service.agent_next_job(req.worker_id)
    if not job:
        # the endpoint will return 204; controller returns None to signal that
        _LOGGER.debug("Worker %s requested next job but none available", req.worker_id)
        return None
    _LOGGER.info("Worker %s received job %s", req.worker_id, job.get("job_id"))
    return job

def job_status(req: StatusRequest):
    try:
        payload = req.model_dump(exclude_none=True)
        job_id = payload.pop("job_id")
        status = payload.pop("status")
        _LOGGER.info("Processing status update from %s: job %s -> %s", req.worker_id, job_id, status)
        return job_service.agent_update_status(job_id, status, payload)
    except HTTPException:
        raise
    except Exception as e:
        _LOGGER.exception("Failed to process status update for job %s", getattr(req, "job_id", "unknown"))
        raise HTTPException(status_code=500, detail=str(e))


def heartbeat(req: HeartbeatRequest):
    _LOGGER.debug("Heartbeat from %s", req.worker_id)
    job_service.record_host_heartbeat(req.worker_id, req.info or {})
    return {"ok": True}
