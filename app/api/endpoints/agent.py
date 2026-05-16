# app/api/endpoints/agent.py
from fastapi import APIRouter, Response
from app.controllers import agent_controller
from app.models.agent import NextJobRequest, StatusRequest, HeartbeatRequest

router = APIRouter()

@router.post("/api/agent/next-job")
def next_job(req: NextJobRequest):
    job = agent_controller.next_job(req)
    if job is None:
        return Response(status_code=204)
    return job

@router.post("/api/agent/job-status")
def job_status(req: StatusRequest):
    return agent_controller.job_status(req)


@router.post("/api/agent/heartbeat")
def heartbeat(req: HeartbeatRequest):
    return agent_controller.heartbeat(req)
