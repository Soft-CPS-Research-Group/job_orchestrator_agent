# app/models/agent.py
from pydantic import BaseModel, Field

class NextJobRequest(BaseModel):
    worker_id: str
    capabilities: list[str] = Field(default_factory=list)

class StatusRequest(BaseModel):
    job_id: str
    status: str
    worker_id: str | None = None
    worker_version: str | None = None
    attempt_number: int | None = None
    attempt_token: str | None = None
    container_id: str | None = None
    container_name: str | None = None
    exit_code: int | None = None
    error: str | None = None
    error_code: str | None = None
    error_category: str | None = None
    error_hint: str | None = None
    details: dict | None = None


class HeartbeatRequest(BaseModel):
    worker_id: str
    info: dict | None = None


class WorkerCommandRequest(BaseModel):
    worker_id: str
