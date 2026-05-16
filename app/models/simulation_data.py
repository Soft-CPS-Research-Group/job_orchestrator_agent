from pydantic import BaseModel


class SimulationDataIndexRequest(BaseModel):
    job_id: str
    session: str | None = "latest"


class SimulationDataFileRequest(BaseModel):
    job_id: str
    relative_path: str
    session: str | None = "latest"
