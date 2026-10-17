from typing import Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class DeucalionRunOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    account: Optional[str] = None
    partition: Optional[str] = None
    time_limit: Optional[str] = Field(default=None, alias="time")
    cpus_per_task: Optional[int] = None
    mem_gb: Optional[int] = None
    gpus: Optional[int] = None
    modules: Optional[list[str]] = None
    command_mode: Optional[str] = None
    datasets: Optional[list[str]] = None
    required_paths: Optional[list[str]] = None


class JobLaunchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: Optional[str] = Field(default=None, max_length=100)
    training_context: Optional[Dict] = None
    config: Optional[Dict] = None
    config_path: Optional[str] = None
    target_host: Optional[str] = None
    target_worker_profile: Optional[Literal["cpu", "gpu"]] = None
    save_as: Optional[str] = None
    job_name: Optional[str] = None
    submitted_by: Optional[str] = None
    image_tag: Optional[str] = None
    deucalion_options: Optional[DeucalionRunOptions] = None

class SimulationRequest(BaseModel):
    config_path: str
    job_name: Optional[str] = None


class JobFolderCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    owner: str = Field(min_length=1, max_length=160)
    name: str = Field(min_length=1, max_length=80)


class JobFolderUpdateRequest(JobFolderCreateRequest):
    pass


class JobOrganizationOwnerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    owner: str = Field(min_length=1, max_length=160)


class JobFolderAssignmentRequest(JobOrganizationOwnerRequest):
    folder_id: Optional[str] = None
