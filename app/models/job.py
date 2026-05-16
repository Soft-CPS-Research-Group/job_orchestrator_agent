from typing import Dict, Optional

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

    config: Optional[Dict] = None
    config_path: Optional[str] = None
    target_host: Optional[str] = None
    save_as: Optional[str] = None
    job_name: Optional[str] = None
    submitted_by: Optional[str] = None
    image_tag: Optional[str] = None
    deucalion_options: Optional[DeucalionRunOptions] = None

class SimulationRequest(BaseModel):
    config_path: str
    job_name: Optional[str] = None
