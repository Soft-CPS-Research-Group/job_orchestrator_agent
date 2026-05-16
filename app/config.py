import json
import os
from typing import Any, ClassVar

from pydantic import field_validator
from pydantic_settings import BaseSettings


def _parse_cors_allowed_origins(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []

        if raw.startswith("["):
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(decoded, list):
                    return [str(item).strip() for item in decoded if str(item).strip()]

        normalized = raw.lstrip("[").rstrip("]")
        return [
            item.strip().strip('"').strip("'").strip()
            for item in normalized.split(",")
            if item.strip().strip('"').strip("'").strip()
        ]

    return []


class Settings(BaseSettings):
    VM_SHARED_DATA: str = "/opt/opeva_shared_data"

    CONFIGS_DIR: str = os.path.join(VM_SHARED_DATA, "configs")
    JOB_TRACK_FILE: str = os.path.join(VM_SHARED_DATA, "job_track.json")
    JOBS_DIR: str = os.path.join(VM_SHARED_DATA, "jobs")
    DATASETS_DIR: str = os.path.join(VM_SHARED_DATA, "datasets")
    QUEUE_DIR: str = os.path.join(VM_SHARED_DATA, "queue")
    QUEUE_CLAIM_TTL: int = 300

    AVAILABLE_HOSTS: list[str] = ["tiago-laptop", "local", "deucalion"]
    HOST_HEARTBEAT_TTL: int = 60

    DEFAULT_JOB_IMAGE: str = "calof/opeva_simulator:latest"
    JOB_IMAGE_REPOSITORY: str = "calof/opeva_simulator"
    JOB_SIF_REPOSITORY: str = "calof/opeva_simulator_sif"
    JOB_IMAGE_TAGS_LIMIT: int = 50
    JOB_IMAGE_CATALOG_TTL_SECONDS: int = 120
    JOB_IMAGE_CATALOG_TIMEOUT_SECONDS: int = 10
    CONTAINER_NAME_PREFIX: str = "opeva_job"
    WORKER_STALE_GRACE_SECONDS: int = 120
    JOB_STATUS_TTL: int = 300
    DEUCALION_DISPATCH_STATUS_TTL: int = 21600
    DEUCALION_MAX_ACTIVE_CPU_JOBS: int = 1
    DEUCALION_MAX_ACTIVE_GPU_JOBS: int = 1
    MLFLOW_TRACKING_URI: str | None = None
    DEUCALION_MLFLOW_TRACKING_URI: str = "file:/data/mlflow/mlruns"
    MLFLOW_UI_BASE_URL: str | None = None

    MONGO_USER: str = "runtimeUI"
    MONGO_PASSWORD: str = "runtimeUIDB"
    MONGO_HOST: str = "193.136.62.78"
    MONGO_PORT: int = 27017
    MONGO_AUTH_SOURCE: str = "admin"
    ACCEPTABLE_GAP_IN_MINUTES: int = 60

    BUILDING_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "month": "first",
        "hour": "first",
        "minutes": "first",
        "day_type": "first",
        "daylight_savings_status": "first",
        "indoor_dry_bulb_temperature": "sum",
        "average_unmet_cooling_setpoint_difference": "sum",
        "indoor_relative_humidity": "sum",
        "non_shiftable_load": "sum",
        "dhw_demand": "sum",
        "cooling_demand": "sum",
        "heating_demand": "sum",
        "solar_generation": "sum",
    }

    TIMESTAMP_DATASET_CSV_HEADER: ClassVar[list[str]] = [
        "month",
        "hour",
        "minutes",
        "day_type",
        "daylight_savings_status",
    ]

    EV_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "timestamp": "first",
        "electric_vehicle_charger_state": "first",
        "power": "first",
        "electric_vehicle_id": "first",
        "electric_vehicle_battery_capacity_khw": "first",
        "current_soc": "first",
        "electric_vehicle_departure_time": "first",
        "electric_vehicle_required_soc_departure": "first",
        "electric_vehicle_estimated_arrival_time": "first",
        "electric_vehicle_estimated_soc_arrival": "first",
        "charger": "first",
        "mode": "first",
    }

    PRICE_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "energy_price": "mean",
        "energy_price_predicted_1": "",
        "energy_price_predicted_2": "",
        "energy_price_predicted_3": "",
    }

    CORS_ALLOWED_ORIGINS: str | list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8006",
        "http://localhost:8011",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8006",
        "http://127.0.0.1:8011",
        "http://193.136.62.78:3000",
        "http://193.136.62.78:8006",
        "http://193.136.62.78:8011",
        "https://softcps.dei.isep.ipp.pt:3001",
        "https://softcps.dei.isep.ipp.pt:8011",
    ]

    @field_validator("CORS_ALLOWED_ORIGINS", mode="after")
    @classmethod
    def _parse_cors_origins(cls, value: Any) -> list[str]:
        return _parse_cors_allowed_origins(value)

    def mongo_uri(self, db_name: str) -> str:
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}/{db_name}"
            f"?authSource={self.MONGO_AUTH_SOURCE}"
        )


settings = Settings()
