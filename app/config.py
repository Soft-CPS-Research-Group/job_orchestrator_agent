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

    AVAILABLE_HOSTS: list[str] = ["server", "deucalion", "tiago-laptop", "jetson-xavier", "local"]
    HOST_HEARTBEAT_TTL: int = 60

    DEFAULT_JOB_IMAGE: str = "calof/opeva_simulator:latest"
    JOB_IMAGE_REPOSITORY: str = "calof/opeva_simulator"
    JOB_SIF_REPOSITORY: str = "calof/opeva_simulator_sif"
    JETSON_WORKER_HOSTS: list[str] = ["jetson-xavier"]
    JETSON_IMAGE_TAG_SUFFIX: str = "-jetson-r35.3.1"
    JOB_IMAGE_TAGS_LIMIT: int = 50
    JOB_IMAGE_CATALOG_TTL_SECONDS: int = 120
    JOB_IMAGE_CATALOG_TIMEOUT_SECONDS: int = 10
    CONTAINER_NAME_PREFIX: str = "opeva_job"
    WORKER_STALE_GRACE_SECONDS: int = 120
    REMOTE_WORKER_HOSTS: list[str] = ["tiago-laptop", "jetson-xavier"]
    REMOTE_WORKER_STALE_GRACE_SECONDS: int = 1800
    JOB_STATUS_TTL: int = 300
    DEUCALION_DISPATCH_STATUS_TTL: int = 21600
    DEUCALION_MAX_ACTIVE_CPU_JOBS: int = 1
    DEUCALION_MAX_ACTIVE_GPU_JOBS: int = 1
    MLFLOW_TRACKING_URI: str | None = None
    DEUCALION_MLFLOW_TRACKING_URI: str = "file:/data/mlflow/mlruns"
    MLFLOW_UI_BASE_URL: str | None = None
    UI_BASE_URL: str | None = "http://193.136.62.78:3000"
    UI_LINK_NETWORK_NOTICE: str | None = "Este link so abre se estiveres ligado a VPN/rede ISEP."

    JOB_EMAIL_NOTIFICATIONS_ENABLED: bool = True
    JOB_EMAIL_RABBITMQ_HOST: str = "rabbitmq"
    JOB_EMAIL_RABBITMQ_PORT: int = 5672
    JOB_EMAIL_RABBITMQ_QUEUE: str = "email_requests"
    JOB_EMAIL_RABBITMQ_USERNAME: str | None = "calof"
    JOB_EMAIL_RABBITMQ_PASSWORD: str | None = "calof"
    JOB_EMAIL_RABBITMQ_VHOST: str = "/"
    JOB_EMAIL_RABBITMQ_SOCKET_TIMEOUT_SECONDS: float = 2.0
    JOB_EMAIL_REPLY_TO: str | None = None
    JOB_EMAIL_NOTIFY_STATUSES: list[str] = [
        "queued",
        "dispatched",
        "running",
        "stop_requested",
        "finished",
        "failed",
        "stopped",
        "canceled",
    ]
    JOB_EMAIL_SUBMITTER_EMAILS: dict[str, str] = {
        "tiago": "calof@isep.ipp.pt",
        "tiago fonseca": "calof@isep.ipp.pt",
        "calof": "calof@isep.ipp.pt",
        "codex": "calof@isep.ipp.pt",
        "pedro monteiro": "1211076@isep.ipp.pt",
        "pedro alves monteiro": "1211076@isep.ipp.pt",
        "pedro.monteiro@energaize.io": "1211076@isep.ipp.pt",
        "gustavo": "1211061@isep.ipp.pt",
        "gustavo jorge": "1211061@isep.ipp.pt",
        "gustavo nuno chaves jorge": "1211061@isep.ipp.pt",
        "gustavo.jorge@energaize.io": "1211061@isep.ipp.pt",
    }
    JOB_EMAIL_SUBMITTER_NAMES: dict[str, str] = {
        "codex": "Tiago Fonseca",
        "pedro monteiro": "Pedro Monteiro",
        "pedro alves monteiro": "Pedro Alves Monteiro",
        "pedro.monteiro@energaize.io": "Pedro Alves Monteiro",
        "gustavo": "Gustavo Nuno Chaves Jorge",
        "gustavo jorge": "Gustavo Nuno Chaves Jorge",
        "gustavo nuno chaves jorge": "Gustavo Nuno Chaves Jorge",
        "gustavo.jorge@energaize.io": "Gustavo Nuno Chaves Jorge",
    }

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

    @field_validator("JOB_EMAIL_NOTIFY_STATUSES", mode="before")
    @classmethod
    def _parse_notify_statuses(cls, value: Any) -> list[str]:
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
            return [item.strip() for item in raw.split(",") if item.strip()]
        return value

    @field_validator("JOB_EMAIL_SUBMITTER_EMAILS", "JOB_EMAIL_SUBMITTER_NAMES", mode="before")
    @classmethod
    def _parse_email_mapping(cls, value: Any) -> dict[str, str]:
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return {}
            if raw.startswith("{"):
                try:
                    decoded = json.loads(raw)
                except json.JSONDecodeError:
                    pass
                else:
                    if isinstance(decoded, dict):
                        return {str(key).strip(): str(item).strip() for key, item in decoded.items() if str(key).strip()}

            parsed: dict[str, str] = {}
            for entry in raw.split(","):
                if "=" not in entry:
                    continue
                key, item = entry.split("=", 1)
                key = key.strip()
                item = item.strip()
                if key and item:
                    parsed[key] = item
            return parsed
        return value

    def mongo_uri(self, db_name: str) -> str:
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}/{db_name}"
            f"?authSource={self.MONGO_AUTH_SOURCE}"
        )


settings = Settings()
