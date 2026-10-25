import json
import os
import re

import yaml

from app.config import settings


def _safe_config_filename(file_name: str) -> str:
    normalized = os.path.normpath(file_name).strip().lstrip("/\\")
    if normalized.startswith("..") or os.path.isabs(file_name) or os.sep in normalized:
        raise ValueError("Invalid config file name")
    if not re.search(r"\.ya?ml$", normalized, flags=re.IGNORECASE):
        raise ValueError("Config file must use .yaml or .yml")
    return normalized


def save_config_dict(config: dict, file_name: str) -> str:
    safe_file_name = _safe_config_filename(file_name)
    os.makedirs(settings.CONFIGS_DIR, exist_ok=True)
    full_path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    with open(full_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)
    return safe_file_name


def collect_results(job_id: str):
    path = os.path.join(settings.JOBS_DIR, job_id, "results", "result.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    return {"status": "pending", "message": "Result not ready yet."}


def read_progress(job_id: str):
    path = os.path.join(settings.JOBS_DIR, job_id, "progress", "progress.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    return {"progress": "No updates yet."}
