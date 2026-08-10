from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from app.config import settings


SCHEMA_VERSION = 1


def normalize_owner(value: str | None) -> str:
    return " ".join(str(value or "").strip().split()).casefold()


def _store_path() -> Path:
    return Path(settings.JOBS_DIR) / ".organization.json"


def _empty_store() -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "folders": {}, "jobs": {}}


def _read_unlocked(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return _empty_store()
    if not isinstance(payload, dict):
        return _empty_store()
    folders = payload.get("folders") if isinstance(payload.get("folders"), dict) else {}
    jobs = payload.get("jobs") if isinstance(payload.get("jobs"), dict) else {}
    return {"schema_version": SCHEMA_VERSION, "folders": folders, "jobs": jobs}


def _write_unlocked(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=".organization.",
        suffix=".json.tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            if os.path.exists(temporary_path):
                os.remove(temporary_path)
        except OSError:
            pass


@contextmanager
def _locked_store(*, write: bool) -> Iterator[dict[str, Any]]:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX if write else fcntl.LOCK_SH)
        payload = _read_unlocked(path)
        try:
            yield payload
            if write:
                _write_unlocked(path, payload)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _public_folder(folder: dict[str, Any]) -> dict[str, Any]:
    return {
        "folder_id": folder.get("folder_id"),
        "name": folder.get("name"),
        "owner": folder.get("owner"),
        "created_at": folder.get("created_at"),
        "updated_at": folder.get("updated_at"),
    }


def list_folders(owner: str) -> list[dict[str, Any]]:
    owner_key = normalize_owner(owner)
    if not owner_key:
        return []
    with _locked_store(write=False) as store:
        folders = [
            _public_folder(folder)
            for folder in store["folders"].values()
            if isinstance(folder, dict) and folder.get("owner_key") == owner_key
        ]
    return sorted(folders, key=lambda folder: (str(folder.get("name") or "").casefold(), str(folder["folder_id"])))


def create_folder(owner: str, name: str) -> dict[str, Any]:
    owner_display = " ".join(owner.strip().split())
    owner_key = normalize_owner(owner_display)
    folder_name = " ".join(name.strip().split())
    if not owner_key:
        raise ValueError("Folder owner is required")
    if not folder_name:
        raise ValueError("Folder name is required")
    if len(folder_name) > 80:
        raise ValueError("Folder name cannot exceed 80 characters")

    with _locked_store(write=True) as store:
        for existing in store["folders"].values():
            if not isinstance(existing, dict):
                continue
            if existing.get("owner_key") == owner_key and str(existing.get("name") or "").casefold() == folder_name.casefold():
                raise ValueError("A folder with this name already exists")
        now = time.time()
        folder_id = str(uuid4())
        folder = {
            "folder_id": folder_id,
            "name": folder_name,
            "owner": owner_display,
            "owner_key": owner_key,
            "created_at": now,
            "updated_at": now,
        }
        store["folders"][folder_id] = folder
        return _public_folder(folder)


def rename_folder(folder_id: str, owner: str, name: str) -> dict[str, Any]:
    owner_key = normalize_owner(owner)
    folder_name = " ".join(name.strip().split())
    if not folder_name:
        raise ValueError("Folder name is required")
    if len(folder_name) > 80:
        raise ValueError("Folder name cannot exceed 80 characters")

    with _locked_store(write=True) as store:
        folder = store["folders"].get(folder_id)
        if not isinstance(folder, dict) or folder.get("owner_key") != owner_key:
            raise KeyError(folder_id)
        for other_id, existing in store["folders"].items():
            if other_id == folder_id or not isinstance(existing, dict):
                continue
            if existing.get("owner_key") == owner_key and str(existing.get("name") or "").casefold() == folder_name.casefold():
                raise ValueError("A folder with this name already exists")
        folder["name"] = folder_name
        folder["updated_at"] = time.time()
        return _public_folder(folder)


def delete_folder(folder_id: str, owner: str) -> None:
    owner_key = normalize_owner(owner)
    with _locked_store(write=True) as store:
        folder = store["folders"].get(folder_id)
        if not isinstance(folder, dict) or folder.get("owner_key") != owner_key:
            raise KeyError(folder_id)
        del store["folders"][folder_id]
        for organization in store["jobs"].values():
            if not isinstance(organization, dict):
                continue
            if organization.get("owner_key") == owner_key and organization.get("folder_id") == folder_id:
                organization["folder_id"] = None
                organization["updated_at"] = time.time()


def get_job_organization(job_id: str) -> dict[str, Any]:
    return get_job_organizations([job_id])[job_id]


def _public_job_organization(organization: Any) -> dict[str, Any]:
    if not isinstance(organization, dict):
        return {
            "folder_id": None,
            "archived": False,
            "archived_at": None,
            "archived_by": None,
        }
    return {
        "folder_id": organization.get("folder_id"),
        "archived": bool(organization.get("archived_at")),
        "archived_at": organization.get("archived_at"),
        "archived_by": organization.get("archived_by"),
        "owner": organization.get("owner"),
        "updated_at": organization.get("updated_at"),
    }


def get_job_organizations(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    with _locked_store(write=False) as store:
        organizations = store["jobs"]
        return {
            job_id: _public_job_organization(organizations.get(job_id))
            for job_id in job_ids
        }


def archived_job_ids() -> set[str]:
    with _locked_store(write=False) as store:
        return {
            job_id
            for job_id, organization in store["jobs"].items()
            if isinstance(organization, dict) and bool(organization.get("archived_at"))
        }


def set_job_folder(job_id: str, owner: str, folder_id: str | None) -> dict[str, Any]:
    owner_display = " ".join(owner.strip().split())
    owner_key = normalize_owner(owner_display)
    with _locked_store(write=True) as store:
        if folder_id is not None:
            folder = store["folders"].get(folder_id)
            if not isinstance(folder, dict) or folder.get("owner_key") != owner_key:
                raise KeyError(folder_id)
        organization = store["jobs"].setdefault(job_id, {})
        organization.update(
            {
                "owner": owner_display,
                "owner_key": owner_key,
                "folder_id": folder_id,
                "updated_at": time.time(),
            }
        )
    return get_job_organization(job_id)


def set_job_archived(job_id: str, owner: str, *, archived: bool) -> dict[str, Any]:
    owner_display = " ".join(owner.strip().split())
    owner_key = normalize_owner(owner_display)
    with _locked_store(write=True) as store:
        organization = store["jobs"].setdefault(job_id, {})
        organization.update(
            {
                "owner": owner_display,
                "owner_key": owner_key,
                "updated_at": time.time(),
            }
        )
        if archived:
            organization["archived_at"] = time.time()
            organization["archived_by"] = owner_display
        else:
            organization.pop("archived_at", None)
            organization.pop("archived_by", None)
    return get_job_organization(job_id)


def remove_job(job_id: str) -> None:
    with _locked_store(write=True) as store:
        store["jobs"].pop(job_id, None)


def remove_jobs(job_ids: set[str]) -> None:
    if not job_ids:
        return
    with _locked_store(write=True) as store:
        for job_id in job_ids:
            store["jobs"].pop(job_id, None)
