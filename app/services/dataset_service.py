import re
from copy import deepcopy
from typing import Any

from fastapi import HTTPException

from app.utils import file_utils


def _dataset_name_part(value: str | None, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    normalized = re.sub(r"_+", "_", normalized).strip("._-")
    return normalized or fallback


def _automatic_dataset_name(name_prefix: str | None, site_id: str) -> str:
    prefix = _dataset_name_part(name_prefix, "auto")[:48]
    site = _dataset_name_part(site_id, "site")[:64]
    return f"{prefix}_{site}_all_buildings"


def _with_all_buildings_selected(config: dict[str, Any], buildings: list[str]) -> dict[str, Any]:
    resolved = deepcopy(config)
    if "selected_buildings" not in resolved and "buildings" not in resolved:
        resolved["selected_buildings"] = list(buildings)
    return resolved


def create_dataset(
    name: str,
    site_id: str,
    citylearn_configs: dict,
    description: str = "",
    period: int = 60,
    from_ts: str = None,
    until_ts: str = None,
    seconds_per_time_step: int | None = None,
):
    payload = file_utils.create_dataset_dir(
        name,
        site_id,
        citylearn_configs,
        description,
        period,
        from_ts,
        until_ts,
        seconds_per_time_step,
    )
    format_metadata = (
        file_utils.dataset_format_metadata(payload["path"])
        if payload.get("path")
        else {"format": "unknown", "type": "unknown", "formats": [], "format_counts": {}}
    )
    return {
        "message": "Dataset created",
        "name": name,
        "description": description,
        **format_metadata,
        "warnings": payload.get("warnings", []),
        "validation": payload.get("validation", {}),
    }


def create_datasets_from_mongo(
    name_prefix: str = "auto",
    site_ids: list[str] | None = None,
    citylearn_configs: dict[str, Any] | None = None,
    description: str = "",
    period: int | None = None,
    from_ts: str | None = None,
    until_ts: str | None = None,
    seconds_per_time_step: int | None = 15,
    dry_run: bool = False,
    continue_on_error: bool = True,
):
    if citylearn_configs is None:
        citylearn_configs = {}
    if not isinstance(citylearn_configs, dict):
        raise HTTPException(status_code=400, detail="citylearn_configs must be a JSON object")
    if period is not None and period < 1:
        raise HTTPException(status_code=400, detail="period must be >= 1 minute")
    resolved_seconds = (
        int(seconds_per_time_step)
        if seconds_per_time_step is not None
        else int(period) * 60
        if period is not None
        else 15
    )
    if resolved_seconds < 1:
        raise HTTPException(status_code=400, detail="seconds_per_time_step must be >= 1 second")

    available_sites = file_utils.list_dataset_sites()
    sites_by_id = {str(site.get("site_id")): site for site in available_sites if site.get("site_id")}

    if site_ids:
        requested_ids = [str(site_id) for site_id in site_ids]
        selected_sites = [sites_by_id[site_id] for site_id in requested_ids if site_id in sites_by_id]
        missing_site_ids = [site_id for site_id in requested_ids if site_id not in sites_by_id]
    else:
        selected_sites = available_sites
        missing_site_ids = []

    if not selected_sites and not missing_site_ids:
        raise HTTPException(status_code=404, detail="No CityLearn-compatible Mongo sites found.")

    failed = [
        {
            "site_id": site_id,
            "error": "Site is not CityLearn-compatible or was not found.",
        }
        for site_id in missing_site_ids
    ]

    if missing_site_ids and not continue_on_error:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "Some requested sites are not CityLearn-compatible or were not found.",
                "site_ids": missing_site_ids,
            },
        )

    planned = []
    created = []

    for site in selected_sites:
        site_id = str(site["site_id"])
        buildings = [str(building) for building in site.get("buildings", [])]
        dataset_name = _automatic_dataset_name(name_prefix, site_id)
        resolved_config = _with_all_buildings_selected(citylearn_configs, buildings)
        item = {
            "site_id": site_id,
            "name": dataset_name,
            "buildings": buildings,
            "building_count": len(buildings),
            "seconds_per_time_step": resolved_seconds,
        }

        if dry_run:
            planned.append(item)
            continue

        try:
            payload = file_utils.create_dataset_dir(
                dataset_name,
                site_id,
                resolved_config,
                description,
                period or 60,
                from_ts,
                until_ts,
                resolved_seconds,
            )
        except HTTPException as exc:
            failed.append({**item, "error": exc.detail})
            if not continue_on_error:
                raise
        except Exception as exc:
            failed.append({**item, "error": str(exc)})
            if not continue_on_error:
                raise HTTPException(status_code=500, detail=str(exc)) from exc
        else:
            created.append(
                {
                    **item,
                    "description": description,
                    "warnings": payload.get("warnings", []),
                    "validation": payload.get("validation", {}),
                }
            )

    return {
        "message": "Dataset generation dry run" if dry_run else "Automatic dataset generation completed",
        "dry_run": dry_run,
        "planned": planned,
        "created": created,
        "failed": failed,
    }

def list_dates_available_per_collection(site_id: str):
    return file_utils.list_dates_available_per_collection(site_id)


def list_dataset_sites():
    return {"sites": file_utils.list_dataset_sites()}

def list_datasets():
    return file_utils.list_available_datasets()

def delete_dataset(name: str):
    success = file_utils.delete_dataset_by_name(name)
    if not success:
        raise FileNotFoundError(f"Dataset {name} not found")
    return {"message": f"Dataset '{name}' deleted"}


def get_dataset_file(name: str) -> str:
    return file_utils.get_dataset_file(name)


def upload_dataset_archive(file_obj, source_filename: str, dataset_name: str | None = None):
    payload = file_utils.upload_dataset_archive(file_obj, source_filename, dataset_name)
    return {
        "message": "Dataset uploaded",
        "name": payload["name"],
        "size_bytes": payload["size_bytes"],
        "format": payload.get("format", "unknown"),
        "type": payload.get("type", "unknown"),
        "formats": payload.get("formats", []),
        "format_counts": payload.get("format_counts", {}),
    }
