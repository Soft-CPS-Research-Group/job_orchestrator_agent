import os

from fastapi.responses import FileResponse

from app.services import dataset_service

def create_dataset(
    name: str,
    site_id: str,
    config: dict,
    description: str = "",
    period: int = 60,
    from_ts: str = None,
    until_ts: str = None,
    seconds_per_time_step: int | None = None,
):
    return dataset_service.create_dataset(
        name,
        site_id,
        config,
        description,
        period,
        from_ts,
        until_ts,
        seconds_per_time_step,
    )


def create_datasets_from_mongo(
    name_prefix: str = "auto",
    site_ids: list[str] | None = None,
    citylearn_configs: dict | None = None,
    description: str = "",
    period: int | None = None,
    from_ts: str = None,
    until_ts: str = None,
    seconds_per_time_step: int | None = 15,
    dry_run: bool = False,
    continue_on_error: bool = True,
):
    return dataset_service.create_datasets_from_mongo(
        name_prefix,
        site_ids,
        citylearn_configs,
        description,
        period,
        from_ts,
        until_ts,
        seconds_per_time_step,
        dry_run,
        continue_on_error,
    )


def list_dates_available_per_collection(site_id: str):
    return dataset_service.list_dates_available_per_collection(site_id)


def list_dataset_sites():
    return dataset_service.list_dataset_sites()

def list_datasets():
    return dataset_service.list_datasets()

def delete_dataset(name: str):
    return dataset_service.delete_dataset(name)


def download_dataset(name: str):
    file_path = dataset_service.get_dataset_file(name)
    return FileResponse(file_path, filename=os.path.basename(file_path))


def upload_dataset(file_obj, source_filename: str, dataset_name: str | None = None):
    return dataset_service.upload_dataset_archive(file_obj, source_filename, dataset_name)
