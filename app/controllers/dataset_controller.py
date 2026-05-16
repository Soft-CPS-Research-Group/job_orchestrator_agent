import os

from fastapi.responses import FileResponse

from app.services import dataset_service

def create_dataset(name: str, site_id: str, config: dict, description: str = "", period : int = 60, from_ts: str = None, until_ts: str = None):
    return dataset_service.create_dataset(name, site_id, config, description, period, from_ts, until_ts)

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
