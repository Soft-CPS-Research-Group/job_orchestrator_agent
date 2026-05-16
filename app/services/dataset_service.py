from app.utils import file_utils

def create_dataset(name: str, site_id: str, citylearn_configs: dict, description: str = "", period : int = 60, from_ts: str = None, until_ts: str = None):
    payload = file_utils.create_dataset_dir(name, site_id, citylearn_configs, description, period, from_ts, until_ts)
    return {
        "message": "Dataset created",
        "name": name,
        "description": description,
        "warnings": payload.get("warnings", []),
        "validation": payload.get("validation", {}),
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
    }
