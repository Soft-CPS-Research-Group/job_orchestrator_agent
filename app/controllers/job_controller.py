from fastapi import HTTPException
from app.services import job_service
from app.models.job import JobLaunchRequest

async def run_simulation(request: JobLaunchRequest):
    try:
        return await job_service.launch_simulation(request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_status(job_id: str):
    return job_service.get_status(job_id)

def get_result(job_id: str):
    return job_service.get_result(job_id)

def get_progress(job_id: str):
    return job_service.get_progress(job_id)

def get_logs(job_id: str):
    return job_service.get_logs(job_id)


def get_logs_chunk(job_id: str, offset: int | None = None, tail_lines: int = 200, max_bytes: int = 262144):
    return job_service.get_logs_chunk(job_id, offset=offset, tail_lines=tail_lines, max_bytes=max_bytes)

def stop_job(job_id: str):
    return job_service.stop_job(job_id)

def list_jobs(workspace_id: str | None = None):
    return job_service.list_jobs(workspace_id)


def list_queue():
    return job_service.list_queue()

def get_job_info(job_id: str):
    return job_service.get_job_info(job_id)

def delete_job(job_id: str):
    return job_service.delete_job(job_id)


def list_job_folders(owner: str):
    return job_service.list_job_folders(owner)


def create_job_folder(owner: str, name: str):
    return job_service.create_job_folder(owner, name)


def rename_job_folder(folder_id: str, owner: str, name: str):
    return job_service.rename_job_folder(folder_id, owner, name)


def delete_job_folder(folder_id: str, owner: str):
    return job_service.delete_job_folder(folder_id, owner)


def move_job_to_folder(job_id: str, owner: str, folder_id: str | None):
    return job_service.move_job_to_folder(job_id, owner, folder_id)


def archive_job(job_id: str, owner: str):
    return job_service.archive_job(job_id, owner)


def restore_archived_job(job_id: str, owner: str):
    return job_service.restore_archived_job(job_id, owner)

def get_file_logs(job_id: str):
    return job_service.get_file_logs(job_id)

def get_hosts():
    return job_service.get_hosts()

def get_deucalion_partition_limits():
    return job_service.get_deucalion_partition_limits()

def get_deucalion_diagnostics():
    return job_service.get_deucalion_diagnostics()

def get_job_resolved_config(job_id: str):
    return job_service.get_job_resolved_config(job_id)


def get_job_image_versions(repository: str | None = None, limit: int | None = None):
    return job_service.list_job_image_versions(repository, limit)
