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

def list_jobs():
    return job_service.list_jobs()


def list_queue():
    return job_service.list_queue()

def get_job_info(job_id: str):
    return job_service.get_job_info(job_id)

def delete_job(job_id: str):
    return job_service.delete_job(job_id)

def get_file_logs(job_id: str):
    return job_service.get_file_logs(job_id)

def get_hosts():
    return job_service.get_hosts()

def get_job_resolved_config(job_id: str):
    return job_service.get_job_resolved_config(job_id)


def get_job_image_versions(repository: str | None = None, limit: int | None = None):
    return job_service.list_job_image_versions(repository, limit)
