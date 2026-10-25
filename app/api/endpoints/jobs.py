from fastapi import APIRouter, HTTPException, Query, Header
from fastapi.responses import StreamingResponse
from app.controllers import job_controller
from app.models.job import (
    JobFolderAssignmentRequest,
    JobFolderCreateRequest,
    JobFolderUpdateRequest,
    JobLaunchRequest,
    JobOrganizationOwnerRequest,
)

router = APIRouter()

@router.post("/run-simulation")
async def run_sim(request: JobLaunchRequest, x_training_token: str | None = Header(None)):
    from app.config import settings
    import hmac
    if request.execution:
        raise HTTPException(400, "Execution identity is assigned by the versioned API")
    if request.workspace_id and request.workspace_id != "laboratory" and settings.TRAINING_SERVICE_TOKEN:
        if not hmac.compare_digest(x_training_token or "", settings.TRAINING_SERVICE_TOKEN):
            raise HTTPException(403, "Scoped submissions require the Training service")
    return await job_controller.run_simulation(request)

@router.get("/status/{job_id}")
async def get_status(job_id: str):
    return job_controller.get_status(job_id)

@router.get("/result/{job_id}")
async def get_result(job_id: str):
    return job_controller.get_result(job_id)

@router.get("/progress/{job_id}")
async def get_progress(job_id: str):
    return job_controller.get_progress(job_id)

@router.get("/logs/{job_id}")
async def get_logs(job_id: str):
    return StreamingResponse(job_controller.get_logs(job_id), media_type="text/plain")

@router.get("/logs-chunk/{job_id}")
async def get_logs_chunk(
    job_id: str,
    offset: int | None = Query(default=None, ge=0),
    tail_lines: int = Query(default=200, ge=1, le=5000),
    max_bytes: int = Query(default=262144, ge=1024, le=2097152),
):
    return job_controller.get_logs_chunk(job_id, offset=offset, tail_lines=tail_lines, max_bytes=max_bytes)

@router.post("/stop/{job_id}")
async def stop_job(job_id: str):
    return job_controller.stop_job(job_id)

@router.get("/jobs")
async def list_jobs(workspace_id: str | None = None):
    return [row for row in job_controller.list_jobs(workspace_id)
            if not row.get('job_meta', {}).get('execution') and not row.get('job_info', {}).get('execution')]


@router.get("/queue")
async def list_queue():
    from app.services import job_service
    job_service._refresh_jobs()
    return [row for row in job_controller.list_queue()
            if not job_service.jobs.get(row['job_id'], {}).get('execution')]

@router.get("/job-info/{job_id}")
async def job_info(job_id: str):
    return job_controller.get_job_info(job_id)

@router.delete("/job/{job_id}")
async def delete_job(job_id: str):
    return job_controller.delete_job(job_id)


@router.get("/job-folders")
async def list_job_folders(owner: str = Query(min_length=1, max_length=160)):
    return job_controller.list_job_folders(owner)


@router.post("/job-folders")
async def create_job_folder(request: JobFolderCreateRequest):
    return job_controller.create_job_folder(request.owner, request.name)


@router.put("/job-folders/{folder_id}")
async def rename_job_folder(folder_id: str, request: JobFolderUpdateRequest):
    return job_controller.rename_job_folder(folder_id, request.owner, request.name)


@router.delete("/job-folders/{folder_id}")
async def delete_job_folder(folder_id: str, owner: str = Query(min_length=1, max_length=160)):
    return job_controller.delete_job_folder(folder_id, owner)


@router.put("/job/{job_id}/folder")
async def move_job_to_folder(job_id: str, request: JobFolderAssignmentRequest):
    return job_controller.move_job_to_folder(job_id, request.owner, request.folder_id)


@router.post("/job/{job_id}/archive")
async def archive_job(job_id: str, request: JobOrganizationOwnerRequest):
    return job_controller.archive_job(job_id, request.owner)


@router.post("/job/{job_id}/restore")
async def restore_archived_job(job_id: str, request: JobOrganizationOwnerRequest):
    return job_controller.restore_archived_job(job_id, request.owner)

@router.get("/file-logs/{job_id}")
async def file_logs(job_id: str):
    return StreamingResponse(job_controller.get_file_logs(job_id), media_type="text/plain")

@router.get("/job-resolved-config/{job_id}")
async def job_resolved_config(job_id: str):
    payload = job_controller.get_job_resolved_config(job_id)
    return StreamingResponse(iter([payload]), media_type="text/yaml")

@router.get("/hosts")
def hosts():
    return job_controller.get_hosts()


@router.get("/deucalion/partitions")
def deucalion_partitions():
    return job_controller.get_deucalion_partition_limits()


@router.get("/deucalion/diagnostics")
def deucalion_diagnostics():
    return job_controller.get_deucalion_diagnostics()


@router.get("/job-images/versions")
async def job_image_versions(
    repository: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=200),
):
    return job_controller.get_job_image_versions(repository, limit)
