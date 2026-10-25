"""Version 1 execution API. Community semantics remain entirely in the client."""
from typing import Any
from pathlib import Path
import hashlib

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from app.api.endpoints import jobs, ops, simulation_data
from app.models.job import JobLaunchRequest
from app.services import execution_auth as auth, execution_inputs as inputs, job_service
from app.utils import job_utils
from app.config import settings


class InputReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class RegisterInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    relative_path: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    namespace: str = Field(min_length=1, max_length=100)
    idempotency_key: str = Field(min_length=8, max_length=160)
    config: dict[str, Any]
    source: dict[str, Any] = Field(default_factory=dict)
    inputs: list[InputReference] = Field(default_factory=list)
    target_host: str | None = None
    target_worker_profile: str | None = None
    image_tag: str | None = None
    job_name: str | None = None
    submitted_by: str | None = None
    deucalion_options: dict | None = None


async def authorized(request: Request):
    principal = auth.client(request)
    job_id = request.path_params.get("job_id")
    if request.url.path.endswith(("/simulation-data/index", "/simulation-data/file")):
        job_id = (await request.json()).get('job_id')
    if job_id:
        auth.job(principal, job_id)
    if '/ops/workers/' in request.url.path and not principal.get('operator'):
        raise HTTPException(403, 'Worker administration requires operator access')
    return principal


router = APIRouter(prefix="/api/execution/v1", dependencies=[Depends(authorized)])


@router.post('/inputs')
def register_input(body: RegisterInput, principal=Depends(authorized)):
    return inputs.register(principal, body.relative_path, body.sha256)


@router.post('/jobs')
async def submit(body: Submission, principal=Depends(authorized)):
    namespace = auth.namespace(principal, body.namespace)
    names = [inputs.verify(principal, reference.model_dump()) for reference in body.inputs]
    inputs.validate_config(body.config, names)
    payload = body.model_dump(exclude={'namespace', 'source', 'inputs'})
    payload.update(workspace_id=namespace, execution={
        'client_id': principal['id'], 'namespace': namespace,
        'source': body.source, 'inputs': [ref.model_dump() for ref in body.inputs],
    })
    return await job_service.launch_simulation(JobLaunchRequest.model_validate(payload))


@router.get('/jobs')
def list_jobs(workspace_id: str | None = None, principal=Depends(authorized)):
    if workspace_id is not None:
        auth.namespace(principal, workspace_id)
    result = job_service.list_jobs(workspace_id)
    return [row for row in result if auth.owns(principal, job_service.jobs.get(row['job_id'], {}))]


@router.get('/queue')
def queue(principal=Depends(authorized)):
    job_service._refresh_jobs()
    return [row for row in job_service.list_queue()
            if auth.owns(principal, job_service.jobs.get(row['job_id'], {}))]


class QueueCleanup(BaseModel):
    workspace_id: str
    force: bool = False


@router.post('/ops/queue/cleanup')
def cleanup_queue(body: QueueCleanup, principal=Depends(authorized)):
    auth.namespace(principal, body.workspace_id)
    if body.force:
        raise HTTPException(400, 'Cancel individual jobs before cleaning the queue')
    job_service._refresh_jobs()
    removed = []
    for entry in job_utils.list_queue():
        meta = job_service.jobs.get(entry['job_id'], {})
        if not auth.owns(principal, meta) or meta.get('workspace_id', 'laboratory') != body.workspace_id:
            continue
        if job_service.get_status(entry['job_id'])['status'] not in {'queued', 'launching'}:
            if job_utils.remove_from_queue(entry['job_id']):
                removed.append(entry['job_id'])
    return {'removed': removed, 'count': len(removed)}


def artifact_path(job_id, relative):
    root = (Path(settings.JOBS_DIR) / job_id).resolve()
    path = root / relative
    if Path(relative).is_absolute() or '..' in Path(relative).parts or not path.resolve().is_relative_to(root):
        raise HTTPException(400, 'Invalid artifact path')
    if Path(relative).parts[0] not in {'bundle', 'results', 'progress', 'logs', 'config.resolved.yaml'}:
        raise HTTPException(404, 'Artifact not found')
    if not path.is_file():
        raise HTTPException(404, 'Artifact not found')
    return path


@router.get('/artifacts/{job_id}')
def artifacts(job_id: str):
    root = Path(settings.JOBS_DIR) / job_id
    files = []
    candidates = [root / 'config.resolved.yaml']
    for folder in ('bundle', 'results', 'progress', 'logs'):
        candidates.extend((root / folder).rglob('*'))
    for candidate in sorted(candidates):
        if not candidate.is_file():
            continue
        relative = candidate.relative_to(root).as_posix()
        path = artifact_path(job_id, relative)
        checksum = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                checksum.update(block)
        files.append({'path': relative, 'size_bytes': path.stat().st_size, 'sha256': checksum.hexdigest()})
    return {'job_id': job_id, 'files': files}


@router.get('/artifacts/{job_id}/{relative:path}')
def artifact(job_id: str, relative: str):
    return FileResponse(artifact_path(job_id, relative))


# These execution handlers are shared with the old transport, with authorization
# applied before invocation. Catalog/folder/global destructive routes are excluded.
for source_router in (jobs.router, ops.router, simulation_data.router):
    for route in source_router.routes:
        if ('{job_id}' in route.path and not route.path.startswith('/job/')) or route.path in {
            '/job/{job_id}', '/hosts', '/job-images/versions',
            '/deucalion/partitions', '/deucalion/diagnostics',
            '/simulation-data/index', '/simulation-data/file',
            '/ops/workers/{worker_id}/authenticate',
        }:
            router.add_api_route(route.path, route.endpoint, methods=list(route.methods),
                                 response_model=route.response_model)
