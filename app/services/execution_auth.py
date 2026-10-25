"""Generic service identities; credentials and namespace grants are operator-owned."""
import hashlib
import hmac
import json
from pathlib import Path

from fastapi import HTTPException, Request

from app.config import settings


def client(request: Request):
    authorization = request.headers.get("authorization", "")
    if not settings.EXECUTION_CLIENTS_FILE or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Execution credentials required")
    digest = hashlib.sha256(authorization[7:].encode()).hexdigest()
    records = json.loads(Path(settings.EXECUTION_CLIENTS_FILE).read_text())
    for identity, record in records.items():
        if hmac.compare_digest(record["token_sha256"], digest):
            return {**record, "id": identity}
    raise HTTPException(401, "Invalid execution credentials")


def namespace(principal, value):
    grants = principal.get("namespaces", [])
    if value not in grants and "*" not in grants:
        raise HTTPException(403, "Namespace is not authorized")
    return value


def owns(principal, meta):
    execution = meta.get("execution") or {}
    owner = execution.get("client_id") or settings.EXECUTION_LEGACY_OWNER
    scope = execution.get("namespace") or meta.get("workspace_id") or "laboratory"
    return owner == principal["id"] and (scope in principal.get("namespaces", []) or "*" in principal.get("namespaces", []))


def job(principal, job_id):
    from app.services import job_service
    job_service._refresh_jobs()
    meta = job_service.jobs.get(job_id)
    if not meta or not owns(principal, meta):
        raise HTTPException(404, "Job not found")
    return meta


async def legacy_access(request: Request):
    """A legacy URL must not bypass ownership assigned by the new API."""
    from app.services import job_service
    job_id = request.path_params.get('job_id')
    if request.url.path.startswith('/simulation-data/'):
        job_id = (await request.json()).get('job_id')
    if job_id:
        job_service._refresh_jobs()
        meta = job_service.jobs.get(job_id, {})
        if meta.get('execution'):
            job(client(request), job_id)
    elif request.url.path in {'/ops/jobs/cleanup', '/ops/queue/cleanup'}:
        job_service._refresh_jobs()
        if any(meta.get('execution') for meta in job_service.jobs.values()):
            raise HTTPException(409, 'Use scoped execution operations; versioned jobs are present')
