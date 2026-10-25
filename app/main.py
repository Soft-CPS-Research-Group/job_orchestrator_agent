from contextlib import asynccontextmanager
import asyncio
from contextlib import suppress
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config import settings
from app.utils.job_utils import ensure_directories
from app.version import __version__


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_directories()
    async def reconcile():
        from app.services.job_service import _mark_stale_jobs
        while True:
            await asyncio.sleep(settings.STALE_RECONCILIATION_INTERVAL)
            try:
                await asyncio.to_thread(_mark_stale_jobs)
            except Exception:
                logging.getLogger(__name__).exception('Unable to reconcile stale jobs; retrying next interval')
    task = asyncio.create_task(reconcile()) if settings.STALE_RECONCILIATION_INTERVAL > 0 else None
    try:
        yield
    finally:
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title="OPEVA Job Orchestrator", version=__version__, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
