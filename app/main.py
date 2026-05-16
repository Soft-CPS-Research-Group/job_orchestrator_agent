from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config import settings
from app.utils.job_utils import ensure_directories
from app.version import __version__


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_directories()
    yield


app = FastAPI(title="OPEVA Job Orchestrator", version=__version__, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
