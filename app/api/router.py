from fastapi import APIRouter, Depends
from app.services.execution_auth import legacy_access

from app.api.endpoints import agent, execution, health, jobs, ops, simulation_data


api_router = APIRouter()
api_router.include_router(execution.router, tags=["Execution v1"])
api_router.include_router(jobs.router, prefix="", tags=["Jobs"], dependencies=[Depends(legacy_access)])
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(agent.router, prefix="", tags=["Agent"])
api_router.include_router(ops.router, prefix="", tags=["Ops"], dependencies=[Depends(legacy_access)])
api_router.include_router(simulation_data.router, prefix="", tags=["SimulationData"], dependencies=[Depends(legacy_access)])
