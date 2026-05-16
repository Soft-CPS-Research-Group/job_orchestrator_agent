from fastapi import APIRouter

from app.api.endpoints import agent, configs, datasets, health, jobs, ops, simulation_data


api_router = APIRouter()
api_router.include_router(jobs.router, prefix="", tags=["Jobs"])
api_router.include_router(configs.router, prefix="", tags=["Configs"])
api_router.include_router(datasets.router, prefix="", tags=["Datasets"])
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(agent.router, prefix="", tags=["Agent"])
api_router.include_router(ops.router, prefix="", tags=["Ops"])
api_router.include_router(simulation_data.router, prefix="", tags=["SimulationData"])
