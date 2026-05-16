from fastapi import APIRouter, Response

from app.controllers import simulation_data_controller
from app.models.simulation_data import SimulationDataFileRequest, SimulationDataIndexRequest

router = APIRouter()


@router.post("/simulation-data/index")
async def simulation_data_index(payload: SimulationDataIndexRequest):
    return simulation_data_controller.index_simulation_data(
        job_id=payload.job_id,
        session=payload.session,
    )


@router.post("/simulation-data/file")
async def simulation_data_file(payload: SimulationDataFileRequest):
    content, media_type = simulation_data_controller.read_simulation_data_file(
        job_id=payload.job_id,
        relative_path=payload.relative_path,
        session=payload.session,
    )
    return Response(content=content, media_type=media_type)
