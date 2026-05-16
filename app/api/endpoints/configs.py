from fastapi import APIRouter, Response
from app.controllers import config_controller
from app.models.config import ConfigCreateRequest, ConfigUpdateRequest

router = APIRouter()

@router.post("/experiment-config/create")
async def create_config(payload: ConfigCreateRequest):
    return config_controller.create_config(payload.yaml_content, payload.file_name)


@router.put("/experiment-config/{file_name}")
async def update_config(file_name: str, payload: ConfigUpdateRequest):
    return config_controller.update_config(file_name, payload.yaml_content)

@router.get("/experiment-configs")
async def list_configs():
    return config_controller.list_configs()

@router.get("/experiment-config/{file_name}")
async def get_config(file_name: str):
    payload = config_controller.get_config(file_name)
    return Response(content=payload["yaml_content"], media_type="text/yaml")

@router.delete("/experiment-config/{file_name}")
async def delete_config(file_name: str):
    return config_controller.delete_config(file_name)
