from fastapi import APIRouter, Body, HTTPException, File, Form, UploadFile
from app.controllers import dataset_controller
from typing import Optional

router = APIRouter()

@router.post("/dataset")
async def create_dataset(
    name: str = Body(...),
    site_id: str = Body(...),
    citylearn_configs: dict = Body(...),
    description: Optional[str] = Body(""),
    period : Optional[int] = Body(60),
    seconds_per_time_step: Optional[int] = Body(None),
    from_ts: Optional[str] = Body(None),
    until_ts: Optional[str] = Body(None)
):
    return dataset_controller.create_dataset(
        name,
        site_id,
        citylearn_configs,
        description,
        60 if period is None else period,
        from_ts,
        until_ts,
        seconds_per_time_step,
    )


@router.post("/datasets/generate")
async def create_datasets_from_mongo(
    name_prefix: Optional[str] = Body("auto"),
    site_ids: Optional[list[str]] = Body(None),
    citylearn_configs: Optional[dict] = Body(None),
    description: Optional[str] = Body(""),
    period: Optional[int] = Body(None),
    seconds_per_time_step: Optional[int] = Body(None),
    from_ts: Optional[str] = Body(None),
    until_ts: Optional[str] = Body(None),
    dry_run: Optional[bool] = Body(False),
    continue_on_error: Optional[bool] = Body(True),
):
    return dataset_controller.create_datasets_from_mongo(
        name_prefix or "auto",
        site_ids,
        citylearn_configs or {},
        description or "",
        period,
        from_ts,
        until_ts,
        seconds_per_time_step,
        bool(dry_run),
        bool(continue_on_error),
    )


@router.get("/dataset/sites")
async def list_dataset_sites():
    return dataset_controller.list_dataset_sites()


@router.get("/dataset/dates-available/{site_id}")
async def list_dates_available_per_collection(site_id : str):
    return dataset_controller.list_dates_available_per_collection(site_id)
@router.get("/datasets")
async def list_datasets():
    return dataset_controller.list_datasets()

@router.get("/dataset/download/{name}")
async def download_dataset(name: str):
    try:
        return dataset_controller.download_dataset(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset not found")


@router.post("/dataset/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
):
    try:
        return dataset_controller.upload_dataset(file.file, file.filename or "dataset.zip", name)
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        await file.close()

@router.delete("/dataset/{name}")
async def delete_dataset(name: str):

    try:
        return dataset_controller.delete_dataset(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset not found")
