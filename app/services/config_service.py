from fastapi import HTTPException
import yaml
from app.utils import file_utils

def _format_yaml_error(exc: yaml.YAMLError) -> str:
    mark = getattr(exc, "problem_mark", None)
    if mark is None:
        return str(exc)
    return f"{exc} (line {mark.line + 1}, column {mark.column + 1})"


def save_config_yaml(yaml_content: str, file_name: str):
    try:
        saved = file_utils.save_config_yaml_content(yaml_content, file_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid YAML: {_format_yaml_error(exc)}")
    return {"message": "Config saved", "file": saved}


def update_config_yaml(file_name: str, yaml_content: str):
    try:
        existing = file_utils.load_config_file_text(file_name)
        del existing
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        saved = file_utils.save_config_yaml_content(yaml_content, file_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid YAML: {_format_yaml_error(exc)}")
    return {"message": "Config updated", "file": saved}

def list_configs():
    return file_utils.list_config_files()

def get_config_by_name(file_name: str):
    try:
        return {"yaml_content": file_utils.load_config_file_text(file_name)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

def delete_config(file_name: str):
    try:
        success = file_utils.delete_config_by_name(file_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not success:
        raise HTTPException(status_code=404, detail="Config file not found")
    return {"message": f"Config {file_name} deleted successfully"}
