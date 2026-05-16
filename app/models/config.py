from pydantic import BaseModel


class ConfigCreateRequest(BaseModel):
    file_name: str
    yaml_content: str


class ConfigUpdateRequest(BaseModel):
    yaml_content: str
