from app.services import config_service

def create_config(yaml_content: str, file_name: str):
    return config_service.save_config_yaml(yaml_content, file_name)


def update_config(file_name: str, yaml_content: str):
    return config_service.update_config_yaml(file_name, yaml_content)

def list_configs():
    return config_service.list_configs()

def get_config(file_name: str):
    return config_service.get_config_by_name(file_name)

def delete_config(file_name: str):
    return config_service.delete_config(file_name)
