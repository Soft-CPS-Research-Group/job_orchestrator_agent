from pymongo import MongoClient
from app.config import settings

_connections = {}

def get_client():
    if "default" not in _connections:
        _connections["default"] = MongoClient(
            f"mongodb://{settings.MONGO_USER}:{settings.MONGO_PASSWORD}@{settings.MONGO_HOST}:{settings.MONGO_PORT}/?authSource={settings.MONGO_AUTH_SOURCE}"
        )
    return _connections["default"]

def get_db(db_name: str):
    return get_client()[db_name]

def list_databases():
    return get_client().list_database_names()