from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest


@pytest.fixture
def api_client():
    from app.api import router as api_router_module

    app = FastAPI()
    app.include_router(api_router_module.api_router)
    with TestClient(app) as client:
        yield client


def test_dataset_sites_endpoint(api_client, monkeypatch):
    from app.controllers import dataset_controller

    monkeypatch.setattr(
        dataset_controller,
        "list_dataset_sites",
        lambda: {"sites": [{"site_id": "living_lab", "buildings": ["R-H-01"]}]},
    )

    response = api_client.get("/dataset/sites")
    assert response.status_code == 200
    body = response.json()
    assert body["sites"][0]["site_id"] == "living_lab"


def test_create_dataset_endpoint_returns_validation(api_client, monkeypatch):
    from app.controllers import dataset_controller

    monkeypatch.setattr(
        dataset_controller,
        "create_dataset",
        lambda *args, **kwargs: {
            "message": "Dataset created",
            "name": "ds1",
            "description": "demo",
            "warnings": ["warning-a"],
            "validation": {"static": {"ok": True}},
        },
    )

    response = api_client.post(
        "/dataset",
        json={
            "name": "ds1",
            "site_id": "living_lab",
            "citylearn_configs": {},
            "description": "demo",
            "period": 60,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["warnings"] == ["warning-a"]
    assert body["validation"]["static"]["ok"] is True


def test_generate_datasets_endpoint_returns_bulk_result(api_client, monkeypatch):
    from app.controllers import dataset_controller

    called = {}

    def fake_generate(*args):
        called["args"] = args
        return {
            "message": "Automatic dataset generation completed",
            "dry_run": True,
            "planned": [
                {
                    "site_id": "living_lab",
                    "name": "demo_living_lab_all_buildings",
                    "buildings": ["R-H-01"],
                    "building_count": 1,
                }
            ],
            "created": [],
            "failed": [],
        }

    monkeypatch.setattr(dataset_controller, "create_datasets_from_mongo", fake_generate)

    response = api_client.post(
        "/datasets/generate",
        json={
            "name_prefix": "demo",
            "site_ids": ["living_lab"],
            "citylearn_configs": {"schema_overrides": {"central_agent": True}},
            "description": "generated automatically",
            "seconds_per_time_step": 15,
            "dry_run": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["dry_run"] is True
    assert body["planned"][0]["name"] == "demo_living_lab_all_buildings"
    assert called["args"][0] == "demo"
    assert called["args"][1] == ["living_lab"]
    assert called["args"][7] == 15
