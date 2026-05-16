from pathlib import Path

import pytest

from app.config import settings
from app.services import dataset_service
from app.controllers import dataset_controller
from app.utils import file_utils


@pytest.fixture(autouse=True)
def shared_env(tmp_path):
    base = tmp_path / "shared"
    datasets = base / "datasets"
    base.mkdir()
    datasets.mkdir()

    original = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "DATASETS_DIR": settings.DATASETS_DIR,
    }

    settings.VM_SHARED_DATA = str(base)
    settings.DATASETS_DIR = str(datasets)
    file_utils.settings = settings
    dataset_service.file_utils = file_utils

    try:
        yield
    finally:
        for key, value in original.items():
            setattr(settings, key, value)
        file_utils.settings = settings
        dataset_service.file_utils = file_utils


def test_dataset_service_create_calls_file_utils(monkeypatch):
    called = {}

    def fake_create(name, site_id, cfg, description, period, from_ts, until_ts):
        called["args"] = (name, site_id, cfg, description, period, from_ts, until_ts)
        return {"warnings": ["w1"], "validation": {"static": {"ok": True}}}

    monkeypatch.setattr(file_utils, "create_dataset_dir", fake_create)

    resp = dataset_service.create_dataset("ds1", "site", {"x": 1}, "desc", 30, "2020-01-01", "2020-01-02")
    assert resp["message"] == "Dataset created"
    assert resp["warnings"] == ["w1"]
    assert resp["validation"]["static"]["ok"] is True
    assert called["args"][0] == "ds1"


def test_dataset_service_upload_calls_file_utils(monkeypatch):
    called = {}

    def fake_upload(file_obj, source_filename, dataset_name=None):
        called["args"] = (file_obj, source_filename, dataset_name)
        return {"name": "uploaded", "size_bytes": 99}

    monkeypatch.setattr(file_utils, "upload_dataset_archive", fake_upload)

    payload = dataset_service.upload_dataset_archive(object(), "sample.zip", "uploaded")
    assert payload["message"] == "Dataset uploaded"
    assert payload["name"] == "uploaded"
    assert called["args"][1] == "sample.zip"


def test_dataset_controller_passthrough(monkeypatch):
    monkeypatch.setattr(dataset_service, "list_datasets", lambda: [{"name": "a"}])
    assert dataset_controller.list_datasets()[0]["name"] == "a"

    monkeypatch.setattr(dataset_service, "delete_dataset", lambda name: {"message": f"deleted {name}"})
    assert dataset_controller.delete_dataset("a")["message"] == "deleted a"

    download_path = Path(settings.DATASETS_DIR) / "f.csv"
    download_path.write_text("data")
    monkeypatch.setattr(dataset_service, "get_dataset_file", lambda name: str(download_path))
    resp = dataset_controller.download_dataset("a")
    assert resp.path == str(download_path)

    monkeypatch.setattr(
        dataset_service,
        "upload_dataset_archive",
        lambda file_obj, source_filename, dataset_name=None: {
            "message": "Dataset uploaded",
            "name": dataset_name or "x",
            "size_bytes": 10,
        },
    )
    upload_resp = dataset_controller.upload_dataset(object(), "a.zip", "imported")
    assert upload_resp["name"] == "imported"


def test_dataset_sites_passthrough(monkeypatch):
    monkeypatch.setattr(file_utils, "list_dataset_sites", lambda: [{"site_id": "s1", "buildings": ["B1"]}])
    service_payload = dataset_service.list_dataset_sites()
    assert service_payload["sites"][0]["site_id"] == "s1"

    monkeypatch.setattr(dataset_service, "list_dataset_sites", lambda: {"sites": [{"site_id": "s2", "buildings": []}]})
    controller_payload = dataset_controller.list_dataset_sites()
    assert controller_payload["sites"][0]["site_id"] == "s2"
