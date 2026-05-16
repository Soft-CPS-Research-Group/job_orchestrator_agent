import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def sim_client(tmp_path):
    from app.config import settings
    from app.api import router as api_router_module
    from app.services import simulation_data_service

    original_jobs_dir = settings.JOBS_DIR
    settings.JOBS_DIR = str(tmp_path / "jobs")
    Path(settings.JOBS_DIR).mkdir(parents=True, exist_ok=True)
    simulation_data_service.settings = settings

    app = FastAPI()
    app.include_router(api_router_module.api_router)
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        settings.JOBS_DIR = original_jobs_dir
        simulation_data_service.settings = settings


def _write_csv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_simulation_data_index_and_file_latest_session(sim_client):
    from app.config import settings

    job_root = Path(settings.JOBS_DIR) / "job-1" / "results" / "simulation_data"
    s1 = job_root / "session-a"
    s2 = job_root / "session-b"
    _write_csv(s1 / "community.csv", "timestamp,value\n2024-08-01T00:00:00Z,1\n")
    _write_csv(s2 / "community.csv", "timestamp,value\n2024-08-02T00:00:00Z,2\n")
    os.utime(s1, (1, 1))
    os.utime(s2, (2, 2))

    resp = sim_client.post("/simulation-data/index", json={"job_id": "job-1", "session": "latest"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["session"] == "session-b"
    assert "community.csv" in body["files"]
    assert "2024-08-02" in body["available_days"]

    file_resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-1", "session": "session-b", "relative_path": "community.csv"},
    )
    assert file_resp.status_code == 200
    assert "timestamp,value" in file_resp.text


def test_simulation_data_reads_bundle_manifest_from_job_root(sim_client):
    from app.config import settings

    job_root = Path(settings.JOBS_DIR) / "job-2"
    manifest_path = job_root / "bundle" / "artifact_manifest.json"
    manifest_payload = '{\n  "metadata": {"experiment_name": "demo"}\n}\n'
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(manifest_payload, encoding="utf-8")

    resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-2", "session": "latest", "relative_path": "bundle/artifact_manifest.json"},
    )
    assert resp.status_code == 200
    assert '"experiment_name": "demo"' in resp.text
    assert resp.headers.get("content-type", "").startswith("application/json")


def test_simulation_data_reads_binary_bundle_artifact(sim_client):
    from app.config import settings

    job_root = Path(settings.JOBS_DIR) / "job-3"
    binary_path = job_root / "bundle" / "onnx_models" / "agent_0.onnx"
    payload = b"\x08\x01onnx-test-content"
    _write_bytes(binary_path, payload)

    resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-3", "session": "latest", "relative_path": "bundle/onnx_models/agent_0.onnx"},
    )
    assert resp.status_code == 200
    assert resp.content == payload
    assert resp.headers.get("content-type", "").startswith("application/octet-stream")


def test_simulation_data_prefers_session_csv_over_job_root_file(sim_client):
    from app.config import settings

    job_root = Path(settings.JOBS_DIR) / "job-4"
    session_dir = job_root / "results" / "simulation_data" / "session-z"
    _write_csv(session_dir / "community.csv", "timestamp,value\n2024-08-03T00:00:00Z,33\n")
    # Same relative path name at job root should not override session file.
    _write_csv(job_root / "community.csv", "timestamp,value\n2024-08-03T00:00:00Z,999\n")

    resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-4", "session": "latest", "relative_path": "community.csv"},
    )
    assert resp.status_code == 200
    assert "33" in resp.text
    assert "999" not in resp.text
    assert resp.headers.get("content-type", "").startswith("text/csv")


def test_simulation_data_rejects_path_traversal(sim_client):
    resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-1", "session": "latest", "relative_path": "../secrets.csv"},
    )
    assert resp.status_code == 400
    assert "relative_path" in resp.text


def test_simulation_data_404_when_missing_job(sim_client):
    resp = sim_client.post("/simulation-data/index", json={"job_id": "missing-job", "session": "latest"})
    assert resp.status_code == 404
