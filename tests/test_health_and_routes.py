from fastapi.testclient import TestClient

from app.config import settings
from app.main import app
from app.version import __version__


def test_health_endpoint(monkeypatch, tmp_path):
    shared = tmp_path / "shared"
    monkeypatch.setattr(settings, "VM_SHARED_DATA", str(shared))
    monkeypatch.setattr(settings, "CONFIGS_DIR", str(shared / "configs"))
    monkeypatch.setattr(settings, "JOB_TRACK_FILE", str(shared / "job_track.json"))
    monkeypatch.setattr(settings, "JOBS_DIR", str(shared / "jobs"))
    monkeypatch.setattr(settings, "DATASETS_DIR", str(shared / "datasets"))
    monkeypatch.setattr(settings, "QUEUE_DIR", str(shared / "queue"))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_app_version_metadata():
    assert __version__ == "1.0.0"
    assert app.version == __version__


def test_job_orchestrator_exposes_expected_routes():
    expected = {
        "/run-simulation",
        "/jobs",
        "/queue",
        "/status/{job_id}",
        "/result/{job_id}",
        "/progress/{job_id}",
        "/logs/{job_id}",
        "/logs-chunk/{job_id}",
        "/job-info/{job_id}",
        "/job-resolved-config/{job_id}",
        "/hosts",
        "/job-images/versions",
        "/api/agent/next-job",
        "/api/agent/job-status",
        "/api/agent/heartbeat",
        "/ops/jobs/{job_id}/requeue",
        "/ops/jobs/{job_id}/fail",
        "/ops/jobs/{job_id}/cancel",
        "/ops/jobs/{job_id}/stop",
        "/ops/queue/cleanup",
        "/ops/jobs/cleanup",
        "/experiment-config/create",
        "/experiment-configs",
        "/experiment-config/{file_name}",
        "/dataset",
        "/dataset/sites",
        "/dataset/dates-available/{site_id}",
        "/datasets",
        "/dataset/download/{name}",
        "/dataset/upload",
        "/dataset/{name}",
        "/simulation-data/index",
        "/simulation-data/file",
    }

    actual = {route.path for route in app.routes}

    assert expected <= actual
