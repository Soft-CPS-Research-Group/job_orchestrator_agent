import json
from pathlib import Path

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    from app.config import settings
    from app.utils import job_utils, file_utils
    from app.services import job_service
    from app.controllers import job_controller
    from app.api import router as api_router_module

    original_values = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "CONFIGS_DIR": settings.CONFIGS_DIR,
        "JOBS_DIR": settings.JOBS_DIR,
        "DATASETS_DIR": settings.DATASETS_DIR,
        "QUEUE_DIR": settings.QUEUE_DIR,
        "JOB_TRACK_FILE": settings.JOB_TRACK_FILE,
        "HOST_HEARTBEATS": dict(job_service.host_heartbeats),
    }

    settings.VM_SHARED_DATA = str(tmp_path)
    settings.CONFIGS_DIR = str(Path(settings.VM_SHARED_DATA) / "configs")
    settings.JOBS_DIR = str(Path(settings.VM_SHARED_DATA) / "jobs")
    settings.DATASETS_DIR = str(Path(settings.VM_SHARED_DATA) / "datasets")
    settings.QUEUE_DIR = str(Path(settings.VM_SHARED_DATA) / "queue")
    settings.JOB_TRACK_FILE = str(Path(settings.VM_SHARED_DATA) / "job_track.json")
    for folder in (settings.CONFIGS_DIR, settings.JOBS_DIR, settings.DATASETS_DIR, settings.QUEUE_DIR):
        Path(folder).mkdir(parents=True, exist_ok=True)
    job_track = Path(settings.JOB_TRACK_FILE)
    if not job_track.exists():
        job_track.write_text("{}")

    job_utils.settings = settings
    file_utils.settings = settings
    job_service.settings = settings
    job_service.job_utils.settings = settings
    job_service.file_utils.settings = settings
    job_controller.job_service = job_service
    original_values["HOST_HEARTBEATS"] = dict(job_service.host_heartbeats)

    job_service.host_heartbeats.clear()

    ensure_calls: list[str] = []

    def _ensure_dirs():
        ensure_calls.append(settings.CONFIGS_DIR)
        for folder in (settings.CONFIGS_DIR, settings.JOBS_DIR, settings.DATASETS_DIR, settings.QUEUE_DIR):
            Path(folder).mkdir(parents=True, exist_ok=True)
        job_track = Path(settings.JOB_TRACK_FILE)
        if not job_track.exists():
            job_track.write_text("{}")

    monkeypatch.setattr(job_service.job_utils, "ensure_directories", _ensure_dirs)

    job_service.jobs.clear()

    try:
        app = FastAPI()
        app.include_router(api_router_module.api_router)
        client = TestClient(app)
        client._ensure_calls = ensure_calls  # type: ignore[attr-defined]
        yield client
    finally:
        client.close()
        job_service.jobs.clear()
        for attr, value in original_values.items():
            if attr == "HOST_HEARTBEATS":
                continue
            else:
                setattr(settings, attr, value)
        job_service.host_heartbeats = dict(original_values.get("HOST_HEARTBEATS", {}))
        job_utils.settings = settings
        file_utils.settings = settings
        job_service.settings = settings
        job_service.job_utils.settings = settings
        job_service.file_utils.settings = settings
        job_controller.job_service = job_service


def test_run_simulation_remote_accepts_prefixed_path(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    (Path(settings.CONFIGS_DIR)).mkdir(parents=True, exist_ok=True)
    config_file = Path(settings.CONFIGS_DIR) / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump({"experiment": {"name": "RemoteDemo", "run_name": "RunA"}})
    )

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local", "remote1"])

    captured = {}

    def fake_enqueue(payload):
        captured["payload"] = payload

    monkeypatch.setattr(job_service.job_utils, "enqueue_job", fake_enqueue)

    response = api_client.post(
        "/run-simulation",
        json={"config_path": "configs/demo.yaml", "target_host": "remote1"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == JobStatus.QUEUED.value
    assert data["host"] == "remote1"
    assert captured["payload"] == {"job_id": data["job_id"], "preferred_host": "remote1", "require_host": True}

    job_id = data["job_id"]
    jobs_resp = api_client.get("/jobs").json()
    assert any(j["status"] == JobStatus.QUEUED.value for j in jobs_resp)

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.QUEUED.value


def test_run_simulation_local_is_queued(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    payload = {"experiment": {"name": "LocalDemo", "run_name": "RunB"}}

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local"])

    from app.utils import file_utils
    from app.controllers import job_controller

    assert settings.VM_SHARED_DATA != "/opt/opeva_shared_data"
    assert Path(job_service.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert Path(job_service.job_utils.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert job_service.job_utils.settings is job_service.settings
    assert Path(file_utils.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert file_utils.settings is job_service.settings
    assert job_controller.job_service is job_service

    response = api_client.post(
        "/run-simulation",
        json={"config": payload},
    )
    assert getattr(api_client, "_ensure_calls"), "ensure_directories not invoked"
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == JobStatus.QUEUED.value
    assert data["host"] is None
    job_id = data["job_id"]

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.QUEUED.value

    jobs_resp = api_client.get("/jobs").json()
    assert any(j["status"] == JobStatus.QUEUED.value for j in jobs_resp)


def test_queue_endpoint_lists_entries(api_client, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Queued", "run_name": "Job"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    queue_resp = api_client.get("/queue")
    assert queue_resp.status_code == 200
    entries = queue_resp.json()
    assert any(entry.get("job_id") == job_id for entry in entries)


def test_run_simulation_allows_job_name_and_submitted_by(api_client, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    config_file = Path(settings.CONFIGS_DIR) / "named.yaml"
    config_file.write_text(yaml.safe_dump({"metadata": {"experiment_name": "Exp", "run_name": "Run"}}))

    response = api_client.post(
        "/run-simulation",
        json={
            "config_path": "named.yaml",
            "target_host": "worker-a",
            "job_name": "My custom job",
            "submitted_by": "tiago@energaize.io",
        },
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    jobs = api_client.get("/jobs").json()
    target = next(item for item in jobs if item["job_id"] == job_id)
    assert target["job_info"]["job_name"] == "My custom job"
    assert target["job_info"]["submitted_by"] == "tiago@energaize.io"

    queue = api_client.get("/queue").json()
    queued = next(item for item in queue if item["job_id"] == job_id)
    assert queued["submitted_by"] == "tiago@energaize.io"


def test_run_simulation_accepts_custom_image(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])
    image_tag = "sha-test123"
    expected_image = f"{settings.JOB_IMAGE_REPOSITORY}:{image_tag}"
    response = api_client.post(
        "/run-simulation",
        json={
            "config": {"experiment": {"name": "Image", "run_name": "API"}},
            "target_host": "worker-a",
            "image_tag": image_tag,
        },
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    assert response.json()["image_tag"] == image_tag
    assert response.json()["image"] == expected_image

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["image"] == expected_image
    assert dispatched["image_tag"] == image_tag

    info = api_client.get(f"/job-info/{job_id}").json()
    assert info["image"] == expected_image
    assert info["image_tag"] == image_tag


def test_job_resolved_config_endpoint(api_client, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local"])

    response = api_client.post(
        "/run-simulation",
        json={"config": {"metadata": {"experiment_name": "Cfg", "run_name": "Resolved"}}},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    resolved_path = Path(settings.JOBS_DIR) / job_id / "config.resolved.yaml"
    resolved_path.write_text("metadata:\n  experiment_name: Cfg\n")

    resolved_resp = api_client.get(f"/job-resolved-config/{job_id}")
    assert resolved_resp.status_code == 200
    assert "metadata:" in resolved_resp.text

    info_resp = api_client.get(f"/job-info/{job_id}")
    assert info_resp.status_code == 200
    assert info_resp.json()["resolved_config_available"] is True


def test_job_logs_chunk_endpoint_supports_tail_and_offset(api_client, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local"])
    response = api_client.post(
        "/run-simulation",
        json={"config": {"metadata": {"experiment_name": "Logs", "run_name": "Chunk"}}},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    logs_dir = Path(settings.JOBS_DIR) / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{job_id}.log"
    log_path.write_text("line-a\nline-b\nline-c\n", encoding="utf-8")

    first = api_client.get(f"/logs-chunk/{job_id}?tail_lines=2")
    assert first.status_code == 200
    first_payload = first.json()
    assert first_payload["text"] == "line-b\nline-c\n"
    assert first_payload["available"] is True
    assert isinstance(first_payload["next_offset"], int)

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("line-d\n")

    second = api_client.get(f"/logs-chunk/{job_id}?offset={first_payload['next_offset']}")
    assert second.status_code == 200
    second_payload = second.json()
    assert second_payload["text"] == "line-d\n"
    assert second_payload["next_offset"] > first_payload["next_offset"]


def test_agent_job_status_updates_exit_code(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Status", "run_name": "Update"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    data = response.json()
    job_id = data["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id

    resp = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": JobStatus.RUNNING.value,
            "worker_id": "worker-a",
            "container_id": "cid-1",
            "container_name": "cont-1",
        },
    )
    assert resp.status_code == 200

    resp = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": JobStatus.FINISHED.value,
            "worker_id": "worker-a",
            "exit_code": 0,
        },
    )
    assert resp.status_code == 200

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.FINISHED.value
    assert status_resp["exit_code"] == 0


def test_stop_job_marks_stop_requested(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Stop", "run_name": "Request"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None

    resp = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": JobStatus.RUNNING.value,
            "worker_id": "worker-a",
            "container_id": "cid-2",
            "container_name": "cont-2",
        },
    )
    assert resp.status_code == 200

    stop_resp = api_client.post(f"/stop/{job_id}")
    assert stop_resp.status_code == 200
    assert "Stop requested" in stop_resp.json()["message"]

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.STOP_REQUESTED.value


def test_ops_stop_endpoint(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    response = api_client.post(
        "/run-simulation",
        json={"config": {"experiment": {"name": "Ops", "run_name": "Stop"}}, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None

    running = api_client.post(
        "/api/agent/job-status",
        json={"job_id": job_id, "status": JobStatus.RUNNING.value, "worker_id": "worker-a"},
    )
    assert running.status_code == 200

    stop_resp = api_client.post(f"/ops/jobs/{job_id}/stop", json={"reason": "ops_manual_stop"})
    assert stop_resp.status_code == 200
    payload = stop_resp.json()
    assert payload["status"] == JobStatus.STOP_REQUESTED.value

    status_resp = api_client.get(f"/status/{job_id}")
    assert status_resp.status_code == 200
    status_payload = status_resp.json()
    assert status_payload["status"] == JobStatus.STOP_REQUESTED.value
    assert status_payload["stop_reason"] == "ops_manual_stop"


def test_ops_requeue_endpoint(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Ops", "run_name": "Requeue"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.DISPATCHED.value

    resp = api_client.post(f"/ops/jobs/{job_id}/requeue")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == JobStatus.QUEUED.value


def test_experiment_config_yaml_endpoints(api_client):
    create = api_client.post(
        "/experiment-config/create",
        json={"file_name": "api-config.yaml", "yaml_content": "experiment:\n  name: api-test\n"},
    )
    assert create.status_code == 200
    assert create.json()["file"] == "api-config.yaml"

    get_resp = api_client.get("/experiment-config/api-config.yaml")
    assert get_resp.status_code == 200
    assert "experiment:" in get_resp.text
    assert get_resp.headers["content-type"].startswith("text/yaml")

    update = api_client.put(
        "/experiment-config/api-config.yaml",
        json={"yaml_content": "experiment:\n  name: api-test-updated\n"},
    )
    assert update.status_code == 200
    assert update.json()["message"] == "Config updated"

    delete = api_client.delete("/experiment-config/api-config.yaml")
    assert delete.status_code == 200


def test_experiment_config_invalid_yaml_returns_422(api_client):
    bad = api_client.post(
        "/experiment-config/create",
        json={"file_name": "bad.yaml", "yaml_content": "experiment: [\n"},
    )
    assert bad.status_code == 422
    detail = bad.json().get("detail", "")
    assert "Invalid YAML" in detail
    assert "line" in detail.lower()


def test_hosts_include_active_job_ids_and_current_job(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Hosts", "run_name": "Active"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    running = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": "running",
            "worker_id": "worker-a",
            "worker_version": "0.4.1",
        },
    )
    assert running.status_code == 200

    status_payload = api_client.get(f"/status/{job_id}").json()
    assert status_payload["worker_version"] == "0.4.1"

    hosts_resp = api_client.get("/hosts")
    assert hosts_resp.status_code == 200
    hosts = hosts_resp.json()["hosts"]
    row = hosts["worker-a"]
    assert job_id in row["active_job_ids"]
    assert row["current_job_id"] == job_id
    assert row["current_job_status"] == "running"
    assert "info" in row
    assert row["info"]["worker_version"] == "0.4.1"
    assert row["info"]["last_status_worker_version"] == "0.4.1"
    assert row["info"]["last_status"] == "running"
    assert row["info"]["last_status_job_id"] == job_id
    assert isinstance(row["info"]["last_status_at"], (int, float))


def test_deucalion_partitions_endpoint(api_client):
    resp = api_client.get("/deucalion/partitions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["source"].startswith("https://docs.deucalion.macc.fccn.pt/")
    partitions = {entry["partition"]: entry for entry in payload["partitions"]}
    assert partitions["normal-x86"]["time_limit_seconds"] == 48 * 60 * 60
    assert partitions["large-x86"]["time_limit_seconds"] == 72 * 60 * 60
    assert partitions["dev-a100-40"]["time_limit_seconds"] == 4 * 60 * 60


def test_job_image_versions_endpoint(api_client, monkeypatch):
    from app.services import job_service

    monkeypatch.setattr(
        job_service,
        "list_job_image_versions",
        lambda repository=None, limit=None: {
            "repository": repository or "calof/algorithms",
            "sif_repository": "calof/algorithms_sif",
            "tags": [
                {
                    "name": "v1.2.3",
                    "last_updated": "2026-03-31T10:00:00Z",
                    "digest": "sha256:abc",
                    "deucalion_ready": True,
                }
            ],
            "count": 1,
            "cached": False,
            "fetched_at": 1_710_000_000.0,
        },
    )

    resp = api_client.get("/job-images/versions", params={"repository": "calof/algorithms", "limit": 20})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["repository"] == "calof/algorithms"
    assert payload["sif_repository"] == "calof/algorithms_sif"
    assert payload["count"] == 1
    assert payload["tags"][0]["name"] == "v1.2.3"
    assert payload["tags"][0]["deucalion_ready"] is True
