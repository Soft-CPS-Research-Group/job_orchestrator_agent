import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from app.config import settings
from app.status import JobStatus
from app.models.job import JobLaunchRequest
from app.utils import job_utils, file_utils
from app.services import job_service
from fastapi import HTTPException


@pytest.fixture(autouse=True)
def jobs_env(tmp_path, monkeypatch):
    base = tmp_path / "shared"
    configs = base / "configs"
    jobs_dir = base / "jobs"
    datasets = base / "datasets"
    queue = base / "queue"
    for folder in (configs, jobs_dir, datasets, queue):
        folder.mkdir(parents=True, exist_ok=True)
    job_track = base / "job_track.json"
    job_track.write_text("{}")

    original = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "CONFIGS_DIR": settings.CONFIGS_DIR,
        "JOBS_DIR": settings.JOBS_DIR,
        "DATASETS_DIR": settings.DATASETS_DIR,
        "QUEUE_DIR": settings.QUEUE_DIR,
        "JOB_TRACK_FILE": settings.JOB_TRACK_FILE,
        "AVAILABLE_HOSTS": list(settings.AVAILABLE_HOSTS),
        "MLFLOW_TRACKING_URI": settings.MLFLOW_TRACKING_URI,
        "DEUCALION_MLFLOW_TRACKING_URI": settings.DEUCALION_MLFLOW_TRACKING_URI,
        "MLFLOW_UI_BASE_URL": settings.MLFLOW_UI_BASE_URL,
        "HOST_HEARTBEATS": dict(job_service.host_heartbeats),
    }

    settings.VM_SHARED_DATA = str(base)
    settings.CONFIGS_DIR = str(configs)
    settings.JOBS_DIR = str(jobs_dir)
    settings.DATASETS_DIR = str(datasets)
    settings.QUEUE_DIR = str(queue)
    settings.JOB_TRACK_FILE = str(job_track)

    job_utils.settings = settings
    file_utils.settings = settings
    job_service.settings = settings
    job_service.job_utils.settings = settings
    job_service.file_utils.settings = settings

    job_service.jobs.clear()
    job_service.host_heartbeats.clear()

    try:
        yield SimpleNamespace(base=base, configs=configs, jobs=jobs_dir, queue=queue)
    finally:
        job_service.jobs.clear()
        job_track.write_text("{}")
        for key, value in original.items():
            if key == "AVAILABLE_HOSTS":
                settings.AVAILABLE_HOSTS = value
            elif key == "HOST_HEARTBEATS":
                job_service.host_heartbeats = dict(value)
            else:
                setattr(settings, key, value)
        job_utils.settings = settings
        file_utils.settings = settings
        job_service.settings = settings
        job_service.job_utils.settings = settings
        job_service.file_utils.settings = settings


def test_launch_remote_persists_and_queues(monkeypatch):
    settings.AVAILABLE_HOSTS = ["local", "remote1"]

    config_path = Path(settings.CONFIGS_DIR) / "demo.yaml"
    config_path.write_text(yaml.safe_dump({"experiment": {"name": "Remote", "run_name": "RunA"}}))

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config_path="demo.yaml", target_host="remote1")
        )
    )

    job_id = result["job_id"]
    assert result["status"] == JobStatus.QUEUED.value
    assert result["host"] == "remote1"

    queued_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queued_file.exists()
    queued_payload = json.loads(queued_file.read_text())
    assert queued_payload["job_id"] == job_id
    assert queued_payload["preferred_host"] == "remote1"
    assert queued_payload["require_host"] is True

    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.QUEUED.value
    assert track[job_id]["config_path"] == "configs/demo.yaml"

    status_path = Path(settings.JOBS_DIR) / job_id / "status.json"
    assert status_path.exists()
    status_data = json.loads(status_path.read_text())
    assert status_data["status"] == JobStatus.QUEUED.value
    assert status_data["preferred_host"] == "remote1"

    info_path = Path(settings.JOBS_DIR) / job_id / "job_info.json"
    info = json.loads(info_path.read_text())
    assert info["target_host"] == "remote1"
    assert info["config_path"] == "configs/demo.yaml"
    assert info["container_id"] == ""

    assert job_service.jobs[job_id]["status"] == JobStatus.QUEUED.value


def test_launch_local_is_queued():
    settings.AVAILABLE_HOSTS = ["local"]

    payload = {"experiment": {"name": "Local", "run_name": "RunB"}}

    result = asyncio.run(
        job_service.launch_simulation(JobLaunchRequest(config=payload))
    )

    job_id = result["job_id"]
    assert result["host"] is None
    assert result["status"] == JobStatus.QUEUED.value

    config_file = Path(settings.CONFIGS_DIR) / f"{job_id}.yaml"
    assert config_file.exists()

    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    status_path = Path(settings.JOBS_DIR) / job_id / "status.json"
    status_data = json.loads(status_path.read_text())
    assert status_data["status"] == JobStatus.QUEUED.value


def test_launch_prefers_metadata_experiment_identity():
    settings.AVAILABLE_HOSTS = ["local"]

    payload = {
        "metadata": {"experiment_name": "MetaExp", "run_name": "MetaRun"},
        "experiment": {"name": "LegacyExp", "run_name": "LegacyRun"},
    }

    result = asyncio.run(
        job_service.launch_simulation(JobLaunchRequest(config=payload))
    )
    job_id = result["job_id"]

    assert job_service.jobs[job_id]["experiment_name"] == "MetaExp"
    assert job_service.jobs[job_id]["run_name"] == "MetaRun"
    assert job_service.jobs[job_id]["job_name"] == "MetaExp-MetaRun"

    info_path = Path(settings.JOBS_DIR) / job_id / "job_info.json"
    info = json.loads(info_path.read_text())
    assert info["experiment_name"] == "MetaExp"
    assert info["run_name"] == "MetaRun"


def test_launch_falls_back_to_legacy_experiment_identity():
    settings.AVAILABLE_HOSTS = ["local"]

    payload = {"experiment": {"name": "LegacyExp", "run_name": "LegacyRun"}}

    result = asyncio.run(
        job_service.launch_simulation(JobLaunchRequest(config=payload))
    )
    job_id = result["job_id"]

    assert job_service.jobs[job_id]["experiment_name"] == "LegacyExp"
    assert job_service.jobs[job_id]["run_name"] == "LegacyRun"
    assert job_service.jobs[job_id]["job_name"] == "LegacyExp-LegacyRun"


def test_launch_rejects_unknown_host():
    payload = {"experiment": {"name": "Bad", "run_name": "Run"}}

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(config=payload, target_host="ghost")
            )
        )
    assert exc.value.status_code == 400


def test_launch_rejects_traversal(monkeypatch):
    Path(settings.CONFIGS_DIR).mkdir(parents=True, exist_ok=True)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(config_path="../evil.yaml", target_host="local")
            )
        )
    assert exc.value.status_code == 400


def test_get_status_updates_on_exit(monkeypatch):
    job_id = "job-exit"
    job_service.jobs[job_id] = {
        "target_host": "local",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.FAILED.value, {"exit_code": 5})

    resp = job_service.get_status(job_id)
    assert resp["status"] == JobStatus.FAILED.value
    assert resp["exit_code"] == 5

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.FAILED.value
    assert status_data["exit_code"] == 5


def test_deucalion_dispatch_limits_one_active_cpu_and_gpu():
    settings.AVAILABLE_HOSTS = ["deucalion"]

    cpu_one = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "CPU1"}},
                target_host="deucalion",
                deucalion_options={"partition": "normal-x86"},
            )
        )
    )
    cpu_two = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "CPU2"}},
                target_host="deucalion",
                deucalion_options={"partition": "normal-x86"},
            )
        )
    )
    gpu_one = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "GPU1"}},
                target_host="deucalion",
                deucalion_options={"partition": "normal-a100-80", "gpus": 1},
            )
        )
    )

    first = job_service.agent_next_job("deucalion")
    assert first is not None
    assert first["job_id"] == cpu_one["job_id"]

    second = job_service.agent_next_job("deucalion")
    assert second is not None
    assert second["job_id"] == gpu_one["job_id"]

    third = job_service.agent_next_job("deucalion")
    assert third is None

    queued_cpu = Path(settings.QUEUE_DIR) / f"{cpu_two['job_id']}.json"
    assert queued_cpu.exists()

    hosts = job_service.get_hosts()
    deucalion = hosts["hosts"]["deucalion"]
    assert deucalion["info"]["max_active_jobs"] == 2
    assert deucalion["info"]["max_active_jobs_by_profile"] == {"cpu": 1, "gpu": 1}
    assert deucalion["info"]["active_job_count_by_profile"] == {"cpu": 1, "gpu": 1}
    assert deucalion["info"]["active_job_ids_by_profile"]["cpu"] == [cpu_one["job_id"]]
    assert deucalion["info"]["active_job_ids_by_profile"]["gpu"] == [gpu_one["job_id"]]
    limits = deucalion["info"]["partition_limits"]
    normal_x86 = next(row for row in limits["partitions"] if row["partition"] == "normal-x86")
    assert limits["source"].startswith("https://docs.deucalion.macc.fccn.pt/")
    assert normal_x86["time_limit_seconds"] == 48 * 60 * 60


def test_launch_deucalion_rejects_walltime_above_partition_limit():
    settings.AVAILABLE_HOSTS = ["deucalion"]

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(
                    config={"experiment": {"name": "Deucalion", "run_name": "TooLong"}},
                    target_host="deucalion",
                    deucalion_options={"partition": "normal-x86", "time": "49:00:00"},
                )
            )
        )

    assert exc.value.status_code == 400
    assert "48 hours" in str(exc.value.detail)


def test_launch_deucalion_allows_large_partition_walltime():
    settings.AVAILABLE_HOSTS = ["deucalion"]

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "Large"}},
                target_host="deucalion",
                deucalion_options={"partition": "large-x86", "time": "72:00:00"},
            )
        )
    )

    dispatched = job_service.agent_next_job("deucalion")
    assert dispatched is not None
    assert dispatched["job_id"] == result["job_id"]
    assert dispatched["deucalion_options"]["partition"] == "large-x86"
    assert dispatched["deucalion_options"]["time"] == "72:00:00"


def test_launch_deucalion_rejects_unknown_partition():
    settings.AVAILABLE_HOSTS = ["deucalion"]

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(
                    config={"experiment": {"name": "Deucalion", "run_name": "BadPartition"}},
                    target_host="deucalion",
                    deucalion_options={"partition": "debug-x86", "time": "01:00:00"},
                )
            )
        )

    assert exc.value.status_code == 400
    assert "Unknown Deucalion partition" in str(exc.value.detail)


def test_launch_writes_resolved_config_with_container_dataset_path():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    config_path = Path(settings.CONFIGS_DIR) / "legacy-dataset.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "metadata": {"experiment_name": "Dataset", "run_name": "Path"},
                "simulator": {
                    "dataset_name": "demo_dataset",
                    "dataset_path": "./datasets/demo_dataset/schema.json",
                },
            }
        )
    )

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config_path="legacy-dataset.yaml", target_host="worker-a")
        )
    )
    job_id = result["job_id"]

    resolved_path = Path(settings.JOBS_DIR) / job_id / "config.resolved.yaml"
    assert resolved_path.exists()
    resolved = yaml.safe_load(resolved_path.read_text())
    assert resolved["simulator"]["dataset_path"] == "/data/datasets/demo_dataset/schema.json"

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["config_path"] == f"jobs/{job_id}/config.resolved.yaml"
    assert dispatched["source_config_path"] == "configs/legacy-dataset.yaml"
    assert f"--config /data/jobs/{job_id}/config.resolved.yaml" in dispatched["command"]


def test_launch_adds_container_dataset_path_when_missing():
    settings.AVAILABLE_HOSTS = ["worker-a"]

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={
                    "metadata": {"experiment_name": "Dataset", "run_name": "MissingPath"},
                    "simulator": {"dataset_name": "generated_dataset"},
                },
                target_host="worker-a",
            )
        )
    )
    job_id = result["job_id"]

    resolved = yaml.safe_load((Path(settings.JOBS_DIR) / job_id / "config.resolved.yaml").read_text())
    assert resolved["simulator"]["dataset_path"] == "/data/datasets/generated_dataset/schema.json"


def test_get_status_remote_uses_file():
    job_id = "job-remote"
    job_service.jobs[job_id] = {
        "target_host": "remote1",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    resp = job_service.get_status(job_id)
    assert resp["status"] == JobStatus.DISPATCHED.value


def test_record_host_heartbeat_enforces_known_hosts():
    settings.AVAILABLE_HOSTS = ["local", "worker-a"]

    job_service.record_host_heartbeat("worker-a", {"gpu": True})
    assert "worker-a" in job_service.host_heartbeats

    with pytest.raises(HTTPException) as exc:
        job_service.record_host_heartbeat("ghost", {})
    assert exc.value.status_code == 400


def test_list_jobs_reports_latest_status(monkeypatch):
    settings.MLFLOW_UI_BASE_URL = "https://mlflow-ui.example"
    job_id = "job-list"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "Demo",
        "config_path": "configs/demo.yaml",
        "target_host": "remote",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})
    info = {
        "job_id": job_id,
        "job_name": "Demo",
        "config_path": "configs/demo.yaml",
        "target_host": "remote",
        "run_id": "run-1",
        "experiment_id": "exp-1",
    }
    (job_dir / "job_info.json").write_text(json.dumps(info))

    result = job_service.list_jobs()
    [entry] = result
    assert entry["status"] == JobStatus.RUNNING.value
    assert entry["job_info"]["job_name"] == "Demo"
    assert entry["job_info"]["mlflow_run_url"] == "https://mlflow-ui.example/#/experiments/exp-1/runs/run-1"
    assert "queue_wait_seconds" in entry
    assert "run_duration_seconds" in entry
    assert "total_duration_seconds" in entry
    assert "job_meta" in entry
    assert entry["job_meta"]["job_id"] == job_id


def test_get_result_exposes_simulation_data_metadata():
    job_id = "job-with-results"
    base = Path(settings.JOBS_DIR) / job_id / "results"
    sim_session = base / "simulation_data" / "session-01"
    sim_session.mkdir(parents=True, exist_ok=True)
    (base / "result.json").write_text(json.dumps({"evaluation": {"kpis": {"score": 1.2}}}))
    (sim_session / "exported_kpis.csv").write_text("timestamp,kpi,value\n2024-08-01T00:00:00Z,score,1.2\n")

    payload = job_service.get_result(job_id)
    assert payload["simulation_data_available"] is True
    assert payload["simulation_data_session_default"] == "session-01"
    assert payload["kpi_source"] == "simulation_data/exported_kpis.csv"
    assert "simulation_data_dir" in payload


def test_list_jobs_contains_lifecycle_timestamps():
    job_id = "job-lifecycle"
    now = time.time()
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "status": JobStatus.RUNNING.value,
        "submitted_at": now - 120,
        "queued_at": now - 115,
        "dispatched_at": now - 100,
        "started_at": now - 90,
        "last_status_at": now - 10,
        "attempt_number": 2,
        "requeue_count": 1,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}))
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {"last_status_at": now - 10})

    [entry] = [item for item in job_service.list_jobs() if item["job_id"] == job_id]
    assert entry["submitted_at"] is not None
    assert entry["started_at"] is not None
    assert entry["queue_wait_seconds"] is not None
    assert entry["run_duration_seconds"] is not None
    assert entry["attempt_number"] == 2
    assert entry["requeue_count"] == 1


def test_get_job_info_adds_mlflow_run_url_when_base_url_is_configured():
    settings.MLFLOW_UI_BASE_URL = "https://mlflow-ui.example"
    job_id = "job-mlflow-url"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    info = {
        "job_id": job_id,
        "run_id": "run-9",
        "experiment_id": "exp-3",
    }
    (job_dir / "job_info.json").write_text(json.dumps(info))

    result = job_service.get_job_info(job_id)
    assert result["mlflow_run_id"] == "run-9"
    assert result["mlflow_experiment_id"] == "exp-3"
    assert result["mlflow_run_url"] == "https://mlflow-ui.example/#/experiments/exp-3/runs/run-9"


def test_get_job_info_keeps_backward_compat_when_base_url_missing():
    settings.MLFLOW_UI_BASE_URL = None
    job_id = "job-mlflow-no-url"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    info = {
        "job_id": job_id,
        "mlflow_run_id": "run-44",
        "mlflow_experiment_id": "exp-12",
    }
    (job_dir / "job_info.json").write_text(json.dumps(info))

    result = job_service.get_job_info(job_id)
    assert result["mlflow_run_id"] == "run-44"
    assert result["mlflow_experiment_id"] == "exp-12"
    assert "mlflow_run_url" not in result


def test_get_job_info_includes_lifecycle_durations():
    job_id = "job-info-lifecycle"
    now = time.time()
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "status": JobStatus.FINISHED.value,
        "submitted_at": now - 300,
        "queued_at": now - 290,
        "dispatched_at": now - 260,
        "started_at": now - 240,
        "finished_at": now - 40,
        "last_status_at": now - 40,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}))

    result = job_service.get_job_info(job_id)
    assert result["started_at"] == job_service.jobs[job_id]["started_at"]
    assert result["finished_at"] == job_service.jobs[job_id]["finished_at"]
    assert result["run_duration_seconds"] is not None
    assert result["run_duration_seconds"] > 0
    assert result["total_duration_seconds"] is not None
    assert result["total_duration_seconds"] > 0


def test_queue_wait_uses_backend_lifecycle_not_slurm_details():
    job_id = "job-lifecycle-queuewait"
    now = time.time()
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "LifecycleQueueWait",
        "config_path": "configs/demo.yaml",
        "target_host": "deucalion",
        "status": JobStatus.DISPATCHED.value,
        "queued_at": now - 120,
        "dispatched_at": now - 90,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}))
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    job_service.agent_update_status(
        job_id,
        JobStatus.FAILED.value,
        {
            "worker_id": "deucalion",
            "details": {
                "slurm_submit_time": "2026-04-04T10:00:00Z",
                "slurm_start_time": "2026-04-04T10:02:30Z",
            },
        },
    )

    [entry] = [item for item in job_service.list_jobs() if item["job_id"] == job_id]
    assert entry["queue_wait_seconds"] == pytest.approx(120.0, rel=0.0, abs=1.0)
    assert entry["job_meta"]["started_at"] is not None
    assert entry["job_meta"]["queued_at"] is not None


def test_queue_wait_ends_when_leaving_dispatched_if_running_missing():
    job_id = "job-queuewait-fallback"
    now = time.time()
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "QueueWaitFallback",
        "config_path": "configs/demo.yaml",
        "target_host": "deucalion",
        "status": JobStatus.DISPATCHED.value,
        "queued_at": now - 30,
        "dispatched_at": now - 10,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}))
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    job_service.agent_update_status(
        job_id,
        JobStatus.FAILED.value,
        {"worker_id": "deucalion"},
    )

    [entry] = [item for item in job_service.list_jobs() if item["job_id"] == job_id]
    assert entry["queue_wait_seconds"] is not None
    assert entry["queue_wait_seconds"] == pytest.approx(30.0, rel=0.0, abs=1.0)
    assert entry["job_meta"]["started_at"] is not None


def test_launch_defaults_to_first_host():
    settings.AVAILABLE_HOSTS = ["remoteA", "remoteB"]

    config_path = Path(settings.CONFIGS_DIR) / "auto.yaml"
    config_path.write_text(yaml.safe_dump({"experiment": {"name": "Auto", "run_name": "Remote"}}))

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config_path="auto.yaml")
        )
    )
    assert result["host"] is None


def test_agent_skips_jobs_for_other_hosts(monkeypatch):
    settings.AVAILABLE_HOSTS = ["worker-a", "worker-b"]

    config_payload = {"experiment": {"name": "Pref", "run_name": "One"}}
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config=config_payload, target_host="worker-a")
        )
    )
    job_id = result["job_id"]
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    # Worker-b should skip the job because it requires worker-a
    assert job_service.agent_next_job("worker-b") is None
    assert queue_file.exists()

    # Worker-a can claim it
    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert not queue_file.exists()
    assert dispatched["image"] == settings.DEFAULT_JOB_IMAGE
    assert "--job_id" in dispatched["command"]


def test_launch_with_custom_image_is_dispatched_to_worker():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    image_tag = "sha-customv2"
    expected_image = f"{settings.JOB_IMAGE_REPOSITORY}:{image_tag}"
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Image", "run_name": "Custom"}},
                target_host="worker-a",
                image_tag=image_tag,
            )
        )
    )

    job_id = result["job_id"]
    assert result["image_tag"] == image_tag
    assert result["image"] == expected_image
    assert job_service.jobs[job_id]["image"] == expected_image
    assert job_service.jobs[job_id]["image_tag"] == image_tag

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert dispatched["image"] == expected_image
    assert dispatched["image_tag"] == image_tag

    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["image"] == expected_image
    assert info["image_tag"] == image_tag


def test_deucalion_does_not_pick_unpinned_jobs():
    settings.AVAILABLE_HOSTS = ["worker-a", "deucalion"]

    config_payload = {"experiment": {"name": "Unpinned", "run_name": "Shared"}}
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config=config_payload)
        )
    )
    job_id = result["job_id"]
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    # Deucalion only accepts jobs explicitly targeted to deucalion.
    assert job_service.agent_next_job("deucalion") is None
    assert queue_file.exists()

    # Generic workers can still consume unpinned jobs.
    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert not queue_file.exists()


def test_deucalion_picks_only_explicit_deucalion_jobs():
    settings.AVAILABLE_HOSTS = ["worker-a", "deucalion"]

    config_payload = {"experiment": {"name": "Pinned", "run_name": "Deucalion"}}
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config=config_payload, target_host="deucalion")
        )
    )
    job_id = result["job_id"]
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    # Non-target worker cannot take a deucalion-pinned job.
    assert job_service.agent_next_job("worker-a") is None
    assert queue_file.exists()

    dispatched = job_service.agent_next_job("deucalion")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert not queue_file.exists()


def test_launch_rejects_executor_specific_config():
    settings.AVAILABLE_HOSTS = ["local", "deucalion"]
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(
                    config={"execution": {"deucalion": {"partition": "normal-x86"}}},
                    target_host="local",
                )
            )
        )
    assert exc.value.status_code == 400
    assert "executor-specific" in str(exc.value.detail)


def test_launch_deucalion_options_are_dispatched_to_deucalion_worker():
    settings.AVAILABLE_HOSTS = ["deucalion"]
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "Opts"}},
                target_host="deucalion",
                image_tag="sha-opt123",
                deucalion_options={
                    "partition": "normal-x86",
                    "cpus_per_task": 8,
                    "datasets": ["datasets/demo.csv"],
                    "command_mode": "run",
                },
            )
        )
    )
    job_id = result["job_id"]
    dispatched = job_service.agent_next_job("deucalion")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert dispatched["image_tag"] == "sha-opt123"
    assert dispatched["deucalion_options"]["partition"] == "normal-x86"
    assert dispatched["deucalion_options"]["cpus_per_task"] == 8
    assert dispatched["deucalion_options"]["datasets"] == ["datasets/demo.csv"]


def test_deucalion_dispatch_uses_local_mlflow_tracking_uri():
    settings.AVAILABLE_HOSTS = ["deucalion", "worker-a"]
    settings.MLFLOW_TRACKING_URI = "http://193.136.62.78:5000"
    settings.DEUCALION_MLFLOW_TRACKING_URI = "file:/data/mlflow/mlruns"

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Deucalion", "run_name": "MLflowLocal"}},
                target_host="deucalion",
                image_tag="sha-mlflowlocal",
            )
        )
    )

    job_id = result["job_id"]
    dispatched = job_service.agent_next_job("deucalion")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert dispatched["env"]["MLFLOW_TRACKING_URI"] == "file:/data/mlflow/mlruns"

    # Non-deucalion workers still receive the global tracking URI.
    result_server = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(
                config={"experiment": {"name": "Server", "run_name": "MLflowRemote"}},
                target_host="worker-a",
                image_tag="sha-mlflowremote",
            )
        )
    )
    dispatched_server = job_service.agent_next_job("worker-a")
    assert dispatched_server is not None
    assert dispatched_server["job_id"] == result_server["job_id"]
    assert dispatched_server["env"]["MLFLOW_TRACKING_URI"] == "http://193.136.62.78:5000"


def test_agent_flow_updates_status_and_info():
    settings.AVAILABLE_HOSTS = ["local", "worker-a"]
    job_id = "job-agent"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "AgentJob",
        "config_path": "configs/demo.yaml",
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
        "experiment_name": "Experiment",
        "run_name": "Run",
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_utils.save_job_info(
        job_id,
        "AgentJob",
        "configs/demo.yaml",
        "worker-a",
        "",
        "",
        "Experiment",
        "Run",
    )
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    job_utils.enqueue_job({
        "job_id": job_id,
        "preferred_host": "worker-a",
    })

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched["job_id"] == job_id

    status_data = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_data["status"] == JobStatus.DISPATCHED.value
    assert status_data["worker_id"] == "worker-a"
    assert job_service.jobs[job_id]["status"] == JobStatus.DISPATCHED.value
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["target_host"] == "worker-a"

    job_service.agent_update_status(
        job_id,
        JobStatus.RUNNING.value,
        {
            "worker_id": "worker-a",
            "container_id": "cid-123",
            "container_name": "cname",
        },
    )

    status_after = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_after["status"] == JobStatus.RUNNING.value
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["container_id"] == "cid-123"
    assert info["container_name"] == "cname"

    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["container_id"] == "cid-123"
    assert track[job_id]["status"] == JobStatus.RUNNING.value

    job_service.agent_update_status(
        job_id,
        JobStatus.FINISHED.value,
        {
            "worker_id": "worker-a",
            "exit_code": 0,
        },
    )

    status_final = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_final["status"] == JobStatus.FINISHED.value
    assert status_final["exit_code"] == 0
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["exit_code"] == 0
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["exit_code"] == 0
    assert track[job_id]["status"] == JobStatus.FINISHED.value


def test_list_queue_returns_entries(tmp_path, jobs_env):
    from app.config import settings
    from app.utils import job_utils

    settings.AVAILABLE_HOSTS = ["worker-a"]

    payload = {"job_id": "job-queued", "preferred_host": "worker-a"}
    job_utils.enqueue_job(payload)

    entries = job_service.list_queue()
    assert len(entries) == 1
    assert entries[0]["job_id"] == payload["job_id"]
    assert entries[0]["preferred_host"] == payload["preferred_host"]
    assert entries[0]["require_host"] is True
    assert isinstance(entries[0].get("enqueued_at"), (int, float))


def test_host_heartbeat_reporting(monkeypatch):
    settings.AVAILABLE_HOSTS = ["local", "worker-hb"]

    now = 1_000.0
    monkeypatch.setattr(job_service.time, "time", lambda: now)
    job_service.record_host_heartbeat("worker-hb", {"load": 0.5})

    hosts = job_service.get_hosts()["hosts"]
    assert hosts["worker-hb"]["online"] is True
    assert hosts["worker-hb"]["info"]["load"] == 0.5

    monkeypatch.setattr(job_service.time, "time", lambda: now + job_service.HEARTBEAT_TTL + 5)
    hosts = job_service.get_hosts()["hosts"]
    assert hosts["worker-hb"]["online"] is False


def test_agent_status_updates_host_runtime_version():
    settings.AVAILABLE_HOSTS = ["worker-a"]

    job_id = "job-worker-version"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.DISPATCHED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    resp = job_service.agent_update_status(
        job_id,
        JobStatus.RUNNING.value,
        {"worker_id": "worker-a", "worker_version": "0.4.1"},
    )

    assert resp["ok"] is True
    hosts = job_service.get_hosts()["hosts"]
    info = hosts["worker-a"]["info"]
    assert info["worker_version"] == "0.4.1"
    assert info["last_status_worker_version"] == "0.4.1"
    assert info["last_status"] == JobStatus.RUNNING.value
    assert info["last_status_job_id"] == job_id


def test_stop_job_local(monkeypatch):
    job_id = "job-stop-local"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "local",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    resp = job_service.stop_job(job_id)
    assert "Stop requested" in resp["message"]
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.STOP_REQUESTED.value


def test_stop_job_remote_removes_queue():
    job_id = "job-stop-remote"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-b",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    payload = {
        "job_id": job_id,
        "preferred_host": "worker-b",
        "require_host": True,
    }
    job_utils.enqueue_job(payload)
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    resp = job_service.stop_job(job_id)
    assert "canceled" in resp["message"].lower()
    assert not queue_file.exists()
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.CANCELED.value


def test_stop_job_running_requests_and_worker_stops():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    job_id = "job-stop-requested"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    resp = job_service.stop_job(job_id)
    assert "Stop requested" in resp["message"]
    status_poll = job_service.get_status(job_id)
    assert status_poll["status"] == JobStatus.STOP_REQUESTED.value

    job_service.agent_update_status(
        job_id,
        JobStatus.STOPPED.value,
        {"worker_id": "worker-a"},
    )
    status_final = json.loads((job_dir / "status.json").read_text())
    assert status_final["status"] == JobStatus.STOPPED.value
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.STOPPED.value


def test_agent_update_status_rejects_unknown_job():
    with pytest.raises(HTTPException) as exc:
        job_service.agent_update_status("missing-job", JobStatus.RUNNING.value, {"worker_id": "worker-a"})
    assert exc.value.status_code == 404


def test_agent_update_status_rejects_invalid_status():
    job_id = "job-invalid-status"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    with pytest.raises(HTTPException) as exc:
        job_service.agent_update_status(job_id, "not-a-status", {"worker_id": "worker-a"})
    assert exc.value.status_code == 400


def test_agent_update_status_blocks_invalid_transition():
    job_id = "job-bad-transition"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    with pytest.raises(HTTPException) as exc:
        job_service.agent_update_status(job_id, JobStatus.RUNNING.value, {"worker_id": "worker-a"})
    assert exc.value.status_code == 409

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.QUEUED.value


def test_agent_update_status_idempotent():
    job_id = "job-idempotent"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    resp = job_service.agent_update_status(job_id, JobStatus.RUNNING.value, {"worker_id": "worker-a"})
    assert resp["ok"] is True
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.RUNNING.value


def test_mark_stale_dispatched_requeues(monkeypatch):
    settings.AVAILABLE_HOSTS = ["worker-a"]
    monkeypatch.setattr(settings, "JOB_STATUS_TTL", 1)

    job_id = "job-stale-dispatched"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "preferred_host": "worker-a",
        "require_host": True,
        "status": JobStatus.DISPATCHED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stale_ts = time.time() - 10
    (job_dir / "status.json").write_text(
        json.dumps({"job_id": job_id, "status": JobStatus.DISPATCHED.value, "status_updated_at": stale_ts})
    )

    job_service._mark_stale_jobs()

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.QUEUED.value
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.QUEUED.value


def test_mark_stale_running_fails(monkeypatch):
    settings.AVAILABLE_HOSTS = ["worker-a"]
    monkeypatch.setattr(settings, "JOB_STATUS_TTL", 1)

    job_id = "job-stale-running"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stale_ts = time.time() - 10
    (job_dir / "status.json").write_text(
        json.dumps({"job_id": job_id, "status": JobStatus.RUNNING.value, "status_updated_at": stale_ts})
    )

    job_service._mark_stale_jobs()

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.FAILED.value
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.FAILED.value


def test_mark_stale_deucalion_dispatched_pending_is_preserved(monkeypatch):
    settings.AVAILABLE_HOSTS = ["deucalion"]
    monkeypatch.setattr(settings, "JOB_STATUS_TTL", 1)
    monkeypatch.setattr(settings, "DEUCALION_DISPATCH_STATUS_TTL", 1)

    job_id = "job-deucalion-pending"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "deucalion",
        "preferred_host": "deucalion",
        "require_host": True,
        "status": JobStatus.DISPATCHED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stale_ts = time.time() - 10
    (job_dir / "status.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "status": JobStatus.DISPATCHED.value,
                "status_updated_at": stale_ts,
                "details": {"slurm_state": "PENDING"},
            }
        )
    )
    job_service.host_heartbeats["deucalion"] = {
        "last_seen": time.time(),
        "info": {"active_job_id": job_id},
    }

    job_service._mark_stale_jobs()

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.DISPATCHED.value
    assert not (Path(settings.QUEUE_DIR) / f"{job_id}.json").exists()
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.DISPATCHED.value


def test_mark_stale_deucalion_running_is_preserved_when_heartbeat_reports_active(monkeypatch):
    settings.AVAILABLE_HOSTS = ["deucalion"]
    monkeypatch.setattr(settings, "JOB_STATUS_TTL", 1)

    job_id = "job-deucalion-running"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "deucalion",
        "preferred_host": "deucalion",
        "require_host": True,
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stale_ts = time.time() - 10
    (job_dir / "status.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "status": JobStatus.RUNNING.value,
                "status_updated_at": stale_ts,
            }
        )
    )
    job_service.host_heartbeats["deucalion"] = {
        "last_seen": time.time(),
        "info": {
            "active_job_ids": [job_id],
            "active_jobs": [{"job_id": job_id, "slurm_state": "RUNNING"}],
        },
    }

    job_service._mark_stale_jobs()

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.RUNNING.value
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.RUNNING.value


def test_ops_requeue_dispatched():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    job_id = "job-ops-requeue"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "preferred_host": "worker-a",
        "require_host": True,
        "status": JobStatus.DISPATCHED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    resp = job_service.ops_requeue_job(job_id)
    assert resp["status"] == JobStatus.QUEUED.value
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.QUEUED.value
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()


def test_ops_requeue_running_requires_force():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    job_id = "job-ops-requeue-running"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    with pytest.raises(HTTPException) as exc:
        job_service.ops_requeue_job(job_id)
    assert exc.value.status_code == 409

    resp = job_service.ops_requeue_job(job_id, force=True)
    assert resp["status"] == JobStatus.QUEUED.value
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.QUEUED.value


def test_ops_fail_and_cancel():
    settings.AVAILABLE_HOSTS = ["worker-a"]

    fail_id = "job-ops-fail"
    job_service.jobs[fail_id] = {
        "job_id": fail_id,
        "target_host": "worker-a",
        "status": JobStatus.DISPATCHED.value,
    }
    job_utils.save_job(fail_id, job_service.jobs[fail_id])
    fail_dir = Path(settings.JOBS_DIR) / fail_id
    fail_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(fail_id, JobStatus.DISPATCHED.value, {})

    resp = job_service.ops_fail_job(fail_id, reason="ops_fail")
    assert resp["status"] == JobStatus.FAILED.value
    status_data = json.loads((fail_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.FAILED.value
    assert status_data["error"] == "ops_fail"

    cancel_id = "job-ops-cancel"
    job_service.jobs[cancel_id] = {
        "job_id": cancel_id,
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(cancel_id, job_service.jobs[cancel_id])
    cancel_dir = Path(settings.JOBS_DIR) / cancel_id
    cancel_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(cancel_id, JobStatus.QUEUED.value, {})
    job_utils.enqueue_job({
        "job_id": cancel_id,
        "preferred_host": "worker-a",
        "require_host": True,
    })

    resp = job_service.ops_cancel_job(cancel_id, reason="ops_cancel")
    assert resp["status"] == JobStatus.CANCELED.value
    status_data = json.loads((cancel_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.CANCELED.value
    assert status_data["error"] == "ops_cancel"
    queue_file = Path(settings.QUEUE_DIR) / f"{cancel_id}.json"
    assert not queue_file.exists()


def test_ops_requeue_failed_job_without_force():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    job_id = "job-ops-requeue-failed"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "preferred_host": "worker-a",
        "status": JobStatus.FAILED.value,
        "error": "boom",
        "exit_code": 2,
        "container_id": "cid-old",
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.FAILED.value, {"error": "boom"})
    (job_dir / "job_info.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "job_name": "RetryMe",
                "config_path": "configs/retry.yaml",
                "target_host": "worker-a",
                "container_id": "cid-old",
                "error": "boom",
            }
        )
    )

    resp = job_service.ops_requeue_job(job_id)
    assert resp["status"] == JobStatus.QUEUED.value

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.QUEUED.value
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()
    refreshed = json.loads(Path(settings.JOB_TRACK_FILE).read_text())[job_id]
    assert "error" not in refreshed
    assert "container_id" not in refreshed

    info = json.loads((job_dir / "job_info.json").read_text())
    assert "error" not in info
    assert "container_id" not in info


def test_ops_stop_job_sets_reason():
    settings.AVAILABLE_HOSTS = ["worker-a"]
    job_id = "job-ops-stop"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    resp = job_service.ops_stop_job(job_id, reason="manual_stop")
    assert resp["status"] == JobStatus.STOP_REQUESTED.value

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.STOP_REQUESTED.value
    assert status_data["stop_reason"] == "manual_stop"
    assert status_data["stopped_by_ops"] is True


def test_ops_cleanup_queue_removes_orphans():
    missing_id = "job-missing"
    job_utils.enqueue_job({
        "job_id": missing_id,
        "preferred_host": None,
        "require_host": False,
    })
    queue_file = Path(settings.QUEUE_DIR) / f"{missing_id}.json"
    assert queue_file.exists()

    resp = job_service.ops_cleanup_queue()
    assert missing_id in resp["removed"]
    assert not queue_file.exists()


def test_ops_cleanup_queue_force_removes_all():
    job_id = "job-queued"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])
    job_utils.enqueue_job({
        "job_id": job_id,
        "preferred_host": None,
        "require_host": False,
    })
    claim_path = Path(settings.QUEUE_DIR) / f"{job_id}.json.claim.worker-a"
    claim_path.write_text(json.dumps({"job_id": job_id}))

    resp = job_service.ops_cleanup_queue(force=True)
    assert job_id in resp["removed"]
    assert not any(Path(settings.QUEUE_DIR).iterdir())


def test_ops_cleanup_jobs_prunes_registry():
    keep_id = "keep-job"
    remove_id = "remove-job"
    for job_id in (keep_id, remove_id, "sample_job"):
        job_service.jobs[job_id] = {
            "job_id": job_id,
            "target_host": "worker-a",
            "status": JobStatus.QUEUED.value if job_id == keep_id else JobStatus.FINISHED.value,
        }
        job_utils.save_job(job_id, job_service.jobs[job_id])

    job_utils.enqueue_job({
        "job_id": remove_id,
        "preferred_host": None,
        "require_host": False,
    })

    resp = job_service.ops_cleanup_jobs(keep=[keep_id])
    assert remove_id in resp["removed"]
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert keep_id in track
    assert "sample_job" in track
    assert remove_id not in track
    assert remove_id not in job_service.jobs
    assert not (Path(settings.QUEUE_DIR) / f"{remove_id}.json").exists()


def test_ops_cleanup_jobs_removes_job_dirs_and_orphans():
    keep_id = "keep-dir-job"
    removed_id = "remove-dir-job"
    orphan_id = "orphan-dir-job"

    for job_id in (keep_id, removed_id):
        job_service.jobs[job_id] = {
            "job_id": job_id,
            "target_host": "worker-a",
            "status": JobStatus.QUEUED.value if job_id == keep_id else JobStatus.FINISHED.value,
        }
        job_utils.save_job(job_id, job_service.jobs[job_id])

    keep_dir = Path(settings.JOBS_DIR) / keep_id
    remove_dir = Path(settings.JOBS_DIR) / removed_id
    orphan_dir = Path(settings.JOBS_DIR) / orphan_id
    for directory in (keep_dir, remove_dir, orphan_dir):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "status.json").write_text(json.dumps({"status": JobStatus.QUEUED.value}))

    resp = job_service.ops_cleanup_jobs(keep=[keep_id])

    assert removed_id in resp["removed"]
    assert removed_id in resp["removed_dirs"]
    assert orphan_id in resp["orphan_removed"]
    assert keep_id not in resp["removed"]
    assert keep_dir.exists()
    assert not remove_dir.exists()
    assert not orphan_dir.exists()


def test_ops_cleanup_jobs_keeps_active_jobs():
    active_id = "active-running-job"
    finished_id = "finished-job"

    job_service.jobs[active_id] = {
        "job_id": active_id,
        "target_host": "worker-a",
        "status": JobStatus.RUNNING.value,
    }
    job_service.jobs[finished_id] = {
        "job_id": finished_id,
        "target_host": "worker-a",
        "status": JobStatus.FINISHED.value,
    }
    job_utils.save_job(active_id, job_service.jobs[active_id])
    job_utils.save_job(finished_id, job_service.jobs[finished_id])

    active_dir = Path(settings.JOBS_DIR) / active_id
    finished_dir = Path(settings.JOBS_DIR) / finished_id
    for directory, status in ((active_dir, JobStatus.RUNNING.value), (finished_dir, JobStatus.FINISHED.value)):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "status.json").write_text(json.dumps({"status": status}))

    resp = job_service.ops_cleanup_jobs()

    assert active_id in resp["active_kept"]
    assert active_id not in resp["removed"]
    assert finished_id in resp["removed"]
    assert active_dir.exists()
    assert not finished_dir.exists()


def test_delete_job_removes_artifacts():
    job_id = "job-delete"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "remote",
        "status": JobStatus.QUEUED.value,
    }
    entry = {
        job_id: job_service.jobs[job_id]
    }
    Path(settings.JOB_TRACK_FILE).write_text(json.dumps(entry))

    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "status.json").write_text(json.dumps({"status": JobStatus.QUEUED.value}))

    resp = job_service.delete_job(job_id)
    assert "deleted" in resp["message"]
    assert not job_dir.exists()
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert job_id not in track
    assert job_id not in job_service.jobs


def test_delete_job_returns_500_when_filesystem_removal_fails(monkeypatch):
    job_id = "job-delete-fails"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "remote",
        "status": JobStatus.QUEUED.value,
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])

    monkeypatch.setattr(job_utils, "delete_job_by_id", lambda _job_id: False)

    with pytest.raises(HTTPException) as exc:
        job_service.delete_job(job_id)
    assert exc.value.status_code == 500
    assert job_id in job_service.jobs
