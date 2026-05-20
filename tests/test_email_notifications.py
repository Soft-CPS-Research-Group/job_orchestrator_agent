import json

import pytest

from app.config import settings
from app.services import email_notification_service, job_service
from app.status import JobStatus
from app.utils import job_utils


@pytest.fixture()
def jobs_env(monkeypatch, tmp_path):
    base = tmp_path / "shared"
    configs = base / "configs"
    jobs_dir = base / "jobs"
    datasets = base / "datasets"
    queue = base / "queue"
    for folder in (configs, jobs_dir, datasets, queue):
        folder.mkdir(parents=True, exist_ok=True)
    job_track = base / "job_track.json"
    job_track.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(settings, "VM_SHARED_DATA", str(base))
    monkeypatch.setattr(settings, "CONFIGS_DIR", str(configs))
    monkeypatch.setattr(settings, "JOBS_DIR", str(jobs_dir))
    monkeypatch.setattr(settings, "DATASETS_DIR", str(datasets))
    monkeypatch.setattr(settings, "QUEUE_DIR", str(queue))
    monkeypatch.setattr(settings, "JOB_TRACK_FILE", str(job_track))

    job_utils.settings = settings
    job_service.settings = settings
    job_service.job_utils.settings = settings
    job_service.jobs.clear()
    yield
    job_service.jobs.clear()


def test_codex_submitter_builds_tiago_email_with_ui_link(monkeypatch):
    monkeypatch.setattr(settings, "UI_BASE_URL", "https://ui.example")
    monkeypatch.setattr(settings, "UI_LINK_NETWORK_NOTICE", "VPN required.")

    job = {
        "job_name": "Demo job",
        "submitted_by": "codex",
        "target_host": "deucalion",
        "config_path": "configs/demo.yaml",
        "image_tag": "sha-1234567",
    }

    message = email_notification_service.build_job_status_email(
        job_id="job 1",
        status=JobStatus.FAILED.value,
        previous_status=JobStatus.RUNNING.value,
        job=job,
    )

    assert email_notification_service.normalize_submitted_by("codex") == "Tiago Fonseca"
    assert message is not None
    assert message["to"] == ["calof@isep.ipp.pt"]
    assert message["subject"] == "[EnergAIze] Job failed: Demo job"
    assert "Tiago Fonseca" in message["body"]
    assert "https://ui.example/app/ai/jobs/job%201" in message["body"]
    assert "VPN required." in message["body"]
    assert "VPN required." in message["html_body"]
    assert "Abrir job na UI" in message["html_body"]


def test_write_status_publishes_once_for_real_transition(monkeypatch, jobs_env):
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFICATIONS_ENABLED", True)
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFY_STATUSES", [JobStatus.QUEUED.value])
    monkeypatch.setattr(settings, "UI_BASE_URL", "https://ui.example")

    published: list[dict] = []
    monkeypatch.setattr(email_notification_service, "_publish_email_request", lambda message: published.append(message))

    job_id = "job-1"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "Queued demo",
        "status": JobStatus.LAUNCHING.value,
        "submitted_by": "Tiago Fonseca",
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])

    job_service._write_status(job_id, JobStatus.QUEUED.value)
    job_service._write_status(job_id, JobStatus.QUEUED.value, {"details": {"ignored": True}})

    assert len(published) == 1
    payload = json.loads(json.dumps(published[0]))
    assert payload["to"] == ["calof@isep.ipp.pt"]
    assert payload["subject"] == "[EnergAIze] Job queued: Queued demo"
