import json
import sys
from types import SimpleNamespace

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


def test_email_submitter_is_used_as_recipient():
    message = email_notification_service.build_job_status_email(
        job_id="job-1",
        status=JobStatus.QUEUED.value,
        previous_status=JobStatus.LAUNCHING.value,
        job={
            "job_name": "Queued demo",
            "submitted_by": "tiago@energaize.io",
        },
    )

    assert message is not None
    assert message["to"] == ["tiago@energaize.io"]


def test_pedro_monteiro_submitter_uses_isep_recipient():
    message = email_notification_service.build_job_status_email(
        job_id="job-1",
        status=JobStatus.QUEUED.value,
        previous_status=JobStatus.LAUNCHING.value,
        job={
            "job_name": "Queued demo",
            "submitted_by": "pedro monteiro",
        },
    )

    assert email_notification_service.normalize_submitted_by("pedro monteiro") == "Pedro Monteiro"
    assert message is not None
    assert message["to"] == ["1211076@isep.ipp.pt"]
    assert "Pedro Monteiro" in message["body"]


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


def test_publish_email_request_declares_queue_and_requires_routing(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeUnroutableError(Exception):
        pass

    class FakeChannel:
        def queue_declare(self, **kwargs):
            calls.append(("queue_declare", kwargs))

        def confirm_delivery(self):
            calls.append(("confirm_delivery", {}))

        def basic_publish(self, **kwargs):
            calls.append(("basic_publish", kwargs))

    class FakeConnection:
        def __init__(self, params):
            calls.append(("connection", {"params": params}))

        def channel(self):
            return FakeChannel()

        def close(self):
            calls.append(("close", {}))

    fake_pika = SimpleNamespace(
        BasicProperties=lambda **kwargs: {"properties": kwargs},
        BlockingConnection=FakeConnection,
        ConnectionParameters=lambda **kwargs: {"connection": kwargs},
        PlainCredentials=lambda username, password: {"username": username, "password": password},
        exceptions=SimpleNamespace(UnroutableError=FakeUnroutableError),
    )
    monkeypatch.setitem(sys.modules, "pika", fake_pika)
    monkeypatch.setattr(settings, "JOB_EMAIL_RABBITMQ_QUEUE", "email_requests")

    email_notification_service._publish_email_request({"to": ["tiago@energaize.io"], "subject": "test", "body": "test"})

    assert ("queue_declare", {"queue": "email_requests", "durable": True}) in calls
    assert ("confirm_delivery", {}) in calls
    publish_call = next(payload for name, payload in calls if name == "basic_publish")
    assert publish_call["exchange"] == ""
    assert publish_call["routing_key"] == "email_requests"
    assert publish_call["mandatory"] is True
