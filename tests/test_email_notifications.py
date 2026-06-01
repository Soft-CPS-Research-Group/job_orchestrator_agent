import json
import sys
from pathlib import Path
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


@pytest.mark.parametrize(
    ("submitted_by", "display_name", "recipient"),
    [
        ("pedro monteiro", "Pedro Monteiro", "1211076@isep.ipp.pt"),
        ("Pedro Alves Monteiro", "Pedro Alves Monteiro", "1211076@isep.ipp.pt"),
        ("pedro.monteiro@energaize.io", "Pedro Alves Monteiro", "1211076@isep.ipp.pt"),
        ("Gustavo Nuno Chaves Jorge", "Gustavo Nuno Chaves Jorge", "1211061@isep.ipp.pt"),
        ("gustavo.jorge@energaize.io", "Gustavo Nuno Chaves Jorge", "1211061@isep.ipp.pt"),
    ],
)
def test_known_ui_submitters_use_isep_recipients(submitted_by, display_name, recipient):
    message = email_notification_service.build_job_status_email(
        job_id="job-1",
        status=JobStatus.QUEUED.value,
        previous_status=JobStatus.LAUNCHING.value,
        job={
            "job_name": "Queued demo",
            "submitted_by": submitted_by,
        },
    )

    assert email_notification_service.normalize_submitted_by(submitted_by) == display_name
    assert message is not None
    assert message["to"] == [recipient]
    assert display_name in message["body"]


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

    status_payload = job_service.get_status(job_id)
    email_record = status_payload["last_email_notification"]
    assert email_record["outcome"] == "published"
    assert email_record["recipients"] == ["calof@isep.ipp.pt"]
    assert len(status_payload["email_notifications"]) == 1

    tracked = job_utils.load_jobs()[job_id]
    assert tracked["last_email_notification"]["subject"] == "[EnergAIze] Job queued: Queued demo"


def test_email_notifications_stay_out_of_jobs_table_payload(monkeypatch, jobs_env):
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFICATIONS_ENABLED", True)
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFY_STATUSES", [JobStatus.QUEUED.value])
    monkeypatch.setattr(email_notification_service, "_publish_email_request", lambda message: None)

    job_id = "job-1"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "Queued demo",
        "status": JobStatus.LAUNCHING.value,
        "submitted_by": "Tiago Fonseca",
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])

    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job_info.json").write_text(
        json.dumps({"job_id": job_id, "job_name": "Queued demo"}),
        encoding="utf-8",
    )

    job_service._write_status(job_id, JobStatus.QUEUED.value)

    [entry] = [item for item in job_service.list_jobs() if item["job_id"] == job_id]
    assert "last_email_notification" not in entry["job_info"]
    assert "email_notifications" not in entry["job_info"]
    assert "last_email_notification" not in entry["job_meta"]
    assert "email_notifications" not in entry["job_meta"]

    details = job_service.get_job_info(job_id)
    status = job_service.get_status(job_id)
    assert details["last_email_notification"]["outcome"] == "published"
    assert len(details["email_notifications"]) == 1
    assert status["last_email_notification"]["outcome"] == "published"


def test_write_status_records_failed_email_publish(monkeypatch, jobs_env):
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFICATIONS_ENABLED", True)
    monkeypatch.setattr(settings, "JOB_EMAIL_NOTIFY_STATUSES", [JobStatus.QUEUED.value])

    def fail_publish(_message):
        raise RuntimeError("rabbit down")

    monkeypatch.setattr(email_notification_service, "_publish_email_request", fail_publish)

    job_id = "job-1"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "Queued demo",
        "status": JobStatus.LAUNCHING.value,
        "submitted_by": "Tiago Fonseca",
    }
    job_utils.save_job(job_id, job_service.jobs[job_id])

    job_service._write_status(job_id, JobStatus.QUEUED.value)

    email_record = job_service.get_status(job_id)["last_email_notification"]
    assert email_record["attempted"] is True
    assert email_record["outcome"] == "failed"
    assert email_record["recipients"] == ["calof@isep.ipp.pt"]
    assert "rabbit down" in email_record["error"]


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
    monkeypatch.setattr(settings, "JOB_EMAIL_RABBITMQ_USERNAME", None)
    monkeypatch.setattr(settings, "JOB_EMAIL_RABBITMQ_PASSWORD", None)

    email_notification_service._publish_email_request({"to": ["tiago@energaize.io"], "subject": "test", "body": "test"})

    connection_call = next(payload for name, payload in calls if name == "connection")
    assert "credentials" not in connection_call["params"]["connection"]
    assert ("queue_declare", {"queue": "email_requests", "durable": True}) in calls
    assert ("confirm_delivery", {}) in calls
    publish_call = next(payload for name, payload in calls if name == "basic_publish")
    assert publish_call["exchange"] == ""
    assert publish_call["routing_key"] == "email_requests"
    assert publish_call["mandatory"] is True
