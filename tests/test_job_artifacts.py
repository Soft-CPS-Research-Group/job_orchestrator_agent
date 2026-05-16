import sys
import json
import time
import os
from pathlib import Path

import pytest

# Ensure repository root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import job_service
from app.utils import file_utils
from app.status import JobStatus


def _use_example_job_dir(monkeypatch, tmp_path):
    """Redirect job directory to bundled examples for testing."""
    base = Path(__file__).resolve().parents[1] / "examples"
    job_track = tmp_path / "job_track.json"
    entries = {}
    for job_id in ("sample_job", "running_job", "failed_job", "queued_job"):
        entries[job_id] = {
            "job_id": job_id,
            "status": JobStatus.UNKNOWN.value,
            "target_host": "remote",
        }
    job_track.write_text(json.dumps(entries))
    monkeypatch.setattr(job_service.settings, "JOBS_DIR", str(base))
    monkeypatch.setattr(job_service.settings, "JOB_TRACK_FILE", str(job_track))
    monkeypatch.setattr(file_utils.settings, "JOBS_DIR", str(base))
    job_service.jobs.clear()
    return base


def test_result_progress_logs(monkeypatch, tmp_path):
    _use_example_job_dir(monkeypatch, tmp_path)
    job_id = "sample_job"

    res = job_service.get_result(job_id)
    assert res["status"] == "completed"

    prog = job_service.get_progress(job_id)
    assert prog["percent"] == 100

    logs = "".join(job_service.get_logs(job_id))
    assert "Simulation completed" in logs

    file_log = "".join(job_service.get_file_logs(job_id))
    assert "Simulation started" in file_log


def test_example_job_statuses(monkeypatch, tmp_path):
    _use_example_job_dir(monkeypatch, tmp_path)
    cases = [
        ("sample_job", "finished", 100),
        ("running_job", "running", 45),
        ("failed_job", "failed", 80),
        ("queued_job", "queued", 0),
    ]
    for job_id, expected_status, expected_percent in cases:
        status = job_service.get_status(job_id)
        assert status["status"] == expected_status
        prog = job_service.get_progress(job_id)
        assert prog["percent"] == expected_percent


def _use_temp_job_dir(monkeypatch, tmp_path, job_id: str, status: str = JobStatus.RUNNING.value):
    jobs_root = tmp_path / "jobs"
    jobs_root.mkdir(parents=True, exist_ok=True)
    queue_root = tmp_path / "queue"
    queue_root.mkdir(parents=True, exist_ok=True)
    job_track = tmp_path / "job_track.json"
    job_track.write_text(
        json.dumps(
            {
                job_id: {
                    "job_id": job_id,
                    "status": status,
                    "target_host": "server",
                }
            }
        )
    )

    monkeypatch.setattr(job_service.settings, "JOBS_DIR", str(jobs_root))
    monkeypatch.setattr(job_service.settings, "QUEUE_DIR", str(queue_root))
    monkeypatch.setattr(job_service.settings, "JOB_TRACK_FILE", str(job_track))
    monkeypatch.setattr(file_utils.settings, "JOBS_DIR", str(jobs_root))
    job_service.jobs.clear()
    job_service.jobs.update(job_service.job_utils.load_jobs())
    return jobs_root


def test_get_file_logs_prefers_merged_job_log(monkeypatch, tmp_path):
    job_id = "job-run-id-log"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.FINISHED.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_root / job_id / "job_info.json").write_text(json.dumps({"run_id": "run-123"}))
    (logs_dir / f"{job_id}.log").write_text("legacy-job-id-log\n")
    (logs_dir / "run-123.log").write_text("run-id-log-line\n")

    payload = "".join(job_service.get_file_logs(job_id))
    assert "legacy-job-id-log" in payload
    assert "run-id-log-line" not in payload


def test_get_file_logs_prefers_non_empty_candidate_when_canonical_is_empty(monkeypatch, tmp_path):
    job_id = "job-empty-canonical"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.RUNNING.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_root / job_id / "job_info.json").write_text(json.dumps({"run_id": "run-123"}))
    (logs_dir / f"{job_id}.log").write_text("", encoding="utf-8")
    (logs_dir / "run-123.log").write_text("run-id-live-log\n", encoding="utf-8")

    payload = "".join(job_service.get_file_logs(job_id))
    assert "run-id-live-log" in payload


def test_get_file_logs_uses_runtime_log_when_known_candidates_are_empty(monkeypatch, tmp_path):
    job_id = "job-runtime-fallback"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.RUNNING.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_root / job_id / "job_info.json").write_text(json.dumps({"run_id": "run-123"}))
    (logs_dir / f"{job_id}.log").write_text("", encoding="utf-8")
    (logs_dir / "run-123.log").write_text("", encoding="utf-8")
    runtime_log = logs_dir / "runtime.log"
    runtime_log.write_text("runtime-live-log\n", encoding="utf-8")

    payload = "".join(job_service.get_file_logs(job_id))
    assert "runtime-live-log" in payload


def test_get_logs_resolves_mlflow_run_id(monkeypatch, tmp_path):
    job_id = "job-mlflow-log"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.FINISHED.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_root / job_id / "job_info.json").write_text(json.dumps({"mlflow_run_id": "ml-456"}))
    (logs_dir / "ml-456.log").write_text("mlflow-run-log\n")

    payload = "".join(job_service.get_logs(job_id))
    assert "mlflow-run-log" in payload


def test_get_file_logs_falls_back_to_latest_log(monkeypatch, tmp_path):
    job_id = "job-latest-log"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.FINISHED.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    older = logs_dir / "a.log"
    newer = logs_dir / "b.log"
    older.write_text("older\n")
    newer.write_text("newer\n")
    now = time.time()
    older_ts = now - 10
    newer_ts = now
    older.touch()
    newer.touch()
    os.utime(older, (older_ts, older_ts))
    os.utime(newer, (newer_ts, newer_ts))

    payload = "".join(job_service.get_file_logs(job_id))
    assert payload.strip() == "newer"


def test_get_logs_active_job_without_file_returns_wait_message(monkeypatch, tmp_path):
    job_id = "job-active-no-log"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.RUNNING.value)
    (jobs_root / job_id).mkdir(parents=True, exist_ok=True)
    job_service.job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    payload = "".join(job_service.get_logs(job_id))
    assert "Logs not available yet" in payload


def test_get_logs_chunk_returns_tail_then_delta(monkeypatch, tmp_path):
    job_id = "job-chunk"
    jobs_root = _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.RUNNING.value)
    logs_dir = jobs_root / job_id / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"{job_id}.log"
    log_file.write_text("line-1\nline-2\nline-3\n", encoding="utf-8")

    first = job_service.get_logs_chunk(job_id, tail_lines=2, max_bytes=4096)
    assert first["available"] is True
    assert first["text"] == "line-2\nline-3\n"
    assert isinstance(first["next_offset"], int)

    with log_file.open("a", encoding="utf-8") as handle:
        handle.write("line-4\n")

    second = job_service.get_logs_chunk(job_id, offset=first["next_offset"], max_bytes=4096)
    assert second["available"] is True
    assert second["text"] == "line-4\n"
    assert second["next_offset"] > first["next_offset"]


def test_get_logs_chunk_active_job_without_file_returns_empty(monkeypatch, tmp_path):
    job_id = "job-active-no-log-chunk"
    _use_temp_job_dir(monkeypatch, tmp_path, job_id, JobStatus.RUNNING.value)
    job_service.job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    payload = job_service.get_logs_chunk(job_id)
    assert payload["available"] is False
    assert payload["text"] == ""
    assert "Logs not available yet" in payload["message"]
