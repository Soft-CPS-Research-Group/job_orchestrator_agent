import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from app.config import settings
from app.models.job import JobLaunchRequest
from app.services import job_service
from app.utils import job_utils, sql_store, submission_store


@pytest.fixture(autouse=True, params=['file', 'sql'])
def isolated(tmp_path, monkeypatch, request):
    monkeypatch.setattr(settings, 'VM_SHARED_DATA', str(tmp_path))
    for name, folder in [('CONFIGS_DIR', 'configs'), ('JOBS_DIR', 'jobs'), ('DATASETS_DIR', 'datasets'), ('QUEUE_DIR', 'queue')]:
        monkeypatch.setattr(settings, name, str(tmp_path / folder))
    monkeypatch.setattr(settings, 'JOB_TRACK_FILE', str(tmp_path / 'job_track.json'))
    monkeypatch.setattr(settings, 'JOB_DATABASE_URL', 'sqlite:///' + str(tmp_path / 'registry.db') if request.param == 'sql' else '')
    monkeypatch.setattr(settings, 'AVAILABLE_HOSTS', ['server'])
    job_service.jobs.clear()
    job_utils.ensure_directories()
    yield
    job_service.jobs.clear()


def launch(**kwargs):
    values = dict(config={'experiment': {'name': 'test'}}, target_host='server', idempotency_key='repeated-request', submitted_by='test')
    values.update(kwargs)
    return asyncio.run(job_service.launch_simulation(JobLaunchRequest(**values)))


def test_concurrent_retries_and_restart_keep_one_job():
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: launch(), range(12)))
    assert len({r['job_id'] for r in results}) == 1
    assert len(job_utils.load_jobs()) == len(job_service.list_queue()) == 1
    if sql_store.enabled():
        sql_store.engine().dispose()
    job_service.jobs.clear()
    assert launch()['job_id'] == results[0]['job_id']


def test_key_conflict_and_workspace_isolation():
    original = launch()
    with pytest.raises(HTTPException) as error:
        launch(job_name='different')
    assert error.value.status_code == 409
    assert launch(workspace_id='community:other')['job_id'] != original['job_id']


def test_lost_response_does_not_launch_again(monkeypatch):
    write = submission_store.write
    def lose_response(key, payload):
        if payload.get('response'):
            raise ConnectionError('response lost after queue commit')
        return write(key, payload)
    monkeypatch.setattr(submission_store, 'write', lose_response)
    with pytest.raises(ConnectionError):
        launch()
    jid = next(iter(job_utils.load_jobs()))
    monkeypatch.setattr(submission_store, 'write', write)
    assert launch()['job_id'] == jid
    assert len(job_service.list_queue()) == 1


def test_interrupted_launch_resumes_with_same_identity(monkeypatch):
    enqueue = job_utils.enqueue_job
    monkeypatch.setattr(job_utils, 'enqueue_job', lambda *_: (_ for _ in ()).throw(ConnectionError('queue offline')))
    with pytest.raises(ConnectionError):
        launch()
    jid = next(iter(job_utils.load_jobs()))
    monkeypatch.setattr(job_utils, 'enqueue_job', enqueue)
    assert launch()['job_id'] == jid
    assert len(job_service.list_queue()) == 1
