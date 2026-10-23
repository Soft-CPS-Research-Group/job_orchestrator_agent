import asyncio
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from app.config import settings
from app.models.job import JobLaunchRequest
from app.services import job_service
from app.utils import job_utils, sql_store, submission_store


@pytest.fixture(autouse=True, params=['file', 'sql'] + (['postgres'] if os.getenv('TEST_JOB_DATABASE_URL') else []))
def isolated(tmp_path, monkeypatch, request):
    monkeypatch.setattr(settings, 'VM_SHARED_DATA', str(tmp_path))
    for name, folder in [('CONFIGS_DIR', 'configs'), ('JOBS_DIR', 'jobs'), ('DATASETS_DIR', 'datasets'), ('QUEUE_DIR', 'queue')]:
        monkeypatch.setattr(settings, name, str(tmp_path / folder))
    monkeypatch.setattr(settings, 'JOB_TRACK_FILE', str(tmp_path / 'job_track.json'))
    url = os.environ['TEST_JOB_DATABASE_URL'] if request.param == 'postgres' else 'sqlite:///' + str(tmp_path / 'registry.db') if request.param == 'sql' else ''
    monkeypatch.setattr(settings, 'JOB_DATABASE_URL', url)
    monkeypatch.setattr(settings, 'AVAILABLE_HOSTS', ['server'])
    job_service.jobs.clear()
    job_utils.ensure_directories()
    if request.param == 'postgres':
        with sql_store.engine().begin() as connection:
            for table in reversed(sql_store.metadata.sorted_tables):
                connection.execute(table.delete())
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


def test_retry_isolates_artifacts_from_a_revoked_worker(monkeypatch):
    from app.utils import attempt_artifacts
    capabilities=[job_service.ATTEMPT_FENCING_CAPABILITY,attempt_artifacts.CAPABILITY]
    jid=launch()['job_id']
    first=job_service.agent_next_job('server',capabilities)
    assert first['artifact_protocol']==attempt_artifacts.CAPABILITY
    first_root=Path(settings.VM_SHARED_DATA)/first['artifact_job_dir']
    (first_root/'bundle/model.onnx').write_bytes(b'old')
    fence=lambda job:{'worker_id':'server','attempt_number':job['attempt_number'],'attempt_token':job['attempt_token']}
    job_service.agent_update_status(jid,'running',fence(first))
    job_service.ops_requeue_job(jid,force=True)
    # A worker without isolated-output support cannot downgrade this job.
    assert job_service.agent_next_job('server',[job_service.ATTEMPT_FENCING_CAPABILITY]) is None
    second=job_service.agent_next_job('server',capabilities)
    second_root=Path(settings.VM_SHARED_DATA)/second['artifact_job_dir']
    assert first_root!=second_root
    assert not (second_root/'bundle/model.onnx').exists()
    (second_root/'bundle/model.onnx').write_bytes(b'new')
    (first_root/'bundle/model.onnx').write_bytes(b'late stale write')
    assert (Path(settings.JOBS_DIR)/jid/'bundle/model.onnx').read_bytes()==b'new'
    with pytest.raises(HTTPException) as stale:
        job_service.agent_update_status(jid,'finished',fence(first))
    assert stale.value.status_code==409
    job_service.agent_update_status(jid,'running',fence(second))
    job_service.agent_update_status(jid,'finished',fence(second))
    assert job_service.get_status(jid)['status']=='finished'
    assert (first_root/'bundle/model.onnx').exists()  # Evidence is retained.


def test_attempt_preparation_can_resume_before_dispatch_is_committed():
    from app.utils import attempt_artifacts
    jid=launch()['job_id']
    first=attempt_artifacts.prepare(jid,1)
    assert attempt_artifacts.prepare(jid,1)==first


def test_training_context_reaches_runtime_config_without_mutating_source():
    import yaml
    result=launch(training_context={'community_id':'rec','datasets':[], 'configuration':{'metadata':{'run_name':'original'}}},workspace_id='community:rec')
    jid=result['job_id']
    runtime=yaml.safe_load((Path(settings.JOBS_DIR)/jid/'config.resolved.yaml').read_text())
    provenance=runtime['tracking']['tags']['energaize_training']
    assert provenance['job_id']==jid
    assert provenance['community_id']=='rec'
    assert 'configuration' not in provenance
