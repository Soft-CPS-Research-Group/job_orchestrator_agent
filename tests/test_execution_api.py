import hashlib
import json
from pathlib import Path

import pytest

from test_api_jobs import api_client  # noqa: F401
from app.config import settings
from app.services import execution_inputs


@pytest.fixture
def execution(api_client, tmp_path, monkeypatch):
    source = tmp_path / 'source'
    (source / 'dataset').mkdir(parents=True)
    (source / 'dataset/schema.json').write_text('{"version":1}')
    clients = tmp_path / 'clients.json'
    clients.write_text(json.dumps({
        name: {'token_sha256': hashlib.sha256(name.encode()).hexdigest(),
               'namespaces': ['lab'], 'input_root': str(source)}
        for name in ('one', 'two')
    }))
    monkeypatch.setattr(settings, 'EXECUTION_CLIENTS_FILE', str(clients))
    monkeypatch.setattr(settings, 'JOB_EMAIL_NOTIFICATIONS_ENABLED', False)
    api_client.headers['Authorization'] = 'Bearer one'
    return api_client, source


def submit(client, **updates):
    body = {'namespace': 'lab', 'idempotency_key': 'same-logical-run',
            'config': {'metadata': {'experiment_name': 'standalone', 'run_name': 'test'}}}
    body.update(updates)
    return client.post('/api/execution/v1/jobs', json=body)


def test_generic_client_identity_isolated_and_legacy_cannot_bypass(execution):
    client, _ = execution
    first = submit(client)
    assert first.status_code == 200, first.text
    jid = first.json()['job_id']
    assert submit(client).json()['job_id'] == jid
    assert submit(client, namespace='foreign').status_code == 403
    assert submit(client, job_name='different').status_code == 409
    assert client.get('/api/execution/v1/status/' + jid).status_code == 200
    client.headers['Authorization'] = 'Bearer two'
    assert client.get('/api/execution/v1/jobs').json() == []
    assert client.get('/api/execution/v1/status/' + jid).status_code == 404
    assert client.post('/api/execution/v1/stop/' + jid).status_code == 404
    assert client.get('/job-info/' + jid).status_code == 404
    assert client.get('/jobs').json() == []
    assert submit(client).json()['job_id'] != jid
    del client.headers['Authorization']
    assert client.get('/api/execution/v1/jobs').status_code == 401
    assert client.get('/result/' + jid).status_code == 401
    # Existing worker control plane remains independent of client credentials.
    assert client.get('/api/agent/status/' + jid).status_code == 200


def test_inputs_are_verified_materialized_and_reject_traversal(execution):
    client, source = execution
    checksum = execution_inputs.digest(execution_inputs.inventory(source / 'dataset'))
    body = {'relative_path': 'dataset', 'sha256': checksum}
    response = client.post('/api/execution/v1/inputs', json=body)
    assert response.status_code == 200, response.text
    reference = response.json()
    config = {'simulator': {'dataset_name': reference['name'], 'dataset_path': reference['schema_path']}}
    assert submit(client, config=config, inputs=[{k: reference[k] for k in ('name', 'sha256')}]).status_code == 200
    (source / 'dataset/schema.json').write_text('changed after snapshot')
    target = Path(settings.DATASETS_DIR) / reference['name'] / 'schema.json'
    assert target.read_text() == '{"version":1}'
    assert client.post('/api/execution/v1/inputs', json=body).json() == reference
    assert submit(client, config={'simulator': {'dataset_path': '/etc/passwd'}}).status_code == 400
    for path in ('../dataset', '/etc', 'dataset/../dataset'):
        assert client.post('/api/execution/v1/inputs', json={**body, 'relative_path': path}).status_code == 400
    client.headers['Authorization'] = 'Bearer two'
    assert submit(client, config=config, inputs=[{k: reference[k] for k in ('name', 'sha256')}]).status_code == 403
    client.headers['Authorization'] = 'Bearer one'
    target.chmod(0o644)
    target.write_text('tampered')
    assert submit(client, config=config, inputs=[{k: reference[k] for k in ('name', 'sha256')}]).status_code == 409


def test_image_is_frozen_once_and_outputs_are_authorized(execution, monkeypatch):
    from app.services import execution_images, job_service
    client, _ = execution
    monkeypatch.setattr(settings, 'EXECUTION_PIN_IMAGES', True)
    pinned = 'calof/opeva_simulator@sha256:' + 'a' * 64
    calls = []
    def freeze(request):
        calls.append(request.image_tag)
        return {'server': pinned}
    monkeypatch.setattr(execution_images, 'freeze', freeze)
    jid = submit(client, target_host='server').json()['job_id']
    assert submit(client, target_host='server').json()['job_id'] == jid
    assert len(calls) == 1
    job_service._refresh_jobs()
    assert job_service.jobs[jid]['execution']['images'] == {'server': pinned}
    assert not job_service._can_dispatch_to_worker('other', {'job_id': jid})
    result = Path(settings.JOBS_DIR) / jid / 'results/result.json'
    result.parent.mkdir(parents=True, exist_ok=True)
    result.write_text('{"status":"finished"}')
    files = client.get('/api/execution/v1/artifacts/' + jid).json()['files']
    entry = next(row for row in files if row['path'] == 'results/result.json')
    assert entry['sha256'] == hashlib.sha256(result.read_bytes()).hexdigest()
    assert client.get('/api/execution/v1/artifacts/' + jid + '/results/result.json').status_code == 200
    client.headers['Authorization'] = 'Bearer two'
    assert client.get('/api/execution/v1/artifacts/' + jid).status_code == 404
