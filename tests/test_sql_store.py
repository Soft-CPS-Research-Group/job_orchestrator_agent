import json
import time
import pytest
from app.config import settings
from app.utils import sql_store,job_utils
from app.migrate_store import migrate

@pytest.fixture
def sql(tmp_path,monkeypatch):
    monkeypatch.setattr(settings,'JOB_DATABASE_URL','sqlite:///'+str(tmp_path/'state.db'))
    sql_store.initialize();return tmp_path

def test_registry_preserves_payload_and_scope(sql):
    payload={'status':'running','workspace_id':'community:one','attempt_number':3,'details':{'a':1}}
    job_utils.save_job('a',payload)
    assert job_utils.load_jobs()=={'a':payload}

def test_claim_is_reserved_until_ack_and_stale_claim_recovers(sql):
    job_utils.enqueue_job({'job_id':'a','preferred_host':'server','require_host':True})
    assert job_utils.agent_pop_next_job('wrong') is None
    assert job_utils.agent_pop_next_job('server')['job_id']=='a'
    assert job_utils.agent_pop_next_job('server') is None
    with sql_store.engine().begin() as c:
        c.execute(sql_store.queue.update().values(claimed_at=time.time()-settings.QUEUE_CLAIM_TTL-1))
    assert job_utils.agent_pop_next_job('server')['job_id']=='a'
    assert job_utils.remove_from_queue('a')
    assert job_utils.agent_pop_next_job('server') is None

def test_rejected_worker_does_not_consume_queue(sql):
    job_utils.enqueue_job({'job_id':'a'})
    assert job_utils.agent_pop_next_job('server',lambda _:False) is None
    assert job_utils.agent_pop_next_job('server',lambda _:True)['job_id']=='a'

def test_import_is_idempotent_and_refuses_conflicts(sql):
    (sql/'queue').mkdir();(sql/'jobs').mkdir()
    (sql/'job_track.json').write_text(json.dumps({'a':{'status':'finished'}}))
    assert migrate(sql)['jobs']==1
    assert job_utils.load_jobs()=={}
    migrate(sql,True);migrate(sql,True)
    assert len(job_utils.load_jobs())==1
    job_utils.save_job('a',{'status':'running'})
    with pytest.raises(ValueError,match='Conflicting'):migrate(sql,True)
    assert job_utils.load_jobs()['a']['status']=='running'
