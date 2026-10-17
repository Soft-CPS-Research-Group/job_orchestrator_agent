"""Real PostgreSQL contract tests; use a dedicated disposable database."""
import os
from concurrent.futures import ThreadPoolExecutor
import pytest
from sqlalchemy import text
from app.config import settings
from app.utils import sql_store

@pytest.mark.skipif(not os.getenv('TEST_JOB_DATABASE_URL'), reason='Dedicated PostgreSQL test database not configured')
def test_postgres_concurrent_claims_and_restart(monkeypatch):
    monkeypatch.setattr(settings,'JOB_DATABASE_URL',os.environ['TEST_JOB_DATABASE_URL'])
    sql_store.initialize()
    with sql_store.engine().begin() as c:
        c.execute(sql_store.queue.delete());c.execute(sql_store.registry.delete())
    for i in range(30):
        sql_store.save_job(str(i),{'status':'queued','workspace_id':'test'})
        sql_store.enqueue({'job_id':str(i),'enqueued_at':float(i)})
    def consume(worker):
        result=[]
        while (entry:=sql_store.claim(worker,lambda _:True)) is not None:
            result.append(entry['job_id'])
        return result
    with ThreadPoolExecutor(max_workers=5) as pool:
        claimed=[jid for group in pool.map(consume,[str(i) for i in range(5)]) for jid in group]
    assert len(claimed)==len(set(claimed))==30
    sql_store.engine().dispose()
    assert len(sql_store.load_jobs())==30
    assert sql_store.list_queue()==[]
    for jid in claimed:sql_store.remove_queue(jid)
