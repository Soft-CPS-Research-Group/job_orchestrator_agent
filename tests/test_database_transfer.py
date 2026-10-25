"""Destructive only to explicitly supplied disposable TEST databases."""
import os
import pytest
from sqlalchemy import create_engine, text
from app.utils import sql_store
from app.utils.database_transfer import transfer


@pytest.mark.skipif(not os.getenv("TEST_TRANSFER_DATABASE_URL"), reason="Dedicated PostgreSQL pair required")
def test_copy_preserves_all_state_and_refuses_different_destination():
    source = create_engine(os.environ["TEST_JOB_DATABASE_URL"])
    target = create_engine(os.environ["TEST_TRANSFER_DATABASE_URL"])
    assert source.url.database.startswith("orchestrator_")
    assert target.url.database.startswith("orchestrator_")
    assert source.url != target.url
    try:
        for engine in (source, target):
            sql_store.metadata.drop_all(engine)
            sql_store.metadata.create_all(engine)
        with source.begin() as conn:
            conn.execute(sql_store.registry.insert(), {
                "job_id": "historical", "workspace_id": "community:one", "status": "queued",
                "payload": {"job_id": "historical", "attempt_number": 3,
                            "training_context": {"config_version": 2}}})
            conn.execute(sql_store.queue.insert(), {"job_id": "historical", "enqueued_at": 1,
                         "claimed_by": "worker", "claimed_at": 2, "payload": {"job_id": "historical"}})
            conn.execute(sql_store.submissions.insert(), {"id": "key", "payload": {"job_id": "historical"}})
            conn.execute(sql_store.organization.insert(), {"id": "personal", "payload": {"folders": {"f": "example"}}})
            conn.execute(sql_store.schema.insert(), {"version": 1})
        first = transfer(source, target, copy=True)
        assert len(first) == 5
        assert transfer(source, target, copy=True) == first
        source.dispose(); target.dispose()
        assert transfer(source, target) == first
        with target.begin() as conn:
            conn.execute(text("UPDATE orchestrator_jobs SET status='finished'"))
        with pytest.raises(ValueError, match="refusing to overwrite"):
            transfer(source, target, copy=True)
        with pytest.raises(ValueError, match="verification failed"):
            transfer(source, target)
    finally:
        source.dispose(); target.dispose()
