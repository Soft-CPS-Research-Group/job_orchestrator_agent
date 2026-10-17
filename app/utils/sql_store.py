"""Optional PostgreSQL execution store. File artefacts and agent contracts stay stable."""
from contextlib import contextmanager
from functools import lru_cache
import time
from sqlalchemy import create_engine, MetaData, Table, Column, String, JSON, Float, Integer, select, delete, text
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from app.config import settings

metadata=MetaData()
document=JSON().with_variant(JSONB(),'postgresql')
registry=Table('orchestrator_jobs',metadata,
    Column('job_id',String(100),primary_key=True),Column('workspace_id',String(100),nullable=False,index=True),
    Column('status',String(40),index=True),Column('submitted_at',Float,index=True),Column('payload',document,nullable=False))
queue=Table('orchestrator_queue',metadata,
    Column('job_id',String(100),primary_key=True),Column('enqueued_at',Float,nullable=False,index=True),
    Column('claimed_by',String(160)),Column('claimed_at',Float),Column('payload',document,nullable=False))
organization=Table('orchestrator_organization',metadata,
    Column('id',String(40),primary_key=True),Column('payload',document,nullable=False))
schema=Table('orchestrator_schema',metadata,Column('version',Integer,primary_key=True))

def enabled():return bool(settings.JOB_DATABASE_URL)

@lru_cache(maxsize=4)
def _engine(url):
    return create_engine(url,pool_pre_ping=True,**({'connect_args':{'check_same_thread':False}} if url.startswith('sqlite') else {}))

def engine():return _engine(settings.JOB_DATABASE_URL)

def upsert(c,table,values,key):
    insert=pg_insert if c.dialect.name=='postgresql' else sqlite_insert
    stmt=insert(table).values(**values)
    c.execute(stmt.on_conflict_do_update(index_elements=[key],set_={k:getattr(stmt.excluded,k) for k in values if k!=key}))

def initialize():
    _initialize(settings.JOB_DATABASE_URL)

@lru_cache(maxsize=4)
def _initialize(url):
    db = _engine(url)
    metadata.create_all(db)
    with db.begin() as c:
        insert=pg_insert if c.dialect.name=='postgresql' else sqlite_insert
        c.execute(insert(schema).values(version=1).on_conflict_do_nothing())
        c.execute(insert(organization).values(id='personal',payload={'schema_version':1,'folders':{},'jobs':{}}).on_conflict_do_nothing())

def load_jobs():
    with engine().connect() as c:
        return dict(c.execute(select(registry.c.job_id,registry.c.payload)).all())

def save_job(job_id,payload):
    with engine().begin() as c:
        upsert(c,registry,{'job_id':job_id,'workspace_id':payload.get('workspace_id') or 'laboratory',
            'status':payload.get('status'),'submitted_at':payload.get('submitted_at'),'payload':payload},'job_id')

def delete_job(job_id):
    with engine().begin() as c:
        c.execute(delete(queue).where(queue.c.job_id==job_id))
        return c.execute(delete(registry).where(registry.c.job_id==job_id)).rowcount>0

def prune_jobs(keep):
    with engine().begin() as c:
        removed=list(c.scalars(select(registry.c.job_id).where(registry.c.job_id.not_in(keep))))
        c.execute(delete(queue).where(queue.c.job_id.in_(removed)))
        c.execute(delete(registry).where(registry.c.job_id.in_(removed)))
        return removed

def enqueue(payload):
    entry=dict(payload);entry.setdefault('enqueued_at',time.time())
    with engine().begin() as c:
        upsert(c,queue,{'job_id':entry['job_id'],'enqueued_at':entry['enqueued_at'],
                       'claimed_at':None,'claimed_by':None,'payload':entry},'job_id')

def list_queue():
    with engine().connect() as c:
        return list(c.scalars(select(queue.c.payload).where(queue.c.claimed_by.is_(None)).order_by(queue.c.enqueued_at)))

def claim(worker_id,can_accept):
    now=time.time()
    with engine().begin() as c:
        c.execute(queue.update().where(queue.c.claimed_at<now-settings.QUEUE_CLAIM_TTL).values(claimed_by=None,claimed_at=None))
        rows=c.execute(select(queue).where(queue.c.claimed_by.is_(None)).order_by(queue.c.enqueued_at).with_for_update(skip_locked=True)).mappings()
        for row in rows:
            p=row['payload'];preferred=p.get('preferred_host');required=p.get('require_host',bool(preferred))
            if worker_id=='deucalion' and (preferred!=worker_id or not required):continue
            if preferred and preferred!=worker_id and required:continue
            if can_accept and not can_accept(p):continue
            c.execute(queue.update().where(queue.c.job_id==row['job_id']).values(claimed_by=worker_id,claimed_at=now))
            return p
    return None

def remove_queue(job_id):
    with engine().begin() as c:return c.execute(delete(queue).where(queue.c.job_id==job_id)).rowcount>0

def release(job_id):
    with engine().begin() as c:c.execute(queue.update().where(queue.c.job_id==job_id).values(claimed_by=None,claimed_at=None))

@contextmanager
def organization_store(write):
    with engine().begin() as c:
        stmt=select(organization.c.payload).where(organization.c.id=='personal')
        if write:stmt=stmt.with_for_update()
        value=c.scalar(stmt)
        yield value
        if write:c.execute(organization.update().where(organization.c.id=='personal').values(payload=value))

@contextmanager
def advisory_lock(key):
    if not enabled() or engine().dialect.name!='postgresql':
        yield;return
    with engine().begin() as c:
        c.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),{'key':key})
        yield
