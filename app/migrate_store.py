"""Rehearse first: python -m app.migrate_store --source /data [--apply]."""
import argparse
import json
from pathlib import Path
from sqlalchemy import select
from app.utils import sql_store


def migrate(source,apply=False):
    source=Path(source)
    jobs=json.loads((source/'job_track.json').read_text())
    if not isinstance(jobs,dict):raise ValueError('Invalid job registry')
    entries=[]
    for path in sorted((source/'queue').glob('*.json')):
        entry=json.loads(path.read_text())
        if entry.get('job_id') not in jobs:raise ValueError('Queue contains an unknown job: '+str(entry.get('job_id')))
        entries.append(entry)
    claims=list((source/'queue').glob('*.claim.*'))
    if claims:raise ValueError('Queue has active claims. Quiesce dispatch before final migration.')
    org_path=source/'jobs'/'.organization.json'
    org=json.loads(org_path.read_text()) if org_path.exists() else {'schema_version':1,'folders':{},'jobs':{}}
    report={'mode':'apply' if apply else 'dry-run','jobs':len(jobs),'queue':len(entries),'folders':len(org['folders']),
            'active_jobs':sum(j.get('status') in ('running','dispatched','setup','recovering','stop_requested') for j in jobs.values())}
    if apply:
        if not sql_store.enabled():raise ValueError('JOB_DATABASE_URL is required')
        sql_store.initialize()
        # One transaction; never overwrite an existing, possibly newer execution.
        with sql_store.engine().begin() as c:
            for jid,payload in jobs.items():
                old=c.scalar(select(sql_store.registry.c.payload).where(sql_store.registry.c.job_id==jid))
                if old is not None and old!=payload:raise ValueError('Conflicting existing job: '+jid)
                if old is None:
                    sql_store.upsert(c,sql_store.registry,{'job_id':jid,'workspace_id':payload.get('workspace_id') or 'laboratory',
                        'status':payload.get('status'),'submitted_at':payload.get('submitted_at'),'payload':payload},'job_id')
            for p in entries:
                old=c.scalar(select(sql_store.queue.c.payload).where(sql_store.queue.c.job_id==p['job_id']))
                if old is not None and old!=p:raise ValueError('Conflicting queue entry: '+p['job_id'])
                if old is None:
                    sql_store.upsert(c,sql_store.queue,{'job_id':p['job_id'],'enqueued_at':p['enqueued_at'],
                        'claimed_by':None,'claimed_at':None,'payload':p},'job_id')
            current=c.scalar(select(sql_store.organization.c.payload).where(sql_store.organization.c.id=='personal'))
            if current.get('folders') or current.get('jobs'):
                if current!=org:raise ValueError('Conflicting organization; do not overwrite live SQL data')
            else:c.execute(sql_store.organization.update().where(sql_store.organization.c.id=='personal').values(payload=org))
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True);parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();print(json.dumps(migrate(args.source,args.apply),indent=2))
