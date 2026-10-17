# SQL execution storage — 1.1.1

Set JOB_DATABASE_URL to use PostgreSQL. Leave it unset for the existing JSON mode.
Metadata, leased queue entries and organization are stored in SQL; logs, checkpoints,
status files and dataset/config artefacts retain their existing paths and worker API.

Rehearse with `python -m app.migrate_store --source /path/to/shared` and apply with
`--apply`. The importer refuses conflicts and active file claims; rerunning an
unchanged snapshot is safe. Startup refuses an empty SQL registry when legacy jobs
exist. Database outages do not silently fall back to an old in-memory registry.

Dispatch uses PostgreSQL advisory locks and SKIP LOCKED reservations. Resolved
configuration snapshots and workspace provenance accompany each new job.
`WORKER_SHARED_DATA_HOST` explicitly sets the Docker daemon's shared-data path when
it differs from the orchestrator's container mount, preventing candidate jobs from
mounting production data.

Tests: 204 passed, including a real PostgreSQL test with 30 jobs and five concurrent
claimants, no duplicate claims, and persistence after reconnect. Production cutover
must quiesce submission/dispatch and reconcile active jobs; an old JSON snapshot is
not a safe rollback after new SQL executions without exporting/reconciling state.
