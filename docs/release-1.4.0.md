# 1.4.0 — reusable execution boundary (SOF-231)

This is a compatible server release. Legacy execution remains available;
`/api/execution/v1` adds authenticated clients, authorized namespaces, immutable
shared-root input snapshots, client-scoped durable idempotency and artifact
listing/download with SHA-256. Inputs are checked again before dispatch. New job
configuration lives in the job directory, not the legacy configuration catalog.

`EXECUTION_CLIENTS_FILE` names an operator-owned JSON file mapping client IDs to
`token_sha256`, `namespaces` (explicit names, or `*` for the Training integration),
optional `input_root`, and optional `operator` for worker administration. Bearer
tokens never appear in request bodies. Each input root is an explicit grant, not
a path selected by the caller. Input registration accepts a relative directory
and SHA-256 of the canonical JSON map of relative filename → file SHA-256.
Snapshot names and hashes are returned by `POST /api/execution/v1/inputs`.

`POST /api/execution/v1/jobs` accepts `namespace`, `idempotency_key`, resolved
`config`, optional opaque `source`, `inputs` (name/hash), job name, submitter,
image tag and destination/profile. No workspace/community lookup occurs here.
The existing `workspace_id` SQL column is retained as an opaque namespace for
historical storage compatibility. `EXECUTION_LEGACY_OWNER` explicitly grants the
pre-existing execution records to the migration's Training client.

Enable `EXECUTION_PIN_IMAGES=true` in production to freeze CPU/Union image
manifests before committing submission intent. Automatic selection only uses
workers with a frozen variant. Deucalion/SIF and Jetson retain their legacy
transport and are outside this release's acceptance scope. Local diagnostic
images can use the unpinned development mode; do not mistake that for production.

Status/result/progress/log and job operations retain their paths below the v1
prefix. `GET /artifacts/{job_id}` lists hashes and relative names;
`GET /artifacts/{job_id}/{relative}` retrieves an authorized file. Catalog,
personal folders and global destructive cleanup are not execution API functions.
The legacy URL cannot expose a new client's job, configuration or cached inputs.
Worker control endpoints remain the existing trusted deployment control plane;
this release does not replace the separately deferred network/admin hardening.

## Clients and compatibility

Training 0.5.0 uses `TRAINING_EXECUTION_API_VERSION=v1` and its configured token.
Already-frozen legacy submissions keep their old transport and identity until
resolved, so a previously accepted request cannot be submitted a second time.
Workers 0.6.1 use `WORKER_EXECUTION_API_V1=1` for cancellation/status polling via
the worker control plane. Install server support before enabling the worker flag.
Legacy workers remain compatible with historical jobs.

For generic scripts, set `EXECUTION_URL` and `EXECUTION_TOKEN`, then use
`python -m app.execution_cli submit request.json` (or inputs/status/result/
artifacts/cancel). This client imports no Training code or database model.

`TRAINING_CATALOG_URL` and `TRAINING_CATALOG_TOKEN` turn old dataset/configuration
URLs into a temporary adapter to Training's private catalog endpoint. Without
these settings the old standalone mode remains available for compatibility.
Execution routes never call this adapter. Keep it until operator consumers have
migrated; do not remove it based only on the UI migration.

## Storage migration

`python -m app.utils.database_transfer copy --writers-stopped` reads URLs from
`SOURCE_JOB_DATABASE_URL` and `DESTINATION_JOB_DATABASE_URL`. It copies only the
five Orchestrator tables into an empty destination (or verifies an identical
retry), without overwriting another database. `verify` compares primary-key-sorted
content digests. Stop all writers for the production copy, test a backup restore,
and use restricted database roles. Keep historical artifacts in place. After new
writes the new database is authoritative; never revert to stale JSON/backups.

Local validation includes the full unit suite, mandatory PostgreSQL migration and
queue tests, authorization/input integrity tests and the Training/HTTP/PostgreSQL/
real Docker worker drill. Installed revisions, restore/role checks and production
CPU/Union acceptance belong in the deployment receipt, not inferred from this note.
