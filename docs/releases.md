# Releases

This is the operational release log for `job_orchestrator_agent`. Every version bump should record what changed, user impact, storage/API impact, validation and migration notes.

Portuguese version: [pt/releases.md](pt/releases.md).

Default release owner: [@calofonseca](https://github.com/calofonseca).

## Version Policy

| Type | Use when | Example |
| --- | --- | --- |
| Patch | Compatible fixes and small additive changes. | `1.0.0 -> 1.0.1` |
| Minor | New compatible capability or endpoint group. | `1.0.x -> 1.1.0` |
| Major | Breaking API, storage or worker-contract change. | `1.x -> 2.0.0` |

## Release Checklist

1. Update `pyproject.toml`.
2. Update `app/version.py`.
3. Update this file with release notes.
4. Update `README.md`, Postman and endpoint docs when public contracts change.
5. Run validation:
   - `.venv/bin/pytest`
   - `python3 -m compileall app`
   - `docker build -t job_orchestrator_agent:test .`
   - `docker run --rm job_orchestrator_agent:test python -c "from app.main import app; print(app.version)"`
6. Validate infra compose when deployment env changes:
   - `docker compose -f ../opeva_infra_services/job_orchestrator_agent/docker-compose.yaml config`
7. Commit and tag.
8. Push `main` and tag `vX.Y.Z`.
9. Verify GitHub Actions pushed Docker tags:
   - `calof/job_orchestrator_agent:<commit-sha>`
   - `calof/job_orchestrator_agent:latest` from `main`
   - `calof/job_orchestrator_agent:vX.Y.Z` from release tags

## Template

```markdown
## vX.Y.Z - YYYY-MM-DD

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary
- ...

### Added
- ...

### Changed
- ...

### Fixed
- ...

### API/Storage Impact
- ...

### Compatibility
- ...

### Validation
- `...`: pass

### Migration Notes
- ...
```

## v1.3.0 - 2026-10-23

- Carries frozen training provenance into the runtime config through the existing
  tracking tags contract, including the logical job and execution attempt.
- Negotiates `attempt_artifacts_v1` with Docker workers (0.6.0+). Each new job's
  attempts write under `jobs/<id>/.attempts/<n>/jobs/<id>`. Relative symlinks keep
  the existing artifact API paths working while revoked writers retain only their
  own attempt's directory. Prior attempts and the submitted config are retained.
- Jobs using this capability cannot fall back to a worker lacking it. Jobs first
  dispatched with older workers keep their existing layout; active historical
  executions are not rewritten. Union/Deucalion contracts are unchanged.
- A periodic stale-job sweep runs every 15 seconds (configurable with
  `STALE_RECONCILIATION_INTERVAL`); existing heartbeat/status grace periods apply.
- Roll out the orchestrator before worker 0.6.0. Use images whose runner supports
  the existing `--base-dir` option. Preserve relative symlinks when backing up the
  complete jobs tree; never delete `.attempts` separately.

## v1.2.0 - 2026-10-23

- Optional `idempotency_key` on `/run-simulation` makes repeated requests return
  the original job, including concurrent requests, restart and a lost response.
- A durable submission record freezes named configuration contents before launch.
  Reusing a key with different parameters returns 409; deleting a completed job
  does not allow replay of that identity.
- SQL mode adds `orchestrator_submissions` automatically; file mode uses the
  existing shared-data volume. Include this table/directory in backups. No existing
  jobs or artifacts are rewritten. Older clients retain their existing behaviour.
- Validation: 214 tests passed; PostgreSQL integration is a separate opt-in check.

## Unreleased - 2026-08-10

### Summary

Adds durable Union result recovery observability and metadata-only personal job
organization.

### Added

- Exposes installed result storage totals for every worker by KPIs, time series,
  checkpoints, logs and other files.
- Adds per-submitter flat folders plus a separate archive flag for terminal
  jobs; restoring an archived job preserves its folder.
- Preserves archived jobs during automatic ops cleanup.

### API/Storage Impact

- Adds the `/job-folders` endpoints and job folder/archive/restore operations.
- Adds optional `organization` and `result_storage` fields to job responses.
- Stores metadata in `jobs/.organization.json`; no existing job directory is
  moved or renamed.

## v1.0.0 - 2026-05-16

Release owner: [@calofonseca](https://github.com/calofonseca).

### Summary

First stable release of the standalone OPEVA Job Orchestrator service. The service owns job lifecycle, worker-agent coordination, experiment configs, job datasets, simulation-data artefacts and operational job controls.

### Added

- FastAPI service entrypoint `uvicorn app.main:app` on port `8011`.
- Job endpoints for launch, listing, queue inspection, status, progress, results, logs, resolved config, hosts and job image versions.
- Worker-agent endpoints for next-job polling, job status updates and heartbeats.
- Worker runtime version capture from heartbeat and job-status publications, exposed in `/hosts`.
- Ops endpoints for requeue, stop, fail, cancel and cleanup flows.
- Experiment config endpoints under `/experiment-config*`.
- Dataset endpoints under `/dataset*` and `/datasets`, including Mongo-backed CityLearn dataset generation and ZIP upload/download.
- Simulation-data endpoints for reading job result artefacts.
- Dockerfile, CI workflow and Docker Hub publishing defaults for `calof/job_orchestrator_agent`.
- Postman collection covering jobs, datasets, configs, agent, ops and simulation-data endpoints.

### Changed

- Job-related contracts moved out of `opeva_backend_api_training`.
- Public job/dataset/config base URL changes from backend port `8000` to orchestrator port `8011`.
- Worker internal server URL is `http://job_orchestrator_agent:8011`.

### Fixed

- Startup directory creation now runs in FastAPI lifespan instead of module import, avoiding import-time writes to `/opt/opeva_shared_data`.
- Package discovery is scoped to `app*`, so docs/Postman folders do not break setuptools builds.
- Docker and CI dependency ranges are constrained for stable dataset-generation installs.

### API/Storage Impact

- The orchestrator owns `/opt/opeva_shared_data/configs`, `/datasets`, `/jobs`, `/queue` and `job_track.json`.
- Existing endpoint paths and payloads are preserved; only the service/port changes.
- No temporary proxy remains in `opeva_backend_api_training`.

### Compatibility

- Compatible with existing worker-agent code when `OPEVA_SERVER` points to `http://job_orchestrator_agent:8011`.
- Clients must update job, config, dataset and simulation-data requests to port `8011`.
- Backend API remains available on port `8000` for Mongo/schema/deploy/real-time/health.

### Validation

- `.venv/bin/pytest`: pass (`109 passed`)
- `python3 -m compileall app`: pass
- `docker build -t job_orchestrator_agent:test .`: pass
- `docker run --rm job_orchestrator_agent:test python -c "from app.main import app; print(app.version)"`: pass
- `docker compose -f ../opeva_infra_services/job_orchestrator_agent/docker-compose.yaml config`: pass

### Migration Notes

- Update frontend/API clients from `http://<host>:8000` to `http://<host>:8011` for jobs, configs, datasets and simulation data.
- Update worker deployments to `OPEVA_SERVER=http://job_orchestrator_agent:8011`.
- Keep `/opt/opeva_shared_data` mounted at the same path in the orchestrator and all workers.

## 1.1.0 — AI Training candidate

- Optional PostgreSQL job registry, organization and leased queue (`JOB_DATABASE_URL`).
- Workspace and training provenance retained without changing worker payload requirements.
- Every job retains its resolved configuration snapshot.
- Explicit idempotent JSON-to-SQL migration with dry-run and conflict checks.
- Existing file storage remains the default until an operator completes migration.
- Validation: 201 tests passed, including queue reservation/recovery and migration conflicts.
