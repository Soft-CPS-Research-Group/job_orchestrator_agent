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
