# OPEVA Job Orchestrator

FastAPI service that owns OPEVA simulation job orchestration and job datasets. It validates and stores job configs, persists job metadata and artefacts on the shared NFS volume, dispatches work to worker agents, tracks heartbeats/status, and exposes datasets/logs/results/progress.

The public API keeps the contracts that previously lived in `opeva_backend_api_training`; only the base service/port changes.

## Runtime

Default API port: `8011`.

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8011 --reload
```

Docker:

```bash
docker build -t calof/job_orchestrator_agent:latest .
docker run --rm -p 8011:8011 \
  -v /opt/opeva_shared_data:/opt/opeva_shared_data \
  calof/job_orchestrator_agent:latest
```

## API Surface

Job clients use this service for:

- `/run-simulation`
- `/jobs`, `/queue`, `/hosts`
- `/status/{job_id}`, `/job-info/{job_id}`
- `/progress/{job_id}`, `/result/{job_id}`
- `/logs/{job_id}`, `/logs-chunk/{job_id}`, `/file-logs/{job_id}`
- `/stop/{job_id}`, `/job/{job_id}`
- `/job-resolved-config/{job_id}`
- `/job-images/versions`
- `/experiment-config*`
- `/dataset`, `/datasets`, `/datasets/generate`, `/dataset/upload`, `/dataset/download/{name}`
- `/dataset/sites`, `/dataset/dates-available/{site_id}`
- `/simulation-data/index`, `/simulation-data/file`

Worker agents use:

- `POST /api/agent/next-job`
- `POST /api/agent/job-status`
- `POST /api/agent/heartbeat`

Operator endpoints live under `/ops/*`.

## Postman

Import [`postman/OPEVA Job Orchestrator.postman_collection.json`](postman/OPEVA%20Job%20Orchestrator.postman_collection.json) to exercise the migrated job, dataset, config, worker-agent, ops and simulation-data endpoints. The collection defaults to `http://localhost:8011`.

## Releases

Current version: `1.0.0`.

Release notes and the release checklist live in [`docs/releases.md`](docs/releases.md). Tags named `vX.Y.Z` trigger the Docker release tag in GitHub Actions, for example `calof/job_orchestrator_agent:v1.0.0`.

## Shared Storage

Default root: `/opt/opeva_shared_data`.

```text
/opt/opeva_shared_data
├── configs/
├── datasets/
├── jobs/
│   └── <job_id>/
│       ├── job_info.json
│       ├── status.json
│       ├── config.resolved.yaml
│       ├── logs/<job_id>.log
│       ├── progress/progress.json
│       └── results/
├── queue/
└── job_track.json
```

All workers must mount the same shared root and point `OPEVA_SERVER` to this service, for example:

```bash
export OPEVA_SERVER=http://job_orchestrator_agent:8011
```

## Configuration

Common environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `VM_SHARED_DATA` | `/opt/opeva_shared_data` | Shared storage root. |
| `AVAILABLE_HOSTS` | `["tiago-laptop","local","deucalion"]` | Valid worker IDs. |
| `DEFAULT_JOB_IMAGE` | `calof/opeva_simulator:latest` | Default image dispatched to workers. |
| `JOB_IMAGE_REPOSITORY` | `calof/opeva_simulator` | Docker Hub repo queried by `/job-images/versions`. |
| `JOB_SIF_REPOSITORY` | `calof/opeva_simulator_sif` | SIF image repo exposed to Deucalion flows. |
| `MLFLOW_TRACKING_URI` | unset | Tracking URI injected into non-Deucalion jobs. |
| `DEUCALION_MLFLOW_TRACKING_URI` | `file:/data/mlflow/mlruns` | Tracking URI injected into Deucalion jobs. |
| `MLFLOW_UI_BASE_URL` | unset | Base URL used to build MLflow links in job info. |
| `UI_BASE_URL` | `http://193.136.62.78:3000` | Public frontend base URL used in job notification emails. Leave unset to omit the UI link. |
| `UI_LINK_NETWORK_NOTICE` | VPN/ISEP notice | Notice shown next to UI links in job notification emails. |
| `JOB_EMAIL_NOTIFICATIONS_ENABLED` | `true` | Enables RabbitMQ job status emails. |
| `JOB_EMAIL_RABBITMQ_HOST` | `smtp-service` | RabbitMQ host for email requests. |
| `JOB_EMAIL_RABBITMQ_PORT` | `8016` | RabbitMQ port for email requests. |
| `JOB_EMAIL_RABBITMQ_QUEUE` | `email_requests` | Queue receiving email request JSON messages. |
| `JOB_EMAIL_NOTIFY_STATUSES` | `queued,dispatched,running,stop_requested,finished,failed,stopped,canceled` | Job statuses that trigger emails. |
| `JOB_EMAIL_SUBMITTER_EMAILS` | Tiago/Codex to `calof@isep.ipp.pt`; Pedro to `1211076@isep.ipp.pt`; Gustavo to `1211061@isep.ipp.pt` | Submitter-to-recipient map. Accepts JSON or `name=email,name2=email2`. |
| `JOB_EMAIL_SUBMITTER_NAMES` | Codex, Pedro and Gustavo aliases | Submitter display-name aliases used in job metadata and emails. |
| `MONGO_*` | existing OPEVA defaults | Mongo connection used by dataset generation. |

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test]'
pytest
```

The GitHub Actions workflow runs tests and builds/pushes Docker images. The default image is `calof/job_orchestrator_agent`; set the `DOCKER_IMAGE_NAME` secret to override it.
