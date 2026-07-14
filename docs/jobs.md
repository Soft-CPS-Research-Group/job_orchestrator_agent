# Job System and Worker Contract

This document defines the job system, lifecycle, and the worker-agent contract.
It is meant to be detailed enough to implement the worker side in a separate
repository without reading server code.

## Scope
- The API server does not execute jobs; it only coordinates workers.
- Workers run containers and write artefacts to the shared NFS mount.
- Shared storage is the source of truth for job metadata, logs, progress, and
  results.

## Shared Storage Layout
Default root: `/opt/opeva_shared_data` (configurable via `VM_SHARED_DATA`).

```
/opt/opeva_shared_data
├── configs/            # YAML experiment configs
├── datasets/           # Generated datasets + schema.json
├── jobs/
│   └── <job_id>/
│       ├── job_info.json
│       ├── status.json
│       ├── logs/<job_id>.log
│       ├── progress/progress.json
│       └── results/result.json
├── queue/              # One JSON payload per queued job + claim files
└── job_track.json      # Registry of known jobs
```

## Files and Their Responsibilities

## Config Identity Keys
- Preferred fields: `metadata.experiment_name` and `metadata.run_name`.
- Legacy fallback remains supported: `experiment.name` and `experiment.run_name`.
- If neither is present, the API falls back to `UnnamedExperiment`/`UnnamedRun`.

### queue/<job_id>.json
Created by the API when a job is queued. Contains only scheduling metadata:
- `job_id`: string
- `preferred_host`: string or null
- `require_host`: boolean
- optional `target_worker_profile`: `"cpu"` or `"gpu"` for automatic host
  selection constrained to CPU-only or GPU-capable workers.

Workers should NOT read queue files directly. Use `/api/agent/next-job`.

### jobs/<job_id>/job_info.json
Written by the API at launch; updated by the worker with container metadata.
Typical fields:
- `job_id`, `job_name`, `config_path`
- `target_host`
- `container_id`, `container_name`
- `experiment_name`, `run_name`
- optional: `exit_code`, `error`, `details`

### jobs/<job_id>/status.json
Updated on every state transition. Important fields:
- `job_id`
- `status`
- `worker_id` (when known)
- `exit_code`, `error`, `details` (optional)
- `stop_requested` (optional)
- `status_updated_at` (epoch seconds; added by server on every write)

### job_track.json
Server-side registry used for listing and recovery. Mirrors the latest known
metadata for each job ID.

## Job States
All states (from `app/status.py`):
- `launching`
- `queued`
- `dispatched`
- `running`
- `stop_requested`
- `stopped`
- `finished`
- `failed`
- `canceled`
- `not_found` (utility)
- `unknown` (utility)

### Normal Transition Paths
- `launching -> queued -> dispatched -> running -> finished|failed`

### Stop Flow
- `dispatched|running -> stop_requested -> stopped`

### Cancel Before Start
- `launching|queued -> canceled`

### Stale Handling
- `dispatched -> queued` when status updates are stale or worker disappears
- `running|stop_requested -> failed` when status updates are stale or worker
  disappears

Invalid transitions return HTTP 409 on `/api/agent/job-status`.
Repeated updates with the same status are accepted (idempotent).

## API Endpoints (Worker + Job Control)

### Job submission and inspection
- `POST /run-simulation` (server only)
  - Optional body field `target_worker_profile: "cpu"|"gpu"` constrains
    automatic host selection without pinning a specific host. It cannot be
    combined with `target_host`.
- `GET /status/{job_id}`
- `GET /job-info/{job_id}`
- `GET /jobs` (all jobs)
- `GET /queue` (queue entries)
- `GET /hosts` (worker health snapshot)
- `GET /deucalion/partitions` (Deucalion partition walltime limits)
- `GET /logs/{job_id}` (stream)
- `GET /file-logs/{job_id}` (file stream)
- `GET /progress/{job_id}`
- `GET /result/{job_id}`
- `POST /stop/{job_id}` (set stop_requested or cancel queued)
- `DELETE /job/{job_id}` (delete artefacts + registry)

### Agent endpoints (worker uses these)
- `POST /api/agent/next-job`
  - Body: `{ "worker_id": "tiago-gpu" }`
  - Response: 200 + job payload or 204 if no job is available
  - Automatic dispatch skips CPU-only workers for jobs whose config declares GPU
    requirements (`require_cuda`, `cuda_required`, `require_gpu`, `gpus`,
    `device: cuda`, etc.). Launch requests with `target_worker_profile="gpu"`
    require a GPU-capable worker even if the config has no GPU marker.
  - Special case: `worker_id="deucalion"` only receives jobs explicitly
    targeted to `deucalion` (`target_host` required).
  - `worker_id="union-inesctec"` is GPU-only. Select it explicitly with
    `target_host="union-inesctec"`, or allow automatic GPU routing with
    `target_worker_profile="gpu"`.

- `POST /api/agent/job-status`
  - Body (example):
    ```json
    {
      "job_id": "<job_id>",
      "status": "running",
      "worker_id": "tiago-gpu",
      "worker_version": "0.4.1",
      "container_id": "<docker-id>",
      "container_name": "opeva_job_demo_1234"
    }
    ```
  - Allowed statuses: `running`, `stop_requested`, `stopped`, `finished`,
    `failed` (plus any other valid transitions)

- `POST /api/agent/heartbeat`
  - Body: `{ "worker_id": "tiago-gpu", "info": { "load": 0.5 } }`

### Ops endpoints (manual recovery)
- `POST /ops/jobs/{job_id}/requeue` (force optional)
- `POST /ops/jobs/{job_id}/fail` (force optional)
- `POST /ops/jobs/{job_id}/cancel` (force optional)
- `POST /ops/queue/cleanup` (body: `{ "force": true }` to clear all queue files)
- `POST /ops/jobs/cleanup` (body: `{ "keep": ["job_id_a", "job_id_b"] }`)

Ops controls are for operators; they can override normal transitions if
`force=true`.

## Job Payload Returned to Workers
Example response from `/api/agent/next-job`:
```json
{
  "job_id": "1234-5678",
  "job_name": "Demo-Run1",
  "config_path": "configs/demo.yaml",
  "preferred_host": "tiago-gpu",
  "image": "calof/opeva_simulator:latest",
  "command": "--config /data/configs/demo.yaml --job_id 1234-5678",
  "container_name": "opeva_job_demo_1234",
  "volumes": [
    {"host": "/opt/opeva_shared_data", "container": "/data", "mode": "rw"}
  ],
  "env": {}
}
```

Workers should use the provided `image`, `command`, `container_name`, and
`volumes` when running the container. The server assumes `/data` maps to the
shared storage root so `/data/configs/...` is accessible.

If the API needs to normalize a config before execution (for example converting
`./datasets/<name>/schema.json` or host paths into container paths), the worker
payload can point `config_path` at `jobs/<job_id>/config.resolved.yaml` and keep
the original file in `source_config_path`. Workers should run the provided
`command` as-is.

Dataset paths in configs should resolve inside the job container. The stable
form is `/data/datasets/<dataset_name>/schema.json` because the shared storage
root is mounted at `/data`.

## Worker Responsibilities

### Startup
- Mount shared storage at the same path as the server (default
  `/opt/opeva_shared_data`).
- Set `WORKER_ID` to a value listed in `AVAILABLE_HOSTS`.
- Send `POST /api/agent/heartbeat` on a fixed interval.

### Main Loop (Suggested)
1. Send heartbeat.
2. Call `POST /api/agent/next-job`.
3. If 204, sleep and loop.
4. If job payload:
   - Validate access to `config_path` under `/data`.
   - Start the container with the provided image, command, container name,
     volumes, and env.
  - Send `job-status` with `status=running`, `worker_version`, and container metadata.
5. While running:
   - Stream container logs to
     `/data/jobs/<job_id>/logs/<job_id>.log`.
  - Send periodic `job-status` updates with `status=running` and `worker_version` (refreshes
     `status_updated_at` to avoid staleness).
   - Check stop requests by polling `GET /status/<job_id>` or reading
     `jobs/<job_id>/status.json`. If `stop_requested`, terminate the container
     and send `status=stopped`.
6. On exit:
   - Send `status=finished` for exit code 0, otherwise `status=failed`.
   - Include `exit_code` and optional error details.

### Concurrency
- General workers decide how many jobs they can run at once.
- Deucalion dispatch is server-limited by runtime profile: at most one active
  CPU job and one active GPU job can be dispatched/taken at the same time.
  Additional Deucalion jobs remain queued until the corresponding profile slot
  is free.
- Deucalion launch requests are validated against the published Slurm partition
  walltime limits. Current limits are exposed by `GET /deucalion/partitions`
  and in `/hosts -> hosts.deucalion.info.partition_limits`.

### Idempotency
- Status updates with the same state are accepted. Invalid transitions return
  HTTP 409. Use `GET /status/<job_id>` to understand current state.

## Staleness and Failure Policies
These settings are in `app/config.py`:
- `QUEUE_CLAIM_TTL`: claim file timeout; old claims are returned to the queue.
- `HOST_HEARTBEAT_TTL`: worker considered offline after this period.
- `WORKER_STALE_GRACE_SECONDS`: additional grace before failing jobs.
- `JOB_STATUS_TTL`: job considered stale if no status update within this period.

Server behavior:
- Dispatched jobs with stale status are requeued.
- Running or stop_requested jobs with stale status are marked failed.
- If a worker is offline beyond TTL + grace, its running jobs are failed and
  dispatched jobs are requeued.

To avoid stale failures, workers should send periodic `job-status` updates
while running.

## Stop Semantics
- `POST /stop/{job_id}` on the API does **not** directly stop a container.
- For running/dispatched jobs, the API sets `stop_requested` and expects the
  worker to terminate the job and report `stopped`.
- For queued jobs, the API cancels them immediately (`canceled`).

## Ops Controls
Ops endpoints are designed to recover from stuck jobs or queue corruption.
Use with caution because requeueing can cause duplicate execution if a worker
is still running the original job.

- `requeue`: moves job back to queue; `force=true` bypasses state checks.
- `fail`: marks job failed with a reason; `force=true` bypasses state checks.
- `cancel`: marks job canceled with a reason; `force=true` bypasses state checks.
- `cleanup`: removes queue entries with missing/invalid job metadata.

## Recommended Worker Telemetry
Include optional info in heartbeat payload:
- CPU/GPU load, available memory
- worker version
- active job count

The API stores heartbeat info in the host snapshot returned by `/hosts`.
Every status update can also carry `worker_version`; the API stores the latest
status publication in `/hosts` as `info.last_status`, `info.last_status_job_id`,
`info.last_status_at`, and `info.last_status_worker_version`.
For Deucalion, `/hosts` also exposes `max_active_jobs_by_profile`,
`active_job_count_by_profile`, and `active_job_ids_by_profile` for the CPU/GPU
dispatch slots enforced by the server.
