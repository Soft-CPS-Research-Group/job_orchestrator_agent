# Worker Agent Integration Guide

This document summarises the contract between the Job Orchestrator API and any worker
agent implementation. The full job lifecycle, payload schema, and ops controls
are documented in `docs/jobs.md`.

## Queue Semantics
- The API stores every launch request as a JSON file in the global
  `queue/` directory. Each file contains:
  - `job_id`
  - `preferred_host`: string or `null`
  - `require_host`: boolean (true if the requester targeted a specific host)
  - optional `target_worker_profile`: `"cpu"` or `"gpu"` when automatic host
    selection was constrained to a compute profile.
- Agents obtain work by POSTing to `/api/agent/next-job` with their
  `worker_id` and supported `capabilities`. Workers implementing attempt
  fencing advertise `"attempt_fencing_v1"`. The server returns the first job whose host requirement is
  satisfied (matching host for required jobs, or any eligible host for
  automatic jobs).
- Automatic GPU jobs are only dispatched to workers whose heartbeat advertises
  GPU support. Workers should set `info.gpu_enabled=true` (or another supported
  GPU flag such as `has_gpu`/`cuda_available`) when they can run CUDA/GPU jobs.
- Special case: worker `deucalion` is strict and only receives jobs explicitly
  pinned to `target_host="deucalion"` (host required).
- Worker `union-inesctec` advertises itself as GPU-only. It can be selected
  explicitly with `target_host="union-inesctec"` or automatically with
  `target_worker_profile="gpu"`.
- Workers listed in `PERSISTENT_RECOVERY_WORKER_HOSTS` may persist a durable
  `.worker/union.json` state. While that state is non-terminal, or its terminal
  outcome is not yet acknowledged, stale reconciliation preserves the job for
  the same worker instead of placing it back in the queue.
- The response to `/api/agent/next-job` includes the fully populated payload
  (image, command, volumes, env, job name) derived from orchestrator metadata, so
  agents do not need to read anything from the queue file beyond the job id.
- For capable workers, the dispatch response also contains `attempt_number`,
  `attempt_protocol="attempt_fencing_v1"` and an opaque `attempt_token`. Every
  subsequent `/api/agent/job-status` publication for that execution must echo
  the same number and token. Requeue invalidates the token immediately; stale
  updates then receive HTTP 409 and cannot mutate the new attempt. Current
  workers respond by stopping the superseded Docker container, Slurm job or
  Union Run. The raw token is never persisted by the orchestrator.
- Capability negotiation permits rolling upgrades. Legacy jobs remain
  compatible with legacy workers. Once a job has used attempt fencing, it stays
  fenced and is no longer eligible for dispatch to a legacy worker.
- Once dispatched, the orchestrator marks the job as `dispatched` and updates
  `target_host` in `job_info.json` and the job registry. Agents are expected to
  begin execution immediately; should they choose not to run the job they
  must call `/api/agent/job-status` with a terminal status so the queue can be
  resubmitted manually.

## Lifecycle Hooks
- **Start:** POST `/api/agent/job-status` with `status="running"` (include
  `worker_id`, `worker_version`, `container_id`, `container_name`, and the
  dispatch attempt fields when supplied).
- **Progress heartbeat:** while running, POST periodic `status="running"`
  updates to refresh `status_updated_at` and avoid stale-job handling.
- **Completion:** POST `/api/agent/job-status` with
  `status="finished"` or `"failed"` plus `worker_id` and optional
  container metadata.
- **Stop requested:** if the API sets `status="stop_requested"`, the worker
  must terminate the container and respond with `status="stopped"`.
- **Cancellation:** queued jobs can be cancelled by the API (`canceled`).

## Heartbeats
- Agents must send a heartbeat at least every
  `OPEVA_HEARTBEAT_INTERVAL` seconds (default 30) by POSTing to
  `/api/agent/heartbeat` with `{"worker_id": ..., "info": {...}}`. The
  orchestrator records the timestamp and optional free-form info block.
- Worker implementations should include `info.worker_version` in heartbeat
  payloads. The orchestrator also accepts `worker_version` on every
  `/api/agent/job-status` request and exposes the latest status publication
  version in `/hosts` as `info.worker_version` and `info.last_status_worker_version`.
- For Deucalion, `/hosts` also exposes `info.partition_limits`; UI clients can
  use this to keep Slurm walltime controls aligned with server validation.
- Hosts are reported as `online` if a heartbeat was received within
  `HOST_HEARTBEAT_TTL` seconds (default 60). Offline hosts remain visible so
  queued jobs can be inspected even if the worker is disconnected.

## Concurrency
- The orchestrator does not enforce concurrency. Agents are responsible for deciding
  whether they have capacity to start a new job before claiming it.

## NFS Requirements
- Every worker must mount the shared storage (same path as the server,
  `/opt/opeva_shared_data` by default). Job payloads assume `config` is
  available under `/data/configs/...` when the container is started.

Implementations that follow this contract can live in a separate repository,
such as `job_worker_agent`.
