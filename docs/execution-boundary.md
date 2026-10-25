# Execution boundary — SOF-232

## Ownership

AI Training owns workspaces, community authorization, dataset/configuration
versions, generation requests and links to executions. Community and Access remain
authoritative through their existing APIs. The dataset generator is an internal
Training process: same repository, database and release, with a separate runtime
image because the simulator dependencies are not needed by the HTTP API.

Orchestrator owns jobs, attempts, queue, idempotency, dispatch, status and execution
artifacts. Workers talk only to Orchestrator. Each backend owns a logical database
and restricted credentials. They may share a PostgreSQL instance, not tables.

## Consumers and compatibility

* UI uses Training's explicit workspace APIs; its paths need not change.
* Training currently calls the legacy execution endpoints, including operations,
  result/config downloads and simulation-data inspection. Migrate this transport
  to the versioned execution API without changing workspace authorization.
* CPU and Union workers use `/api/agent/*` and `/status/{id}`. Preserve dispatch,
  attempt directories, cancellation polling and Union input transfers.
* Existing operator scripts use legacy endpoints. Keep an explicitly documented
  compatibility surface during migration; do not silently remove workflows.
* Historical jobs have no client identity. Assign ownership only through an
  explicit migration policy, retaining their workspace, submitter and provenance.

## Versioned contract

Authenticated clients submit resolved configuration, an idempotency key,
namespace, opaque origin metadata and immutable input references. Namespaces are
authorized by server-side client configuration; knowing a namespace/job/input ID
does not grant access. Training is a configured client, not an execution domain.
Retries with the same identity and parameters return the original job; changed
parameters conflict. Ownership is fixed before a job can enter the queue.

Inputs are registered and verified before submission. Shared filesystem sources
are permitted only below an operator-configured client root, with validated
relative paths and content hashes. Execution must continue if Training is down.
Historical paths remain compatible; new snapshots are isolated from mutable
catalog names. Configuration and provenance are frozen with each submission.

Results/status/progress/cancellation are retrieved through execution APIs. Training
stores job IDs and provenance, not a competing authoritative execution state.
No automatic deletion of datasets, outputs, logs, models or checkpoints.

## Publication gates

Ship compatible API support before switching Training. Rehearse database copy and
restore against real PostgreSQL. Stop every state writer for the final copy,
including worker callbacks, and activate only one queue. Verify all five owned
tables by primary key and canonical content digest. Historical files stay in place.
After new writes, recovery uses the new authoritative database; never blindly
restore JSON or a stale snapshot. Restricted roles must fail cross-database access.

Acceptance includes a generic job without Training, Training generation without
Orchestrator, concurrent retries, cancellation, worker loss/recovery, restart,
historical UI workflows and CPU/Union execution. Deucalion/Jetson, model manifest,
BAU and MCP remain separate work. Unrelated server services remain untouched.
