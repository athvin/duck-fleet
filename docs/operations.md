# Operational telemetry

`GET /metrics` requires the same fleet-read permission as `/v1/workers`; a wrong
or missing bearer receives 401. It returns Prometheus text format 0.0.4 and does
not require Prometheus, Grafana, another database or an always-on agent. Scrape
it only over the deployment's authenticated operator channel. An optional scrape
job can use `metrics_path: /metrics`, `scheme: https`, and the operator bearer
secret; do not expose it unauthenticated.

## Contract

All durations have explicit `_milliseconds` or `_seconds` units. Database-backed
`*_total` counters are cumulative across process restarts; process-local status
is a gauge and resets when its owning process restarts. Queries are answered from
current worker status and a bounded PostgreSQL read on each scrape, so freshness
is scrape-time rather than a background publishing interval. An unavailable
observation is not inferred.

- `duckfleet_active_queries`, `duckfleet_queued_queries`,
  `duckfleet_oldest_queued_milliseconds`: current fleet pressure and oldest wait.
- `duckfleet_execution_duration_milliseconds_{count,sum}`: completed execution
  durations. Count/sum supports average and scrape-range rates; no unbounded
  per-query histogram labels exist.
- `duckfleet_provisioning_duration_milliseconds_{count,sum}`: request-to-ready
  (or terminal) lifecycle durations, including failed provisioning.
- `duckfleet_admission_rejections_total{reason}`: bounded `capacity`,
  `queue_capacity`, `compute_unavailable`, `not_placeable`, `provisioning_failed`,
  `worker_restarted`, `gateway_restarted`, `unknown` or `other` reasons. SQL,
  principals, query/session/worker IDs and detailed errors never become labels.
- `duckfleet_profile_capture_failures_total`: `io_error`, `invalid`, `too_large`
  or `not_emitted` terminal records.
- `duckfleet_orphan_reservations` and
  `duckfleet_oldest_orphan_reservation_seconds`: unreleased reservations and the
  oldest age. Ownership uncertainty does not release capacity early.
- `duckfleet_maintenance_backlog{task}`,
  `duckfleet_maintenance_last_success_seconds{task}`, and
  `duckfleet_maintenance_last_result{task,status}`: coordinator state independent
  of execution-worker availability.
- `duckfleet_worker_observed_oom_terminations_total`: zero unless an OOM reason is
  actually observed. Current deployments do not supply observed-OOM evidence;
  ordinary exit and unknown loss are not relabelled as OOM.

Gateway and worker query logs use the same request/query ID, session ID, worker
boot identity and elapsed duration as `/v1/queries/{id}`. Maintenance logs use the
maintenance run ID, generation, task, outcome and affected count. Identifiers stay
in protected logs/history, never metric labels.

## Troubleshoot one query

1. Fetch `/v1/queries/{request_id}` for its session, worker, admission/execution
   timestamps, terminal reason and profile status.
2. Compare `active`, `queued` and oldest-queue gauges before and during the next
   attempt. Admission rejection counters distinguish capacity from other denials.
3. Compare provisioning and execution count/sum to separate startup from engine
   time. Use lifecycle `failure_kind` for failed provisioning.
4. If a worker disappears, inspect session loss and unknown-commit semantics;
   do not infer OOM or migrate a transaction. Check orphan reservations before
   releasing capacity.
5. If metadata or profile persistence fails, inspect the protected log's request
   ID and profile capture counter. SQL success is not relabelled as profile failure.
6. Check maintenance backlog, last result and last success even when no worker is
   ready. Retry is bounded and recorded; do not run an unbounded manual sweep.
