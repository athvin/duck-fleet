# DuckFleet

A managed DuckLake platform, starting with a local Docker feasibility harness.
DuckFleet is planned as a separate entity from DupeZero, using the same AWS
organization. The intended domain is `duckfleet.com`, pending purchase.

## What DuckFleet is

An open-source, self-hosted data warehouse for a single organization, built on
DuckDB and DuckLake. A Rust gateway routes SQL to a fleet of DuckDB workers
sharing one DuckLake — PostgreSQL holds the catalog, S3 holds the Parquet — so
many processes read and write the same tables concurrently. DuckLake, not
DuckFleet, removes DuckDB's single-writer limit: writers commit through the
catalog, conflicts surface at commit, and the losing transaction is discarded
whole and reported retry-safe. What DuckFleet adds is the warehouse around
that: routing, admission control, per-model compute sizing, query history,
profiling, failure classification, and metering.

The scaling model is horizontal across queries, not within one. Each query runs
on a single DuckDB process bounded by the node it is on; the fleet scales by
running more processes against the same lake. A workload whose individual
queries need more than the largest node you will buy has outgrown DuckFleet and
belongs on a distributed engine such as Trino or Snowflake. Most do not.

Compute comes in two tiers: prestarted persistent workers for everyday queries,
and an isolated per-session pod with exactly the requested CPU and memory for
jobs that need more. The namespace boundary is per team or environment, each
running its own DuckLake; the trust model is teams within one organization, not
hostile tenants. Clients speak HTTP with Arrow IPC results natively — a built-in
CLI and a dbt adapter exist — and an opt-in
[Postgres wire-protocol front](#the-postgres-wire-front) lets `psql` and
simple-protocol drivers connect directly.

## Run locally

Requires Docker Engine with Compose v2 (Docker Desktop works). No host Rust,
Python, dbt, Kubernetes, or AWS credentials are needed. Allow roughly 6 GiB of
Docker memory and internet access for the initial images, packages, and DuckDB
extension downloads at image build time. Workers require Linux cgroup v2, as
provided by the Docker Desktop and Ubuntu CI environments tested here. DuckDB and its required extensions
are checksum-pinned in the images; startup needs only the local services.

```sh
make test
```

This builds the images, starts PostgreSQL, RustFS, a Rust gateway, and two Rust workers, and
runs a real dbt project through the server. It then tests an unavailable worker, restarts the workers and gateway, and verifies
that results and history survived, and checks that RustFS contains Parquet files.
Repeated runs reuse the local volumes and rebuild the fixture from scratch.
Tests leave the services running for inspection. The failure stages intentionally print dbt errors: one proves pre-hook rollback,
and another proves that an unsupported size runs no model SQL.

```sh
make up     # start the services without running the harness
make check  # Rust formatting, clippy, and unit tests inside Docker
make down   # stop this project's containers; keep data
```

The API is bound to `127.0.0.1:18080`. Override `DUCKFLEET_PORT` if that port is
occupied. `DUCKFLEET_TOKEN` overrides the development bearer token in both the
server and dbt containers.

```sh
curl http://localhost:18080/healthz
curl -H 'Authorization: Bearer duckfleet-local-development-token' \
  http://localhost:18080/v1/queries
```

To remove **this project's local data**, run `docker compose down -v`.
This deletes the catalog, history, and object-store volumes.

## What the harness proves

- A thin `type: duckfleet` dbt adapter sends SQL and bound parameters over HTTP to
  an Axum gateway routing to workers embedding DuckDB 1.5.4. Arrow IPC preserves result types.
- DuckLake metadata lives in PostgreSQL; data lives in S3-compatible RustFS.
  A separate PostgreSQL database stores execution history. Workers attach the
  catalog and object store; the gateway reads the platform history database in
  read-only mode. The development PostgreSQL user is shared, so database
  separation is not a database-credential isolation boundary.
- dbt seeds, views, tables, `ref`, tests, documentation generation, and
  `delete+insert` incremental updates run remotely. A test-only guard makes a
  client-side `duckdb.connect()` call fail.
- A dbt connection maps to one server session, preserving temporary tables and
  transactions through pre-hooks, materialization, and post-hooks. The adapter reads
  resolved node metadata before opening the connection; comments are disabled in
  the fixture. History stores model/run IDs, requested resources, tags, and
  server-observed placement and cgroup limits.
- The protocol checks authentication, parameter/result fidelity, session isolation,
  rollback, rollback on close, failed SQL recovery, duplicate request rejection,
  response limits, statement timeout, active-query cancellation, and rollback of
  a deliberately failed dbt materialization.
- Data and query history survive worker and gateway restarts; tiny fixture tables
  explicitly disable DuckLake data inlining so the test verifies real Parquet
  objects, rather than only catalog rows.
- Overlapping transactions from two sessions against one catalog resolve as
  documented in [Concurrent writes and retry safety](#concurrent-writes-and-retry-safety):
  compatible writes both commit, and a losing commit is discarded whole and
  reported as retry-safe.
- No execution escapes the usage ledger, and budgets are enforced before and during
  execution; see [Metering and budgets](#metering-and-budgets). PostgreSQL itself
  rejects edits to recorded usage.

The dbt project runs with two threads. Each worker executes its own statements
serially, and dependent models read the preceding model's committed DuckLake
results. Test artifacts and the last 500 query records are written under
`artifacts/`.

## Size a model's compute

```sql
{{ config(meta={
  'platform': {'cpu': 2, 'memory': '2Gi', 'disk': '20Gi'}
}) }}
select * from {{ ref('stg_orders') }}
```

Naming any of `cpu`, `memory`, or `disk` under `meta.platform` asks for an
**isolated pod** of exactly that size — used by this model alone and removed when
it finishes. A model that names none runs on the shared **persistent** tier, the
default and the only mode Compose serves. The mode is *derived* from what you ask
for; there is no separate `compute` key. Defaults can be set under
`models.<project>.+meta.platform` in `dbt_project.yml`, and the adapter receives
dbt's resolved config before its first SQL statement. Use `meta.platform.tags` for
workload attribution — tags never size compute — and other platform keys are
rejected rather than ignored.

`cpu` is an integer; `memory` and `disk` are binary quantities such as `2Gi` or
`512Mi`. Memory is the container cap, and DuckDB-managed memory is held to half of
it, leaving room for extensions, networking, and Arrow results. `disk` is the
pod's ephemeral-storage limit, enforced by the kubelet. The CPU quota the worker
asserts comes from its limit, always the exact size requested. The local harness
runs one persistent worker out of the box:

| Tier | CPU / memory | DuckDB threads | DuckDB memory limit |
| --- | --- | --- | --- |
| persistent (`small`) | 1 / `1Gi` | 1 | 512 MiB |
| isolated (per model) | as requested | = CPU | half of memory |

Docker CPU quotas are hard limits on CPU time, not dedicated cores.

A model's entire lifecycle stays on one session; it cannot resize in the middle of
a transaction. The persistent tier is a prestarted worker shared across sessions;
an isolated pod serves its one session alone, with requests equal to limits, and is
removed afterwards — nothing else is admitted to it, before or after.

An isolated request the fleet cannot serve is refused, never downgraded to the
persistent tier. Where isolation is off (Compose) a named size is refused `501
compute_unavailable`; where it is on but no placement fits, it is `503` carrying the
refusing component's own reason — the namespace quota's when the size is beyond what
the namespace may hold, or the scheduler's when no node can hold it. Both are
retry-safe: no statement ran, and a smaller request may succeed.

A pod of one's own is not free, and the routing choice turns on how much it costs.
The harness measures three opens — a persistent session, a cold isolated session
(a pod created, scheduled, and pulled to ready), and a second isolated session with
the node and image warm — and records the wall-clock a caller waits for each to
`artifacts/cold-start.json`, on EKS where a cold open also pays node scale-up. The
guidance that falls out: reach for an isolated pod when a statement's own runtime
dwarfs that open cost (large ETL, a job that needs more memory or cores than the
shared tier offers), and stay on a persistent worker for everything interactive,
where the pod's startup would dominate the work.

Isolated compute is **off by default**, and where it is off the mode is refused
with `501 compute_unavailable` rather than quietly served. It needs the gateway to
hold a namespaced Role over pods, which is not the deploy's to create:
`scripts/aws/check-access.sh` asserts the deploy identity cannot create Roles and
fails the deploy if that ever changes. The grant lives in
`infra/kubernetes/boundary` and an operator applies it once with
`scripts/aws/apply-boundary.sh`; see `infra/README.md`.

The gateway asks the API server whether it actually holds that grant and reports
the answer as `isolated_compute` on `GET /v1/workers`, so the chart value records
an intention and the cluster is the only statement of fact. Enabling the mode
without the grant is inert rather than broken — the same refusal, and no statement
runs — and removing the `RoleBinding` is a supported way to turn the feature off.
`duck-fleet#39` tracks switching it on for the shared development namespace.

A cold isolated open can take as long as `readySeconds` (420s on the AWS lane,
to pay node scale-up), which is longer than a gateway's drain budget. A gateway
rollout that lands during one therefore abandons the open — but retry-safe: no
statement ran, and the client's bounded new-session wait rides through the
replacement gap and reopens. The drain budget is deliberately **not** stretched
to cover the full node-provisioning window; doing so would make every rollout
block for minutes even with nothing in flight, on a single-replica deployment
where a rollout is already a brief outage. A pod already serving statements
drains normally within the grace period.

The default lifecycle builds the fixture on the persistent tier with hooks, tests,
and docs, and rebuilds it idempotently. It verifies Docker's actual CPU/memory
limits, the worker's kernel cgroup limits, and history for hooks through commit. On
Kubernetes the `--dbt-isolated` stage additionally runs a model whose
`meta.platform` names a size, proving the adapter carries the derived mode through
to a dedicated pod.
Evidence is saved in `artifacts/model-placement.json`, `worker-resources.json`,
and `docker-worker-limits.jsonl`.

To reproduce the size change manually after `make test`:

```sh
docker compose run --rm dbt dbt run --select fct_orders --full-refresh \
  --vars '{"fct_cpu":1,"fct_memory":"1Gi"}'
docker compose run --rm dbt dbt run --select fct_orders --full-refresh \
  --vars '{"fct_cpu":2,"fct_memory":"2Gi"}'
```

## Layout

```text
server/                 Rust gateway, workers, query history, and the cli + harness roles
server/src/client.rs    HTTP/Arrow client the cli role and harness speak to the gateway with
server/src/harness.rs   The dbt + HTTP + Postgres + S3 integration gate (the harness role)
compose.yaml            PostgreSQL, RustFS, gateway, the persistent worker, dbt and CLI runners
docker/                 Images and initial PostgreSQL databases
scripts/                Build and harness helpers
tests/dbt/              Representative seed, view, table, incremental model, tests
```

The dbt adapter itself is not in this repo: it is a first-class `AdapterType::DuckFleet`
in a fork of dbt-core v2 (`athvin/dbt`), whose compiled `dbt` binary this repo downloads
by pinned release. A model's `meta.platform` (cpu/memory/disk) is what asks for a pod.

## Use it without dbt

dbt is an option, not a dependency. Everything the platform does is reachable from
the HTTP API, and the same `duckfleet-server` binary speaks it as its `cli` role —
no separate package, and no dbt in the image at all:

```sh
export DUCKFLEET_URL=http://localhost:18080
export DUCKFLEET_TOKEN=duckfleet-local-development-token
alias duckfleet='docker compose run --rm api'   # the cli role; dbt is not installed here

duckfleet sql 'create or replace table analytics.events (id integer, region varchar)' \
              "insert into analytics.events values (1, 'eu')"
duckfleet sql 'select region, count(*) from analytics.events group by region'
duckfleet sql 'select * from analytics.events where region = ?' --param eu --format json
```

The `cli` role and the dbt adapter are two front-ends over one wire protocol — the
Arrow-IPC client in `server/src/client.rs` — so neither can drift from the other.

Statements given to one `sql` invocation share a session, which is what pins a load
and the query that checks it to the same worker. Compute is selectable per invocation:
naming any of `--cpu`/`--memory`/`--disk` asks for a pod of that size, the same derived
trigger a dbt model expresses through `meta.platform`; naming none runs on the shared
persistent tier.

```sh
duckfleet sql 'select current_setting("threads")' --cpu 2 --memory 2Gi
duckfleet sql 'select 1' --cpu 4 --memory 8Gi --disk 20Gi   # a pod of its own
```

`duckfleet history`, `query <id>`, `usage`, `budget` and `workers` cover inspection.
A refusal exits non-zero and prints the server's own classification and `retry_safe`,
so a shell script can tell "ask differently" from "ask later" without parsing prose.

The `api` compose service runs this `cli` role from `docker/cli.Dockerfile`, an image
built without the `dbt` binary. That is what proves the whole create → load → query →
inspect path works with no dbt present rather than merely where dbt happens to be absent.

## Local execution API

All `/v1` endpoints require `Authorization: Bearer <token>` — the bootstrap
credential from the environment, or a credential it issued; see
[Credentials](#credentials).

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/healthz` | Process health after startup initialization |
| POST | `/v1/sessions` | Select compute, open a pinned connection; returns `session_id` and placement |
| POST | `/v1/sessions/{id}/queries` | Execute SQL; returns an Arrow IPC stream and `x-query-id` |
| DELETE | `/v1/sessions/{id}` | Close connection and roll back any open transaction |
| POST | `/v1/sessions/{id}/cancel` | Interrupt this session's currently executing statement |
| GET | `/v1/queries` | Filtered history with cursor pagination; defaults to 500 records |
| GET | `/v1/queries/{query_id}` | One durable record, metrics, and raw DuckDB profile when available |
| GET | `/v1/workers` | Every configured worker's state and execution pressure, observed limits, and whether isolated compute is available |
| GET | `/v1/usage` | Append-only usage events with cursor pagination |
| GET | `/v1/budget` | Prepaid balance, held reservations, spend, caps, rate card |
| PUT | `/v1/budget` | Set the prepaid balance and per-job cost cap |
| GET | `/v1/lake/snapshots` | DuckLake snapshots, as an Arrow stream |
| POST | `/v1/lake/maintenance` | Expire snapshots, compact small files, clean unreferenced files (bootstrap only) |
| POST | `/v1/credentials` | Issue a credential for a principal; the token appears only in this response (bootstrap only) |
| GET | `/v1/credentials` | List credentials without any secret material (bootstrap only) |
| DELETE | `/v1/credentials/{id}` | Revoke one credential immediately (bootstrap only) |

`GET /v1/workers` remains available while SQL runs. Each reachable worker reports
its `worker_id`, per-process `boot_id`, `state`, `ready`, `draining`,
`active_queries`, `queued_queries`, `open_sessions`, and `oldest_queued_ms`, along
with its existing placement and engine settings. `oldest_queued_ms` is null when
no query is waiting. States are `starting`, `ready`, `busy`, `draining`, and
`unavailable`; startup may precede availability of placement details. Status reads
bypass the execution queue. The counters include HTTP, pgwire, and platform
operations, so a pgwire query competes for capacity just as an HTTP query does.
Active queries include admission, engine execution, and settlement. Session
counts include idle connections until the worker's next idle-expiry sweep.

The gateway probes workers concurrently and shares observations between discovery
and the fleet report for at most one second. `observation_age_ms` describes the
age since the fleet refresh completed, not the age of every sample: a successful
sample may be older while another probe times out. `last_success_at` on each
entry is its actual successful observation time as a Unix timestamp in
milliseconds, or null if the gateway has never observed that endpoint.
An unreachable worker remains in the report as `state: "unreachable"`, with its
`endpoint` and last successful observation time. Any retained `last_observation`
is historical; stale pressure is not presented as current counters. One failed
probe does not hide the healthy workers.

New persistent sessions prefer eligible workers with fewer active plus queued
queries, then fewer open sessions, with configured endpoint order breaking ties.
Successful opens adjust the cached session count used for ranking without
re-probing an unreachable sibling; reported counters remain the last sampled
values. These are recent observations, not reservations of execution capacity: concurrent opens may see the same snapshot, and workers still enforce
their own admission limits. Existing sessions stay pinned to their worker, and
SQL is never replayed or moved as part of placement. The local harness verifies
status during a query running longer than five seconds, mixed HTTP/pgwire
placement, and a stopped sibling that remains visible in the fleet report.

## Credentials

The environment token (`DUCKFLEET_TOKEN`) is the **bootstrap credential**: the
operator-held secret that issues, lists, and revokes everything else, and the
only principal allowed to — a named credential that could mint could quietly
become every principal at once, so management refusals are a classified `403
forbidden`. Issue per-user and per-service credentials with
`POST /v1/credentials` (`{"principal": "analyst", "expires_seconds": 86400}`,
expiry optional); the token is shown exactly once, in that response, and the
database stores only its SHA-256, so neither a listing, a history row, a log
line, nor a database backup discloses a usable secret. Every query's history
record names the `principal` the gateway authenticated — server-derived, so a
caller's context cannot claim to be someone else; a worker addressed directly
inside the cluster records `direct`.

HTTP and pgwire retain the authenticated credential ID, principal, and expiry.
Expiry has no cache grace period. Revocation invalidates the cache on the gateway
handling the revoke request before its response returns; another gateway
or a direct database revocation can leave a cached read authorized for at most
15 seconds. An earlier lookup cannot repopulate the cache after invalidation.
Workers independently check the credential against PostgreSQL when admitting
each session or statement, after it leaves the execution queue and again after
any wait for budget admission. The final check immediately before execution is
the SQL authorization boundary: revoking a credential prevents queued and later
work from starting, while a statement already admitted may finish within its
existing deadline (at most 60 seconds). Revocation neither cancels nor replays
that statement. Operators can still explicitly cancel it.

Unknown, revoked, and expired tokens all return HTTP `401 unauthorized` or
pgwire SQLSTATE `28P01`. Failed authentication-store lookups fail closed with
HTTP `503 unavailable` or pgwire `57P01`; they do not count as successful
authentication. Credential reads have a three-second database statement timeout.
Pgwire revalidates before its lazy first session and every statement, and checks
idle connections every five seconds. An authentication refusal ends that wire
connection and closes its worker session. Protocol writes are bounded so a
client that stops reading cannot keep the session alive indefinitely.

Workers sweep idle credentials every five seconds, including when no new
request arrives. A running statement delays that sweep until it completes or
reaches its deadline. Closing a session rolls back uncommitted transactions;
an authentication-store failure also closes issued-credential sessions. The
gateway periodically removes idle revoked or expired routes and releases their
isolated pods, subject to Kubernetes deletion latency. These checks do not
require a refused follow-up request to trigger cleanup. The ordinary five-minute
session idle limit still applies to valid credentials.

Rotate a token by issuing another credential for the same principal, completing
a statement with it on each existing HTTP session, then revoking the old
credential. Session ownership
stays with the principal; cleanup follows the last admitted credential. Tokens
for other principals cannot claim that session. An idle session still held by
the revoked token is closed, even if another token exists for its principal.
Submitting a replacement-token statement that is still queued does not transfer
the session yet; if cleanup wins that race, reopen the session after the refusal.
Pgwire clients reconnect with the replacement token because their password is
fixed for the life of a connection.

Credentials authenticate and attribute, and they scope the platform's levers —
not its SQL. Every principal reaches the same full SQL surface; per-principal
SQL scoping is explicitly not built. On the platform API the mutations are
owned: credential management, lake maintenance, and budget **writes** are
bootstrap-only (a spend ceiling writable by the spenders it constrains would be
an accident log, not a control), and a session answers only to the principal
that opened it — another principal's query, cancel, or close is refused `403
forbidden`, while the bootstrap may act on any session because interrupting a
runaway statement is the operator's job. Reads stay org-global by design:
budget position, query history, and usage — including SQL text — are visible to
every credential, because debugging a teammate's run and seeing the one shared
bill are what a single-organization warehouse is for.

## The Postgres wire front

`pgwire.enabled: true` (env `DUCKFLEET_PGWIRE`) has the gateway listen on 5432
speaking enough of the v3 protocol for `psql` and the simple-protocol path
drivers and BI tools are built on. The password is a platform credential — the
bootstrap token or one issued through `/v1/credentials`; the startup `user` is
advisory, identity comes from the credential exactly as over HTTP. Each
connection pins to one ordinary worker session, so transactions and temp tables
behave, and history, principal attribution, metering, and failure
classification all hold with no wire-specific code: classified failures arrive
as `ERROR` with a SQLSTATE a driver can act on (`40001` for a lost commit race,
`57014` for a deadline, `42601` for SQL errors) and the platform's
`failure_kind` in the message.

The honest limits, refused rather than fudged: no extended-protocol bind
parameters (interpolate client-side, as `psql` and psycopg2 already do), no
COPY, LISTEN/NOTIFY, cursors, or CancelRequest, and SSLRequests are answered
`N` — this listener is in-cluster plain TCP, off by default, and transport
security belongs to the exposure layer. Results travel in text format with
typed OIDs (ints, floats, booleans, text, decimals, dates, microsecond
timestamps); anything else says to cast to `VARCHAR`. A gateway rollout ends
wire connections; reconnect and the session-recovery contract applies.

## Exposure

By default the API is a ClusterIP Service: reachable from inside the namespace
and by `kubectl port-forward`, and from nowhere else. The chart's `exposure`
values opt into an Ingress that terminates TLS at the controller, for a team
that queries the warehouse from laptops and CI:

```yaml
exposure:
  enabled: true
  host: duckfleet.example.com
  tlsSecretName: duckfleet-tls        # a kubernetes.io/tls Secret in this namespace
  controllerNamespace: ingress-nginx  # where the ingress controller pods run
```

All three facts are required, not defaulted, because each belongs to the
operator: the DNS name, the certificate (this chart never mints one — bring
cert-manager or your own), and the controller's namespace, which the isolation
NetworkPolicy otherwise keeps out. A bearer token must never cross plain HTTP:
the render always carries the TLS block, `check-chart.py` asserts every exposed
host terminates TLS, and the nginx-family `ssl-redirect` annotation is set by
default — an operator running a different controller owns configuring its
equivalent through `exposure.annotations`. The AWS development lane stays
unexposed; its render checks prove the default produces no Ingress at all.

A session request can supply `{"compute":{"cpu":2,"memory":"2Gi"},"context":{}}`.
Compute is fixed when the session opens; query bodies cannot override it.
A query request has this shape:

```json
{
  "request_id": "2d201944-8826-4e2e-a156-ad9bc861ca33",
  "sql": "select ?::integer as answer",
  "parameters": [42],
  "context": {"workload": "example"},
  "timeout_seconds": 30
}
```

Every error response carries a machine-readable classification alongside its
message, so a caller can tell a lost commit race from a broken statement without
parsing engine text. See
[Concurrent writes and retry safety](#concurrent-writes-and-retry-safety) for the
full table.

```json
{"error": "…", "failure_kind": "transaction_conflict", "retry_safe": true}
```

Request IDs are unique across sessions and retained in PostgreSQL. A repeated ID
returns HTTP 409 with `failure_kind` `duplicate_request` and without executing SQL
again; results are not cached or replayed.
An HTTP/transport failure can occur after a write commits. Inspect history and
application state before retrying; the adapter does not automatically retry SQL.
Accepted worker executions are recorded before SQL runs and updated on completion.
At worker startup, its own unfinished records become `interrupted` with an
unknown-commit warning; another worker's running records are left alone.

Each worker queue holds at most 64 operations, with at most 32 sessions. The
gateway allows at most 64 routed sessions. Exceeding either limit returns HTTP 429
with `failure_kind` `capacity`, which is retry-safe because the request is rejected
before any SQL runs. Sessions idle for
five minutes are closed on the next worker operation. Statement execution has a
30-second default deadline (1–60 seconds allowed); queue time is additional.
Responses fail rather than silently truncate beyond 100,000 rows or 16 MiB.
Each worker has a 1 GiB spill limit and the resource settings listed above.
These are local resource limits, not billing budgets.

## Query history and profiling

List queries by `model_id`, `invocation_id`, `model_run_id`, `worker_id` (`small`
or `medium`), `status`, and `failure_kind`. Filters combine with AND and are bound
SQL parameters. Supported statuses are `running`, `succeeded`, `failed`, `conflict`,
`refused`, `budget_exceeded`, `canceled`, `timed_out`, and `interrupted`. Only failure kinds that can reach a
history record are accepted as filters; rejection kinds such as `capacity` never
become records. All history endpoints require the same bearer authentication
as execution and remain restricted to the trusted development environment.

```sh
curl -G -H 'Authorization: Bearer duckfleet-local-development-token' \
  --data-urlencode 'model_id=model.duckfleet_harness.fct_orders' \
  --data-urlencode 'worker_id=medium' --data-urlencode 'limit=20' \
  http://localhost:18080/v1/queries
```

The response contains `queries` and `next_before`. Pass `before=<next_before>`
with the same filters to fetch the next page; stop when the cursor is null.
`limit` must be 1–500. Ordering uses a durable, descending `history_id` sequence,
not SQL completion order. This is a live history view, not a frozen snapshot:
new records and status updates may arrive between pages. Pre-existing records
receive sequence IDs when the additive schema upgrade first runs.

Each record includes SQL, lifecycle timestamps, execution elapsed milliseconds,
row count, error, requested compute, worker placement, resolved dbt metadata,
`profile_status`, compact `metrics`, and the stored `failure_kind` and
`retry_safe` classification. Both are null on success and on records written
before those columns existed, in the same way older records report a
`not_recorded` profile. Fetch `/v1/queries/<query_id>` for the
full `profile` JSON and operator tree. Raw plans are omitted from list responses.
Unknown IDs return 404; invalid filters return 400.

Workers enable standard JSON profiling by default. Set `DUCKFLEET_PROFILING=false`
when starting Compose workers, or `profiling: false` in the Helm values, to disable
it. The test harness expects profiling enabled. Before each statement the worker
removes the previous profile output; after execution it caps stored profiles at 1 MiB,
persists the profile alongside the query's terminal state, and removes the file.
Statements that emit no profile, disabled profiling, malformed output and oversized
profiles have explicit statuses (`not_emitted`, `disabled`, `invalid`, `too_large`,
`io_error`). Older records and in-flight executions use `not_recorded`. A missing
profile does not change a SQL success into a SQL failure. Profiling remains
best-effort for trusted SQL; callers can modify DuckDB settings themselves.

`latency` and `cpu_time` are seconds; buffer-memory, temporary-directory, and byte
counters are bytes. DuckDB CPU/operator times can overlap and must not be summed
as wall-clock time. Its system peak memory/spill measurements describe the
engine's scope, not container RSS or an isolated per-model allocation. Byte
counters are not AWS request/transfer meters. These metrics support diagnosis;
they are not a billable usage ledger. For multi-statement SQL, DuckDB's file can
represent only the last profiled statement; the history record covers the whole
submitted request.

Profiles live in the platform PostgreSQL database for this development release,
with no S3 archival yet. SQL, profiles, and errors can contain sensitive
literals; inline `password=` values in error text are scrubbed before the error
reaches the caller or the history row, because the engine quotes a caller's
connection string back verbatim when it fails. This is a single-tenant operational history API, not a customer
authorization boundary. A `running` record means the worker accepted and began
the request; queued operations are not yet durable history records.

History retention is configurable and off by default: `historyRetentionDays: 0`
keeps everything, because discarding records is an operator's decision, never a
default's. With a window set, workers sweep expired rows — SQL text and profiles
with them — at startup and hourly, in bounded batches that never hold the engine
behind a long delete, and each sweep that removed anything records itself in
history under the `platform` principal. The usage ledger has **no** retention:
it is billing evidence, so a pruned query's `usage_events` row remains the proof
of what ran, when, for how long, at which rate — everything except the SQL
itself. The harness ages rows past the window and proves the sweep removes them,
spares everything inside it, and leaves the ledger untouched to the row.

The integration gate verifies filtering and pagination, observes/cancels a running
query without waiting behind it, rejects stale profiles after SQL failure, checks
history with both workers stopped, and confirms saved profile data survives
restart. `artifacts/query-profile.json` contains a real profiled dbt model query.

## Concurrent writes and retry safety

DuckLake detects a write conflict when a transaction **commits**, not while its
statements run. The first transaction to commit wins, and a losing transaction is
discarded whole rather than partly applied. Everything in this section was measured
against DuckDB 1.5.4 with a DuckLake PostgreSQL catalog and is re-verified on every
run by the harness `--concurrency` stage (`server/src/harness/concurrency.rs`), which
interleaves two sessions on the persistent worker so the conflict window is exact
instead of a race. Evidence is saved to
`artifacts/write-conflicts.json`.

| Overlapping writes | Outcome |
| --- | --- |
| Appends to the same table | Both commit; no row lost |
| Writes to different tables | Both commit |
| `delete`+`insert` on one table | Second commit conflicts |
| `update` vs `update` | Second commit conflicts |
| `insert` vs `delete` | Second commit conflicts, in either commit order |
| `ALTER` or `CREATE` on the same object | Second commit conflicts |

Conflict granularity is the **data file**, not the row: two writers that rewrite
disjoint rows of one table still conflict. A dbt DAG that runs two `delete+insert`
models against the same table concurrently will therefore lose one of them. Order
those models or give them separate tables; do not rely on row ranges keeping them
apart. A write becomes visible to the other worker as soon as it commits, including
to a transaction that starts after it, so a dependent model does not read a stale
snapshot. A transaction that started earlier keeps the snapshot it opened with.

A losing commit returns HTTP 409, `failure_kind` `transaction_conflict`, and
`retry_safe` true. Because the engine has already discarded the transaction, the
recovery is to replay the whole transaction from `BEGIN`. Do **not** send `ROLLBACK`
first: no transaction is active and the statement fails. The session itself stays
usable.

Neither the server nor the adapter replays SQL on its own. A dbt model that loses a
commit fails, and rerunning it (for example `dbt retry`) replays its transaction;
a single statement cannot be retried in isolation because the materialization spans
several statements.

Two sessions on one worker share a DuckDB instance, but behave the same way: even
two updates to the same row both succeed as statements and conflict only at commit.

`retry_safe` is true only where the platform can prove the request left nothing
behind. An interruption always outranks the engine's message, because a commit
stopped by a deadline may still have committed.

| `failure_kind` | HTTP | `retry_safe` | Meaning |
| --- | --- | --- | --- |
| `transaction_conflict` | 409 | yes | Lost a commit race; the transaction was discarded whole |
| `budget_exhausted` | 402 | yes | The prepaid balance cannot buy a second; nothing ran and nothing was metered |
| `capacity` | 429 | yes | A queue or session limit rejected the request before any SQL ran |
| `sql_error` | 400 | no | The engine refused or failed the SQL; earlier statements in a multi-statement request may have applied |
| `timeout` | 400 | no | A deadline interrupted the statement; an interrupted commit may still have committed |
| `budget_exceeded` | 400 | no | Stopped on reaching its per-job cost cap; same unknown-commit caveat |
| `budget_job_cost` | 400 | no | The per-job cap cannot buy a second at this size; raise the cap or ask for less |
| `canceled` | 400 | no | A caller interrupted the statement; same unknown outcome |
| `invalid_request` | 400 | no | Rejected before execution; correct the request rather than resend it |
| `unknown_session` | 404 | no | The session expired or was closed; open a new one |
| `not_found` | 404 | no | No such history record |
| `duplicate_request` | 409 | no | This `request_id` was already accepted; inspect its history record |
| `no_ready_worker` | 503 | yes | Discovery could not find available compute; no session open was forwarded. New-session clients may wait for recovery |
| `unavailable` | 503 | no | A worker or the history database was unreachable mid-request |
| `interrupted` | — | no | Recorded at worker startup for its own unfinished records; never an HTTP response |

Find conflicts in history with the `status` or `failure_kind` filter:

```sh
curl -G -H 'Authorization: Bearer duckfleet-local-development-token' \
  --data-urlencode 'failure_kind=transaction_conflict' \
  http://localhost:18080/v1/queries
```

What this does not yet cover: conflict rates under genuine parallel load, automatic
or coordinated retry, locking or reservations that would prevent a conflict, and
cleanup of the data files a losing transaction already wrote to object storage.

## Metering and budgets

Every accepted execution is metered. Admission reserves funds before any SQL runs,
settlement records exactly one usage event, and the reservation is released at the
same time — for successes, SQL errors, timeouts, and lost commit races alike.
The harness `--metering` stage (`server/src/harness/metering.rs`) gates that on every
run and saves `artifacts/metering.json`.

The ledger is deliberately separate from query history. Diagnostics get rewritten
as a query progresses; a billable record must not. `usage_events` is append-only
**in the database**: PostgreSQL rejects `UPDATE` and `DELETE` on it, so a correction
has to be a new row whose `corrects` column points at what it compensates. Each
event stores the `rate_card_version` it was priced with, so an old event keeps the
price it was charged at.

The rate card is deployment configuration: `DUCKFLEET_RATE_CARD_VERSION` and
`DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR` (the chart's `rateCard` values),
defaulting to the development placeholder below. Change both together. Each
reservation copies the pair at admission, and settlement and recovery price an
execution at the card it was admitted under — so changing rates affects only new
work, and the ledger is never repriced, even by a restart that recovers old holds
under a new card. The isolated-pod cold-start measurements are what should turn
the placeholder into real numbers.

Money is integer micro-USD throughout; a ledger must not accumulate floating-point
error. Charges round **up** and affordable time rounds **down**, so a budget is
never overrun by rounding.

```sh
curl -H 'Authorization: Bearer duckfleet-local-development-token' \
  http://localhost:18080/v1/budget
```

```json
{
  "prepaid_micro_usd": 10000000, "spent_micro_usd": 109, "held_micro_usd": 0,
  "available_micro_usd": 9999891, "max_job_micro_usd": 1000000,
  "usage_events": 20, "held_reservations": 0,
  "rate_card_version": "local-dev-1", "compute_micro_usd_per_unit_hour": 100000
}
```

A compute class bills on a normalized unit of `max(vCPU, GiB RAM / 4)`, so `small`
is one unit and `medium` is two. `DUCKFLEET_PREPAID_MICRO_USD` (default $10) and
`DUCKFLEET_MAX_JOB_MICRO_USD` (default $1) seed the budget the first time it is
created; a restart never refills a balance that has been spent. `PUT /v1/budget`
sets either limit afterwards, absolutely rather than as a delta. A full `make test`
run meters only a few thousand micro-USD, but a long-lived local stack does spend
its seeded balance eventually; raise it with `PUT /v1/budget`, or start over with
`docker compose down -v`.

**The price is a placeholder.** $0.10 per unit-hour exists to make the metering path
exercisable. Nothing here is reconciled against a real AWS bill, so this is the
third of the blueprint's three numbers (billable usage) without the first two
(physical cost and attributed cost).

Budgets are enforced two ways:

- **Before execution.** If the remaining balance cannot buy even one second,
  admission refuses with HTTP 402 and `failure_kind` `budget_exhausted`. No SQL
  runs, the record is `refused`, and nothing is metered — so the same request is
  safe to send again once funded. If the per-job cap is what cannot buy a second,
  the refusal is `budget_job_cost` instead: resending will not help.
- **During execution.** A per-job cost cap is just a shorter deadline, enforced by
  the same timer as `timeout_seconds`. Whichever binds first decides how the stop
  is reported: `budget_exceeded` or `timeout`. Both consume real compute, so both
  are metered, and neither is retry-safe because an interrupted commit may still
  have committed.

Two sessions cannot both spend the last of the balance: admission locks the budget
row, so reservations serialize across workers.

If settlement itself fails, the reservation stays held and the execution stays
`running`; the next worker start settles it from the elapsed time history recorded
and marks the event `source: recovery`. That keeps "no unmetered execution" true
across a crash, at the cost of a delay. Sessions themselves are free — only
statement execution meters.

```sh
curl -G -H 'Authorization: Bearer duckfleet-local-development-token' \
  --data-urlencode 'execution_id=<query_id>' http://localhost:18080/v1/usage
```

Usage listing pages on a descending `ledger_id` exactly like query history, filters
by `execution_id` and `source`, and caps `limit` at 500.

What this does not yet cover: the ledger meters compute only — no storage, S3
requests, catalog, or network meters. The per-job cap is therefore enforceable as a
deadline, because elapsed time is the only thing that costs anything; adding a
request or byte meter would break that equivalence and need separate enforcement.
There is no tenant dimension, because the harness bills one tenant. Nothing issues a
compensating event yet; `corrects` and the append-only trigger establish how a
correction must work, not a feature that performs one. Spend is not reconciled
against an AWS bill, so of the blueprint's three numbers this is only the third.

## Backup and restore

The catalog **is** the lake: every Parquet object in the bucket is unreachable
without the catalog rows that name it, so losing the single PostgreSQL loses the
warehouse however intact S3 is. `scripts/backup-catalog.sh <compose|kube> <dir>`
dumps both databases — `lake` (the DuckLake catalog) and `platform` (query
history, usage ledger, budget) — as custom-format archives, and
`scripts/restore-catalog.sh` restores them into a freshly initialised PostgreSQL,
refusing to run without `DUCKFLEET_CONFIRM_RESTORE=yes`. Stop workers and the
gateway around a restore; their startup migrations are idempotent against the
restored schema.

The harness proves the whole cycle, not just the dump: it backs up, commits more
work, destroys the PostgreSQL volume outright, restores, and asserts that
pre-backup data reads through the restored catalog from the bucket, that work
committed after the backup is gone, that history and the ledger are back, and
that a fresh write commits cleanly against the restored snapshot chain. The kind
harness additionally exercises the `kube` mode an operator would actually run.

The recovery point is the age of the newest backup you kept — scheduling and
retention are the operator's policy, and the honest statement of loss is:
everything committed after that backup, including its history and ledger rows.
Parquet written after the backup survives in the bucket as unreferenced objects;
they are harmless to correctness and belong to table maintenance (#14) to reap.
The two databases are dumped sequentially, each internally consistent, so history
may lead or trail the catalog by the dump gap.

## Team namespaces

The namespace boundary is per team or environment, each running one DuckLake —
the aligned model. `scripts/provision-namespace.sh <namespace>` stamps out the
next one: the ownership boundary (operator-applied, as always), a fresh
credential Secret the re-run never rotates, and the release with
`lakePrefix=<namespace>/lake` so no two namespaces share a byte of the bucket.
`scripts/deprovision-namespace.sh` removes a namespace whole — compute,
catalog, credentials, PostgreSQL volume — behind two spoken confirmations for
the primary, and prints rather than runs the bucket-prefix deletion.

Isolation is by construction, not discipline: each namespace has its own
PostgreSQL and passwords, its own bootstrap token and issued credentials, its
own prefix — and the isolation NetworkPolicy keeps 5432 namespace-local, which
the Kubernetes harness proves the hard way: from a freshly provisioned
namespace, the first namespace's catalog is unreachable *even when the caller
holds its real password*. On AWS, the worker IAM policy scoped to the
namespace's prefix is the storage half of the same boundary, provisioned with
whoever owns IAM.

## Lake maintenance

DuckLake accumulates snapshots and small Parquet files as models rematerialize;
left alone, storage grows and scans slow. `GET /v1/lake/snapshots` lists
snapshots (any principal — looking is how you decide what expiry would cost),
and `POST /v1/lake/maintenance` takes any combination of
`{"expire_older_than_days": N, "compact": true, "cleanup": true}`: expire
snapshots older than the window (`0` keeps only the current state, which is
exactly as destructive as it sounds), merge adjacent small files, and delete
files no snapshot references. Maintenance is **bootstrap-only** — expiry
forfeits time travel and cleanup deletes files, which is the operator's call.

The operations run through the ordinary execution machinery rather than a side
door, so each lands in query history under the real principal, is admitted and
settled by the usage ledger, and a maintenance commit that loses a race to a
concurrent write fails classified `transaction_conflict`, retry-safe, like
anyone else's — DuckLake resolves the conflict at the catalog, maintenance gets
no special lock. Each call is bounded by the 60-second statement cap; a very
large lake may need repeated calls, which are safe to repeat. The harness
fragments a table into a dozen files, runs the sweep, and asserts files and
snapshots shrink while the data stays identical to the row.

## External PostgreSQL

The bundled PostgreSQL is a development fixture — one replica, one PVC, no HA —
and the catalog it holds is the write-coordination point for the whole lake.
Running DuckFleet as a real warehouse, set `postgres.bundled: false` and point
the deploy Secret's `CATALOG_DSN` and `PLATFORM_DSN` at managed PostgreSQL; the
release renders no bundled instance at all, and the server side never knew where
the catalog lived beyond the DSNs anyway. A database outside the namespace also
needs its address in `postgres.externalCidrs`, because the isolation
NetworkPolicy otherwise permits 5432 only between the namespace's own pods.
Migrating from bundled to external is a [backup and restore](#backup-and-restore)
with compute stopped: back up the bundled instance, restore into the external
one, change the DSNs, disable the bundle. The Kubernetes harness proves the
path by swapping a running release onto a standalone PostgreSQL and running the
full dbt-free workflow against it.

## Scope and limitations

This is a **trusted, single-tenant development environment** with known local
credentials and plain HTTP inside Docker. Every credential — bootstrap or
issued — controls access to an administrator SQL connection; credentials
authenticate and attribute, they are not a SQL sandbox. Do not expose the API
publicly or use customer data. Query history includes SQL text and errors.
Logical model IDs and tags are caller-supplied correlation data, not
authenticated billing data; the `principal` column is the authenticated part.

Only SQL models are implemented. The Docker harness runs the two persistent
compute classes; isolated per-session pods are implemented in the Kubernetes
chart and exercised by the kind harness, and are refused with `501` wherever the
pod grant is absent — which includes everywhere in Docker. There is no warm
pool, durable job queue, multi-tenant authorization, production secret
management, or automatic write retry.
The usage ledger meters compute only, against a configurable rate card whose
default is a placeholder, and nothing
reconciles it against a real AWS bill; see
[Metering and budgets](#metering-and-budgets).
Arbitrary DuckDB SQL can access worker files and network resources. Python models,
external materializations, and dbt plugins are unsupported. Unchecked dbt
materializations or version combinations are not part of the tested contract.

Health reports process availability, not continuous dependency readiness. History
uses separate, read-only PostgreSQL connections and remains available while
workers are busy or stopped. Each process admits at most eight concurrent history
reads, with a three-second connection timeout and PostgreSQL statement timeout;
excess requests return 429. A database outage returns 503. Cancellation bypasses
the execution queue. The response limit applies to returned data;
DuckDB may materialize a result before serialization. Canceling or losing an HTTP
request is not proof that a write was rolled back. Conflicting writes from multiple
DuckLake workers are covered above, and the backup/restore cycle is drilled in
[Backup and restore](#backup-and-restore), but this harness does not yet test
crash recovery during a commit.
A gateway restart loses its in-memory session routes; the recovery contract is
classified rather than magical, and the Kubernetes harness proves it against a
gateway killed without grace. A request severed in transport fails as
`transport_failure`, never retry-safe — its true outcome is read back from
`/v1/queries` by `request_id`, because the worker finishes what it was running
whether or not the gateway survives. A lost session answers `unknown_session`
with instructions, its uncommitted work can only be rolled back, and the client
rides new-session opens through a rollout's `draining` window and the replacement
gap on a bounded wait. Statements are never replayed by the client,
under any classification.

## Design and upstream contracts

The [founding CTO blueprint](MANAGED_DUCKLAKE_CTO_BLUEPRINT.md) describes the broader
product and the feasibility gates. This implementation starts the adapter and
persistent-worker spikes with Docker rather than Kubernetes.

- [DuckLake connections and PostgreSQL catalogs](https://ducklake.select/docs/stable/duckdb/usage/connecting)
- [dbt adapter development](https://docs.getdbt.com/guides/adapter-creation)
- [dbt-duckdb adapter and materializations](https://github.com/duckdb/dbt-duckdb)

## AWS development lane

[Terraform, isolated EKS namespace, and live dbt sizing harness](infra/README.md)
are available for a reviewed AWS deployment. The infrastructure document records
what has been validated locally and what still needs a live run.
