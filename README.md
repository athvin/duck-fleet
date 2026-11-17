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
hostile tenants. Clients speak HTTP with Arrow IPC results today — a Python
client, CLI, and dbt adapter exist — and a Postgres wire-protocol front is
planned later so Postgres drivers and BI tools connect directly.

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
- Overlapping transactions from two workers against one catalog resolve as
  documented in [Concurrent writes and retry safety](#concurrent-writes-and-retry-safety):
  compatible writes both commit, and a losing commit is discarded whole and
  reported as retry-safe.
- No execution escapes the usage ledger, and budgets are enforced before and during
  execution; see [Metering and budgets](#metering-and-budgets). PostgreSQL itself
  rejects edits to recorded usage.

The dbt project runs with two threads. Each worker executes its own statements
serially, and dependent models read the preceding worker's committed DuckLake
results. Test artifacts and the last 500 query records are written under
`artifacts/`.

## Request a worker size per model

```sql
{{ config(meta={
  'platform': {'compute': 'persistent', 'cpu': 2, 'memory': '2Gi'}
}) }}
select * from {{ ref('stg_orders') }}
```

Defaults can also be set under `models.<project>.+meta.platform` in
`dbt_project.yml`. The adapter receives dbt's resolved model config before its
first SQL statement. Both `cpu` and `memory` must be supplied together; omitting
both requests 1 CPU and `1Gi`. Use `meta.platform.tags` for workload
attribution. Other platform keys are rejected rather than ignored.

Any CPU and memory pair may be requested. There is no fixed menu of sizes: a
worker reports the resources it actually has, read from its own cgroup, and the
gateway routes a request to a worker providing exactly that. Running a new size
is running a worker with those limits and listing its endpoint in
`DUCKFLEET_WORKER_URLS`, not teaching the platform a new name.

The local harness runs two workers, so these resolve out of the box:

| Requested CPU / memory | Worker | DuckDB threads | DuckDB memory limit |
| --- | --- | --- | --- |
| 1 / `1Gi` | `small` | 1 | 512 MiB |
| 2 / `2Gi` | `medium` | 2 | 1 GiB |

Memory in the request is the **container cap**, given as a binary quantity such
as `2Gi` or `512Mi`. DuckDB-managed memory is limited to half that cap, leaving
room for extensions, networking, and Arrow results. Docker CPU quotas are hard
limits on CPU time, not dedicated CPU cores.

A request no worker provides is refused with `503`, naming the sizes the
deployment does run. It is never served by a different size, and an unreachable
worker is reported separately from a size that was never provisioned. A model's
entire lifecycle stays on one worker/session; it cannot resize in the middle of a
transaction. Unsupported sizes return an error, and an unavailable worker never
falls back to a smaller one.

A request carries a mode as well as a size. `persistent` selects an exact
matching, prestarted worker, shared with other sessions of the same size.
`isolated` creates a pod for that session alone, with requests equal to limits,
and removes it afterwards; nothing else is admitted to that pod, before or after.
An isolated request that cannot be served is refused with the refusing component's
own reason — the namespace quota's, when the size is beyond what the namespace may
hold at all, or the scheduler's, when no node can hold it — and is never downgraded
to a shared worker. Both are retry-safe: no statement ran, and a smaller request
may succeed.

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

The harness builds `fct_orders` on medium and `customer_totals` on small, checks
resource settings inside the materialized results, then rebuilds `fct_orders` on
small and medium again by changing config. It verifies Docker's actual CPU/memory
settings, the workers' kernel cgroup limits, and history for hooks through commit.
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
server/                 Rust gateway, resource classes, workers, query history
clients/duckfleet-python/  HTTP/Arrow client and CLI; no dbt dependency
adapters/dbt-duckfleet/  Python dbt adapter, which depends on that client
compose.yaml            PostgreSQL, RustFS, gateway, sized workers, dbt and API runners
docker/                 Images and initial PostgreSQL databases
scripts/                Build and harness helpers
tests/dbt/              Representative seed, view, table, incremental model, tests
tests/harness.py         Protocol, dbt, and restart assertions
tests/api_workflow.py    The same warehouse workflow with dbt absent
tests/concurrency.py     Overlapping DuckLake transactions and retry classification
tests/metering.py        Usage ledger, pricing, and budget enforcement
```

## Use it without dbt

dbt is an option, not a dependency. Everything the platform does is reachable from
the HTTP API, and the client that speaks it is its own package:

```sh
pip install ./clients/duckfleet-python
export DUCKFLEET_URL=http://localhost:18080
export DUCKFLEET_TOKEN=duckfleet-local-development-token

duckfleet sql 'create or replace table analytics.events (id integer, region varchar)' \
              "insert into analytics.events values (1, 'eu')"
duckfleet sql 'select region, count(*) from analytics.events group by region'
duckfleet sql 'select * from analytics.events where region = ?' --param eu --format json
```

`duckfleet` depends on `pyarrow` and the standard library. It does **not** depend on
dbt-core, and the dbt adapter depends on *it*, so both speak one wire protocol rather
than two that can drift apart.

Statements given to one `sql` invocation share a session, which is what pins a load
and the query that checks it to the same worker. Compute is selectable per invocation,
the same choice a dbt model expresses through `meta.platform`:

```sh
duckfleet sql 'select current_setting("threads")' --cpu 2 --memory 2Gi --placement
duckfleet sql 'select 1' --isolated --cpu 4 --memory 8Gi   # a pod of its own
```

`duckfleet history`, `query <id>`, `usage`, `budget` and `workers` cover inspection.
A refusal exits non-zero and prints the server's own classification and `retry_safe`,
so a shell script can tell "ask differently" from "ask later" without parsing prose.

From Python, the same surface is a DB-API-shaped client:

```python
from duckfleet import RemoteConnection

connection = RemoteConnection(URL, TOKEN, compute={"cpu": 2, "memory": "2Gi"})
rows = connection.cursor().execute("select * from analytics.events where region = ?",
                                   ["eu"]).fetchall()
connection.close()   # rolls back anything uncommitted
```

`tests/api_workflow.py` runs this whole path — create, load with bound parameters,
query, read history and usage, then the same workflow through the CLI — in
`docker/cli.Dockerfile`, an image where dbt is **not installed**. Its first assertion
is that `import dbt` fails, because running it anywhere dbt happened to be present
would show that the API works rather than that it works without dbt.

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
| GET | `/v1/workers` | Worker IDs, engine settings, observed cgroup limits, and whether isolated compute is available |
| GET | `/v1/usage` | Append-only usage events with cursor pagination |
| GET | `/v1/budget` | Prepaid balance, held reservations, spend, caps, rate card |
| PUT | `/v1/budget` | Set the prepaid balance and per-job cost cap |
| POST | `/v1/credentials` | Issue a credential for a principal; the token appears only in this response (bootstrap only) |
| GET | `/v1/credentials` | List credentials without any secret material (bootstrap only) |
| DELETE | `/v1/credentials/{id}` | Revoke one credential immediately (bootstrap only) |

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

Revocation takes effect immediately on the gateway that processed it. Unknown,
revoked, and expired tokens are all refused with the same `401 unauthorized`,
so a probing caller learns nothing about which it was.

Credentials authenticate and attribute; they do not yet authorize differently.
Every principal reaches the same full SQL surface and the same platform
endpoints apart from credential management — per-principal SQL or budget
scoping is explicitly not built.

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
with no automatic retention or S3 archival yet. SQL, profiles, and errors can
contain sensitive literals. This is a single-tenant operational history API, not
a customer authorization boundary. A `running` record means the worker accepted
and began the request; queued operations are not yet durable history records.

The integration gate verifies filtering and pagination, observes/cancels a running
query without waiting behind it, rejects stale profiles after SQL failure, checks
history with both workers stopped, and confirms saved profile data survives
restart. `artifacts/query-profile.json` contains a real profiled dbt model query.

## Concurrent writes and retry safety

DuckLake detects a write conflict when a transaction **commits**, not while its
statements run. The first transaction to commit wins, and a losing transaction is
discarded whole rather than partly applied. Everything in this section was measured
against DuckDB 1.5.4 with a DuckLake PostgreSQL catalog and is re-verified on every
run by `tests/concurrency.py`, which interleaves two real worker sessions so the
conflict window is exact instead of a race. Evidence is saved to
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
`tests/metering.py` gates that on every run and saves `artifacts/metering.json`.

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

Two workers cannot both spend the last of the balance: admission locks the budget
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
with instructions, its uncommitted work can only be rolled back, and the Python
client rides new-session opens through a rollout's `draining` window and the
replacement gap on a bounded wait. Statements are never replayed by the client,
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
