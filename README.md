# DuckFleet

A managed DuckLake platform, starting with a local Docker feasibility harness.
The intended domain is `duckfleet.com`, pending purchase.

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

The current engine is DuckDB 2.0.1. See the [release, cutover, and rollback guide](docs/duckdb-2.0.1-release.md) and [engine qualification procedure](docs/engine-upgrades.md).

## Native CLI quickstart

Install the native `duckfleet` executable without Cargo or Docker, start the real local stack, and save project-local connection defaults:

```sh
brew install athvin/duckfleet/duckfleet
duckfleet init
duckfleet dev
duckfleet sql 'select 42'
```

`duckfleet.json` is non-secret and committable; `.duckfleet/` holds protected local state and is gitignored. See the [native CLI installation, configuration, and release guide](docs/cli.md) for direct downloads, checksums, supported architectures, and configuration precedence. Existing `DUCKFLEET_URL` and `DUCKFLEET_TOKEN` environment-based invocations remain supported.

Use `duckfleet dev --demo` to load the retail example once, `duckfleet status` to inspect readiness, `duckfleet stop` to retain data while stopping services, and `duckfleet reset` for confirmed project-scoped deletion.

## Supported installation

For a credentialed Linux installation without dbt or Kubernetes, follow the
[single-host guide](docs/single-host.md) and run
`bash scripts/install-single-host.sh`. The guide covers native architectures,
persistent capacity, external storage, TLS, backup/restore, upgrades and local
capacity measurement. `make test` below remains the disposable development
harness with explicit fixture credentials.

## Run locally

Requires Docker Engine with Compose v2 (Docker Desktop works) and Rust 1.99 for
repository operations. No host Python, dbt, Kubernetes, or AWS credentials are needed. Allow roughly 6 GiB of
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
  an Axum gateway routing to workers embedding DuckDB 2.0.1. Arrow IPC preserves result types.
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
default. Defaults can be set under
`models.<project>.+meta.platform` in `dbt_project.yml`, and the adapter receives
dbt's resolved config before its first SQL statement. Use `meta.platform.tags` for
workload attribution — tags never size compute.

To choose a size from evidence rather than by guessing, `duckfleet advise query <id>` or `duckfleet advise model <dbt-node-id>` reads retained history and profiles. It reports whether queueing, isolated startup, memory/spill, or CPU dominates, then suggests one explicit rerun. It is read-only and never changes compute; see the [advice guide](docs/cli.md#compute-advice-from-observed-evidence).

The pinned `athvin/dbt` adapter release `duckfleet-v0.1.6` forwards only resource
selection (`cpu`, `memory`, `disk`); it silently drops `mode` and `pool` metadata.
Use the HTTP API or CLI for explicit pool selection. Adding these fields to dbt
models does not enable the feature: a coordinated follow-up in
[the adapter repository](https://github.com/athvin/dbt) must validate and forward
them, test model/hook session affinity, and publish a new pinned release. Existing
dbt configurations keep their resource-based isolation behavior.

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
persistent tier. Where isolation is off (Compose) an isolated selection is refused `501
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
to pay node scale-up). Provisioning and SQL execution have separate budgets.
Rollout can still interrupt an open; clients distinguish a definite refusal
before admission from an uncertain response and never replay SQL. Shutdown allows
up to the longest admitted statement plus 15 seconds for cleanup (615 seconds),
inside a 630-second container grace. Idle drains finish after two seconds rather
than waiting for that maximum. External I/O and process loss can still prevent a
clean completion; retained request identity is the recovery evidence.

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
server/cli/             Native duckfleet executable, reusing the first-party CLI and client
server/sdk/              The published `duckfleet` Rust crate: the HTTP/Arrow client the CLI, harness, and SDK users share
server/src/harness.rs   The dbt + HTTP + Postgres + S3 integration gate (the harness role)
compose.yaml            PostgreSQL, RustFS, gateway, the persistent worker, dbt and CLI runners
docker/                 Images and initial PostgreSQL databases
scripts/                Build and harness helpers
tests/dbt/              Representative seed, view, table, incremental model, tests
```

The dbt adapter itself is not in this repo: it is a first-class `AdapterType::DuckFleet`
in a fork of dbt-core v2 (`athvin/dbt`), whose compiled `dbt` binary this repo downloads
by pinned release. A model's `meta.platform` (cpu/memory/disk) is what asks for a pod.

## Rust SDK

The [`duckfleet`](server/sdk/README.md) crate is the supported Rust client. It provides explicit sessions with transactions, cancellation, and compute selection, typed bound parameters, Arrow `RecordBatch` results, durable jobs with verified resumable downloads, and a stable `ErrorKind` that never replays SQL. The CLI and the integration harness use the same crate. Its minimum supported Rust version is 1.89. `make test-sdk` runs its real-Compose suite, and a `duckfleet-sdk-v<version>` tag publishes it to crates.io after the gates pass.

```rust,ignore
let client = duckfleet::Client::new(url, token)?;
let session = client.session(duckfleet::SessionOptions::named("app")).await?;
let result = session.query("SELECT ?::BIGINT + 1", [41_i64]).await?;
```

## Use it without dbt

dbt is an option, not a dependency. Everything the platform does is reachable from
the HTTP API. The native `duckfleet` executable packages the existing CLI role without
DuckDB, dbt, Cargo, or Docker as runtime requirements. Initialize a project or use the
existing environment variables:

```sh
export DUCKFLEET_URL=http://localhost:18080
export DUCKFLEET_TOKEN=duckfleet-local-development-token
duckfleet init

duckfleet sql 'create or replace table analytics.events (id integer, region varchar)' \
              "insert into analytics.events values (1, 'eu')"
duckfleet sql 'select region, count(*) from analytics.events group by region'
duckfleet sql 'select * from analytics.events where region = ?' --param eu --format json
```

Use `duckfleet shell` for a persistent multiline session with completion, secure history, transaction-aware prompts, table inspection, four output formats, and explicit Ctrl-C cancellation. `duckfleet sql --file <path>` and piped stdin provide the same session semantics for scripts; see the [CLI guide](docs/cli.md#interactive-sql-shell).

```sh
duckfleet shell
duckfleet sql --file ./load.sql
```

Catalog discovery and bounded previews use the same authorized session and safely parsed identifiers:

```sh
duckfleet schemas
duckfleet tables --schema analytics
duckfleet views --schema analytics
duckfleet describe analytics.events
duckfleet preview analytics.events --limit 20
```

Local CSV and Parquet files import into the remote lake without mounting a host directory into a worker. The CLI streams a bounded, checksummed file through private staging, then performs an atomic create or append with DuckDB's native ingestion semantics:

```sh
duckfleet import ./events.parquet --table raw.events --create
duckfleet import ./more-events.csv --table raw.events --append
duckfleet import ./legacy.data --format csv --delimiter '|' --no-header --table raw.legacy --create
```

Exactly one mode is required. Existing tables are never overwritten; schema/coercion and parse failures roll back the entire transaction. See the [CLI import guide](docs/cli.md#import-local-csv-and-parquet-files) for type inference, file limits, cancellation, and staging cleanup.

Query results and finished durable jobs export to local CSV, JSON, Arrow, or Parquet through the durable-job path. Chunks are verified, the publish is atomic, existing files are never clobbered without `--force`, and interrupted downloads resume:

```sh
duckfleet export 'select * from raw.events' --output events.parquet
duckfleet export --job <request-id> --output result.csv --resume
```

See the [CLI export guide](docs/cli.md#export-queries-and-durable-jobs).

DuckLake snapshots are readable from the CLI: `duckfleet snapshots` lists them, `duckfleet snapshot preview <table> --at <id>` time-travels, and `duckfleet snapshot diff <table> --from <a> --to <b>` reports bounded added/deleted/updated rows. Every one of these is a read that never expires snapshots or changes current data. See the [snapshot guide](docs/cli.md#ducklake-snapshots-and-time-travel).

History defaults to a SQL-free table with existing filters, cursor pagination, and bounded watch. Profiles expose readable operator and engine-metric evidence without confusing it for container memory or cost:

```sh
duckfleet history --status failed --worker-id small
duckfleet history --watch --interval 2
duckfleet profile <query-id>
duckfleet profile <query-id> --metrics
duckfleet profile <query-id> --raw
```

The CLI uses the HTTP/Arrow client in `server/src/client.rs`. The external dbt
adapter speaks the same API; its supported selection fields depend on the pinned
adapter release described above.

Statements given to one `sql` invocation share a session, which is what pins a load
and the query that checks it to the same worker. Compute is selectable per invocation:
naming any of `--cpu`/`--memory`/`--disk` asks for a pod of that size, the same derived
trigger a dbt model expresses through `meta.platform`; naming none runs on the shared
persistent tier.

Explicit `--mode persistent --pool interactive` runs on a configured prestarted
pool. Resource flags with that selection specify minimum requirements for the
pool; the actual configured allocation is returned and metered. Invalid or
repeated selection flags fail before opening a session.

```sh
duckfleet sql 'select current_setting("threads")' --cpu 2 --memory 2Gi
duckfleet sql 'select 1' --cpu 4 --memory 8Gi --disk 20Gi   # a pod of its own
duckfleet pools
duckfleet sql 'select 42' --mode persistent --pool interactive --memory 2Gi --placement
```

`duckfleet history`, `query <id>`, `request <id>`, `cancel <request-id>`, `usage`, `budget`, `workers` and `pools`
cover inspection. `request <id>` reads durable outcome metadata even after the
diagnostic history has expired.
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
| GET | `/v1/pools` | Configured persistent pools, allocations, capacity limits and current eligible-worker observations |
| POST | `/v1/pools/reload` | Validate and reload the operator-configured pool file; fleet management permission |
| POST | `/v1/sessions/{id}/queries` | Durably admit SQL and await Arrow; reports request ID, status URL and acceptance |
| DELETE | `/v1/sessions/{id}` | Close connection and roll back any open transaction |
| POST | `/v1/sessions/{id}/cancel` | Interrupt this session's currently executing statement |
| POST | `/v1/imports/{request_id}/upload` | Stream one bounded, checksummed CSV or Parquet file into owner-fenced private staging |
| GET | `/v1/imports/{request_id}` | Owner-only import metadata and staging state |
| DELETE | `/v1/imports/{request_id}` | Owner-requested staged-object cleanup; maintenance retries failures |
| GET | `/v1/queries` | Filtered history with cursor pagination; defaults to 500 records |
| GET | `/v1/queries/{query_id}` | One durable record, metrics, and raw DuckDB profile when available |
| POST | `/v1/requests/{request_id}/cancel` | Durably request cancellation independently of engine availability; authorized own/all cancellation |
| GET | `/v1/requests/{request_id}` | Authorized own/all durable identity, attempt lineage, outcome, and diagnostic availability |
| GET | `/v1/workers` | Every configured worker's state and execution pressure, observed limits, and whether isolated compute is available |
| GET | `/v1/usage` | Append-only usage events with cursor pagination |
| POST | `/v1/system/query` | Bounded SQL over authorized history, attempts and usage; JSON or Arrow |
| GET | `/v1/system/schema` | Versioned column catalog with the caller's visibility and masking |
| GET | `/v1/budget` | Prepaid balance, held reservations, spend, caps, rate card |
| PUT | `/v1/budget` | Set the prepaid balance and per-job cost cap |
| GET | `/v1/lake/snapshots` | DuckLake snapshots, as an Arrow stream |
| POST | `/v1/lake/maintenance` | Validated snapshot expiry, compaction, and cleanup (maintenance and compute grants) |
| POST | `/v1/credentials` | Issue a credential; token appears once (credential management permission) |
| GET | `/v1/credentials` | List credentials without secret material (credential management permission) |
| DELETE | `/v1/credentials/{id}` | Revoke one credential (credential management permission) |
| GET | `/v1/principals` | Paginated stable principals (grant management permission) |
| GET/PUT | `/v1/principals/{principal}/policy` | Inspect or atomically replace a principal policy |
| GET | `/v1/audit` | Paginated append-only audit events (audit read permission) |

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

New persistent sessions consider only their selected pool and exact configured
CPU, memory and disk allocation. They prefer eligible workers with fewer active plus queued
queries, then fewer open sessions, with configured endpoint order breaking ties.
Successful opens adjust the cached session count used for ranking without
re-probing an unreachable sibling; reported counters remain the last sampled
values. These are recent observations, not reservations of execution capacity: concurrent opens may see the same snapshot, and workers still enforce
their own admission limits. Existing sessions stay pinned to their worker, and
SQL is never replayed or moved as part of placement. The local harness verifies
status during a query running longer than five seconds, mixed HTTP/pgwire
placement, and a stopped sibling that remains visible in the fleet report.

## Explicit compute selection and persistent pools

A session resolves its compute policy once. Its response and query history retain
`requested_compute`, `resolved_compute`, the registry revision, and observed
worker placement. CPU, memory and disk are independent configuration dimensions.
A pool is a group of prestarted workers sharing one allocation; it is not a data
or security boundary.

| Compute request | Placement |
| --- | --- |
| Omitted or `{}` | Configured default persistent pool |
| Resource fields only | Isolated pod; unspecified dimensions retain 1 CPU / 1Gi / 2Gi defaults |
| `{"pool":"interactive"}` | Named persistent pool |
| `{"mode":"persistent"}` | Default persistent pool |
| Persistent mode/pool plus resource fields | Named/default pool must satisfy every supplied minimum |
| `{"mode":"isolated"}` | Isolated pod with historical defaults |
| Isolated mode plus a pool, unknown pool, malformed quantities, insufficient allocation | `400 invalid_request` before SQL |
| Known pool without an eligible worker | `503 no_ready_worker`; no fallback to another pool |

For example, open a persistent session through HTTP:

```sh
curl -H "Authorization: Bearer $DUCKFLEET_TOKEN" -H 'Content-Type: application/json' \
  "$DUCKFLEET_URL/v1/sessions" \
  -d '{"compute":{"mode":"persistent","pool":"interactive","memory":"2Gi"},"context":{"workload":"interactive"}}'
```

Operators supply either `DUCKFLEET_POOLS_FILE` (a JSON file) or
`DUCKFLEET_POOLS_JSON` (inline JSON). The two sources are mutually exclusive.
Without either, the gateway synthesizes the legacy `small` pool from
`DUCKFLEET_WORKER_URLS`. An example registry is:

```json
{"default_pool":"small","pools":{
  "small":{"cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":["http://worker-small:8080"],"max_sessions_per_worker":32},
  "interactive":{"cpu":2,"memory":"3Gi","disk":"4Gi","endpoints":["http://worker-pool-interactive-0:8080"],"max_sessions_per_worker":32}
}}
```

Workers declare their pool with `DUCKFLEET_POOL`; isolated workers have no pool.
A registry does not resize a container: configure its resource limits and worker
settings together. A named pool also requires its session cap to match the worker-enforced cap;
changing resources or that cap requires a worker rollout. A mismatched worker
cannot serve the pool. The optional Compose
`pools` profile supplies the example above through a gateway on port 18081:

```sh
docker compose --profile pools up -d --build --wait server-pools
```

See [infra/README.md](infra/README.md) for named Helm pools, replica counts,
legacy-value migration and resource configuration.

After replacing the configured JSON file, the bootstrap operator can call
`POST /v1/pools/reload` with an empty body. A complete valid registry replaces the
previous snapshot atomically; an invalid file leaves it intact. Projected
ConfigMap updates may take time to reach the container; inspect the returned
revision. Inline and legacy environment configurations require gateway restart.
Adding/removing routing membership or changing the default does not move existing
sessions: their transactions and temporary tables stay on the original worker.
Replacing the gateway or worker process still ends its sessions. Drain workers
before removing them permanently; after a crash, recover the same stable worker
identity and settle its holds before retirement.

`DUCKFLEET_GATEWAY_MAX_SESSIONS` defaults to 64 shared backend sessions across
HTTP, pgwire and platform operations. `DUCKFLEET_WORKER_MAX_SESSIONS` defaults to
32 per persistent worker and `DUCKFLEET_WORKER_QUEUE_CAPACITY` to 64 pending
operations; one worker executes SQL serially. Pools expose configured limits and
sampled availability separately. Isolated workers still serve one session for
their lifetime. These are concurrency limits, not provisioned-cost ceilings;
provisioned/idle compute accounting and budget caps are tracked in #107.

Session permits remain held while a backend close is unconfirmed. Discovery
reports `gateway_closing_sessions` and `gateway_closing_endpoints`, including
endpoints removed from the current registry. Restore reachability so the worker
can acknowledge closure before permanently retiring an endpoint. A reload or a
network timeout does not prove a backend session has ended; an unreachable
endpoint can therefore hold capacity until cleanup succeeds. Gateway restart
abandons its routes and ends client sessions; it is not transaction recovery.

[Durable query jobs](docs/jobs.md) provide submit/status/cancel and private Arrow results.
Job runtime, result size and retention limits are configured separately from
interactive query limits. Compose supplies local private result storage; Kubernetes
requires explicit result-prefix IAM before enabling `results.enabled`. See
[job storage and policy configuration](infra/README.md#durable-jobs-and-private-results)
and the [Helm example](examples/jobs-values.yaml). Platform result permissions do
not isolate untrusted SQL from worker credentials.

## Credentials

The gateway's `DUCKFLEET_TOKEN` is the operator-held bootstrap recovery credential.
It can repair grants when named administrators lose access. Workers instead receive
`DUCKFLEET_INTERNAL_TOKEN`; they reject the public bootstrap token. Keep both
secrets distinct. Internal-worker endpoints are trusted service interfaces.

Issue a named user or service credential with `POST /v1/credentials`
(`{"principal":"analyst","expires_seconds":86400}`, expiry optional). Only
credential administrators can issue, list, or revoke tokens. The token appears
once in the response; PostgreSQL stores its SHA-256. Each principal has a stable
ID in one persisted deployment namespace. Names cannot be renamed or reused;
revoked credentials and disabled principals remain tombstones for attribution.
Caller headers, pgwire usernames, SQL settings, and query context never supply
roles, namespace, or authenticated actor identity.

Permissions live in the existing platform database. Presets are additive, and
explicit grants can add a specific capability without a policy service:

| Preset | Execute | Cancel / close | History / usage | SQL text / raw profiles | Administration |
| --- | --- | --- | --- | --- | --- |
| `analyst` | Own sessions, granted compute | Own | Own | Own | None |
| `viewer` | Own sessions, reader pools only | Own | Own | Hidden | None |
| `operator` | Fixed maintenance only, granted compute | All | All | Hidden | Fleet/budget reads and maintenance |
| `admin` | Own sessions, granted compute | All | All | All | Credentials, grants, budgets, pool reload, audit |

`own` means the immutable workload owner within this namespace. An operator's
cancellation does not transfer ownership. Organization-wide history access does
not let a named administrator execute SQL in someone else's session or claim
their request ID or retry chain. Bootstrap recovery is a separate actor kind.

### Reader and writer data authority

`viewer`, `analyst`, `operator`, and `admin` presets also select a data-authority
profile. Viewers without a broader role are **readers**; the other presets and the
bootstrap recovery actor are **trusted writers**. A pool declares
`authority: reader` or `writer` (the default). A reader with no explicit pool uses
the default only when it is a reader pool; otherwise exactly one reader pool must
be configured, and multiple reader pools require an explicit choice. Changing
roles cannot move or recreate an existing session: the next operation refuses the
authority mismatch and the client must close and reopen with current grants.

Reader workers are separate processes, not sessions cloned from a writer engine.
They receive only `CATALOG_READER_DSN` and reader storage credentials, never the
writer/migrator DSNs or result-write credentials. PostgreSQL grants `USAGE` on the
catalog schema and `SELECT` on current and future catalog tables; storage policy
allows lake reads and denies writes/deletes. `READ_ONLY` attachment is defense in
depth, not the authorization mechanism. Reader durable-job result storage is
disabled. Writers remain trusted catalog actors and need their existing DDL/DML
and object-write privileges; migrations and maintenance use their documented
operator identities.

This is coarse namespace/catalog authority, not table grants, column masking, row
policies, or hostile multitenancy. Readers can see catalog metadata and any data
inlined into it. With inlining disabled, the storage policy protects Parquet data.
Direct `ATTACH`/`COPY`, alternate object paths, extension/configuration changes,
and credential-discovery attempts cannot elevate a correctly provisioned reader
into a writer. Compose starts an isolated proof on port 18082 with
`docker compose --profile readers up -d --wait server-readers`; its registry is
`examples/pools/compose-reader.json`.

Row visibility, SQL visibility, and profile visibility are separate decisions.
SQL permission covers SQL text, arbitrary context/model tags, and raw error text.
Full profiles require **both** `sql.read` and `profile.read`, because engine profiles
can contain SQL and expressions. Metadata-only readers receive typed state,
placement, timestamps, cost, and curated numeric metrics. Hidden payload fields
are absent or null; filtering on a hidden sensitive field is refused. Invisible
history/request IDs use the same not-found response as absent IDs. Row scopes
are applied in PostgreSQL before pagination, and HTTP metadata remains available
while execution workers are stopped. The same policy protects the [SQL system relations](docs/system-schema.md).

Manage a whole principal policy with
`GET/PUT /v1/principals/{principal}/policy`. A PUT replaces its roles, explicit
permissions, complete compute grants, and disabled state atomically. For example,
this policy grants an organization observer metadata without SQL text or execution:

```json
{
  "roles": ["viewer"],
  "permissions": [
    {"action":"history.read","scope":"all"},
    {"action":"usage.read","scope":"all"}
  ],
  "compute": [],
  "disabled": false
}
```

The CLI supports `principals`, `credentials`, `policy <name>`,
`policy <name> --file policy.json`, and `audit`. Listings accept `--limit` (1–500),
with `--after` for principals/credentials and `--before` for audit; responses carry
the next cursor. Create the principal by issuing its first credential, inspect its
policy, then replace it deliberately. Giving a role does not silently grant every
pool or isolated mode.

A compute grant is one complete allocation envelope:
`{"mode":"persistent","pool":"small","cpu":1,"memory_bytes":1073741824,"disk_bytes":2147483648}`.
An isolated grant uses `"mode":"isolated","pool":null` plus all three ceilings.
One grant must cover the resolved allocation; CPU from one grant cannot combine
with memory from another. Checks run before provisioning, after provisioning
waits, at durable submission/grant, and immediately before SQL. Operational queue,
session, and budget caps apply independently. Fixed maintenance also needs a
compute grant. API maintenance rebuilds validated server-owned SQL; setting
`context.operation` cannot authorize arbitrary SQL.

HTTP and pgwire retain the authenticated credential ID, principal, and expiry.
Authentication uses a positive cache of at most 15 seconds, never past expiry;
revocation invalidates that gateway's cache before returning. Authorization has
no policy cache: each operation resolves the current credential, principal state,
and complete policy snapshot. Workers repeat the check after queue and budget
waits as the final metadata step before engine entry. Revoked credentials or
grants prevent queued and subsequent SQL from starting. Already active SQL may
finish within its existing deadline, at most 600 seconds, unless explicitly
canceled. A grant change never replays SQL.

Unknown, expired, revoked, and disabled credentials return `401 unauthorized`;
missing permissions return `403 forbidden`. Database failures refuse new work.
Pgwire checks before lazy session creation, every statement, and at five-second
idle intervals. Idle-session cleanup rechecks current authority: a stale revoke
observation cannot close a session after a regrant. Transient policy lookup failure
leaves cleanup pending and execution unavailable, rather than proving revocation.
Closing a session rolls back its uncommitted transaction.

Rotate credentials by issuing another for the same principal, durably accepting
work with it on each HTTP session, then revoking the old credential. Accepted work
rotates cleanup authority immediately, even while queued; rejected requests do
not. Cleanup compares the durable authority generation. Pgwire clients reconnect
because their password is fixed for the connection. Principal-wide policy changes
apply to both old and replacement credentials.

Credential, grant, budget, cancellation, and privileged-operation decisions append
auditable events. External effects have a committed start event and a subsequent
outcome event; an unavailable outcome write is reported without pretending the
external effect rolled back. Audits exclude bearer tokens, SQL, arbitrary context,
profiles, and raw errors. Privileged effects fail if their required start audit
cannot persist. Denial still denies during an audit/database outage. Usage and
reservations retain authenticated actor/credential IDs directly, separately from
the workload owner, so pruning history does not erase attribution. Unknown legacy
actors remain unknown. A denial under a revoked accepted credential uses its
retained actor; audit `policy_version: 0` means current authorization could not be
resolved, rather than inventing a current grant snapshot.

Schema 3 deliberately tightens upgrade defaults: existing named principals become
analysts with own visibility and one bounded default persistent grant. The default
is `small`, 1 CPU, 1 GiB memory, 2 GiB disk. Before migration, deployments with a
different default pool must review `DUCKFLEET_AUTHZ_DEFAULT_POOL`, `_CPU`, `_MEMORY`,
and `_DISK` (each has the `DUCKFLEET_AUTHZ_DEFAULT` prefix). Extra pools, isolated
execution, and organization-wide debugging require explicit grants. Repeating
migration does not restore revoked application grants. The configured
`DUCKFLEET_NAMESPACE` must match the persisted deployment identity.

Platform owner/migrator, gateway, worker, maintainer, and lake-catalog PostgreSQL
credentials are separate. Runtime roles cannot update, delete, truncate, alter,
or own audit/usage tables. Workers cannot read token hashes or manage credentials,
grants, or budget limits. Worker pods receive neither the public bootstrap nor
migration credentials. Compose provisions the roles; external PostgreSQL uses the
explicit privileged provisioning procedure in [infra/README.md](infra/README.md).
Startup rejects unsafe owner/superuser runtime DSNs.

These permissions protect supported platform interfaces. SQL executors remain
trusted with their worker's files, network access, catalog connection, and internal
service authority until #106 separates execution authority. Platform roles alone
are not table permissions, row policies, or a hostile-SQL sandbox.

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

Pgwire uses the configured default persistent pool when its backend session
opens. This release has no per-connection pool selection mechanism; database
names and startup options do not select a pool. A pool reload affects new backend
sessions, while existing ones retain their worker and allocation.

The honest limits, refused rather than fudged: no extended-protocol bind
parameters (interpolate client-side, as `psql` and psycopg2 already do), no
COPY, LISTEN/NOTIFY, cursors, or CancelRequest, and SSLRequests are answered
`N` — this listener is in-cluster plain TCP, off by default, and transport
security belongs to the exposure layer. Results travel in text format with
typed OIDs (ints, floats, booleans, text, decimals, dates, microsecond
timestamps); anything else says to cast to `VARCHAR`. A gateway rollout ends
wire connections; reconnect and the session-recovery contract applies.

## Worker file access

After startup, a worker's engine can reach only four places from SQL: the lake data path, the writer's private import staging prefix, its spill directory, and its profile output. The worker sets DuckDB's `allowed_directories` to exactly those prefixes and then sets `enable_external_access=false`, which DuckDB cannot re-enable while running. As a result, SQL from any principal, over HTTP, durable jobs, or pgwire, cannot read worker-local files. That includes `/proc/self/environ`, which holds platform credentials. SQL also cannot run `COPY ... TO STDOUT`/`FROM STDIN`, which would otherwise write rows to the worker's own stdout and logs, fetch arbitrary URLs, or touch object prefixes outside the lake. Refusals name `duckfleet export` and `duckfleet import` as the supported ways to move files. `make test-local-io` verifies this against real Compose.


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
the render always carries the TLS block, ``duckfleet-ops check-chart`` asserts every exposed
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
  "timeout_seconds": 30,
  "queue_timeout_seconds": 10
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

Each `request_id` identifies one immutable execution attempt across all sessions
and workers. A compact PostgreSQL identity record has no diagnostic-retention
expiry and contains no raw SQL, parameters, or profile. Reusing the ID with the
same payload returns HTTP 409 `duplicate_request`, including the original
outcome and `diagnostics_available`; it never executes SQL again. Reusing it with
a different payload returns 409 `request_id_mismatch`. Results are not cached.

New attempts use the version-3 SHA-256 fingerprint covering exact SQL bytes,
validated bound parameters (including number types), effective execution and queue timeouts,
application context, original compute requirements and resolved pool/allocation.
Version-1 and version-2 records remain comparable using their frozen historical
encodings; a known historical budget refusal can produce a linked version-3 retry.
Stored hashes are never rewritten from a replay. JSON object key order is irrelevant;
array order and value types matter. It excludes attempt IDs, credential IDs,
worker identity, boot identity and pool-registry revision. It does not fingerprint ambient session or
database state, or establish semantic equivalence between different SQL text.

`GET /v1/requests/<id>` follows own/all history permissions. An invisible ID
returns the same 404 as an absent ID. Duplicate submission and retry ownership
remain restricted to the workload owner or bootstrap recovery; observation grants
do not transfer them. Credential rotation preserves the immutable owner.

After a `budget_exhausted` refusal, fund the budget and submit the same payload
with a **new** `request_id` and `"retry_of": "<refused-attempt-id>"`. The response
from the old ID stays a duplicate of its refusal. Both attempts retain the same
`logical_request_id`, and only one child attempt may reference each refusal;
retry a refused child by referring to that latest child. The lookup's `retry.allowed`
describes retained outcome eligibility, not the reader's current permission to retry.
New submission still checks immutable ownership, current grants, compute, and budget;
all-history access does not grant execution. Other kinds of
failure do not become permission to retry through this mechanism.

Identity, diagnostic acceptance, bounded SQL payload and admission permits commit
together before the submission becomes accepted. Budget preparation follows when
the scheduler selects the request. Terminal identity, diagnostic completion, usage
settlement, payload deletion and permit release commit together. `outcome` distinguishes `not_executed`, `statement_result`, and
`unknown`. A statement result does not prove an enclosing transaction committed.
A dropped connection or worker can leave an unknown outcome; inspect durable
identity, diagnostics if present, and application state before deciding what to
do. Workers never automatically replay SQL. Restart reconciliation preserves
uncertainty for that worker's unfinished attempts and leaves other workers alone.

Before recovery, each worker holds an exclusive PostgreSQL advisory lock for its
stable identity on a dedicated connection. A replacement waits while its
predecessor drains, including replacements caused by deleting a Kubernetes pod.
A separate heartbeat runs every two seconds throughout SQL execution; PostgreSQL's
ten-second idle-session timeout releases abandoned ownership connections even
when a dead pod's socket is not promptly reaped. Losing ownership interrupts active
SQL, stops further admission and exits the worker; it is not silently reacquired.
An in-flight write can still have an unknown
outcome when database connectivity or the process is lost. This lock does not
provide distributed transaction fencing during a network partition.

Upgrades backfill compact identities once from history and retained ledger keys.
Historical parameters cannot be reconstructed, so these records permanently
report `fingerprint_status: legacy_unavailable` and never acquire a fingerprint
from a replay. Missing or historical fallback ownership is operator-only.
Retained reservations continue guarding against duplicate execution. Quiesce
writers while upgrading before relying on the new fingerprint/retry contract;
older workers do not implement it. Keep identity records and the usage ledger
when pruning diagnostic history.

The durable query queue uses the existing platform PostgreSQL database. One
accepted request may be outstanding per session, including budget preparation
and settlement; a different simultaneous request receives `409 session_busy`.
This preserves the actual DuckDB connection, transactions, temporary tables and
settings. There is no connection migration or statement pipeline.

Queue allowance defaults to 10 seconds (1–120); execution defaults to 30 seconds
(1–600). Queue time includes scheduling, preparation and final authorization;
execution time starts at engine entry. Use `--queue-timeout 60` with CLI `sql`
to select a longer allowance. HTTP clients can explicitly set `timeout_seconds`
up to 600; the gateway-to-worker wait covers the validated queue plus execution
allowances and 15 seconds for delivery. The bundled CLI retains its 75-second
transport wait and 30-second execution default. External clients requesting
longer statements must also allow a suitably bounded HTTP wait. Shutdown allows up to 615 seconds within a 630-second container grace,
covering the maximum statement allowance and cleanup. Idle drains remain short. A timeout or disconnect does not cancel accepted work. On an uncertain
response, inspect `request <id>` or call `cancel <id>`; the CLI retains this ID
and reports `acceptance: unknown`. Results are not retained for later download.

Reproduce the long-statement boundary on disposable Compose volumes with
`bash scripts/ops.sh test-long-deadline --image <built-image> --output artifacts/long-deadline.json`
(uses the Rust Arrow reader). It delays a synthetic HTTP CSV response by 65 seconds,
checks success with a 120-second allowance and refusal with a one-second
allowance, and rejects 601 seconds. External I/O can defer when DuckDB observes
an interrupt; the report records actual elapsed time rather than claiming an
HTTP read stops immediately at the statement deadline.

Parsed query responses carry `x-query-id`, `x-query-status-url` and
`x-query-acceptance` (`accepted`, `not_accepted`, or `unknown`). Classified JSON
errors also include `request_id`, `status_url` and `acceptance`. A cancellation
request is a durable latch; its acknowledgement is distinct from confirmed
terminal cancellation. Queued cancellation or expiry executes no SQL. Active
cancellation interrupts the engine and may leave an unknown write outcome.

Defaults bound each worker to 64 queued queries and 16 MiB of payloads, each pool
to 256 queued and 16 preparing/executing queries, and each principal within a
pool to 16 queued and the pool's running allowance. PostgreSQL live permits
coordinate these limits across workers. The worker operation channel remains a
separate 64-operation bound; session defaults remain 32 per worker and 64 per
gateway. The pool registry's optional `admission` object overrides
`max_queued_queries`, `max_running_queries`, `max_queued_per_principal` and
`max_running_per_principal`; omitted principal running limits inherit the pool
limit. See [infrastructure configuration](infra/README.md) for environment and
Helm settings. Queue/session refusals never authorize blind transaction replay.

The scheduler gives continuously eligible principals on one worker equal turns,
then chooses the oldest queued session request for that principal. Many sessions
do not buy a principal extra turns. Active SQL is nonpreemptive; affinity and
shared caps mean this is not a pool-wide latency guarantee. HTTP, pgwire and
platform SQL use the same admission and scheduling path. The current pgwire
ReadyForQuery `I` marker is a compatibility limitation, not reliable transaction
state; native wire CancelRequest remains unsupported. Pgwire notices expose the
request ID for authenticated HTTP cancellation and inspection.

Shutdown stops admission and scheduling immediately. Queued/preparing requests
become `queue_drained`, without execution and without replay permission. Only
already active SQL may finish during the shutdown grace. After ownership-proven
worker replacement, old queued/preparing work becomes `session_lost`, while
old executing work retains an unknown interrupted outcome. Isolated recovery
includes work with no budget reservation and requires confirmation that its pod
is absent. No request is automatically replayed after a worker or gateway loss.

Responses fail rather than silently truncate beyond 100,000 rows or 16 MiB.
Each worker has a 1 GiB spill limit and the resource settings listed above.
These are local resource limits, not billing budgets.

## SQL system tables

Use `duckfleet system tables`, `duckfleet system describe query_history`, or
`duckfleet system sql '<query>'` to inspect `duckfleet_system.query_history`,
`query_attempts`, and `usage`. HTTP serves `/v1/system/query` and `/v1/system/schema`;
pgwire selects this mode with database `duckfleet_system`. These bounded,
authenticated reads work with every execution worker stopped and require no
additional service. See the [SQL examples, permissions, and limits](docs/system-schema.md).

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
`profileSamplePercent` (environment `DUCKFLEET_PROFILE_SAMPLE_PERCENT`) selects a
deterministic percentage of sessions whose raw JSON persists. Sampling still
captures profiles to obtain compact metrics, so it reduces storage, not capture
overhead; sampled-out records say `sampled_out`. Open context
`{"profile":"debug"}` explicitly retains a full profile even at 0%. Expired raw
profiles say `expired`, and SQL text has a matching `sql_status`.

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
authorization boundary. Accepted work has durable `queued`, `preparing`, `executing` and terminal states.
Queue, selection, engine and terminal timestamps are distinct; historical or
unobserved timings remain null. Gateway provisioning spans are separate from
query queue time and are best-effort diagnostics when metadata is unavailable.

History retention is configurable and off by default: `historyRetentionDays: 0`
keeps everything. With a window set, the gateway coordinates hourly cleanup in
batches of at most 5,000 rows, with two batches per pass. A backlog makes the
next bounded pass due sooner. Running requests, held reservations, and rows
without a durable terminal identity remain protected. Request identities and
append-only usage events have no diagnostic retention policy. SQL text and raw
profiles also have independent `sqlRetentionDays` and `profileRetentionDays`
settings (matching `DUCKFLEET_SQL_RETENTION_DAYS` and
`DUCKFLEET_PROFILE_RETENTION_DAYS`). Zero keeps each artifact forever, preserving
upgrade behavior. Artifact expiry nulls only that payload, retains the compact
summary/metrics, and never touches usage evidence. `GET /v1/retention` requires
fleet-read permission and reports stored profile/SQL counts and bytes, expired
artifact counts, and the last cleanup's status, affected count and backlog.
A small installation can keep summaries for 90 days, SQL for 7 and profiles for 1
while sampling 10%; this trades forensic detail for bounded storage.

## Platform migrations and maintenance

Workers validate the platform schema and recover only their own stable identity.
They do not create global tables, aggregate the lifetime usage ledger, or prune
history. Startup logs report the metadata recovery duration separately from
DuckDB/catalog initialization. Both workers and the gateway wait up to
`DUCKFLEET_PLATFORM_WAIT_SECONDS` (default 120) for a missing or known older schema and reject
unsupported future versions with an actionable error.

Deployment runs `DUCKFLEET_ROLE=migrate` using the same binary and its dedicated
owner/migrator `PLATFORM_DSN`; runtime processes use their own restricted DSNs.
This PostgreSQL-only command supports a fresh database, unversioned installations
and ordered schema-1 through schema-6 upgrades. Schema 7 adds independent SQL and
profile artifact states; existing interactive request
fingerprints, credentials, policies and usage are preserved. It adopts existing request identities,
reservations and usage without refilling the budget. Migrations commit their
version atomically; repeats skip the baseline scans. Quiesce older writers for
an upgrade and preserve the `platform` database along with the DuckLake catalog.

Compose gates workers on the successful `platform-migrate` service. Kubernetes
uses an ordinary migration Job so bundled PostgreSQL can start before migration
and compute can wait for a compatible schema. Deployment helpers check that the
desired Job completed, including when old pods are still healthy. A failed Job
prevents rollout success; inspect its logs before retrying. Suspended releases do
not launch migrations.

Schema-changing upgrades require stopping old gateways and every persistent and
isolated worker before migration. The deployment guard refuses a live older
schema; run `bash scripts/stop-compute.sh compose` (or `kube` with an explicit
context/namespace), then deploy the new image. PostgreSQL, object storage and
volumes remain available. Same-schema updates need no extra stop.

After restoring or retargeting a database, explicitly rerun migration even when
the image has not changed:

```sh
bash scripts/migrate-platform.sh compose
# Explicit privilege repair after restore or database-role changes:
bash scripts/migrate-platform.sh compose platform-grants
# Kubernetes: set DUCKFLEET_KUBE_CONTEXT and DUCKFLEET_NAMESPACE first.
bash scripts/migrate-platform.sh kube
```

A direct Helm deployment can instead change `platformMigration.runId`. The restore
helper stops compute, restores both databases, and migrates before reporting
success; it leaves compute stopped for an explicit restart. A failed restore or
migration never restarts compute against partial state.

The existing gateway attempts due maintenance every minute. PostgreSQL owns a
30-second lease with a fencing generation; each work transaction locks and
validates it. Defaults allow 10 seconds to start work, three seconds per SQL
statement and 250 ms for lock acquisition, followed by bounded result bookkeeping.
Lost transactions have an idle timeout, and a successor marks unfinished runs
interrupted after lease expiry. Retention and six-hour budget reconciliation
have independent schedules; the oldest due task runs first so a retention backlog
cannot keep reconciliation waiting. A failed task retries after a minute. Reconciliation
locks the budget before calculating totals so concurrent settlement deltas are
preserved. Maintenance never infers worker death from a request's age.

No additional always-on service is required. With gateways stopped, an operator
can run one bounded pass independently of execution workers:

```sh
docker compose run --rm --no-deps -e DUCKFLEET_ROLE=platform-maintain server
```

A standalone binary needs only `PLATFORM_DSN` and that role. The command returns
JSON describing whether it acquired ownership and each task's outcome. For a
large deliberate repair, `DUCKFLEET_MAINTENANCE_STATEMENT_SECONDS`,
`DUCKFLEET_MAINTENANCE_PASS_SECONDS` and `DUCKFLEET_MAINTENANCE_LEASE_SECONDS`
can raise the bounds; lease duration must exceed the pass budget, which must be
at least the statement timeout. Keep recurring defaults small to protect query
admission. Operators can inspect PostgreSQL directly:

```sql
SELECT task, next_due_at, last_success_at, last_failure_at, last_error
FROM platform_maintenance_tasks;
SELECT task, started_at, finished_at, status, rows_pruned,
       backlog_remaining, oldest_eligible_at, error
FROM platform_maintenance_runs ORDER BY started_at DESC LIMIT 20;
```

The integration fixture verifies failed-migration rollback, supported adoption,
future-version refusal, competing owners, server-driven abandoned-transaction
rollback, retention backlog and concurrent accounting. It measures nine metadata
startup samples before and after adding 50,000 history rows and 50,000 usage rows,
with the global budget locked during the larger case. Results are written to
`artifacts/platform-startup-volume.json`; this measures schema validation and
worker-local recovery, excluding DuckDB/catalog attachment, and is not a universal
startup latency promise.

A local Docker Desktop Linux/aarch64 run with PostgreSQL 17.6 measured a median
of 1.00 ms for the small fixture and 1.70 ms after those additional rows, across
nine samples per case. The larger case completed while another connection held
the budget row lock. These are measurements of the metadata phase on that
fixture, not end-to-end cold-start timings or a hardware-independent target.

The integration gate verifies filtering and pagination, observes/cancels a running
query without waiting behind it, rejects stale profiles after SQL failure, checks
history with both workers stopped, and confirms saved profile data survives
restart. `artifacts/query-profile.json` contains a real profiled dbt model query.

## Concurrent writes and retry safety

DuckLake detects a write conflict when a transaction **commits**, not while its
statements run. The first transaction to commit wins, and a losing transaction is
discarded whole rather than partly applied. Everything in this section was measured
against DuckDB 2.0.1 with a DuckLake PostgreSQL catalog and is re-verified on every
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
apart. `duckfleet advise dbt` finds such pairs statically from
`target/manifest.json` and can correlate them with recorded conflicts; see the
[dbt conflict guide](docs/cli.md#dbt-write-conflict-warnings). A write becomes visible to the other worker as soon as it commits, including
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
| `budget_exhausted` | 402 | yes | Nothing ran or was metered; after funding, use a new ID with `retry_of` pointing to this refusal |
| `capacity` | 429 | yes | A queue or session limit rejected the request before any SQL ran |
| `sql_error` | 400 | no | The engine refused or failed the SQL; earlier statements in a multi-statement request may have applied |
| `timeout` | 400 | no | A deadline interrupted the statement; an interrupted commit may still have committed |
| `budget_exceeded` | 400 | no | Stopped on reaching its per-job cost cap; same unknown-commit caveat |
| `budget_job_cost` | 400 | no | The per-job cap cannot buy a second at this size; raise the cap or ask for less |
| `canceled` | 400 | no | A caller interrupted the statement; same unknown outcome |
| `invalid_request` | 400 | no | Rejected before execution; correct the request rather than resend it |
| `unknown_session` | 404 | no | The session expired or was closed; open a new one |
| `not_found` | 404 | no | No visible record for this ID |
| `duplicate_request` | 409 | no | This attempt ID was accepted before; the response includes its durable original outcome |
| `request_id_mismatch` | 409 | no | This attempt ID or linked retry refers to a different payload; no SQL executed |
| `invalid_retry` | 400 | no | The referenced attempt is not eligible for a linked retry or already has a child |
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

## Compute lifecycle and provisioned usage

`GET /v1/compute` uses the same fleet-read permission as `/v1/workers` and reports
up to 500 recent lifecycle records. Each includes the authenticated principal,
pool, worker/boot identity, state, failure kind, requested/provisioning/ready/
activity/termination timestamps, execution duration, and measured
`cpu_seconds`, `gib_seconds`, `startup_ms`, `idle_ms`, and utilization. Standing
persistent capacity appears alongside isolated sessions, including failed
provisioning. The `estimated_provider_cost` object uses the deployment rate card
and is explicitly `estimate_only_not_reconciled_provider_billing`; it is not an
AWS invoice, node-packing allocation, or minimum cloud charge. This is separate
from the immutable query rate-card events in `/v1/usage`.

`DUCKFLEET_COMPUTE_POLICY_JSON` (chart `computePolicy`) optionally bounds total
and per-principal CPU, memory, and ephemeral disk before an isolated pod is
created. Reservation and lifecycle insert commit atomically under a database
advisory lock, so competing opens cannot oversubscribe an allowance. `max_cpu`,
`max_memory_bytes`, and `max_disk_bytes` are deployment ceilings;
`max_principal_*` applies to one authenticated principal. Refusals happen before
provisioning and are visible without starting a worker. No ceilings are imposed
by default.

`idle_timeout_seconds` stops an idle worker only when no query is active.
`max_lifetime_seconds` bounds the whole worker, interrupting active SQL; closing
the session rolls back uncommitted work. Both accept 1–86400 seconds and are
unset by default. The existing isolated `maxSessionSeconds` kubelet backstop and
statement/query deadlines remain in force. Failed provisioning, cancellation,
and confirmed pod removal settle reservations once; an unresolved owner does not
release capacity early. The platform schema is version 6.

`artifacts/cold-start.json` records the named warm/cold fixture and its lifecycle
usage/cost, including persistent open, isolated cold open and warm-node open.
The PostgreSQL fixture covers competing opens at a one-CPU cap, failed
provisioning, repeated settlement, and report fields.

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
gateway around a restore. The restore helper runs the explicit platform
migration against the restored schema before compute can be restarted.

The Kubernetes gate inspects both backup archives and verifies data after worker
replacement. Restore command sequencing and failure behavior have operator-script
fixtures; the full fresh-host restore-and-write drill is tracked in #108. The
restore helper requires fresh, empty databases. On Kubernetes it reuses the
existing release's migration Job template, so keep that Job until recovery is
complete.

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
files no snapshot references. Maintenance requires the **maintenance permission and a compute grant** — expiry
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

This is a **trusted, single-organization deployment**. Platform roles control
execution, observation, and administration through supported interfaces. Named
executors still use a privileged DuckDB/catalog connection, and unrestricted SQL
can reach worker files and network resources. SQL-process and reader/writer data
authority separation is tracked in #106. Local Docker uses known development
credentials and plain HTTP. Caller-supplied model IDs and tags are correlation
data; durable actor IDs are authenticated attribution.

Only SQL models are implemented. The Docker harness runs the two persistent
compute classes; isolated per-session pods are implemented in the Kubernetes
chart and exercised by the kind harness, and are refused with `501` wherever the
pod grant is absent — which includes everywhere in Docker. There is no warm
pool, hostile-tenant isolation, production secret
management, or automatic write retry.
The usage ledger meters compute only, against a configurable rate card whose
default is a placeholder, and nothing
reconciles it against a real AWS bill; see
[Metering and budgets](#metering-and-budgets).
Arbitrary DuckDB SQL can access worker files and network resources. Python models,
external materializations, and dbt plugins are unsupported. Unchecked dbt
materializations or version combinations are not part of the tested contract.

Health reports process availability, not continuous dependency readiness. History
uses separate bounded PostgreSQL connections and remains available while
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
