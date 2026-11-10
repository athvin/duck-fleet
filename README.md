# DuckFleet

A managed DuckLake platform, starting with a local Docker feasibility harness.
DuckFleet is planned as a separate entity from DupeZero, using the same AWS
organization. The intended domain is `duckfleet.com`, pending purchase.

## Run locally

Requires Docker Engine with Compose v2 (Docker Desktop works). No host Rust,
Python, dbt, Kubernetes, or AWS credentials are needed. Allow roughly 4 GiB of
Docker memory and internet access for the initial images, packages, and DuckDB
extension downloads at image build time. DuckDB and its three required extensions
are checksum-pinned in the images; startup needs only the local services.

```sh
make test
```

This builds the images, starts PostgreSQL, RustFS, and the Rust HTTP server, and
runs a real dbt project through the server. It then restarts the server, verifies
that results and history survived, and checks that RustFS contains Parquet files.
Repeated runs reuse the local volumes and rebuild the fixture from scratch.
Tests leave the services running for inspection. The expected-failure stage
intentionally prints one dbt model error; the harness requires that error and
verifies that its pre-hook write rolled back.

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
  an Axum server embedding DuckDB 1.5.4. Arrow IPC preserves result types.
- DuckLake metadata lives in PostgreSQL; data lives in S3-compatible RustFS.
  A separate PostgreSQL database stores execution history. Only the server has
  catalog and object-store credentials.
- dbt seeds, views, tables, `ref`, tests, documentation generation, and
  `delete+insert` incremental updates run remotely. A test-only guard makes a
  client-side `duckdb.connect()` call fail.
- A dbt connection maps to one server session, preserving temporary tables and
  transactions through pre-hooks, materialization, and post-hooks. Query comments
  carry invocation/model IDs and resolved metadata; history groups statements by
  model run and retains the tags supplied by the example project.
- The protocol checks authentication, parameter/result fidelity, session isolation,
  rollback, rollback on close, failed SQL recovery, duplicate request rejection,
  response limits, statement timeout, active-query cancellation, and rollback of
  a deliberately failed dbt materialization.
- Data and query history survive a server-container restart; tiny fixture tables
  explicitly disable DuckLake data inlining so the test verifies real Parquet
  objects, rather than only catalog rows.

The dbt project runs with two threads, while the first worker executes statements
serially. This checks session interleaving, not multi-worker write concurrency.
Test artifacts and the last 500 query records are written under `artifacts/`.

## Layout

```text
server/                 Rust Axum API, persistent worker, query history
adapters/dbt-duckfleet/  Python dbt adapter and HTTP/Arrow transport
compose.yaml            PostgreSQL, RustFS, bucket setup, server, dbt runner
docker/                 Images and initial PostgreSQL databases
scripts/                Build and harness helpers
tests/dbt/              Representative seed, view, table, incremental model, tests
tests/harness.py         Protocol, dbt, and restart assertions
```

## Local execution API

All `/v1` endpoints require `Authorization: Bearer <token>`.

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/healthz` | Process health after startup initialization |
| POST | `/v1/sessions` | Open a connection; returns `session_id` |
| POST | `/v1/sessions/{id}/queries` | Execute SQL; returns an Arrow IPC stream and `x-query-id` |
| DELETE | `/v1/sessions/{id}` | Close connection and roll back any open transaction |
| POST | `/v1/sessions/{id}/cancel` | Interrupt this session's currently executing statement |
| GET | `/v1/queries` | Most recent 500 execution records |

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

Request IDs are unique across sessions and retained in PostgreSQL. A repeated ID
returns HTTP 409 without executing SQL again; results are not cached or replayed.
An HTTP/transport failure can occur after a write commits. Inspect history and
application state before retrying; the adapter does not automatically retry SQL.
Accepted worker executions are recorded before SQL runs and updated on completion.
At startup, unfinished records become `interrupted` with an unknown-commit warning.

The queue holds at most 64 operations, with at most 32 sessions. Sessions idle for
five minutes are closed on the next worker operation. Statement execution has a
30-second default deadline (1–60 seconds allowed); queue time is additional.
Responses fail rather than silently truncate beyond 100,000 rows or 16 MiB.
DuckDB has a 512 MiB memory setting and 1 GiB spill limit inside a 2 GiB / 2 CPU
container. These are local resource limits, not billing budgets.

## Scope and limitations

This is a **trusted, single-tenant development environment** with known local
credentials and plain HTTP inside Docker. The bearer token controls access to an
administrator SQL connection; it is not a SQL sandbox. Do not expose it publicly
or use customer data. Query history includes SQL text and errors. Logical model
IDs and tags are caller-supplied correlation data, not authenticated billing data.

Only persistent compute and SQL models are implemented. There is no burst routing,
warm pool, model resource sizing, durable job queue, pricing/budget ledger,
multi-tenant authorization, production secret management, or automatic write retry.
Arbitrary DuckDB SQL can access worker files and network resources. Python models,
external materializations, and dbt plugins are unsupported. Unchecked dbt
materializations or version combinations are not part of the tested contract.

Health reports process availability, not continuous dependency readiness. Query
history uses the same worker queue, so it waits behind a running statement;
cancellation bypasses that queue. The response limit applies to returned data;
DuckDB may materialize a result before serialization. Canceling or losing an HTTP
request is not proof that a write was rolled back. This harness does not yet test
crash recovery during a commit, backup restoration, or concurrent DuckLake writers.

## Design and upstream contracts

The [founding CTO blueprint](MANAGED_DUCKLAKE_CTO_BLUEPRINT.md) describes the broader
product and the feasibility gates. This implementation starts the adapter and
persistent-worker spikes with Docker rather than Kubernetes.

- [DuckLake connections and PostgreSQL catalogs](https://ducklake.select/docs/stable/duckdb/usage/connecting)
- [dbt adapter development](https://docs.getdbt.com/guides/adapter-creation)
- [dbt-duckdb adapter and materializations](https://github.com/duckdb/dbt-duckdb)
