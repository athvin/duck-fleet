# duckfleet

Async Rust client for [DuckFleet](https://github.com/athvin/duck-fleet), a shared DuckLake warehouse with an HTTP/Arrow API. It provides:

- explicit sessions: temporary tables, transactions, rollback, cancellation, and fixed compute selection
- typed bound parameters, with results as Arrow `RecordBatch`es plus display strings
- durable jobs: submit, status, wait, cancel, and verified, resumable result download
- a stable error taxonomy that keeps the server's `failure_kind`, `retry_safe`, request identity, and acceptance

It needs no DuckDB C library and no Python runtime. The `duckfleet` CLI and DuckFleet's own integration harness use this same crate.

## Install

```toml
[dependencies]
duckfleet = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The minimum supported Rust version is **1.89**.

## Query

```rust,no_run
use duckfleet::{Client, Param, SessionOptions};

#[tokio::main]
async fn main() -> Result<(), duckfleet::Error> {
    let client = Client::new("http://127.0.0.1:18080", "<token>")?;
    let mut session = client.session(SessionOptions::named("example")).await?;
    let result = session
        .query("SELECT ?::BIGINT * 2 AS doubled", [Param::from(21_i64)])
        .await?;
    println!("{:?}", result.batches[0]);
    session.close().await?;
    Ok(())
}
```

## Transactions

```rust,no_run
# async fn run(session: &duckfleet::Session) -> Result<(), duckfleet::Error> {
session.execute("BEGIN", Vec::new()).await?;
session.query("INSERT INTO raw.events VALUES (?, ?)", [duckfleet::Param::from(1_i64), "signup".into()]).await?;
session.commit().await?; // or session.rollback(); closing also rolls back
# Ok(()) }
```

## Compute selection

```rust,no_run
# async fn run(client: &duckfleet::Client) -> Result<(), duckfleet::Error> {
use duckfleet::{ComputeRequest, Mode, SessionOptions};
let session = client
    .session(SessionOptions::named("heavy").compute(ComputeRequest {
        mode: Some(Mode::Isolated),
        cpu: Some(4),
        memory: Some("16Gi".into()),
        ..ComputeRequest::default()
    }))
    .await?;
println!("placed on {}", session.placement);
# Ok(()) }
```

## Durable jobs

```rust,no_run
# async fn run(client: &duckfleet::Client) -> Result<(), duckfleet::Error> {
use std::time::Duration;
let request: duckfleet::JobRequest = serde_json::from_value(serde_json::json!({
    "request_id": uuid::Uuid::new_v4(),
    "sql": "SELECT * FROM raw.events",
})).expect("valid job request");
client.submit_job(&request).await?;
let status = client.wait_job(request.request_id, Duration::from_secs(1), Duration::from_secs(600)).await?;
assert_eq!(status["status"], "succeeded");
let manifest = client
    .download_job_result(request.request_id, std::path::Path::new("events-result"), false)
    .await?;
println!("{} rows in {} verified Arrow chunks", manifest.rows, manifest.chunks.len());
# Ok(()) }
```

Each chunk is streamed to a `.part` file and checked for length, SHA-256, schema, and row count before it is published. Pass `resume = true` to keep verified chunks after an interruption. The stored manifest must match the server's current result generation.

## Parameters

Parameters always travel separately from the SQL text. The wire carries JSON scalars: `Param::Int` binds as `BIGINT`, `Param::UInt` as `UBIGINT`, `Param::Double` as `DOUBLE`, `Param::Bool` as `BOOLEAN`, `Param::Text` as `VARCHAR`, and `Param::Null` as `NULL`. Bind decimals, dates, timestamps, and other types as text and cast them in SQL (`?::DECIMAL(18,4)`, `?::TIMESTAMP`). Non-finite doubles are refused before sending. Results keep every Arrow type the server returns: decimals, dates, timestamps, binary, lists, and structs.

## Errors and retries

Every failure is an `Error` with `kind()`, `failure_kind()`, `retry_safe()`, `request_id()`, `acceptance()`, and `status_url()`. **The SDK never replays SQL.** `ErrorKind::TransportUncertain` means the request may have run. Inspect `status_url()` (for example `/v1/requests/<id>`) before deciding whether to resend a write. `retry_safe()` is true only where the server proved the request left no effect, and resending is still your decision. A session's engine state does not survive `ErrorKind::SessionLost`: open a new session and treat uncommitted work as rolled back.

| `ErrorKind` | Server `failure_kind` |
|---|---|
| `Sql` | `sql_error` |
| `Timeout` / `QueueTimeout` | `timeout` / `queue_timeout` |
| `Canceled` | `canceled` |
| `Conflict` | `transaction_conflict` |
| `DuplicateRequest` | `duplicate_request`, `request_id_mismatch` |
| `BudgetRefused` | `budget_exceeded`, `budget_exhausted`, `budget_job_cost` |
| `SessionLost` | `session_lost`, `unknown_session`, `interrupted` |
| `SessionBusy` | `session_busy` (a session runs one statement at a time) |
| `TransportUncertain` | `transport_failure` (client-side) |
| `Unauthorized` / `Forbidden` / `NotFound` | `unauthorized` / `forbidden` / `not_found` |
| `InvalidRequest` | `invalid_request`, `invalid_retry`, `unsupported_parameter` |
| `Unavailable` | capacity, draining, starting, and no-ready-worker refusals |
| `ResultUnavailable` | `result_expired`, `result_pending`, `result_unavailable` |
| `Decode` | invalid or unverifiable responses |
| `Other` | anything newer; `ErrorKind` is `#[non_exhaustive]` |

## TLS

The crate uses rustls with the `ring` provider and installs it as the process default on first `Client::new` unless the application already installed one. There are no optional Cargo features in 0.1.

## Versioning

The crate follows Semantic Versioning. Before 1.0, a minor bump (0.1 → 0.2) may break the API and a patch release will not. Raising the minimum supported Rust version is a minor-version change. Every release is listed in [CHANGELOG.md](CHANGELOG.md).
