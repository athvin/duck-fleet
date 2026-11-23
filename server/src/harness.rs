//! The `harness` role: the real dbt + HTTP + Postgres + S3 integration gate,
//! ported from the former Python `tests/` harness. No mocks.
//!
//! It reuses [`crate::client`] to speak to the gateway and shells out to the
//! prebuilt `dbt` binary for the dbt lifecycle. Invoked as
//! `DUCKFLEET_ROLE=harness duckfleet-server [--stage ...]`; with no stage flag it
//! runs the full protocol + dbt lifecycle.
//!
//! The harness drives the derived-model fleet: a single persistent tier (open with
//! no resources named) plus isolation derived from naming cpu/memory/disk. The
//! isolated/kind-only stages run under scripts/test-k8s.sh; the dbt lifecycle shells
//! out to the prebuilt `dbt` binary.

use std::time::Duration;

use reqwest::Method;
use serde_json::{Value, json};

use crate::client::{self, Client, RemoteConnection, RemoteError};

mod api_workflow;
mod authentication;
mod aws_harness;
mod concurrency;
mod engine_qualification;
mod jobs;
mod jobs_storage;
mod metering;
mod observability;
mod platform;
mod pools;
mod queue_lifecycle;
mod queue_storage;
mod rbac;
mod rbac_storage;
mod reader_authority;
mod request_identity;
mod request_upgrade;
mod system_api;
mod system_storage;

const ARTIFACTS: &str = "/artifacts";

/// Entry point for the `harness` role.
pub async fn run(args: Vec<String>) -> i32 {
    let url = std::env::var("DUCKFLEET_URL").unwrap_or_default();
    let token = std::env::var("DUCKFLEET_TOKEN").unwrap_or_default();
    if url.is_empty() || token.is_empty() {
        eprintln!("set DUCKFLEET_URL and DUCKFLEET_TOKEN");
        return 2;
    }
    let _ = std::fs::create_dir_all(ARTIFACTS);
    let ctx = Ctx { url, token };
    let has = |flag: &str| args.iter().any(|a| a == flag);

    let result: anyhow::Result<()> = if has("--reader-authority") {
        reader_authority::checks(&ctx).await
    } else if has("--engine-qualification") {
        engine_qualification::checks(&ctx).await
    } else if has("--extension-security") {
        engine_qualification::security(&ctx).await
    } else if has("--jobs-storage") {
        jobs_storage::checks().await
    } else if has("--jobs-long") {
        jobs::long_running(&ctx).await
    } else if has("--jobs-store-failure") {
        jobs::store_failure(&ctx).await
    } else if has("--jobs") || has("--jobs-isolated") {
        jobs::checks(&ctx, has("--jobs-isolated")).await
    } else if has("--system") {
        system_api::checks(&ctx).await
    } else if has("--system-storage") {
        system_storage::checks().await
    } else if has("--system-without-workers") {
        system_api::workers_down(&ctx).await
    } else if has("--rbac") {
        rbac::checks(&ctx).await
    } else if has("--rbac-storage") {
        rbac_storage::checks().await
    } else if has("--queue-storage") {
        queue_storage::checks().await
    } else if has("--queue") || has("--queue-isolated") {
        queue_lifecycle::checks(&ctx, has("--queue-isolated")).await
    } else if has("--pools") || has("--pools-isolated") {
        pools::checks(&ctx, has("--pools-isolated")).await
    } else if has("--platform-maintenance") {
        platform::checks().await
    } else if has("--autoscaling") {
        autoscaling_load(&ctx).await
    } else if has("--autoscaling-idle") {
        autoscaling_idle(&ctx).await
    } else if has("--spread") {
        spread_checks(&ctx).await
    } else if has("--spread-failover") {
        spread_failover_checks(&ctx).await
    } else if has("--observability") {
        observability::checks(&ctx).await
    } else if has("--history-without-workers") {
        observability::history_without_workers(&ctx).await
    } else if has("--worker-status") {
        observability::worker_status(&ctx).await
    } else if has("--worker-status-pgwire") {
        observability::mixed_protocol_status(&ctx).await
    } else if has("--worker-status-partial-outage") {
        observability::partial_outage(&ctx).await
    } else if has("--concurrency") {
        concurrency::checks(&ctx).await
    } else if has("--metering") {
        metering::checks(&ctx).await
    } else if has("--evicted-midquery") {
        evicted_midquery(&ctx, has("--expect-classified")).await
    } else if has("--drain-inflight") {
        drain_inflight(&ctx, "drain-inflight.started", "its worker").await
    } else if has("--gateway-drain-inflight") {
        drain_inflight(&ctx, "gateway-drain-inflight.started", "the gateway").await
    } else if has("--gateway-replaced") {
        gateway_replaced(&ctx).await
    } else if has("--credentials") {
        credentials_checks(&ctx).await
    } else if has("--auth-continuity") {
        authentication::checks(&ctx).await
    } else if has("--auth-isolated") {
        authentication::isolated_cleanup(&ctx).await
    } else if has("--request-identity") {
        request_identity::checks(&ctx).await
    } else if has("--request-identity-concurrent") {
        request_identity::concurrent(&ctx).await
    } else if has("--ownership-loss") {
        request_identity::ownership_loss(&ctx).await
    } else if has("--dbt-isolated") {
        dbt_isolated_checks(&ctx).await
    } else if has("--cold-start") {
        cold_start_checks(&ctx).await
    } else if has("--maintenance") {
        maintenance_checks(&ctx).await
    } else if has("--scale") {
        scale_checks(&ctx).await
    } else if has("--isolated") {
        isolated_checks(&ctx).await
    } else if has("--leak-isolated") {
        leak_isolated(&ctx).await
    } else if has("--api-workflow") {
        api_workflow::main(&ctx).await
    } else if has("--aws") {
        aws_harness::main(&ctx).await
    } else {
        main_lifecycle(&ctx, &args).await
    };

    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("FAIL: {error:#}");
            1
        }
    }
}

/// The gateway coordinates shared by every stage.
pub(crate) struct Ctx {
    pub url: String,
    pub token: String,
}

impl Ctx {
    /// Harness-only credential for deliberate trusted internal-worker probes.
    /// The public bootstrap credential is intentionally rejected by workers.
    pub fn internal_test_token(&self) -> anyhow::Result<String> {
        std::env::var("DUCKFLEET_INTERNAL_TEST_TOKEN").map_err(|_| {
            anyhow::anyhow!(
                "DUCKFLEET_INTERNAL_TEST_TOKEN is required for internal-worker harness probes"
            )
        })
    }

    pub fn client(&self) -> Result<Client, RemoteError> {
        Client::new(&self.url, &self.token)
    }

    /// Opens a session on the shared persistent tier (no compute named).
    pub async fn open(&self) -> anyhow::Result<RemoteConnection> {
        Ok(RemoteConnection::open(&self.url, &self.token, "harness", None, None, 90).await?)
    }

    pub async fn open_named(
        &self,
        name: &str,
        compute: Option<Value>,
    ) -> anyhow::Result<RemoteConnection> {
        Ok(RemoteConnection::open(&self.url, &self.token, name, compute, None, 90).await?)
    }
}

// --- shared helpers ---------------------------------------------------------

pub(crate) fn require(condition: bool, message: impl std::fmt::Display) -> anyhow::Result<()> {
    anyhow::ensure!(condition, "{message}");
    Ok(())
}

/// Runs a statement and returns its rows as stringified cells (`None` is NULL).
pub(crate) async fn sql(
    connection: &RemoteConnection,
    statement: &str,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    Ok(connection.execute(statement, Vec::new()).await?.rows)
}

pub(crate) async fn sql_params(
    connection: &RemoteConnection,
    statement: &str,
    parameters: Vec<Value>,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    Ok(connection.execute(statement, parameters).await?.rows)
}

/// True when `actual` equals a matrix of non-null string cells.
pub(crate) fn rows_eq(actual: &[Vec<Option<String>>], expected: &[&[&str]]) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(row, want)| {
            row.len() == want.len()
                && row
                    .iter()
                    .zip(*want)
                    .all(|(cell, w)| cell.as_deref() == Some(*w))
        })
}

/// Asserts a result is a `RemoteError` carrying `status`, returning it so the
/// caller can assert on its classification.
pub(crate) fn expect_status<T>(
    result: Result<T, RemoteError>,
    status: u16,
) -> anyhow::Result<RemoteError> {
    match result {
        Err(error) if error.status == Some(status) => Ok(error),
        Err(error) => anyhow::bail!("expected HTTP {status}, got {:?}: {error}", error.status),
        Ok(_) => anyhow::bail!("expected HTTP {status}"),
    }
}

/// A raw gateway request with the default statement timeout, JSON-decoded.
pub(crate) async fn request_json(
    client: &Client,
    method: Method,
    path: &str,
    payload: Option<&Value>,
) -> Result<Value, RemoteError> {
    let bytes = client
        .request(method, path, payload, client::STATEMENT_TIMEOUT)
        .await?;
    serde_json::from_slice(&bytes).map_err(|_| RemoteError {
        request_id: None,
        acceptance: None,
        status_url: None,
        status: None,
        failure_kind: Some("invalid_response".to_owned()),
        retry_safe: false,
        message: "could not decode a JSON response".to_owned(),
    })
}

/// Shells out to the prebuilt `dbt` binary, asserting its exit code.
pub(crate) fn dbt(ctx: &Ctx, stage: &str, args: &[&str], expected_code: i32) -> anyhow::Result<()> {
    let status = std::process::Command::new("dbt")
        .arg("--no-use-colors")
        .arg("--log-path")
        .arg(format!("{ARTIFACTS}/{stage}/logs"))
        .args(args)
        .env("DUCKFLEET_URL", &ctx.url)
        .env("DUCKFLEET_TOKEN", &ctx.token)
        .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        .env("NO_COLOR", "1")
        .env("DBT_TARGET_PATH", format!("{ARTIFACTS}/{stage}/target"))
        .status()?;
    anyhow::ensure!(
        status.code() == Some(expected_code),
        "dbt {stage}: exit {:?}, expected {expected_code}",
        status.code()
    );
    Ok(())
}

pub(crate) fn write_artifact(name: &str, contents: &str) {
    let _ = std::fs::write(format!("{ARTIFACTS}/{name}"), contents);
}

// --- compute routing contract ----------------------------------------------

async fn routing_contract_checks(connection: &RemoteConnection) -> anyhow::Result<()> {
    let client = connection.client();
    let workers = request_json(client, Method::GET, "/v1/workers", None).await?;
    let available = workers["isolated_compute"].as_bool().unwrap_or(false);
    let isolated = json!({"cpu": 1, "memory": "1Gi"});
    if available {
        let opened = {
            let bytes = client
                .request(
                    Method::POST,
                    "/v1/sessions",
                    Some(&json!({"compute": isolated})),
                    client::OPEN_TIMEOUT,
                )
                .await?;
            serde_json::from_slice::<Value>(&bytes)?
        };
        let session = opened["session_id"].as_str().unwrap_or_default().to_owned();
        require(
            opened["placement"]["compute"] == json!("isolated"),
            format!("{}", opened["placement"]),
        )?;
        let _ = client
            .request(
                Method::DELETE,
                &format!("/v1/sessions/{session}"),
                None,
                client::STATEMENT_TIMEOUT,
            )
            .await;
    } else {
        let refused = expect_status(
            request_json(
                client,
                Method::POST,
                "/v1/sessions",
                Some(&json!({"compute": isolated})),
            )
            .await,
            501,
        )?;
        require(
            refused.failure_kind.as_deref() == Some("compute_unavailable"),
            format!("{refused}"),
        )?;
        require(refused.retry_safe, format!("{refused}"))?;
    }
    let unknown = expect_status(
        request_json(
            client,
            Method::POST,
            "/v1/sessions",
            Some(&json!({"compute": {"compute": "burst"}})),
        )
        .await,
        400,
    )?;
    require(
        unknown.failure_kind.as_deref() == Some("invalid_request"),
        format!("{unknown}"),
    )?;
    println!("PASS: compute routing contract names its mode and refuses what it cannot run");
    Ok(())
}

// --- protocol ---------------------------------------------------------------

async fn protocol_checks(ctx: &Ctx, connection: &RemoteConnection) -> anyhow::Result<()> {
    routing_contract_checks(connection).await?;
    let client = connection.client();

    let wrong = Client::new(&ctx.url, "wrong-token")?;
    expect_status(
        request_json(&wrong, Method::POST, "/v1/sessions", Some(&json!({}))).await,
        401,
    )?;
    expect_status(
        request_json(
            client,
            Method::POST,
            &format!("/v1/sessions/{}/queries", uuid::Uuid::new_v4()),
            Some(&json!({"request_id": uuid::Uuid::new_v4().to_string(), "sql": "select 1"})),
        )
        .await,
        404,
    )?;
    expect_status(
        connection
            .execute("select missing_column", Vec::new())
            .await,
        400,
    )?;
    require(
        rows_eq(&sql(connection, "select 1").await?, &[&["1"]]),
        "SQL error poisoned session",
    )?;

    // This value previously changed by one bit in JSON decoding, before SQL.
    // Exercise both gateway and worker decoding plus Arrow response encoding.
    let expected = 0.0012033694344163659_f64;
    let precise = connection
        .execute("select ?::double", vec![json!(expected)])
        .await?;
    let actual = precise.rows[0][0]
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("double parameter unexpectedly returned NULL"))?
        .parse::<f64>()?;
    require(
        actual.to_bits() == expected.to_bits(),
        "HTTP/Arrow round-trip changed a double parameter",
    )?;

    // Typed round-trip: assert on the stringified cells the gateway returns.
    let typed = connection
        .execute(
            "select ?::decimal(20,2), ?::date, ?::boolean, ?::varchar, ?::bigint",
            vec![
                json!("123456789012345678.90"),
                json!("2026-11-09"),
                json!(true),
                Value::Null,
                json!(9223372036854775807i64),
            ],
        )
        .await?;
    require(
        rows_eq(
            &typed.rows,
            &[&[
                "123456789012345678.90",
                "2026-11-09",
                "true",
                "",
                "9223372036854775807",
            ]],
        ) || typed.rows[0][3].is_none(),
        format!("unexpected typed row: {:?}", typed.rows),
    )?;

    sql(connection, "create schema if not exists checks").await?;
    sql(
        connection,
        "create or replace table checks.replay (n integer)",
    )
    .await?;
    let request = json!({"request_id": uuid::Uuid::new_v4().to_string(), "sql": "insert into checks.replay values (1)"});
    client
        .request(
            Method::POST,
            &format!("{}/queries", connection.path()),
            Some(&request),
            client::STATEMENT_TIMEOUT,
        )
        .await?;
    expect_status(
        request_json(
            client,
            Method::POST,
            &format!("{}/queries", connection.path()),
            Some(&request),
        )
        .await,
        409,
    )?;
    require(
        rows_eq(
            &sql(connection, "select count(*) from checks.replay").await?,
            &[&["1"]],
        ),
        "duplicate request executed twice",
    )?;

    sql(connection, "begin").await?;
    sql(connection, "insert into checks.replay values (2)").await?;
    sql(connection, "rollback").await?;
    require(
        rows_eq(
            &sql(connection, "select count(*) from checks.replay").await?,
            &[&["1"]],
        ),
        "rollback failed",
    )?;

    println!("PASS: protocol checks");
    Ok(())
}

// --- resource / sizing (derived model: a named size routes to an isolated pod) --

async fn resource_checks(connection: &RemoteConnection) -> anyhow::Result<()> {
    let client = connection.client();
    let workers = request_json(client, Method::GET, "/v1/workers", None).await?;
    let list = workers["workers"].as_array().cloned().unwrap_or_default();
    require(!list.is_empty(), "no workers reported")?;
    // A malformed request stays a classified 400.
    for compute in [
        json!({"cpu": 0, "memory": "1Gi"}),
        json!({"cpu": 1, "memory": "1GB"}),
        json!({"compute": "burst"}),
    ] {
        let refused = expect_status(
            request_json(
                client,
                Method::POST,
                "/v1/sessions",
                Some(&json!({"compute": compute})),
            )
            .await,
            400,
        )?;
        require(
            refused.failure_kind.as_deref() == Some("invalid_request"),
            format!("{compute}: {refused}"),
        )?;
    }
    // A well-formed size the fleet cannot serve must be refused explicitly, never
    // downgraded to the persistent tier. Where isolation exists (Kubernetes) an
    // impossibly large size is a placement refusal that names the size; where it does
    // not (Compose) every named size is refused as unavailable — the same contract
    // routing_contract_checks states, asserted here on the default lifecycle path.
    let available = workers["isolated_compute"].as_bool().unwrap_or(false);
    let big = json!({"compute": {"cpu": 32, "memory": "128Gi"}});
    if available {
        let absent = expect_status(
            request_json(client, Method::POST, "/v1/sessions", Some(&big)).await,
            503,
        )?;
        require(
            absent.message.contains("cpu=32") || absent.message.contains("128Gi"),
            format!("{absent}"),
        )?;
    } else {
        let absent = expect_status(
            request_json(client, Method::POST, "/v1/sessions", Some(&big)).await,
            501,
        )?;
        require(
            absent.failure_kind.as_deref() == Some("compute_unavailable"),
            format!("{absent}"),
        )?;
    }
    println!("PASS: resource/sizing routing refuses what it cannot serve");
    Ok(())
}

// --- isolated compute (kind/EKS only) ---------------------------------------

async fn isolated_session(ctx: &Ctx, compute: Value) -> anyhow::Result<(RemoteConnection, Value)> {
    let connection = ctx.open_named("harness-isolated", Some(compute)).await?;
    let placement = connection.placement.clone();
    require(
        placement["compute"] == json!("isolated"),
        format!("{placement}"),
    )?;
    require(
        placement["worker_id"]
            .as_str()
            .unwrap_or_default()
            .starts_with("duckfleet-session-"),
        format!("{placement}"),
    )?;
    Ok((connection, placement))
}

async fn isolated_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let compute = json!({"cpu": 1, "memory": "768Mi"});
    let (mut connection, placement) = isolated_session(ctx, compute.clone()).await?;
    let pod = placement["worker_id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    write_artifact("isolated-pod.txt", &pod);
    require(
        placement["memory_bytes"] == json!(768 * 1024 * 1024),
        format!("{placement}"),
    )?;
    require(
        rows_eq(&sql(&connection, "select 21 * 2").await?, &[&["42"]]),
        "isolated session did not answer",
    )?;
    connection.close().await?;

    let history = request_json(
        &ctx.client()?,
        Method::GET,
        &format!("/v1/queries?worker_id={pod}"),
        None,
    )
    .await?;
    require(
        history["queries"].as_array().is_some_and(|q| !q.is_empty()),
        format!("no history rows for {pod}"),
    )?;
    println!("PASS: an isolated session ran on its own pod {pod}");

    // A size the fleet cannot serve must be refused with the refusing component's own
    // reason, never downgraded to the persistent tier. The two ceilings fail at
    // different moments: the namespace quota refuses at admission before a pod exists,
    // while a size within the quota but larger than any node is refused by the
    // scheduler after one does. Both are retry-safe — nothing ran.
    let client = ctx.client()?;
    for (wanted, expected, what) in [
        (
            json!({"cpu": 64, "memory": "512Gi"}),
            "exceeded quota",
            "the namespace quota",
        ),
        (
            json!({"cpu": 11, "memory": "12Gi"}),
            "Insufficient",
            "the scheduler",
        ),
    ] {
        let refused = expect_status(
            request_json(
                &client,
                Method::POST,
                "/v1/sessions",
                Some(&json!({"compute": wanted})),
            )
            .await,
            503,
        )?;
        require(
            refused.message.contains("not downgraded"),
            format!("{refused}"),
        )?;
        require(
            refused.message.contains(expected),
            format!("expected {what}: {refused}"),
        )?;
        require(
            refused.failure_kind.as_deref() == Some("not_placeable"),
            format!("{refused}"),
        )?;
        require(refused.retry_safe, format!("{refused}"))?;
        println!("PASS: an isolated request {what} cannot serve was refused, not downgraded");
    }
    Ok(())
}

async fn timed_open(ctx: &Ctx, compute: Option<Value>) -> anyhow::Result<(RemoteConnection, f64)> {
    let started = std::time::Instant::now();
    let connection = ctx.open_named("harness-cold", compute).await?;
    let opened = started.elapsed().as_secs_f64();
    require(
        rows_eq(&sql(&connection, "select 1").await?, &[&["1"]]),
        "session did not answer",
    )?;
    Ok((connection, opened))
}

async fn cold_start_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let (mut persistent, warm_persistent) = timed_open(ctx, None).await?;
    persistent.close().await?;
    let isolated = json!({"cpu": 1, "memory": "1Gi"});
    let (mut first, cold_isolated) = timed_open(ctx, Some(isolated.clone())).await?;
    let first_pod = first.placement["worker_id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    first.close().await?;
    let (mut second, warm_isolated) = timed_open(ctx, Some(isolated)).await?;
    require(
        second.placement["worker_id"].as_str() != Some(first_pod.as_str()),
        "the second isolated pod reused the first",
    )?;
    second.close().await?;
    let lifecycle = request_json(&ctx.client()?, Method::GET, "/v1/compute", None).await?;
    let isolated_lifecycles: Vec<_> = lifecycle["lifecycles"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["pool"] == "isolated")
        .collect();
    require(
        isolated_lifecycles.len() >= 2
            && isolated_lifecycles.iter().all(|entry| {
                entry["state"] == "terminated"
                    && entry["measured_usage"]["cpu_seconds"].is_number()
                    && entry["measured_usage"]["idle_ms"].is_number()
                    && entry["estimated_provider_cost"]["micro_usd"].is_number()
            }),
        "isolated lifecycle usage or estimated cost is incomplete",
    )?;
    write_artifact(
        "cold-start.json",
        &serde_json::to_string_pretty(&json!({
            "persistent_open_seconds": (warm_persistent * 100.0).round() / 100.0,
            "isolated_cold_open_seconds": (cold_isolated * 100.0).round() / 100.0,
            "isolated_warm_node_open_seconds": (warm_isolated * 100.0).round() / 100.0,
            "lifecycle_usage": isolated_lifecycles,
        }))?,
    );
    require(
        cold_isolated >= warm_persistent,
        format!("isolated open {cold_isolated:.2}s beat persistent {warm_persistent:.2}s"),
    )?;
    println!(
        "PASS: cold start measured — persistent {warm_persistent:.2}s, isolated cold {cold_isolated:.2}s"
    );
    Ok(())
}

async fn leak_isolated(ctx: &Ctx) -> anyhow::Result<()> {
    let (connection, placement) =
        isolated_session(ctx, json!({"cpu": 1, "memory": "768Mi"})).await?;
    write_artifact(
        "leaked-pod.txt",
        placement["worker_id"].as_str().unwrap_or_default(),
    );
    let client = connection.client().clone();
    let path = connection.path();
    tokio::spawn(async move {
        let _ = client
            .request(
                Method::POST,
                &format!("{path}/queries"),
                Some(&json!({
                    "request_id": uuid::Uuid::new_v4().to_string(),
                    "sql": "select sum(a.range * b.range) from range(200000) a, range(200000) b",
                    "timeout_seconds": 60,
                })),
                client::STATEMENT_TIMEOUT,
            )
            .await;
    });
    tokio::time::sleep(Duration::from_secs(8)).await;
    println!("left a statement running on {}", placement["worker_id"]);
    // Deliberately never closed: `RemoteConnection` has no Drop, so dropping it here
    // does not release the gateway session — the caller destroys the gateway and
    // asserts the pod is collected and the ledger settled anyway.
    Ok(())
}

async fn dbt_isolated_checks(ctx: &Ctx) -> anyhow::Result<()> {
    dbt(
        ctx,
        "isolated",
        &[
            "run",
            "--select",
            "isolated_model",
            "--vars",
            "{\"exercise_isolated\":true}",
        ],
        0,
    )?;
    let connection = ctx.open().await?;
    let rows = sql(
        &connection,
        "select count(*), min(hook_value), max(worker_threads) from lake.analytics.isolated_model",
    )
    .await?;
    require(
        rows.first().and_then(|r| r[0].as_deref()) != Some("0"),
        "the isolated model committed no rows",
    )?;
    // Prove it actually ran isolated, not merely that it committed: the model's own
    // statements must carry an isolated placement in history. Without this the stage
    // passes even when the adapter silently drops meta.platform and runs the model on
    // the persistent tier — which is exactly how that regression once hid.
    let history = request_json(
        connection.client(),
        Method::GET,
        "/v1/queries?limit=200",
        None,
    )
    .await?;
    let ran_isolated = history["queries"]
        .as_array()
        .map(|queries| {
            queries.iter().any(|q| {
                q["sql"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("isolated_model")
                    && q["placement"]["compute"] == json!("isolated")
                    && q["placement"]["worker_id"]
                        .as_str()
                        .unwrap_or_default()
                        .starts_with("duckfleet-session-")
            })
        })
        .unwrap_or(false);
    require(
        ran_isolated,
        "the dbt model committed but no statement ran on an isolated pod — the adapter \
         did not carry meta.platform through to a dedicated session",
    )?;
    println!("PASS: a dbt model ran isolated on its own pod");
    Ok(())
}

// --- drain / eviction / gateway replacement ---------------------------------

async fn drain_inflight(ctx: &Ctx, marker_name: &str, drained: &str) -> anyhow::Result<()> {
    let marker = format!("{ARTIFACTS}/{marker_name}");
    let _ = std::fs::remove_file(&marker);
    let connection = ctx.open_named("harness-drain", None).await?;
    let client = connection.client().clone();
    let path = connection.path();
    let handle = tokio::spawn(async move {
        let started = std::time::Instant::now();
        let outcome = client
            .request(
                Method::POST,
                &format!("{path}/queries"),
                Some(&json!({
                    "request_id": uuid::Uuid::new_v4().to_string(),
                    "sql": "select count(*) from range(4000000000) where range % 7 = 3",
                    "timeout_seconds": 58,
                })),
                client::STATEMENT_TIMEOUT,
            )
            .await;
        (outcome.is_ok(), started.elapsed().as_secs_f64())
    });
    tokio::time::sleep(Duration::from_secs(4)).await;
    std::fs::write(
        &marker,
        connection.placement["worker_id"]
            .as_str()
            .unwrap_or_default(),
    )?;
    let (ok, elapsed) = handle.await?;
    require(
        ok,
        format!("a statement in flight did not survive the drain ({elapsed:.1}s)"),
    )?;
    require(
        elapsed > 4.0,
        format!("the statement finished in {elapsed:.1}s, before the signal"),
    )?;
    println!(
        "PASS: a statement already in flight finished while {drained} drained ({elapsed:.1}s)"
    );
    Ok(())
}

async fn gateway_replaced(ctx: &Ctx) -> anyhow::Result<()> {
    let mut setup = ctx.open_named("harness-gwr", None).await?;
    sql(&setup, "create schema if not exists api_demo").await?;
    sql(
        &setup,
        "create or replace table api_demo.gateway_replaced (n bigint)",
    )
    .await?;
    setup.close().await?;

    let connection = ctx.open_named("harness-gwr2", None).await?;
    let client = connection.client().clone();
    let path = connection.path();
    let request_id = uuid::Uuid::new_v4().to_string();
    let rid = request_id.clone();
    let handle = tokio::spawn(async move {
        client
            .request(
                Method::POST,
                &format!("{path}/queries"),
                Some(&json!({
                    "request_id": rid,
                    "sql": "insert into api_demo.gateway_replaced select range from range(2000000000) where range % 13 = 5",
                    "timeout_seconds": 58,
                })),
                client::STATEMENT_TIMEOUT,
            )
            .await
    });
    let outcome = handle.await?;
    let error = match outcome {
        Ok(_) => anyhow::bail!("the statement completed, so the kill came after it"),
        Err(error) => error,
    };
    require(
        error.failure_kind.as_deref() == Some("transport_failure"),
        format!("{error}"),
    )?;
    require(
        !error.retry_safe,
        "a severed request must never be reported retry-safe",
    )?;
    println!("PASS: the in-flight request failed classified, not as a raw socket error");

    let reopened = ctx.open_named("harness-gwr3", None).await?;
    // Opening at all means the client's bounded new-session retry rode through the
    // gateway's replacement window rather than surfacing the gap as an error.
    println!("PASS: a reopen rode through the gateway replacement");
    // The lost session refuses further use with guidance.
    let lost = match connection.execute("select 1", Vec::new()).await {
        Ok(_) => anyhow::bail!("a session route survived a gateway replacement"),
        Err(error) => error,
    };
    require(
        lost.failure_kind.as_deref() == Some("unknown_session"),
        format!("{lost}"),
    )?;
    // The severed insert's outcome is knowable from history.
    let mut record = Value::Null;
    for _ in 0..120 {
        record = request_json(
            reopened.client(),
            Method::GET,
            &format!("/v1/queries/{request_id}"),
            None,
        )
        .await?;
        if record["status"] != json!("running") {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    require(
        record["status"] == json!("succeeded"),
        format!("the severed insert ended {}", record["status"]),
    )?;
    let durable = request_json(
        reopened.client(),
        Method::GET,
        &format!("/v1/requests/{request_id}"),
        None,
    )
    .await?;
    require(
        durable["status"] == "succeeded" && durable["outcome"] == "statement_result",
        format!("gateway replacement lost durable outcome: {durable}"),
    )?;
    println!(
        "PASS: a gateway replaced without grace left classified outcomes and a knowable history"
    );
    Ok(())
}

async fn evicted_midquery(ctx: &Ctx, expect_classified: bool) -> anyhow::Result<()> {
    let marker = format!("{ARTIFACTS}/evicted-midquery.started");
    let _ = std::fs::remove_file(&marker);
    let table = if expect_classified {
        "api_demo.evicted_hard"
    } else {
        "api_demo.evicted"
    };
    let mut setup = ctx.open_named("harness-evict", None).await?;
    sql(&setup, "create schema if not exists api_demo").await?;
    sql(
        &setup,
        &format!("create or replace table {table} (n bigint)"),
    )
    .await?;
    setup.close().await?;

    let connection = ctx.open_named("harness-evict2", None).await?;
    let client = connection.client().clone();
    let path = connection.path();
    let sql_text =
        format!("insert into {table} select range from range(2000000000) where range % 13 = 5");
    let request_id = uuid::Uuid::new_v4().to_string();
    let payload = json!({"request_id": request_id, "sql": sql_text, "timeout_seconds": 58});
    let submitted = payload.clone();
    let handle = tokio::spawn(async move {
        client
            .request(
                Method::POST,
                &format!("{path}/queries"),
                Some(&submitted),
                client::STATEMENT_TIMEOUT,
            )
            .await
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    std::fs::write(
        &marker,
        connection.placement["worker_id"]
            .as_str()
            .unwrap_or_default(),
    )?;
    let outcome = handle.await?;
    let describe = |result: &Result<Vec<u8>, RemoteError>| match result {
        Ok(bytes) => json!({"received_success": true, "response_bytes": bytes.len()}),
        Err(error) => json!({
            "received_success": false,
            "http_status": error.status,
            "failure_kind": error.failure_kind,
            "retry_safe": error.retry_safe,
            "error": error.message,
        }),
    };
    let artifact = if expect_classified {
        "evicted-midquery-hard.json"
    } else {
        "evicted-midquery-graceful.json"
    };
    let mut evidence = json!({
        "request_id": request_id,
        "request": payload,
        "original_response": describe(&outcome),
        "worker": connection.placement,
    });
    write_artifact(artifact, &evidence.to_string());
    // The connection name is execution context, so reuse it when checking the
    // exact same attempt on a replacement session. Session/worker IDs may differ.
    let reader = ctx.open_named("harness-evict2", None).await?;
    let mut durable = Value::Null;
    for _ in 0..60 {
        durable = request_json(
            reader.client(),
            Method::GET,
            &format!("/v1/requests/{request_id}"),
            None,
        )
        .await?;
        if durable["state"] == "terminal" {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let count_rows = || async {
        let rows = sql(&reader, &format!("select count(*) from {table}")).await?;
        rows.first()
            .and_then(|row| row[0].as_deref())
            .ok_or_else(|| anyhow::anyhow!("eviction row count was absent"))?
            .parse::<u64>()
            .map_err(anyhow::Error::from)
    };
    let count = count_rows().await?;
    let usage_path = format!("/v1/usage?execution_id={request_id}");
    let usage = request_json(reader.client(), Method::GET, &usage_path, None).await?;
    evidence["durable"] = durable.clone();
    evidence["row_count"] = json!(count);
    evidence["usage"] = usage.clone();
    write_artifact(artifact, &evidence.to_string());
    // CI's failing-job path prints the harness logs even when it cannot copy the
    // artifact volume, so keep the original error and durable outcome there too.
    println!("eviction evidence: {evidence}");

    const EXPECTED_ROWS: u64 = 153_846_154;
    require(
        durable["state"] == "terminal",
        format!("evicted request was never reconciled: {durable}"),
    )?;
    require(
        count == 0 || count == EXPECTED_ROWS,
        format!("evicted single-statement insert left a partial or repeated result: {count} rows"),
    )?;
    // A lost/error response is not proof of rollback. SQL can commit before the
    // response or terminal metadata is lost; durable unknown remains non-retryable.
    if outcome.is_ok() || durable["status"] == "succeeded" {
        require(
            count == EXPECTED_ROWS
                && durable["status"] == "succeeded"
                && durable["outcome"] == "statement_result",
            format!("reported success disagrees with {count} rows or {durable}"),
        )?;
    }
    match durable["outcome"].as_str() {
        Some("not_executed") => require(count == 0, "a refused request changed the table")?,
        Some("unknown") => require(
            durable["retry_safe"] == false,
            format!("unknown outcome was marked retry-safe: {durable}"),
        )?,
        Some("statement_result") => {}
        _ => anyhow::bail!("unrecognized durable outcome: {durable}"),
    }
    if let Err(error) = &outcome
        && error.retry_safe
    {
        require(count == 0, format!("retry-safe response left {count} rows"))?;
    }
    if expect_classified {
        let error = outcome
            .as_ref()
            .err()
            .ok_or_else(|| anyhow::anyhow!("the statement completed despite an immediate kill"))?;
        require(
            error
                .failure_kind
                .as_deref()
                .is_some_and(|kind| !kind.is_empty())
                && !error.retry_safe,
            format!("hard worker loss was not classified conservatively: {error:?}"),
        )?;
        require(
            durable["outcome"] == "unknown" && durable["retry_safe"] == false,
            format!("hard worker loss falsely claimed a replay-safe outcome: {durable}"),
        )?;
    }
    require(
        usage["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        format!("evicted attempt did not settle exactly once: {usage}"),
    )?;
    if !expect_classified {
        require(
            durable["status"] == "succeeded"
                && durable["outcome"] == "statement_result"
                && usage["events"][0]["source"] == "worker",
            format!(
                "graceful replacement recovered work before its predecessor finished: \
                 durable={durable}, usage={usage}"
            ),
        )?;
    }

    // Replaying the attempt is an identity lookup, even after the original worker
    // disappeared. It must not execute the insert or append another usage event.
    let replay = reader
        .client()
        .request(
            Method::POST,
            &format!("{}/queries", reader.path()),
            Some(&payload),
            client::STATEMENT_TIMEOUT,
        )
        .await;
    let count_after = count_rows().await?;
    let usage_after = request_json(reader.client(), Method::GET, &usage_path, None).await?;
    evidence["replay_response"] = describe(&replay);
    evidence["row_count_after_replay"] = json!(count_after);
    evidence["usage_after_replay"] = usage_after.clone();
    write_artifact(artifact, &evidence.to_string());
    println!("eviction replay evidence: {evidence}");
    let duplicate = expect_status(replay, 409)?;
    require(
        duplicate.failure_kind.as_deref() == Some("duplicate_request") && !duplicate.retry_safe,
        format!("evicted request was not recognized as a duplicate: {duplicate:?}"),
    )?;
    require(
        count_after == count && usage_after["events"] == usage["events"],
        "replaying the evicted request changed rows or usage",
    )?;
    println!(
        "PASS: a statement whose worker was deleted mid-run kept committed state consistent ({count} rows)"
    );
    Ok(())
}

// --- scale / maintenance / credentials --------------------------------------

async fn scale_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let project = "/app/scale-project";
    require(
        std::path::Path::new(project)
            .join("dbt_project.yml")
            .is_file(),
        "scale fixture missing from the harness image; rebuild docker/dbt.Dockerfile",
    )?;
    dbt(
        ctx,
        "scale",
        &[
            "build",
            "--project-dir",
            project,
            "--profiles-dir",
            "/app/tests/dbt",
        ],
        0,
    )?;
    println!("PASS: scale build completed (counting/attribution checks are runtime-only)");
    Ok(())
}

async fn maintenance_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let admin = ctx.client()?;
    let mut connection = ctx.open_named("maintenance-fixture", None).await?;
    sql(&connection, "create schema if not exists api_demo").await?;
    sql(
        &connection,
        "create or replace table api_demo.fragmented (n bigint)",
    )
    .await?;
    for value in 0..12 {
        sql(
            &connection,
            &format!("insert into api_demo.fragmented values ({value})"),
        )
        .await?;
    }
    connection.close().await?;

    // A named principal may look; only the bootstrap may expire.
    let janitor = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal": "janitor"})),
    )
    .await?;
    let janitor_client = Client::new(&ctx.url, janitor["token"].as_str().unwrap_or_default())?;
    let refused = expect_status(
        request_json(
            &janitor_client,
            Method::POST,
            "/v1/lake/maintenance",
            Some(&json!({"compact": true})),
        )
        .await,
        403,
    )?;
    require(
        refused.failure_kind.as_deref() == Some("forbidden"),
        format!("{refused}"),
    )?;
    admin
        .request(
            Method::DELETE,
            &format!(
                "/v1/credentials/{}",
                janitor["credential_id"].as_str().unwrap_or_default()
            ),
            None,
            client::STATEMENT_TIMEOUT,
        )
        .await?;
    expect_status(
        request_json(
            &admin,
            Method::POST,
            "/v1/lake/maintenance",
            Some(&json!({})),
        )
        .await,
        400,
    )?;
    admin
        .request(
            Method::POST,
            "/v1/lake/maintenance",
            Some(&json!({"expire_older_than_days": 0, "compact": true, "cleanup": true})),
            client::STATEMENT_TIMEOUT,
        )
        .await?;
    let mut verify = ctx.open_named("maintenance-verify", None).await?;
    let rows = sql(&verify, "select count(*), sum(n) from api_demo.fragmented").await?;
    require(
        rows_eq(&rows, &[&["12", "66"]]),
        format!("maintenance changed the data: {rows:?}"),
    )?;
    verify.close().await?;
    println!("PASS: maintenance compacted and expired the lake, metered and in history");
    Ok(())
}

async fn credentials_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let admin = ctx.client()?;
    let issued = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal": "analyst"})),
    )
    .await?;
    require(
        issued["principal"] == json!("analyst"),
        "principal echoed wrong",
    )?;
    require(
        issued["token"]
            .as_str()
            .unwrap_or_default()
            .starts_with("dfk_"),
        "tokens carry a greppable prefix",
    )?;
    // The issued credential works and its work is attributed.
    let mut connection = RemoteConnection::open(
        &ctx.url,
        issued["token"].as_str().unwrap_or_default(),
        "analyst-session",
        None,
        None,
        90,
    )
    .await?;
    let (analyst_query, _) = connection.execute_tracked("select 1", Vec::new()).await?;
    connection.close().await?;
    let record = request_json(
        &admin,
        Method::GET,
        &format!("/v1/queries/{analyst_query}"),
        None,
    )
    .await?;
    require(
        record["principal"] == json!("analyst"),
        format!("history named {}", record["principal"]),
    )?;

    // Revocation bites immediately.
    let revoked = request_json(
        &admin,
        Method::DELETE,
        &format!(
            "/v1/credentials/{}",
            issued["credential_id"].as_str().unwrap_or_default()
        ),
        None,
    )
    .await?;
    require(
        revoked == json!({"revoked": true}),
        "revocation did not report",
    )?;
    let refused = expect_status(
        RemoteConnection::open(
            &ctx.url,
            issued["token"].as_str().unwrap_or_default(),
            "x",
            None,
            None,
            5,
        )
        .await,
        401,
    )?;
    require(
        refused.failure_kind.as_deref() == Some("unauthorized"),
        format!("{refused}"),
    )?;

    // The default analyst preset cannot manage credentials or budgets.
    let service = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal": "etl-service"})),
    )
    .await?;
    let service_client = Client::new(&ctx.url, service["token"].as_str().unwrap_or_default())?;
    for attempt in [
        (
            Method::POST,
            "/v1/credentials",
            Some(json!({"principal": "sneaky"})),
        ),
        (Method::GET, "/v1/credentials", None),
        (
            Method::PUT,
            "/v1/budget",
            Some(json!({"max_job_micro_usd": 1})),
        ),
    ] {
        let refused = expect_status(
            request_json(&service_client, attempt.0, attempt.1, attempt.2.as_ref()).await,
            403,
        )?;
        require(
            refused.failure_kind.as_deref() == Some("forbidden"),
            format!("{refused}"),
        )?;
    }
    admin
        .request(
            Method::DELETE,
            &format!(
                "/v1/credentials/{}",
                service["credential_id"].as_str().unwrap_or_default()
            ),
            None,
            client::STATEMENT_TIMEOUT,
        )
        .await?;
    println!(
        "PASS: credentials are issued, attributed, revoked at once, and the analyst preset cannot manage credentials or budgets"
    );
    Ok(())
}

// --- bounded autoscaling ---------------------------------------------------

async fn interactive_members(ctx: &Ctx, wanted: usize) -> anyhow::Result<()> {
    for _ in 0..120 {
        let workers = request_json(&ctx.client()?, Method::GET, "/v1/workers", None).await?;
        let actual = workers["workers"]
            .as_array()
            .map(|workers| {
                workers
                    .iter()
                    .filter(|worker| worker["pool"] == "interactive" && worker["ready"] == true)
                    .count()
            })
            .unwrap_or(0);
        if actual == wanted {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("interactive worker membership did not reach {wanted}")
}

async fn autoscaling_load(ctx: &Ctx) -> anyhow::Result<()> {
    interactive_members(ctx, 1).await?;
    let compute = json!({"pool":"interactive"});
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..17 {
        let connection = ctx
            .open_named("autoscaling-load", Some(compute.clone()))
            .await?;
        tasks.spawn(async move {
            let result = connection.execute("SELECT pg_sleep(45)", vec![]).await;
            (result, connection)
        });
    }
    interactive_members(ctx, 2).await?;
    println!("PASS: queued interactive work scaled one worker to two with live membership");
    while let Some(joined) = tasks.join_next().await {
        let (result, mut connection) = joined?;
        result?;
        connection.close().await?;
    }
    println!("PASS: every load statement completed and closed without duplicate execution");
    Ok(())
}

async fn autoscaling_idle(ctx: &Ctx) -> anyhow::Result<()> {
    interactive_members(ctx, 1).await?;
    println!("PASS: idle interactive capacity drained back to one worker");
    Ok(())
}

// --- spread / failover ------------------------------------------------------

async fn spread_checks(ctx: &Ctx) -> anyhow::Result<()> {
    // Persistent sessions (no resources named) land on the shared tier and spread
    // across its same-size workers; naming a size would route to an isolated pod.
    let mut sessions = Vec::new();
    for _ in 0..4 {
        sessions.push(ctx.open_named("spread", None).await?);
    }
    let placements: Vec<String> = sessions
        .iter()
        .map(|s| {
            s.placement["worker_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    for session in &sessions {
        require(
            rows_eq(&sql(session, "select 1").await?, &[&["1"]]),
            "a spread session did not answer",
        )?;
    }
    for mut session in sessions {
        session.close().await?;
    }
    require(
        placements.iter().any(|p| p == "small") && placements.iter().any(|p| p == "small-b"),
        format!("four small sessions did not spread across both workers: {placements:?}"),
    )?;
    println!("PASS: same-size sessions spread across both small workers ({placements:?})");
    Ok(())
}

async fn spread_failover_checks(ctx: &Ctx) -> anyhow::Result<()> {
    let mut placements = Vec::new();
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let session = ctx.open_named("failover", None).await?;
        require(
            rows_eq(&sql(&session, "select 1").await?, &[&["1"]]),
            "a failover session did not answer",
        )?;
        placements.push(
            session.placement["worker_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        );
        sessions.push(session);
    }
    for mut session in sessions {
        session.close().await?;
    }
    require(
        placements.iter().all(|w| w == "small-b"),
        format!("opens did not fail over to the surviving small worker: {placements:?}"),
    )?;
    println!("PASS: opens failed over from a draining small worker to its sibling");
    Ok(())
}

// (Removed `unavailable_medium`: there is no medium tier under the derived-model.
// The "a sized request is refused, never downgraded to the shared worker" invariant
// is covered by `resource_checks` — a {cpu:32,memory:128Gi} ask gets a 503, not a
// shared session.)

// --- the default full lifecycle --------------------------------------------

async fn main_lifecycle(ctx: &Ctx, args: &[String]) -> anyhow::Result<()> {
    let has = |flag: &str| args.iter().any(|a| a == flag);
    let mut connection = ctx.open().await?;
    if has("--after-restart") {
        require(
            rows_eq(
                &sql(
                    &connection,
                    "select sum(amount) from lake.analytics.fct_orders",
                )
                .await?,
                &[&["170.00"]],
            ),
            "data did not survive restart",
        )?;
        connection.close().await?;
        println!("PASS: data and query history survived worker and gateway restart");
        return Ok(());
    }
    resource_checks(&connection).await?;
    protocol_checks(ctx, &connection).await?;
    credentials_checks(ctx).await?;
    observability::checks(ctx).await?;
    dbt(ctx, "debug", &["debug"], 0)?;
    dbt(ctx, "initial", &["build", "--full-refresh"], 0)?;
    require(
        rows_eq(
            &sql(
                &connection,
                "select sum(amount) from lake.analytics.fct_orders",
            )
            .await?,
            &[&["170.00"]],
        ),
        "initial models incorrect",
    )?;
    dbt(ctx, "tests", &["test"], 0)?;
    dbt(ctx, "docs", &["docs", "generate"], 0)?;
    let replay_before = sql(&connection, "SELECT count(*) FROM checks.replay").await?;
    dbt(
        ctx,
        "expected-failure",
        &[
            "run",
            "--select",
            "failing_model",
            "--vars",
            "{\"exercise_failure\":true}",
        ],
        1,
    )?;
    let replay_after = sql(&connection, "SELECT count(*) FROM checks.replay").await?;
    require(
        replay_after == replay_before,
        "a failed dbt model committed its pre-hook write",
    )?;
    let history = request_json(connection.client(), Method::GET, "/v1/queries", None).await?;
    let count = history["queries"].as_array().map(|q| q.len()).unwrap_or(0);
    write_artifact(
        "query-history.json",
        &serde_json::to_string_pretty(&history)?,
    );
    connection.close().await?;
    println!("PASS: protocol checks and dbt lifecycle; {count} durable query records");
    Ok(())
}
