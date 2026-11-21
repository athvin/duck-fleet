//! Named-pool placement, original policy, live reload affinity and CLI contract.
use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};
use std::path::Path;

use super::{Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::Client;

async fn refused(ctx: &Ctx, compute: Value, status: u16, kind: &str) -> Result<()> {
    let error = expect_status(
        request_json(
            &ctx.client()?,
            Method::POST,
            "/v1/sessions",
            Some(&json!({"compute":compute})),
        )
        .await,
        status,
    )?;
    require(
        error.failure_kind.as_deref() == Some(kind),
        format!("unexpected pool refusal: {error}"),
    )
}

async fn record(
    ctx: &Ctx,
    compute: Value,
    pool: Option<&str>,
    cpu: u64,
    memory_gib: u64,
    disk_gib: u64,
) -> Result<Value> {
    let mut connection = ctx
        .open_named("pool-placement", Some(compute.clone()))
        .await?;
    let placement = connection.placement.clone();
    require(
        placement["pool"].as_str() == pool,
        format!("pool placement: {placement}"),
    )?;
    require(
        placement["cpu"] == cpu
            && placement["memory_bytes"] == memory_gib * (1 << 30)
            && placement["disk_bytes"] == disk_gib * (1 << 30),
        format!("resource placement: {placement}"),
    )?;
    require(
        placement["requested_compute"] == compute,
        format!("original request was changed: {placement}"),
    )?;
    let (id, result) = connection.execute_tracked("SELECT 42", vec![]).await?;
    require(
        rows_eq(&result.rows, &[&["42"]]),
        "selected pool did not answer",
    )?;
    let history = request_json(
        &ctx.client()?,
        Method::GET,
        &format!("/v1/queries/{id}"),
        None,
    )
    .await?;
    require(
        history["placement"] == placement,
        format!("history lost session placement: {history}"),
    )?;
    connection.close().await?;
    Ok(placement)
}

fn cli(ctx: &Ctx, args: &[&str], success: bool) -> Result<String> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .env("DUCKFLEET_ROLE", "cli")
        .env("DUCKFLEET_URL", &ctx.url)
        .env("DUCKFLEET_TOKEN", &ctx.token)
        .args(args)
        .output()?;
    require(
        output.status.success() == success,
        format!("CLI {args:?}: {}", String::from_utf8_lossy(&output.stderr)),
    )?;
    Ok(String::from_utf8(output.stdout)?)
}

async fn reload(ctx: &Ctx) -> Result<Value> {
    Ok(request_json(&ctx.client()?, Method::POST, "/v1/pools/reload", None).await?)
}

fn replace(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("replacement.json");
    std::fs::write(&temporary, serde_json::to_vec(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

async fn reload_affinity(ctx: &Ctx, path: &Path) -> Result<()> {
    let original: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut pinned = ctx
        .open_named("pool-reload-affinity", Some(json!({"pool":"small"})))
        .await?;
    let original_placement = pinned.placement.clone();
    let result = async {
        sql(&pinned, "CREATE TEMP TABLE affinity (n INTEGER)").await?;
        sql(&pinned, "BEGIN TRANSACTION").await?;
        sql(&pinned, "INSERT INTO affinity VALUES (41)").await?;
        let mut changed = original.clone();
        changed["default_pool"] = json!("interactive");
        changed["pools"].as_object_mut().context("pools object")?.remove("small");
        let mut offline = changed["pools"]["interactive"].clone();
        offline["endpoints"] = json!([]);
        changed["pools"]["offline"] = offline;
        replace(path, &changed)?;
        reload(ctx).await?;
        let mut fresh = ctx.open_named("pool-new-default", None).await?;
        require(fresh.placement["pool"] == "interactive", "new open ignored the reloaded default")?;
        fresh.close().await?;
        require(rows_eq(&sql(&pinned, "SELECT n + 1 FROM affinity").await?, &[&["42"]]),
            "reload lost an existing transaction or temporary table")?;
        let (id, _) = pinned.execute_tracked("SELECT 42", vec![]).await?;
        let history = request_json(&ctx.client()?, Method::GET, &format!("/v1/queries/{id}"), None).await?;
        require(history["placement"] == original_placement, "reload changed an existing session's recorded placement")?;
        sql(&pinned, "COMMIT").await?;
        require(rows_eq(&sql(&pinned, "SELECT n FROM affinity").await?, &[&["41"]]), "transaction did not commit on its original session")?;
        refused(ctx, json!({"pool":"offline"}), 503, "no_ready_worker").await?;
        let before = request_json(&ctx.client()?, Method::GET, "/v1/pools", None).await?;
        let mut invalid = changed.clone();
        invalid["default_pool"] = json!("absent");
        replace(path, &invalid)?;
        expect_status(request_json(&ctx.client()?, Method::POST, "/v1/pools/reload", None).await, 400)?;
        let after = request_json(&ctx.client()?, Method::GET, "/v1/pools", None).await?;
        require(before["revision"] == after["revision"] && after["default_pool"] == "interactive",
            "invalid reload replaced the valid registry")?;
        // Ordinary credentials cannot change the operator's routing policy.
        let issued = request_json(&ctx.client()?, Method::POST, "/v1/credentials", Some(&json!({"principal":"pool-fixture-reader"}))).await?;
        let reader = Client::new(&ctx.url, issued["token"].as_str().context("issued token")?)?;
        let denied = expect_status(request_json(&reader, Method::POST, "/v1/pools/reload", None).await, 403);
        request_json(&ctx.client()?, Method::DELETE, &format!("/v1/credentials/{}", issued["credential_id"].as_str().context("credential id")?), None).await?;
        denied?;
        println!("PASS: pool reload preserves transactions/temp tables and recorded placement; new sessions use the new default and invalid/unauthorized reloads are refused");
        Ok::<_, anyhow::Error>(())
    }.await;
    // Restore the operator fixture even when an assertion fails.
    let restored = replace(path, &original);
    let reloaded = reload(ctx).await;
    let closed = pinned.close().await;
    result?;
    restored?;
    reloaded?;
    closed?;
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx, isolated: bool) -> Result<()> {
    let discovery = request_json(&ctx.client()?, Method::GET, "/v1/pools", None).await?;
    require(
        discovery["default_pool"] == "small",
        format!("unexpected default: {discovery}"),
    )?;
    let pools = discovery["pools"]
        .as_array()
        .context("pool discovery entries")?;
    for name in ["small", "interactive"] {
        require(
            pools.iter().any(|pool| pool["name"] == name),
            format!("missing configured pool {name}"),
        )?;
    }
    let small = record(
        ctx,
        json!({"mode":"persistent","pool":"small","cpu":1}),
        Some("small"),
        1,
        1,
        2,
    )
    .await?;
    let interactive = record(
        ctx,
        json!({"pool":"interactive","memory":"2Gi"}),
        Some("interactive"),
        2,
        3,
        4,
    )
    .await?;
    require(
        small["worker_id"] != interactive["worker_id"],
        "distinct pools selected the same worker",
    )?;
    for compute in [
        json!({"pool":"missing"}),
        json!({"pool":"../invalid"}),
        json!({"mode":"isolated","pool":"small"}),
        json!({"pool":"small","memory":"2Gi"}),
        json!({"mode":"automatic"}),
    ] {
        refused(ctx, compute, 400, "invalid_request").await?;
    }
    // A caller cannot submit the gateway's authoritative forwarding envelope.
    expect_status(request_json(&ctx.client()?, Method::POST, "/v1/sessions", Some(&json!({
        "version":1,"request":{"compute":{},"context":{}},
        "resolved":{"mode":"persistent","pool":"interactive","resources":{"cpu":1,"memory_bytes":1073741824,"disk_bytes":2147483648_u64}},
        "registry_revision":"caller-supplied"
    }))).await, 400)?;
    if isolated {
        record(
            ctx,
            json!({"mode":"isolated","cpu":1,"memory":"1Gi","disk":"2Gi"}),
            None,
            1,
            1,
            2,
        )
        .await?;
    } else {
        refused(
            ctx,
            json!({"mode":"isolated","cpu":1}),
            501,
            "compute_unavailable",
        )
        .await?;
        refused(ctx, json!({"cpu":1}), 501, "compute_unavailable").await?;
    }
    let output = cli(
        ctx,
        &[
            "sql",
            "--mode",
            "persistent",
            "--pool",
            "interactive",
            "--format",
            "json",
            "SELECT 42 AS answer",
        ],
        true,
    )?;
    require(
        serde_json::from_str::<Value>(&output)? == json!([{"answer":42}]),
        "CLI explicit pool selection failed",
    )?;
    let output = cli(ctx, &["pools"], true)?;
    require(
        serde_json::from_str::<Value>(&output)?["default_pool"] == "small",
        "CLI pool discovery failed",
    )?;
    for cpu in ["bogus", "0", "65536"] {
        cli(ctx, &["sql", "--cpu", cpu, "SELECT 42"], false)?;
    }
    if let Ok(file) = std::env::var("DUCKFLEET_POOL_TEST_FILE") {
        reload_affinity(ctx, Path::new(&file)).await?;
    }
    super::write_artifact(
        "pool-placement.json",
        &serde_json::to_string_pretty(
            &json!({"small":small,"interactive":interactive,"discovery":discovery}),
        )?,
    );
    println!(
        "PASS: explicit pools preserve requested/resolved/observed placement, classify invalid selections and work through the CLI"
    );
    Ok(())
}
