//! The whole warehouse workflow with dbt absent: HTTP client and the `cli` role
//! only. Ported from `tests/api_workflow.py`. In the Rust port the "dbt is not
//! installed" premise is structural: this runs inside the `duckfleet-server`
//! binary's `harness` role, which links no dbt at all — the dbt binary is a
//! separate artifact. The CLI workflow shells out to this same binary's `cli`
//! role (DUCKFLEET_ROLE=cli), which is the user-facing, dbt-free client.

use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, request_json, require, rows_eq, sql_params};
use crate::client::RemoteConnection;

const SCHEMA: &str = "api_demo";

async fn create_and_load(ctx: &Ctx) -> anyhow::Result<String> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let context = json!({"api_workflow_run": run_id});
    let connection = RemoteConnection::open(
        &ctx.url,
        &ctx.token,
        "api-workflow",
        None,
        Some(context),
        90,
    )
    .await?;
    super::sql(
        &connection,
        &format!("create schema if not exists {SCHEMA}"),
    )
    .await?;
    super::sql(
        &connection,
        &format!("create or replace table {SCHEMA}.events (event_id integer, region varchar, amount decimal(10,2))"),
    )
    .await?;
    for (event_id, region, amount) in [(1, "eu", "10.50"), (2, "us", "20.25"), (3, "eu", "30.00")] {
        sql_params(
            &connection,
            &format!("insert into {SCHEMA}.events values (?, ?, ?)"),
            vec![json!(event_id), json!(region), json!(amount)],
        )
        .await?;
    }
    let rows = sql_params(
        &connection,
        &format!(
            "select region, sum(amount) from {SCHEMA}.events where region = ? group by region"
        ),
        vec![json!("eu")],
    )
    .await?;
    require(
        rows_eq(&rows, &[&["eu", "40.50"]]),
        format!("aggregate came back as {rows:?}"),
    )?;
    let total = super::sql(
        &connection,
        &format!("select count(*) from {SCHEMA}.events"),
    )
    .await?;
    require(
        rows_eq(&total, &[&["3"]]),
        format!("expected three rows, got {total:?}"),
    )?;
    let mut connection = connection;
    connection.close().await?;
    println!("PASS: created a table, loaded rows with bound parameters, and queried them back");
    Ok(run_id)
}

async fn durable_across_sessions(ctx: &Ctx, run_id: &str) -> anyhow::Result<()> {
    let mut reader = ctx.open_named("api-workflow-reader", None).await?;
    let rows = super::sql(&reader, &format!("select count(*) from {SCHEMA}.events")).await?;
    require(
        rows_eq(&rows, &[&["3"]]),
        format!("a second session saw {rows:?}"),
    )?;
    reader.close().await?;
    let history = request_json(&ctx.client()?, Method::GET, "/v1/queries?limit=200", None).await?;
    let mine: Vec<Value> = history["queries"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|row| row["context"]["api_workflow_run"].as_str() == Some(run_id))
        .collect();
    require(
        !mine.is_empty(),
        format!("no history rows carried api_workflow_run={run_id}"),
    )?;
    require(
        mine.iter().any(|row| {
            row["sql"]
                .as_str()
                .unwrap_or_default()
                .contains("insert into")
        }),
        "no load statement in history",
    )?;
    println!(
        "PASS: data survived the session, and {} statements are in history",
        mine.len()
    );
    Ok(())
}

fn cli(ctx: &Ctx, args: &[&str], expect_code: i32) -> anyhow::Result<std::process::Output> {
    let exe = std::env::current_exe()?;
    let output = std::process::Command::new(exe)
        .env("DUCKFLEET_ROLE", "cli")
        .env("DUCKFLEET_URL", &ctx.url)
        .env("DUCKFLEET_TOKEN", &ctx.token)
        .args(args)
        .output()?;
    anyhow::ensure!(
        output.status.code() == Some(expect_code),
        "cli {args:?}: exit {:?}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(output)
}

async fn cli_workflow(ctx: &Ctx) -> anyhow::Result<()> {
    let table = format!("{SCHEMA}.cli_events");
    cli(
        ctx,
        &[
            "sql",
            &format!("create or replace table {table} (id integer, label varchar)"),
            &format!("insert into {table} values (1, 'from-the-cli')"),
        ],
        0,
    )?;
    let selected = cli(
        ctx,
        &[
            "sql",
            &format!("select label from {table} where id = ?"),
            "--param",
            "1",
            "--format",
            "json",
        ],
        0,
    )?;
    let value: Value = serde_json::from_slice(&selected.stdout)?;
    require(
        value == json!([{"label": "from-the-cli"}]),
        format!("{}", String::from_utf8_lossy(&selected.stdout)),
    )?;
    for (command, expected) in [
        (vec!["workers"], "isolated_compute"),
        (vec!["history", "--limit", "1"], "queries"),
        (vec!["budget"], "prepaid_micro_usd"),
    ] {
        let out = cli(ctx, &command, 0)?;
        require(
            String::from_utf8_lossy(&out.stdout).contains(expected),
            format!("duckfleet {command:?} lost {expected}"),
        )?;
    }
    println!("PASS: the CLI ran the workflow (dbt-free, via the cli role of this binary)");
    Ok(())
}

pub(super) async fn main(ctx: &Ctx) -> anyhow::Result<()> {
    let run_id = create_and_load(ctx).await?;
    durable_across_sessions(ctx, &run_id).await?;
    cli_workflow(ctx).await?;
    println!("PASS: a complete warehouse workflow ran with no dbt installed");
    Ok(())
}
