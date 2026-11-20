//! Metering and budget gate: no execution escapes the ledger, and caps are real.
//! Ported from `tests/metering.py`. The budget is shared stack state, so every
//! case that changes it restores it afterwards.

use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, request_json, require, rows_eq, sql};
use crate::client::Client;

async fn budget(client: &Client) -> anyhow::Result<Value> {
    Ok(request_json(client, Method::GET, "/v1/budget", None).await?)
}

async fn set_budget(client: &Client, limits: Value) -> anyhow::Result<Value> {
    Ok(request_json(client, Method::PUT, "/v1/budget", Some(&limits)).await?)
}

async fn events_for(client: &Client, execution_id: &str) -> anyhow::Result<Vec<Value>> {
    let page = request_json(
        client,
        Method::GET,
        &format!("/v1/usage?execution_id={execution_id}"),
        None,
    )
    .await?;
    Ok(page["events"].as_array().cloned().unwrap_or_default())
}

async fn history_for(client: &Client, query_id: &str) -> anyhow::Result<Value> {
    Ok(request_json(
        client,
        Method::GET,
        &format!("/v1/queries/{query_id}"),
        None,
    )
    .await?)
}

async fn one_statement_is_metered_once(ctx: &Ctx, client: &Client) -> anyhow::Result<()> {
    let position = budget(client).await?;
    let mut connection = ctx.open_named("metering", None).await?;
    let (query_id, _) = connection
        .execute_tracked("select sum(i) from range(200000) t(i)", Vec::new())
        .await?;
    connection.close().await?;

    let events = events_for(client, &query_id).await?;
    require(
        events.len() == 1,
        format!("expected one usage event, got {}", events.len()),
    )?;
    let event = &events[0];
    let record = history_for(client, &query_id).await?;
    require(
        record["status"] == json!("succeeded"),
        format!("{}", record["status"]),
    )?;
    require(event["meter_name"] == json!("compute"), format!("{event}"))?;
    require(event["worker_id"] == json!("small"), format!("{event}"))?;
    require(event["resource_units"] == json!(1), format!("{event}"))?;
    require(
        event["quantity"] == record["elapsed_ms"],
        format!("{event}"),
    )?;

    let after = budget(client).await?;
    require(
        after["spent_micro_usd"].as_i64().unwrap_or(0)
            == position["spent_micro_usd"].as_i64().unwrap_or(0)
                + event["amount_micro_usd"].as_i64().unwrap_or(0),
        "spend did not advance by the event amount",
    )?;
    require(
        after["held_micro_usd"] == position["held_micro_usd"],
        "reservation leaked",
    )?;
    println!("PASS: a statement is metered exactly once, priced from the recorded rate card");
    Ok(())
}

async fn an_exhausted_balance_refuses_admission(ctx: &Ctx, client: &Client) -> anyhow::Result<()> {
    let position = budget(client).await?;
    let mut connection = ctx.open_named("metering-exhausted", None).await?;
    set_budget(client, json!({"prepaid_micro_usd": 0})).await?;
    let exhausted = budget(client).await?;
    require(
        exhausted["available_micro_usd"].as_i64().unwrap_or(1) <= 0,
        format!("{exhausted}"),
    )?;
    let query_id = uuid::Uuid::new_v4().to_string();
    let refused = super::expect_status(
        request_json(
            connection.client(),
            Method::POST,
            &format!("{}/queries", connection.path()),
            Some(&json!({"request_id": query_id, "sql": "create table never_created (n integer)"})),
        )
        .await,
        402,
    );
    // Restore the budget whatever happened.
    set_budget(
        client,
        json!({"prepaid_micro_usd": position["prepaid_micro_usd"]}),
    )
    .await?;
    let refused = refused?;
    require(
        refused.failure_kind.as_deref() == Some("budget_exhausted"),
        format!("{refused}"),
    )?;
    require(
        refused.retry_safe,
        "a refusal before execution should be retry-safe",
    )?;
    let record = history_for(client, &query_id).await?;
    require(
        record["status"] == json!("refused"),
        format!("{}", record["status"]),
    )?;
    require(
        events_for(client, &query_id).await?.is_empty(),
        "a refused execution was metered",
    )?;
    require(
        rows_eq(&sql(&connection, "select 1").await?, &[&["1"]]),
        "session unusable after refusal",
    )?;
    connection.close().await?;
    println!("PASS: an exhausted balance refuses admission before any SQL runs");
    Ok(())
}

async fn accumulators_match_the_ledger(client: &Client) -> anyhow::Result<()> {
    let position = budget(client).await?;
    let mut total: i64 = 0;
    let mut before: Option<i64> = None;
    loop {
        let path = match before {
            Some(cursor) => format!("/v1/usage?limit=500&before={cursor}"),
            None => "/v1/usage?limit=500".to_owned(),
        };
        let listing = request_json(client, Method::GET, &path, None).await?;
        for event in listing["events"].as_array().cloned().unwrap_or_default() {
            total += event["amount_micro_usd"].as_i64().unwrap_or(0);
        }
        match listing["next_before"].as_i64() {
            Some(cursor) => before = Some(cursor),
            None => break,
        }
    }
    require(
        total == position["spent_micro_usd"].as_i64().unwrap_or(-1),
        format!(
            "ledger sums to {total} but the accumulator says {}",
            position["spent_micro_usd"]
        ),
    )?;
    require(
        position["held_micro_usd"] == json!(0),
        format!("idle stack holds funds: {position}"),
    )?;
    println!("PASS: the spent accumulator equals the ledger summed across pages");
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let opening = budget(&client).await?;
    require(
        opening["available_micro_usd"].as_i64().unwrap_or(0) > 0,
        format!("no budget to start from: {opening}"),
    )?;
    let outcome = async {
        one_statement_is_metered_once(ctx, &client).await?;
        an_exhausted_balance_refuses_admission(ctx, &client).await?;
        accumulators_match_the_ledger(&client).await?;
        anyhow::Ok(())
    }
    .await;
    // However this ended, the next stage needs a working budget.
    set_budget(
        &client,
        json!({
            "prepaid_micro_usd": opening["prepaid_micro_usd"],
            "max_job_micro_usd": opening["max_job_micro_usd"],
        }),
    )
    .await?;
    outcome?;
    println!("PASS: metering cases complete");
    Ok(())
}
