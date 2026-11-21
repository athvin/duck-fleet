//! Durable identity is tested against live SQL, independent diagnostic pruning,
//! budget changes, and separate workers sharing the same platform database.
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::{Client, RemoteConnection};

async fn submit(ctx: &Ctx, token: &str, path: &str, payload: &Value) -> Result<(u16, Value)> {
    let response = reqwest::Client::new()
        .post(format!("{}{path}/queries", ctx.url))
        .bearer_auth(token)
        .json(payload)
        .timeout(Duration::from_secs(75))
        .send()
        .await?;
    let status = response.status().as_u16();
    let bytes = response.bytes().await?;
    Ok((
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    ))
}

async fn detail(client: &Client, id: &str) -> Result<Value> {
    Ok(request_json(client, Method::GET, &format!("/v1/requests/{id}"), None).await?)
}

async fn event_count(client: &Client, id: &str) -> Result<usize> {
    let value = request_json(
        client,
        Method::GET,
        &format!("/v1/usage?execution_id={id}"),
        None,
    )
    .await?;
    Ok(value["events"].as_array().context("usage events")?.len())
}

async fn issue(admin: &Client, principal: &str) -> Result<Value> {
    Ok(request_json(
        admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal": principal})),
    )
    .await?)
}

async fn retire(admin: &Client, credential: &Value) -> Result<()> {
    request_json(
        admin,
        Method::DELETE,
        &format!(
            "/v1/credentials/{}",
            credential["credential_id"]
                .as_str()
                .context("credential ID")?
        ),
        None,
    )
    .await?;
    Ok(())
}

async fn metadata<T: Send + 'static>(
    dsn: &str,
    operation: impl FnOnce(&mut postgres::Client) -> Result<T> + Send + 'static,
) -> Result<T> {
    let dsn = dsn.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut config: postgres::Config = dsn.parse()?;
        config.connect_timeout(Duration::from_secs(3));
        config.options("-c statement_timeout=10000");
        operation(&mut config.connect(postgres::NoTls)?)
    })
    .await?
}

async fn retained_outcome(ctx: &Ctx, admin: &Client, dsn: &str) -> Result<()> {
    let owner = issue(admin, "identity-owner").await?;
    let other = issue(admin, "identity-other").await?;
    let replacement = issue(admin, "identity-owner").await?;
    let token = owner["token"].as_str().context("owner token")?;
    let owner_client = Client::new(&ctx.url, token)?;
    let other_token = other["token"].as_str().context("other token")?;
    let other_client = Client::new(&ctx.url, other_token)?;
    let mut connection = RemoteConnection::open(&ctx.url, token, "identity", None, None, 5).await?;
    let table = format!(
        "lake.main.request_identity_{}",
        uuid::Uuid::new_v4().simple()
    );
    sql(&connection, &format!("create table {table}(value integer)")).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"request_id": id, "sql": format!("insert into {table} values (?)"),
        "parameters": [7], "context": {"purpose": "retention", "nested": {"b": 2, "a": 1}}, "timeout_seconds": 30});
    let initial = submit(ctx, token, &connection.path(), &body).await?;
    require(
        initial.0 == 200,
        format!("initial execution failed: {initial:?}"),
    )?;
    let before = detail(&owner_client, &id).await?;
    require(
        before["status"] == "succeeded"
            && before["outcome"] == "statement_result"
            && before["diagnostics_available"] == true,
        format!("missing durable success: {before}"),
    )?;
    require(
        event_count(admin, &id).await? == 1,
        "initial execution not metered once",
    )?;
    let pruned_id = id.clone();
    metadata(dsn, move |client| {
        let removed =
            client.execute("DELETE FROM query_history WHERE query_id=$1", &[&pruned_id])?;
        require(removed == 1, "diagnostic prune removed no row")
    })
    .await?;
    expect_status(
        request_json(admin, Method::GET, &format!("/v1/queries/{id}"), None).await,
        404,
    )?;
    let replay = submit(ctx, token, &connection.path(), &body).await?;
    require(
        replay.0 == 409
            && replay.1["failure_kind"] == "duplicate_request"
            && replay.1["original"]["status"] == "succeeded"
            && replay.1["original"]["diagnostics_available"] == false,
        format!("pruned replay lost its original outcome: {replay:?}"),
    )?;
    for (field, value) in [
        ("sql", json!(format!("insert into {table} values (8)"))),
        ("parameters", json!([8])),
        ("parameters", json!([7.0])),
        ("timeout_seconds", json!(31)),
        ("context", json!({"purpose": "different"})),
    ] {
        let mut changed = body.clone();
        changed[field] = value;
        let mismatch = submit(ctx, token, &connection.path(), &changed).await?;
        require(
            mismatch.0 == 409 && mismatch.1["failure_kind"] == "request_id_mismatch",
            format!("changed {field} was not a payload mismatch: {mismatch:?}"),
        )?;
    }
    let mut foreign =
        RemoteConnection::open(&ctx.url, other_token, "identity", None, None, 5).await?;
    let hidden = submit(ctx, other_token, &foreign.path(), &body).await?;
    require(
        hidden.0 == 404 && hidden.1.get("original").is_none(),
        format!("another principal learned an outcome: {hidden:?}"),
    )?;
    expect_status(
        request_json(
            &other_client,
            Method::GET,
            &format!("/v1/requests/{id}"),
            None,
        )
        .await,
        404,
    )?;
    foreign.close().await?;
    let rotated = Client::new(
        &ctx.url,
        replacement["token"].as_str().context("replacement token")?,
    )?;
    require(
        detail(&rotated, &id).await?["status"] == "succeeded",
        "rotation lost request ownership",
    )?;
    require(
        event_count(admin, &id).await? == 1,
        "duplicate/mismatch produced extra usage",
    )?;
    require(
        rows_eq(
            &sql(
                &connection,
                &format!("select count(*), sum(value) from {table}"),
            )
            .await?,
            &[&["1", "7"]],
        ),
        "duplicate/mismatch executed SQL",
    )?;
    sql(&connection, &format!("drop table {table}")).await?;
    connection.close().await?;
    for credential in [&owner, &other, &replacement] {
        retire(admin, credential).await?;
    }
    println!(
        "PASS: durable outcomes survive history pruning, payload mismatches execute no SQL, and ownership follows principals"
    );
    Ok(())
}

async fn budget_attempts(ctx: &Ctx, admin: &Client) -> Result<()> {
    let mut connection = ctx.open_named("identity-budget", None).await?;
    let table = format!("lake.main.request_budget_{}", uuid::Uuid::new_v4().simple());
    sql(&connection, &format!("create table {table}(value integer)")).await?;
    let limits = request_json(admin, Method::GET, "/v1/budget", None).await?;
    request_json(
        admin,
        Method::PUT,
        "/v1/budget",
        Some(&json!({"prepaid_micro_usd": 0})),
    )
    .await?;
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"request_id": id, "sql": format!("insert into {table} values (9)")});
    let refused = submit(ctx, &ctx.token, &connection.path(), &body).await;
    // Always restore the shared balance before checking the refusal.
    request_json(
        admin,
        Method::PUT,
        "/v1/budget",
        Some(&json!({"prepaid_micro_usd": limits["prepaid_micro_usd"]})),
    )
    .await?;
    let refused = refused?;
    require(
        refused.0 == 402 && refused.1["retry_safe"] == true,
        format!("budget refusal: {refused:?}"),
    )?;
    let original = detail(admin, &id).await?;
    require(
        original["outcome"] == "not_executed" && original["retry"]["allowed"] == true,
        format!("budget refusal has no safe retry contract: {original}"),
    )?;
    let replay = submit(ctx, &ctx.token, &connection.path(), &body).await?;
    require(
        replay.0 == 409 && replay.1["original"]["failure_kind"] == "budget_exhausted",
        format!("funding changed old attempt's outcome: {replay:?}"),
    )?;
    let retry_id = uuid::Uuid::new_v4().to_string();
    let mut retry = body.clone();
    retry["request_id"] = json!(retry_id);
    retry["retry_of"] = json!(id);
    let executed = submit(ctx, &ctx.token, &connection.path(), &retry).await?;
    require(
        executed.0 == 200,
        format!("funded linked attempt failed: {executed:?}"),
    )?;
    let outcome = detail(admin, &retry_id).await?;
    require(
        outcome["logical_request_id"] == id
            && outcome["retry_of"] == id
            && outcome["status"] == "succeeded",
        format!("attempt lineage lost: {outcome}"),
    )?;
    require(
        event_count(admin, &id).await? == 0 && event_count(admin, &retry_id).await? == 1,
        "refused/retried logical request had wrong usage count",
    )?;
    retry["request_id"] = json!(uuid::Uuid::new_v4());
    let fanout = submit(ctx, &ctx.token, &connection.path(), &retry).await?;
    require(
        fanout.0 == 400 && fanout.1["failure_kind"] == "invalid_retry",
        format!("one refused attempt created competing retry branches: {fanout:?}"),
    )?;
    require(
        rows_eq(
            &sql(&connection, &format!("select count(*) from {table}")).await?,
            &[&["1"]],
        ),
        "retry contract executed mutation more than once",
    )?;
    sql(&connection, &format!("drop table {table}")).await?;
    connection.close().await?;
    println!(
        "PASS: funded retries use a new linked attempt, preserve the refusal, and cannot fan out"
    );
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> Result<()> {
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("request identity checks require fixture metadata access")?;
    let admin = ctx.client()?;
    retained_outcome(ctx, &admin, &dsn).await?;
    budget_attempts(ctx, &admin).await?;
    super::request_upgrade::checks(ctx).await?;
    Ok(())
}

pub(super) async fn concurrent(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let mut first = ctx.open_named("identity-concurrent", None).await?;
    let mut second = ctx.open_named("identity-concurrent", None).await?;
    require(
        first.placement["worker_id"] != second.placement["worker_id"],
        "concurrent identity fixture requires two workers",
    )?;
    let table = format!("lake.main.request_race_{}", uuid::Uuid::new_v4().simple());
    sql(&first, &format!("create table {table}(value integer)")).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"request_id": id, "sql": format!("insert into {table} values (1)")});
    let first_path = first.path();
    let second_path = second.path();
    let (a, b) = tokio::join!(
        submit(ctx, &ctx.token, &first_path, &body),
        submit(ctx, &ctx.token, &second_path, &body)
    );
    let (a, b) = (a?, b?);
    let mut statuses = [a.0, b.0];
    statuses.sort();
    require(
        statuses == [200, 409],
        format!("concurrent claims did not yield one execution: {a:?}, {b:?}"),
    )?;
    require(
        event_count(&admin, &id).await? == 1,
        "concurrent claims created extra usage",
    )?;
    require(
        rows_eq(
            &sql(&first, &format!("select count(*) from {table}")).await?,
            &[&["1"]],
        ),
        "two workers executed the same request",
    )?;
    sql(&first, &format!("drop table {table}")).await?;
    first.close().await?;
    second.close().await?;
    println!(
        "PASS: concurrent claims on separate workers admit one SQL execution and one usage event"
    );
    Ok(())
}
