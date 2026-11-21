//! Real engine queue semantics, connection affinity and detached HTTP delivery.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::{Client, RemoteConnection};

const BUSY_SQL: &str = "SELECT sum(range % 7) FROM range(500000000000)";

async fn detail(client: &Client, id: Uuid) -> Result<Value> {
    Ok(request_json(client, Method::GET, &format!("/v1/requests/{id}"), None).await?)
}

async fn wait_state(client: &Client, id: Uuid, state: &str, seconds: u64) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        match detail(client, id).await {
            Ok(value) if value["state"] == state => return Ok(value),
            Ok(value) => require(
                Instant::now() < deadline,
                format!("request {id} did not reach {state}: {value}"),
            )?,
            Err(error) => require(
                Instant::now() < deadline,
                format!("request {id} did not become visible: {error}"),
            )?,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn cancel(client: &Client, id: Uuid) -> Result<Value> {
    Ok(request_json(
        client,
        Method::POST,
        &format!("/v1/requests/{id}/cancel"),
        None,
    )
    .await?)
}

fn submit(
    connection: &RemoteConnection,
    body: Value,
) -> tokio::task::JoinHandle<Result<Vec<u8>, crate::client::RemoteError>> {
    let client = connection.client().clone();
    let path = format!("{}/queries", connection.path());
    tokio::spawn(async move {
        client
            .request(Method::POST, &path, Some(&body), Duration::from_secs(75))
            .await
    })
}

async fn pinned(ctx: &Ctx, name: &str) -> Result<RemoteConnection> {
    ctx.open_named(name, Some(json!({"mode":"persistent","pool":"small"})))
        .await
}

async fn queued_controls(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let mut transaction = pinned(ctx, "queue-transaction").await?;
    let mut blocker = pinned(ctx, "queue-blocker").await?;
    require(
        transaction.placement["worker_id"] == blocker.placement["worker_id"],
        "queue fixture requires one small-pool worker",
    )?;
    sql(&transaction, "CREATE TEMP TABLE queue_values(v INTEGER)").await?;
    sql(
        &transaction,
        "/* preserve the actual connection */ BEGIN; INSERT INTO queue_values VALUES (7)",
    )
    .await?;
    let blocked_id = Uuid::new_v4();
    let blocked = submit(
        &blocker,
        json!({"request_id":blocked_id,"sql":BUSY_SQL,"timeout_seconds":60}),
    );
    wait_state(&admin, blocked_id, "executing", 10).await?;
    let id = Uuid::new_v4();
    let body = json!({"request_id":id,"sql":"INSERT INTO queue_values VALUES (9)","queue_timeout_seconds":30});
    let waiting = submit(&transaction, body.clone());
    wait_state(&admin, id, "queued", 10).await?;
    let started = Instant::now();
    let duplicate = expect_status(
        transaction
            .client()
            .request(
                Method::POST,
                &format!("{}/queries", transaction.path()),
                Some(&body),
                Duration::from_secs(5),
            )
            .await,
        409,
    )?;
    require(
        duplicate.failure_kind.as_deref() == Some("duplicate_request"),
        "queued duplicate was not retained",
    )?;
    let mut changed = body.clone();
    changed["queue_timeout_seconds"] = json!(31);
    let mismatch = expect_status(
        transaction
            .client()
            .request(
                Method::POST,
                &format!("{}/queries", transaction.path()),
                Some(&changed),
                Duration::from_secs(5),
            )
            .await,
        409,
    )?;
    require(
        mismatch.failure_kind.as_deref() == Some("request_id_mismatch"),
        "queue allowance missing from request identity",
    )?;
    let commit = expect_status(
        transaction
            .client()
            .request(
                Method::POST,
                &format!("{}/queries", transaction.path()),
                Some(&json!({"request_id":Uuid::new_v4(),"sql":"COMMIT"})),
                Duration::from_secs(5),
            )
            .await,
        409,
    )?;
    require(
        commit.failure_kind.as_deref() == Some("session_busy") && !commit.retry_safe,
        "concurrent COMMIT bypassed the outstanding session request",
    )?;
    cancel(&admin, id).await?;
    let terminal = wait_state(&admin, id, "terminal", 5).await?;
    require(
        started.elapsed() < Duration::from_secs(5),
        "queue controls waited for active SQL",
    )?;
    require(
        terminal["status"] == "canceled"
            && terminal["outcome"] == "not_executed"
            && terminal["retry_safe"] == false,
        format!("queued cancel outcome: {terminal}"),
    )?;
    require(waiting.await?.is_err(), "canceled queued SQL returned rows")?;
    let expired_id = Uuid::new_v4();
    let expires = submit(
        &transaction,
        json!({"request_id":expired_id,
        "sql":"INSERT INTO queue_values VALUES (11)","queue_timeout_seconds":1}),
    );
    let expired = wait_state(&admin, expired_id, "terminal", 5).await?;
    require(
        expired["failure_kind"] == "queue_timeout" && expired["outcome"] == "not_executed",
        format!("queued deadline did not expire independently: {expired}"),
    )?;
    require(expires.await?.is_err(), "expired queued SQL returned rows")?;
    // A valid colleague cannot discover or cancel another principal's request.
    let stranger = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":"queue-stranger"})),
    )
    .await?;
    let other = Client::new(
        &ctx.url,
        stranger["token"].as_str().context("stranger token")?,
    )?;
    expect_status(
        request_json(
            &other,
            Method::POST,
            &format!("/v1/requests/{id}/cancel"),
            None,
        )
        .await,
        404,
    )?;
    cancel(&admin, blocked_id).await?;
    wait_state(&admin, blocked_id, "terminal", 10).await?;
    require(
        blocked.await?.is_err(),
        "interrupted blocker unexpectedly succeeded",
    )?;
    require(
        rows_eq(
            &sql(&transaction, "SELECT v FROM queue_values ORDER BY v").await?,
            &[&["7"]],
        ),
        "cancel/expiry changed prior transaction effects or ran queued writes",
    )?;
    sql(&transaction, "ROLLBACK").await?;
    require(
        rows_eq(
            &sql(&transaction, "SELECT count(*) FROM queue_values").await?,
            &[&["0"]],
        ),
        "queue scheduling lost the real transaction rollback",
    )?;
    sql(
        &transaction,
        "BEGIN; INSERT INTO queue_values VALUES (13); COMMIT",
    )
    .await?;
    require(
        rows_eq(
            &sql(&transaction, "SELECT v FROM queue_values").await?,
            &[&["13"]],
        ),
        "temporary connection state was lost between engine turns",
    )?;
    transaction.close().await?;
    blocker.close().await?;
    request_json(
        &admin,
        Method::DELETE,
        &format!(
            "/v1/credentials/{}",
            stranger["credential_id"]
                .as_str()
                .context("credential id")?
        ),
        None,
    )
    .await?;
    println!(
        "PASS: independent queued cancel/deadline/duplicate controls, single outstanding statement, owner privacy, real transaction rollback and temporary state"
    );
    Ok(())
}

async fn transport_timeout(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let mut first = pinned(ctx, "queue-http-first").await?;
    let mut second = pinned(ctx, "queue-http-second").await?;
    let mut result = pinned(ctx, "queue-http-detached").await?;
    require(
        first.placement["worker_id"] == second.placement["worker_id"]
            && first.placement["worker_id"] == result.placement["worker_id"],
        "detached fixture requires one worker",
    )?;
    sql(&result, "CREATE TEMP TABLE detached_values(v INTEGER)").await?;
    let first_id = Uuid::new_v4();
    let first_run = submit(
        &first,
        json!({"request_id":first_id,"sql":BUSY_SQL,"timeout_seconds":50}),
    );
    wait_state(&admin, first_id, "executing", 10).await?;
    let second_id = Uuid::new_v4();
    let second_run = submit(
        &second,
        json!({"request_id":second_id,"sql":BUSY_SQL,
        "timeout_seconds":40,"queue_timeout_seconds":120}),
    );
    wait_state(&admin, second_id, "queued", 10).await?;
    println!(
        "Checking accepted work after the production 75-second HTTP timeout (about 90 seconds total)"
    );
    let started = Instant::now();
    let error = result
        .execute_with_queue_timeout("INSERT INTO detached_values VALUES (1)", vec![], 120)
        .await
        .err()
        .context("queued request unexpectedly finished before the transport deadline")?;
    require(
        started.elapsed() >= Duration::from_secs(74),
        "fixture did not cross the real HTTP deadline",
    )?;
    require(
        !error.retry_safe && error.acceptance.as_deref() == Some("unknown"),
        format!("transport error lost unknown acceptance: {error:?}"),
    )?;
    let id = Uuid::parse_str(
        error
            .request_id
            .as_deref()
            .context("error omitted durable request ID")?,
    )?;
    let queued = detail(&admin, id).await?;
    require(
        queued["state"] == "queued",
        format!("request was no longer queued when HTTP stopped waiting: {queued}"),
    )?;
    require(
        detail(&admin, second_id).await?["state"] == "executing",
        "second deadline blocker was not executing at HTTP timeout",
    )?;
    let terminal = wait_state(&admin, id, "terminal", 45).await?;
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("detached timing fixture requires platform test DSN")?;
    let queue_seconds = tokio::task::spawn_blocking(move || -> Result<f64> {
        let mut config: postgres::Config = dsn.parse()?;
        config.connect_timeout(Duration::from_secs(3));
        let mut db = config.connect(postgres::NoTls)?;
        db.batch_execute("SET statement_timeout='3s'")?;
        Ok(db.query_one("SELECT EXTRACT(EPOCH FROM(engine_started_at-queued_at))::DOUBLE PRECISION FROM requests WHERE request_id=$1",
            &[&id.to_string()])?.get(0))
    }).await??;
    require(
        queue_seconds >= 75.0,
        format!("write waited only {queue_seconds}s before engine entry"),
    )?;
    require(
        terminal["status"] == "succeeded",
        format!("HTTP timeout canceled accepted work: {terminal}"),
    )?;
    require(
        first_run.await?.is_err() && second_run.await?.is_err(),
        "deadline blockers completed unexpectedly",
    )?;
    for blocker_id in [first_id, second_id] {
        let blocker = detail(&admin, blocker_id).await?;
        require(
            blocker["status"] == "timed_out" && blocker["failure_kind"] == "timeout",
            format!("blocker ended before its execution deadline: {blocker}"),
        )?;
    }
    require(
        rows_eq(
            &sql(&result, "SELECT count(*) FROM detached_values").await?,
            &[&["1"]],
        ),
        "detached write was lost or duplicated",
    )?;
    let usage = request_json(
        &admin,
        Method::GET,
        &format!("/v1/usage?execution_id={id}"),
        None,
    )
    .await?;
    require(
        usage["events"].as_array().context("usage events")?.len() == 1,
        "detached work settled more than once",
    )?;
    first.close().await?;
    second.close().await?;
    result.close().await?;
    println!(
        "PASS: accepted SQL survives the actual HTTP timeout with recoverable identity, one effect and one settlement"
    );
    Ok(())
}

async fn isolated(ctx: &Ctx) -> Result<()> {
    let mut connection = ctx
        .open_named(
            "queue-isolated",
            Some(json!({"mode":"isolated","cpu":1,"memory":"1Gi","disk":"2Gi"})),
        )
        .await?;
    let (id, rows) = connection.execute_tracked("SELECT 42", vec![]).await?;
    require(
        rows_eq(&rows.rows, &[&["42"]]),
        "isolated queue did not execute",
    )?;
    let request = detail(&ctx.client()?, Uuid::parse_str(&id)?).await?;
    require(
        request["state"] == "terminal"
            && request["status"] == "succeeded"
            && request["fingerprint_version"] == 3,
        "isolated execution bypassed durable admission",
    )?;
    connection.close().await?;
    println!("PASS: isolated sessions use the same durable queue identity and settlement");
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx, with_isolated: bool) -> Result<()> {
    queued_controls(ctx).await?;
    if with_isolated {
        isolated(ctx).await?;
    } else {
        transport_timeout(ctx).await?;
    }
    Ok(())
}
