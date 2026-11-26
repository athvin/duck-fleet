//! Durable jobs through real HTTP, DuckDB, PostgreSQL and object storage.
use super::{ARTIFACTS, Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::Client;
use anyhow::{Context, Result};
use arrow_array::{Array, Decimal128Array, Int64Array};
use arrow_ipc::reader::StreamReader;
use duckfleet::protocol::JobRequest;
use reqwest::Method;
use serde_json::{Value, json};
use std::{
    io::Cursor,
    time::{Duration, Instant},
};
use uuid::Uuid;

const BUSY_SQL: &str = "SELECT sum(sin(i)) FROM range(100000000000) AS source(i)";

fn request(sql: &str) -> JobRequest {
    serde_json::from_value(json!({"request_id":Uuid::new_v4(),"sql":sql,
        "compute":{"mode":"persistent","pool":"small"},
        "timeout_seconds":120,"queue_timeout_seconds":120}))
    .expect("static job fixture")
}

async fn submit(client: &Client, request: &JobRequest) -> Result<()> {
    let receipt = client.submit_job(request).await?;
    require(
        receipt["acceptance"] == "accepted"
            && receipt["request_id"] == request.request_id.to_string(),
        format!("job acceptance receipt: {receipt}"),
    )
}

async fn wait(client: &Client, id: Uuid, state: &str, seconds: u64) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        match client.job(id).await {
            Ok(value) if value["state"] == state => return Ok(value),
            Ok(value) => require(
                Instant::now() < deadline,
                format!("job {id} did not reach {state}: {value}"),
            )?,
            Err(error) => require(
                Instant::now() < deadline,
                format!("job {id} became unavailable while waiting: {error}"),
            )?,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn usage(client: &Client, id: Uuid) -> Result<Value> {
    Ok(request_json(
        client,
        Method::GET,
        &format!("/v1/usage?execution_id={id}"),
        None,
    )
    .await?)
}

async fn one_charge(client: &Client, id: Uuid) -> Result<()> {
    let value = usage(client, id).await?;
    require(
        value["events"].as_array().context("usage events")?.len() == 1,
        format!("job must settle exactly once: {value}"),
    )
}

async fn ready(client: &Client, request: &JobRequest) -> Result<Value> {
    submit(client, request).await?;
    let value = wait(client, request.request_id, "terminal", 120).await?;
    require(
        value["status"] == "succeeded"
            && value["sql_completion"] == "succeeded"
            && value["result_state"] == "ready"
            && value["retry_safe"] != true,
        format!("job did not publish successful result: {value}"),
    )?;
    one_charge(client, request.request_id).await?;
    Ok(value)
}

async fn chunks(
    client: &Client,
    id: Uuid,
    mut consume: impl FnMut(arrow_array::RecordBatch) -> Result<()>,
) -> Result<Value> {
    let manifest = client.job_result(id).await?;
    require(
        manifest["complete"] == true && manifest["format"] == "arrow_ipc_chunks",
        format!("invalid complete manifest: {manifest}"),
    )?;
    let descriptors = manifest["chunks"].as_array().context("manifest chunks")?;
    require(
        !descriptors.is_empty() && descriptors.len() <= 4096,
        "manifest chunk count",
    )?;
    let mut rows = 0u64;
    let mut bytes = 0u64;
    for (sequence, descriptor) in descriptors.iter().enumerate() {
        require(
            descriptor["chunk_id"].as_u64() == Some(sequence as u64),
            "chunk order is not dense",
        )?;
        let length = descriptor["bytes"].as_u64().context("chunk bytes")?;
        require(
            length <= 16 * 1024 * 1024,
            "artifact chunk exceeded hard byte bound",
        )?;
        let mut body = Vec::new();
        client
            .download_job_chunk(
                id,
                &sequence.to_string(),
                length,
                descriptor["sha256"].as_str().context("chunk digest")?,
                &mut body,
            )
            .await?;
        let reader = StreamReader::try_new(Cursor::new(body), None)?;
        let mut chunk_rows = 0u64;
        for batch in reader {
            let batch = batch?;
            chunk_rows += batch.num_rows() as u64;
            consume(batch)?;
        }
        require(
            descriptor["rows"].as_u64() == Some(chunk_rows),
            "chunk row count mismatch",
        )?;
        rows += chunk_rows;
        bytes += length;
    }
    require(
        manifest["rows"].as_u64() == Some(rows) && manifest["bytes"].as_u64() == Some(bytes),
        "manifest totals differ from verified chunks",
    )?;
    Ok(manifest)
}

async fn parameter_and_empty(client: &Client, isolated: bool) -> Result<()> {
    let mut query = request("SELECT $1::DECIMAL(20,4) AS exact, NULL::BIGINT AS missing");
    query.parameters = vec![json!("12345678901234.5678")];
    if isolated {
        query.compute =
            serde_json::from_value(json!({"mode":"isolated","cpu":1,"memory":"1Gi","disk":"2Gi"}))?;
    }
    ready(client, &query).await?;
    let mut count = 0;
    chunks(client, query.request_id, |batch| {
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .context("exact decimal array")?;
        require(
            value.value(0) == 123456789012345678 && batch.column(1).is_null(0),
            "job decimal precision or NULL changed",
        )?;
        count += batch.num_rows();
        Ok(())
    })
    .await?;
    require(count == 1, "job result repeated a row")?;
    let duplicate = expect_status(client.submit_job(&query).await, 409)?;
    require(
        duplicate.failure_kind.as_deref() == Some("duplicate_request") && !duplicate.retry_safe,
        "completed job replay was accepted",
    )?;
    let mut different = query.clone();
    different.sql = "SELECT 2".into();
    require(
        expect_status(client.submit_job(&different).await, 409)?
            .failure_kind
            .as_deref()
            == Some("request_id_mismatch"),
        "job ID mismatch did not bind its SQL",
    )?;
    one_charge(client, query.request_id).await?;
    if !isolated {
        let query = request("SELECT 1::BIGINT AS value WHERE false");
        ready(client, &query).await?;
        let value = chunks(client, query.request_id, |batch| {
            require(batch.num_rows() == 0, "empty job emitted rows")
        })
        .await?;
        require(
            value["rows"] == 0
                && value["schema"]["fields"]
                    .as_array()
                    .is_some_and(|f| f.len() == 1),
            "empty job lost its executed schema",
        )?;
    }
    Ok(())
}

async fn large(client: &Client) -> Result<()> {
    let query = request("SELECT i::BIGINT AS value FROM range(4194304) source(i)");
    ready(client, &query).await?;
    let mut rows = 0u64;
    let mut sum = 0i128;
    let manifest = chunks(client, query.request_id, |batch| {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("large BIGINT array")?;
        require(values.null_count() == 0, "large result gained NULLs")?;
        rows += values.len() as u64;
        sum += values.values().iter().map(|v| i128::from(*v)).sum::<i128>();
        Ok(())
    })
    .await?;
    require(
        rows == 4_194_304 && sum == i128::from(rows) * i128::from(rows - 1) / 2,
        "large result omitted or duplicated values",
    )?;
    require(
        manifest["bytes"].as_u64().unwrap_or(0) > 16 * 1024 * 1024
            && manifest["chunks"]
                .as_array()
                .is_some_and(|chunks| chunks.len() > 1),
        "large fixture did not cross the interactive result cap",
    )?;
    println!("PASS: 32 MiB result streams as individually verified bounded Arrow chunks");
    Ok(())
}

async fn side_effects(ctx: &Ctx, unavailable_store: bool) -> Result<()> {
    let client = ctx.client()?;
    let mut connection = ctx.open().await?;
    let table = format!("job_effect_{}", Uuid::new_v4().simple());
    let mut create = request(&format!(
        "CREATE TABLE {table} AS SELECT 7::BIGINT AS value"
    ));
    if !unavailable_store {
        create.result = Some(serde_json::from_value(json!({"max_bytes":1}))?);
    }
    submit(&client, &create).await?;
    let terminal = wait(&client, create.request_id, "terminal", 30).await?;
    require(
        terminal["sql_completion"] == "succeeded"
            && terminal["status"] == "succeeded"
            && terminal["result_state"] == "failed"
            && terminal["retry_safe"] != true,
        format!("materialized SQL lost completion after artifact failure: {terminal}"),
    )?;
    require(
        terminal["result_failure_kind"]
            == if unavailable_store {
                "result_store_unavailable"
            } else {
                "result_too_large"
            },
        format!("artifact fault did not exercise expected boundary: {terminal}"),
    )?;
    expect_status(client.job_result(create.request_id).await, 409)?;
    let duplicate = expect_status(client.submit_job(&create).await, 409)?;
    require(
        !duplicate.retry_safe,
        "artifact failure authorized SQL replay",
    )?;
    require(
        rows_eq(
            &sql(
                &connection,
                &format!("SELECT count(*),sum(value) FROM {table}"),
            )
            .await?,
            &[&["1", "7"]],
        ),
        "CTAS effect missing or repeated",
    )?;
    one_charge(&client, create.request_id).await?;

    // DuckLake refuses RETURNING on its tables at bind time, so the
    // mutating-with-delivery-failure case uses plain DML: its one-row count
    // result still exceeds the sized variant's 1-byte cap and still fails
    // against an unavailable store. Engine-level RETURNING stream semantics
    // stay covered by the engine_stream unit tests.
    let mut insert = request(&format!("INSERT INTO {table} VALUES (11)"));
    insert.result = create.result.clone();
    submit(&client, &insert).await?;
    let terminal = wait(&client, insert.request_id, "terminal", 30).await?;
    require(
        terminal["status"] == "succeeded"
            && terminal["sql_completion"] == "succeeded"
            && terminal["result_state"] == "failed",
        format!("DML lost SQL success: {terminal}"),
    )?;
    expect_status(client.submit_job(&insert).await, 409)?;
    one_charge(&client, insert.request_id).await?;
    require(
        rows_eq(
            &sql(
                &connection,
                &format!("SELECT count(*),sum(value) FROM {table}"),
            )
            .await?,
            &[&["2", "18"]],
        ),
        "DML repeated execution",
    )?;

    let script = request(&format!("INSERT INTO {table} VALUES (99); SELECT 1"));
    submit(&client, &script).await?;
    let terminal = wait(&client, script.request_id, "terminal", 20).await?;
    require(
        terminal["outcome"] == "not_executed" && terminal["sql_completion"] == "not_executed",
        format!("script prefix was not refused before execution: {terminal}"),
    )?;
    require(
        rows_eq(
            &sql(&connection, &format!("SELECT count(*) FROM {table}")).await?,
            &[&["2"]],
        ),
        "strict prepare executed a script prefix",
    )?;
    sql(&connection, &format!("DROP TABLE {table}")).await?;
    connection.close().await?;
    println!(
        "PASS: CTAS and DML keep SQL success after artifact failure, and scripts execute no prefix"
    );
    Ok(())
}

async fn controls(ctx: &Ctx) -> Result<()> {
    let client = ctx.client()?;
    let active = request(BUSY_SQL);
    submit(&client, &active).await?;
    wait(&client, active.request_id, "executing", 15).await?;
    let waiting = request("SELECT 99");
    submit(&client, &waiting).await?;
    let value = wait(&client, waiting.request_id, "queued", 5).await?;
    require(
        value["cancel_requested"] != true,
        "fresh queued job already canceled",
    )?;
    client.cancel_job(waiting.request_id).await?;
    let queued = wait(&client, waiting.request_id, "terminal", 5).await?;
    require(
        queued["outcome"] == "not_executed" && queued["status"] == "canceled",
        format!("queued cancellation executed SQL: {queued}"),
    )?;
    client.cancel_job(active.request_id).await?;
    let stopped = wait(&client, active.request_id, "terminal", 5).await?;
    require(
        stopped["status"] == "canceled" && stopped["retry_safe"] != true,
        format!("running job cancellation was not terminal: {stopped}"),
    )?;
    one_charge(&client, active.request_id).await?;
    Ok(())
}

async fn result_authorization(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let owner = format!("job-owner-{}", Uuid::new_v4().simple());
    let other = format!("job-other-{}", Uuid::new_v4().simple());
    let first = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":owner})),
    )
    .await?;
    let second = request_json(
        &admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":other})),
    )
    .await?;
    let actor = Client::new(
        &ctx.url,
        first["token"].as_str().context("owner credential")?,
    )?;
    let stranger = Client::new(
        &ctx.url,
        second["token"].as_str().context("other credential")?,
    )?;
    let mut query = request("SELECT 17::BIGINT AS private_value");
    query.timeout_seconds = Some(30);
    ready(&actor, &query).await?;
    let manifest = actor.job_result(query.request_id).await?;
    expect_status(stranger.job(query.request_id).await, 404)?;
    expect_status(stranger.job_result(query.request_id).await, 404)?;
    let mut attempted = Vec::new();
    let chunk = &manifest["chunks"][0];
    expect_status(
        stranger
            .download_job_chunk(
                query.request_id,
                "0",
                chunk["bytes"].as_u64().context("private bytes")?,
                chunk["sha256"].as_str().context("private hash")?,
                &mut attempted,
            )
            .await,
        404,
    )?;
    require(
        attempted.is_empty(),
        "unauthorized chunk wrote result bytes",
    )?;
    let path = format!("/v1/principals/{owner}/policy");
    request_json(
        &admin,
        Method::PUT,
        &path,
        Some(&json!({"roles":["viewer"],"permissions":[],"compute":[],"disabled":false})),
    )
    .await?;
    // History access remains useful, but it does not authorize raw schemas/data.
    let metadata = actor.job(query.request_id).await?;
    require(
        metadata["schema"].is_null(),
        "job status exposed raw result schema",
    )?;
    expect_status(actor.job_result(query.request_id).await, 404)?;
    request_json(&admin, Method::PUT, &path,
        Some(&json!({"roles":["viewer"],"permissions":[{"action":"result.read","scope":"own"}],"compute":[],"disabled":false}))).await?;
    actor.job_result(query.request_id).await?;
    for credential in [&first, &second] {
        request_json(
            &admin,
            Method::DELETE,
            &format!(
                "/v1/credentials/{}",
                credential["credential_id"]
                    .as_str()
                    .context("credential id")?
            ),
            None,
        )
        .await?;
    }
    expect_status(actor.job_result(query.request_id).await, 401)?;
    println!(
        "PASS: every manifest/chunk read checks fresh result permission independently of history"
    );
    Ok(())
}

async fn private_sessions_closed() -> Result<()> {
    let dsn = std::env::var("DUCKFLEET_PLATFORM_TEST_DSN")
        .context("job fixtures require platform test DSN")?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut config: postgres::Config = dsn.parse()?;
        config.connect_timeout(Duration::from_secs(3));
        config.options("-c statement_timeout=3000");
        let mut db = config.connect(postgres::NoTls)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let rows: i64 = db.query_one("SELECT COUNT(*)::BIGINT FROM job_bindings b JOIN requests r USING(request_id) JOIN queue_sessions s ON s.session_id=b.session_id WHERE r.state='terminal' AND s.closed_at IS NULL", &[])?.get(0);
            if rows == 0 { return Ok(()); }
            require(Instant::now() < deadline, "terminal private job sessions leaked capacity")?;
            std::thread::sleep(Duration::from_millis(100));
        }
    }).await??;
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx, isolated: bool) -> Result<()> {
    let client = ctx.client()?;
    parameter_and_empty(&client, isolated).await?;
    if !isolated {
        large(&client).await?;
        side_effects(ctx, false).await?;
        controls(ctx).await?;
        result_authorization(ctx).await?;
    }
    private_sessions_closed().await?;
    println!("PASS: durable jobs, verified results, retained identity and private session cleanup");
    Ok(())
}

pub(super) async fn store_failure(ctx: &Ctx) -> Result<()> {
    side_effects(ctx, true).await?;
    private_sessions_closed().await
}

pub(super) async fn long_running(ctx: &Ctx) -> Result<()> {
    let query = request(BUSY_SQL);
    let client = ctx.client()?;
    submit(&client, &query).await?;
    wait(&client, query.request_id, "executing", 20).await?;
    let started = Instant::now();
    std::fs::write(
        format!("{ARTIFACTS}/jobs-long.started"),
        query.request_id.to_string(),
    )?;
    drop(client);
    // The parent restarts only the gateway after observing this durable marker.
    // Neither dropping the submission client nor a gateway lifetime owns SQL.
    tokio::time::sleep(Duration::from_secs(65)).await;
    let client = ctx.client()?;
    let running = wait(&client, query.request_id, "executing", 20).await?;
    require(
        started.elapsed() >= Duration::from_secs(65) && running["sql_completion"] == "pending",
        format!("detached long job was implicitly canceled: {running}"),
    )?;
    client.cancel_job(query.request_id).await?;
    let terminal = wait(&client, query.request_id, "terminal", 5).await?;
    require(
        terminal["status"] == "canceled"
            && terminal["retry_safe"] != true
            && terminal["elapsed_ms"]
                .as_i64()
                .is_some_and(|elapsed| elapsed >= 65_000),
        format!("long job did not honor explicit cancellation: {terminal}"),
    )?;
    one_charge(&client, query.request_id).await?;
    private_sessions_closed().await?;
    std::fs::write(
        format!("{ARTIFACTS}/jobs-long.json"),
        serde_json::to_vec_pretty(&terminal)?,
    )?;
    println!(
        "PASS: job ran over65 seconds across submission disconnect and gateway restart, then canceled and settled once"
    );
    Ok(())
}
