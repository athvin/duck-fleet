//! History availability, pagination, profile identity, and auth on the read
//! endpoints. Ported from `tests/observability.py`.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, request_json, require, write_artifact};
use crate::client::{Client, RemoteConnection};

pub(super) async fn checks(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let run = uuid::Uuid::new_v4().to_string();
    let context = json!({
        "model_run_id": run,
        "dbt": {"node_id": "model.observability.fixture", "invocation_id": run},
    });
    let connection = RemoteConnection::open(
        &ctx.url,
        &ctx.token,
        "observability",
        None,
        Some(context),
        90,
    )
    .await?;

    // A succeeding statement captures a profile that belongs to its own SQL.
    let (first_id, _) = connection
        .execute_tracked(
            "select sum(i) as observed_total from range(10000) t(i)",
            Vec::new(),
        )
        .await?;
    let first = request_json(
        &client,
        Method::GET,
        &format!("/v1/queries/{first_id}"),
        None,
    )
    .await?;
    require(first["status"] == json!("succeeded"), format!("{first}"))?;
    require(
        first["profile_status"] == json!("captured"),
        format!("{first}"),
    )?;

    // A failing statement is recorded as failed and does not reuse the prior profile.
    let failed_id = uuid::Uuid::new_v4().to_string();
    let _ = client
        .request(
            Method::POST,
            &format!("{}/queries", connection.path()),
            Some(
                &json!({"request_id": failed_id, "sql": "select * from table_that_does_not_exist"}),
            ),
            crate::client::STATEMENT_TIMEOUT,
        )
        .await;
    let failed = request_json(
        &client,
        Method::GET,
        &format!("/v1/queries/{failed_id}"),
        None,
    )
    .await?;
    require(failed["status"] == json!("failed"), format!("{failed}"))?;

    // A couple more, then page over this run's records with a cursor.
    let (id3, _) = connection
        .execute_tracked("select 73 as next_profile", Vec::new())
        .await?;
    let (id4, _) = connection
        .execute_tracked("select 74 as final_profile", Vec::new())
        .await?;
    let mut found: Vec<String> = Vec::new();
    let mut cursor: Option<i64> = None;
    loop {
        let mut path = format!("/v1/queries?limit=2&model_run_id={run}");
        if let Some(c) = cursor {
            path.push_str(&format!("&before={c}"));
        }
        let page = request_json(&client, Method::GET, &path, None).await?;
        let queries = page["queries"].as_array().cloned().unwrap_or_default();
        require(
            queries.iter().all(|q| q.get("profile").is_none()),
            "list includes expensive raw plans",
        )?;
        for q in &queries {
            found.push(q["query_id"].as_str().unwrap_or_default().to_owned());
        }
        match page["next_before"].as_i64() {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    let mut expected = vec![first_id.clone(), failed_id.clone(), id3, id4];
    expected.reverse();
    require(
        found == expected,
        format!("pagination missed or duplicated records: {found:?}"),
    )?;

    // A SQL-injection-shaped filter value is treated as data, returning nothing.
    let injected = request_json(
        &client,
        Method::GET,
        "/v1/queries?model_id=%27%20OR%20true%20--",
        None,
    )
    .await?;
    require(
        injected["queries"].as_array().is_some_and(|q| q.is_empty()),
        "filter input became SQL",
    )?;

    // Bad filter inputs are classified 4xx.
    let bad: [(String, u16); 4] = [
        ("/v1/queries?limit=501".to_owned(), 400),
        ("/v1/queries?before=0".to_owned(), 400),
        ("/v1/queries?status=bogus".to_owned(), 400),
        (format!("/v1/queries/{}", uuid::Uuid::new_v4()), 404),
    ];
    for (path, expected) in &bad {
        super::expect_status(
            request_json(&client, Method::GET, path, None).await,
            *expected,
        )?;
    }

    // History is behind authentication.
    let wrong = Client::new(&ctx.url, "wrong")?;
    super::expect_status(
        request_json(&wrong, Method::GET, "/v1/queries", None).await,
        401,
    )?;

    write_artifact("profile-query-id.txt", &first_id);
    let mut connection = connection;
    connection.close().await?;
    worker_status(ctx).await?;
    println!("PASS: independent history, cursor pagination, filters, auth, and profile identity");
    Ok(())
}

// A streaming aggregate: long enough to cancel deliberately, without allocating
// a huge result or a hash table. Real engine execution, not a test-only sleep.
const BUSY_SQL: &str = "select sum(range % 7) from range(1000000000000)";
const STATUS_TIMEOUT: Duration = Duration::from_secs(4);

async fn fleet(client: &Client) -> anyhow::Result<Value> {
    let bytes = client
        .request(Method::GET, "/v1/workers", None, STATUS_TIMEOUT)
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn worker<'a>(report: &'a Value, id: &str) -> anyhow::Result<&'a Value> {
    report["workers"]
        .as_array()
        .and_then(|workers| workers.iter().find(|worker| worker["worker_id"] == id))
        .ok_or_else(|| anyhow::anyhow!("worker {id} missing from fleet report: {report}"))
}

fn valid_snapshot(snapshot: &Value) -> anyhow::Result<()> {
    require(
        snapshot["boot_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        format!("worker has no boot identity: {snapshot}"),
    )?;
    for key in ["active_queries", "queued_queries", "open_sessions"] {
        require(
            snapshot[key].as_u64().is_some(),
            format!("worker has no nonnegative {key}: {snapshot}"),
        )?;
    }
    require(
        snapshot["ready"] == true && snapshot["draining"] == false,
        format!("fixture worker is not ready: {snapshot}"),
    )?;
    require(
        if snapshot["queued_queries"] == 0 {
            snapshot["oldest_queued_ms"].is_null()
        } else {
            snapshot["oldest_queued_ms"].as_u64().is_some()
        },
        format!("queue age does not match queue depth: {snapshot}"),
    )
}

async fn wait_worker(
    client: &Client,
    id: &str,
    matches: impl Fn(&Value) -> bool,
) -> anyhow::Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let report = fleet(client).await?;
        let snapshot = worker(&report, id)?;
        valid_snapshot(snapshot)?;
        if matches(snapshot) {
            return Ok(snapshot.clone());
        }
        require(
            Instant::now() < deadline,
            format!("worker state did not converge: {snapshot}"),
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A query remains active longer than the five-second discovery timeout while
/// both worker and fleet status keep responding. A queued query is counted too.
pub(super) async fn worker_status(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let internal_token = ctx.internal_test_token()?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let baseline = fleet(&client).await?;
    let mut session = ctx.open_named("worker-status", None).await?;
    let id = session.placement["worker_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("session has no worker identity"))?
        .to_owned();
    let baseline_sessions = worker(&baseline, &id)?["open_sessions"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("worker has no session count"))?;
    let endpoint = worker(&baseline, &id)?["endpoint"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("worker has no endpoint"))?;
    let direct = Client::new(endpoint, &internal_token)?;
    // A session allows one outstanding accepted query. Open the second real
    // connection before occupying the serial engine, pinned to the same worker.
    let mut queued_session = RemoteConnection::open(
        endpoint,
        &internal_token,
        "worker-status-queued",
        None,
        None,
        90,
    )
    .await?;
    let query_client = client.clone();
    let query_path = format!("{}/queries", session.path());
    let busy = tokio::spawn(async move {
        query_client
            .request(
                Method::POST,
                &query_path,
                Some(&json!({
                    "request_id": uuid::Uuid::new_v4(),
                    "sql": BUSY_SQL,
                    "timeout_seconds": 30,
                })),
                crate::client::STATEMENT_TIMEOUT,
            )
            .await
    });
    let mut queued = None;
    // Always cancel/join below, including when an assertion fails: a broken
    // status endpoint must not leave expensive fixture work behind.
    let checked = async {
        let active = wait_worker(&client, &id, |w| w["active_queries"] == 1).await?;
        let boot = active["boot_id"].clone();
        require(
            active["open_sessions"] == baseline_sessions + 2,
            format!("HTTP session missing from worker census: {active}"),
        )?;
        let query_client = queued_session.client().clone();
        let query_path = format!("{}/queries", queued_session.path());
        queued = Some(tokio::spawn(async move {
            query_client
                .request(
                    Method::POST,
                    &query_path,
                    Some(&json!({
                        "request_id": uuid::Uuid::new_v4(),
                        "sql": "select 1",
                        "queue_timeout_seconds": 30,
                    })),
                    crate::client::STATEMENT_TIMEOUT,
                )
                .await
        }));
        wait_worker(&client, &id, |w| w["queued_queries"] == 1).await?;
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(6) {
            let bytes = direct
                .request(Method::GET, "/v1/worker", None, STATUS_TIMEOUT)
                .await?;
            let snapshot: Value = serde_json::from_slice(&bytes)?;
            valid_snapshot(&snapshot)?;
            require(
                snapshot["state"] == "busy"
                    && snapshot["active_queries"] == 1
                    && snapshot["queued_queries"] == 1
                    && snapshot["boot_id"] == boot,
                format!("busy worker status is inconsistent: {snapshot}"),
            )?;
            let report = fleet(&client).await?;
            let observed = worker(&report, &id)?;
            require(
                observed["state"] == "busy" && observed["active_queries"] == 1,
                format!("healthy busy worker disappeared from discovery: {report}"),
            )?;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let canceled = session.cancel().await;
    let outcome = busy.await?;
    let queued_outcome = match queued {
        Some(query) => Some(query.await?),
        None => None,
    };
    let queued_closed = queued_session.close().await;
    let closed = session.close().await;
    checked?;
    canceled?;
    require(
        outcome
            .as_ref()
            .err()
            .is_some_and(|error| error.failure_kind.as_deref() == Some("canceled")),
        "fixture's long query was not canceled",
    )?;
    if let Some(outcome) = queued_outcome {
        outcome?;
    }
    closed?;
    queued_closed?;
    let idle = wait_worker(&client, &id, |w| {
        w["active_queries"] == 0
            && w["queued_queries"] == 0
            && w["open_sessions"] == baseline_sessions
    })
    .await?;
    require(idle["state"] == "ready", format!("{idle}"))?;
    write_artifact("worker-status.json", &serde_json::to_string_pretty(&idle)?);
    println!(
        "PASS: worker and fleet status stay responsive during >5s SQL, with queue/session counters cleaned up"
    );
    Ok(())
}

/// Requires two configured persistent workers and the gateway's pgwire listener.
/// A real pgwire query must influence placement of a new HTTP session.
pub(super) async fn mixed_protocol_status(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let internal_token = ctx.internal_test_token()?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let baseline = fleet(&client).await?;
    require(
        baseline["workers"].as_array().is_some_and(|w| w.len() == 2),
        "mixed-protocol placement fixture needs exactly two workers",
    )?;
    let host = reqwest::Url::parse(&ctx.url)?
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("gateway URL has no host"))?
        .to_owned();
    let token = ctx.token.clone();
    let marker = format!("worker-status-pgwire-{}", uuid::Uuid::new_v4());
    let statement = format!("/* {marker} */ {BUSY_SQL}");
    let busy = tokio::task::spawn_blocking(move || {
        let mut pg = postgres::Config::new()
            .host(&host)
            .port(5432)
            .user("operator")
            .dbname("lake")
            .password(token)
            .connect_timeout(Duration::from_secs(5))
            .connect(postgres::NoTls)?;
        pg.simple_query(&statement)
    });
    let mut active_session = None;
    let checked = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        let record = loop {
            let history = request_json(
                &client,
                Method::GET,
                "/v1/queries?status=running&limit=100",
                None,
            )
            .await?;
            if let Some(record) = history["queries"].as_array().and_then(|queries| {
                queries
                    .iter()
                    .find(|q| q["sql"].as_str().is_some_and(|s| s.contains(&marker)))
            }) {
                break record.clone();
            }
            require(Instant::now() < deadline, "pgwire query did not start")?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let id = record["worker_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("pgwire history has no worker"))?;
        let endpoint = worker(&baseline, id)?["endpoint"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("worker has no endpoint"))?;
        let session_id = record["session_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("pgwire history has no session"))?;
        active_session = Some((
            Client::new(endpoint, &internal_token)?,
            session_id.to_owned(),
        ));
        let active = wait_worker(&client, id, |w| w["active_queries"] == 1).await?;
        require(
            active["open_sessions"].as_u64()
                == worker(&baseline, id)?["open_sessions"]
                    .as_u64()
                    .map(|n| n + 1),
            format!("pgwire session absent from the worker census: {active}"),
        )?;
        let mut http = tokio::time::timeout(
            Duration::from_secs(5),
            ctx.open_named("http-alongside-pgwire", None),
        )
        .await??;
        let placed_elsewhere = http.placement["worker_id"] != id;
        let answered = super::sql(&http, "select 42").await;
        let closed = http.close().await;
        require(
            placed_elsewhere,
            "HTTP session was routed to a worker busy with pgwire SQL",
        )?;
        require(
            super::rows_eq(&answered?, &[&["42"]]),
            "HTTP sibling did not answer",
        )?;
        closed?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    // Cancel through the worker's authenticated internal endpoint: pgwire does
    // not implement CancelRequest.
    if let Some((direct, id)) = active_session {
        let _ = request_json(
            &direct,
            Method::POST,
            &format!("/v1/sessions/{id}/cancel"),
            None,
        )
        .await;
    }
    let outcome = busy.await?;
    checked?;
    require(
        outcome
            .as_ref()
            .err()
            .and_then(postgres::Error::code)
            .is_some_and(|code| code.code() == "57014"),
        "pgwire query did not finish with classified cancellation",
    )?;
    for original in baseline["workers"].as_array().unwrap() {
        let id = original["worker_id"].as_str().unwrap_or_default();
        wait_worker(&client, id, |w| {
            w["active_queries"] == 0
                && w["queued_queries"] == 0
                && w["open_sessions"] == original["open_sessions"]
        })
        .await?;
    }
    println!("PASS: pgwire load is visible and new HTTP sessions use the idle sibling");
    Ok(())
}

/// Run after a previously observed sibling has stopped, without restarting the
/// gateway, so the unavailable entry must preserve its last observation time.
pub(super) async fn partial_outage(ctx: &Ctx) -> anyhow::Result<()> {
    // Let the one-second load cache expire after the script stops the sibling.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let client = ctx.client()?;
    let bytes = client
        .request(Method::GET, "/v1/workers", None, Duration::from_secs(7))
        .await?;
    let report: Value = serde_json::from_slice(&bytes)?;
    let workers = report["workers"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing fleet: {report}"))?;
    require(
        workers.len() == 2,
        format!("partial report lost a configured worker: {report}"),
    )?;
    let missing = workers
        .iter()
        .find(|w| w["state"] == "unreachable")
        .ok_or_else(|| anyhow::anyhow!("no unreachable entry: {report}"))?;
    require(
        missing["last_success_at"].as_u64().is_some(),
        format!("last successful observation was lost: {missing}"),
    )?;
    require(
        missing["endpoint"].as_str().is_some(),
        format!("unreachable endpoint was lost: {missing}"),
    )?;
    require(
        missing.get("active_queries").is_none(),
        format!("stale pressure looks current: {missing}"),
    )?;
    require(
        workers.iter().any(|w| w["state"] == "ready"),
        format!("healthy sibling disappeared: {report}"),
    )?;
    let mut connection = ctx.open_named("partial-outage", None).await?;
    let outcome = super::sql(&connection, "select 42").await;
    let closed = connection.close().await;
    require(
        super::rows_eq(&outcome?, &[&["42"]]),
        "healthy sibling did not execute",
    )?;
    closed?;
    write_artifact(
        "worker-partial-outage.json",
        &serde_json::to_string_pretty(&report)?,
    );
    println!("PASS: a stopped worker remains visible alongside the serving sibling");
    Ok(())
}

pub(super) async fn history_without_workers(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let records = request_json(&client, Method::GET, "/v1/queries?limit=10", None).await?;
    let list = records["queries"].as_array().cloned().unwrap_or_default();
    require(
        !list.is_empty(),
        "history disappeared while workers were stopped",
    )?;
    let id = list[0]["query_id"].as_str().unwrap_or_default();
    let detail = request_json(&client, Method::GET, &format!("/v1/queries/{id}"), None).await?;
    require(
        detail["query_id"].as_str() == Some(id),
        "query detail unavailable without workers",
    )?;
    println!("PASS: query history and details remain available with both workers stopped");
    Ok(())
}
