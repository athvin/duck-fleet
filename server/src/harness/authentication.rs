//! Real HTTP/pgwire credential continuity, queue admission, and cleanup checks.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::{Client, RemoteConnection};

struct Credential {
    id: String,
    token: String,
}

async fn issue(admin: &Client, principal: &str, seconds: Option<u64>) -> Result<Credential> {
    let issued = request_json(
        admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal": principal, "expires_seconds": seconds})),
    )
    .await?;
    Ok(Credential {
        id: issued["credential_id"]
            .as_str()
            .context("credential ID")?
            .to_owned(),
        token: issued["token"].as_str().context("issued token")?.to_owned(),
    })
}

async fn revoke(admin: &Client, credential: &Credential) -> Result<()> {
    request_json(
        admin,
        Method::DELETE,
        &format!("/v1/credentials/{}", credential.id),
        None,
    )
    .await?;
    Ok(())
}

async fn open(ctx: &Ctx, credential: &Credential) -> Result<RemoteConnection> {
    Ok(RemoteConnection::open(
        &ctx.url,
        &credential.token,
        "auth-continuity",
        None,
        None,
        5,
    )
    .await?)
}

/// The synchronous PostgreSQL client always runs off the async executor, including
/// connection setup and drop. No plaintext credential enters test output.
struct Pg(Option<postgres::Client>);

impl Drop for Pg {
    fn drop(&mut self) {
        if let Some(pg) = self.0.take() {
            // Also safe when a failed assertion returns before explicit close.
            tokio::task::spawn_blocking(move || drop(pg));
        }
    }
}

impl Pg {
    async fn connect(ctx: &Ctx, token: &str) -> Result<Self> {
        let host = reqwest::Url::parse(&ctx.url)?
            .host_str()
            .context("gateway hostname")?
            .to_owned();
        let token = token.to_owned();
        let pg = tokio::task::spawn_blocking(move || {
            postgres::Config::new()
                .host(&host)
                .port(5432)
                .user("advisory-user")
                .dbname("lake")
                .password(token)
                .connect_timeout(Duration::from_secs(5))
                .connect(postgres::NoTls)
        })
        .await??;
        Ok(Self(Some(pg)))
    }

    async fn query(
        &mut self,
        statement: &str,
    ) -> Result<Vec<postgres::SimpleQueryMessage>, postgres::Error> {
        let mut pg = self.0.take().expect("test connection exists");
        let statement = statement.to_owned();
        let (pg, result) = tokio::task::spawn_blocking(move || {
            let result = pg.simple_query(&statement);
            (pg, result)
        })
        .await
        .expect("pgwire test task did not panic");
        self.0 = Some(pg);
        result
    }

    async fn close(mut self) {
        let pg = self.0.take();
        tokio::task::spawn_blocking(move || drop(pg))
            .await
            .expect("pgwire close task");
    }
}

fn denied_pg(result: Result<Vec<postgres::SimpleQueryMessage>, postgres::Error>) -> Result<()> {
    let error = result
        .err()
        .context("invalid credential executed pgwire SQL")?;
    require(
        error.is_closed()
            || error
                .code()
                .is_some_and(|code| matches!(code.code(), "28000" | "28P01")),
        format!("expected authentication refusal or terminated connection, got {error}"),
    )
}

async fn http_expiry(ctx: &Ctx, admin: &Client) -> Result<()> {
    let credential = issue(admin, "auth-expiry", Some(2)).await?;
    let client = Client::new(&ctx.url, &credential.token)?;
    request_json(&client, Method::GET, "/v1/queries?limit=1", None).await?;
    tokio::time::sleep(Duration::from_millis(2300)).await;
    // A read endpoint exercises the gateway cache itself, without a worker check
    // masking an expired entry that is still accepted by middleware.
    expect_status(
        request_json(&client, Method::GET, "/v1/queries?limit=1", None).await,
        401,
    )?;
    revoke(admin, &credential).await?;
    let unknown = Client::new(&ctx.url, "dfk_unknown")?;
    expect_status(
        request_json(&unknown, Method::GET, "/v1/queries", None).await,
        401,
    )?;
    println!("PASS: HTTP cache has no grace period after credential expiry");
    Ok(())
}

async fn pgwire_continuity(ctx: &Ctx, admin: &Client) -> Result<()> {
    require(
        Pg::connect(ctx, "dfk_unknown").await.is_err(),
        "pgwire accepted unknown credential",
    )?;
    let lazy = issue(admin, "auth-lazy", None).await?;
    let mut pg = Pg::connect(ctx, &lazy.token).await?;
    revoke(admin, &lazy).await?;
    denied_pg(pg.query("select 1 /* revoked-before-lazy-open */").await)?;
    pg.close().await;

    let expiring = issue(admin, "auth-pg-expiry", Some(2)).await?;
    let mut pg = Pg::connect(ctx, &expiring.token).await?;
    pg.query("select 1").await?;
    tokio::time::sleep(Duration::from_millis(2300)).await;
    denied_pg(pg.query("select 2 /* expired-established-pgwire */").await)?;
    pg.close().await;
    revoke(admin, &expiring).await?;

    let revoked = issue(admin, "auth-pg-revoke", None).await?;
    let mut pg = Pg::connect(ctx, &revoked.token).await?;
    pg.query("select 1").await?;
    revoke(admin, &revoked).await?;
    denied_pg(pg.query("select 2 /* revoked-established-pgwire */").await)?;
    pg.close().await;
    println!("PASS: pgwire checks credentials before lazy session creation and later statements");
    Ok(())
}

async fn rotation(ctx: &Ctx, admin: &Client) -> Result<()> {
    let original = issue(admin, "auth-rotation", None).await?;
    let replacement = issue(admin, "auth-rotation", None).await?;
    let connection = open(ctx, &original).await?;
    sql(
        &connection,
        "create temp table rotated_session(value integer)",
    )
    .await?;
    let rotated = Client::new(&ctx.url, &replacement.token)?;
    let path = format!("{}/queries", connection.path());
    let rotated_id = uuid::Uuid::new_v4().to_string();
    // Use B on A's session before revocation, proving ownership is principal-based
    // and moving idle-cleanup responsibility to the credential now using it.
    rotated
        .request(
            Method::POST,
            &path,
            Some(&json!({
                "sql": "insert into rotated_session values (42)", "request_id": rotated_id,
                "context": {"principal": "bootstrap-admin", "credential_id": "spoofed"}
            })),
            Duration::from_secs(10),
        )
        .await?;
    let recorded = request_json(
        admin,
        Method::GET,
        &format!("/v1/queries/{rotated_id}"),
        None,
    )
    .await?;
    require(
        recorded["principal"] == "auth-rotation",
        "query context spoofed authenticated identity",
    )?;
    revoke(admin, &original).await?;
    expect_status(connection.execute("select 1", Vec::new()).await, 401)?;
    // Cross a full worker housekeeping period: revoking A must not reap a session
    // now legitimately held by B, nor destroy its temp tables.
    tokio::time::sleep(Duration::from_secs(6)).await;
    rotated
        .request(
            Method::POST,
            &path,
            Some(&json!({
                "sql": "select value from rotated_session", "request_id": uuid::Uuid::new_v4()
            })),
            Duration::from_secs(10),
        )
        .await?;
    request_json(&rotated, Method::DELETE, &connection.path(), None).await?;
    revoke(admin, &replacement).await?;
    println!("PASS: rotating one credential preserves the principal's active session");
    Ok(())
}

async fn wait_running(admin: &Client, id: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(record) =
            request_json(admin, Method::GET, &format!("/v1/queries/{id}"), None).await
        {
            require(
                record["status"] == "running",
                format!("busy fixture ended too early: {record}"),
            )?;
            return Ok(());
        }
        require(
            Instant::now() < deadline,
            "busy query did not reach admission",
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_worker(admin: &Client, id: &str, predicate: impl Fn(&Value) -> bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let fleet = request_json(admin, Method::GET, "/v1/workers", None).await?;
        if fleet["workers"]
            .as_array()
            .is_some_and(|workers| workers.iter().any(|w| w["worker_id"] == id && predicate(w)))
        {
            return Ok(());
        }
        require(
            Instant::now() < deadline,
            format!("worker did not reach expected state: {fleet}"),
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn queued_revocation(ctx: &Ctx, admin: &Client) -> Result<()> {
    let mut operator = ctx.open_named("auth-queue-observer", None).await?;
    let table = format!("lake.main.auth_queue_{}", uuid::Uuid::new_v4().simple());
    sql(&operator, &format!("create table {table}(value integer)")).await?;
    let credential = issue(admin, "auth-queued", None).await?;
    let running = open(ctx, &credential).await?;
    let queued = open(ctx, &credential).await?;
    require(
        running.placement["worker_id"] == queued.placement["worker_id"],
        "auth queue fixture requires one worker",
    )?;
    let worker_id = running.placement["worker_id"]
        .as_str()
        .context("worker ID")?
        .to_owned();
    let run_id = uuid::Uuid::new_v4().to_string();
    let queued_id = uuid::Uuid::new_v4().to_string();
    let client = running.client().clone();
    let path = format!("{}/queries", running.path());
    let body = json!({"request_id": run_id, "sql": "select sum(range % 7) from range(5000000000)", "timeout_seconds": 60});
    let active = tokio::spawn(async move {
        client
            .request(Method::POST, &path, Some(&body), Duration::from_secs(70))
            .await
    });
    let result = async {
        wait_running(admin, &run_id).await?;
        let client = queued.client().clone();
        let path = format!("{}/queries", queued.path());
        let body =
            json!({"request_id": queued_id, "sql": format!("insert into {table} values (1)")});
        let pending = tokio::spawn(async move {
            client
                .request(Method::POST, &path, Some(&body), Duration::from_secs(70))
                .await
        });
        wait_worker(admin, &worker_id, |w| {
            w["queued_queries"].as_u64().is_some_and(|n| n > 0)
        })
        .await?;
        revoke(admin, &credential).await?;
        active.await??;
        let refused = expect_status(pending.await?, 401)?;
        require(
            refused.failure_kind.as_deref() == Some("unauthorized"),
            format!("queued refusal: {refused}"),
        )?;
        let completed =
            request_json(admin, Method::GET, &format!("/v1/queries/{run_id}"), None).await?;
        require(
            completed["status"] == "succeeded",
            format!("revocation interrupted active work: {completed}"),
        )?;
        expect_status(
            request_json(
                admin,
                Method::GET,
                &format!("/v1/queries/{queued_id}"),
                None,
            )
            .await,
            404,
        )?;
        require(
            rows_eq(
                &sql(&operator, &format!("select count(*) from {table}")).await?,
                &[&["0"]],
            ),
            "revoked queued write had an effect",
        )?;
        expect_status(running.execute("select 1", Vec::new()).await, 401)?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let _ = request_json(admin, Method::DELETE, &running.path(), None).await;
    let _ = request_json(admin, Method::DELETE, &queued.path(), None).await;
    let _ = revoke(admin, &credential).await;
    sql(&operator, &format!("drop table {table}")).await?;
    operator.close().await?;
    result?;
    println!(
        "PASS: active SQL completes after revocation; queued and follow-up SQL execute no effects"
    );
    Ok(())
}

async fn idle_rollback(ctx: &Ctx, admin: &Client) -> Result<()> {
    let mut operator = ctx.open_named("auth-rollback-observer", None).await?;
    let table = format!("lake.main.auth_rollback_{}", uuid::Uuid::new_v4().simple());
    sql(&operator, &format!("create table {table}(value integer)")).await?;
    let worker_id = operator.placement["worker_id"]
        .as_str()
        .context("worker ID")?;
    let fleet = request_json(admin, Method::GET, "/v1/workers", None).await?;
    let endpoint = fleet["workers"]
        .as_array()
        .and_then(|workers| workers.iter().find(|w| w["worker_id"] == worker_id))
        .and_then(|worker| worker["endpoint"].as_str())
        .context("worker endpoint")?;
    let direct = Client::new(endpoint, &ctx.token)?;
    let credential = issue(admin, "auth-idle", None).await?;
    let abandoned = open(ctx, &credential).await?;
    sql(&abandoned, "begin").await?;
    sql(&abandoned, &format!("insert into {table} values (7)")).await?;
    revoke(admin, &credential).await?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    // Probe with the internal operator credential after the idle sweep. An auth
    // refusal or a gateway route sweep must not mask a live worker transaction.
    expect_status(
        direct
            .request(
                Method::POST,
                &format!("{}/queries", abandoned.path()),
                Some(&json!({
                    "sql": "select 1", "request_id": uuid::Uuid::new_v4()
                })),
                Duration::from_secs(10),
            )
            .await,
        404,
    )?;
    require(
        rows_eq(
            &sql(&operator, &format!("select count(*) from {table}")).await?,
            &[&["0"]],
        ),
        "abandoned revoked transaction was committed",
    )?;
    sql(&operator, &format!("drop table {table}")).await?;
    operator.close().await?;
    println!(
        "PASS: idle revoked sessions are reaped and transactions roll back without client cleanup"
    );
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    http_expiry(ctx, &admin).await?;
    pgwire_continuity(ctx, &admin).await?;
    rotation(ctx, &admin).await?;
    queued_revocation(ctx, &admin).await?;
    idle_rollback(ctx, &admin).await?;
    Ok(())
}

/// Kubernetes checks actual pod removal after this returns. Intentionally never
/// close the session or send a refused query that could trigger cleanup.
pub(super) async fn isolated_cleanup(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let credential = issue(&admin, "auth-isolated", None).await?;
    let connection = RemoteConnection::open(
        &ctx.url,
        &credential.token,
        "auth-isolated",
        Some(json!({"cpu": 1, "memory": "1Gi"})),
        None,
        90,
    )
    .await?;
    sql(&connection, "begin").await?;
    sql(
        &connection,
        "create temp table abandoned_auth_transaction(value integer)",
    )
    .await?;
    revoke(&admin, &credential).await?;
    tokio::time::sleep(Duration::from_secs(22)).await;
    println!("PASS: revoked isolated credential left idle without an explicit close");
    Ok(())
}
