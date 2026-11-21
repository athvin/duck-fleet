//! Real HTTP/pgwire platform permissions; no policy or database mocks.
use super::{Ctx, expect_status, request_json, require, rows_eq, sql};
use crate::client::{Client, RemoteConnection};
use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use uuid::Uuid;

struct Principal {
    name: String,
    credential: String,
    token: String,
    compute: Value,
}
async fn principal(admin: &Client, tag: &str) -> Result<Principal> {
    let name = format!("rbac-{tag}-{}", Uuid::new_v4().simple());
    let issued = request_json(
        admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":name})),
    )
    .await?;
    let policy = request_json(
        admin,
        Method::GET,
        &format!("/v1/principals/{name}/policy"),
        None,
    )
    .await?;
    Ok(Principal {
        name,
        credential: issued["credential_id"]
            .as_str()
            .context("credential ID")?
            .into(),
        token: issued["token"].as_str().context("token")?.into(),
        compute: policy["compute"].clone(),
    })
}
async fn policy(admin: &Client, p: &Principal, role: &str, permissions: Value) -> Result<()> {
    request_json(
        admin,
        Method::PUT,
        &format!("/v1/principals/{}/policy", p.name),
        Some(
            &json!({"roles":[role],"permissions":permissions,"compute":p.compute,"disabled":false}),
        ),
    )
    .await?;
    Ok(())
}
async fn open(ctx: &Ctx, p: &Principal) -> Result<RemoteConnection> {
    Ok(RemoteConnection::open(&ctx.url, &p.token, "rbac", None, None, 30).await?)
}
fn hidden(value: &Value, sentinel: &str) -> Result<()> {
    require(
        !value.to_string().contains(sentinel),
        "metadata-only projection exposed a SQL/context sentinel",
    )?;
    for key in ["sql", "context", "error", "profile"] {
        require(
            value.get(key).is_none_or(Value::is_null),
            format!("metadata projection exposed {key}"),
        )?;
    }
    Ok(())
}
async fn wait_state(admin: &Client, id: &str, state: &str) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(row) = request_json(admin, Method::GET, &format!("/v1/requests/{id}"), None).await
        {
            if row["state"] == state {
                return Ok(());
            }
            if row["state"] == "terminal" {
                anyhow::bail!(
                    "request terminalized before expected {state}: {}",
                    row["status"]
                );
            }
        }
        anyhow::ensure!(Instant::now() < until, "request did not enter {state}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn metadata_matrix(
    ctx: &Ctx,
    admin: &Client,
    a: &Principal,
    b: &Principal,
    viewer: &Principal,
    operator: &Principal,
) -> Result<()> {
    let mut session = open(ctx, a).await?;
    let sentinel = format!("private_literal_{}", Uuid::new_v4().simple());
    session.set_context_field("private_payload", json!(sentinel));
    let (id, _) = session
        .execute_tracked(&format!("SELECT '{sentinel}' AS private_value"), vec![])
        .await?;
    let own = Client::new(&ctx.url, &a.token)?;
    let other = Client::new(&ctx.url, &b.token)?;
    let observer = Client::new(&ctx.url, &viewer.token)?;
    let path = format!("/v1/queries/{id}");
    require(
        request_json(&own, Method::GET, &path, None).await?["sql"]
            .as_str()
            .is_some_and(|v| v.contains(&sentinel)),
        "analyst lost own SQL visibility",
    )?;
    expect_status(request_json(&other, Method::GET, &path, None).await, 404)?;
    expect_status(
        request_json(&other, Method::GET, &format!("/v1/requests/{id}"), None).await,
        404,
    )?;
    policy(
        admin,
        viewer,
        "viewer",
        json!([{"action":"history.read","scope":"all"}]),
    )
    .await?;
    hidden(
        &request_json(&observer, Method::GET, &path, None).await?,
        &sentinel,
    )?;
    expect_status(
        request_json(
            &observer,
            Method::GET,
            &format!("/v1/queries?model_id={sentinel}"),
            None,
        )
        .await,
        403,
    )?;
    policy(
        admin,
        viewer,
        "viewer",
        json!([{"action":"history.read","scope":"all"},{"action":"profile.read","scope":"all"}]),
    )
    .await?;
    hidden(
        &request_json(&observer, Method::GET, &path, None).await?,
        &sentinel,
    )?;
    policy(
        admin,
        viewer,
        "viewer",
        json!([{"action":"history.read","scope":"all"},{"action":"sql.read","scope":"all"}]),
    )
    .await?;
    let sql_only = request_json(&observer, Method::GET, &path, None).await?;
    require(
        sql_only["sql"]
            .as_str()
            .is_some_and(|v| v.contains(&sentinel)),
        "explicit SQL grant was ignored",
    )?;
    require(
        sql_only.get("profile").is_none_or(Value::is_null),
        "SQL grant implicitly exposed full profile",
    )?;
    let op = Client::new(&ctx.url, &operator.token)?;
    policy(admin, operator, "operator", json!([])).await?;
    hidden(
        &request_json(&op, Method::GET, &path, None).await?,
        &sentinel,
    )?;
    expect_status(
        request_json(&op, Method::GET, "/v1/credentials", None).await,
        403,
    )?;
    expect_status(request_json(&op, Method::GET, "/v1/audit", None).await, 403)?;
    expect_status(
        request_json(&observer, Method::POST, "/v1/sessions", Some(&json!({}))).await,
        403,
    )?;
    expect_status(
        request_json(&op, Method::POST, "/v1/sessions", Some(&json!({}))).await,
        403,
    )?;
    expect_status(
        request_json(
            &op,
            Method::POST,
            "/v1/sessions",
            Some(&json!({"context":{"operation":"lake_maintenance"}})),
        )
        .await,
        403,
    )?;
    op.request(
        Method::GET,
        "/v1/lake/snapshots",
        None,
        Duration::from_secs(30),
    )
    .await?;
    op.request(
        Method::POST,
        "/v1/lake/maintenance",
        Some(&json!({"compact":true})),
        Duration::from_secs(75),
    )
    .await?;

    // An all-history observer cannot claim or retry another owner's durable ID.
    expect_status(
        request_json(
            &observer,
            Method::POST,
            &format!("{}/queries", session.path()),
            Some(&json!({"request_id":id,"sql":"SELECT 1"})),
        )
        .await,
        403,
    )?;
    request_json(&op, Method::DELETE, &session.path(), None).await?;
    // Administrative close succeeds without granting execution as the owner.
    println!(
        "PASS: own/all metadata and SQL/profile grants are independent; operator closes without SQL authority"
    );
    Ok(())
}

async fn compute_and_spoofing(ctx: &Ctx, admin: &Client, a: &Principal) -> Result<()> {
    let client = Client::new(&ctx.url, &a.token)?;
    expect_status(
        request_json(
            &client,
            Method::POST,
            "/v1/sessions",
            Some(&json!({"compute":{"mode":"isolated","cpu":1,"memory":"1Gi","disk":"2Gi"}})),
        )
        .await,
        403,
    )?;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/credentials", ctx.url.trim_end_matches('/')))
        .bearer_auth(&a.token)
        .header(crate::credentials::PRINCIPAL_HEADER, "bootstrap-admin")
        .header(crate::credentials::ACTOR_KIND_HEADER, "bootstrap")
        .json(&json!({"principal":format!("forged-{}",Uuid::new_v4().simple()),"roles":["admin"]}))
        .send()
        .await?;
    // Unknown body fields may reject the input before the permission check; neither creates a credential.
    require(
        matches!(response.status().as_u16(), 400 | 403 | 422),
        "forged actor header elevated a named credential",
    )?;
    let response = reqwest::Client::new()
        .get(format!("{}/v1/credentials", ctx.url.trim_end_matches('/')))
        .bearer_auth(&a.token)
        .header(crate::credentials::PRINCIPAL_HEADER, "bootstrap-admin")
        .header(crate::credentials::ACTOR_KIND_HEADER, "bootstrap")
        .send()
        .await?;
    require(
        response.status().as_u16() == 403,
        "forged actor-kind header exposed credentials",
    )?;
    let fleet = request_json(admin, Method::GET, "/v1/workers", None).await?;
    let endpoint = fleet["workers"]
        .as_array()
        .and_then(|rows| rows.iter().find_map(|r| r["endpoint"].as_str()))
        .context("worker endpoint")?;
    let public = Client::new(endpoint, &ctx.token)?;
    expect_status(
        request_json(&public, Method::POST, "/v1/sessions", Some(&json!({}))).await,
        401,
    )?;
    println!(
        "PASS: compute grants deny isolation before provisioning; public tokens and spoofed actor headers do not authorize worker/admin access"
    );
    Ok(())
}

async fn queued_revocation(ctx: &Ctx, admin: &Client, a: &Principal) -> Result<()> {
    let mut observer = ctx.open().await?;
    let table = format!("lake.main.rbac_queue_{}", Uuid::new_v4().simple());
    sql(&observer, &format!("CREATE TABLE {table}(value INTEGER)")).await?;
    let mut victim = open(ctx, a).await?;
    let fleet = request_json(admin, Method::GET, "/v1/workers", None).await?;
    let worker = &victim.placement["worker_id"];
    let endpoint = fleet["workers"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| &r["worker_id"] == worker))
        .and_then(|r| r["endpoint"].as_str())
        .context("victim worker endpoint")?;
    let mut blocker = RemoteConnection::open(
        endpoint,
        &ctx.internal_test_token()?,
        "rbac-blocker",
        None,
        None,
        20,
    )
    .await?;
    let active_id = Uuid::new_v4().to_string();
    let queued_id = Uuid::new_v4().to_string();
    let active_client = blocker.client().clone();
    let active_path = format!("{}/queries", blocker.path());
    let active_body = json!({"request_id":active_id,"sql":"SELECT sum(range % 7) FROM range(5000000000)","timeout_seconds":20});
    let active = tokio::spawn(async move {
        active_client
            .request(
                Method::POST,
                &active_path,
                Some(&active_body),
                Duration::from_secs(30),
            )
            .await
    });
    wait_state(admin, &active_id, "executing").await?;
    let pending_client = victim.client().clone();
    let pending_path = format!("{}/queries", victim.path());
    let pending_body = json!({"request_id":queued_id,"sql":format!("INSERT INTO {table} VALUES (1)"),"queue_timeout_seconds":30});
    let pending = tokio::spawn(async move {
        pending_client
            .request(
                Method::POST,
                &pending_path,
                Some(&pending_body),
                Duration::from_secs(40),
            )
            .await
    });
    wait_state(admin, &queued_id, "queued").await?;
    policy(admin, a, "viewer", json!([])).await?;
    request_json(
        admin,
        Method::POST,
        &format!("/v1/requests/{active_id}/cancel"),
        None,
    )
    .await?;
    let _ = active.await?;
    let denied = pending
        .await?
        .err()
        .context("queued statement executed after execute grant was removed")?;
    require(
        matches!(denied.status, Some(401 | 403 | 410)),
        format!("unexpected queued permission refusal: {denied}"),
    )?;
    let retained = request_json(
        admin,
        Method::GET,
        &format!("/v1/requests/{queued_id}"),
        None,
    )
    .await?;
    require(
        retained["outcome"] == "not_executed",
        "queue permission refusal lost no-execution outcome",
    )?;
    require(
        rows_eq(
            &sql(&observer, &format!("SELECT count(*) FROM {table}")).await?,
            &[&["0"]],
        ),
        "revoked queued mutation changed data",
    )?;
    policy(admin, a, "analyst", json!([])).await?;
    let _ = victim.close().await;
    blocker.close().await?;
    sql(&observer, &format!("DROP TABLE {table}")).await?;
    observer.close().await?;
    println!(
        "PASS: accepted queued SQL rechecks current grants before engine entry and preserves not-executed outcome"
    );
    Ok(())
}

async fn pgwire_continuity(ctx: &Ctx, admin: &Client, a: &Principal) -> Result<()> {
    let host = reqwest::Url::parse(&ctx.url)?
        .host_str()
        .context("gateway host")?
        .to_owned();
    let token = a.token.clone();
    let mut pg = tokio::task::spawn_blocking(move || {
        postgres::Config::new()
            .host(&host)
            .port(5432)
            .user("bootstrap-admin")
            .dbname("lake")
            .password(token)
            .connect_timeout(Duration::from_secs(5))
            .connect(postgres::NoTls)
    })
    .await??;
    pg = tokio::task::spawn_blocking(move || {
        pg.simple_query("SELECT 1")?;
        Ok::<_, postgres::Error>(pg)
    })
    .await??;
    policy(admin, a, "viewer", json!([])).await?;
    let result =
        tokio::task::spawn_blocking(move || pg.simple_query("SELECT 2 /* permission removed */"))
            .await?;
    let denied = result
        .err()
        .context("established pgwire retained revoked execute grant")?;
    require(
        denied.is_closed()
            || denied
                .code()
                .is_some_and(|code| matches!(code.code(), "42501" | "28P01" | "28000" | "57P01")),
        format!("unexpected pgwire permission error: {denied}"),
    )?;
    policy(admin, a, "analyst", json!([])).await?;
    println!("PASS: established pgwire checks live grants and ignores advisory startup username");
    Ok(())
}

async fn idle_permission_cleanup(ctx: &Ctx, admin: &Client, a: &Principal) -> Result<()> {
    let baseline =
        request_json(admin, Method::GET, "/v1/pools", None).await?["gateway_open_sessions"]
            .as_u64()
            .context("gateway route count")?;
    let _idle = open(ctx, a).await?;
    policy(admin, a, "viewer", json!([])).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current =
            request_json(admin, Method::GET, "/v1/pools", None).await?["gateway_open_sessions"]
                .as_u64()
                .context("gateway route count")?;
        if current <= baseline {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "idle grant revocation did not reclaim gateway capacity after actual worker close"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    policy(admin, a, "analyst", json!([])).await?;
    println!(
        "PASS: idle permission revocation reclaims worker and gateway capacity without client activity"
    );
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let a = principal(&admin, "analyst-a").await?;
    let b = principal(&admin, "analyst-b").await?;
    let viewer = principal(&admin, "viewer").await?;
    let operator = principal(&admin, "operator").await?;
    let result = async {
        metadata_matrix(ctx, &admin, &a, &b, &viewer, &operator).await?;
        compute_and_spoofing(ctx, &admin, &a).await?;
        queued_revocation(ctx, &admin, &a).await?;
        pgwire_continuity(ctx, &admin, &a).await?;
        idle_permission_cleanup(ctx, &admin, &a).await?;
        let events = request_json(&admin, Method::GET, "/v1/audit?limit=500", None).await?;
        require(
            events["events"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["decision"] == "denied" && r["actor_principal"] == a.name)
            }),
            "authenticated authorization denial missing from audit",
        )?;
        let page = request_json(&admin, Method::GET, "/v1/principals?limit=1", None).await?;
        let next = page["next_after"]
            .as_str()
            .context("principal pagination cursor")?;
        let second = request_json(
            &admin,
            Method::GET,
            &format!("/v1/principals?limit=1&after={next}"),
            None,
        )
        .await?;
        require(
            page["principals"][0]["principal_id"] != second["principals"][0]["principal_id"],
            "principal cursor repeated a row",
        )?;
        Ok(())
    }
    .await;
    for p in [&a, &b, &viewer, &operator] {
        let _ = request_json(
            &admin,
            Method::DELETE,
            &format!("/v1/credentials/{}", p.credential),
            None,
        )
        .await;
    }
    result
}
