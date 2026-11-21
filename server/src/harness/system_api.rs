//! End-to-end constrained metadata SQL over HTTP and the reserved pgwire database.
use super::{Ctx, expect_status, request_json, require};
use crate::client::{Client, RemoteConnection, RemoteError};
use anyhow::{Context, Result};
use reqwest::Method;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

struct Principal {
    name: String,
    id: String,
    token: String,
}
async fn issue(admin: &Client, tag: &str) -> Result<Principal> {
    let name = format!("system-{tag}-{}", Uuid::new_v4().simple());
    let issued = request_json(
        admin,
        Method::POST,
        "/v1/credentials",
        Some(&json!({"principal":name})),
    )
    .await?;
    Ok(Principal {
        name,
        id: issued["credential_id"]
            .as_str()
            .context("credential id")?
            .into(),
        token: issued["token"].as_str().context("credential token")?.into(),
    })
}
async fn policy(admin: &Client, p: &Principal, role: &str, permissions: Value) -> Result<()> {
    request_json(
        admin,
        Method::PUT,
        &format!("/v1/principals/{}/policy", p.name),
        Some(&json!({"roles":[role],"permissions":permissions,"compute":[],"disabled":false})),
    )
    .await?;
    Ok(())
}
async fn query(client: &Client, sql: &str) -> Result<Value, RemoteError> {
    request_json(
        client,
        Method::POST,
        "/v1/system/query",
        Some(&json!({"sql":sql})),
    )
    .await
}
fn rows(value: &Value) -> Result<&Vec<Value>> {
    value["rows"].as_array().context("system rows")
}
fn cell(value: &Value, row: usize, column: usize) -> Result<&Value> {
    rows(value)?
        .get(row)
        .and_then(|r| r.as_array())
        .and_then(|r| r.get(column))
        .context("system cell")
}
fn id_list(ids: &[String]) -> Result<String> {
    ids.iter()
        .map(|id| {
            Uuid::parse_str(id)?;
            Ok(format!("'{id}'"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|ids| ids.join(","))
}
fn strings(value: &Value) -> Result<Vec<Vec<Option<String>>>> {
    rows(value)?
        .iter()
        .map(|row| {
            row.as_array().context("system row").map(|row| {
                row.iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else if let Some(s) = v.as_str() {
                            Some(s.into())
                        } else if let Some(value) = v.as_bool() {
                            Some(if value { "t" } else { "f" }.into())
                        } else {
                            Some(v.to_string())
                        }
                    })
                    .collect()
            })
        })
        .collect()
}
struct Pg {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<()>,
}
impl Drop for Pg {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
impl Pg {
    async fn open(ctx: &Ctx, token: &str) -> Result<Self> {
        let url = reqwest::Url::parse(&ctx.url)?;
        let host = url.host_str().context("gateway host")?;
        let (client, connection) = tokio::time::timeout(
            Duration::from_secs(8),
            tokio_postgres::Config::new()
                .host(host)
                .port(5432)
                .user("bootstrap-admin")
                .dbname("duckfleet_system")
                .password(token)
                .connect_timeout(Duration::from_secs(5))
                .connect(tokio_postgres::NoTls),
        )
        .await??;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { client, driver })
    }
    async fn query(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let messages =
            tokio::time::timeout(Duration::from_secs(10), self.client.simple_query(sql)).await??;
        Ok(messages
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
                }
                _ => None,
            })
            .collect())
    }
}
async fn same_pg(pg: &Pg, client: &Client, sql: &str) -> Result<Value> {
    let value = query(client, sql).await?;
    require(
        pg.query(sql).await? == strings(&value)?,
        "HTTP/system pgwire projections diverged",
    )?;
    Ok(value)
}

async fn seed(
    ctx: &Ctx,
    p: &Principal,
    marker: &str,
    fail: bool,
) -> Result<(String, Option<String>)> {
    let mut connection = RemoteConnection::open(
        &ctx.url,
        &p.token,
        "system-fixture",
        None,
        Some(json!({"private_marker":marker,"model_id":marker})),
        30,
    )
    .await?;
    let result = async {
        let (id, _) = connection
            .execute_tracked(&format!("SELECT '{marker}' AS protected_value"), vec![])
            .await?;
        let failed = if fail {
            let error = connection
                .execute_tracked(
                    "SELECT * FROM duckfleet_system_fixture_missing_relation",
                    vec![],
                )
                .await
                .err()
                .context("fixture SQL unexpectedly succeeded")?;
            require(
                error.failure_kind.as_deref() == Some("sql_error"),
                "fixture did not fail as SQL",
            )?;
            Some(
                error
                    .request_id
                    .as_deref()
                    .context("failed request id")?
                    .to_owned(),
            )
        } else {
            None
        };
        Ok((id, failed))
    }
    .await;
    connection.close().await?;
    result
}
async fn aggregate_examples(admin: &Client, ids: &[String]) -> Result<()> {
    let list = id_list(ids)?;
    let histories=query(admin,&format!("SELECT request_id,engine_elapsed_ms,query_fingerprint FROM duckfleet_system.query_history WHERE request_id IN ({list}) ORDER BY request_id")).await?;
    require(
        rows(&histories)?.len() == ids.len(),
        "seed histories missing",
    )?;
    for row in rows(&histories)? {
        let id = row[0].as_str().context("history id")?;
        let http = request_json(admin, Method::GET, &format!("/v1/queries/{id}"), None).await?;
        require(
            row[1].as_str().and_then(|s| s.parse::<i64>().ok()) == http["elapsed_ms"].as_i64(),
            "system elapsed duration differs from HTTP",
        )?;
    }
    let latency=query(admin,&format!("SELECT query_fingerprint,query_fingerprint_version,COUNT(*) AS runs,AVG(engine_elapsed_ms) AS average_ms FROM duckfleet_system.query_history WHERE request_id IN ({list}) GROUP BY query_fingerprint,query_fingerprint_version ORDER BY query_fingerprint")).await?;
    let counted = rows(&latency)?
        .iter()
        .map(|r| r[2].as_str().unwrap_or("0").parse::<i64>())
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<i64>();
    require(
        counted == ids.len() as i64,
        "fingerprint aggregate dropped requests",
    )?;
    require(
        latency["columns"][3]["type"] == "DECIMAL(38,6)",
        "AVG duration lost exact decimal descriptor",
    )?;
    let failed=query(admin,&format!("SELECT failure_kind,outcome,COUNT(*) AS attempts FROM duckfleet_system.query_attempts WHERE request_id IN ({list}) AND status='failed' GROUP BY failure_kind,outcome")).await?;
    require(
        rows(&failed)?.len() == 1
            && cell(&failed, 0, 0)? == "sql_error"
            && cell(&failed, 0, 2)? == "1",
        "failed attempt example differs from fixture",
    )?;
    let usage=query(admin,&format!("SELECT owner_principal_name,model_id,rate_card_version,SUM(amount_micro_usd) AS total_micro_usd FROM duckfleet_system.usage WHERE request_id IN ({list}) GROUP BY owner_principal_name,model_id,rate_card_version ORDER BY owner_principal_name")).await?;
    let actual = rows(&usage)?
        .iter()
        .map(|r| {
            r[3].as_str()
                .context("exact cost")?
                .parse::<i128>()
                .context("integer cost")
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<i128>();
    let mut expected = 0_i128;
    for id in ids {
        let events = request_json(
            admin,
            Method::GET,
            &format!("/v1/usage?execution_id={id}&limit=100"),
            None,
        )
        .await?;
        expected += events["events"]
            .as_array()
            .context("HTTP usage events")?
            .iter()
            .map(|e| i128::from(e["amount_micro_usd"].as_i64().unwrap()))
            .sum::<i128>();
    }
    require(
        actual == expected && usage["columns"][3]["type"] == "DECIMAL(38,0)",
        "system usage aggregate differs from saved HTTP accounting",
    )?;
    let joined=query(admin,&format!("SELECT h.request_id,h.status,h.engine_elapsed_ms,u.amount_micro_usd FROM duckfleet_system.query_history h LEFT JOIN duckfleet_system.usage u ON h.request_id=u.request_id AND u.request_id IN ({list}) WHERE h.request_id IN ({list}) ORDER BY h.request_id")).await?;
    require(
        rows(&joined)?.len() >= ids.len(),
        "history/usage LEFT JOIN dropped history rows",
    )?;
    println!(
        "PASS: bounded fingerprint latency, failed attempts, model/principal usage and LEFT JOIN examples match HTTP records"
    );
    Ok(())
}
async fn totals(admin: &Client) -> Result<Value> {
    let now = chrono::Utc::now();
    let from =
        (now - chrono::Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let until =
        (now + chrono::Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let mut counts = serde_json::Map::new();
    for relation in ["query_history", "query_attempts", "usage"] {
        let value=request_json(admin,Method::POST,"/v1/system/query",Some(&json!({"sql":format!("SELECT COUNT(*) AS total FROM duckfleet_system.{relation}"),"bounds":{relation:{"from":from,"until":until}}}))).await?;
        counts.insert(relation.into(), cell(&value, 0, 0)?.clone());
    }
    counts.insert(
        "sessions".into(),
        request_json(admin, Method::GET, "/v1/pools", None).await?["gateway_open_sessions"].clone(),
    );
    Ok(Value::Object(counts))
}

async fn permission_matrix(
    ctx: &Ctx,
    admin: &Client,
    a: &Principal,
    b: &Principal,
    viewer: &Principal,
    ids: &[String],
    markers: &[String],
) -> Result<()> {
    let list = id_list(&ids[..2])?;
    let sql = format!(
        "SELECT request_id,status,sql_text,application_context,profile_json FROM duckfleet_system.query_history WHERE request_id IN ({list}) ORDER BY request_id"
    );
    policy(admin, a, "viewer", json!([])).await?;
    let own = Client::new(&ctx.url, &a.token)?;
    let other = Client::new(&ctx.url, &b.token)?;
    let observer = Client::new(&ctx.url, &viewer.token)?;
    policy(admin, viewer, "viewer", json!([])).await?;
    let pg = Pg::open(ctx, &a.token).await?;
    let own_rows = same_pg(&pg, &own, &sql).await?;
    require(
        rows(&own_rows)?.len() == 1 && cell(&own_rows, 0, 0)? == &ids[0],
        "viewer own rows were not owner-scoped",
    )?;
    require(
        (2..5).all(|i| own_rows["rows"][0][i].is_null()),
        "metadata-only viewer saw sensitive values",
    )?;
    require(
        rows(&query(&observer, &sql).await?)?.is_empty(),
        "unrelated viewer saw another owner's rows",
    )?;
    expect_status(
        request_json(
            &observer,
            Method::GET,
            &format!("/v1/queries/{}", ids[0]),
            None,
        )
        .await,
        404,
    )?;
    expect_status(
        request_json(&own, Method::POST, "/v1/sessions", Some(&json!({}))).await,
        403,
    )?;
    policy(
        admin,
        a,
        "viewer",
        json!([{"action":"history.read","scope":"all"},{"action":"profile.read","scope":"all"}]),
    )
    .await?;
    let profile_only = same_pg(&pg, &own, &sql).await?;
    require(
        rows(&profile_only)?.len() == 2
            && rows(&profile_only)?
                .iter()
                .all(|row| (2..5).all(|column| row[column].is_null())),
        "profile grant without SQL permission exposed SQL-bearing fields",
    )?;
    // Same token, now all rows but own SQL. Current policy must apply to the
    // already-established system connection without granting arbitrary SQL.
    policy(admin,a,"viewer",json!([{"action":"history.read","scope":"all"},{"action":"sql.read","scope":"own"},{"action":"profile.read","scope":"all"}])).await?;
    let mixed = same_pg(&pg, &own, &sql).await?;
    require(
        rows(&mixed)?.len() == 2,
        "all-history grant did not expose metadata",
    )?;
    for row in rows(&mixed)? {
        let id = row[0].as_str().context("request id")?;
        let http = request_json(&own, Method::GET, &format!("/v1/queries/{id}"), None).await?;
        if id == ids[0] {
            require(
                row[2].as_str().is_some_and(|s| s.contains(&markers[0])),
                "own SQL was masked",
            )?;
            require(row[2] == http["sql"], "HTTP/system SQL projection differs")?;
            require(
                serde_json::from_str::<Value>(row[3].as_str().context("context text")?)?
                    == http["context"],
                "HTTP/system context projection differs",
            )?;
            require(
                row[4].is_string() && !http["profile"].is_null(),
                "both SQL/profile grants failed to expose own profile",
            )?;
        } else {
            require(
                (2..5).all(|i| row[i].is_null()),
                "all-history/own-SQL exposed other owner's sensitive values",
            )?;
            require(
                http.get("sql").is_none_or(Value::is_null),
                "HTTP own-SQL masking differs",
            )?;
        }
    }
    expect_status(query(&own,&format!("SELECT request_id FROM query_history WHERE request_id IN ({list}) AND sql_text IS NOT NULL")).await,403)?;
    expect_status(query(&own,&format!("SELECT request_id FROM query_history WHERE request_id IN ({list}) ORDER BY query_fingerprint")).await,403)?;
    policy(admin, viewer, "operator", json!([])).await?;
    let operator = query(&observer, &sql).await?;
    require(
        rows(&operator)?.len() == 2
            && !operator.to_string().contains(&markers[0])
            && !operator.to_string().contains(&markers[1]),
        "operator metadata exposed SQL/context/profile",
    )?;
    // Supplying internal identity headers on the public system route has no
    // authority. Restore own-only viewer so the correct result is empty.
    policy(admin, viewer, "viewer", json!([])).await?;
    let forged = reqwest::Client::new()
        .post(format!("{}/v1/system/query", ctx.url.trim_end_matches('/')))
        .bearer_auth(&viewer.token)
        .header(crate::credentials::PRINCIPAL_HEADER, &a.name)
        .header(crate::credentials::CREDENTIAL_HEADER, &a.id)
        .header(crate::credentials::ACTOR_KIND_HEADER, "bootstrap")
        .json(&json!({"sql":sql}))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    require(
        forged.status().is_success(),
        "forged header probe failed unexpectedly",
    )?;
    require(
        rows(&forged.json::<Value>().await?)?.is_empty(),
        "caller headers substituted authenticated identity",
    )?;
    expect_status(
        request_json(
            &observer,
            Method::POST,
            "/v1/system/query",
            Some(&json!({"sql":"SHOW TABLES","principal":a.name})),
        )
        .await,
        400,
    )?;
    // B still sees only its own rows; neither relation scope nor current names
    // permit claiming another principal's history.
    require(
        rows(&query(&other, &sql).await?)?.len() == 1,
        "other analyst row scope changed",
    )?;
    request_json(
        admin,
        Method::DELETE,
        &format!("/v1/credentials/{}", a.id),
        None,
    )
    .await?;
    let refusal = pg
        .query("SHOW TABLES")
        .await
        .err()
        .context("revoked established system connection executed")?;
    require(
        refusal
            .downcast_ref::<tokio_postgres::Error>()
            .is_some_and(|e| {
                e.is_closed()
                    || e.code()
                        .is_some_and(|s| matches!(s.code(), "28P01" | "28000"))
            }),
        "system revocation did not return authentication failure",
    )?;
    println!(
        "PASS: metadata-only HTTP/pgwire entry, own/all SQL/profile scopes, forged identity and established revocation"
    );
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> Result<()> {
    let admin = ctx.client()?;
    let a = issue(&admin, "a").await?;
    let b = issue(&admin, "b").await?;
    let viewer = issue(&admin, "viewer").await?;
    let result=async {
        let markers=[format!("system_private_a_{}",Uuid::new_v4().simple()),format!("system_private_b_{}",Uuid::new_v4().simple())];
        let (first,failed)=seed(ctx,&a,&markers[0],true).await?;
        let (second,_)=seed(ctx,&b,&markers[1],false).await?;
        let ids=vec![first,second,failed.context("failed fixture id")?];
        aggregate_examples(&admin,&ids).await?;
        let before=totals(&admin).await?;
        let discovery=request_json(&admin,Method::GET,"/v1/system/schema",None).await?;
        require(discovery["system_contract_version"]==1 && !rows(&discovery)?.is_empty(),"system schema contract missing")?;
        let pg=Pg::open(ctx,&ctx.token).await?;
        same_pg(&pg,&admin,"SHOW TABLES").await?;
        same_pg(&pg,&admin,"SHOW TABLES FROM duckfleet_system").await?;
        for relation in ["query_history","query_attempts","usage"] {
            same_pg(&pg,&admin,&format!("DESCRIBE duckfleet_system.{relation}")).await?;
        }
        let utc=query(&admin,&format!("SELECT accepted_at FROM query_attempts WHERE request_id='{}'",ids[0])).await?;
        let wire=pg.query(&format!("SELECT accepted_at FROM query_attempts WHERE request_id='{}'",ids[0])).await?;
        let http_at=chrono::DateTime::parse_from_rfc3339(cell(&utc,0,0)?.as_str().context("UTC timestamp")?)?;
        let wire_at=chrono::DateTime::parse_from_str(wire[0][0].as_deref().context("pgwire UTC timestamp")?,"%Y-%m-%d %H:%M:%S%.f%:z")?;
        require(http_at==wire_at,"HTTP/pgwire UTC instants differ")?;
        same_pg(&pg,&admin,&format!("SELECT SUM(amount_micro_usd) AS amount FROM usage WHERE request_id IN ({})",id_list(&ids)?)).await?;
        for sql in ["SELECT token_sha256 FROM credentials","SELECT * FROM pg_catalog.pg_authid","ATTACH 'postgres:secret' AS x","SELECT read_text('/proc/self/environ')","SHOW TABLES; SELECT 1","SELECT request_id FROM query_history LIMIT 1"] {
            expect_status(query(&admin,sql).await,400)?;
        }
        expect_status(query(&Client::new(&ctx.url,"invalid-system-bearer")?,"SHOW TABLES").await,401)?;
        permission_matrix(ctx,&admin,&a,&b,&viewer,&ids,&markers).await?;
        require(totals(&admin).await?==before,"system reads created warehouse sessions, request history, or usage")?;
        println!("PASS: system v1 discovery, exact numeric/UTC transport and diagnostics without warehouse side effects");
        Ok(())
    }.await;
    for p in [&a, &b, &viewer] {
        let _ = request_json(
            &admin,
            Method::DELETE,
            &format!("/v1/credentials/{}", p.id),
            None,
        )
        .await;
    }
    result
}

pub(super) async fn workers_down(ctx: &Ctx) -> Result<()> {
    super::observability::history_without_workers(ctx).await?;
    let admin = ctx.client()?;
    let recent = request_json(&admin, Method::GET, "/v1/queries?limit=1", None).await?;
    let id = recent["queries"][0]["query_id"]
        .as_str()
        .context("retained history id")?;
    let id = Uuid::parse_str(id)?.to_string();
    request_json(&admin, Method::GET, &format!("/v1/requests/{id}"), None).await?;
    request_json(
        &admin,
        Method::GET,
        &format!("/v1/usage?execution_id={id}"),
        None,
    )
    .await?;
    let pg = Pg::open(ctx, &ctx.token).await?;
    for relation in ["query_history", "query_attempts", "usage"] {
        same_pg(&pg,&admin,&format!("SELECT COUNT(*) AS records FROM duckfleet_system.{relation} WHERE request_id='{id}'")).await?;
    }
    println!(
        "PASS: existing HTTP diagnostics and all three system SQL relations remain available with all workers stopped"
    );
    Ok(())
}
