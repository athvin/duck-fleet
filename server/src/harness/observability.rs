//! History availability, pagination, profile identity, and auth on the read
//! endpoints. Ported from `tests/observability.py`.

use reqwest::Method;
use serde_json::json;

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
    println!("PASS: independent history, cursor pagination, filters, auth, and profile identity");
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
