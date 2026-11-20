//! Concurrent DuckLake write correctness. Ported from `tests/concurrency.py`.
//! Statements serialize inside a worker, so overlapping transactions are built by
//! interleaving requests across two sessions rather than racing threads.

use serde_json::{Value, json};

use super::{Ctx, require, rows_eq, sql};
use crate::client::{RemoteConnection, RemoteError};

fn small() -> Value {
    json!({"cpu": 1, "memory": "1Gi"})
}
fn medium() -> Value {
    json!({"cpu": 2, "memory": "2Gi"})
}

/// Asserts a statement lost a commit race and said so usefully.
async fn expect_conflict(
    connection: &RemoteConnection,
    statement: &str,
    context: &str,
) -> anyhow::Result<RemoteError> {
    match connection.execute(statement, Vec::new()).await {
        Err(error) => {
            require(
                error.status == Some(409),
                format!("{context}: HTTP {:?}", error.status),
            )?;
            require(
                error.failure_kind.as_deref() == Some("transaction_conflict"),
                format!("{context}: kind {:?}", error.failure_kind),
            )?;
            require(
                error.retry_safe,
                format!("{context}: a discarded transaction must be retry-safe"),
            )?;
            Ok(error)
        }
        Ok(_) => anyhow::bail!("{context}: expected a commit conflict"),
    }
}

async fn reset(ctx: &Ctx, rows: Option<&str>) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("concurrency-admin", Some(small())).await?;
    sql(&connection, "create schema if not exists concurrency").await?;
    sql(&connection, "drop table if exists concurrency.target").await?;
    sql(
        &connection,
        "create table concurrency.target (id integer, amount integer)",
    )
    .await?;
    if let Some(rows) = rows {
        sql(
            &connection,
            &format!("insert into concurrency.target values {rows}"),
        )
        .await?;
    }
    connection.close().await?;
    Ok(())
}

async fn read_target(ctx: &Ctx) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let mut connection = ctx.open_named("concurrency-admin", Some(small())).await?;
    let rows = sql(
        &connection,
        "select id,amount from concurrency.target order by id",
    )
    .await?;
    connection.close().await?;
    Ok(rows)
}

async fn appends_never_conflict(ctx: &Ctx) -> anyhow::Result<()> {
    reset(ctx, None).await?;
    let mut first = ctx.open_named("append-first", Some(small())).await?;
    let mut second = ctx.open_named("append-second", Some(medium())).await?;
    sql(&first, "begin").await?;
    sql(&second, "begin").await?;
    sql(&first, "insert into concurrency.target values (1,10)").await?;
    sql(&second, "insert into concurrency.target values (2,20)").await?;
    sql(&first, "commit").await?;
    sql(&second, "commit").await?;
    let _ = first.close().await;
    let _ = second.close().await;
    let rows = read_target(ctx).await?;
    require(
        rows_eq(&rows, &[&["1", "10"], &["2", "20"]]),
        format!("appends lost rows: {rows:?}"),
    )?;
    println!("PASS: concurrent appends to one table both commit and keep every row");
    Ok(())
}

async fn delete_insert_conflicts_atomically(ctx: &Ctx) -> anyhow::Result<()> {
    reset(ctx, Some("(1,10),(2,20),(3,30)")).await?;
    let mut first = ctx.open_named("di-first", Some(small())).await?;
    let mut second = ctx.open_named("di-second", Some(medium())).await?;
    sql(&first, "begin").await?;
    sql(&second, "begin").await?;
    sql(&first, "delete from concurrency.target where id = 1").await?;
    sql(&first, "insert into concurrency.target values (1,11)").await?;
    sql(&second, "delete from concurrency.target where id = 2").await?;
    sql(&second, "insert into concurrency.target values (2,22)").await?;
    sql(&first, "commit").await?;
    expect_conflict(&second, "commit", "delete+insert on one table").await?;
    require(
        rows_eq(&sql(&second, "select 1").await?, &[&["1"]]),
        "a lost commit poisoned the session",
    )?;
    let _ = first.close().await;
    let _ = second.close().await;
    let rows = read_target(ctx).await?;
    require(
        rows_eq(&rows, &[&["1", "11"], &["2", "20"], &["3", "30"]]),
        format!("losing transaction was not discarded whole: {rows:?}"),
    )?;
    println!("PASS: a losing delete+insert is discarded whole");
    Ok(())
}

async fn ordinary_sql_errors_stay_unsafe(ctx: &Ctx) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("concurrency-admin", Some(small())).await?;
    for (statement, kind) in [
        ("select * from concurrency.does_not_exist", "sql_error"),
        ("select missing_column", "sql_error"),
    ] {
        match connection.execute(statement, Vec::new()).await {
            Err(error) => {
                require(
                    error.status == Some(400),
                    format!("{statement}: HTTP {:?}", error.status),
                )?;
                require(
                    error.failure_kind.as_deref() == Some(kind),
                    format!("{statement}: {:?}", error.failure_kind),
                )?;
                require(
                    !error.retry_safe,
                    format!("{statement}: must not be retry-safe"),
                )?;
            }
            Ok(_) => anyhow::bail!("expected an error from {statement}"),
        }
    }
    connection.close().await?;
    println!("PASS: SQL errors are classified and not retry-safe");
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> anyhow::Result<()> {
    appends_never_conflict(ctx).await?;
    delete_insert_conflicts_atomically(ctx).await?;
    ordinary_sql_errors_stay_unsafe(ctx).await?;
    let mut connection = ctx.open_named("concurrency-admin", Some(small())).await?;
    sql(&connection, "drop table if exists concurrency.target").await?;
    connection.close().await?;
    println!("PASS: concurrent-write cases match the documented DuckLake semantics");
    Ok(())
}
