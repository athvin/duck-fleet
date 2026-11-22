//! Concurrent DuckLake write correctness. Ported from `tests/concurrency.py`.
//! Statements serialize inside a worker, so overlapping transactions are built by
//! interleaving requests across two sessions rather than racing threads.

use super::{Ctx, require, rows_eq, sql};
use crate::client::{RemoteConnection, RemoteError};

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
    let mut connection = ctx.open_named("concurrency-admin", None).await?;
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
    let mut connection = ctx.open_named("concurrency-admin", None).await?;
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
    let mut first = ctx.open_named("append-first", None).await?;
    let mut second = ctx.open_named("append-second", None).await?;
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
    let mut first = ctx.open_named("di-first", None).await?;
    let mut second = ctx.open_named("di-second", None).await?;
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
    let mut connection = ctx.open_named("concurrency-admin", None).await?;
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

async fn failed_statements_abort_explicit_transactions(ctx: &Ctx) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("transaction-abort", None).await?;
    sql(
        &connection,
        "CREATE TEMP TABLE transaction_values(value INTEGER)",
    )
    .await?;
    for statement in [
        "SELECT * FROM",
        "SELECT * FROM qualification_missing_transaction_relation",
    ] {
        sql(&connection, "BEGIN").await?;
        sql(&connection, "INSERT INTO transaction_values VALUES (1)").await?;
        let error = match connection.execute(statement, Vec::new()).await {
            Err(error) => error,
            Ok(_) => anyhow::bail!("parser/binder error must fail"),
        };
        require(
            !error.retry_safe,
            format!("unsafe error became retryable: {error}"),
        )?;
        let blocked = match connection.execute("SELECT 1", Vec::new()).await {
            Err(error) => error,
            Ok(_) => anyhow::bail!("an aborted transaction accepted another statement"),
        };
        require(
            !blocked.retry_safe,
            format!("aborted transaction became retryable: {blocked}"),
        )?;
        sql(&connection, "ROLLBACK").await?;
        require(
            rows_eq(
                &sql(&connection, "SELECT count(*) FROM transaction_values").await?,
                &[&["0"]],
            ),
            "rollback retained a write from an aborted transaction",
        )?;
    }
    connection.close().await?;
    println!(
        "PASS: parser/binder errors abort explicit transactions and rollback discards their writes"
    );
    Ok(())
}

async fn multi_match_writes_follow_native_201_semantics(ctx: &Ctx) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("multi-match", None).await?;
    sql(
        &connection,
        "CREATE TEMP TABLE multi_target(id INTEGER, value INTEGER)",
    )
    .await?;
    sql(
        &connection,
        "CREATE TEMP TABLE multi_source(id INTEGER, value INTEGER)",
    )
    .await?;
    sql(&connection, "INSERT INTO multi_target VALUES (1, 0)").await?;
    sql(
        &connection,
        "INSERT INTO multi_source VALUES (1, 10), (1, 20)",
    )
    .await?;
    sql(
        &connection,
        "UPDATE multi_target t SET value=s.value FROM multi_source s WHERE t.id=s.id",
    )
    .await?;
    require(
        rows_eq(
            &sql(
                &connection,
                "SELECT value IN (10, 20), count(*) FROM multi_target",
            )
            .await?,
            &[&["true", "1"]],
        ),
        "multi-match UPDATE FROM changed row cardinality or selected an unknown value",
    )?;
    sql(
        &connection,
        "MERGE INTO multi_target t USING multi_source s ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value",
    )
    .await?;
    require(
        rows_eq(
            &sql(
                &connection,
                "SELECT value IN (10, 20), count(*) FROM multi_target",
            )
            .await?,
            &[&["true", "1"]],
        ),
        "multi-match MERGE changed row cardinality or selected an unknown value",
    )?;
    connection.close().await?;
    println!("PASS: multi-match UPDATE FROM and MERGE retain native 2.0.1 single-target semantics");
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> anyhow::Result<()> {
    appends_never_conflict(ctx).await?;
    delete_insert_conflicts_atomically(ctx).await?;
    ordinary_sql_errors_stay_unsafe(ctx).await?;
    failed_statements_abort_explicit_transactions(ctx).await?;
    multi_match_writes_follow_native_201_semantics(ctx).await?;
    let mut connection = ctx.open_named("concurrency-admin", None).await?;
    sql(&connection, "drop table if exists concurrency.target").await?;
    connection.close().await?;
    println!("PASS: concurrent-write cases match the documented DuckLake semantics");
    Ok(())
}
