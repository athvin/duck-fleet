use std::time::Instant;

use reqwest::Method;
use serde_json::{Value, json};

use super::{Ctx, request_json};
use crate::client::{QueryResult, RemoteError};

struct Case {
    id: &'static str,
    sql: &'static str,
    parameters: Vec<Value>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            id: "scalar_types",
            sql: "SELECT 42::BIGINT AS answer, 'duck'::VARCHAR AS label, NULL::VARCHAR AS missing",
            parameters: vec![],
        },
        Case {
            id: "decimal_parameter",
            sql: "SELECT $1::DECIMAL(20,4) AS exact, $2::VARCHAR AS region",
            parameters: vec![json!("12345678901234.5678"), json!("eu")],
        },
        Case {
            id: "aggregate",
            sql: "WITH events(region, amount) AS (VALUES ('eu', 10.50::DECIMAL(10,2)), ('us', 20.25::DECIMAL(10,2)), ('eu', 30.00::DECIMAL(10,2))) SELECT region, sum(amount) AS total FROM events GROUP BY region ORDER BY region",
            parameters: vec![],
        },
        Case {
            id: "timestamp",
            sql: "SELECT TIMESTAMPTZ '2026-01-02 03:04:05+00' AS instant, TIMESTAMP '2026-01-02 03:04:05' AS local_timestamp",
            parameters: vec![],
        },
        Case {
            id: "regex_partial",
            sql: "SELECT 'abc' ~ 'b' AS matched, 'abc' !~ 'd' AS not_matched",
            parameters: vec![],
        },
        Case {
            id: "factorial_precedence",
            sql: "SELECT 2 * 3! AS value",
            parameters: vec![],
        },
        Case {
            id: "binary_literals",
            sql: "SELECT X'41' AS blob_value, B'101' AS bit_value, typeof(X'41') AS blob_type, typeof(B'101') AS bit_type",
            parameters: vec![],
        },
        Case {
            id: "integer_division_by_zero",
            sql: "SELECT 1 // 0 AS value",
            parameters: vec![],
        },
        Case {
            id: "modulo_by_zero",
            sql: "SELECT 1 % 0 AS value",
            parameters: vec![],
        },
        Case {
            id: "single_arrow_lambda",
            sql: "SELECT list_transform([1, 2, 3], x -> x + 1) AS values",
            parameters: vec![],
        },
        Case {
            id: "python_lambda",
            sql: "SELECT list_transform([1, 2, 3], lambda x : x + 1) AS values",
            parameters: vec![],
        },
        Case {
            id: "untyped_null",
            sql: "SELECT NULL AS value, typeof(NULL) AS type_name",
            parameters: vec![],
        },
        Case {
            id: "missing_relation",
            sql: "SELECT * FROM qualification_missing_relation",
            parameters: vec![],
        },
        Case {
            id: "calendar_timezone",
            sql: "SELECT timezone('America/New_York', TIMESTAMPTZ '2026-07-01 12:00:00+00') AS local_time, date_part('isodow', DATE '2026-11-21') AS weekday",
            parameters: vec![],
        },
        Case {
            id: "collation",
            sql: "SELECT 'a' COLLATE NOCASE = 'A' AS case_insensitive, 'a' COLLATE NOACCENT = 'á' AS accent_insensitive",
            parameters: vec![],
        },
        Case {
            id: "profile_aggregate",
            sql: "SELECT sum(i)::BIGINT AS total, max(i)::BIGINT AS maximum FROM range(100000) source(i)",
            parameters: vec![],
        },
        Case {
            id: "peg_parser_quoted_identifiers",
            sql: "WITH \"MixedCase\"(value) AS (SELECT 42) SELECT value FROM \"MixedCase\"",
            parameters: vec![],
        },
        Case {
            id: "identifier_case",
            sql: "SELECT current_setting('preserve_identifier_case') AS preserved",
            parameters: vec![],
        },
        Case {
            id: "result_type_matrix",
            sql: "SELECT UUID '00000000-0000-4000-8000-000000000001' AS id, '\\x00ff'::BLOB AS bytes, B'101' AS bits, [1,NULL,3] AS items, {'name':'duck','value':7} AS record",
            parameters: vec![],
        },
        Case {
            id: "variant_result",
            sql: "SELECT {'duck': 2}::VARIANT AS value",
            parameters: vec![],
        },
    ]
}

fn success(result: QueryResult) -> Value {
    json!({
        "kind": "success",
        "columns": result.columns,
        "column_types": result.column_types,
        "rows": result.rows,
    })
}

fn failure(error: &RemoteError) -> Value {
    json!({
        "kind": "error",
        "status": error.status,
        "failure_kind": error.failure_kind,
        "retry_safe": error.retry_safe,
        "message": error.message,
    })
}

fn engine_version(value: &Value) -> Option<&str> {
    match value {
        Value::Object(map) => {
            if let Some(version) = map.get("duckdb_version").and_then(Value::as_str) {
                return Some(version);
            }
            map.values().find_map(engine_version)
        }
        Value::Array(values) => values.iter().find_map(engine_version),
        _ => None,
    }
}

pub(super) async fn security(ctx: &Ctx) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("extension-security", None).await?;
    let settings = connection.execute(
        "SELECT current_setting('allow_extension_repositories'), current_setting('pg_enable_remote_pushdown')",
        Vec::new(),
    ).await?;
    anyhow::ensure!(
        settings.rows == vec![vec![Some("forbidden".into()), Some("true".into())]],
        "unexpected extension or PostgreSQL policy: {:?}",
        settings.rows
    );
    let error = match connection
        .execute(
            "CREATE EXTENSION REPOSITORY duckfleet_forbidden",
            Vec::new(),
        )
        .await
    {
        Err(error) => error,
        Ok(_) => anyhow::bail!("custom extension repository creation succeeded"),
    };
    anyhow::ensure!(
        error.failure_kind.as_deref() == Some("sql_error"),
        "repository refusal lost its classification: {error}"
    );
    connection.close().await?;
    super::write_artifact(
        "extension-security.json",
        &serde_json::to_string_pretty(&json!({
            "allow_extension_repositories": "forbidden",
            "custom_repository_rejected": true,
            "pg_enable_remote_pushdown": true,
        }))?,
    );
    println!(
        "PASS: workers forbid custom extension repositories and retain reviewed PostgreSQL pushdown"
    );
    Ok(())
}

pub(super) async fn checks(ctx: &Ctx) -> anyhow::Result<()> {
    let mut connection = ctx.open_named("engine-qualification", None).await?;
    let placement = connection.placement.clone();
    let version = engine_version(&placement)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("worker placement has no DuckDB version: {placement}"))?;
    let mut results = Vec::new();
    for case in cases() {
        let started = Instant::now();
        let outcome = connection.execute_tracked(case.sql, case.parameters).await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let (request_id, outcome, profile) = match outcome {
            Ok((request_id, result)) => {
                let history = request_json(
                    &ctx.client()?,
                    Method::GET,
                    &format!("/v1/queries/{request_id}"),
                    None,
                )
                .await?;
                let profile = json!({
                    "status": history["profile_status"],
                    "metrics": history["metrics"],
                });
                (request_id, success(result), profile)
            }
            Err(error) => {
                let request_id = error.request_id.as_deref().unwrap_or_default().to_owned();
                (request_id, failure(&error), Value::Null)
            }
        };
        results.push(json!({
            "id": case.id,
            "sql": case.sql,
            "outcome": outcome,
            "elapsed_ms": elapsed_ms,
            "request_id": request_id,
            "profile": profile,
        }));
    }
    let extension_rows = connection.execute(
        "SELECT extension_name, loaded, installed, install_path, extension_version FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'postgres_scanner', 'ducklake', 'aws', 'icu') ORDER BY extension_name",
        Vec::new(),
    ).await?;
    let extensions = extension_rows
        .rows
        .into_iter()
        .map(|row| {
            json!({
                "extension_name": row[0].clone(),
                "loaded": row[1].clone(),
                "installed": row[2].clone(),
                "install_path": row[3].clone(),
                "extension_version": row[4].clone(),
            })
        })
        .collect::<Vec<_>>();
    let workers = request_json(&ctx.client()?, Method::GET, "/v1/workers", None).await?;
    connection.close().await?;
    let report = json!({
        "schema_version": 1,
        "suite": "duckfleet-engine-qualification",
        "engine_version": version,
        "placement": placement,
        "extensions": extensions,
        "workers": workers,
        "queries": results,
    });
    super::write_artifact(
        "engine-qualification.json",
        &serde_json::to_string_pretty(&report)?,
    );
    println!(
        "PASS: captured {} engine qualification cases on {version}",
        report["queries"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}
