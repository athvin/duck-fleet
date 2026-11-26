//! DuckLake snapshot exploration: list, inspect, time-travel preview, and a
//! bounded net diff. Every command is a read through an ordinary authorized
//! session, so a reader principal can use it exactly as far as its catalog
//! and storage credentials allow. Nothing here expires snapshots, deletes
//! files, or changes current data.

use crate::client::{QueryResult, RemoteConnection, RemoteError};
use serde_json::{Value, json};

pub const DEFAULT_PAGE: u32 = 50;
pub const MAX_PAGE: u32 = 500;
pub const DEFAULT_EXAMPLES: u32 = 10;
pub const MAX_EXAMPLES: u32 = 100;
const STATEMENT_SECONDS: u64 = 60;

pub enum Command {
    List {
        limit: u32,
        before: Option<u64>,
    },
    Show {
        id: u64,
    },
    Preview {
        relation: String,
        at: u64,
        limit: u32,
    },
    Diff {
        relation: String,
        from: u64,
        to: u64,
        examples: u32,
    },
}

pub fn parse_id(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(|| format!("{flag} needs a snapshot ID (a non-negative whole number)"))
}

pub fn bounded(flag: &str, value: Option<&str>, default: u32, maximum: u32) -> Result<u32, String> {
    match value {
        None => Ok(default),
        Some(text) => text
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=maximum).contains(n))
            .ok_or_else(|| format!("{flag} must be a whole number from 1 to {maximum}")),
    }
}

pub async fn run(
    url: &str,
    token: &str,
    command: Command,
    format: &str,
) -> Result<(), RemoteError> {
    if let Command::Diff { from, to, .. } = &command
        && from > to
    {
        return Err(local_error(
            "invalid_request",
            format!(
                "--from {from} is after --to {to}; a diff reads forward from an older snapshot"
            ),
        ));
    }
    let mut connection = RemoteConnection::open(url, token, "snapshots", None, None, 90).await?;
    let result = tokio::select! {
        result = execute(&connection, command, format) => result,
        _ = tokio::signal::ctrl_c() => {
            let _ = connection.cancel().await;
            Err(local_error("canceled", "snapshot read canceled; nothing was changed"))
        }
    };
    let close = connection.close().await;
    result?;
    close
}

async fn execute(
    connection: &RemoteConnection,
    command: Command,
    format: &str,
) -> Result<(), RemoteError> {
    let catalog = current_catalog(connection).await?;
    match command {
        Command::List { limit, before } => {
            let mut sql = format!(
                "SELECT snapshot_id, snapshot_time::VARCHAR AS snapshot_time, schema_version, changes::VARCHAR AS changes FROM ducklake_snapshots({})",
                literal(&catalog)
            );
            if let Some(before) = before {
                sql.push_str(&format!(" WHERE snapshot_id < {before}"));
            }
            sql.push_str(&format!(" ORDER BY snapshot_id DESC LIMIT {limit}"));
            let rows = query(connection, &sql).await?;
            let next = (rows.rows.len() == limit as usize)
                .then(|| rows.rows.last().and_then(|row| row[0].clone()))
                .flatten();
            if format == "json" {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "snapshots": crate::cli::rows_json(&rows),
                        "next_before": next.as_deref().map(|id| id.parse::<u64>().unwrap_or_default()),
                    }))
                    .unwrap_or_default()
                );
            } else {
                crate::cli::emit(&rows, format);
                if let Some(next) = next {
                    eprintln!("next page: --before {next}");
                }
            }
        }
        Command::Show { id } => {
            let rows = snapshot_rows(connection, &catalog, &[id]).await?;
            if rows.rows.is_empty() {
                return Err(missing(id));
            }
            crate::cli::emit(
                &rows,
                if format == "table" {
                    "vertical"
                } else {
                    format
                },
            );
        }
        Command::Preview {
            relation,
            at,
            limit,
        } => {
            let relation = crate::catalog::resolve_table(connection, &relation).await?;
            require_snapshots(connection, &catalog, &[at]).await?;
            eprintln!("snapshot: {at}; preview_limit: {limit}; current data is unchanged");
            let rows = query(
                connection,
                &format!(
                    "SELECT * FROM {} AT (VERSION => {at}) LIMIT {limit}",
                    relation.qualified()
                ),
            )
            .await?;
            crate::cli::emit(&rows, format);
        }
        Command::Diff {
            relation,
            from,
            to,
            examples,
        } => {
            let relation = crate::catalog::resolve_table(connection, &relation).await?;
            require_snapshots(connection, &catalog, &[from, to]).await?;
            let (counts, rows) = if from == to {
                (json!({"added": 0, "deleted": 0, "updated": 0}), Vec::new())
            } else {
                diff(connection, &relation, from, to, examples).await?
            };
            let report = json!({
                "relation": relation.display(),
                "from": from,
                "to": to,
                "added": counts["added"],
                "deleted": counts["deleted"],
                "updated": counts["updated"],
                "example_limit": examples,
                "examples": rows,
            });
            if format == "json" {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).unwrap_or_default()
                );
            } else {
                println!(
                    "{} from snapshot {from} to {to}: {} added, {} deleted, {} updated",
                    relation.display(),
                    counts["added"],
                    counts["deleted"],
                    counts["updated"]
                );
                for example in report["examples"].as_array().into_iter().flatten() {
                    match example["change"].as_str() {
                        Some("updated") => println!(
                            "  updated rowid {}: {} -> {}",
                            example["rowid"], example["before"], example["after"]
                        ),
                        Some(change) => println!(
                            "  {change} rowid {}: {}",
                            example["rowid"],
                            if change == "added" {
                                &example["after"]
                            } else {
                                &example["before"]
                            }
                        ),
                        None => {}
                    }
                }
                eprintln!("example_limit: {examples} per change kind");
            }
        }
    }
    Ok(())
}

/// Net change per DuckLake row ID across the snapshots after `from` up to and
/// including `to`. A row counts as updated only when DuckLake attributed an
/// update to the same row ID and its value at `to` differs from `from`; rows
/// inserted and deleted inside the range are transient and not reported.
async fn diff(
    connection: &RemoteConnection,
    relation: &crate::catalog::ResolvedRelation,
    from: u64,
    to: u64,
    examples: u32,
) -> Result<(Value, Vec<Value>), RemoteError> {
    let changes = format!(
        "ducklake_table_changes({}, {}, {}, {}, {to})",
        literal(&relation.catalog),
        literal(&relation.schema),
        literal(&relation.relation),
        from + 1
    );
    let sql = format!(
        "WITH c AS (
            SELECT raw.rowid AS row_id,
                   raw.snapshot_id * 2 + CASE WHEN raw.change_type IN ('delete', 'update_preimage') THEN 0 ELSE 1 END AS seq,
                   raw.change_type AS change_type,
                   to_json(d)::VARCHAR AS image
            FROM {changes} raw, LATERAL (SELECT raw.* EXCLUDE (snapshot_id, rowid, change_type)) d
         ), r AS (
            SELECT row_id,
                   arg_min(change_type, seq) AS first_type, arg_max(change_type, seq) AS last_type,
                   arg_min(image, seq) AS first_image, arg_max(image, seq) AS last_image
            FROM c GROUP BY row_id
         ), k AS (
            SELECT row_id,
                   CASE
                     WHEN first_type IN ('delete', 'update_preimage') AND last_type IN ('insert', 'update_postimage')
                       THEN CASE WHEN first_image IS DISTINCT FROM last_image THEN 'updated' END
                     WHEN first_type IN ('delete', 'update_preimage') THEN 'deleted'
                     WHEN last_type IN ('insert', 'update_postimage') THEN 'added'
                   END AS change,
                   CASE WHEN first_type IN ('delete', 'update_preimage') THEN first_image END AS before_image,
                   CASE WHEN last_type IN ('insert', 'update_postimage') THEN last_image END AS after_image
            FROM r
         ), n AS (
            SELECT *, count(*) OVER (PARTITION BY change) AS total,
                   row_number() OVER (PARTITION BY change ORDER BY row_id) AS position
            FROM k WHERE change IS NOT NULL
         )
         SELECT change, total, row_id, before_image, after_image
         FROM n WHERE position <= {examples}
         ORDER BY change, row_id"
    );
    let rows = query(connection, &sql).await?;
    let mut counts = json!({"added": 0, "deleted": 0, "updated": 0});
    let mut examples = Vec::new();
    for row in &rows.rows {
        let change = row[0].clone().unwrap_or_default();
        let total = row[1]
            .as_deref()
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        counts[&change] = json!(total);
        let image = |cell: &Option<String>| {
            cell.as_deref()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .unwrap_or(Value::Null)
        };
        examples.push(json!({
            "change": change,
            "rowid": row[2].as_deref().and_then(|n| n.parse::<i64>().ok()),
            "before": image(&row[3]),
            "after": image(&row[4]),
        }));
    }
    Ok((counts, examples))
}

async fn current_catalog(connection: &RemoteConnection) -> Result<String, RemoteError> {
    let rows = query(connection, "SELECT current_catalog()").await?;
    rows.rows
        .first()
        .and_then(|row| row[0].clone())
        .ok_or_else(|| local_error("invalid_response", "no current catalog"))
}

async fn snapshot_rows(
    connection: &RemoteConnection,
    catalog: &str,
    ids: &[u64],
) -> Result<QueryResult, RemoteError> {
    let ids = ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    query(
        connection,
        &format!(
            "SELECT snapshot_id, snapshot_time::VARCHAR AS snapshot_time, schema_version, changes::VARCHAR AS changes, author, commit_message FROM ducklake_snapshots({}) WHERE snapshot_id IN ({ids}) ORDER BY snapshot_id",
            literal(catalog)
        ),
    )
    .await
}

/// Missing and expired snapshots look the same once expiry removed them; both
/// are refused before any table scan.
async fn require_snapshots(
    connection: &RemoteConnection,
    catalog: &str,
    ids: &[u64],
) -> Result<(), RemoteError> {
    let rows = snapshot_rows(connection, catalog, ids).await?;
    let found = rows
        .rows
        .iter()
        .filter_map(|row| row[0].as_deref()?.parse::<u64>().ok())
        .collect::<Vec<_>>();
    match ids.iter().find(|id| !found.contains(id)) {
        Some(id) => Err(missing(*id)),
        None => Ok(()),
    }
}

async fn query(connection: &RemoteConnection, sql: &str) -> Result<QueryResult, RemoteError> {
    connection
        .execute_with_timeouts(
            sql,
            Vec::new(),
            STATEMENT_SECONDS,
            duckfleet::protocol::DEFAULT_QUEUE_TIMEOUT,
        )
        .await
        .map(|(_, rows)| rows)
}

fn missing(id: u64) -> RemoteError {
    local_error(
        "snapshot_not_found",
        format!(
            "snapshot {id} does not exist or has expired; list retained snapshots with `duckfleet snapshots`"
        ),
    )
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn local_error(kind: &str, message: impl Into<String>) -> RemoteError {
    RemoteError {
        status: None,
        failure_kind: Some(kind.into()),
        retry_safe: true,
        message: message.into(),
        request_id: None,
        acceptance: Some("not_accepted".into()),
        status_url: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_bounds_are_validated_before_any_request() {
        assert_eq!(parse_id("--at", "42").unwrap(), 42);
        assert!(parse_id("--at", "-1").is_err());
        assert!(parse_id("--at", "1; DROP TABLE t").is_err());
        assert!(parse_id("--at", "9223372036854775808").is_err());
        assert_eq!(bounded("--limit", None, 50, 500).unwrap(), 50);
        assert!(bounded("--limit", Some("0"), 50, 500).is_err());
        assert!(bounded("--limit", Some("501"), 50, 500).is_err());
        assert_eq!(literal("a'b"), "'a''b'");
    }
}
