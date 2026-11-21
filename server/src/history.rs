//! Read-only history requests use independent, bounded PostgreSQL connections.
//! They never enter the DuckDB worker queue or acquire its execution lock.
use crate::{ApiError, failure};
use postgres::{Client, NoTls, Row};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Reader {
    dsn: Arc<str>,
    capacity: Arc<Semaphore>,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Filter {
    pub limit: i64,
    pub before: Option<i64>,
    pub status: Option<String>,
    pub failure_kind: Option<String>,
    pub worker_id: Option<String>,
    pub model_id: Option<String>,
    pub invocation_id: Option<String>,
    pub model_run_id: Option<String>,
}
impl Default for Filter {
    fn default() -> Self {
        Self {
            limit: 500,
            before: None,
            status: None,
            failure_kind: None,
            worker_id: None,
            model_id: None,
            invocation_id: None,
            model_run_id: None,
        }
    }
}
impl Filter {
    fn validate(&self) -> Result<(), ApiError> {
        if !(1..=500).contains(&self.limit) || self.before.is_some_and(|n| n <= 0) {
            return Err(ApiError::bad(
                "limit must be 1–500 and before must be positive",
            ));
        }
        if self
            .status
            .as_deref()
            .is_some_and(|s| !failure::STATUSES.contains(&s))
        {
            return Err(ApiError::bad("unknown query status"));
        }
        if self
            .failure_kind
            .as_deref()
            .is_some_and(|s| failure::by_kind(s).is_none_or(|f| f.status.is_none()))
        {
            return Err(ApiError::bad("unknown failure kind"));
        }
        // A format check, not an allowlist: worker identities are deployment
        // configuration now, so naming one that does not exist is an empty page
        // rather than a bad request. Shape is still bounded, since this reaches a
        // query parameter.
        if self.worker_id.as_deref().is_some_and(|id| {
            id.is_empty()
                || id.len() > 63
                || !id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        }) {
            return Err(ApiError::bad(
                "worker_id must be 1-63 characters of letters, digits, dashes or underscores",
            ));
        }
        if [&self.model_id, &self.invocation_id, &self.model_run_id]
            .iter()
            .any(|v| v.as_ref().is_some_and(|s| s.is_empty() || s.len() > 512))
        {
            return Err(ApiError::bad(
                "history filter values must contain 1–512 bytes",
            ));
        }
        Ok(())
    }
}

const COLUMNS: &str = "query_id,session_id,sql,context,status,accepted_at::text,finished_at::text,elapsed_ms,result_rows,error,placement,history_id,worker_id,profile_status,metrics,failure_kind,retry_safe,principal,to_jsonb(query_history)::text";
fn record(row: &Row) -> Value {
    fn json_column(row: &Row, column: usize) -> Value {
        serde_json::from_str(row.get::<_, &str>(column)).unwrap_or(Value::Null)
    }
    let mut value = json!({
        "query_id":row.get::<_, String>(0), "session_id":row.get::<_, String>(1),
        "sql":row.get::<_, String>(2), "context":json_column(row,3),
        "status":row.get::<_, String>(4), "accepted_at":row.get::<_, String>(5),
        "finished_at":row.get::<_, Option<String>>(6), "elapsed_ms":row.get::<_, Option<i64>>(7),
        "result_rows":row.get::<_, Option<i64>>(8), "error":row.get::<_, Option<String>>(9),
        "placement":json_column(row,10), "history_id":row.get::<_, i64>(11),
        "worker_id":row.get::<_, String>(12), "profile_status":row.get::<_, String>(13),
        "metrics":json_column(row,14),
        // Null on success, and on records written before this column existed.
        "failure_kind":row.get::<_, Option<String>>(15),
        "retry_safe":row.get::<_, Option<bool>>(16),
        // Who the gateway authenticated for the session; `direct` for a worker
        // addressed without one, and for records older than the column.
        "principal":row.get::<_, String>(17)
    });
    queue_metadata(&mut value, &json_column(row, 18));
    value
}
fn queue_metadata(value: &mut Value, stored: &Value) {
    // Explicit projection is legacy-safe and never exposes the raw storage row.
    // accepted_at is durable admission; engine_started_at alone denotes SQL entry.
    for key in [
        "queued_at",
        "selected_at",
        "execution_granted_at",
        "engine_started_at",
        "queue_deadline_at",
        "session_sequence",
        "worker_boot_id",
        "pool_scope",
        "protocol",
        "open_span_id",
    ] {
        value[key] = stored.get(key).cloned().unwrap_or(Value::Null);
    }
}
impl Reader {
    pub fn new(dsn: String) -> Self {
        Self {
            dsn: dsn.into(),
            capacity: Arc::new(Semaphore::new(8)),
        }
    }
    async fn read<F>(&self, operation: F) -> Result<Value, ApiError>
    where
        F: FnOnce(&mut Client) -> Result<Value, ApiError> + Send + 'static,
    {
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "history reader capacity reached"))?;
        let dsn = self.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.options("-c default_transaction_read_only=on -c statement_timeout=3000");
            let mut client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            operation(&mut client)
        })
        .await
        .map_err(|_| ApiError::internal())?
    }
    pub async fn list(&self, filter: Filter) -> Result<Value, ApiError> {
        filter.validate()?;
        self.read(move |client| {
            let rows = client
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM query_history
                WHERE ($1::bigint IS NULL OR history_id < $1)
                  AND ($2::text IS NULL OR status=$2)
                  AND ($3::text IS NULL OR worker_id=$3)
                  AND ($4::text IS NULL OR context::jsonb #>> '{{dbt,node_id}}'=$4)
                  AND ($5::text IS NULL OR context::jsonb #>> '{{dbt,invocation_id}}'=$5)
                  AND ($6::text IS NULL OR context::jsonb ->> 'model_run_id'=$6)
                  AND ($7::text IS NULL OR failure_kind=$7)
                ORDER BY history_id DESC LIMIT $8"
                    ),
                    &[
                        &filter.before,
                        &filter.status,
                        &filter.worker_id,
                        &filter.model_id,
                        &filter.invocation_id,
                        &filter.model_run_id,
                        &filter.failure_kind,
                        &(filter.limit + 1),
                    ],
                )
                .map_err(|_| ApiError::internal())?;
            let mut queries: Vec<Value> = rows.iter().map(record).collect();
            let has_more = queries.len() > filter.limit as usize;
            queries.truncate(filter.limit as usize);
            let next_before = if has_more {
                queries.last().map(|q| q["history_id"].clone())
            } else {
                None
            };
            Ok(json!({"queries":queries, "next_before":next_before}))
        })
        .await
    }
    pub async fn detail(&self, id: Uuid) -> Result<Value, ApiError> {
        self.read(move |client| {
            let row = client
                .query_opt(
                    &format!("SELECT {COLUMNS},profile FROM query_history WHERE query_id=$1"),
                    &[&id.to_string()],
                )
                .map_err(|_| ApiError::internal())?
                .ok_or_else(|| ApiError::new(failure::NOT_FOUND, "query not found"))?;
            let mut value = record(&row);
            // Appended after COLUMNS above, so it is always the last column —
            // counted from the end rather than hardcoded, which is how adding
            // `principal` to COLUMNS once silently moved this to a column that
            // happened to parse as null.
            value["profile"] = row
                .get::<_, Option<&str>>(row.len() - 1)
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null);
            Ok(value)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queue_timing_is_nullable_for_legacy_and_does_not_infer_engine_entry() {
        let mut history = json!({"accepted_at":"2026-11-20T00:00:00Z"});
        queue_metadata(
            &mut history,
            &json!({"queued_at":"2026-11-20T00:00:00Z","protocol":"pgwire","private_payload":"secret"}),
        );
        assert_eq!(history["protocol"], "pgwire");
        assert!(history["engine_started_at"].is_null());
        assert!(history["session_sequence"].is_null());
        assert!(history.get("private_payload").is_none());
        queue_metadata(
            &mut history,
            &json!({"engine_started_at":"2026-11-20T00:00:02Z","session_sequence":3}),
        );
        assert_eq!(history["session_sequence"], 3);
        assert_eq!(history["engine_started_at"], "2026-11-20T00:00:02Z");
    }
    #[test]
    fn bounded_filters_reject_invalid_values() {
        for input in [
            json!({"limit":0}),
            json!({"limit":501}),
            json!({"before":0}),
            json!({"status":"made_up"}),
            // Shape, not membership: a worker that does not exist is an empty page.
            json!({"worker_id":""}),
            json!({"worker_id":"has spaces"}),
            json!({"worker_id":"a'b"}),
            json!({"model_id":""}),
            json!({"failure_kind":"made_up"}),
            // Rejection kinds never reach a history record, so they cannot be filtered.
            json!({"failure_kind":"capacity"}),
            json!({"failure_kind":"duplicate_request"}),
        ] {
            assert!(
                serde_json::from_value::<Filter>(input)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        assert!(Filter::default().validate().is_ok());
        assert!(serde_json::from_value::<Filter>(json!({"unknown":true})).is_err());
        for input in [
            json!({"status":"conflict"}),
            json!({"failure_kind":"transaction_conflict"}),
        ] {
            assert!(
                serde_json::from_value::<Filter>(input)
                    .unwrap()
                    .validate()
                    .is_ok()
            );
        }
    }
}
