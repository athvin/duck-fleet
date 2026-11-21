//! Read-only history requests use independent, bounded PostgreSQL connections.
//! They never enter the DuckDB worker queue or acquire its execution lock.
use crate::{
    ApiError,
    authz::{Action, MetadataAccess},
    credentials::Identity,
    failure,
    metadata::{self, Relation, RelationPlan},
};
use postgres::{Row, Transaction};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Reader {
    reader: metadata::Reader,
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

fn policy(profile: bool) -> metadata::PrivacySql {
    let mut policy = metadata::privacy_sql(
        Relation::History,
        "h",
        None,
        metadata::PrivacyBindings {
            namespace: 1,
            principal: 2,
            all: "$3",
            sql_all: "$4",
            sql_own: "$5",
            profile_all: if profile { "$6" } else { "false" },
            profile_own: if profile { "$7" } else { "false" },
        },
    );
    policy.from.push_str(&metadata::history_enrichment(
        None,
        "h",
        "d",
        1,
        &policy.owner,
    ));
    policy
}
fn columns(policy: &metadata::PrivacySql) -> String {
    let sql = &policy.sql_visible;
    let owner = &policy.owner;
    let captured = metadata::history_capture_json(sql);
    format!("h.query_id,h.session_id,CASE WHEN {sql} THEN h.sql ELSE NULL END,h.context,h.status,h.accepted_at::text,h.finished_at::text,h.elapsed_ms,h.result_rows,CASE WHEN {sql} THEN h.error ELSE NULL END,h.placement,h.history_id,h.worker_id,h.profile_status,h.metrics,h.failure_kind,h.retry_safe,h.principal,
        jsonb_build_object('queued_at',h.queued_at,'selected_at',h.selected_at,'execution_granted_at',h.execution_granted_at,'engine_started_at',h.engine_started_at,'queue_deadline_at',h.queue_deadline_at,'session_sequence',h.session_sequence,'worker_boot_id',h.worker_boot_id,'pool_scope',h.pool_scope,'protocol',h.protocol,'open_span_id',h.open_span_id,'namespace_id',COALESCE(h.namespace_id,$1),'owner_principal_id',{owner},'actor_principal_id',h.actor_principal_id,'actor_kind',h.actor_kind,'actor_principal',h.actor_principal,'actor_credential_id',h.actor_credential_id)::text,{owner},{captured}").replace("h.context,",&format!("CASE WHEN {sql} THEN h.context ELSE 'null' END,"))
}
fn record(row: &Row, plan: &RelationPlan) -> Value {
    let json_column =
        |column| serde_json::from_str(row.get::<_, &str>(column)).unwrap_or(Value::Null);
    let owner = row.get::<_, Option<&str>>(19);
    let placement = json_column(10);
    let mut value = json!({
        "query_id":row.get::<_,String>(0),"session_id":row.get::<_,Option<String>>(1),"sql":row.get::<_,Option<String>>(2),
        "context":json_column(3),"status":row.get::<_,String>(4),"accepted_at":row.get::<_,String>(5),
        "finished_at":row.get::<_,Option<String>>(6),"elapsed_ms":row.get::<_,Option<i64>>(7),"result_rows":row.get::<_,Option<i64>>(8),
        "error":row.get::<_,Option<String>>(9),"placement":if plan.sql_visible(owner){placement}else{metadata::safe_placement(&placement)},
        "history_id":row.get::<_,i64>(11),"worker_id":row.get::<_,Option<String>>(12),"profile_status":row.get::<_,String>(13),
        "metrics":metadata::numeric_metrics(&json_column(14)),"failure_kind":row.get::<_,Option<String>>(15),"retry_safe":row.get::<_,Option<bool>>(16),
        "principal":row.get::<_,String>(17)
    });
    queue_metadata(&mut value, &json_column(18));
    if let Some(captured) = json_column(20).as_object() {
        value
            .as_object_mut()
            .expect("history object")
            .extend(captured.clone());
    }
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
        "namespace_id",
        "owner_principal_id",
        "actor_principal_id",
        "actor_kind",
        "actor_principal",
        "actor_credential_id",
    ] {
        value[key] = stored.get(key).cloned().unwrap_or(Value::Null);
    }
}
pub(crate) fn list_in(
    tx: &mut Transaction<'_>,
    access: &MetadataAccess,
    filter: Filter,
) -> Result<Value, ApiError> {
    filter.validate()?;
    let plan = metadata::relation_plan(access, Relation::History)?;
    if filter.model_id.is_some() || filter.invocation_id.is_some() || filter.model_run_id.is_some()
    {
        plan.sensitive_filter()?;
    }
    let policy = policy(false);
    let from = &policy.from;
    let visible = &policy.visible;
    let rows = tx
        .query(
            &format!(
                "SELECT {} FROM {from} WHERE {visible} AND ($6::bigint IS NULL OR h.history_id<$6)
        AND ($7::text IS NULL OR h.status=$7) AND ($8::text IS NULL OR h.worker_id=$8)
        AND ($9::text IS NULL OR h.context::jsonb #>> '{{dbt,node_id}}'=$9)
        AND ($10::text IS NULL OR h.context::jsonb #>> '{{dbt,invocation_id}}'=$10)
        AND ($11::text IS NULL OR h.context::jsonb ->> 'model_run_id'=$11)
        AND ($12::text IS NULL OR h.failure_kind=$12) ORDER BY h.history_id DESC LIMIT $13",
                columns(&policy)
            ),
            &[
                &plan.namespace_id,
                &plan.principal_id,
                &plan.all,
                &plan.sql_all,
                &plan.sql_own,
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
    let mut queries: Vec<Value> = rows.iter().map(|row| record(row, &plan)).collect();
    let more = queries.len() > filter.limit as usize;
    queries.truncate(filter.limit as usize);
    let next = if more {
        queries.last().map(|row| row["history_id"].clone())
    } else {
        None
    };
    Ok(json!({"queries":queries,"next_before":next}))
}
pub(crate) fn detail_in(
    tx: &mut Transaction<'_>,
    access: &MetadataAccess,
    id: Uuid,
) -> Result<Value, ApiError> {
    let plan = metadata::relation_plan(access, Relation::History)?;
    let policy = policy(true);
    let row=tx.query_opt(&format!("SELECT {},CASE WHEN {} THEN h.profile ELSE NULL END FROM {} WHERE {} AND h.query_id=$8",columns(&policy),policy.profile_visible,policy.from,policy.visible),
        &[&plan.namespace_id,&plan.principal_id,&plan.all,&plan.sql_all,&plan.sql_own,&plan.profile_all,&plan.profile_own,&id.to_string()])
        .map_err(|_|ApiError::internal())?.ok_or_else(||ApiError::new(failure::NOT_FOUND,"query not found"))?;
    let mut value = record(&row, &plan);
    value["profile"] = (if plan.profile_visible(row.get::<_, Option<&str>>(19)) {
        row.get::<_, Option<&str>>(row.len() - 1)
    } else {
        None
    })
    .and_then(|raw| serde_json::from_str(raw).ok())
    .unwrap_or(Value::Null);
    Ok(value)
}
impl Reader {
    pub fn new(dsn: String) -> Self {
        Self {
            reader: metadata::Reader::new(dsn),
        }
    }
    pub async fn list(&self, identity: Identity, filter: Filter) -> Result<Value, ApiError> {
        filter.validate()?;
        self.reader
            .run(identity, Action::HistoryRead, None, move |tx, access| {
                list_in(tx, &access.metadata(), filter)
            })
            .await
    }
    pub async fn detail(&self, identity: Identity, id: Uuid) -> Result<Value, ApiError> {
        self.reader
            .run(
                identity,
                Action::HistoryRead,
                Some(id.to_string()),
                move |tx, access| detail_in(tx, &access.metadata(), id),
            )
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
