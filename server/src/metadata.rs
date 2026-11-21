//! Shared policy and projection boundary for HTTP metadata and future system SQL.
//! Visibility is applied by PostgreSQL before pagination; sensitive columns are
//! independent permissions, never inferred from permission to see a row.
use crate::{
    ApiError, audit,
    authz::{self, Access, Action, MetadataAccess, Scope},
    credentials::Identity,
    failure,
};
use postgres::{Client, NoTls, Transaction};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> anyhow::Result<()> {
    for table in [
        "requests",
        "query_history",
        "queue_sessions",
        "reservations",
        "usage_events",
    ] {
        let actor = if table == "requests" {
            ""
        } else {
            ", ADD COLUMN actor_principal TEXT"
        };
        tx.batch_execute(&format!("ALTER TABLE {table} ADD COLUMN namespace_id TEXT, ADD COLUMN owner_principal_id TEXT,
            ADD COLUMN actor_principal_id TEXT, ADD COLUMN actor_kind TEXT, ADD COLUMN actor_credential_id TEXT{actor}"))?;
    }
    tx.batch_execute(r#"ALTER TABLE requests ADD COLUMN authorization_policy_version BIGINT;
        ALTER TABLE queue_sessions ADD COLUMN authority_kind TEXT NOT NULL DEFAULT 'named',
            ADD COLUMN authority_principal_id TEXT, ADD COLUMN authority_policy_version BIGINT NOT NULL DEFAULT 0,
            ADD COLUMN operation TEXT NOT NULL DEFAULT '{"kind":"sql"}';
        CREATE INDEX history_owner_order ON query_history(namespace_id,owner_principal_id,history_id DESC);
        CREATE INDEX requests_owner_order ON requests(namespace_id,owner_principal_id,accepted_at,request_id);
        CREATE INDEX usage_owner_order ON usage_events(namespace_id,owner_principal_id,ledger_id DESC);"#)?;
    // Only live holds are enriched at upgrade, using credential evidence retained
    // by schema2. No full-history rewrite, invented actor, or pricing change.
    tx.execute("UPDATE reservations z SET namespace_id=p.namespace_id,owner_principal_id=p.principal_id,
        actor_principal_id=p.principal_id,actor_kind='named',actor_principal=r.actor_principal,actor_credential_id=r.credential_id
        FROM requests r JOIN credentials c ON c.credential_id=r.credential_id AND c.principal=r.actor_principal
        JOIN principals p ON p.principal_id=c.principal_id
        WHERE z.state='held' AND z.execution_id=r.request_id AND r.principal=r.actor_principal",&[])?;
    Ok(())
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub(crate) enum Relation {
    #[serde(rename = "query_history")]
    History,
    #[serde(rename = "query_attempts")]
    Attempts,
    #[serde(rename = "usage")]
    Usage,
}
#[derive(Clone, Debug)]
pub(crate) struct RelationPlan {
    pub namespace_id: String,
    pub principal_id: Option<String>,
    pub all: bool,
    pub sql_all: bool,
    pub sql_own: bool,
    pub profile_all: bool,
    pub profile_own: bool,
}
pub(crate) fn relation_plan(
    access: &MetadataAccess,
    relation: Relation,
) -> Result<RelationPlan, ApiError> {
    let scope = match relation {
        Relation::History | Relation::Attempts => access.history,
        Relation::Usage => access.usage,
    };
    let scope =
        scope.ok_or_else(|| ApiError::new(failure::FORBIDDEN, "metadata access is not granted"))?;
    Ok(RelationPlan {
        namespace_id: access.actor.namespace_id.clone(),
        principal_id: access.actor.principal_id.clone(),
        all: scope == Scope::All,
        sql_all: access.sql == Some(Scope::All),
        sql_own: access.sql == Some(Scope::Own),
        profile_all: access.profile == Some(Scope::All),
        profile_own: access.profile == Some(Scope::Own),
    })
}
impl RelationPlan {
    pub fn sensitive_filter(&self) -> Result<(), ApiError> {
        if self.sql_all || (!self.all && self.sql_own) {
            Ok(())
        } else {
            Err(ApiError::new(
                failure::FORBIDDEN,
                "filtering SQL-sensitive metadata requires SQL-read permission for every visible row",
            ))
        }
    }
    pub fn sql_visible(&self, owner: Option<&str>) -> bool {
        self.sql_all || (self.sql_own && owner.is_some() && owner == self.principal_id.as_deref())
    }
    pub fn profile_visible(&self, owner: Option<&str>) -> bool {
        self.sql_visible(owner)
            && (self.profile_all
                || (self.profile_own && owner.is_some() && owner == self.principal_id.as_deref()))
    }
}
/// A trusted fixed metric list: arbitrary JSON values/keys are never metadata.
pub(crate) fn numeric_metrics(value: &Value) -> Value {
    let mut result = json!({});
    for key in [
        "latency",
        "cpu_time",
        "rows_returned",
        "cumulative_rows_scanned",
        "system_peak_buffer_memory",
        "system_peak_temp_dir_size",
        "total_bytes_read",
        "total_bytes_written",
    ] {
        if let Some(number) = value.get(key).filter(|v| v.is_number()) {
            result[key] = number.clone();
        }
    }
    result
}
pub(crate) fn safe_placement(value: &Value) -> Value {
    let mut result = json!({});
    for key in [
        "mode",
        "pool",
        "worker_id",
        "worker_boot_id",
        "boot_id",
        "duckdb_version",
        "registry_revision",
    ] {
        if let Some(text) = value.get(key).filter(|v| v.is_string()) {
            result[key] = text.clone();
        }
    }
    for key in ["cpu", "memory_bytes", "disk_bytes"] {
        if let Some(number) = value.get(key).filter(|v| v.is_u64()) {
            result[key] = number.clone();
        }
    }
    for key in ["resources", "requested_compute", "resolved_compute"] {
        if let Some(nested) = value.get(key).filter(|v| v.is_object()) {
            result[key] = safe_placement(nested);
        }
    }
    result
}
pub(crate) fn bounded(tx: &mut Transaction<'_>) -> Result<(), ApiError> {
    tx.batch_execute("SET LOCAL statement_timeout=3000; SET LOCAL lock_timeout=500; SET LOCAL idle_in_transaction_session_timeout=3000")
        .map_err(|_|ApiError::internal())
}
#[derive(Clone)]
pub(crate) struct Reader {
    dsn: Arc<str>,
    capacity: Arc<Semaphore>,
}
impl Reader {
    pub fn new(dsn: String) -> Self {
        Self {
            dsn: dsn.into(),
            capacity: Arc::new(Semaphore::new(8)),
        }
    }
    pub async fn run<T, F>(
        &self,
        identity: Identity,
        action: Action,
        target: Option<String>,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Transaction<'_>, &Access) -> Result<T, ApiError> + Send + 'static,
    {
        let permit =
            self.capacity.clone().try_acquire_owned().map_err(|_| {
                ApiError::new(failure::CAPACITY, "metadata reader capacity reached")
            })?;
        let dsn = self.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.tcp_user_timeout(Duration::from_secs(3));
            // Preserve DSN options/search_path. Reads are application-owned SELECT
            // statements; the same transaction may append an authorization denial.
            let mut client: Client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            let mut tx = client.transaction().map_err(|_| ApiError::internal())?;
            bounded(&mut tx)?;
            let access = authz::resolve_in(&mut tx, &identity)?;
            match operation(&mut tx, &access) {
                Ok(value) => {
                    tx.commit().map_err(|_| ApiError::internal())?;
                    Ok(value)
                }
                Err(error) => {
                    if error.0 == failure::FORBIDDEN {
                        let event = audit::Event {
                            actor: access.actor.clone(),
                            action,
                            target_type: "metadata".into(),
                            target_id: target,
                            decision: "denied".into(),
                            reason: "permission_denied".into(),
                            policy_version: access.policy_version,
                            correlation_id: None,
                        };
                        if audit::append_in(&mut tx, &event).is_ok() {
                            let _ = tx.commit();
                        } else {
                            tracing::warn!(
                                "metadata authorization denied; audit persistence unavailable"
                            );
                        }
                    }
                    Err(error)
                }
            }
        })
        .await
        .map_err(|_| ApiError::internal())?
    }
}

#[path = "metadata_system.rs"]
mod system;
pub(crate) use system::*;

#[path = "metadata_capture.rs"]
mod capture;
pub(crate) use capture::*;
