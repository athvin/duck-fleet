//! Small platform authorization model. Reader and trusted-writer data authority
//! is enforced by separate query processes and their provisioned credentials.
pub(crate) use crate::credentials::ActorKind;
use crate::{
    ApiError, audit,
    compute::{Authority, Mode, Placement, Resources},
    credentials::{self, Identity},
    failure,
    job_policy::{AcceptedJobPolicy, JobDefaults, JobLimits, JobPoolPolicy, ValidatedJobRequest},
};
use anyhow::Result;
use postgres::{Client, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const OPERATION_HEADER: &str = "x-duckfleet-operation";
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Scope {
    Own,
    All,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Admin,
    Operator,
    Analyst,
    Viewer,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    #[serde(rename = "query.execute")]
    QueryExecute,
    #[serde(rename = "query.cancel")]
    QueryCancel,
    #[serde(rename = "session.close")]
    SessionClose,
    #[serde(rename = "history.read")]
    HistoryRead,
    #[serde(rename = "sql.read")]
    SqlRead,
    #[serde(rename = "profile.read")]
    ProfileRead,
    #[serde(rename = "result.read")]
    ResultRead,
    #[serde(rename = "usage.read")]
    UsageRead,
    #[serde(rename = "fleet.read")]
    FleetRead,
    #[serde(rename = "fleet.manage")]
    FleetManage,
    #[serde(rename = "budget.read")]
    BudgetRead,
    #[serde(rename = "budget.manage")]
    BudgetManage,
    #[serde(rename = "maintenance")]
    Maintenance,
    #[serde(rename = "credential.manage")]
    CredentialManage,
    #[serde(rename = "grant.manage")]
    GrantManage,
    #[serde(rename = "audit.read")]
    AuditRead,
}
impl Action {
    pub fn name(self) -> &'static str {
        match self {
            Self::QueryExecute => "query.execute",
            Self::QueryCancel => "query.cancel",
            Self::SessionClose => "session.close",
            Self::HistoryRead => "history.read",
            Self::SqlRead => "sql.read",
            Self::ProfileRead => "profile.read",
            Self::ResultRead => "result.read",
            Self::UsageRead => "usage.read",
            Self::FleetRead => "fleet.read",
            Self::FleetManage => "fleet.manage",
            Self::BudgetRead => "budget.read",
            Self::BudgetManage => "budget.manage",
            Self::Maintenance => "maintenance",
            Self::CredentialManage => "credential.manage",
            Self::GrantManage => "grant.manage",
            Self::AuditRead => "audit.read",
        }
    }
}
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TrustedOperation {
    #[default]
    Sql,
    LakeSnapshots,
    LakeMaintenance {
        expire_older_than_days: Option<u32>,
        compact: bool,
        cleanup: bool,
    },
}
impl TrustedOperation {
    pub fn action(&self) -> Action {
        match self {
            Self::Sql => Action::QueryExecute,
            Self::LakeSnapshots => Action::FleetRead,
            Self::LakeMaintenance { .. } => Action::Maintenance,
        }
    }
    pub fn canonical_sql(&self) -> Option<String> {
        match self {
        Self::Sql=>None,
        Self::LakeSnapshots=>Some("SELECT snapshot_id, snapshot_time, schema_version, changes FROM ducklake_snapshots('lake') ORDER BY snapshot_id".into()),
        Self::LakeMaintenance{expire_older_than_days,compact,cleanup}=>{
            let mut sql=Vec::new();
            if let Some(days)=expire_older_than_days {sql.push(format!("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '{days} days')"));}
            if *compact {sql.push("CALL ducklake_merge_adjacent_files('lake')".into());}
            if *cleanup {sql.push(match expire_older_than_days {Some(days)=>format!("CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '{days} days')"),None=>"CALL ducklake_cleanup_old_files('lake', cleanup_all => true)".into()});}
            Some(sql.join(";\n"))
        }
    }
    }
    pub fn validate(&self) -> Result<(), ApiError> {
        if let Self::LakeMaintenance {
            expire_older_than_days,
            compact,
            cleanup,
        } = self
            && (expire_older_than_days.is_some_and(|v| v > 3650)
                || (expire_older_than_days.is_none() && !compact && !cleanup))
        {
            return Err(ApiError::bad("invalid maintenance operation"));
        }
        Ok(())
    }
    pub fn validate_sql(&self, sql: &str) -> Result<(), ApiError> {
        self.validate()?;
        if self
            .canonical_sql()
            .is_some_and(|canonical| canonical != sql)
        {
            return Err(denied(
                "SQL does not match the authorized maintenance operation",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Actor {
    pub namespace_id: String,
    pub principal_id: Option<String>,
    pub principal: String,
    pub credential_id: Option<String>,
    pub kind: ActorKind,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PermissionGrant {
    pub action: Action,
    pub scope: Scope,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputeGrant {
    pub pool: Option<String>,
    pub mode: Mode,
    pub cpu: u16,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_limits: Option<JobLimits>,
}
impl ComputeGrant {
    fn validate(&self) -> Result<()> {
        Resources {
            cpu: self.cpu,
            memory_bytes: self.memory_bytes,
            disk_bytes: self.disk_bytes,
        }
        .usable()
        .map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            self.memory_bytes <= i64::MAX as u64 && self.disk_bytes <= i64::MAX as u64,
            "compute grant too large"
        );
        if let Some(limits) = self.job_limits {
            limits.validate()?;
        }
        match self.mode {
            Mode::Persistent => anyhow::ensure!(
                self.pool
                    .as_deref()
                    .is_some_and(crate::compute::valid_pool_name),
                "persistent grant requires a valid pool"
            ),
            Mode::Isolated => {
                anyhow::ensure!(self.pool.is_none(), "isolated grant cannot name a pool")
            }
        };
        Ok(())
    }
    fn covers(&self, requested: &Self) -> bool {
        self.pool == requested.pool
            && self.mode == requested.mode
            && self.cpu >= requested.cpu
            && self.memory_bytes >= requested.memory_bytes
            && self.disk_bytes >= requested.disk_bytes
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PolicyUpdate {
    pub roles: Vec<Role>,
    #[serde(default)]
    pub permissions: Vec<PermissionGrant>,
    #[serde(default)]
    pub compute: Vec<ComputeGrant>,
    #[serde(default)]
    pub disabled: bool,
}
impl PolicyUpdate {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.roles.len() <= 4 && self.permissions.len() <= 64 && self.compute.len() <= 64,
            "policy exceeds grant bounds"
        );
        for grant in &self.compute {
            grant.validate()?;
        }
        for grant in &self.permissions {
            anyhow::ensure!(
                grant.action != Action::QueryExecute || grant.scope == Scope::Own,
                "execute is owner-scoped"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Access {
    pub actor: Actor,
    pub policy_version: i64,
    roles: Vec<Role>,
    permissions: Vec<PermissionGrant>,
    compute: Vec<ComputeGrant>,
}
#[derive(Clone, Debug)]
pub(crate) struct MetadataAccess {
    pub actor: Actor,
    pub history: Option<Scope>,
    pub sql: Option<Scope>,
    pub profile: Option<Scope>,
    pub usage: Option<Scope>,
}
impl Access {
    pub fn authority(&self) -> Authority {
        if self.actor.kind == ActorKind::Named
            && !self
                .roles
                .iter()
                .any(|role| matches!(role, Role::Admin | Role::Operator | Role::Analyst))
        {
            Authority::Reader
        } else {
            Authority::Writer
        }
    }

    pub fn scope(&self, action: Action) -> Option<Scope> {
        if self.actor.kind != ActorKind::Named {
            return Some(Scope::All);
        }
        let mut scope = None;
        for role in &self.roles {
            let allowed = match role {
                Role::Admin => Some(if action == Action::QueryExecute {
                    Scope::Own
                } else {
                    Scope::All
                }),
                Role::Operator => match action {
                    Action::QueryCancel
                    | Action::SessionClose
                    | Action::HistoryRead
                    | Action::UsageRead
                    | Action::FleetRead
                    | Action::BudgetRead
                    | Action::Maintenance => Some(Scope::All),
                    _ => None,
                },
                Role::Analyst => match action {
                    Action::QueryExecute
                    | Action::QueryCancel
                    | Action::SessionClose
                    | Action::HistoryRead
                    | Action::SqlRead
                    | Action::ProfileRead
                    | Action::ResultRead
                    | Action::UsageRead => Some(Scope::Own),
                    _ => None,
                },
                Role::Viewer => match action {
                    Action::QueryExecute
                    | Action::QueryCancel
                    | Action::SessionClose
                    | Action::HistoryRead
                    | Action::UsageRead => Some(Scope::Own),
                    _ => None,
                },
            };
            scope = union(scope, allowed);
        }
        for grant in &self.permissions {
            if grant.action == action {
                scope = union(scope, Some(grant.scope));
            }
        }
        scope
    }
    /// System-mode entry needs metadata visibility, including an own-only scope.
    /// Exact relation and sensitive-column checks happen in the shared reader.
    pub fn require_metadata_access(&self) -> Result<(), ApiError> {
        if self.scope(Action::HistoryRead).is_some() || self.scope(Action::UsageRead).is_some() {
            Ok(())
        } else {
            Err(denied("metadata access is required"))
        }
    }
    /// Owner is an immutable principal UUID, never a display name.
    pub fn require(&self, action: Action, owner: Option<&str>) -> Result<(), ApiError> {
        let allowed = match self.scope(action) {
            Some(Scope::All) => true,
            Some(Scope::Own) => owner.is_some() && owner == self.actor.principal_id.as_deref(),
            None => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(denied("permission denied"))
        }
    }
    pub fn require_execution(
        &self,
        owner_id: Option<&str>,
        compute: &Value,
        operation: &TrustedOperation,
    ) -> Result<(), ApiError> {
        operation.validate()?;
        if self.actor.kind == ActorKind::Named
            && (owner_id.is_none() || owner_id != self.actor.principal_id.as_deref())
        {
            return Err(denied("session belongs to another principal"));
        }
        self.require(operation.action(), owner_id)?;
        let authority = compute["authority"].as_str().unwrap_or("writer");
        if authority != self.authority().name() {
            return Err(denied(
                "data authority changed; close this session and open a new one with the current role",
            ));
        }
        self.require_compute(compute)
    }
    #[cfg(test)]
    pub fn bootstrap_for_test() -> Self {
        Self {
            actor: Actor {
                namespace_id: "test".into(),
                principal_id: None,
                principal: credentials::BOOTSTRAP_PRINCIPAL.into(),
                credential_id: None,
                kind: ActorKind::Bootstrap,
            },
            policy_version: 0,
            roles: vec![],
            permissions: vec![],
            compute: vec![],
        }
    }
    pub fn metadata(&self) -> MetadataAccess {
        MetadataAccess {
            actor: self.actor.clone(),
            history: self.scope(Action::HistoryRead),
            sql: self.scope(Action::SqlRead),
            profile: self.scope(Action::ProfileRead),
            usage: self.scope(Action::UsageRead),
        }
    }
    /// Resolve omitted job options once. Each candidate comes from one complete
    /// compute grant; choosing a deterministic candidate never unions its fields.
    pub fn resolve_job_policy(
        &self,
        owner_id: Option<&str>,
        placement: &Placement,
        request: &ValidatedJobRequest,
        defaults: &JobDefaults,
        operator_ceiling: JobLimits,
        pool: &JobPoolPolicy,
    ) -> Result<AcceptedJobPolicy, ApiError> {
        request.request().compute.validate_resolved(placement)?;
        let compute = serde_json::to_value(placement).map_err(|_| ApiError::internal())?;
        self.require_execution(owner_id, &compute, &TrustedOperation::Sql)?;
        operator_ceiling
            .validate()
            .map_err(|_| ApiError::internal())?;
        pool.ceilings.validate().map_err(|_| ApiError::internal())?;
        let ceiling = operator_ceiling.intersect(pool.ceilings);
        if self.actor.kind != ActorKind::Named {
            return request
                .options()
                .resolve(defaults, ceiling, pool.generation);
        }
        let asked = allocation(&compute)?;
        let mut selected: Option<AcceptedJobPolicy> = None;
        for grant in self.compute.iter().filter(|grant| grant.covers(&asked)) {
            let limit = ceiling.intersect(grant.job_limits.unwrap_or(JobLimits::BASELINE));
            match request.options().resolve(defaults, limit, pool.generation) {
                Ok(candidate)
                    if selected
                        .as_ref()
                        .is_none_or(|old| candidate.limits > old.limits) =>
                {
                    selected = Some(candidate);
                }
                Ok(_) => {}
                Err(error) if error.0.kind == failure::FORBIDDEN.kind => {}
                Err(error) => return Err(error),
            }
        }
        selected.ok_or_else(|| denied("job bounds are outside the principal's compute grants"))
    }

    /// Current permission must cover the retained policy; never shorten or
    /// re-resolve a replay or accepted queued job when defaults change.
    pub fn require_job(
        &self,
        owner_id: Option<&str>,
        placement: &Placement,
        accepted: &AcceptedJobPolicy,
        operator_ceiling: JobLimits,
        pool: &JobPoolPolicy,
    ) -> Result<(), ApiError> {
        let compute = serde_json::to_value(placement).map_err(|_| ApiError::internal())?;
        self.require_execution(owner_id, &compute, &TrustedOperation::Sql)?;
        accepted.validate().map_err(|_| ApiError::internal())?;
        operator_ceiling
            .validate()
            .map_err(|_| ApiError::internal())?;
        pool.ceilings.validate().map_err(|_| ApiError::internal())?;
        if !operator_ceiling
            .intersect(pool.ceilings)
            .covers(accepted.limits)
        {
            return Err(denied(
                "accepted job exceeds the current operator or pool limits",
            ));
        }
        let asked = allocation(&compute)?;
        if self.actor.kind != ActorKind::Named
            || self.compute.iter().any(|grant| {
                grant.covers(&asked)
                    && grant
                        .job_limits
                        .unwrap_or(JobLimits::BASELINE)
                        .covers(accepted.limits)
            })
        {
            Ok(())
        } else {
            Err(denied("accepted job is outside the current compute grants"))
        }
    }

    pub fn require_compute(&self, compute: &Value) -> Result<(), ApiError> {
        let asked = allocation(compute)?;
        if self.actor.kind != ActorKind::Named || self.compute.iter().any(|g| g.covers(&asked)) {
            Ok(())
        } else {
            Err(denied(
                "compute allocation is outside the principal's grants",
            ))
        }
    }
}
fn union(a: Option<Scope>, b: Option<Scope>) -> Option<Scope> {
    match (a, b) {
        (Some(Scope::All), _) | (_, Some(Scope::All)) => Some(Scope::All),
        (Some(Scope::Own), _) | (_, Some(Scope::Own)) => Some(Scope::Own),
        _ => None,
    }
}
fn allocation(value: &Value) -> Result<ComputeGrant, ApiError> {
    let value = value.get("resolved_compute").unwrap_or(value);
    let resources = value.get("resources").unwrap_or(value);
    let mode = match value.get("mode").and_then(Value::as_str) {
        Some("persistent") => Mode::Persistent,
        Some("isolated") => Mode::Isolated,
        _ => return Err(ApiError::bad("resolved compute mode is required")),
    };
    let grant = ComputeGrant {
        pool: value.get("pool").and_then(Value::as_str).map(str::to_owned),
        mode,
        cpu: resources
            .get("cpu")
            .and_then(Value::as_u64)
            .and_then(|v| u16::try_from(v).ok())
            .ok_or_else(|| ApiError::bad("invalid resolved CPU"))?,
        memory_bytes: resources
            .get("memory_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| ApiError::bad("invalid resolved memory"))?,
        disk_bytes: resources
            .get("disk_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| ApiError::bad("invalid resolved disk"))?,
        job_limits: None,
    };
    grant
        .validate()
        .map_err(|_| ApiError::bad("invalid resolved compute"))?;
    Ok(grant)
}
fn denied(message: &str) -> ApiError {
    ApiError::new(failure::FORBIDDEN, message)
}
fn unavailable(_: impl std::fmt::Display) -> ApiError {
    ApiError::new(failure::UNAVAILABLE, "authorization service unavailable")
}
pub(crate) fn bounded(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("SET LOCAL statement_timeout=3000; SET LOCAL lock_timeout=3000")?;
    Ok(())
}

pub(crate) struct Seed {
    pub namespace: String,
    pub compute: ComputeGrant,
}
impl Seed {
    pub fn from_env() -> Result<Self> {
        let get =
            |name: &str, fallback: &str| std::env::var(name).unwrap_or_else(|_| fallback.into());
        let resources = Resources::parse(
            &get("DUCKFLEET_AUTHZ_DEFAULT_CPU", "1"),
            &get("DUCKFLEET_AUTHZ_DEFAULT_MEMORY", "1Gi"),
            &get("DUCKFLEET_AUTHZ_DEFAULT_DISK", "2Gi"),
        )
        .map_err(anyhow::Error::msg)?;
        let seed = Self {
            namespace: get("DUCKFLEET_NAMESPACE", "default"),
            compute: ComputeGrant {
                pool: Some(get("DUCKFLEET_AUTHZ_DEFAULT_POOL", "small")),
                mode: Mode::Persistent,
                cpu: resources.cpu,
                memory_bytes: resources.memory_bytes,
                disk_bytes: resources.disk_bytes,
                job_limits: None,
            },
        };
        seed.compute.validate()?;
        anyhow::ensure!(
            !seed.namespace.is_empty() && seed.namespace.len() <= 128,
            "invalid namespace"
        );
        Ok(seed)
    }
}
pub(crate) fn migrate(tx: &mut Transaction<'_>, seed: &Seed) -> Result<()> {
    seed.compute.validate()?;
    tx.batch_execute("CREATE TABLE platform_namespace(singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),id TEXT NOT NULL UNIQUE,label TEXT NOT NULL);
        CREATE TABLE principals(principal_id TEXT PRIMARY KEY,namespace_id TEXT NOT NULL REFERENCES platform_namespace(id),name TEXT NOT NULL,disabled BOOLEAN NOT NULL DEFAULT false,policy_version BIGINT NOT NULL DEFAULT 1,UNIQUE(namespace_id,name));
        CREATE TABLE principal_roles(principal_id TEXT NOT NULL REFERENCES principals(principal_id),role TEXT NOT NULL,PRIMARY KEY(principal_id,role));
        CREATE TABLE permission_grants(principal_id TEXT NOT NULL REFERENCES principals(principal_id),action TEXT NOT NULL,scope TEXT NOT NULL,PRIMARY KEY(principal_id,action,scope));
        CREATE TABLE compute_grants(principal_id TEXT NOT NULL REFERENCES principals(principal_id),grant_number BIGINT NOT NULL,grant_json TEXT NOT NULL,PRIMARY KEY(principal_id,grant_number));
        CREATE TABLE authz_legacy_owners(namespace_id TEXT NOT NULL,legacy_name TEXT NOT NULL,principal_id TEXT NOT NULL REFERENCES principals(principal_id),PRIMARY KEY(namespace_id,legacy_name));
        CREATE TABLE authz_defaults(singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),compute_json TEXT NOT NULL);
        ALTER TABLE credentials ADD COLUMN principal_id TEXT REFERENCES principals(principal_id);
        CREATE INDEX credentials_principal ON credentials(principal_id);")?;
    let namespace = Uuid::new_v4().to_string();
    tx.execute(
        "INSERT INTO platform_namespace(id,label) VALUES($1,$2)",
        &[&namespace, &seed.namespace],
    )?;
    tx.execute(
        "INSERT INTO authz_defaults(compute_json) VALUES($1)",
        &[&serde_json::to_string(&seed.compute)?],
    )?;
    for row in tx.query(
        "SELECT DISTINCT principal FROM credentials ORDER BY principal",
        &[],
    )? {
        let name: String = row.get(0);
        let id = ensure_principal_in(tx, &name)?;
        tx.execute(
            "UPDATE credentials SET principal_id=$2 WHERE principal=$1",
            &[&name, &id],
        )?;
        if !["direct", "platform", "legacy", "bootstrap-admin"].contains(&name.as_str()) {
            tx.execute(
                "INSERT INTO authz_legacy_owners VALUES($1,$2,$3)",
                &[&namespace, &name, &id],
            )?;
        }
    }
    Ok(())
}
pub(crate) fn ensure_principal_in(tx: &mut Transaction<'_>, name: &str) -> Result<String> {
    anyhow::ensure!(credentials::valid_principal(name), "invalid principal");
    let namespace: String = tx
        .query_one("SELECT id FROM platform_namespace WHERE singleton", &[])?
        .get(0);
    let id = Uuid::new_v4().to_string();
    let fresh=tx.execute("INSERT INTO principals(principal_id,namespace_id,name) VALUES($1,$2,$3) ON CONFLICT(namespace_id,name) DO NOTHING",&[&id,&namespace,&name])?>0;
    let id: String = tx
        .query_one(
            "SELECT principal_id FROM principals WHERE namespace_id=$1 AND name=$2",
            &[&namespace, &name],
        )?
        .get(0);
    if fresh {
        tx.execute(
            "INSERT INTO principal_roles VALUES($1,'\"analyst\"')",
            &[&id],
        )?;
        tx.execute("INSERT INTO compute_grants SELECT $1,0,compute_json FROM authz_defaults WHERE singleton",&[&id])?;
    }
    Ok(id)
}
pub(crate) fn owner_id_in(
    tx: &mut Transaction<'_>,
    name: &str,
) -> Result<Option<String>, ApiError> {
    Ok(tx.query_opt("SELECT principal_id FROM principals WHERE name=$1 AND namespace_id=(SELECT id FROM platform_namespace WHERE singleton)",&[&name]).map_err(unavailable)?.map(|r|r.get(0)))
}
const NAMESPACE_SQL: &str = "SELECT id FROM platform_namespace WHERE singleton";
// One statement observes credential, principal version, and every grant from
// one PostgreSQL snapshot, including caller-owned READ COMMITTED transactions.
const RESOLVE_POLICY_SQL: &str = "SELECT p.principal_id,p.policy_version,c.expires_at,
    COALESCE((SELECT json_agg(role::json ORDER BY role)::text FROM principal_roles WHERE principal_id=p.principal_id),'[]'),
    COALESCE((SELECT json_agg(json_build_object('action',action::json,'scope',scope::json) ORDER BY action,scope)::text FROM permission_grants WHERE principal_id=p.principal_id),'[]'),
    COALESCE((SELECT json_agg(grant_json::json ORDER BY grant_number)::text FROM compute_grants WHERE principal_id=p.principal_id),'[]')
    FROM credentials c JOIN principals p USING(principal_id) WHERE c.credential_id=$1 AND c.principal=$2 AND p.name=$2 AND p.namespace_id=$3 AND NOT p.disabled AND c.revoked_at IS NULL AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())";

pub(crate) fn resolve_in(
    tx: &mut Transaction<'_>,
    identity: &Identity,
) -> Result<Access, ApiError> {
    let namespace: String = tx
        .query_one(NAMESPACE_SQL, &[])
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    let Some(credential) = policy_credential(identity)? else {
        return Ok(privileged_access(namespace, identity));
    };
    let row = tx
        .query_opt(
            RESOLVE_POLICY_SQL,
            &[&credential, &identity.principal, &namespace],
        )
        .map_err(unavailable)?;
    policy_access(namespace, identity, row)
}

/// Async system reads share the exact policy snapshot and decoder with ordinary
/// authorization. The caller owns its read-only snapshot, timeout and audit.
pub(crate) async fn resolve_async_in(
    tx: &tokio_postgres::Transaction<'_>,
    identity: &Identity,
) -> Result<Access, ApiError> {
    let namespace: String = tx
        .query_one(NAMESPACE_SQL, &[])
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    let Some(credential) = policy_credential(identity)? else {
        return Ok(privileged_access(namespace, identity));
    };
    let row = tx
        .query_opt(
            RESOLVE_POLICY_SQL,
            &[&credential, &identity.principal, &namespace],
        )
        .await
        .map_err(unavailable)?;
    policy_access(namespace, identity, row)
}

fn policy_credential(identity: &Identity) -> Result<Option<&str>, ApiError> {
    if identity.kind == ActorKind::Named {
        return identity.credential_id.as_deref().map(Some).ok_or_else(|| {
            ApiError::new(failure::UNAUTHORIZED, "named actor requires a credential")
        });
    }
    if identity.credential_id.is_some()
        || (identity.kind == ActorKind::Bootstrap
            && identity.principal != credentials::BOOTSTRAP_PRINCIPAL)
    {
        return Err(ApiError::new(
            failure::UNAUTHORIZED,
            "invalid authenticated actor",
        ));
    }
    Ok(None)
}

fn privileged_access(namespace: String, identity: &Identity) -> Access {
    Access {
        actor: Actor {
            namespace_id: namespace,
            principal_id: None,
            principal: identity.principal.clone(),
            credential_id: None,
            kind: identity.kind,
        },
        policy_version: 0,
        roles: vec![],
        permissions: vec![],
        compute: vec![],
    }
}

fn policy_access(
    namespace: String,
    identity: &Identity,
    row: Option<postgres::Row>,
) -> Result<Access, ApiError> {
    let row = row.ok_or_else(|| {
        ApiError::new(
            failure::UNAUTHORIZED,
            "credential expired, revoked, or principal disabled",
        )
    })?;
    // The response may arrive after the SQL expiry predicate was evaluated.
    if row
        .try_get::<_, Option<std::time::SystemTime>>(2)
        .map_err(unavailable)?
        .is_some_and(|at| at <= std::time::SystemTime::now())
    {
        return Err(ApiError::new(failure::UNAUTHORIZED, "credential expired"));
    }
    let id: String = row.try_get(0).map_err(unavailable)?;
    let roles = serde_json::from_str(row.try_get::<_, &str>(3).map_err(unavailable)?)
        .map_err(unavailable)?;
    let permissions = serde_json::from_str(row.try_get::<_, &str>(4).map_err(unavailable)?)
        .map_err(unavailable)?;
    let compute = serde_json::from_str(row.try_get::<_, &str>(5).map_err(unavailable)?)
        .map_err(unavailable)?;
    Ok(Access {
        actor: Actor {
            namespace_id: namespace,
            principal_id: Some(id),
            principal: identity.principal.clone(),
            credential_id: identity.credential_id.clone(),
            kind: ActorKind::Named,
        },
        policy_version: row.try_get(1).map_err(unavailable)?,
        roles,
        permissions,
        compute,
    })
}
pub(crate) fn authorize_execution_for_owner_in(
    tx: &mut Transaction<'_>,
    identity: &Identity,
    owner_id: Option<&str>,
    compute: &Value,
    operation: &TrustedOperation,
) -> Result<Access, ApiError> {
    let access = resolve_in(tx, identity)?;
    access.require_execution(owner_id, compute, operation)?;
    Ok(access)
}
pub(crate) fn authorize_execution_for_owner(
    client: &mut Client,
    identity: &Identity,
    owner_id: Option<&str>,
    compute: &Value,
    operation: &TrustedOperation,
) -> Result<Access, ApiError> {
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    let result = access.require_execution(owner_id, compute, operation);
    if let Err(error) = &result {
        audit_denial(&mut tx, &access, operation.action(), owner_id, error)?;
    }
    tx.commit().map_err(unavailable)?;
    result.map(|()| access)
}
pub(crate) fn policy_fence_in(
    tx: &mut Transaction<'_>,
    principal_id: &str,
) -> Result<(), ApiError> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('duckfleet-policy/' || $1::text,0))",
        &[&principal_id],
    )
    .map_err(unavailable)?;
    Ok(())
}
pub(crate) fn lock_actor_in(tx: &mut Transaction<'_>, identity: &Identity) -> Result<(), ApiError> {
    if let Some(id) = &identity.credential_id {
        tx.query("SELECT p.principal_id FROM principals p JOIN credentials c USING(principal_id) WHERE c.credential_id=$1 FOR UPDATE OF p",&[id]).map_err(unavailable)?;
    }
    Ok(())
}
pub(crate) fn execution_authority(
    client: &mut Client,
    identity: &Identity,
) -> Result<Authority, ApiError> {
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let authority = resolve_in(&mut tx, identity)?.authority();
    tx.commit().map_err(unavailable)?;
    Ok(authority)
}

pub(crate) fn authorize_execution(
    client: &mut Client,
    identity: &Identity,
    owner: &str,
    compute: &Value,
    operation: &TrustedOperation,
) -> Result<Access, ApiError> {
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    let owner_id = owner_id_in(&mut tx, owner)?;
    let result = access.require_execution(owner_id.as_deref(), compute, operation);
    if let Err(error) = &result {
        audit_denial(
            &mut tx,
            &access,
            operation.action(),
            owner_id.as_deref(),
            error,
        )?;
    }
    tx.commit().map_err(unavailable)?;
    result.map(|()| access)
}
pub(crate) fn authorize_action(
    client: &mut Client,
    identity: &Identity,
    action: Action,
    owner: Option<&str>,
) -> Result<Access, ApiError> {
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    let result = access.require(action, owner);
    if let Err(error) = &result {
        audit_denial(&mut tx, &access, action, owner, error)?;
    }
    tx.commit().map_err(unavailable)?;
    result.map(|()| access)
}
fn audit_denial(
    tx: &mut Transaction<'_>,
    access: &Access,
    action: Action,
    target: Option<&str>,
    error: &ApiError,
) -> Result<(), ApiError> {
    audit::append_in(
        tx,
        &audit::Event::new(
            access,
            action,
            "authorization",
            target,
            "denied",
            error.0.kind,
            None,
        ),
    )
    .map_err(unavailable)
}

pub(crate) fn list_principals(
    client: &mut Client,
    identity: &Identity,
    limit: i64,
    after: Option<&str>,
) -> Result<Value, ApiError> {
    validate_page(limit, after)?;
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    if let Err(error) = require_in(&mut tx, &access, Action::GrantManage, None) {
        tx.commit().map_err(unavailable)?;
        return Err(error);
    }
    let rows=tx.query("SELECT principal_id,name,disabled,policy_version FROM principals WHERE namespace_id=$1 AND ($2::text IS NULL OR principal_id>$2) ORDER BY principal_id LIMIT $3",&[&access.actor.namespace_id,&after,&(limit+1)]).map_err(unavailable)?;
    let more = rows.len() > limit as usize;
    let page = &rows[..rows.len().min(limit as usize)];
    let value = json!({"principals":page.iter().map(|r|json!({"principal_id":r.get::<_,String>(0),"principal":r.get::<_,String>(1),"disabled":r.get::<_,bool>(2),"policy_version":r.get::<_,i64>(3)})).collect::<Vec<_>>(),"next_after":if more{page.last().map(|r|r.get::<_,String>(0))}else{None}});
    tx.commit().map_err(unavailable)?;
    Ok(value)
}
pub(crate) fn validate_page(limit: i64, cursor: Option<&str>) -> Result<(), ApiError> {
    if !(1..=500).contains(&limit) || cursor.is_some_and(|v| Uuid::parse_str(v).is_err()) {
        Err(ApiError::bad(
            "limit must be 1-500 and cursor must be a UUID",
        ))
    } else {
        Ok(())
    }
}
/// Caller must commit an authorization denial to retain its audit event; do not
/// use `?` to drop the transaction on this result at an API boundary.
pub(crate) fn require_in(
    tx: &mut Transaction<'_>,
    access: &Access,
    action: Action,
    owner: Option<&str>,
) -> Result<(), ApiError> {
    if let Err(error) = access.require(action, owner) {
        audit::append_in(
            tx,
            &audit::Event::new(
                access,
                action,
                "authorization",
                owner,
                "denied",
                "forbidden",
                None,
            ),
        )
        .map_err(unavailable)?;
        Err(error)
    } else {
        Ok(())
    }
}

pub(crate) fn get_policy(
    client: &mut Client,
    identity: &Identity,
    name: &str,
) -> Result<Value, ApiError> {
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    if let Err(error) = require_in(&mut tx, &access, Action::GrantManage, None) {
        tx.commit().map_err(unavailable)?;
        return Err(error);
    }
    let id = owner_id_in(&mut tx, name)?.ok_or_else(|| ApiError::bad("unknown principal"))?;
    let value = policy_value(&mut tx, &id).map_err(unavailable)?;
    tx.commit().map_err(unavailable)?;
    Ok(value)
}
fn policy_value(tx: &mut Transaction<'_>, id: &str) -> Result<Value> {
    let row = tx.query_one(
        "SELECT name,disabled,policy_version FROM principals WHERE principal_id=$1",
        &[&id],
    )?;
    let roles = tx
        .query(
            "SELECT role FROM principal_roles WHERE principal_id=$1 ORDER BY role",
            &[&id],
        )?
        .iter()
        .map(|r| serde_json::from_str::<Value>(r.get(0)))
        .collect::<Result<Vec<_>, _>>()?;
    let permissions=tx.query("SELECT action,scope FROM permission_grants WHERE principal_id=$1 ORDER BY action,scope",&[&id])?.iter().map(|r|Ok(json!({"action":serde_json::from_str::<Value>(r.get(0))?,"scope":serde_json::from_str::<Value>(r.get(1))?}))).collect::<Result<Vec<_>>>()?;
    let compute = tx
        .query(
            "SELECT grant_json FROM compute_grants WHERE principal_id=$1 ORDER BY grant_number",
            &[&id],
        )?
        .iter()
        .map(|r| serde_json::from_str::<Value>(r.get(0)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        json!({"principal_id":id,"principal":row.get::<_,String>(0),"disabled":row.get::<_,bool>(1),"policy_version":row.get::<_,i64>(2),"roles":roles,"permissions":permissions,"compute":compute}),
    )
}
pub(crate) fn set_policy(
    client: &mut Client,
    identity: &Identity,
    name: &str,
    policy: &PolicyUpdate,
) -> Result<Value, ApiError> {
    policy
        .validate()
        .map_err(|e| ApiError::bad(e.to_string()))?;
    let mut tx = client.transaction().map_err(unavailable)?;
    bounded(&mut tx).map_err(unavailable)?;
    let initial = resolve_in(&mut tx, identity)?;
    if let Err(error) = require_in(&mut tx, &initial, Action::GrantManage, None) {
        tx.commit().map_err(unavailable)?;
        return Err(error);
    }
    let target = owner_id_in(&mut tx, name)?.ok_or_else(|| ApiError::bad("unknown principal"))?;
    // Stable principal locks precede authorization: a revoked manager cannot use
    // a previously read grant to commit another policy mutation.
    let actor = identity
        .credential_id
        .as_ref()
        .map(|id| {
            tx.query_opt(
                "SELECT principal_id FROM credentials WHERE credential_id=$1",
                &[id],
            )
        })
        .transpose()
        .map_err(unavailable)?
        .flatten()
        .and_then(|r| r.get::<_, Option<String>>(0));
    let mut ids = vec![target.clone()];
    if let Some(id) = actor {
        ids.push(id)
    }
    ids.sort();
    ids.dedup();
    for id in &ids {
        policy_fence_in(&mut tx, id)?;
    }
    tx.query("SELECT principal_id FROM principals WHERE principal_id=ANY($1) ORDER BY principal_id FOR UPDATE",&[&ids]).map_err(unavailable)?;
    let access = resolve_in(&mut tx, identity)?;
    if let Err(error) = require_in(&mut tx, &access, Action::GrantManage, None) {
        tx.commit().map_err(unavailable)?;
        return Err(error);
    }
    tx.execute(
        "UPDATE principals SET disabled=$2,policy_version=policy_version+1 WHERE principal_id=$1",
        &[&target, &policy.disabled],
    )
    .map_err(unavailable)?;
    for table in ["principal_roles", "permission_grants", "compute_grants"] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE principal_id=$1"),
            &[&target],
        )
        .map_err(unavailable)?;
    }
    for role in &policy.roles {
        tx.execute(
            "INSERT INTO principal_roles VALUES($1,$2) ON CONFLICT DO NOTHING",
            &[&target, &serde_json::to_string(role).map_err(unavailable)?],
        )
        .map_err(unavailable)?;
    }
    for grant in &policy.permissions {
        tx.execute(
            "INSERT INTO permission_grants VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            &[
                &target,
                &serde_json::to_string(&grant.action).map_err(unavailable)?,
                &serde_json::to_string(&grant.scope).map_err(unavailable)?,
            ],
        )
        .map_err(unavailable)?;
    }
    for (i, grant) in policy.compute.iter().enumerate() {
        tx.execute(
            "INSERT INTO compute_grants VALUES($1,$2,$3)",
            &[
                &target,
                &(i as i64),
                &serde_json::to_string(grant).map_err(unavailable)?,
            ],
        )
        .map_err(unavailable)?;
    }
    audit::append_in(
        &mut tx,
        &audit::Event::new(
            &access,
            Action::GrantManage,
            "principal",
            Some(&target),
            "allowed",
            "policy_replaced",
            None,
        ),
    )
    .map_err(unavailable)?;
    let value = policy_value(&mut tx, &target).map_err(unavailable)?;
    tx.commit().map_err(unavailable)?;
    Ok(value)
}

pub(crate) fn validate_namespace(client: &mut Client) -> Result<()> {
    let configured = std::env::var("DUCKFLEET_NAMESPACE").unwrap_or_else(|_| "default".into());
    let mut tx = client.build_transaction().read_only(true).start()?;
    bounded(&mut tx)?;
    let label: String = tx
        .query_one("SELECT label FROM platform_namespace WHERE singleton", &[])?
        .get(0);
    anyhow::ensure!(
        label == configured,
        "configured namespace does not match platform database"
    );
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn access(role: Role) -> Access {
        Access {
            actor: Actor {
                namespace_id: "namespace".into(),
                principal_id: Some("owner".into()),
                principal: "analyst".into(),
                credential_id: Some("credential".into()),
                kind: ActorKind::Named,
            },
            policy_version: 1,
            roles: vec![role],
            permissions: vec![],
            compute: vec![],
        }
    }
    #[test]
    fn system_entry_accepts_own_metadata_independently_of_compute() {
        let viewer = access(Role::Viewer);
        assert!(viewer.require_metadata_access().is_ok());
        assert!(viewer.require(Action::HistoryRead, None).is_err());
        assert!(viewer.require(Action::QueryExecute, Some("owner")).is_ok());
        assert!(viewer.require(Action::QueryExecute, Some("other")).is_err());
        assert!(viewer.compute.is_empty());
        let mut custom = access(Role::Viewer);
        custom.roles.clear();
        assert!(custom.require_metadata_access().is_err());
        custom.permissions.push(PermissionGrant {
            action: Action::SqlRead,
            scope: Scope::All,
        });
        assert!(custom.require_metadata_access().is_err());
        custom.permissions.push(PermissionGrant {
            action: Action::UsageRead,
            scope: Scope::Own,
        });
        assert!(custom.require_metadata_access().is_ok());
    }

    #[test]
    fn presets_keep_observation_and_execution_separate() {
        let operator = access(Role::Operator);
        assert!(operator.require(Action::QueryCancel, Some("other")).is_ok());
        assert!(
            operator
                .require(Action::QueryExecute, Some("owner"))
                .is_err()
        );
        assert_eq!(operator.scope(Action::SqlRead), None);
        let admin = access(Role::Admin);
        assert!(admin.require(Action::QueryExecute, Some("other")).is_err());
        let viewer = access(Role::Viewer);
        assert_eq!(viewer.authority(), Authority::Reader);
        assert_eq!(admin.authority(), Authority::Writer);
        assert!(viewer.require(Action::HistoryRead, Some("owner")).is_ok());
        assert!(viewer.require(Action::HistoryRead, Some("other")).is_err());
        assert!(
            admin
                .require_execution(
                    Some("owner"),
                    &json!({"authority":"reader"}),
                    &TrustedOperation::Sql,
                )
                .is_err(),
            "a writer cannot reuse or open a reader-authority session"
        );
    }
    #[test]
    fn compute_requires_one_complete_envelope() {
        let mut a = access(Role::Analyst);
        a.compute = vec![
            ComputeGrant {
                pool: Some("small".into()),
                mode: Mode::Persistent,
                cpu: 1,
                memory_bytes: 4 << 30,
                disk_bytes: 2 << 30,
                job_limits: None,
            },
            ComputeGrant {
                pool: Some("small".into()),
                mode: Mode::Persistent,
                cpu: 4,
                memory_bytes: 1 << 30,
                disk_bytes: 2 << 30,
                job_limits: None,
            },
        ];
        assert!(a.require_compute(&json!({"mode":"persistent","pool":"small","cpu":4,"memory_bytes":4u64<<30,"disk_bytes":2u64<<30})).is_err());
        assert!(a.require_compute(&json!({"mode":"persistent","pool":"small","cpu":1,"memory_bytes":1u64<<30,"disk_bytes":2u64<<30})).is_ok());
    }
    fn job_placement() -> Placement {
        Placement {
            mode: Mode::Persistent,
            pool: Some("small".into()),
            resources: Resources::default(),
            authority: Authority::Writer,
        }
    }
    fn job_request(options: Value) -> ValidatedJobRequest {
        let mut value = json!({"request_id":Uuid::new_v4(),"sql":"SELECT 1"});
        value
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        serde_json::from_value::<crate::job_policy::JobRequest>(value)
            .unwrap()
            .validate()
            .unwrap()
    }
    fn job_grant(limits: Option<JobLimits>) -> ComputeGrant {
        ComputeGrant {
            pool: Some("small".into()),
            mode: Mode::Persistent,
            cpu: 1,
            memory_bytes: 1 << 30,
            disk_bytes: 2 << 30,
            job_limits: limits,
        }
    }

    #[test]
    fn result_permission_is_independent_and_owner_scoped() {
        assert!(
            access(Role::Analyst)
                .require(Action::ResultRead, Some("owner"))
                .is_ok()
        );
        assert!(
            access(Role::Analyst)
                .require(Action::ResultRead, Some("other"))
                .is_err()
        );
        for role in [Role::Operator, Role::Viewer] {
            assert!(
                access(role)
                    .require(Action::ResultRead, Some("owner"))
                    .is_err()
            );
        }
        assert!(
            access(Role::Admin)
                .require(Action::ResultRead, Some("other"))
                .is_ok()
        );
        let mut custom = access(Role::Viewer);
        custom.permissions = vec![
            PermissionGrant {
                action: Action::SqlRead,
                scope: Scope::All,
            },
            PermissionGrant {
                action: Action::ProfileRead,
                scope: Scope::All,
            },
        ];
        assert!(custom.require(Action::ResultRead, Some("owner")).is_err());
        custom.permissions.push(PermissionGrant {
            action: Action::ResultRead,
            scope: Scope::All,
        });
        assert!(custom.require(Action::ResultRead, Some("other")).is_ok());
    }

    #[test]
    fn job_resolution_uses_one_complete_envelope_and_is_order_independent() {
        let mut actor = access(Role::Analyst);
        actor.compute = vec![
            job_grant(Some(JobLimits {
                runtime_seconds: 900,
                retention_seconds: 60,
                ..JobLimits::HARD
            })),
            job_grant(Some(JobLimits {
                runtime_seconds: 60,
                retention_seconds: 86400,
                ..JobLimits::HARD
            })),
        ];
        let pool = JobPoolPolicy {
            ceilings: JobLimits::HARD,
            generation: 1,
        };
        let impossible =
            job_request(json!({"timeout_seconds":900,"result":{"retention_seconds":86400}}));
        assert!(
            actor
                .resolve_job_policy(
                    Some("owner"),
                    &job_placement(),
                    &impossible,
                    &JobDefaults::default(),
                    JobLimits::HARD,
                    &pool
                )
                .is_err()
        );
        let omitted = job_request(json!({}));
        let first = actor
            .resolve_job_policy(
                Some("owner"),
                &job_placement(),
                &omitted,
                &JobDefaults::default(),
                JobLimits::HARD,
                &pool,
            )
            .unwrap();
        actor.compute.reverse();
        assert_eq!(
            first,
            actor
                .resolve_job_policy(
                    Some("owner"),
                    &job_placement(),
                    &omitted,
                    &JobDefaults::default(),
                    JobLimits::HARD,
                    &pool
                )
                .unwrap()
        );
        assert_eq!(
            (first.limits.runtime_seconds, first.limits.retention_seconds),
            (300, 60)
        );
    }

    #[test]
    fn job_defaults_are_conservative_and_retained_policy_is_not_rewritten() {
        let mut actor = access(Role::Analyst);
        actor.compute = vec![job_grant(None)];
        let mut pool = JobPoolPolicy {
            ceilings: JobLimits::HARD,
            generation: 1,
        };
        let accepted = actor
            .resolve_job_policy(
                Some("owner"),
                &job_placement(),
                &job_request(json!({})),
                &JobDefaults::default(),
                JobLimits::HARD,
                &pool,
            )
            .unwrap();
        assert_eq!(accepted.limits.runtime_seconds, 60);
        pool.generation = 99;
        assert!(
            actor
                .require_job(
                    Some("owner"),
                    &job_placement(),
                    &accepted,
                    JobLimits::HARD,
                    &pool
                )
                .is_ok()
        );
        pool.ceilings.runtime_seconds = 30;
        assert!(
            actor
                .require_job(
                    Some("owner"),
                    &job_placement(),
                    &accepted,
                    JobLimits::HARD,
                    &pool
                )
                .is_err()
        );
        let privileged = Access::bootstrap_for_test();
        assert!(
            privileged
                .require_job(None, &job_placement(), &accepted, JobLimits::HARD, &pool)
                .is_err()
        );
        assert_eq!(accepted.limits.runtime_seconds, 60);
    }

    #[test]
    fn maintenance_requires_exact_server_template() {
        let op = TrustedOperation::LakeMaintenance {
            expire_older_than_days: Some(7),
            compact: true,
            cleanup: false,
        };
        assert!(op.validate_sql(&op.canonical_sql().unwrap()).is_ok());
        assert!(op.validate_sql("SELECT 1").is_err());
        assert!(
            TrustedOperation::LakeMaintenance {
                expire_older_than_days: Some(3651),
                compact: false,
                cleanup: false
            }
            .validate()
            .is_err()
        );
    }
}
