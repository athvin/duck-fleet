//! PostgreSQL queue authority. No DuckDB connection or execution lives here.
use crate::{
    ApiError, audit,
    authz::{self, Action, TrustedOperation},
    credentials::{ActorKind, Identity},
    failure, metering,
    pricing::RateCard,
    query::{AdmissionLimits, Protocol, QueryRequest, WorkerLimits},
    requests,
};
use anyhow::Result;
use postgres::{Client, GenericClient, Row, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct WorkerEpoch {
    pub worker_id: String,
    pub boot_id: Uuid,
}
#[derive(Clone, Debug)]
pub(crate) struct SessionRegistration {
    pub session_id: Uuid,
    pub epoch: WorkerEpoch,
    pub principal: String,
    pub authority: Identity,
    pub context: Value,
    pub compute: Value,
    pub placement: Value,
    pub scope: String,
    pub protocol: Protocol,
    pub open_span_id: Uuid,
    pub operation: TrustedOperation,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Actor {
    pub principal: String,
    pub credential_id: Option<String>,
    pub kind: ActorKind,
    pub namespace_id: String,
    pub principal_id: Option<String>,
}
impl Actor {
    pub fn identity(&self) -> Identity {
        Identity {
            principal: self.principal.clone(),
            credential_id: self.credential_id.clone(),
            expires_at: None,
            kind: self.kind,
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Payload {
    pub session_id: Uuid,
    pub epoch: WorkerEpoch,
    pub principal: String,
    pub actor: Actor,
    pub request: QueryRequest,
    pub compute: Value,
    pub placement: Value,
    pub scope: String,
    pub protocol: Protocol,
    pub sequence: i64,
    pub namespace_id: String,
    pub owner_principal_id: Option<String>,
    pub operation: TrustedOperation,
}
#[derive(Debug)]
pub(crate) enum SubmitResult {
    Accepted {
        request_id: Uuid,
        sequence: i64,
        authority_generation: i64,
    },
    Duplicate(Value),
    Refused {
        request_id: Uuid,
        error: ApiError,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct Selected {
    pub payload: Payload,
    pub version: i64,
}
pub(crate) struct Prepared {
    pub selected: Selected,
    pub admission: metering::Admission,
}
pub(crate) struct ExecutionGrant {
    pub prepared: Prepared,
    pub remaining_queue: Duration,
}
pub(crate) enum GrantResult {
    Granted(Box<ExecutionGrant>),
    Refused { request_id: Uuid, error: ApiError },
}
#[derive(Clone, Debug)]
pub(crate) enum CloseIntent {
    Explicit {
        actor: Identity,
    },
    Policy {
        authority_generation: i64,
        policy_version: i64,
    },
    Credential {
        credential_id: Option<String>,
        authority_generation: i64,
    },
    Idle {
        authority_generation: i64,
        touched_at: SystemTime,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct SessionAuthority {
    pub session_id: Uuid,
    pub identity: Identity,
    pub generation: i64,
    pub touched_at: SystemTime,
    pub outstanding: Option<Uuid>,
    pub closing: bool,
    pub owner_principal_id: Option<String>,
    pub compute: Value,
    pub operation: TrustedOperation,
    pub authority_policy_version: i64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Counts {
    pub queued: u64,
    pub preparing: u64,
    pub executing: u64,
    pub oldest_queued_ms: Option<u64>,
    pub queued_bytes: u64,
}
#[derive(Default)]
pub(crate) struct ControlReport {
    pub close_ready: Vec<Uuid>,
    pub terminal: Vec<(Uuid, Value)>,
    pub cancel_active: Vec<Uuid>,
    pub counts: Counts,
}
pub(crate) struct Finish<'a> {
    pub request_id: Uuid,
    pub epoch: &'a WorkerEpoch,
    pub status: &'a str,
    pub failure: Option<failure::Failure>,
    pub outcome: requests::Outcome,
    pub elapsed_ms: i64,
    pub result_rows: Option<i64>,
    pub error: Option<&'a str>,
    pub engine_started_at: Option<SystemTime>,
    pub profile: Option<&'a str>,
    pub profile_status: &'a str,
    pub metrics: &'a Value,
    pub source: &'a str,
}
#[derive(Clone, Debug)]
pub(crate) struct PolicyPublication {
    pub publication_id: Uuid,
    pub digest: String,
    pub policies: Vec<(String, AdmissionLimits)>,
}

// Public implementation follows. Storage owns every transition; callers never
// update these tables independently or infer execution from a channel/waiter.
pub(crate) fn migrate(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("ALTER TABLE requests DROP CONSTRAINT requests_state_check;
        ALTER TABLE requests ADD CHECK(state IN ('accepted','queued','preparing','executing','terminal'));
        ALTER TABLE requests ADD COLUMN session_id TEXT;
        ALTER TABLE requests ADD COLUMN session_sequence BIGINT;
        ALTER TABLE requests ADD COLUMN worker_boot_id TEXT;
        ALTER TABLE requests ADD COLUMN pool_scope TEXT;
        ALTER TABLE requests ADD COLUMN protocol TEXT;
        ALTER TABLE requests ADD COLUMN actor_principal TEXT;
        ALTER TABLE requests ADD COLUMN credential_id TEXT;
        ALTER TABLE requests ADD COLUMN queued_at TIMESTAMPTZ;
        ALTER TABLE requests ADD COLUMN selected_at TIMESTAMPTZ;
        ALTER TABLE requests ADD COLUMN execution_granted_at TIMESTAMPTZ;
        ALTER TABLE requests ADD COLUMN engine_started_at TIMESTAMPTZ;
        ALTER TABLE requests ADD COLUMN queue_deadline_at TIMESTAMPTZ;
        ALTER TABLE requests ADD COLUMN queue_timeout_seconds BIGINT;
        ALTER TABLE requests ADD COLUMN policy_generation BIGINT;
        ALTER TABLE requests ADD COLUMN state_version BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE requests ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'unknown';
        CREATE INDEX requests_dispatch ON requests(worker_id,worker_boot_id,queued_at,request_id) WHERE state='queued';
        CREATE INDEX requests_queue_expiry ON requests(queue_deadline_at,request_id) WHERE state IN ('queued','preparing');
        CREATE INDEX requests_worker_pending ON requests(worker_id,worker_boot_id,request_id) WHERE state<>'terminal';
        CREATE TABLE admission_policies(scope TEXT PRIMARY KEY,max_queued BIGINT NOT NULL,max_running BIGINT NOT NULL,
            principal_queued BIGINT NOT NULL,principal_running BIGINT NOT NULL,generation BIGINT NOT NULL);
        INSERT INTO admission_policies VALUES ('persistent:small',256,16,16,16,1),('isolated',256,16,16,16,1);
        CREATE TABLE admission_publication(singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
            publication_id TEXT NOT NULL,digest TEXT NOT NULL,generation BIGINT NOT NULL);
        CREATE TABLE admission_scopes(kind TEXT NOT NULL,scope TEXT NOT NULL,principal TEXT NOT NULL,
            PRIMARY KEY(kind,scope,principal));
        CREATE TABLE queue_workers(worker_id TEXT PRIMARY KEY,boot_id TEXT NOT NULL,scope TEXT NOT NULL,
            max_queued BIGINT NOT NULL,max_bytes BIGINT NOT NULL,next_turn BIGINT NOT NULL DEFAULT 0);
        CREATE TABLE queue_sessions(session_id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,boot_id TEXT NOT NULL,
            principal TEXT NOT NULL,authority_principal TEXT NOT NULL,credential_id TEXT,authority_generation BIGINT NOT NULL DEFAULT 0,
            context TEXT NOT NULL,compute TEXT NOT NULL,placement TEXT NOT NULL,scope TEXT NOT NULL,protocol TEXT NOT NULL,
            open_span_id TEXT NOT NULL,next_sequence BIGINT NOT NULL DEFAULT 0,outstanding TEXT,
            closing BOOLEAN NOT NULL DEFAULT false,opened_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            touched_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),closed_at TIMESTAMPTZ);
        CREATE INDEX queue_sessions_live ON queue_sessions(worker_id,boot_id) WHERE closed_at IS NULL;
        CREATE TABLE query_payloads(request_id TEXT PRIMARY KEY REFERENCES requests(request_id),payload TEXT NOT NULL,bytes BIGINT NOT NULL);
        CREATE TABLE query_controls(request_id TEXT PRIMARY KEY REFERENCES requests(request_id),cancel_requested BOOLEAN NOT NULL DEFAULT false,
            requested_at TIMESTAMPTZ,actor TEXT,credential_id TEXT);
        CREATE TABLE query_permits(request_id TEXT PRIMARY KEY REFERENCES requests(request_id),worker_id TEXT NOT NULL,boot_id TEXT NOT NULL,
            scope TEXT NOT NULL,principal TEXT NOT NULL,stage TEXT NOT NULL CHECK(stage IN ('queued','running')),bytes BIGINT NOT NULL);
        CREATE INDEX query_permits_worker ON query_permits(worker_id,stage);
        CREATE INDEX query_permits_pool ON query_permits(scope,stage);
        CREATE INDEX query_permits_principal ON query_permits(scope,principal,stage);
        CREATE TABLE queue_fairness(worker_id TEXT NOT NULL,boot_id TEXT NOT NULL,principal TEXT NOT NULL,last_turn BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY(worker_id,boot_id,principal));
        CREATE TABLE queue_notifications(request_id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,boot_id TEXT NOT NULL,body TEXT NOT NULL);
        CREATE INDEX queue_notifications_worker ON queue_notifications(worker_id,boot_id);
        CREATE TABLE session_events(open_span_id TEXT PRIMARY KEY,session_id TEXT,principal TEXT NOT NULL,worker_id TEXT,
            boot_id TEXT,scope TEXT NOT NULL,opened_at TIMESTAMPTZ,closed_at TIMESTAMPTZ,
            provisioning_ms BIGINT,provisioning_started_at TIMESTAMPTZ,finished_at TIMESTAMPTZ,error TEXT);
        ALTER TABLE query_history ADD COLUMN queued_at TIMESTAMPTZ;
        ALTER TABLE query_history ADD COLUMN selected_at TIMESTAMPTZ;
        ALTER TABLE query_history ADD COLUMN execution_granted_at TIMESTAMPTZ;
        ALTER TABLE query_history ADD COLUMN engine_started_at TIMESTAMPTZ;
        ALTER TABLE query_history ADD COLUMN queue_deadline_at TIMESTAMPTZ;
        ALTER TABLE query_history ADD COLUMN session_sequence BIGINT;
        ALTER TABLE query_history ADD COLUMN worker_boot_id TEXT;
        ALTER TABLE query_history ADD COLUMN pool_scope TEXT;
        ALTER TABLE query_history ADD COLUMN protocol TEXT;
        ALTER TABLE query_history ADD COLUMN open_span_id TEXT;")?;
    Ok(())
}
fn bounded(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='500ms'; SET LOCAL idle_in_transaction_session_timeout='3s'")?;
    Ok(())
}
fn bounded_for(tx: &mut Transaction<'_>, remaining: Duration) -> Result<()> {
    let millis = remaining.as_millis().clamp(1, 3000).to_string();
    let lock = remaining.as_millis().clamp(1, 500).to_string();
    tx.query_one("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$2,true),set_config('idle_in_transaction_session_timeout','3000',true)", &[&millis,&lock])?;
    Ok(())
}
fn lock_scope(tx: &mut Transaction<'_>, kind: &str, scope: &str, principal: &str) -> Result<()> {
    tx.execute("INSERT INTO admission_scopes(kind,scope,principal) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&kind,&scope,&principal])?;
    tx.query_one(
        "SELECT kind FROM admission_scopes WHERE kind=$1 AND scope=$2 AND principal=$3 FOR UPDATE",
        &[&kind, &scope, &principal],
    )?;
    Ok(())
}
fn lock_scopes(tx: &mut Transaction<'_>, worker: &str, scope: &str, principal: &str) -> Result<()> {
    for (kind, key, owner) in [
        ("pool", scope, ""),
        ("principal", scope, principal),
        ("worker", worker, ""),
    ] {
        lock_scope(tx, kind, key, owner)?;
    }
    Ok(())
}
pub(crate) fn register_worker(
    client: &mut Client,
    epoch: &WorkerEpoch,
    scope: &str,
    limits: WorkerLimits,
) -> Result<bool> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    lock_scope(&mut tx, "worker", &epoch.worker_id, "")?;
    tx.execute("INSERT INTO queue_workers(worker_id,boot_id,scope,max_queued,max_bytes) VALUES($1,$2,$3,$4,$5)
        ON CONFLICT(worker_id) DO UPDATE SET boot_id=EXCLUDED.boot_id,scope=EXCLUDED.scope,max_queued=EXCLUDED.max_queued,max_bytes=EXCLUDED.max_bytes,next_turn=0",
        &[&epoch.worker_id,&epoch.boot_id.to_string(),&scope,&i64::from(limits.max_queued_queries),&i64::try_from(limits.max_queued_bytes)?])?;
    let exists = tx
        .query_opt(
            "SELECT scope FROM admission_policies WHERE scope=$1",
            &[&scope],
        )?
        .is_some();
    tx.commit()?;
    Ok(exists)
}
pub(crate) fn register_session(
    client: &mut Client,
    s: &SessionRegistration,
) -> Result<(), ApiError> {
    register_session_dimensions(client, s, None)
}
pub(crate) fn register_session_captured(
    client: &mut Client,
    s: &SessionRegistration,
    dimensions: &crate::metadata::SessionDimensions,
) -> Result<(), ApiError> {
    register_session_dimensions(client, s, Some(dimensions))
}
fn register_session_dimensions(
    client: &mut Client,
    s: &SessionRegistration,
    dimensions: Option<&crate::metadata::SessionDimensions>,
) -> Result<(), ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let canonical = authz::resolve_in(&mut tx, &s.authority)?;
    let owner_id = canonical.actor.principal_id.clone();
    let access = match authz::authorize_execution_for_owner_in(
        &mut tx,
        &s.authority,
        owner_id.as_deref(),
        &s.compute,
        &s.operation,
    ) {
        Ok(access) => access,
        Err(error) => {
            api(audit::append_in(
                &mut tx,
                &audit::Event::new(
                    &canonical,
                    s.operation.action(),
                    "session",
                    Some(&s.session_id.to_string()),
                    "denied",
                    "permission_denied",
                    None,
                ),
            ))?;
            api(tx.commit())?;
            return Err(error);
        }
    };
    let worker = api(tx.query_opt(
        "SELECT scope FROM queue_workers WHERE worker_id=$1 AND boot_id=$2",
        &[&s.epoch.worker_id, &s.epoch.boot_id.to_string()],
    ))?
    .ok_or_else(ApiError::internal)?;
    if worker.get::<_, String>(0) != s.scope {
        return Err(ApiError::bad("session pool differs from its worker"));
    }
    api(tx.execute("INSERT INTO queue_sessions(session_id,worker_id,boot_id,principal,authority_principal,credential_id,context,compute,placement,scope,protocol,open_span_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        &[&s.session_id.to_string(),&s.epoch.worker_id,&s.epoch.boot_id.to_string(),&s.principal,&s.authority.principal,&s.authority.credential_id,
          &s.context.to_string(),&s.compute.to_string(),&s.placement.to_string(),&s.scope,&s.protocol.name(),&s.open_span_id.to_string()]))?;
    api(tx.execute("UPDATE queue_sessions SET namespace_id=$2,owner_principal_id=$3,actor_principal_id=$4,actor_kind=$5,actor_principal=$6,actor_credential_id=$7,
        authority_kind=$5,authority_principal_id=$4,authority_policy_version=$8,operation=$9 WHERE session_id=$1",
        &[&s.session_id.to_string(),&access.actor.namespace_id,&owner_id,&access.actor.principal_id,&access.actor.kind.name(),&access.actor.principal,&access.actor.credential_id,&access.policy_version,&api(serde_json::to_string(&s.operation))?]))?;
    api(tx.execute("INSERT INTO session_events(open_span_id,session_id,principal,worker_id,boot_id,scope,opened_at) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()) ON CONFLICT(open_span_id) DO UPDATE SET session_id=EXCLUDED.session_id,worker_id=EXCLUDED.worker_id,boot_id=EXCLUDED.boot_id,opened_at=EXCLUDED.opened_at",
        &[&s.open_span_id.to_string(),&s.session_id.to_string(),&s.principal,&s.epoch.worker_id,&s.epoch.boot_id.to_string(),&s.scope]))?;
    if let Some(dimensions) = dimensions {
        api(tx.execute(
            "UPDATE queue_sessions SET system_dimensions=$2 WHERE session_id=$1",
            &[
                &s.session_id.to_string(),
                &api(serde_json::to_string(dimensions))?,
            ],
        ))?;
    }
    api(tx.commit())?;
    Ok(())
}
pub(crate) fn session_closed(client: &mut Client, id: Uuid, epoch: &WorkerEpoch) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let n=tx.execute("UPDATE queue_sessions SET closed_at=clock_timestamp(),closing=true WHERE session_id=$1 AND worker_id=$2 AND boot_id=$3 AND outstanding IS NULL",
        &[&id.to_string(),&epoch.worker_id,&epoch.boot_id.to_string()])?;
    anyhow::ensure!(
        n == 1,
        "cannot acknowledge close with an outstanding request"
    );
    tx.execute(
        "UPDATE session_events SET closed_at=clock_timestamp() WHERE session_id=$1",
        &[&id.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}
fn api<T>(value: std::result::Result<T, impl std::fmt::Display>) -> Result<T, ApiError> {
    value.map_err(|error| {
        tracing::warn!(
            error = crate::secrets::Redactor::from_env().scrub(&error.to_string()),
            "queue metadata operation failed"
        );
        ApiError::internal()
    })
}
fn audit_denied(
    tx: &mut Transaction<'_>,
    identity: &Identity,
    action: Action,
    id: &str,
    error: &ApiError,
) -> Result<(), ApiError> {
    if let Ok(access) = authz::resolve_in(tx, identity) {
        api(audit::append_in(
            tx,
            &audit::Event::new(
                &access,
                action,
                "request",
                Some(id),
                "denied",
                error.0.kind,
                Some(id),
            ),
        ))?;
    }
    Ok(())
}
fn payload(row: &Row) -> Result<Payload> {
    Ok(serde_json::from_str(row.get::<_, &str>("payload"))?)
}
fn uuid(value: String) -> Result<Uuid> {
    Ok(Uuid::parse_str(&value)?)
}

pub(crate) fn submit(
    client: &mut Client,
    epoch: &WorkerEpoch,
    session: Uuid,
    mut request: QueryRequest,
    actor: &Identity,
    protocol: Protocol,
) -> Result<SubmitResult, ApiError> {
    request.validate()?;
    let shape = crate::query_shape::capture(&request.sql, &request.parameters);
    let session_id = session.to_string();
    let boot = epoch.boot_id.to_string();
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let lookup=api(tx.query_opt("SELECT scope,principal FROM queue_sessions WHERE session_id=$1 AND worker_id=$2 AND boot_id=$3 AND closed_at IS NULL",
        &[&session_id,&epoch.worker_id,&boot]))?.ok_or_else(||ApiError::new(failure::UNKNOWN_SESSION,"unknown session"))?;
    let scope: String = lookup.get(0);
    let principal: String = lookup.get(1);
    api(lock_scopes(&mut tx, &epoch.worker_id, &scope, &principal))?;
    let s=api(tx.query_opt("SELECT * FROM queue_sessions WHERE session_id=$1 AND worker_id=$2 AND boot_id=$3 AND closed_at IS NULL FOR UPDATE NOWAIT",
        &[&session_id,&epoch.worker_id,&boot]))?.ok_or_else(||ApiError::new(failure::UNKNOWN_SESSION,"unknown session"))?;
    let context: Value = api(serde_json::from_str(s.get::<_, &str>("context")))?;
    if let (Some(q), Some(s)) = (request.context.as_object_mut(), context.as_object()) {
        q.extend(s.clone());
    }
    let compute: Value = api(serde_json::from_str(s.get::<_, &str>("compute")))?;
    let placement: Value = api(serde_json::from_str(s.get::<_, &str>("placement")))?;
    let owner_id = s.get::<_, Option<String>>("owner_principal_id");
    let operation: TrustedOperation = api(serde_json::from_str(s.get::<_, &str>("operation")))?;
    let authorized = operation.validate_sql(&request.sql).and_then(|_| {
        authz::authorize_execution_for_owner_in(
            &mut tx,
            actor,
            owner_id.as_deref(),
            &compute,
            &operation,
        )
    });
    let access = match authorized {
        Ok(access) => access,
        Err(error) => {
            audit_denied(
                &mut tx,
                actor,
                operation.action(),
                &request.request_id.to_string(),
                &error,
            )?;
            api(tx.commit())?;
            return Err(error);
        }
    };
    if s.get::<_, Option<String>>("namespace_id").as_deref()
        != Some(access.actor.namespace_id.as_str())
    {
        return Err(ApiError::new(
            failure::FORBIDDEN,
            "session namespace differs from authenticated namespace",
        ));
    }
    let id = request.request_id;
    let id_text = id.to_string();
    let retry = request.retry_of.map(|v| v.to_string());
    let input = requests::Submission {
        request_id: &id_text,
        retry_of: retry.as_deref(),
        session_id: &session_id,
        sql: &request.sql,
        parameters: &request.parameters,
        context: &request.context,
        timeout_seconds: request.timeout_seconds,
        queue_timeout_seconds: request.queue_timeout_seconds,
        compute: &compute,
        worker_id: &epoch.worker_id,
        placement: &placement,
        principal: &principal,
    };
    if let requests::ClaimResult::Duplicate(value) = requests::claim_authorized_in(
        &mut tx,
        &input,
        "queued",
        &requests::Ownership {
            namespace_id: &access.actor.namespace_id,
            owner_principal_id: owner_id.as_deref(),
            actor_kind: access.actor.kind,
        },
    )? {
        api(tx.commit())?;
        return Ok(SubmitResult::Duplicate(value));
    }
    let dimensions: crate::metadata::SessionDimensions = s
        .get::<_, Option<&str>>("system_dimensions")
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    api(crate::metadata::record_dimensions(
        &mut tx,
        &id_text,
        &dimensions,
        &shape,
        &request.context,
        request.timeout_seconds,
        s.get::<_, &str>("open_span_id"),
    ))?;
    // Metadata exists before a possible terminal admission refusal, but neither
    // identity nor history becomes visible until this short transaction commits.
    api(tx.execute("UPDATE requests SET session_id=$2,worker_boot_id=$3,pool_scope=$4,protocol=$5,actor_principal=$6,credential_id=$7,
        queue_timeout_seconds=$8 WHERE request_id=$1",&[&id_text,&session_id,&boot,&scope,&protocol.name(),&actor.principal,&actor.credential_id,&(request.queue_timeout_seconds as i64)]))?;
    for (table, key) in [("requests", "request_id"), ("query_history", "query_id")] {
        api(tx.execute(&format!("UPDATE {table} SET namespace_id=$2,owner_principal_id=$3,actor_principal_id=$4,actor_kind=$5,actor_principal=$6,actor_credential_id=$7 WHERE {key}=$1"),
            &[&id_text,&access.actor.namespace_id,&owner_id,&access.actor.principal_id,&access.actor.kind.name(),&access.actor.principal,&access.actor.credential_id]))?;
    }
    api(tx.execute(
        "UPDATE requests SET authorization_policy_version=$2 WHERE request_id=$1",
        &[&id_text, &access.policy_version],
    ))?;
    api(tx.execute(
        "INSERT INTO query_controls(request_id) VALUES($1)",
        &[&id_text],
    ))?;
    let sequence: i64 = s
        .get::<_, i64>("next_sequence")
        .checked_add(1)
        .ok_or_else(ApiError::internal)?;
    let p = Payload {
        session_id: session,
        epoch: epoch.clone(),
        principal: principal.clone(),
        actor: Actor {
            principal: actor.principal.clone(),
            credential_id: access.actor.credential_id.clone(),
            kind: access.actor.kind,
            namespace_id: access.actor.namespace_id.clone(),
            principal_id: access.actor.principal_id.clone(),
        },
        request,
        compute,
        placement,
        scope: scope.clone(),
        protocol,
        sequence,
        namespace_id: access.actor.namespace_id.clone(),
        owner_principal_id: owner_id.clone(),
        operation,
    };
    let encoded = api(serde_json::to_string(&p))?;
    let bytes = api(i64::try_from(encoded.len()))?;
    api(tx.execute(
        "INSERT INTO query_payloads(request_id,payload,bytes) VALUES($1,$2,$3)",
        &[&id_text, &encoded, &bytes],
    ))?;
    let policy = api(tx.query_opt("SELECT * FROM admission_policies WHERE scope=$1", &[&scope]))?;
    let busy = s.get::<_, Option<String>>("outstanding").is_some();
    let refusal = if s.get::<_, bool>("closing") {
        Some(failure::SESSION_LOST)
    } else if busy {
        Some(failure::SESSION_BUSY)
    } else if let Some(policy) = policy.as_ref() {
        if bytes > 1_048_576
            || !api(queue_space(
                &mut tx, epoch, &scope, &principal, bytes, policy,
            ))?
        {
            Some(failure::QUEUE_CAPACITY)
        } else {
            None
        }
    } else {
        Some(failure::POLICY_UNAVAILABLE)
    };
    if let Some(failure) = refusal {
        api(terminal_unstarted(
            &mut tx,
            epoch,
            id,
            failure,
            "query admission refused",
            false,
        ))?;
        api(tx.commit())?;
        return Ok(SubmitResult::Refused {
            request_id: id,
            error: ApiError::new(
                failure,
                "query admission refused; inspect this request before creating another attempt",
            ),
        });
    }
    let generation: i64 = policy.ok_or_else(ApiError::internal)?.get("generation");
    api(tx.execute("UPDATE requests SET session_sequence=$2,queued_at=clock_timestamp(),queue_deadline_at=clock_timestamp()+($3::BIGINT*interval '1 second'),policy_generation=$4 WHERE request_id=$1",
        &[&id_text,&sequence,&(p.request.queue_timeout_seconds as i64),&generation]))?;
    api(tx.execute("UPDATE query_history SET queued_at=r.queued_at,queue_deadline_at=r.queue_deadline_at,session_sequence=$2,worker_boot_id=$3,pool_scope=$4,protocol=$5,open_span_id=$6
        FROM requests r WHERE query_id=$1 AND r.request_id=query_id",&[&id_text,&sequence,&boot,&scope,&protocol.name(),&s.get::<_,String>("open_span_id")]))?;
    api(tx.execute("INSERT INTO query_permits(request_id,worker_id,boot_id,scope,principal,stage,bytes) VALUES($1,$2,$3,$4,$5,'queued',$6)",
        &[&id_text,&epoch.worker_id,&boot,&scope,&principal,&bytes]))?;
    let rotate = access.actor.principal_id == owner_id
        && s.get::<_, Option<String>>("actor_kind").as_deref() == Some(access.actor.kind.name());
    let updated = api(tx.query_one(
        "UPDATE queue_sessions SET outstanding=$2,next_sequence=$3,touched_at=clock_timestamp(),
        authority_principal=CASE WHEN $4 THEN $5 ELSE authority_principal END,
        credential_id=CASE WHEN $4 THEN $6 ELSE credential_id END,
        authority_kind=CASE WHEN $4 THEN $7 ELSE authority_kind END,
        authority_principal_id=CASE WHEN $4 THEN $8 ELSE authority_principal_id END,
        authority_policy_version=CASE WHEN $4 THEN $9 ELSE authority_policy_version END,
        authority_generation=authority_generation+CASE WHEN $4 THEN 1 ELSE 0 END
        WHERE session_id=$1 RETURNING authority_generation",
        &[
            &session_id,
            &id_text,
            &sequence,
            &rotate,
            &actor.principal,
            &actor.credential_id,
            &access.actor.kind.name(),
            &access.actor.principal_id,
            &access.policy_version,
        ],
    ))?;
    let authority_generation = updated.get(0);
    api(tx.execute("INSERT INTO queue_fairness(worker_id,boot_id,principal) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&epoch.worker_id,&boot,&principal]))?;
    api(tx.commit())?;
    Ok(SubmitResult::Accepted {
        request_id: id,
        sequence,
        authority_generation,
    })
}
fn queue_space(
    tx: &mut Transaction<'_>,
    epoch: &WorkerEpoch,
    scope: &str,
    principal: &str,
    bytes: i64,
    policy: &Row,
) -> Result<bool> {
    let worker = tx.query_one(
        "SELECT max_queued,max_bytes FROM queue_workers WHERE worker_id=$1 AND boot_id=$2",
        &[&epoch.worker_id, &epoch.boot_id.to_string()],
    )?;
    let used=tx.query_one("SELECT COUNT(*)::BIGINT,COALESCE(SUM(bytes),0)::BIGINT FROM query_permits WHERE worker_id=$1 AND stage='queued'",&[&epoch.worker_id])?;
    if used.get::<_, i64>(0) >= worker.get::<_, i64>(0)
        || used.get::<_, i64>(1).saturating_add(bytes) > worker.get::<_, i64>(1)
    {
        return Ok(false);
    }
    let used=tx.query_one("SELECT COUNT(*)::BIGINT,COUNT(*) FILTER(WHERE principal=$2)::BIGINT FROM query_permits WHERE scope=$1 AND stage='queued'",&[&scope,&principal])?;
    Ok(used.get::<_, i64>(0) < policy.get::<_, i64>("max_queued")
        && used.get::<_, i64>(1) < policy.get::<_, i64>("principal_queued"))
}
pub(crate) fn select_next(
    client: &mut Client,
    epoch: &WorkerEpoch,
) -> Result<Option<Selected>, ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    // One candidate per eligible principal. A principal already at its shared
    // running cap cannot hide an eligible principal behind its many sessions.
    let candidates=api(tx.query("SELECT q.request_id,q.principal,q.pool_scope FROM (
        SELECT DISTINCT ON(r.principal) r.request_id,r.principal,r.pool_scope,r.queued_at
        FROM requests r JOIN admission_policies p ON p.scope=r.pool_scope
        WHERE r.worker_id=$1 AND r.worker_boot_id=$2 AND r.state='queued'
          AND (SELECT COUNT(*) FROM query_permits u WHERE u.scope=r.pool_scope AND u.stage='running')<p.max_running
          AND (SELECT COUNT(*) FROM query_permits u WHERE u.scope=r.pool_scope AND u.principal=r.principal AND u.stage='running')<p.principal_running
        ORDER BY r.principal,r.queued_at,r.request_id) q
        LEFT JOIN queue_fairness f ON f.worker_id=$1 AND f.boot_id=$2 AND f.principal=q.principal
        ORDER BY COALESCE(f.last_turn,0),q.queued_at,q.request_id LIMIT 64",&[&epoch.worker_id,&epoch.boot_id.to_string()]))?;
    for candidate in candidates {
        let id: String = candidate.get(0);
        let principal: String = candidate.get(1);
        let scope: String = candidate.get(2);
        api(lock_scopes(&mut tx, &epoch.worker_id, &scope, &principal))?;
        let Some(p) = api(tx.query_opt(
            "SELECT p.payload FROM query_payloads p JOIN requests r USING(request_id)
            WHERE r.request_id=$1 AND r.state='queued'",
            &[&id],
        ))?
        else {
            continue;
        };
        let p = api(payload(&p))?;
        let session = api(tx.query_one(
            "SELECT closing,outstanding FROM queue_sessions WHERE session_id=$1 FOR UPDATE NOWAIT",
            &[&p.session_id.to_string()],
        ))?;
        let control = api(tx.query_one(
            "SELECT cancel_requested FROM query_controls WHERE request_id=$1 FOR UPDATE NOWAIT",
            &[&id],
        ))?;
        let request=api(tx.query_one("SELECT state,queue_deadline_at<=clock_timestamp() AS expired FROM requests WHERE request_id=$1 FOR UPDATE NOWAIT",&[&id]))?;
        if request.get::<_, String>(0) != "queued" {
            continue;
        }
        let refusal = if session.get::<_, bool>(0) {
            Some(failure::SESSION_LOST)
        } else if control.get::<_, bool>(0) {
            Some(failure::CANCELED)
        } else if request.get::<_, bool>(1) {
            Some(failure::QUEUE_TIMEOUT)
        } else {
            None
        };
        if let Some(failure) = refusal {
            api(terminal_unstarted(
                &mut tx,
                epoch,
                p.request.request_id,
                failure,
                "waiting query did not execute",
                true,
            ))?;
            api(tx.commit())?;
            return Ok(None);
        }
        let policy = api(tx.query_one(
            "SELECT max_running,principal_running FROM admission_policies WHERE scope=$1",
            &[&scope],
        ))?;
        let usage=api(tx.query_one("SELECT COUNT(*)::BIGINT,COUNT(*) FILTER(WHERE principal=$2)::BIGINT FROM query_permits WHERE scope=$1 AND stage='running'",&[&scope,&principal]))?;
        if usage.get::<_, i64>(0) >= policy.get::<_, i64>(0)
            || usage.get::<_, i64>(1) >= policy.get::<_, i64>(1)
        {
            continue;
        }
        // Exactly one selected/executing statement occupies this worker, even if
        // a caller accidentally runs two selectors on independent connections.
        if api(tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM query_permits WHERE worker_id=$1 AND stage='running')",
            &[&epoch.worker_id],
        ))?
        .get::<_, bool>(0)
        {
            break;
        }
        let version=api(tx.query_one("UPDATE requests SET state='preparing',status='preparing',selected_at=clock_timestamp(),state_version=state_version+1 WHERE request_id=$1 RETURNING state_version",&[&id]))?.get(0);
        api(tx.execute("UPDATE query_history SET status='preparing',selected_at=clock_timestamp() WHERE query_id=$1",&[&id]))?;
        api(tx.execute(
            "UPDATE query_permits SET stage='running' WHERE request_id=$1",
            &[&id],
        ))?;
        let turn:i64=api(tx.query_one("UPDATE queue_workers SET next_turn=next_turn+1 WHERE worker_id=$1 AND boot_id=$2 RETURNING next_turn",&[&epoch.worker_id,&epoch.boot_id.to_string()]))?.get(0);
        api(tx.execute("UPDATE queue_fairness SET last_turn=$4 WHERE worker_id=$1 AND boot_id=$2 AND principal=$3",&[&epoch.worker_id,&epoch.boot_id.to_string(),&principal,&turn]))?;
        api(tx.commit())?;
        return Ok(Some(Selected {
            payload: p,
            version,
        }));
    }
    api(tx.commit())?;
    Ok(None)
}
fn stored_refusal(row: &Row) -> ApiError {
    let kind: Option<String> = row.get("failure_kind");
    ApiError::new(
        kind.as_deref()
            .and_then(failure::by_kind)
            .unwrap_or(failure::UNAVAILABLE),
        "request no longer eligible to execute; inspect its retained outcome",
    )
}
pub(crate) fn reserve(
    client: &mut Client,
    selected: Selected,
    rate: &RateCard,
) -> Result<Prepared, ApiError> {
    let id = selected.payload.request.request_id.to_string();
    let result = (|| {
        let mut tx = api(client.transaction())?;
        api(bounded(&mut tx))?;
        // Budget contention is bounded independently of the short scope locks.
        api(tx.batch_execute("SET LOCAL lock_timeout = '3000ms'"))?;
        let row = api(tx.query_one(
            "SELECT state,state_version,failure_kind FROM requests WHERE request_id=$1 FOR UPDATE",
            &[&id],
        ))?;
        if row.get::<_, String>(0) != "preparing" || row.get::<_, i64>(1) != selected.version {
            return Err(stored_refusal(&row));
        }
        let cpu = selected.payload.compute["cpu"]
            .as_u64()
            .and_then(|v| u16::try_from(v).ok())
            .ok_or_else(ApiError::internal)?;
        let memory = selected.payload.compute["memory_bytes"]
            .as_u64()
            .ok_or_else(ApiError::internal)?;
        let resources = crate::compute::Resources {
            cpu,
            memory_bytes: memory,
            disk_bytes: selected.payload.compute["disk_bytes"]
                .as_u64()
                .ok_or_else(ApiError::internal)?,
        };
        resources.usable().map_err(|_| ApiError::internal())?;
        let admission = metering::admit_in(
            &mut tx,
            &id,
            resources.units(),
            selected.payload.request.timeout_seconds,
            &selected.payload.epoch.worker_id,
            rate,
        )?;
        api(tx.commit())?;
        Ok(admission)
    })();
    match result {
        Ok(admission) => Ok(Prepared {
            selected,
            admission,
        }),
        Err(error) => {
            // No execution grant exists. The budget transaction has rolled back
            // or committed ambiguously; inspect/release any saved hold atomically.
            let mut tx = api(client.transaction())?;
            api(bounded(&mut tx))?;
            api(terminal_unstarted(
                &mut tx,
                &selected.payload.epoch,
                selected.payload.request.request_id,
                error.0,
                &error.1,
                true,
            ))?;
            api(tx.commit())?;
            Err(error)
        }
    }
}
pub(crate) fn grant(client: &mut Client, prepared: Prepared) -> Result<GrantResult, ApiError> {
    let p = &prepared.selected.payload;
    let id = p.request.request_id.to_string();
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let session = api(tx.query_one(
        "SELECT closing,boot_id,outstanding FROM queue_sessions WHERE session_id=$1 FOR UPDATE",
        &[&p.session_id.to_string()],
    ))?;
    let control = api(tx.query_one(
        "SELECT cancel_requested FROM query_controls WHERE request_id=$1 FOR UPDATE",
        &[&id],
    ))?;
    // Earlier session/control lock waits are already reflected in DB remaining.
    let started = Instant::now();
    let r=api(tx.query_one("SELECT state,state_version,failure_kind,FLOOR(EXTRACT(EPOCH FROM(queue_deadline_at-clock_timestamp()))*1000)::BIGINT AS remaining FROM requests WHERE request_id=$1 FOR UPDATE",&[&id]))?;
    let mut refusal = if r.get::<_, String>("state") != "preparing"
        || r.get::<_, i64>("state_version") != prepared.selected.version
    {
        Some(stored_refusal(&r))
    } else if session.get::<_, bool>(0)
        || session.get::<_, String>(1) != p.epoch.boot_id.to_string()
        || session.get::<_, Option<String>>(2).as_deref() != Some(&id)
    {
        Some(ApiError::new(
            failure::SESSION_LOST,
            "session is closing or lost",
        ))
    } else if control.get::<_, bool>(0) {
        Some(ApiError::new(
            failure::CANCELED,
            "query canceled before execution",
        ))
    } else if r.get::<_, i64>("remaining") <= 0 {
        Some(ApiError::new(
            failure::QUEUE_TIMEOUT,
            "query queue deadline expired",
        ))
    } else {
        None
    };
    if refusal.is_none() {
        refusal = p
            .operation
            .validate_sql(&p.request.sql)
            .and_then(|_| {
                authz::authorize_execution_for_owner_in(
                    &mut tx,
                    &p.actor.identity(),
                    p.owner_principal_id.as_deref(),
                    &p.compute,
                    &p.operation,
                )
            })
            .err();
        if let Some(error) = &refusal {
            if error.0 == failure::UNAUTHORIZED {
                // Accepted work already has an immutable authenticated actor.
                // A revoked token cannot erase that evidence from denial audit.
                // The helper isolates an audit failure with a savepoint: the
                // no-execution refusal and zero settlement still commit.
                if !matches!(
                    audit::append_accepted_denial_in(
                        &mut tx,
                        &id,
                        p.operation.action(),
                        error.0.kind
                    ),
                    Ok(true)
                ) {
                    tracing::warn!("accepted query denied; audit persistence unavailable");
                }
            } else {
                audit_denied(
                    &mut tx,
                    &p.actor.identity(),
                    p.operation.action(),
                    &id,
                    error,
                )?;
            }
        }
    }
    if let Some(error) = refusal {
        api(terminal_unstarted(
            &mut tx,
            &p.epoch,
            p.request.request_id,
            error.0,
            &error.1,
            true,
        ))?;
        api(tx.commit())?;
        return Ok(GrantResult::Refused {
            request_id: p.request.request_id,
            error,
        });
    }
    let remaining = Duration::from_millis(u64::try_from(r.get::<_, i64>("remaining")).unwrap_or(0));
    api(tx.execute("UPDATE requests SET state='executing',status='running',execution_granted_at=clock_timestamp(),state_version=state_version+1 WHERE request_id=$1",&[&id]))?;
    api(tx.execute("UPDATE query_history SET status='running',execution_granted_at=clock_timestamp() WHERE query_id=$1",&[&id]))?;
    api(tx.commit())?;
    Ok(GrantResult::Granted(Box::new(ExecutionGrant {
        prepared,
        remaining_queue: remaining.saturating_sub(started.elapsed()),
    })))
}
/// All terminal writers take the same session → control → request order.
/// The initial MVCC read cannot classify completion; the final locked read does.
fn lock_attempt(
    tx: &mut Transaction<'_>,
    epoch: &WorkerEpoch,
    id: Uuid,
) -> Result<Option<(String, Row)>> {
    let id = id.to_string();
    let Some(row) = tx.query_opt("SELECT session_id,state FROM requests WHERE request_id=$1 AND worker_id=$2 AND worker_boot_id=$3", &[&id,&epoch.worker_id,&epoch.boot_id.to_string()])? else {return Ok(None)};
    if row.get::<_, String>(1) == "terminal" {
        return Ok(None);
    }
    let session: String = row.get(0);
    tx.query_one(
        "SELECT session_id FROM queue_sessions WHERE session_id=$1 FOR UPDATE",
        &[&session],
    )?;
    tx.query_one(
        "SELECT request_id FROM query_controls WHERE request_id=$1 FOR UPDATE",
        &[&id],
    )?;
    let row = tx.query_one(
        "SELECT state FROM requests WHERE request_id=$1 FOR UPDATE",
        &[&id],
    )?;
    Ok(Some((session, row)))
}

/// The caller bounds its transaction. Preserve that timeout so a maintenance
/// pass can use less than the normal three-second metadata limit.
pub(crate) fn finish_in(tx: &mut Transaction<'_>, finish: &Finish<'_>) -> Result<bool> {
    let id = finish.request_id.to_string();
    let Some((session, row)) = lock_attempt(tx, finish.epoch, finish.request_id)? else {
        return Ok(false);
    };
    if row.get::<_, String>(0) == "terminal" {
        return Ok(false);
    }
    requests::finish_in(
        tx,
        &id,
        finish.status,
        finish.failure.map(|f| f.kind),
        finish.failure.map(|f| f.retry_safe),
        finish.elapsed_ms,
        finish.result_rows,
        finish.outcome,
    )?;
    tx.execute("UPDATE requests SET engine_started_at=$2,state_version=state_version+1 WHERE request_id=$1",&[&id,&finish.engine_started_at])?;
    let context=tx.query_one("UPDATE query_history SET status=$2,finished_at=clock_timestamp(),elapsed_ms=$3,result_rows=$4,error=$5,failure_kind=$6,retry_safe=$7,
        engine_started_at=$8,profile=$9,profile_status=$10,metrics=$11 WHERE query_id=$1 RETURNING context",
        &[&id,&finish.status,&finish.elapsed_ms,&finish.result_rows,&finish.error,&finish.failure.map(|f|f.kind),&finish.failure.map(|f|f.retry_safe),
          &finish.engine_started_at,&finish.profile,&finish.profile_status,&finish.metrics.to_string()])?.get::<_,String>(0);
    crate::metadata::record_metrics(tx, &id, finish.metrics, finish.profile_status == "captured")?;
    metering::settle_saved_in(
        tx,
        &id,
        finish.elapsed_ms,
        &finish.epoch.worker_id,
        &context,
        finish.source,
    )?;
    tx.execute("DELETE FROM query_permits WHERE request_id=$1", &[&id])?;
    tx.execute("DELETE FROM query_payloads WHERE request_id=$1", &[&id])?;
    tx.execute("UPDATE queue_sessions SET outstanding=NULL,touched_at=clock_timestamp() WHERE session_id=$1 AND outstanding=$2",&[&session,&id])?;
    Ok(true)
}
fn terminal_unstarted(
    tx: &mut Transaction<'_>,
    epoch: &WorkerEpoch,
    id: Uuid,
    failure: failure::Failure,
    message: &str,
    notify: bool,
) -> Result<bool> {
    let Some((session, row)) = lock_attempt(tx, epoch, id)? else {
        return Ok(false);
    };
    if !matches!(row.get::<_, String>(0).as_str(), "queued" | "preparing") {
        // A stale selector/reservation handle is never evidence that execution
        // did not start. Only the engine owner or dead-worker recovery may end it.
        return Ok(false);
    }
    let (failure, message) = if failure == failure::UNAVAILABLE
        || failure == failure::BUDGET_EXHAUSTED
    {
        let current = tx.query_one("SELECT s.closing,c.cancel_requested,r.queue_deadline_at<=clock_timestamp() FROM requests r JOIN queue_sessions s ON s.session_id=$2 JOIN query_controls c ON c.request_id=r.request_id WHERE r.request_id=$1", &[&id.to_string(),&session])?;
        if current.get::<_, bool>(0) {
            (failure::SESSION_LOST, "session closed before execution")
        } else if current.get::<_, bool>(1) {
            (failure::CANCELED, "query canceled before execution")
        } else if current.get::<_, bool>(2) {
            (failure::QUEUE_TIMEOUT, "query queue deadline expired")
        } else {
            (failure, message)
        }
    } else {
        (failure, message)
    };
    let changed = finish_in(
        tx,
        &Finish {
            request_id: id,
            epoch,
            status: failure.status.unwrap_or("refused"),
            failure: Some(failure),
            outcome: requests::Outcome::NotExecuted,
            elapsed_ms: 0,
            result_rows: None,
            error: Some(message),
            engine_started_at: None,
            profile: None,
            profile_status: "not_recorded",
            metrics: &json!({}),
            source: "queue",
        },
    )?;
    if changed && notify {
        let body = json!({"request_id":id,"error":message,"failure_kind":failure.kind,"retry_safe":failure.retry_safe,"status":failure.status.unwrap_or("refused"),"outcome":"not_executed","acceptance":"accepted"});
        tx.execute("INSERT INTO queue_notifications(request_id,worker_id,boot_id,body) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",&[&id.to_string(),&epoch.worker_id,&epoch.boot_id.to_string(),&body.to_string()])?;
    }
    Ok(changed)
}
pub(crate) fn request_cancel(
    client: &mut Client,
    id: Uuid,
    actor: &Identity,
) -> Result<Value, ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    // MVCC read, not FOR UPDATE: budget preparation owns the request row, but
    // cancellation must still commit its separate control latch immediately.
    let access = authz::resolve_in(&mut tx, actor)?;
    let owner=api(tx.query_opt("SELECT COALESCE(r.owner_principal_id,l.principal_id) FROM requests r LEFT JOIN authz_legacy_owners l
        ON r.owner_principal_id IS NULL AND r.namespace_id IS NULL AND l.namespace_id=$2 AND l.legacy_name=r.principal
        WHERE r.request_id=$1 AND COALESCE(r.namespace_id,$2)=$2",&[&id.to_string(),&access.actor.namespace_id]))?
        .ok_or_else(||ApiError::new(failure::NOT_FOUND,"request not found"))?.get::<_,Option<String>>(0);
    if let Err(error) = access.require(Action::QueryCancel, owner.as_deref()) {
        let refusal = if owner.is_some() && owner == access.actor.principal_id {
            error
        } else {
            ApiError::new(failure::NOT_FOUND, "request not found")
        };
        persist_cancel_denial(tx, &access, id);
        return Err(refusal);
    }
    // Finish takes this same control latch before changing state. Once we hold
    // it, re-read the request so a completion that won the race stays immutable.
    api(tx.query_opt(
        "SELECT request_id FROM query_controls WHERE request_id=$1 FOR UPDATE",
        &[&id.to_string()],
    ))?;
    // The latch can wait behind terminal persistence. Authorize again at the
    // mutation boundary rather than spending a grant that changed during wait.
    let access = authz::resolve_in(&mut tx, actor)?;
    if let Err(error) = access.require(Action::QueryCancel, owner.as_deref()) {
        let refusal = if owner.is_some() && owner == access.actor.principal_id {
            error
        } else {
            ApiError::new(failure::NOT_FOUND, "request not found")
        };
        persist_cancel_denial(tx, &access, id);
        return Err(refusal);
    }
    let r = api(tx.query_one(
        "SELECT principal,state,status,outcome FROM requests WHERE request_id=$1",
        &[&id.to_string()],
    ))?;
    let terminal = r.get::<_, String>(1) == "terminal";
    if !terminal {
        let changed=api(tx.execute("UPDATE query_controls SET cancel_requested=true,requested_at=COALESCE(requested_at,clock_timestamp()),actor=COALESCE(actor,$2),credential_id=COALESCE(credential_id,$3) WHERE request_id=$1",
            &[&id.to_string(),&actor.principal,&actor.credential_id]))?;
        if changed != 1 {
            return Err(ApiError::internal());
        }
    }
    let requested = api(tx.query_opt(
        "SELECT cancel_requested FROM query_controls WHERE request_id=$1",
        &[&id.to_string()],
    ))?
    .is_some_and(|row| row.get(0));
    api(audit::append_in(
        &mut tx,
        &audit::Event::new(
            &access,
            Action::QueryCancel,
            "request",
            Some(&id.to_string()),
            "allowed",
            if terminal {
                "already_terminal"
            } else {
                "cancel_requested"
            },
            Some(&id.to_string()),
        ),
    ))?;
    api(tx.commit())?;
    Ok(
        json!({"request_id":id,"state":r.get::<_,String>(1),"status":r.get::<_,String>(2),"outcome":r.get::<_,String>(3),
        "cancel_requested":requested,"cancellation_confirmed":terminal&&r.get::<_,String>(2)=="canceled"&&r.get::<_,String>(3)=="not_executed"}),
    )
}
fn persist_cancel_denial(mut tx: Transaction<'_>, access: &authz::Access, id: Uuid) {
    let appended = audit::append_in(
        &mut tx,
        &audit::Event::new(
            access,
            Action::QueryCancel,
            "request",
            Some(&id.to_string()),
            "denied",
            "permission_denied",
            Some(&id.to_string()),
        ),
    )
    .is_ok();
    if !appended || tx.commit().is_err() {
        // The denial stays opaque even when the audit store is unavailable.
        tracing::warn!("query cancellation denied; audit persistence unavailable");
    }
}
fn authority_identity(row: &Row) -> Result<Identity, ApiError> {
    let kind = match row.get::<_, &str>("authority_kind") {
        "named" => ActorKind::Named,
        "bootstrap" => ActorKind::Bootstrap,
        "internal" => ActorKind::Internal,
        _ => return Err(ApiError::internal()),
    };
    Ok(Identity {
        principal: row.get("authority_principal"),
        credential_id: row.get("credential_id"),
        expires_at: None,
        kind,
    })
}
pub(crate) fn begin_close(
    client: &mut Client,
    id: Uuid,
    epoch: &WorkerEpoch,
    intent: &CloseIntent,
) -> Result<bool, ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let Some(row)=api(tx.query_opt("SELECT * FROM queue_sessions WHERE session_id=$1 AND worker_id=$2 AND boot_id=$3 AND closed_at IS NULL FOR UPDATE",&[&id.to_string(),&epoch.worker_id,&epoch.boot_id.to_string()]))? else {return Ok(false)};
    let owner = row.get::<_, Option<String>>("owner_principal_id");
    let matches = match intent {
        CloseIntent::Explicit { actor } => {
            let access = authz::resolve_in(&mut tx, actor)?;
            if row.get::<_, Option<String>>("namespace_id").as_deref()
                != Some(access.actor.namespace_id.as_str())
            {
                return Err(ApiError::new(failure::NOT_FOUND, "session not found"));
            }
            if let Err(error) = access.require(Action::SessionClose, owner.as_deref()) {
                api(audit::append_in(
                    &mut tx,
                    &audit::Event::new(
                        &access,
                        Action::SessionClose,
                        "session",
                        Some(&id.to_string()),
                        "denied",
                        "permission_denied",
                        None,
                    ),
                ))?;
                api(tx.commit())?;
                return Err(error);
            }
            api(audit::append_in(
                &mut tx,
                &audit::Event::new(
                    &access,
                    Action::SessionClose,
                    "session",
                    Some(&id.to_string()),
                    "allowed",
                    "session_close_requested",
                    None,
                ),
            ))?;
            true
        }
        CloseIntent::Credential {
            credential_id,
            authority_generation,
        } => {
            row.get::<_, Option<String>>("credential_id") == *credential_id
                && row.get::<_, i64>("authority_generation") == *authority_generation
        }
        CloseIntent::Policy {
            authority_generation,
            policy_version,
        } => {
            if row.get::<_, i64>("authority_generation") != *authority_generation {
                return Ok(false);
            }
            if let Some(id) = row.get::<_, Option<String>>("authority_principal_id") {
                authz::policy_fence_in(&mut tx, &id)?;
            }
            let identity = authority_identity(&row)?;
            let compute: Value = api(serde_json::from_str(row.get::<_, &str>("compute")))?;
            let operation: TrustedOperation =
                api(serde_json::from_str(row.get::<_, &str>("operation")))?;
            match authz::authorize_execution_for_owner_in(
                &mut tx,
                &identity,
                owner.as_deref(),
                &compute,
                &operation,
            ) {
                Ok(_) => false,
                Err(error) if error.0 == failure::UNAUTHORIZED || error.0 == failure::FORBIDDEN => {
                    // A new policy version is permitted only if fresh validation
                    // still denies it; a completed regrant always wins this CAS.
                    let _observed_policy_version = policy_version;
                    true
                }
                Err(error) => return Err(error),
            }
        }
        CloseIntent::Idle {
            authority_generation,
            touched_at,
        } => {
            row.get::<_, i64>("authority_generation") == *authority_generation
                && row.get::<_, SystemTime>("touched_at") == *touched_at
                && row.get::<_, Option<String>>("outstanding").is_none()
        }
    };
    if !matches {
        return Ok(false);
    }
    api(tx.execute(
        "UPDATE queue_sessions SET closing=true WHERE session_id=$1",
        &[&id.to_string()],
    ))?;
    api(tx.commit())?;
    Ok(true)
}
pub(crate) fn authorities(
    client: &mut Client,
    epoch: &WorkerEpoch,
) -> Result<Vec<SessionAuthority>> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let rows=tx.query("SELECT * FROM queue_sessions WHERE worker_id=$1 AND boot_id=$2 AND closed_at IS NULL ORDER BY session_id LIMIT 1024",&[&epoch.worker_id,&epoch.boot_id.to_string()])?;
    let result = rows
        .into_iter()
        .map(|row| {
            Ok(SessionAuthority {
                session_id: uuid(row.get("session_id"))?,
                identity: authority_identity(&row).map_err(|e| anyhow::anyhow!("{}", e.1))?,
                generation: row.get("authority_generation"),
                touched_at: row.get("touched_at"),
                outstanding: row
                    .get::<_, Option<String>>("outstanding")
                    .map(uuid)
                    .transpose()?,
                closing: row.get("closing"),
                owner_principal_id: row.get("owner_principal_id"),
                compute: serde_json::from_str(row.get::<_, &str>("compute"))?,
                operation: serde_json::from_str(row.get::<_, &str>("operation"))?,
                authority_policy_version: row.get("authority_policy_version"),
            })
        })
        .collect();
    tx.commit()?;
    result
}
fn finalize_candidates(
    client: &mut Client,
    epoch: Option<&WorkerEpoch>,
    draining: bool,
    batch: usize,
    budget: Duration,
) -> Result<u64> {
    let started = Instant::now();
    if budget < Duration::from_millis(1) {
        return Ok(0);
    }
    let mut tx = client.transaction()?;
    bounded_for(&mut tx, budget.saturating_sub(started.elapsed()))?;
    let worker = epoch.map(|e| e.worker_id.as_str());
    let boot = epoch.map(|e| e.boot_id.to_string());
    let rows=tx.query("SELECT r.request_id,r.worker_id,r.worker_boot_id FROM requests r
        JOIN query_controls c USING(request_id) JOIN queue_sessions s ON s.session_id=r.session_id
        WHERE r.state IN ('queued','preparing') AND ($1::TEXT IS NULL OR (r.worker_id=$1 AND r.worker_boot_id=$2))
        AND (r.queue_deadline_at<=clock_timestamp() OR ($1::TEXT IS NOT NULL AND (c.cancel_requested OR s.closing OR $3)))
        ORDER BY r.queue_deadline_at,r.request_id LIMIT $4",&[&worker,&boot,&draining,&i64::try_from(batch.min(256))?])?;
    tx.commit()?;
    let mut changed = 0;
    for row in rows {
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining < Duration::from_millis(1) {
            break;
        }
        let id: String = row.get(0);
        let owner = WorkerEpoch {
            worker_id: row.get(1),
            boot_id: uuid(row.get(2))?,
        };
        let mut tx = client.transaction()?;
        bounded_for(&mut tx, remaining)?;
        // SKIP LOCKED never establishes completion; a busy preparation is retried
        // next pass. Acquiring the session first retains one global lock order.
        let Some(s)=tx.query_opt("SELECT s.closing FROM queue_sessions s JOIN requests r ON r.session_id=s.session_id WHERE r.request_id=$1 FOR UPDATE OF s SKIP LOCKED",&[&id])? else {continue};
        let Some(c)=tx.query_opt("SELECT cancel_requested FROM query_controls WHERE request_id=$1 FOR UPDATE SKIP LOCKED",&[&id])? else {continue};
        let Some(r)=tx.query_opt("SELECT state,queue_deadline_at<=clock_timestamp() AS expired FROM requests WHERE request_id=$1 FOR UPDATE SKIP LOCKED",&[&id])? else {continue};
        if !matches!(r.get::<_, &str>(0), "queued" | "preparing") {
            continue;
        }
        let failure = if draining {
            Some(failure::QUEUE_DRAINED)
        } else if s.get::<_, bool>(0) {
            Some(failure::SESSION_LOST)
        } else if c.get::<_, bool>(0) {
            Some(failure::CANCELED)
        } else if r.get::<_, bool>(1) {
            Some(failure::QUEUE_TIMEOUT)
        } else {
            None
        };
        if let Some(failure) = failure {
            changed += u64::from(terminal_unstarted(
                &mut tx,
                &owner,
                uuid(id)?,
                failure,
                "waiting query did not execute",
                true,
            )?);
        }
        tx.commit()?;
    }
    Ok(changed)
}
pub(crate) fn poll_control(
    client: &mut Client,
    epoch: &WorkerEpoch,
    draining: bool,
    batch: usize,
) -> Result<ControlReport> {
    finalize_candidates(client, Some(epoch), draining, batch, Duration::from_secs(3))?;
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let boot = epoch.boot_id.to_string();
    let close_ready=tx.query("SELECT session_id FROM queue_sessions WHERE worker_id=$1 AND boot_id=$2 AND closing AND outstanding IS NULL AND closed_at IS NULL LIMIT $3",
        &[&epoch.worker_id,&boot,&i64::try_from(batch.min(256))?])?.into_iter().map(|r|uuid(r.get(0))).collect::<Result<_>>()?;
    let cancel_active=tx.query("SELECT r.request_id FROM requests r JOIN query_controls c USING(request_id)
        WHERE r.worker_id=$1 AND r.worker_boot_id=$2 AND r.state='executing' AND c.cancel_requested",&[&epoch.worker_id,&boot])?
        .into_iter().map(|r|uuid(r.get(0))).collect::<Result<_>>()?;
    let terminal=tx.query("DELETE FROM queue_notifications WHERE request_id IN (
        SELECT request_id FROM queue_notifications WHERE worker_id=$1 AND boot_id=$2 LIMIT $3 FOR UPDATE SKIP LOCKED)
        RETURNING request_id,body",&[&epoch.worker_id,&boot,&i64::try_from(batch.min(256))?])?
        .into_iter().map(|r|Ok((uuid(r.get(0))?,serde_json::from_str(r.get::<_,&str>(1))?))).collect::<Result<_>>()?;
    let counts = counts(&mut tx, epoch)?;
    tx.commit()?;
    Ok(ControlReport {
        close_ready,
        cancel_active,
        terminal,
        counts,
    })
}
pub(crate) fn counts(client: &mut impl GenericClient, epoch: &WorkerEpoch) -> Result<Counts> {
    let r=client.query_one("SELECT COUNT(*) FILTER(WHERE r.state='queued')::BIGINT,
        COUNT(*) FILTER(WHERE r.state='preparing')::BIGINT,COUNT(*) FILTER(WHERE r.state='executing')::BIGINT,
        FLOOR(EXTRACT(EPOCH FROM(clock_timestamp()-MIN(r.queued_at) FILTER(WHERE r.state='queued')))*1000)::BIGINT,
        COALESCE(SUM(p.bytes) FILTER(WHERE r.state='queued'),0)::BIGINT
        FROM query_permits p JOIN requests r USING(request_id) WHERE p.worker_id=$1 AND p.boot_id=$2",&[&epoch.worker_id,&epoch.boot_id.to_string()])?;
    Ok(Counts {
        queued: u64::try_from(r.get::<_, i64>(0))?,
        preparing: u64::try_from(r.get::<_, i64>(1))?,
        executing: u64::try_from(r.get::<_, i64>(2))?,
        oldest_queued_ms: r.get::<_, Option<i64>>(3).map(|v| v.max(0) as u64),
        queued_bytes: u64::try_from(r.get::<_, i64>(4))?,
    })
}
/// Caller already owns the stable worker identity, or has proved this isolated
/// pod gone. A boot ID is not a lease and this helper never establishes death.
pub(crate) fn recover_worker(
    client: &mut Client,
    epoch: &WorkerEpoch,
    _rate: &RateCard,
) -> Result<u64> {
    let mut recovered = 0;
    for _ in 0..18 {
        let mut tx = client.transaction()?;
        bounded(&mut tx)?;
        let rows=tx.query("SELECT request_id,worker_boot_id FROM requests WHERE worker_id=$1 AND worker_boot_id<>$2 AND state<>'terminal' ORDER BY request_id LIMIT 256",
            &[&epoch.worker_id,&epoch.boot_id.to_string()])?;
        tx.commit()?;
        if rows.is_empty() {
            let mut tx = client.transaction()?;
            bounded(&mut tx)?;
            tx.execute("UPDATE queue_sessions SET closing=true,closed_at=clock_timestamp(),outstanding=NULL WHERE worker_id=$1 AND boot_id<>$2 AND closed_at IS NULL",
                &[&epoch.worker_id,&epoch.boot_id.to_string()])?;
            tx.execute("UPDATE session_events SET closed_at=clock_timestamp() WHERE worker_id=$1 AND boot_id<>$2 AND closed_at IS NULL",&[&epoch.worker_id,&epoch.boot_id.to_string()])?;
            tx.execute(
                "DELETE FROM queue_fairness WHERE worker_id=$1 AND boot_id<>$2",
                &[&epoch.worker_id, &epoch.boot_id.to_string()],
            )?;
            tx.execute(
                "DELETE FROM queue_notifications WHERE worker_id=$1 AND boot_id<>$2",
                &[&epoch.worker_id, &epoch.boot_id.to_string()],
            )?;
            tx.commit()?;
            return Ok(recovered);
        }
        for row in rows {
            let id = uuid(row.get(0))?;
            let old = WorkerEpoch {
                worker_id: epoch.worker_id.clone(),
                boot_id: uuid(row.get(1))?,
            };
            let mut tx = client.transaction()?;
            bounded(&mut tx)?;
            // Owning startup excludes new grants from the prior process. The
            // terminal helper takes session/control/request locks consistently.
            let r = tx.query_one(
                "SELECT state,elapsed_ms FROM requests WHERE request_id=$1",
                &[&id.to_string()],
            )?;
            let unstarted = matches!(r.get::<_, &str>(0), "queued" | "preparing");
            let f = if unstarted {
                failure::SESSION_LOST
            } else {
                failure::INTERRUPTED
            };
            recovered += u64::from(finish_in(
                &mut tx,
                &Finish {
                    request_id: id,
                    epoch: &old,
                    status: f.status.unwrap_or("refused"),
                    failure: Some(f),
                    outcome: if unstarted {
                        requests::Outcome::NotExecuted
                    } else {
                        requests::Outcome::Unknown
                    },
                    elapsed_ms: if unstarted {
                        0
                    } else {
                        r.get::<_, Option<i64>>(1).unwrap_or(0)
                    },
                    result_rows: None,
                    error: Some("worker session lost; retained work was not replayed"),
                    engine_started_at: None,
                    profile: None,
                    profile_status: "not_recorded",
                    metrics: &json!({}),
                    source: "recovery",
                },
            )?);
            tx.commit()?;
        }
    }
    anyhow::bail!("worker recovery exceeded bounded live-queue batches")
}
pub(crate) fn orphan_workers(client: &mut Client, prefix: &str) -> Result<Vec<String>> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let rows=tx.query("SELECT worker_id FROM requests WHERE state<>'terminal' AND starts_with(worker_id,$1)
        UNION SELECT worker_id FROM queue_sessions WHERE closed_at IS NULL AND starts_with(worker_id,$1)
        UNION SELECT worker_id FROM reservations WHERE state='held' AND starts_with(worker_id,$1)",&[&prefix])?;
    tx.commit()?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}
pub(crate) fn recover_isolated(
    client: &mut Client,
    worker_id: &str,
    rate: &RateCard,
) -> Result<u64> {
    let recovered = recover_worker(
        client,
        &WorkerEpoch {
            worker_id: worker_id.to_owned(),
            boot_id: Uuid::nil(),
        },
        rate,
    )?;
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    metering::interrupt_abandoned_history(&mut tx, worker_id)?;
    let held = metering::recover(&mut tx, worker_id, rate)?;
    tx.commit()?;
    Ok(recovered + held)
}
pub(crate) fn expire_batch(client: &mut Client, batch: usize, budget: Duration) -> Result<u64> {
    finalize_candidates(client, None, false, batch, budget)
}
pub(crate) fn publish_policies(
    client: &mut Client,
    publication: &PolicyPublication,
) -> Result<i64> {
    let mut policies = publication.policies.clone();
    policies.sort_by(|a, b| a.0.cmp(&b.0));
    anyhow::ensure!(
        policies.windows(2).all(|pair| pair[0].0 != pair[1].0),
        "duplicate policy scope"
    );
    for (scope, limits) in &policies {
        anyhow::ensure!(
            scope == "isolated"
                || scope
                    .strip_prefix("persistent:")
                    .is_some_and(crate::compute::valid_pool_name),
            "invalid admission policy scope"
        );
        limits.validate()?;
    }
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let generation:i64=tx.query_one("INSERT INTO admission_publication(singleton,publication_id,digest,generation) VALUES(true,$1,$2,1)
        ON CONFLICT(singleton) DO UPDATE SET publication_id=EXCLUDED.publication_id,digest=EXCLUDED.digest,generation=admission_publication.generation+1 RETURNING generation",
        &[&publication.publication_id.to_string(),&publication.digest])?.get(0);
    for (scope, limits) in policies {
        lock_scope(&mut tx, "pool", &scope, "")?;
        tx.execute("INSERT INTO admission_policies(scope,max_queued,max_running,principal_queued,principal_running,generation) VALUES($1,$2,$3,$4,$5,$6)
            ON CONFLICT(scope) DO UPDATE SET max_queued=EXCLUDED.max_queued,max_running=EXCLUDED.max_running,
            principal_queued=EXCLUDED.principal_queued,principal_running=EXCLUDED.principal_running,generation=EXCLUDED.generation",
            &[&scope,&i64::from(limits.max_queued_queries),&i64::from(limits.max_running_queries),&i64::from(limits.max_queued_per_principal),&i64::from(limits.max_running_per_principal),&generation])?;
    }
    tx.commit()?;
    Ok(generation)
}
pub(crate) fn publication_confirmed(
    client: &mut Client,
    publication: &PolicyPublication,
) -> Result<bool> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let found=tx.query_opt("SELECT publication_id FROM admission_publication WHERE singleton AND publication_id=$1 AND digest=$2",&[&publication.publication_id.to_string(),&publication.digest])?.is_some();
    tx.commit()?;
    Ok(found)
}

/// Provisioning diagnostics never authorize query execution. Missing writes leave
/// unknown timestamps rather than fabricating a completed span.
pub(crate) fn begin_open(
    client: &mut Client,
    span: Uuid,
    principal: &str,
    scope: &str,
) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    tx.execute("INSERT INTO session_events(open_span_id,principal,scope,provisioning_started_at) VALUES($1,$2,$3,clock_timestamp()) ON CONFLICT DO NOTHING",&[&span.to_string(),&principal,&scope])?;
    tx.commit()?;
    Ok(())
}
pub(crate) fn finish_open(
    client: &mut Client,
    span: Uuid,
    session: Option<Uuid>,
    elapsed_ms: i64,
    error: Option<&str>,
) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    tx.execute("UPDATE session_events SET session_id=COALESCE($2,session_id),
        provisioning_ms=CASE WHEN provisioning_started_at IS NOT NULL THEN $3::BIGINT ELSE NULL END,
        finished_at=CASE WHEN provisioning_started_at IS NOT NULL THEN clock_timestamp() ELSE NULL END,
        error=$4 WHERE open_span_id=$1",&[&span.to_string(),&session.map(|id|id.to_string()),&elapsed_ms,&error])?;
    tx.commit()?;
    Ok(())
}
