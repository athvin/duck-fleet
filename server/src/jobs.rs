//! Durable standalone work. Acceptance owns a queue permit before any engine or
//! pod exists; a retained request identity never authorizes SQL replay.
use crate::{
    ApiError,
    authz::{self, Action, TrustedOperation},
    compute::{Mode, Placement},
    credentials::Identity,
    failure,
    job_policy::{
        AcceptedJobPolicy, JobLimits, JobPoolPolicy, RetainedJobOptions, ValidatedJobRequest,
    },
    metadata,
    pools::PoolRegistry,
    query::{Protocol, QueryRequest},
    queue::{self, Actor, WorkerEpoch},
    requests,
};
use anyhow::Result;
use postgres::{Client, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

pub(crate) fn api<T>(r: Result<T, impl std::fmt::Display>) -> Result<T, ApiError> {
    r.map_err(|_| ApiError::internal())
}
pub(crate) fn bounded(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("SET LOCAL statement_timeout=3000; SET LOCAL lock_timeout=500; SET LOCAL idle_in_transaction_session_timeout=3000")?;
    Ok(())
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WorkTarget {
    UnboundJob,
    Session {
        session_id: Uuid,
        epoch: WorkerEpoch,
    },
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BindingToken {
    pub request_id: Uuid,
    pub generation: i64,
    pub dispatcher_owner: Uuid,
    pub session_id: Uuid,
    pub open_span_id: Uuid,
}
#[derive(Clone, Debug)]
pub(crate) struct BindingClaim {
    pub bound: bool,
    pub principal_id: Option<String>,
    pub token: BindingToken,
    pub placement: Placement,
    pub policy: AcceptedJobPolicy,
    pub target: Option<BindingTarget>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BindingTarget {
    pub endpoint: Option<String>,
    pub pod_name: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionToken {
    pub request_id: Uuid,
    pub binding_generation: i64,
    pub artifact_generation: Uuid,
    pub epoch: WorkerEpoch,
    pub state_version: i64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Execution {
    pub token: ExecutionToken,
    pub policy: AcceptedJobPolicy,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingPayload {
    target: WorkTarget,
    request: QueryRequest,
    actor: Actor,
}
pub(crate) enum SubmitResult {
    Accepted(Value),
    Duplicate(Value),
    Refused(ApiError),
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum BindingFailure {
    NotOpened,
    Unknown,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum SqlCompletion {
    Succeeded,
    Failed,
}
pub(crate) enum ResultDisposition {
    Ready(crate::job_artifacts::VerifiedManifest),
    Failed(&'static str),
    None,
}

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("ALTER TABLE requests ADD COLUMN work_kind TEXT NOT NULL DEFAULT 'interactive' CHECK(work_kind IN ('interactive','job'));
        ALTER TABLE requests ALTER COLUMN worker_id DROP NOT NULL;
        ALTER TABLE requests ADD CHECK(work_kind='job' OR worker_id IS NOT NULL);
        ALTER TABLE query_history ADD COLUMN work_kind TEXT NOT NULL DEFAULT 'interactive' CHECK(work_kind IN ('interactive','job'));
        ALTER TABLE query_history ALTER COLUMN worker_id DROP NOT NULL;
        ALTER TABLE query_history ALTER COLUMN session_id DROP NOT NULL;
        ALTER TABLE query_history ADD CHECK(work_kind='job' OR (worker_id IS NOT NULL AND session_id IS NOT NULL));
        ALTER TABLE query_permits ALTER COLUMN worker_id DROP NOT NULL;
        ALTER TABLE query_permits ALTER COLUMN boot_id DROP NOT NULL;
        ALTER TABLE query_permits ADD CHECK((worker_id IS NULL)=(boot_id IS NULL));
        ALTER TABLE query_permits ADD CHECK(stage='queued' OR worker_id IS NOT NULL);
        ALTER TABLE admission_policies ADD COLUMN job_limits TEXT NOT NULL DEFAULT '{\"runtime_seconds\":3600,\"result_bytes\":1073741824,\"result_rows\":10000000,\"retention_seconds\":86400}';
        ALTER TABLE reservations ADD COLUMN deadline_seconds BIGINT;
        CREATE TABLE query_jobs(request_id TEXT PRIMARY KEY REFERENCES requests(request_id),intent_options_json TEXT NOT NULL,
            policy_json TEXT NOT NULL,resolved_compute_json TEXT NOT NULL,binding_state TEXT NOT NULL DEFAULT 'unbound' CHECK(binding_state IN ('unbound','opening','bound','closed')),
            binding_generation BIGINT NOT NULL DEFAULT 0,dispatcher_owner TEXT,dispatcher_lease_until TIMESTAMPTZ,
            artifact_generation TEXT NOT NULL,sql_completion TEXT NOT NULL DEFAULT 'pending' CHECK(sql_completion IN ('pending','succeeded','failed','not_executed')),
            sql_finished_at TIMESTAMPTZ,occupied_checkpoint_ms BIGINT NOT NULL DEFAULT 0 CHECK(occupied_checkpoint_ms>=0),
            result_state TEXT NOT NULL DEFAULT 'pending' CHECK(result_state IN ('pending','writing','ready','failed','expired')),
            result_expires_at TIMESTAMPTZ,manifest_digest TEXT,result_rows BIGINT,result_bytes BIGINT,result_failure_kind TEXT);
        CREATE INDEX jobs_dispatch ON query_jobs(binding_state,dispatcher_lease_until,request_id) WHERE binding_state IN ('unbound','opening');
        CREATE INDEX jobs_expire ON query_jobs(result_expires_at,request_id) WHERE result_state='ready';
        CREATE TABLE job_bindings(request_id TEXT NOT NULL REFERENCES requests(request_id),generation BIGINT NOT NULL,
            session_id TEXT NOT NULL UNIQUE,open_span_id TEXT NOT NULL,endpoint TEXT,pod_name TEXT,pod_uid TEXT,
            worker_id TEXT,boot_id TEXT,opened_at TIMESTAMPTZ,closed_at TIMESTAMPTZ,cleanup_confirmed_at TIMESTAMPTZ,last_probed_at TIMESTAMPTZ,cleanup_next_probe_at TIMESTAMPTZ,
            state TEXT NOT NULL DEFAULT 'intent' CHECK(state IN ('intent','bound','closed')),
            PRIMARY KEY(request_id,generation));
        CREATE INDEX job_binding_live ON job_bindings(worker_id,boot_id,request_id) WHERE state<>'closed';
        CREATE TABLE job_dispatch_fairness(scope TEXT NOT NULL,principal TEXT NOT NULL,last_turn BIGINT NOT NULL DEFAULT 0,PRIMARY KEY(scope,principal));
        CREATE SEQUENCE job_dispatch_turn;
        INSERT INTO platform_maintenance_tasks(task,next_due_at) VALUES('artifacts',clock_timestamp());")?;
    crate::job_artifacts::migrate(tx)?;
    Ok(())
}
pub(crate) fn scope(placement: &Placement) -> String {
    match placement.mode {
        Mode::Isolated => "isolated".into(),
        Mode::Persistent => format!(
            "persistent:{}",
            placement.pool.as_deref().unwrap_or("small")
        ),
    }
}
pub(crate) fn flat_compute(placement: &Placement) -> Value {
    json!({"mode":placement.mode.name(),"pool":placement.pool,"authority":placement.authority.name(),
        "cpu":placement.resources.cpu,"memory_bytes":placement.resources.memory_bytes,
        "disk_bytes":placement.resources.disk_bytes})
}
pub(crate) fn pool_policy(
    tx: &mut Transaction<'_>,
    scope: &str,
) -> Result<JobPoolPolicy, ApiError> {
    let row = api(tx.query_opt(
        "SELECT job_limits,generation FROM admission_policies WHERE scope=$1",
        &[&scope],
    ))?
    .ok_or_else(|| ApiError::new(failure::POLICY_UNAVAILABLE, "job pool policy unavailable"))?;
    let ceilings: JobLimits = api(serde_json::from_str(row.get::<_, &str>(0)))?;
    api(ceilings.validate())?;
    Ok(JobPoolPolicy {
        ceilings,
        generation: row.get(1),
    })
}
fn fingerprint(
    request: &ValidatedJobRequest,
    placement: &Placement,
    policy: &AcceptedJobPolicy,
) -> Result<String, ApiError> {
    let r = request.request();
    requests::hash_payload(
        b"duckfleet-job-v4\0",
        &json!({"work_kind":"job",
        "statement":{"sql":r.sql,"parameters":r.parameters,"context":r.context},"requested":{"compute":{"mode":request.options().compute.mode,"pool":request.options().compute.pool,"cpu":request.options().compute.cpu,"memory_bytes":request.options().compute.memory_bytes,"disk_bytes":request.options().compute.disk_bytes},"timeout_seconds":request.options().timeout_seconds,"queue_timeout_seconds":request.options().queue_timeout_seconds,"result":{"format":request.options().result.format,"retention_seconds":request.options().result.retention_seconds,"max_bytes":request.options().result.max_bytes,"max_rows":request.options().result.max_rows}},
        "effective":{"compute":flat_compute(placement),"limits":policy.limits,"queue_timeout_seconds":policy.queue_timeout_seconds,"format":policy.format}}),
    )
}
fn saved(
    tx: &mut Transaction<'_>,
    id: &str,
) -> Result<(RetainedJobOptions, Placement, AcceptedJobPolicy), ApiError> {
    let row=api(tx.query_opt("SELECT intent_options_json,resolved_compute_json,policy_json FROM query_jobs WHERE request_id=$1",&[&id]))?
        .ok_or_else(||ApiError::new(failure::REQUEST_ID_MISMATCH,"request_id belongs to another work kind"))?;
    Ok((
        api(serde_json::from_str(row.get::<_, &str>(0)))?,
        api(serde_json::from_str(row.get::<_, &str>(1)))?,
        api(serde_json::from_str(row.get::<_, &str>(2)))?,
    ))
}
fn mismatch() -> ApiError {
    ApiError::new(
        failure::REQUEST_ID_MISMATCH,
        "job options differ from the retained attempt; SQL was not replayed",
    )
}
fn accepted(id: Uuid) -> Value {
    json!({"request_id":id,"acceptance":"accepted","status_url":format!("/v1/jobs/{id}"),"cancel_url":format!("/v1/jobs/{id}/cancel"),"result_url":format!("/v1/jobs/{id}/result")})
}

pub(crate) fn submit(
    client: &mut Client,
    actor: &Identity,
    request: ValidatedJobRequest,
    registry: &PoolRegistry,
    enabled: bool,
) -> Result<SubmitResult, ApiError> {
    let shape = crate::query_shape::capture(&request.request().sql, &request.request().parameters);
    let id = request.request().request_id;
    let id_text = id.to_string();
    let retry = request.request().retry_of.map(|v| v.to_string());
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let access = authz::resolve_in(&mut tx, actor)?;
    let ownership = requests::Ownership {
        namespace_id: &access.actor.namespace_id,
        owner_principal_id: access.actor.principal_id.as_deref(),
        actor_kind: access.actor.kind,
    };
    if let Some(stored) = requests::load_for_claim(&mut tx, &id_text, &ownership)? {
        access.require(Action::QueryExecute, access.actor.principal_id.as_deref())?;
        let (options, placement, policy) = saved(&mut tx, &id_text)?;
        let current = pool_policy(&mut tx, &scope(&placement))?;
        access.require_job(
            access.actor.principal_id.as_deref(),
            &placement,
            &policy,
            JobLimits::HARD,
            &current,
        )?;
        if options != *request.options() {
            return Err(mismatch());
        }
        let value = stored.job_duplicate(
            &fingerprint(&request, &placement, &policy)?,
            retry.as_deref(),
        )?;
        api(tx.commit())?;
        return Ok(SubmitResult::Duplicate(value));
    }
    if !enabled {
        return Err(ApiError::new(
            failure::JOB_UNAVAILABLE,
            "job result storage is unavailable",
        ));
    }
    let (placement, policy, logical) = if let Some(parent_id) = retry.as_deref() {
        if parent_id == id_text {
            return Err(ApiError::new(
                failure::INVALID_RETRY,
                "retry_of must name a prior attempt",
            ));
        }
        let parent = requests::load_for_claim(&mut tx, parent_id, &ownership)?
            .ok_or_else(|| ApiError::new(failure::NOT_FOUND, "request not found"))?;
        let (options, placement, mut policy) = saved(&mut tx, parent_id)?;
        let current = pool_policy(&mut tx, &scope(&placement))?;
        access.require_job(
            access.actor.principal_id.as_deref(),
            &placement,
            &policy,
            JobLimits::HARD,
            &current,
        )?;
        if options != *request.options() {
            return Err(mismatch());
        }
        let logical = parent.job_parent(&fingerprint(&request, &placement, &policy)?)?;
        policy.policy_generation = current.generation;
        (placement, policy, logical)
    } else {
        let placement = registry.resolve(&request.request().compute)?;
        let current = pool_policy(&mut tx, &scope(&placement))?;
        let policy = access.resolve_job_policy(
            access.actor.principal_id.as_deref(),
            &placement,
            &request,
            &registry.job_defaults,
            registry.job_ceiling,
            &current,
        )?;
        (placement, policy, id_text.clone())
    };
    let scope = scope(&placement);
    api(queue::lock_scopes_in(
        &mut tx,
        None,
        &scope,
        &access.actor.principal,
    ))?;
    // Admission waits may outlive a policy change; require fresh accepted bounds.
    let access = authz::resolve_in(&mut tx, actor)?;
    let current = pool_policy(&mut tx, &scope)?;
    access.require_job(
        access.actor.principal_id.as_deref(),
        &placement,
        &policy,
        JobLimits::HARD,
        &current,
    )?;
    let hash = fingerprint(&request, &placement, &policy)?;
    let inserted=api(tx.execute("INSERT INTO requests(request_id,principal,logical_request_id,retry_of,fingerprint_version,payload_hash,state,status,outcome,worker_id,work_kind,
        namespace_id,owner_principal_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id,authorization_policy_version,pool_scope,protocol,queued_at,queue_deadline_at,queue_timeout_seconds,policy_generation)
        VALUES($1,$2,$3,$4,4,$5,'queued','queued','unknown',NULL,'job',$6,$7,$7,$8,$2,$9,$10,$11,'http',clock_timestamp(),clock_timestamp()+($12::BIGINT*interval '1 second'),$12,$13) ON CONFLICT DO NOTHING",
        &[&id_text,&access.actor.principal,&logical,&retry,&hash,&access.actor.namespace_id,&access.actor.principal_id,&access.actor.kind.name(),&access.actor.credential_id,&access.policy_version,&scope,&(policy.queue_timeout_seconds as i64),&policy.policy_generation]))?;
    if inserted == 0 {
        // Reload the winner with its immutable defaults, not our losing resolution.
        api(tx.rollback())?;
        let mut probe = api(client.transaction())?;
        api(bounded(&mut probe))?;
        let a = authz::resolve_in(&mut probe, actor)?;
        let own = requests::Ownership {
            namespace_id: &a.actor.namespace_id,
            owner_principal_id: a.actor.principal_id.as_deref(),
            actor_kind: a.actor.kind,
        };
        let winner = requests::load_for_claim(&mut probe, &id_text, &own)?;
        let Some(winner) = winner else {
            return Err(ApiError::new(
                failure::INVALID_RETRY,
                "prior attempt already has a retry",
            ));
        };
        let (options, p, accepted) = saved(&mut probe, &id_text)?;
        let current = pool_policy(&mut probe, &scope_for(&p))?;
        a.require_job(
            a.actor.principal_id.as_deref(),
            &p,
            &accepted,
            JobLimits::HARD,
            &current,
        )?;
        if options != *request.options() {
            return Err(mismatch());
        }
        let value =
            winner.job_duplicate(&fingerprint(&request, &p, &accepted)?, retry.as_deref())?;
        api(probe.commit())?;
        return Ok(SubmitResult::Duplicate(value));
    }
    let r = request.request();
    api(tx.execute("INSERT INTO query_history(query_id,session_id,sql,context,status,worker_id,placement,principal,work_kind,namespace_id,owner_principal_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id,pool_scope,protocol,queued_at,queue_deadline_at)
        SELECT request_id,NULL,$2,$3,status,NULL,'{}',principal,'job',namespace_id,owner_principal_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id,pool_scope,protocol,queued_at,queue_deadline_at FROM requests WHERE request_id=$1",&[&id_text,&r.sql,&r.context.to_string()]))?;
    api(tx.execute("INSERT INTO query_jobs(request_id,intent_options_json,policy_json,resolved_compute_json,artifact_generation) VALUES($1,$2,$3,$4,$5)",
        &[&id_text,&api(serde_json::to_string(request.options()))?,&api(serde_json::to_string(&policy))?,&api(serde_json::to_string(&placement))?,&Uuid::new_v4().to_string()]))?;
    api(tx.execute(
        "INSERT INTO query_controls(request_id) VALUES($1)",
        &[&id_text],
    ))?;
    let pending = PendingPayload {
        target: WorkTarget::UnboundJob,
        request: QueryRequest {
            request_id: id,
            retry_of: r.retry_of,
            sql: r.sql.clone(),
            parameters: r.parameters.clone(),
            context: r.context.clone(),
            timeout_seconds: policy.limits.runtime_seconds,
            queue_timeout_seconds: policy.queue_timeout_seconds,
        },
        actor: Actor {
            principal: access.actor.principal.clone(),
            credential_id: access.actor.credential_id.clone(),
            kind: access.actor.kind,
            namespace_id: access.actor.namespace_id.clone(),
            principal_id: access.actor.principal_id.clone(),
        },
    };
    let payload = api(serde_json::to_string(&pending))?;
    let bytes = api(i64::try_from(payload.len()))?;
    let limits = api(tx.query_one(
        "SELECT max_queued,principal_queued FROM admission_policies WHERE scope=$1",
        &[&scope],
    ))?;
    let used=api(tx.query_one("SELECT COUNT(*)::BIGINT,COUNT(*) FILTER(WHERE principal=$2)::BIGINT FROM query_permits WHERE scope=$1 AND stage='queued'",&[&scope,&access.actor.principal]))?;
    let dims = metadata::capture_session(&r.compute, &placement, "", Some(&registry.revision))?;
    api(metadata::record_dimensions(
        &mut tx,
        &id_text,
        &dims,
        &shape,
        &r.context,
        policy.limits.runtime_seconds,
        "",
    ))?;
    api(tx.execute(
        "UPDATE requests SET open_span_id=NULL WHERE request_id=$1",
        &[&id_text],
    ))?;
    if payload.len() > crate::job_policy::MAX_JOB_PAYLOAD_BYTES
        || used.get::<_, i64>(0) >= limits.get::<_, i64>(0)
        || used.get::<_, i64>(1) >= limits.get::<_, i64>(1)
    {
        api(requests::finish_in(
            &mut tx,
            &id_text,
            "refused",
            Some(failure::QUEUE_CAPACITY.kind),
            Some(false),
            0,
            None,
            requests::Outcome::NotExecuted,
        ))?;
        api(tx.execute("UPDATE query_history SET status='refused',finished_at=clock_timestamp(),failure_kind=$2,retry_safe=false,elapsed_ms=0 WHERE query_id=$1",&[&id_text,&failure::QUEUE_CAPACITY.kind]))?;
        api(tx.execute("UPDATE query_jobs SET binding_state='closed',sql_completion='not_executed',result_state='failed',result_failure_kind=$2 WHERE request_id=$1",&[&id_text,&failure::QUEUE_CAPACITY.kind]))?;
        api(tx.commit())?;
        return Ok(SubmitResult::Refused(ApiError::new(
            failure::QUEUE_CAPACITY,
            "job queue capacity reached; this attempt did not execute",
        )));
    }
    api(tx.execute(
        "INSERT INTO query_payloads(request_id,payload,bytes) VALUES($1,$2,$3)",
        &[&id_text, &payload, &bytes],
    ))?;
    api(tx.execute("INSERT INTO query_permits(request_id,worker_id,boot_id,scope,principal,stage,bytes) VALUES($1,NULL,NULL,$2,$3,'queued',$4)",&[&id_text,&scope,&access.actor.principal,&bytes]))?;
    api(tx.execute(
        "INSERT INTO job_dispatch_fairness(scope,principal) VALUES($1,$2) ON CONFLICT DO NOTHING",
        &[&scope, &access.actor.principal],
    ))?;
    api(tx.commit())?;
    Ok(SubmitResult::Accepted(accepted(id)))
}
// Kept separate from submit's local variable for clarity at the race reload.
fn scope_for(p: &Placement) -> String {
    scope(p)
}

fn token_row(r: &postgres::Row, owner: Uuid) -> Result<BindingToken> {
    Ok(BindingToken {
        request_id: Uuid::parse_str(r.get::<_, &str>("request_id"))?,
        generation: r.get("binding_generation"),
        dispatcher_owner: owner,
        session_id: Uuid::parse_str(r.get::<_, &str>("session_id"))?,
        open_span_id: Uuid::parse_str(r.get::<_, &str>("open_span_id"))?,
    })
}
/// A crashed dispatcher resumes the same target and generation. Lease expiry is
/// never evidence that an ambiguous Open failed or that a worker died.
pub(crate) fn claim_binding(client: &mut Client, owner: Uuid) -> Result<Option<BindingClaim>> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let candidate=tx.query_opt("SELECT j.request_id,r.pool_scope,r.principal FROM query_jobs j JOIN requests r USING(request_id) LEFT JOIN job_dispatch_fairness f ON f.scope=r.pool_scope AND f.principal=r.principal WHERE r.state='queued' AND r.queue_deadline_at>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM query_controls c WHERE c.request_id=r.request_id AND c.cancel_requested) AND j.binding_state IN ('unbound','opening') AND (j.dispatcher_lease_until IS NULL OR j.dispatcher_lease_until<=clock_timestamp()) ORDER BY COALESCE(f.last_turn,0),r.queued_at,r.request_id LIMIT 1",&[])?;
    let Some(c) = candidate else {
        tx.commit()?;
        return Ok(None);
    };
    let id: String = c.get(0);
    let scope: String = c.get(1);
    let principal: String = c.get(2);
    queue::lock_scopes_in(&mut tx, None, &scope, &principal)?;
    let Some(j)=tx.query_opt("SELECT binding_state,binding_generation FROM query_jobs WHERE request_id=$1 AND binding_state IN ('unbound','opening') AND (dispatcher_lease_until IS NULL OR dispatcher_lease_until<=clock_timestamp()) FOR UPDATE SKIP LOCKED",&[&id])? else{tx.commit()?;return Ok(None)};
    let r=tx.query_one("SELECT state,queue_deadline_at<=clock_timestamp(),cancel_requested FROM requests JOIN query_controls USING(request_id) WHERE request_id=$1",&[&id])?;
    if r.get::<_, &str>(0) != "queued" || r.get::<_, bool>(1) || r.get::<_, bool>(2) {
        tx.commit()?;
        return Ok(None);
    }
    if j.get::<_, &str>(0) == "unbound" {
        let generation = j.get::<_, i64>(1) + 1;
        tx.execute("INSERT INTO job_bindings(request_id,generation,session_id,open_span_id) VALUES($1,$2,$3,$4)",&[&id,&generation,&Uuid::new_v4().to_string(),&Uuid::new_v4().to_string()])?;
        tx.execute("UPDATE query_jobs SET binding_state='opening',binding_generation=$2 WHERE request_id=$1",&[&id,&generation])?;
    }
    tx.execute("UPDATE query_jobs SET dispatcher_owner=$2,dispatcher_lease_until=clock_timestamp()+interval '10 seconds' WHERE request_id=$1",&[&id,&owner.to_string()])?;
    tx.execute("UPDATE job_dispatch_fairness SET last_turn=nextval('job_dispatch_turn') WHERE scope=$1 AND principal=$2",&[&scope,&principal])?;
    let r=tx.query_one("SELECT j.request_id,j.binding_generation,b.session_id,b.open_span_id,b.endpoint,b.pod_name,j.policy_json,j.resolved_compute_json,r.owner_principal_id FROM query_jobs j JOIN job_bindings b ON b.request_id=j.request_id AND b.generation=j.binding_generation JOIN requests r ON r.request_id=j.request_id WHERE j.request_id=$1",&[&id])?;
    let endpoint = r.get::<_, Option<String>>(4);
    let pod_name = r.get::<_, Option<String>>(5);
    let claim = BindingClaim {
        bound: false,
        principal_id: r.get(8),
        token: token_row(&r, owner)?,
        placement: serde_json::from_str(r.get::<_, &str>(7))?,
        policy: serde_json::from_str(r.get::<_, &str>(6))?,
        target: if endpoint.is_some() || pod_name.is_some() {
            Some(BindingTarget { endpoint, pod_name })
        } else {
            None
        },
    };
    tx.commit()?;
    Ok(Some(claim))
}
fn lock_binding(tx: &mut Transaction<'_>, token: &BindingToken) -> Result<postgres::Row> {
    let r=tx.query_one("SELECT binding_state,binding_generation,dispatcher_owner,policy_json,resolved_compute_json,artifact_generation FROM query_jobs WHERE request_id=$1 FOR UPDATE",&[&token.request_id.to_string()])?;
    anyhow::ensure!(
        r.get::<_, i64>(1) == token.generation
            && r.get::<_, Option<String>>(2).as_deref()
                == Some(&token.dispatcher_owner.to_string()),
        "stale dispatcher binding"
    );
    let b = tx.query_one(
        "SELECT session_id,open_span_id FROM job_bindings WHERE request_id=$1 AND generation=$2",
        &[&token.request_id.to_string(), &token.generation],
    )?;
    anyhow::ensure!(
        b.get::<_, String>(0) == token.session_id.to_string()
            && b.get::<_, String>(1) == token.open_span_id.to_string(),
        "binding identity mismatch"
    );
    Ok(r)
}
pub(crate) fn record_binding_target(
    client: &mut Client,
    token: &BindingToken,
    target: BindingTarget,
) -> Result<()> {
    anyhow::ensure!(
        target.endpoint.is_some() || target.pod_name.is_some(),
        "empty binding target"
    );
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let j = lock_binding(&mut tx, token)?;
    anyhow::ensure!(
        j.get::<_, &str>(0) == "opening",
        "binding no longer opening"
    );
    let r = tx.query_one(
        "SELECT endpoint,pod_name FROM job_bindings WHERE request_id=$1 AND generation=$2",
        &[&token.request_id.to_string(), &token.generation],
    )?;
    let old = BindingTarget {
        endpoint: r.get(0),
        pod_name: r.get(1),
    };
    anyhow::ensure!(
        (old.endpoint.is_none() && old.pod_name.is_none())
            || (old.pod_name == target.pod_name
                && (old.endpoint.is_none() || old.endpoint == target.endpoint)),
        "binding target cannot change after network intent"
    );
    tx.execute(
        "UPDATE job_bindings SET endpoint=$3,pod_name=$4 WHERE request_id=$1 AND generation=$2",
        &[
            &token.request_id.to_string(),
            &token.generation,
            &target.endpoint,
            &target.pod_name,
        ],
    )?;
    tx.commit()?;
    Ok(())
}
pub(crate) fn record_pod_uid(client: &mut Client, token: &BindingToken, uid: &str) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    lock_binding(&mut tx, token)?;
    let n=tx.execute("UPDATE job_bindings SET pod_uid=$3 WHERE request_id=$1 AND generation=$2 AND (pod_uid IS NULL OR pod_uid=$3)",&[&token.request_id.to_string(),&token.generation,&uid])?;
    anyhow::ensure!(n == 1, "pod identity changed");
    tx.commit()?;
    Ok(())
}

pub(crate) fn bind_session(
    client: &mut Client,
    token: &BindingToken,
    epoch: &WorkerEpoch,
    observed: &Placement,
    dimensions: &metadata::SessionDimensions,
) -> Result<queue::SessionRegistration, ApiError> {
    let id = token.request_id.to_string();
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let summary = api(tx.query_one(
        "SELECT r.pool_scope,r.principal FROM requests r WHERE request_id=$1",
        &[&id],
    ))?;
    api(queue::lock_scopes_in(
        &mut tx,
        Some(&epoch.worker_id),
        summary.get::<_, &str>(0),
        summary.get::<_, &str>(1),
    ))?;
    let j = api(lock_binding(&mut tx, token))?;
    let placement: Placement = api(serde_json::from_str(j.get::<_, &str>(4)))?;
    if flat_compute(&placement) != flat_compute(observed) {
        return Err(ApiError::bad(
            "job binding placement differs from accepted compute",
        ));
    }
    let policy: AcceptedJobPolicy = api(serde_json::from_str(j.get::<_, &str>(3)))?;
    let r=api(tx.query_one("SELECT r.state,r.queue_deadline_at<=clock_timestamp(),c.cancel_requested,r.owner_principal_id,r.namespace_id,r.principal,r.state_version,p.payload FROM requests r JOIN query_controls c USING(request_id) JOIN query_payloads p USING(request_id) WHERE r.request_id=$1 FOR UPDATE OF r,c",&[&id]))?;
    if r.get::<_, &str>(0) != "queued" || r.get::<_, bool>(1) || r.get::<_, bool>(2) {
        return Err(ApiError::new(
            failure::JOB_UNAVAILABLE,
            "job no longer eligible for binding",
        ));
    }
    let pending: PendingPayload = if j.get::<_, &str>(0) == "bound" {
        let p: queue::Payload = api(serde_json::from_str(r.get::<_, &str>(7)))?;
        if p.epoch != *epoch {
            return Err(ApiError::new(
                failure::JOB_UNAVAILABLE,
                "job already bound to another worker",
            ));
        }
        PendingPayload {
            target: WorkTarget::UnboundJob,
            request: p.request,
            actor: p.actor,
        }
    } else {
        api(serde_json::from_str(r.get::<_, &str>(7)))?
    };
    let identity = pending.actor.identity();
    let access = authz::resolve_in(&mut tx, &identity)?;
    let current = pool_policy(&mut tx, &scope(&placement))?;
    access.require_job(
        r.get::<_, Option<String>>(3).as_deref(),
        &placement,
        &policy,
        JobLimits::HARD,
        &current,
    )?;
    let registration = queue::SessionRegistration {
        session_id: token.session_id,
        epoch: epoch.clone(),
        principal: r.get(5),
        authority: identity,
        context: json!({}),
        compute: flat_compute(&placement),
        placement: json!({"mode":observed.mode.name(),"pool":observed.pool,"resources":observed.resources.describe(),"worker_id":epoch.worker_id,"worker_boot_id":epoch.boot_id,"duckdb_version":dimensions.engine_version}),
        scope: scope(&placement),
        protocol: Protocol::Http,
        open_span_id: token.open_span_id,
        operation: TrustedOperation::Sql,
    };
    if j.get::<_, &str>(0) == "bound" {
        api(tx.commit())?;
        return Ok(registration);
    }
    let worker = api(tx.query_one(
        "SELECT scope,max_queued,max_bytes FROM queue_workers WHERE worker_id=$1 AND boot_id=$2",
        &[&epoch.worker_id, &epoch.boot_id.to_string()],
    ))?;
    if worker.get::<_, String>(0) != registration.scope {
        return Err(ApiError::bad("job worker pool mismatch"));
    }
    let execution = Execution {
        token: ExecutionToken {
            request_id: token.request_id,
            binding_generation: token.generation,
            artifact_generation: api(Uuid::parse_str(j.get::<_, &str>(5)))?,
            epoch: epoch.clone(),
            state_version: r.get(6),
        },
        policy,
    };
    let p = queue::Payload {
        session_id: token.session_id,
        epoch: epoch.clone(),
        principal: registration.principal.clone(),
        actor: pending.actor,
        request: pending.request,
        compute: registration.compute.clone(),
        placement: registration.placement.clone(),
        scope: registration.scope.clone(),
        protocol: Protocol::Http,
        sequence: 1,
        namespace_id: r.get(4),
        owner_principal_id: r.get(3),
        operation: TrustedOperation::Sql,
        job: Some(execution),
    };
    let encoded = api(serde_json::to_string(&p))?;
    let bytes = api(i64::try_from(encoded.len()))?;
    let used=api(tx.query_one("SELECT COUNT(*)::BIGINT,COALESCE(SUM(bytes),0)::BIGINT FROM query_permits WHERE worker_id=$1 AND stage='queued'",&[&epoch.worker_id]))?;
    if used.get::<_, i64>(0) >= worker.get::<_, i64>(1)
        || used.get::<_, i64>(1).saturating_add(bytes) > worker.get::<_, i64>(2)
    {
        return Err(ApiError::new(
            failure::QUEUE_CAPACITY,
            "worker queue is full",
        ));
    }
    api(tx.execute("INSERT INTO queue_sessions(session_id,worker_id,boot_id,principal,authority_principal,credential_id,context,compute,placement,scope,protocol,open_span_id,next_sequence,outstanding,namespace_id,owner_principal_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id,authority_kind,authority_principal_id,authority_policy_version,operation,system_dimensions) VALUES($1,$2,$3,$4,$4,$5,'{}',$6,$7,$8,'http',$9,1,$10,$11,$12,$13,$14,$4,$5,$14,$13,$15,$16,$17)",
        &[&token.session_id.to_string(),&epoch.worker_id,&epoch.boot_id.to_string(),&registration.principal,&registration.authority.credential_id,&registration.compute.to_string(),&registration.placement.to_string(),&registration.scope,&token.open_span_id.to_string(),&id,&p.namespace_id,&p.owner_principal_id,&p.actor.principal_id,&p.actor.kind.name(),&access.policy_version,&api(serde_json::to_string(&TrustedOperation::Sql))?,&api(serde_json::to_string(dimensions))?]))?;
    api(tx.execute("UPDATE query_jobs SET binding_state='bound',dispatcher_lease_until=NULL WHERE request_id=$1",&[&id]))?;
    api(tx.execute("UPDATE job_bindings SET worker_id=$3,boot_id=$4,state='bound',opened_at=clock_timestamp() WHERE request_id=$1 AND generation=$2",&[&id,&token.generation,&epoch.worker_id,&epoch.boot_id.to_string()]))?;
    api(tx.execute("UPDATE requests SET worker_id=$2,worker_boot_id=$3,session_id=$4,session_sequence=1,open_span_id=$5 WHERE request_id=$1",&[&id,&epoch.worker_id,&epoch.boot_id.to_string(),&token.session_id.to_string(),&token.open_span_id.to_string()]))?;
    api(tx.execute(
        "UPDATE requests SET engine_version=$2 WHERE request_id=$1",
        &[&id, &dimensions.engine_version],
    ))?;
    api(tx.execute("UPDATE query_history SET worker_id=$2,worker_boot_id=$3,session_id=$4,session_sequence=1,open_span_id=$5,placement=$6 WHERE query_id=$1",&[&id,&epoch.worker_id,&epoch.boot_id.to_string(),&token.session_id.to_string(),&token.open_span_id.to_string(),&registration.placement.to_string()]))?;
    api(tx.execute(
        "UPDATE query_permits SET worker_id=$2,boot_id=$3,bytes=$4 WHERE request_id=$1",
        &[&id, &epoch.worker_id, &epoch.boot_id.to_string(), &bytes],
    ))?;
    api(tx.execute(
        "UPDATE query_payloads SET payload=$2,bytes=$3 WHERE request_id=$1",
        &[&id, &encoded, &bytes],
    ))?;
    api(tx.execute("INSERT INTO queue_fairness(worker_id,boot_id,principal) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&epoch.worker_id,&epoch.boot_id.to_string(),&registration.principal]))?;
    api(tx.execute("INSERT INTO session_events(open_span_id,session_id,principal,worker_id,boot_id,scope,opened_at) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()) ON CONFLICT(open_span_id) DO UPDATE SET session_id=EXCLUDED.session_id,worker_id=EXCLUDED.worker_id,boot_id=EXCLUDED.boot_id,opened_at=EXCLUDED.opened_at",&[&token.open_span_id.to_string(),&token.session_id.to_string(),&registration.principal,&epoch.worker_id,&epoch.boot_id.to_string(),&registration.scope]))?;
    api(tx.commit())?;
    Ok(registration)
}

pub(crate) fn placement_in(tx: &mut Transaction<'_>, id: Uuid) -> Result<Placement, ApiError> {
    let r = api(tx.query_one(
        "SELECT resolved_compute_json FROM query_jobs WHERE request_id=$1",
        &[&id.to_string()],
    ))?;
    api(serde_json::from_str(r.get::<_, &str>(0)))
}
pub(crate) fn mark_sql_complete(
    client: &mut Client,
    token: &ExecutionToken,
    completion: SqlCompletion,
    occupied_ms: i64,
) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    queue::lock_job_in(&mut tx, &token.request_id.to_string())?;
    let r=tx.query_one("SELECT r.state,r.state_version,r.worker_id,r.worker_boot_id,j.binding_generation,j.artifact_generation,j.sql_completion,j.policy_json FROM requests r JOIN query_jobs j USING(request_id) WHERE request_id=$1 FOR UPDATE OF r",&[&token.request_id.to_string()])?;
    anyhow::ensure!(
        r.get::<_, &str>(0) == "executing"
            && r.get::<_, i64>(1) == token.state_version
            && r.get::<_, Option<String>>(2).as_deref() == Some(&token.epoch.worker_id)
            && r.get::<_, Option<String>>(3).as_deref() == Some(&token.epoch.boot_id.to_string())
            && r.get::<_, i64>(4) == token.binding_generation
            && r.get::<_, String>(5) == token.artifact_generation.to_string(),
        "stale SQL completion token"
    );
    let complete = match completion {
        SqlCompletion::Succeeded => "succeeded",
        SqlCompletion::Failed => "failed",
    };
    let old = r.get::<_, &str>(6);
    anyhow::ensure!(
        old == "pending" || old == complete,
        "SQL completion cannot change"
    );
    let p: AcceptedJobPolicy = serde_json::from_str(r.get::<_, &str>(7))?;
    tx.execute("UPDATE query_jobs SET sql_completion=$2,sql_finished_at=COALESCE(sql_finished_at,clock_timestamp()),result_expires_at=COALESCE(result_expires_at,clock_timestamp()+($3::BIGINT*interval '1 second')),occupied_checkpoint_ms=GREATEST(occupied_checkpoint_ms,$4) WHERE request_id=$1",&[&token.request_id.to_string(),&complete,&i64::try_from(p.limits.retention_seconds)?,&occupied_ms.max(0)])?;
    tx.commit()?;
    Ok(())
}
/// Result publication and accounting share one terminal commit. A previously
/// acknowledged SQL completion always wins over a later delivery/worker failure.
pub(crate) fn finish_in(
    tx: &mut Transaction<'_>,
    finish: &queue::Finish<'_>,
    result: ResultDisposition,
) -> Result<bool> {
    let id = finish.request_id.to_string();
    let j=tx.query_one("SELECT j.sql_completion,j.occupied_checkpoint_ms,j.artifact_generation,r.state FROM query_jobs j JOIN requests r USING(request_id) WHERE request_id=$1 FOR UPDATE OF j",&[&id])?;
    if j.get::<_, &str>(3) == "terminal" {
        return Ok(false);
    }
    let known = j.get::<_, &str>(0) == "succeeded";
    let known_failure = j.get::<_, &str>(0) == "failed";
    let effective_outcome = if known || known_failure {
        requests::Outcome::StatementResult
    } else {
        finish.outcome
    };
    // A grant can occupy a scheduler slot while parsing/authentication refuses
    // before engine entry. Occupied diagnostics never turn that into execution.
    let elapsed = if effective_outcome == requests::Outcome::NotExecuted {
        0
    } else {
        finish.elapsed_ms.max(j.get::<_, i64>(1))
    };
    let normalized = queue::Finish {
        request_id: finish.request_id,
        epoch: finish.epoch,
        status: if known {
            "succeeded"
        } else if known_failure {
            "failed"
        } else {
            finish.status
        },
        failure: if known {
            None
        } else if known_failure {
            Some(failure::SQL_ERROR)
        } else {
            finish.failure
        },
        outcome: effective_outcome,
        elapsed_ms: elapsed,
        result_rows: finish.result_rows,
        error: if known {
            None
        } else if known_failure && finish.source == "recovery" {
            Some("SQL failed; detailed error unavailable after worker loss")
        } else {
            finish.error
        },
        engine_started_at: finish.engine_started_at,
        profile: finish.profile,
        profile_status: finish.profile_status,
        metrics: finish.metrics,
        source: finish.source,
    };
    let changed = queue::finish_common_in(tx, &normalized)?;
    if !changed {
        return Ok(false);
    }
    match result {
        ResultDisposition::Ready(manifest) => {
            anyhow::ensure!(
                known
                    && manifest.request_id == finish.request_id
                    && manifest.generation.to_string() == j.get::<_, String>(2),
                "result publication requires matching successful SQL"
            );
            let valid=tx.query_opt("SELECT request_id FROM job_artifacts WHERE request_id=$1 AND generation=$2 AND sequence=4096 AND state='uploaded' AND sha256=$3",&[&id,&manifest.generation.to_string(),&manifest.digest])?.is_some();
            anyhow::ensure!(valid, "manifest publication is unconfirmed");
            let n=tx.execute("UPDATE query_jobs SET result_state='ready',manifest_digest=$2,result_rows=$3,result_bytes=$4,binding_state='closed' WHERE request_id=$1 AND result_expires_at>clock_timestamp()",&[&id,&manifest.digest,&i64::try_from(manifest.rows)?,&i64::try_from(manifest.bytes)?])?;
            if n == 0 {
                tx.execute("UPDATE query_jobs SET result_state='expired',result_failure_kind='result_expired',binding_state='closed' WHERE request_id=$1",&[&id])?;
                crate::job_artifacts::cleanup_in(tx, &id)?;
            }
        }
        other => {
            let kind = match other {
                ResultDisposition::Failed(k) => k,
                _ => "result_unavailable",
            };
            tx.execute("UPDATE query_jobs SET result_state=CASE WHEN $2='result_expired' THEN 'expired' ELSE 'failed' END,result_failure_kind=$2,binding_state='closed',sql_completion=CASE WHEN sql_completion='pending' AND $3 THEN 'not_executed' ELSE sql_completion END WHERE request_id=$1",&[&id,&kind,&matches!(finish.outcome,requests::Outcome::NotExecuted)])?;
            crate::job_artifacts::cleanup_in(tx, &id)?;
        }
    }
    tx.execute("UPDATE queue_sessions SET closing=true WHERE session_id=(SELECT session_id FROM requests WHERE request_id=$1)",&[&id])?;
    Ok(true)
}
fn finish_unbound_in(tx: &mut Transaction<'_>, id: &str, f: failure::Failure) -> Result<bool> {
    queue::lock_job_in(tx, id)?;
    tx.query_one(
        "SELECT request_id FROM query_controls WHERE request_id=$1 FOR UPDATE",
        &[&id],
    )?;
    let r = tx.query_one(
        "SELECT state,worker_id FROM requests WHERE request_id=$1 FOR UPDATE",
        &[&id],
    )?;
    if r.get::<_, &str>(0) != "queued" || r.get::<_, Option<String>>(1).is_some() {
        return Ok(false);
    }
    requests::finish_in(
        tx,
        id,
        f.status.unwrap_or("refused"),
        Some(f.kind),
        Some(false),
        0,
        None,
        requests::Outcome::NotExecuted,
    )?;
    tx.execute("UPDATE query_history SET status=$2,failure_kind=$3,retry_safe=false,elapsed_ms=0,finished_at=clock_timestamp() WHERE query_id=$1",&[&id,&f.status.unwrap_or("refused"),&f.kind])?;
    tx.execute("UPDATE query_jobs SET sql_completion='not_executed',result_state='failed',result_failure_kind=$2,binding_state='closed' WHERE request_id=$1",&[&id,&f.kind])?;
    tx.execute("DELETE FROM query_payloads WHERE request_id=$1", &[&id])?;
    tx.execute("DELETE FROM query_permits WHERE request_id=$1", &[&id])?;
    Ok(true)
}
pub(crate) fn expire_unbound(client: &mut Client, batch: usize, budget: Duration) -> Result<u64> {
    let start = std::time::Instant::now();
    if budget < Duration::from_millis(1) {
        return Ok(0);
    }
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let rows=tx.query("SELECT r.request_id FROM requests r JOIN query_jobs j USING(request_id) JOIN query_controls c USING(request_id) WHERE r.state='queued' AND r.worker_id IS NULL AND (r.queue_deadline_at<=clock_timestamp() OR c.cancel_requested) ORDER BY r.queue_deadline_at LIMIT $1",&[&i64::try_from(batch.min(64))?])?;
    tx.commit()?;
    let mut count = 0;
    for row in rows {
        let remaining = budget.saturating_sub(start.elapsed());
        if remaining < Duration::from_millis(1) {
            break;
        }
        let mut tx = client.transaction()?;
        bounded(&mut tx)?;
        let ms = remaining.as_millis().min(3000).to_string();
        tx.query_one(
            "SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",
            &[&ms],
        )?;
        let id: String = row.get(0);
        queue::lock_job_in(&mut tx, &id)?;
        let r = tx.query_one(
            "SELECT cancel_requested FROM query_controls WHERE request_id=$1 FOR UPDATE",
            &[&id],
        )?;
        let f = if r.get::<_, bool>(0) {
            failure::CANCELED
        } else {
            failure::QUEUE_TIMEOUT
        };
        count += u64::from(finish_unbound_in(&mut tx, &id, f)?);
        tx.commit()?;
    }
    Ok(count)
}
/// Called only with externally proven predecessor death (ownership guard or
/// absent pod), never from timeouts, queue age, or an unreachable endpoint.
pub(crate) fn recover_queued_in(
    tx: &mut Transaction<'_>,
    id: Uuid,
    old: &WorkerEpoch,
) -> Result<bool> {
    let Some(_) = tx.query_opt(
        "SELECT request_id FROM query_jobs WHERE request_id=$1 FOR UPDATE",
        &[&id.to_string()],
    )?
    else {
        return Ok(false);
    };
    let r=tx.query_one("SELECT r.state,r.session_id,r.worker_id,r.worker_boot_id,EXISTS(SELECT 1 FROM reservations WHERE execution_id=r.request_id) FROM requests r WHERE r.request_id=$1",&[&id.to_string()])?;
    if r.get::<_, &str>(0) != "queued"
        || r.get::<_, bool>(4)
        || r.get::<_, Option<String>>(2).as_deref() != Some(&old.worker_id)
        || r.get::<_, Option<String>>(3).as_deref() != Some(&old.boot_id.to_string())
    {
        return Ok(false);
    }
    let session = r
        .get::<_, Option<String>>(1)
        .ok_or_else(|| anyhow::anyhow!("bound job missing session"))?;
    tx.query_one(
        "SELECT session_id FROM queue_sessions WHERE session_id=$1 FOR UPDATE",
        &[&session],
    )?;
    tx.query_one(
        "SELECT request_id FROM query_controls WHERE request_id=$1 FOR UPDATE",
        &[&id.to_string()],
    )?;
    let current = tx.query_one(
        "SELECT state FROM requests WHERE request_id=$1 FOR UPDATE",
        &[&id.to_string()],
    )?;
    if current.get::<_, &str>(0) != "queued" {
        return Ok(false);
    }
    let row = tx.query_one(
        "SELECT payload FROM query_payloads WHERE request_id=$1",
        &[&id.to_string()],
    )?;
    let p: queue::Payload = serde_json::from_str(row.get::<_, &str>(0))?;
    let pending = PendingPayload {
        target: WorkTarget::UnboundJob,
        request: p.request,
        actor: p.actor,
    };
    let encoded = serde_json::to_string(&pending)?;
    tx.execute("UPDATE query_jobs SET binding_state='unbound',dispatcher_owner=NULL,dispatcher_lease_until=NULL WHERE request_id=$1",&[&id.to_string()])?;
    tx.execute("UPDATE job_bindings SET state='closed',closed_at=clock_timestamp() WHERE request_id=$1 AND state<>'closed'",&[&id.to_string()])?;
    tx.execute("UPDATE queue_sessions SET outstanding=NULL,closing=true,closed_at=clock_timestamp() WHERE session_id=$1",&[&session])?;
    tx.execute("UPDATE requests SET worker_id=NULL,worker_boot_id=NULL,session_id=NULL,session_sequence=NULL,open_span_id=NULL WHERE request_id=$1",&[&id.to_string()])?;
    tx.execute("UPDATE query_history SET worker_id=NULL,worker_boot_id=NULL,session_id=NULL,session_sequence=NULL,open_span_id=NULL WHERE query_id=$1",&[&id.to_string()])?;
    tx.execute(
        "UPDATE query_permits SET worker_id=NULL,boot_id=NULL,bytes=$2 WHERE request_id=$1",
        &[&id.to_string(), &i64::try_from(encoded.len())?],
    )?;
    tx.execute(
        "UPDATE query_payloads SET payload=$2,bytes=$3 WHERE request_id=$1",
        &[&id.to_string(), &encoded, &i64::try_from(encoded.len())?],
    )?;
    Ok(true)
}
#[derive(Clone, Debug)]
pub(crate) struct BindingStatus {
    pub state: String,
    pub pod_uid: Option<String>,
}
pub(crate) fn inspect_binding(client: &mut Client, token: &BindingToken) -> Result<BindingStatus> {
    let r=client.query_one("SELECT j.binding_state,b.worker_id,b.boot_id,b.endpoint,b.pod_name,b.pod_uid FROM query_jobs j JOIN job_bindings b ON b.request_id=j.request_id AND b.generation=$2 WHERE j.request_id=$1",&[&token.request_id.to_string(),&token.generation])?;
    Ok(BindingStatus {
        state: r.get(0),
        pod_uid: r.get(5),
    })
}
pub(crate) fn binding_failed(
    client: &mut Client,
    token: &BindingToken,
    evidence: BindingFailure,
) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let j = lock_binding(&mut tx, token)?;
    if matches!(evidence, BindingFailure::Unknown) {
        tx.execute("UPDATE query_jobs SET dispatcher_lease_until=clock_timestamp()+interval '10 seconds' WHERE request_id=$1",&[&token.request_id.to_string()])?;
        tx.commit()?;
        return Ok(());
    }
    if j.get::<_, &str>(0) == "opening" {
        tx.execute("UPDATE job_bindings SET state='closed',closed_at=clock_timestamp() WHERE request_id=$1 AND generation=$2",&[&token.request_id.to_string(),&token.generation])?;
        tx.execute("UPDATE query_jobs SET binding_state='unbound',dispatcher_owner=NULL,dispatcher_lease_until=clock_timestamp()+interval '1 second' WHERE request_id=$1",&[&token.request_id.to_string()])?;
    } else {
        anyhow::bail!("bound job cannot be declared unopened");
    }
    tx.commit()?;
    Ok(())
}

pub(crate) fn binding_for_worker(
    client: &mut Client,
    token: &BindingToken,
) -> Result<BindingClaim, ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let j = api(lock_binding(&mut tx, token))?;
    let r=api(tx.query_one("SELECT b.endpoint,b.pod_name,b.worker_id,r.actor_principal,r.actor_credential_id,r.actor_kind,r.owner_principal_id FROM job_bindings b JOIN requests r USING(request_id) WHERE b.request_id=$1 AND b.generation=$2",&[&token.request_id.to_string(),&token.generation]))?;
    let bound = r.get::<_, Option<String>>(2).is_some();
    if !bound && j.get::<_, &str>(0) != "opening" {
        return Err(ApiError::new(
            failure::JOB_UNAVAILABLE,
            "job binding is closed",
        ));
    }
    let actor = Identity {
        principal: r.get(3),
        credential_id: r.get(4),
        expires_at: None,
        kind: api(serde_json::from_value(json!(r.get::<_, String>(5))))?,
    };
    let placement: Placement = api(serde_json::from_str(j.get::<_, &str>(4)))?;
    let policy: AcceptedJobPolicy = api(serde_json::from_str(j.get::<_, &str>(3)))?;
    let access = authz::resolve_in(&mut tx, &actor)?;
    let current = pool_policy(&mut tx, &scope(&placement))?;
    access.require_job(
        r.get::<_, Option<String>>(6).as_deref(),
        &placement,
        &policy,
        JobLimits::HARD,
        &current,
    )?;
    let claim = BindingClaim {
        bound,
        principal_id: r.get(6),
        token: token.clone(),
        placement,
        policy,
        target: Some(BindingTarget {
            endpoint: r.get(0),
            pod_name: r.get(1),
        }),
    };
    api(tx.commit())?;
    Ok(claim)
}
pub(crate) fn authorize_execution(
    client: &mut Client,
    execution: &Execution,
) -> Result<(), ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let t = &execution.token;
    let r=api(tx.query_one("SELECT p.payload,r.state,r.state_version,j.binding_generation,j.artifact_generation FROM requests r JOIN query_payloads p USING(request_id) JOIN query_jobs j USING(request_id) WHERE request_id=$1",&[&t.request_id.to_string()]))?;
    let p: queue::Payload = api(serde_json::from_str(r.get::<_, &str>(0)))?;
    if r.get::<_, &str>(1) != "executing"
        || r.get::<_, i64>(2) != t.state_version
        || r.get::<_, i64>(3) != t.binding_generation
        || r.get::<_, String>(4) != t.artifact_generation.to_string()
        || p.epoch != t.epoch
    {
        return Err(ApiError::new(
            failure::JOB_UNAVAILABLE,
            "job execution grant is stale",
        ));
    }
    let access = authz::resolve_in(&mut tx, &p.actor.identity())?;
    let placement = placement_in(&mut tx, t.request_id)?;
    let policy = pool_policy(&mut tx, &p.scope)?;
    let decision = access.require_job(
        p.owner_principal_id.as_deref(),
        &placement,
        &execution.policy,
        JobLimits::HARD,
        &policy,
    );
    if let Err(error) = decision {
        if error.0 == failure::FORBIDDEN {
            let persisted = (|| -> Result<()> {
                let mut audit_tx = tx.transaction()?;
                crate::audit::append_in(
                    &mut audit_tx,
                    &crate::audit::Event::new(
                        &access,
                        Action::QueryExecute,
                        "request",
                        Some(&t.request_id.to_string()),
                        "denied",
                        "job_bounds_denied",
                        Some(&t.request_id.to_string()),
                    ),
                )?;
                audit_tx.commit()?;
                Ok(())
            })();
            if persisted.is_err() {
                tracing::warn!("accepted job envelope denied; audit persistence unavailable");
            }
            if tx.commit().is_err() {
                tracing::warn!("accepted job denial audit commit unavailable");
            }
        }
        return Err(error);
    }
    api(tx.commit())?;
    Ok(())
}
pub(crate) fn detail(
    client: &mut Client,
    identity: &Identity,
    id: Uuid,
) -> Result<Value, ApiError> {
    let mut tx = api(client.transaction())?;
    api(bounded(&mut tx))?;
    let access = authz::resolve_in(&mut tx, identity)?;
    let mut value = requests::detail_in(&mut tx, &access.metadata(), id)?;
    let r=api(tx.query_opt("SELECT sql_completion,result_state,result_expires_at,sql_finished_at,result_failure_kind,result_rows,result_bytes,binding_state,policy_json,(result_expires_at<=clock_timestamp()) FROM query_jobs WHERE request_id=$1",&[&id.to_string()]))?.ok_or_else(||ApiError::new(failure::NOT_FOUND,"job not found"))?;
    let expired = r.get::<_, Option<bool>>(9) == Some(true);
    value["work_kind"] = json!("job");
    value["sql_completion"] = json!(r.get::<_, String>(0));
    value["result_state"] = json!(if expired {
        "expired"
    } else {
        r.get::<_, &str>(1)
    });
    value["result_failure_kind"] = json!(r.get::<_, Option<String>>(4));
    value["result_rows"] = json!(r.get::<_, Option<i64>>(5));
    value["result_bytes"] = json!(r.get::<_, Option<i64>>(6));
    value["binding_state"] = json!(r.get::<_, String>(7));
    value["accepted_policy"] = api(serde_json::from_str(r.get::<_, &str>(8)))?;
    value["result_expires_at"] = json!(
        r.get::<_, Option<SystemTime>>(2)
            .map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339())
    );
    value["sql_finished_at"] = json!(
        r.get::<_, Option<SystemTime>>(3)
            .map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339())
    );
    value["result_url"] = json!(format!("/v1/jobs/{id}/result"));
    api(tx.commit())?;
    Ok(value)
}

pub(crate) fn renew_binding(client: &mut Client, token: &BindingToken) -> Result<bool> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let n=tx.execute("UPDATE query_jobs SET dispatcher_lease_until=clock_timestamp()+interval '10 seconds' WHERE request_id=$1 AND binding_generation=$2 AND dispatcher_owner=$3 AND binding_state='opening' AND dispatcher_lease_until>clock_timestamp()",&[&token.request_id.to_string(),&token.generation,&token.dispatcher_owner.to_string()])?;
    tx.commit()?;
    Ok(n == 1)
}
pub(crate) fn refuse_unbound(
    client: &mut Client,
    token: &BindingToken,
    f: failure::Failure,
) -> Result<bool> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    lock_binding(&mut tx, token)?;
    let changed = finish_unbound_in(&mut tx, &token.request_id.to_string(), f)?;
    if changed
        && matches!(f, failure::UNAUTHORIZED | failure::FORBIDDEN)
        && !matches!(
            crate::audit::append_accepted_denial_in(
                &mut tx,
                &token.request_id.to_string(),
                Action::QueryExecute,
                f.kind
            ),
            Ok(true)
        )
    {
        tracing::warn!("accepted job refused; audit persistence unavailable");
    }
    tx.commit()?;
    Ok(changed)
}

#[derive(Clone, Debug)]
pub(crate) struct PodCleanup {
    pub request_id: Uuid,
    pub generation: i64,
    pub pod_name: String,
    pub pod_uid: Option<String>,
}
pub(crate) fn live_job_pods(client: &mut Client) -> Result<Vec<String>> {
    let rows=client.query("SELECT DISTINCT pod_name FROM job_bindings WHERE pod_name IS NOT NULL AND (cleanup_confirmed_at IS NULL OR pod_uid IS NULL) ORDER BY pod_name LIMIT 65537",&[])?;
    anyhow::ensure!(
        rows.len() <= 65536,
        "job pod protection set exceeds bounded snapshot"
    );
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}
/// Call only after a deletion acknowledgement followed by confirmed absence of
/// this exact pod UID (or absence of the immutable named create intent).
pub(crate) fn confirm_pod_cleanup(client: &mut Client, pod: &PodCleanup) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    queue::lock_job_in(&mut tx, &pod.request_id.to_string())?;
    let n=tx.execute("UPDATE job_bindings b SET cleanup_confirmed_at=clock_timestamp(),cleanup_next_probe_at=CASE WHEN b.pod_uid IS NULL THEN clock_timestamp()+interval '1 day' ELSE NULL END,state='closed',closed_at=COALESCE(closed_at,clock_timestamp()) FROM requests r WHERE b.request_id=$1 AND b.generation=$2 AND b.pod_name=$3 AND b.pod_uid IS NOT DISTINCT FROM $4 AND r.request_id=b.request_id AND (r.state='terminal' OR b.state='closed')",&[&pod.request_id.to_string(),&pod.generation,&pod.pod_name,&pod.pod_uid])?;
    anyhow::ensure!(n == 1, "pod cleanup proof is stale");
    tx.commit()?;
    Ok(())
}
#[derive(Clone, Debug)]
pub(crate) struct PodCandidate {
    pub cleanup: PodCleanup,
    pub terminal: bool,
}
pub(crate) fn binding_pod_candidates(client: &mut Client, limit: u32) -> Result<Vec<PodCandidate>> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let rows=tx.query("SELECT b.request_id,b.generation,b.pod_name,b.pod_uid,(r.state='terminal' OR b.state='closed') FROM job_bindings b JOIN requests r USING(request_id) WHERE b.pod_name IS NOT NULL AND (b.cleanup_confirmed_at IS NULL OR b.pod_uid IS NULL) AND (b.cleanup_next_probe_at IS NULL OR b.cleanup_next_probe_at<=clock_timestamp()) ORDER BY b.last_probed_at NULLS FIRST,b.request_id,b.generation LIMIT $1",&[&i64::from(limit.min(64))])?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let id: String = r.get(0);
        let generation: i64 = r.get(1);
        tx.execute("UPDATE job_bindings SET last_probed_at=clock_timestamp() WHERE request_id=$1 AND generation=$2",&[&id,&generation])?;
        out.push(PodCandidate {
            cleanup: PodCleanup {
                request_id: Uuid::parse_str(&id)?,
                generation,
                pod_name: r.get(2),
                pod_uid: r.get(3),
            },
            terminal: r.get(4),
        });
    }
    tx.commit()?;
    Ok(out)
}

/// Occupied allocation time, not CPU time. The control lane can advance this
/// while the engine thread is blocked; the saved admission bounds the charge.
pub(crate) fn checkpoint_worker(client: &mut Client, epoch: &WorkerEpoch) -> Result<()> {
    let mut tx = client.transaction()?;
    bounded(&mut tx)?;
    let rows=tx.query("SELECT j.request_id,GREATEST(0,FLOOR(EXTRACT(EPOCH FROM(clock_timestamp()-r.execution_granted_at))*1000))::BIGINT AS elapsed FROM query_jobs j JOIN requests r USING(request_id) WHERE r.worker_id=$1 AND r.worker_boot_id=$2 AND r.state='executing' AND r.execution_granted_at IS NOT NULL ORDER BY j.request_id LIMIT 1 FOR UPDATE OF j SKIP LOCKED",&[&epoch.worker_id,&epoch.boot_id.to_string()])?;
    for row in rows {
        tx.execute("UPDATE query_jobs SET occupied_checkpoint_ms=GREATEST(occupied_checkpoint_ms,$2) WHERE request_id=$1",&[&row.get::<_,String>(0),&row.get::<_,i64>(1)])?;
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> ValidatedJobRequest {
        serde_json::from_value::<crate::job_policy::JobRequest>(json!({"request_id":"10000000-0000-4000-8000-000000000001","sql":"SELECT ?","parameters":[1],"context":{"tag":"x"},"compute":{"pool":"small"}})).unwrap().validate().unwrap()
    }
    fn placement() -> Placement {
        Placement {
            mode: Mode::Persistent,
            pool: Some("small".into()),
            resources: Default::default(),
            authority: crate::compute::Authority::Writer,
        }
    }
    fn policy() -> AcceptedJobPolicy {
        AcceptedJobPolicy {
            limits: JobLimits::BASELINE,
            queue_timeout_seconds: 120,
            format: crate::job_policy::ResultFormat::ArrowIpcChunks,
            policy_generation: 1,
        }
    }
    #[test]
    fn job_hash_freezes_semantics_and_excludes_retry_and_diagnostics() {
        let original = input();
        let hash = fingerprint(&original, &placement(), &policy()).unwrap();
        // Data authority is part of the execution policy: the same SQL under a
        // reader and a writer is not the same accepted job.
        assert_eq!(
            hash,
            "6405a14d47375450d5cc846799e16f565e647bce4863e2ef3a8746d930682fd9"
        );
        let mut reader = placement();
        reader.authority = crate::compute::Authority::Reader;
        assert_ne!(hash, fingerprint(&original, &reader, &policy()).unwrap());
        let mut retry = original.request().clone();
        retry.request_id = Uuid::new_v4();
        retry.retry_of = Some(original.request().request_id);
        let mut later = policy();
        later.policy_generation = 999;
        assert_eq!(
            hash,
            fingerprint(&retry.validate().unwrap(), &placement(), &later).unwrap()
        );
        for changed in [
            json!({"queue_timeout_seconds":121}),
            json!({"timeout_seconds":60}),
            json!({"parameters":[2]}),
            json!({"sql":"SELECT 2"}),
            json!({"result":{"max_rows":1}}),
        ] {
            let mut value = serde_json::to_value(original.request()).unwrap();
            for (k, v) in changed.as_object().unwrap() {
                value[k] = v.clone();
            }
            let changed = serde_json::from_value::<crate::job_policy::JobRequest>(value)
                .unwrap()
                .validate()
                .unwrap();
            assert_ne!(
                hash,
                fingerprint(&changed, &placement(), &policy()).unwrap()
            );
        }
    }
    #[test]
    fn retained_options_never_include_statement_parameters_or_context() {
        let input = input();
        let options = serde_json::to_value(input.options()).unwrap();
        let fields = options
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            fields,
            [
                "compute",
                "queue_timeout_seconds",
                "result",
                "timeout_seconds"
            ]
            .into_iter()
            .collect()
        );
        assert!(!options.to_string().contains("SELECT"));
        assert!(!options.to_string().contains("tag"));
    }
}
