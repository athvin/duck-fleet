//! Durable attempt identities survive diagnostic retention. Request IDs remain
//! execution IDs: a retry has a new ID and an explicit link to a prior refusal.
//! No outcome in this module is evidence of exactly-once external side effects.

use crate::{
    ApiError,
    compute::{ComputeRequest, Resources},
    credentials, failure, metering,
    pricing::RateCard,
};
use postgres::{Client, GenericClient, NoTls, Row, Transaction};
use serde_json::{Value, json};
use std::{fmt::Write, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use uuid::Uuid;

const FINGERPRINT_VERSION: i32 = 3;
const FINGERPRINT_DOMAIN: &[u8] = b"duckfleet-request-v3\0";
const V2_FINGERPRINT_DOMAIN: &[u8] = b"duckfleet-request-v2\0";
const LEGACY_FINGERPRINT_DOMAIN: &[u8] = b"duckfleet-request-v1\0";

/// Inputs after parameter validation and the session-context merge. `compute`
/// contains resolved mode/resources, never observed worker or boot identity.
pub(crate) struct Submission<'a> {
    pub request_id: &'a str,
    pub retry_of: Option<&'a str>,
    pub session_id: &'a str,
    pub sql: &'a str,
    pub parameters: &'a [Value],
    pub context: &'a Value,
    pub timeout_seconds: u64,
    pub queue_timeout_seconds: u64,
    pub compute: &'a Value,
    pub worker_id: &'a str,
    pub placement: &'a Value,
    pub principal: &'a str,
}

pub(crate) enum AdmissionResult {
    Admitted(metering::Admission),
    Duplicate(Value),
}

/// A statement result is not a transaction-commit assertion. Interruptions may
/// have occurred after a side effect and therefore remain explicitly unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    NotExecuted,
    StatementResult,
    Unknown,
}
impl Outcome {
    fn name(self) -> &'static str {
        match self {
            Self::NotExecuted => "not_executed",
            Self::StatementResult => "statement_result",
            Self::Unknown => "unknown",
        }
    }
}

/// Sorting explicitly makes the encoding independent of serde_json's optional
/// preserve_order feature, including nested execution context objects.
fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort_unstable();
            let mut sorted = serde_json::Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical(&object[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

fn hash_payload(domain: &[u8], payload: &Value) -> Result<String, ApiError> {
    let encoded = serde_json::to_vec(&canonical(payload)).map_err(|_| ApiError::internal())?;
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(domain);
    digest.update(&encoded);
    Ok(digest
        .finish()
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        }))
}

fn requested(input: &Submission<'_>) -> Result<ComputeRequest, ApiError> {
    input.context.get("requested_compute").map_or_else(
        || Ok(ComputeRequest::default()),
        |value| serde_json::from_value(value.clone()).map_err(|_| ApiError::internal()),
    )
}

fn fingerprint_v2(input: &Submission<'_>) -> Result<String, ApiError> {
    let mut context = input.context.clone();
    if let Some(context) = context.as_object_mut() {
        context.remove("requested_compute");
    }
    // Project fields explicitly: adding a diagnostic field or a new optional
    // serde field must never silently redefine this fingerprint version.
    let resolved = json!({"mode":input.compute["mode"],"pool":input.compute["pool"],
        "cpu":input.compute["cpu"],"memory_bytes":input.compute["memory_bytes"],
        "disk_bytes":input.compute["disk_bytes"]});
    let payload = canonical(&json!({
        "sql": input.sql,
        "parameters": input.parameters,
        "context": context,
        "timeout_seconds": input.timeout_seconds,
        "compute": {"requested":requested(input)?.canonical_intent()?,"resolved":resolved},
    }));
    hash_payload(V2_FINGERPRINT_DOMAIN, &payload)
}

fn fingerprint(input: &Submission<'_>) -> Result<String, ApiError> {
    // V2 stays byte-for-byte frozen. Domain separation and an explicit relative
    // queue deadline add semantics without reinterpreting existing identities.
    hash_payload(
        FINGERPRINT_DOMAIN,
        &json!({
            "v2": fingerprint_v2(input)?, "queue_timeout_seconds": input.queue_timeout_seconds,
        }),
    )
}

/// V1 saw different reserved context on the direct and gateway ingress paths.
/// Reconstruct only those old shapes, never weaken application-context or SQL
/// matching, and never project another persistent pool onto historical small.
fn legacy_fingerprints(input: &Submission<'_>) -> Result<Vec<String>, ApiError> {
    let original = requested(input)?;
    let Some(cpu) = input.compute["cpu"]
        .as_u64()
        .and_then(|cpu| u16::try_from(cpu).ok())
    else {
        return Ok(Vec::new());
    };
    let (Some(memory_bytes), Some(disk_bytes)) = (
        input.compute["memory_bytes"].as_u64(),
        input.compute["disk_bytes"].as_u64(),
    ) else {
        return Ok(Vec::new());
    };
    let resources = Resources {
        cpu,
        memory_bytes,
        disk_bytes,
    };
    let persistent = input.compute["mode"] == "persistent";
    if persistent {
        if input.compute["pool"]
            .as_str()
            .is_some_and(|pool| pool != "small")
            || resources != Resources::default()
            || original.cpu.is_some()
            || original.memory.is_some()
            || original.disk.is_some()
        {
            return Ok(Vec::new());
        }
    } else if input.compute["mode"] != "isolated" || !input.compute["pool"].is_null() {
        return Ok(Vec::new());
    }
    let compute = json!({"mode":input.compute["mode"],"cpu":cpu,
        "memory_bytes":memory_bytes,"disk_bytes":disk_bytes});
    let direct = json!({"cpu":original.cpu,"memory":original.memory,"disk":original.disk});
    let forwarded = if persistent {
        json!({"cpu":null,"memory":null,"disk":null})
    } else {
        // V1 rounded non-Gi quantities down to Mi. Preserve its historical hash
        // projection even though new resource forwarding retains exact KiB.
        let historical_quantity = |bytes: u64| {
            let gib = 1024 * 1024 * 1024;
            if bytes.is_multiple_of(gib) {
                format!("{}Gi", bytes / gib)
            } else {
                format!("{}Mi", bytes / (1024 * 1024))
            }
        };
        json!({"cpu":cpu,"memory":historical_quantity(resources.memory_bytes),
            "disk":historical_quantity(resources.disk_bytes)})
    };
    let mut contexts = Vec::with_capacity(2);
    if input.context.get("requested_compute").is_none() {
        // Historical internal callers/fixtures could supply no session alias.
        contexts.push(input.context.clone());
    } else {
        for requested in [direct, forwarded] {
            let mut context = input.context.clone();
            context["requested_compute"] = requested;
            if !contexts.contains(&context) {
                contexts.push(context);
            }
        }
    }
    contexts
        .into_iter()
        .map(|context| {
            hash_payload(
                LEGACY_FINGERPRINT_DOMAIN,
                &json!({
                    "sql":input.sql,"parameters":input.parameters,"context":context,
                    "timeout_seconds":input.timeout_seconds,"compute":compute,
                }),
            )
        })
        .collect()
}

// Legacy statements never retained bound parameters. These inserts deliberately
// leave the fingerprint NULL, even where SQL text is still available. A replay
// must never supply the missing historical fingerprint or claim its ownership.
const HISTORY_BACKFILL: &str = "INSERT INTO requests (
    request_id, principal, logical_request_id, state, status, outcome,
    failure_kind, retry_safe, worker_id, accepted_at, finished_at, elapsed_ms, result_rows)
 SELECT h.query_id,
    CASE WHEN h.principal IN ('direct','legacy','platform') THEN NULL ELSE h.principal END,
    h.query_id,
    CASE WHEN h.status='running' THEN 'executing' ELSE 'terminal' END,
    h.status,
    CASE WHEN h.status='refused' THEN 'not_executed'
         WHEN h.status IN ('running','interrupted','canceled','timed_out','budget_exceeded') THEN 'unknown'
         ELSE 'statement_result' END,
    h.failure_kind, h.retry_safe, h.worker_id, h.accepted_at, h.finished_at,
    h.elapsed_ms, h.result_rows
 FROM query_history h";

const RESERVATION_BACKFILL: &str = "INSERT INTO requests (
    request_id, logical_request_id, state, status, outcome, failure_kind,
    retry_safe, worker_id, accepted_at, finished_at)
 SELECT r.execution_id, r.execution_id,
    CASE WHEN r.state='held' THEN 'executing' ELSE 'terminal' END,
    CASE WHEN r.state='held' THEN 'running' ELSE 'interrupted' END,
    'unknown', CASE WHEN r.state='held' THEN NULL ELSE 'interrupted' END,
    false, r.worker_id, r.created_at, r.settled_at
 FROM reservations r";

const USAGE_BACKFILL: &str = "INSERT INTO requests (
    request_id, logical_request_id, state, status, outcome, failure_kind,
    retry_safe, worker_id, accepted_at, finished_at)
 SELECT u.execution_id, u.execution_id, 'terminal', 'interrupted', 'unknown',
    'interrupted', false, MIN(u.worker_id), MIN(u.started_at), MAX(u.ended_at)
 FROM usage_events u";

fn backfill(transaction: &mut Transaction<'_>, request_id: Option<&str>) -> anyhow::Result<()> {
    for (source, key, group) in [
        (HISTORY_BACKFILL, "h.query_id", ""),
        (RESERVATION_BACKFILL, "r.execution_id", ""),
        (USAGE_BACKFILL, "u.execution_id", " GROUP BY u.execution_id"),
    ] {
        let predicate = if request_id.is_some() {
            format!(" WHERE {key}=$1")
        } else {
            String::new()
        };
        let sql = format!("{source}{predicate}{group} ON CONFLICT DO NOTHING");
        match request_id {
            Some(id) => transaction.execute(&sql, &[&id])?,
            None => transaction.execute(&sql, &[])?,
        };
    }
    Ok(())
}

/// The platform baseline calls this once for history written after the previous
/// request-backfill marker, including old synthetic retention audit records.
/// Existing ownership, fingerprints and outcomes are never overwritten. This is
/// deployment migration work, not a worker-startup or maintenance-loop scan.
pub(crate) fn backfill_missing_history(transaction: &mut Transaction<'_>) -> anyhow::Result<u64> {
    Ok(transaction.execute(
        &format!(
            "{HISTORY_BACKFILL} WHERE NOT EXISTS (
            SELECT 1 FROM requests r WHERE r.request_id=h.query_id)
            ON CONFLICT DO NOTHING"
        ),
        &[],
    )?)
}

/// Called after metering migration and before recovery, under the platform's
/// migration advisory lock. The marker and backfill commit together,
/// so a failed upgrade retries, and later migration invocations skip full scans.
pub(crate) fn migrate(transaction: &mut Transaction<'_>) -> anyhow::Result<()> {
    transaction.batch_execute(
        "CREATE TABLE IF NOT EXISTS requests (
            request_id TEXT PRIMARY KEY,
            principal TEXT,
            logical_request_id TEXT NOT NULL,
            retry_of TEXT,
            fingerprint_version INTEGER,
            payload_hash TEXT,
            state TEXT NOT NULL CHECK (state IN ('accepted','executing','terminal')),
            status TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN ('not_executed','statement_result','unknown')),
            failure_kind TEXT,
            retry_safe BOOLEAN,
            worker_id TEXT NOT NULL,
            accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at TIMESTAMPTZ,
            elapsed_ms BIGINT,
            result_rows BIGINT,
            CHECK ((fingerprint_version IS NULL) = (payload_hash IS NULL))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS requests_retry_child
            ON requests(retry_of) WHERE retry_of IS NOT NULL;
        CREATE INDEX IF NOT EXISTS requests_logical ON requests(logical_request_id,accepted_at);
        CREATE INDEX IF NOT EXISTS requests_unfinished ON requests(worker_id) WHERE state <> 'terminal';
        CREATE TABLE IF NOT EXISTS requests_schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );",
    )?;
    if transaction.execute(
        "INSERT INTO requests_schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING",
        &[],
    )? > 0
    {
        backfill(transaction, None)?;
    }
    Ok(())
}

const COLUMNS: &str = "r.request_id,r.principal,r.logical_request_id,r.retry_of,
    r.fingerprint_version,r.payload_hash,r.state,r.status,r.outcome,r.failure_kind,
    r.retry_safe,r.worker_id,r.accepted_at::text,r.finished_at::text,r.elapsed_ms,r.result_rows,
    EXISTS(SELECT 1 FROM query_history h WHERE h.query_id=r.request_id) AS diagnostics_available,
    (SELECT child.request_id FROM requests child WHERE child.retry_of=r.request_id) AS next_attempt_id, to_jsonb(r)::text AS queue_record";

struct Stored {
    request_id: String,
    principal: Option<String>,
    logical_request_id: String,
    retry_of: Option<String>,
    fingerprint_version: Option<i32>,
    payload_hash: Option<String>,
    state: String,
    status: String,
    outcome: String,
    failure_kind: Option<String>,
    retry_safe: Option<bool>,
    worker_id: String,
    accepted_at: String,
    finished_at: Option<String>,
    elapsed_ms: Option<i64>,
    result_rows: Option<i64>,
    diagnostics_available: bool,
    next_attempt_id: Option<String>,
    queue_record: Value,
}
impl Stored {
    fn from_row(row: Row) -> Self {
        Self {
            request_id: row.get("request_id"),
            principal: row.get("principal"),
            logical_request_id: row.get("logical_request_id"),
            retry_of: row.get("retry_of"),
            fingerprint_version: row.get("fingerprint_version"),
            payload_hash: row.get("payload_hash"),
            state: row.get("state"),
            status: row.get("status"),
            outcome: row.get("outcome"),
            failure_kind: row.get("failure_kind"),
            retry_safe: row.get("retry_safe"),
            worker_id: row.get("worker_id"),
            accepted_at: row.get("accepted_at"),
            finished_at: row.get("finished_at"),
            elapsed_ms: row.get("elapsed_ms"),
            result_rows: row.get("result_rows"),
            diagnostics_available: row.get("diagnostics_available"),
            next_attempt_id: row.get("next_attempt_id"),
            queue_record: serde_json::from_str(row.get::<_, &str>("queue_record"))
                .unwrap_or(Value::Null),
        }
    }

    fn authorize(&self, principal: &str, bootstrap: bool) -> Result<(), ApiError> {
        if !bootstrap && self.principal.as_deref() != Some(principal) {
            return Err(not_found());
        }
        Ok(())
    }

    fn retryable(&self) -> bool {
        self.state == "terminal"
            && self.outcome == Outcome::NotExecuted.name()
            && self.retry_safe == Some(true)
            && self.failure_kind.as_deref() == Some("budget_exhausted")
            && matches!(self.fingerprint_version, Some(1..=3))
            && self.payload_hash.is_some()
            && self.principal.is_some()
            && self.next_attempt_id.is_none()
    }

    fn matches_payload(
        &self,
        input: &Submission<'_>,
        current_hash: &str,
    ) -> Result<bool, ApiError> {
        let Some(previous) = self.payload_hash.as_deref() else {
            return Ok(false);
        };
        match self.fingerprint_version {
            Some(1) => Ok(legacy_fingerprints(input)?
                .iter()
                .any(|hash| hash == previous)),
            Some(2) => Ok(previous == fingerprint_v2(input)?),
            Some(FINGERPRINT_VERSION) => Ok(previous == current_hash),
            _ => Ok(false),
        }
    }

    fn detail(&self) -> Value {
        let mut detail = json!({
            "request_id": self.request_id,
            "logical_request_id": self.logical_request_id,
            "retry_of": self.retry_of,
            "next_attempt_id": self.next_attempt_id,
            "principal": self.principal,
            "state": self.state,
            "status": self.status,
            "outcome": self.outcome,
            "failure_kind": self.failure_kind,
            "retry_safe": self.retry_safe,
            "worker_id": self.worker_id,
            "accepted_at": self.accepted_at,
            "finished_at": self.finished_at,
            "elapsed_ms": self.elapsed_ms,
            "result_rows": self.result_rows,
            "fingerprint_version": self.fingerprint_version,
            "fingerprint_status": if self.payload_hash.is_none() { "legacy_unavailable" }
                else if matches!(self.fingerprint_version, Some(1..=3)) { "known" }
                else { "unsupported_version" },
            "diagnostics_available": self.diagnostics_available,
            "retry": {
                "allowed": self.retryable(),
                "new_request_id_required": true,
                "retry_of": if self.retryable() { Some(&self.request_id) } else { None },
            },
        });
        for key in [
            "session_id",
            "session_sequence",
            "worker_boot_id",
            "pool_scope",
            "protocol",
            "actor_principal",
            "credential_id",
            "queued_at",
            "selected_at",
            "execution_granted_at",
            "engine_started_at",
            "queue_deadline_at",
            "queue_timeout_seconds",
            "policy_generation",
            "state_version",
            "delivery_state",
        ] {
            if let Some(value) = self.queue_record.get(key) {
                detail[key] = value.clone();
            }
        }
        detail
    }

    fn duplicate(&self, input: &Submission<'_>, hash: &str) -> Result<Value, ApiError> {
        self.authorize(input.principal, is_bootstrap(input.principal))?;
        // Legacy fingerprints stay unknown permanently. Returning their outcome
        // is safe; asserting either equality or mismatch would invent evidence.
        if self.retry_of.as_deref() != input.retry_of
            || (self.payload_hash.is_some()
                && matches!(self.fingerprint_version, Some(1..=3))
                && !self.matches_payload(input, hash)?)
        {
            return Err(ApiError::new(
                failure::REQUEST_ID_MISMATCH,
                "request_id belongs to a different execution payload or retry chain; no SQL ran",
            ));
        }
        Ok(json!({
            "error": "request_id was already accepted; inspect its durable outcome, SQL was not replayed",
            "failure_kind": failure::DUPLICATE_REQUEST.kind,
            "retry_safe": false,
            "request_id": self.request_id,
            "original": self.detail(),
        }))
    }
}

fn is_bootstrap(principal: &str) -> bool {
    principal == credentials::BOOTSTRAP_PRINCIPAL
}

fn not_found() -> ApiError {
    ApiError::new(failure::NOT_FOUND, "request not found")
}

fn load(client: &mut impl GenericClient, id: &str) -> Result<Option<Stored>, ApiError> {
    client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM requests r WHERE r.request_id=$1"),
            &[&id],
        )
        .map(|row| row.map(Stored::from_row))
        .map_err(|_| ApiError::internal())
}

/// A legacy writer may race a quiesced-upgrade invariant. Keep its reservation
/// effective and remove only this transaction's speculative diagnostic/identity
/// rows before reconstructing legacy evidence. Never certify the replay's hash.
fn legacy_collision(
    transaction: &mut Transaction<'_>,
    input: &Submission<'_>,
    hash: &str,
    inserted_history: bool,
) -> Result<Value, ApiError> {
    if inserted_history {
        transaction
            .execute(
                "DELETE FROM query_history WHERE query_id=$1",
                &[&input.request_id],
            )
            .map_err(|_| ApiError::internal())?;
    }
    transaction
        .execute(
            "DELETE FROM requests WHERE request_id=$1",
            &[&input.request_id],
        )
        .map_err(|_| ApiError::internal())?;
    backfill(transaction, Some(input.request_id)).map_err(|_| ApiError::internal())?;
    load(transaction, input.request_id)?
        .ok_or_else(ApiError::internal)?
        .duplicate(input, hash)
}

pub(crate) enum ClaimResult {
    New,
    Duplicate(Value),
}

/// Atomic identity/history claim, deliberately without execution-budget locks.
pub(crate) fn claim_in(
    transaction: &mut Transaction<'_>,
    input: &Submission<'_>,
    state: &str,
) -> Result<ClaimResult, ApiError> {
    let status = if state == "queued" {
        "queued"
    } else {
        "running"
    };
    let hash = fingerprint(input)?;
    // Indexed fallback probes also recognize old evidence that was restored
    // after the one-time migration; these never scan all retained history.
    backfill(transaction, Some(input.request_id)).map_err(|_| ApiError::internal())?;
    if let Some(stored) = load(transaction, input.request_id)? {
        let duplicate = stored.duplicate(input, &hash);
        return duplicate.map(ClaimResult::Duplicate);
    }
    let logical_request_id = if let Some(parent_id) = input.retry_of {
        if parent_id == input.request_id {
            return Err(ApiError::new(
                failure::INVALID_RETRY,
                "retry_of must name a prior attempt",
            ));
        }
        backfill(transaction, Some(parent_id)).map_err(|_| ApiError::internal())?;
        let parent = load(transaction, parent_id)?.ok_or_else(not_found)?;
        parent.authorize(input.principal, is_bootstrap(input.principal))?;
        if !parent.retryable() || parent.principal.as_deref() != Some(input.principal) {
            return Err(ApiError::new(
                failure::INVALID_RETRY,
                "retry_of must name this principal's fingerprinted, retry-safe budget refusal before SQL execution",
            ));
        }
        // The parent may predate the current policy schema. Verify its original
        // version, then give this new attempt its own current-version identity.
        if !parent.matches_payload(input, &hash)? {
            return Err(ApiError::new(
                failure::REQUEST_ID_MISMATCH,
                "retry payload differs from the refused attempt; no SQL ran",
            ));
        }
        parent.logical_request_id
    } else {
        input.request_id.to_owned()
    };
    let claimed = transaction
        .execute(
            "INSERT INTO requests (request_id,principal,logical_request_id,retry_of,
            fingerprint_version,payload_hash,state,status,outcome,worker_id)
         VALUES ($1,$2,$3,$4,$5,$6,$8,$9,'unknown',$7)
         ON CONFLICT DO NOTHING",
            &[
                &input.request_id,
                &input.principal,
                &logical_request_id,
                &input.retry_of,
                &FINGERPRINT_VERSION,
                &hash,
                &input.worker_id,
                &state,
                &status,
            ],
        )
        .map_err(|_| ApiError::internal())?;
    if claimed == 0 {
        if let Some(stored) = load(transaction, input.request_id)? {
            return stored.duplicate(input, &hash).map(ClaimResult::Duplicate);
        }
        return Err(ApiError::new(
            failure::INVALID_RETRY,
            "the refused attempt already has a retry; inspect the existing chain instead of creating parallel attempts",
        ));
    }
    let inserted_history = transaction.execute(
        "INSERT INTO query_history (query_id,session_id,sql,context,status,worker_id,placement,principal)
         VALUES ($1,$2,$3,$4,$8,$5,$6,$7) ON CONFLICT DO NOTHING",
        &[&input.request_id, &input.session_id, &input.sql, &input.context.to_string(),
          &input.worker_id, &input.placement.to_string(), &input.principal, &status],
    ).map_err(|_| ApiError::internal())?;
    if inserted_history == 0 {
        let duplicate = legacy_collision(transaction, input, &hash, false);
        return duplicate.map(ClaimResult::Duplicate);
    }
    Ok(ClaimResult::New)
}

pub(crate) fn admit(
    client: &mut Client,
    input: Submission<'_>,
    units: i64,
    rate: &RateCard,
) -> Result<AdmissionResult, ApiError> {
    let mut transaction = client.transaction().map_err(|_| ApiError::internal())?;
    if let ClaimResult::Duplicate(value) = claim_in(&mut transaction, &input, "accepted")? {
        transaction.commit().map_err(|_| ApiError::internal())?;
        return Ok(AdmissionResult::Duplicate(value));
    }
    let hash = fingerprint(&input)?;
    match metering::admit_in(
        &mut transaction,
        input.request_id,
        units,
        input.timeout_seconds,
        input.worker_id,
        rate,
    ) {
        Ok(admission) => {
            transaction.commit().map_err(|_| ApiError::internal())?;
            Ok(AdmissionResult::Admitted(admission))
        }
        Err(refusal) if refusal.0.kind == failure::DUPLICATE_REQUEST.kind => {
            let duplicate = legacy_collision(&mut transaction, &input, &hash, true);
            transaction.commit().map_err(|_| ApiError::internal())?;
            duplicate.map(AdmissionResult::Duplicate)
        }
        Err(mut refusal) if matches!(refusal.0.kind, "budget_exhausted" | "budget_job_cost") => {
            let status = refusal.0.status.unwrap_or("refused");
            if refusal.0.retry_safe {
                refusal.1.push_str("; after funding, submit a new request_id with retry_of set to this attempt's request_id");
            }
            transaction
                .execute(
                    "UPDATE query_history SET status=$2,finished_at=now(),elapsed_ms=0,
                    error=$3,failure_kind=$4,retry_safe=$5 WHERE query_id=$1",
                    &[
                        &input.request_id,
                        &status,
                        &refusal.1,
                        &refusal.0.kind,
                        &refusal.0.retry_safe,
                    ],
                )
                .map_err(|_| ApiError::internal())?;
            finish_in(
                &mut transaction,
                input.request_id,
                status,
                Some(refusal.0.kind),
                Some(refusal.0.retry_safe),
                0,
                None,
                Outcome::NotExecuted,
            )
            .map_err(|_| ApiError::internal())?;
            transaction.commit().map_err(|_| ApiError::internal())?;
            Err(refusal)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn mark_executing(client: &mut Client, request_id: &str) -> Result<(), ApiError> {
    let changed = client
        .execute(
            "UPDATE requests SET state='executing' WHERE request_id=$1 AND state='accepted'",
            &[&request_id],
        )
        .map_err(|_| ApiError::internal())?;
    if changed != 1 {
        return Err(ApiError::internal());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn finish_in(
    transaction: &mut Transaction<'_>,
    request_id: &str,
    status: &str,
    failure_kind: Option<&str>,
    retry_safe: Option<bool>,
    elapsed_ms: i64,
    result_rows: Option<i64>,
    outcome: Outcome,
) -> anyhow::Result<()> {
    let changed = transaction.execute(
        "UPDATE requests SET state='terminal',status=$2,outcome=$3,failure_kind=$4,
            retry_safe=$5,finished_at=now(),elapsed_ms=$6,result_rows=$7
         WHERE request_id=$1 AND state <> 'terminal'",
        &[
            &request_id,
            &status,
            &outcome.name(),
            &failure_kind,
            &retry_safe,
            &elapsed_ms,
            &result_rows,
        ],
    )?;
    anyhow::ensure!(
        changed == 1,
        "request terminal transition did not own an unfinished attempt"
    );
    Ok(())
}

pub(crate) fn interrupt_abandoned(
    transaction: &mut Transaction<'_>,
    worker_id: &str,
) -> anyhow::Result<u64> {
    Ok(transaction.execute(
        "UPDATE requests SET state='terminal',status='interrupted',outcome='unknown',
            failure_kind='interrupted',retry_safe=false,finished_at=now()
         WHERE worker_id=$1 AND state IN ('accepted','executing') AND to_jsonb(requests)->>'worker_boot_id' IS NULL",
        &[&worker_id],
    )?)
}

pub(crate) fn interrupt_abandoned_prefix(
    transaction: &mut Transaction<'_>,
    prefix: &str,
) -> anyhow::Result<u64> {
    Ok(transaction.execute(
        "UPDATE requests SET state='terminal',status='interrupted',outcome='unknown',
            failure_kind='interrupted',retry_safe=false,finished_at=now()
         WHERE starts_with(worker_id,$1) AND state IN ('accepted','executing') AND to_jsonb(requests)->>'worker_boot_id' IS NULL",
        &[&prefix],
    )?)
}

/// Owner-scoped durable lookup, using the same bounded independent connection
/// pattern as diagnostic history. Shared team-visible history remains a separate
/// API contract until RBAC: this reader grants no additional outcome visibility.
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

    pub async fn detail(
        &self,
        id: Uuid,
        principal: String,
        is_bootstrap: bool,
    ) -> Result<Value, ApiError> {
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "request reader capacity reached"))?;
        let dsn = self.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.options("-c default_transaction_read_only=on -c statement_timeout=3000");
            let mut client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            let row = client.query_opt(
                &format!("SELECT {COLUMNS} FROM requests r WHERE r.request_id=$1 AND (r.principal=$2 OR $3)"),
                &[&id.to_string(), &principal, &is_bootstrap],
            ).map_err(|_| ApiError::internal())?.ok_or_else(not_found)?;
            Ok(Stored::from_row(row).detail())
        }).await.map_err(|_| ApiError::internal())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submission<'a>(
        parameters: &'a [Value],
        context: &'a Value,
        compute: &'a Value,
    ) -> Submission<'a> {
        Submission {
            request_id: "attempt-one",
            retry_of: None,
            session_id: "session-one",
            sql: "insert into events values (?)",
            parameters,
            context,
            timeout_seconds: 30,
            queue_timeout_seconds: 10,
            compute,
            worker_id: "small",
            placement: compute,
            principal: "analyst",
        }
    }

    fn stored(hash: Option<String>) -> Stored {
        Stored {
            request_id: "attempt-one".into(),
            principal: Some("analyst".into()),
            logical_request_id: "attempt-one".into(),
            retry_of: None,
            fingerprint_version: hash.as_ref().map(|_| FINGERPRINT_VERSION),
            payload_hash: hash,
            state: "terminal".into(),
            status: "succeeded".into(),
            outcome: "statement_result".into(),
            failure_kind: None,
            retry_safe: None,
            worker_id: "small".into(),
            accepted_at: "2026-11-20 12:00:00+00".into(),
            finished_at: Some("2026-11-20 12:00:01+00".into()),
            elapsed_ms: Some(1000),
            result_rows: Some(1),
            diagnostics_available: false,
            next_attempt_id: None,
            queue_record: Value::Null,
        }
    }

    #[test]
    fn v2_fingerprint_is_frozen_and_only_v3_adds_queue_timeout() {
        let parameters = [json!(7)];
        let context = json!({"suite":"queue-storage","requested_compute":{"pool":"fixture"}});
        let compute = json!({"mode":"persistent","pool":"fixture","cpu":1,"memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64});
        let mut input = submission(&parameters, &context, &compute);
        input.sql = "SELECT ?";
        let frozen = "28721fd97f39352eadabfbf471615fe922c6b2ab769ac0ede3579bda694b11aa";
        assert_eq!(fingerprint_v2(&input).unwrap(), frozen);
        let first = fingerprint(&input).unwrap();
        input.queue_timeout_seconds = 120;
        assert_eq!(fingerprint_v2(&input).unwrap(), frozen);
        assert_ne!(fingerprint(&input).unwrap(), first);
        let mut retained = stored(Some(frozen.into()));
        retained.fingerprint_version = Some(2);
        assert!(
            retained
                .duplicate(&input, &fingerprint(&input).unwrap())
                .is_ok()
        );
        retained.fingerprint_version = Some(3);
        retained.payload_hash = Some(first);
        assert_eq!(
            retained
                .duplicate(&input, &fingerprint(&input).unwrap())
                .unwrap_err()
                .0,
            failure::REQUEST_ID_MISMATCH
        );
    }

    #[test]
    fn fingerprint_is_canonical_but_preserves_binding_types_and_execution_policy() {
        let parameters = [json!(1), json!(null), json!(true), json!("secret-value")];
        let context = json!({"b":2,"a":{"z":3,"x":4}});
        let compute = json!({"mode":"persistent","cpu":1,"memory_bytes":1073741824_u64});
        let mut input = submission(&parameters, &context, &compute);
        let original = fingerprint(&input).unwrap();
        assert_eq!(original.len(), 64);
        let reordered: Value = serde_json::from_str(r#"{"a":{"x":4,"z":3},"b":2}"#).unwrap();
        input.context = &reordered;
        assert_eq!(fingerprint(&input).unwrap(), original);
        input.request_id = "another-attempt";
        input.retry_of = Some("prior-attempt");
        input.session_id = "replacement-session";
        input.worker_id = "another-worker";
        input.principal = "another-principal";
        assert_eq!(
            fingerprint(&input).unwrap(),
            original,
            "routing, ownership, and IDs are not payload"
        );

        let floating = [json!(1.0), json!(null), json!(true), json!("secret-value")];
        input.parameters = &floating;
        assert_ne!(
            fingerprint(&input).unwrap(),
            original,
            "integer and double bind differently"
        );
        input.parameters = &parameters;
        input.timeout_seconds = 60;
        assert_ne!(fingerprint(&input).unwrap(), original);
        input.timeout_seconds = 30;
        input.sql = "insert into events values (?);";
        assert_ne!(
            fingerprint(&input).unwrap(),
            original,
            "SQL bytes are compared exactly"
        );
        input.sql = "insert into events values (?)";
        let resized = json!({"mode":"persistent","cpu":2,"memory_bytes":1073741824_u64});
        input.compute = &resized;
        assert_ne!(fingerprint(&input).unwrap(), original);
        input.compute = &compute;
        let changed_context = json!({"b":2,"a":{"z":3,"x":5}});
        input.context = &changed_context;
        assert_ne!(fingerprint(&input).unwrap(), original);
    }

    #[test]
    fn owner_check_precedes_duplicate_payload_comparison() {
        let parameters = [json!(1)];
        let context = json!({});
        let compute = json!({"mode":"persistent"});
        let mut input = submission(&parameters, &context, &compute);
        let original = stored(Some(fingerprint(&input).unwrap()));
        input.principal = "different-user";
        let refused = original.duplicate(&input, "different-hash").unwrap_err();
        assert_eq!(refused.0, failure::NOT_FOUND);
        assert_eq!(refused.1, "request not found");
        input.principal = "analyst";
        assert_eq!(
            original.duplicate(&input, "different-hash").unwrap_err().0,
            failure::REQUEST_ID_MISMATCH
        );
    }

    #[test]
    fn legacy_payload_is_neither_certified_nor_replaced_by_a_replay() {
        let parameters = [json!(42)];
        let context = json!({});
        let compute = json!({});
        let mut input = submission(&parameters, &context, &compute);
        let mut original = stored(None);
        let duplicate = original.duplicate(&input, "new-unproven-hash").unwrap();
        assert_eq!(duplicate["failure_kind"], "duplicate_request");
        assert_eq!(
            duplicate["original"]["fingerprint_status"],
            "legacy_unavailable"
        );
        assert!(original.payload_hash.is_none());
        original.principal = None;
        assert_eq!(
            original
                .duplicate(&input, "new-unproven-hash")
                .unwrap_err()
                .0,
            failure::NOT_FOUND
        );
        input.principal = credentials::BOOTSTRAP_PRINCIPAL;
        assert!(original.duplicate(&input, "new-unproven-hash").is_ok());
    }

    #[test]
    fn diagnostic_expiry_does_not_remove_original_outcome_or_expose_payload() {
        let parameters = [json!("sensitive")];
        let context = json!({});
        let compute = json!({});
        let input = submission(&parameters, &context, &compute);
        let hash = fingerprint(&input).unwrap();
        let original = stored(Some(hash.clone()));
        let duplicate = original.duplicate(&input, &hash).unwrap();
        assert_eq!(duplicate["retry_safe"], false);
        assert_eq!(duplicate["original"]["diagnostics_available"], false);
        assert_eq!(duplicate["original"]["status"], "succeeded");
        assert_eq!(duplicate["original"]["outcome"], "statement_result");
        let response = duplicate.to_string();
        assert!(!response.contains("sensitive"));
        assert!(!response.contains(&hash));
        assert!(!response.contains(input.sql));
    }

    #[test]
    fn retry_requires_proven_budget_refusal_and_cannot_branch() {
        let mut original = stored(Some("known-hash".into()));
        original.status = "refused".into();
        original.outcome = "not_executed".into();
        original.failure_kind = Some("budget_exhausted".into());
        original.retry_safe = Some(true);
        assert!(original.retryable());
        assert_eq!(original.detail()["retry"]["retry_of"], "attempt-one");
        original.next_attempt_id = Some("attempt-two".into());
        assert!(!original.retryable());
        assert_eq!(original.detail()["next_attempt_id"], "attempt-two");
        original.next_attempt_id = None;
        original.outcome = "unknown".into();
        assert!(!original.retryable());
        original.outcome = "not_executed".into();
        original.payload_hash = None;
        original.fingerprint_version = None;
        assert!(
            !original.retryable(),
            "a legacy payload cannot be verified for linked retry"
        );
    }

    #[test]
    fn frozen_v1_direct_and_gateway_shapes_match_without_rewriting_identity() {
        // Independently frozen from the pre-pools v1 canonical payload. Direct
        // opens retained partial fields; gateways filled the isolated allocation.
        const DIRECT: &str = "a249ad7c77cc1abc9c6a2e6f479e1df1d58c229218bb9cee1ceaa4aa708e7103";
        const GATEWAY: &str = "227f543c6262a7fcf36ddf2ebf4acdc823512cff439e9fd60bfab229503866bc";
        let context = json!({"connection_name":"legacy-policy","requested_compute":{"cpu":2}});
        let compute = json!({"mode":"isolated","pool":null,"cpu":2,
            "memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64});
        let parameters = [json!(7)];
        let mut input = submission(&parameters, &context, &compute);
        input.sql = "INSERT INTO events VALUES (?)";
        let current = fingerprint(&input).unwrap();
        let candidates = legacy_fingerprints(&input).unwrap();
        assert!(candidates.iter().any(|hash| hash == DIRECT));
        assert!(candidates.iter().any(|hash| hash == GATEWAY));
        for hash in [DIRECT, GATEWAY] {
            let mut original = stored(Some(hash.into()));
            original.fingerprint_version = Some(1);
            assert!(original.duplicate(&input, &current).is_ok());
            original.status = "refused".into();
            original.failure_kind = Some("budget_exhausted".into());
            original.outcome = "not_executed".into();
            original.retry_safe = Some(true);
            assert!(original.retryable());
            assert!(original.matches_payload(&input, &current).unwrap());
            assert_eq!(original.payload_hash.as_deref(), Some(hash));
            input.sql = "INSERT INTO events VALUES (?);";
            assert_eq!(
                original
                    .duplicate(&input, &fingerprint(&input).unwrap())
                    .unwrap_err()
                    .0,
                failure::REQUEST_ID_MISMATCH
            );
            input.sql = "INSERT INTO events VALUES (?)";
        }
    }

    #[test]
    fn v1_persistent_identity_cannot_be_reinterpreted_as_another_same_sized_pool() {
        const SMALL: &str = "969f5d969855167364657a532132f703ba79a3a5dc3d3d7024927ef3d0ae5f2f";
        let parameters = [json!(7)];
        let context =
            json!({"connection_name":"legacy-policy","requested_compute":{"pool":"small"}});
        let compute = json!({"mode":"persistent","pool":"small","cpu":1,
            "memory_bytes":1073741824_u64,"disk_bytes":2147483648_u64});
        let mut input = submission(&parameters, &context, &compute);
        input.sql = "INSERT INTO events VALUES (?)";
        let mut original = stored(Some(SMALL.into()));
        original.fingerprint_version = Some(1);
        assert!(
            original
                .duplicate(&input, &fingerprint(&input).unwrap())
                .is_ok()
        );
        let mut other = compute.clone();
        other["pool"] = json!("another");
        input.compute = &other;
        assert_eq!(
            original
                .duplicate(&input, &fingerprint(&input).unwrap())
                .unwrap_err()
                .0,
            failure::REQUEST_ID_MISMATCH
        );
        input.compute = &compute;
        let minimum =
            json!({"connection_name":"legacy-policy","requested_compute":{"pool":"small","cpu":1}});
        input.context = &minimum;
        assert_eq!(
            original
                .duplicate(&input, &fingerprint(&input).unwrap())
                .unwrap_err()
                .0,
            failure::REQUEST_ID_MISMATCH
        );
    }

    #[test]
    fn v2_policy_uses_canonical_requirements_and_keeps_named_pools_distinct() {
        let parameters = [json!(1)];
        let context = json!({"application":"billing","requested_compute":{"pool":"analytics","memory":"1Gi"}});
        let compute = json!({"mode":"persistent","pool":"analytics","cpu":4,
            "memory_bytes":8589934592_u64,"disk_bytes":2147483648_u64});
        let mut input = submission(&parameters, &context, &compute);
        let original = fingerprint(&input).unwrap();
        let equivalent = json!({"application":"billing","requested_compute":{"memory":"1024Mi","pool":"analytics"}});
        input.context = &equivalent;
        assert_eq!(fingerprint(&input).unwrap(), original);
        let changed_minimum = json!({"application":"billing","requested_compute":{"memory":"2Gi","pool":"analytics"}});
        input.context = &changed_minimum;
        assert_ne!(fingerprint(&input).unwrap(), original);
        input.context = &context;
        let mut other = compute.clone();
        other["pool"] = json!("interactive");
        input.compute = &other;
        assert_ne!(fingerprint(&input).unwrap(), original);
    }

    #[test]
    fn unsupported_fingerprint_versions_never_authorize_retry_or_claim_payload_equality() {
        let parameters = [];
        let context = json!({});
        let compute = json!({});
        let input = submission(&parameters, &context, &compute);
        let mut original = stored(Some("future-hash".into()));
        original.fingerprint_version = Some(99);
        original.status = "refused".into();
        original.failure_kind = Some("budget_exhausted".into());
        original.outcome = "not_executed".into();
        original.retry_safe = Some(true);
        assert!(!original.retryable());
        let duplicate = original
            .duplicate(&input, &fingerprint(&input).unwrap())
            .unwrap();
        assert_eq!(duplicate["failure_kind"], "duplicate_request");
        assert_eq!(
            duplicate["original"]["fingerprint_status"],
            "unsupported_version"
        );
        assert!(
            !original
                .matches_payload(&input, &fingerprint(&input).unwrap())
                .unwrap()
        );
    }
}
