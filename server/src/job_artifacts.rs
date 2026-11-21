//! Immutable artifact journal. Storage authorizes keys; object IO never owns SQL state.
use crate::{
    ApiError,
    authz::{Access, Action},
    failure,
    jobs::{self, ExecutionToken},
};
use anyhow::{Result, ensure};
use postgres::{Client, Transaction};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct BlobDescriptor {
    pub sequence: u32,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug)]
pub(crate) struct PutIntent {
    pub request_id: Uuid,
    pub generation: Uuid,
    pub sequence: u32,
    pub upload_token: Uuid,
    pub relative_key: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ChunkRef {
    pub sequence: u32,
    pub relative_key: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug)]
pub(crate) struct ManifestParts {
    pub request_id: Uuid,
    pub generation: Uuid,
    pub chunks: Vec<ChunkRef>,
    pub rows: u64,
    pub bytes: u64,
    pub expires_at: SystemTime,
}
#[derive(Clone, Debug)]
pub(crate) struct VerifiedManifest {
    pub(crate) request_id: Uuid,
    pub(crate) generation: Uuid,
    pub(crate) digest: String,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}
#[derive(Clone, Debug)]
pub(crate) struct ReadObject {
    pub request_id: Uuid,
    pub generation: Uuid,
    pub relative_key: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug)]
pub(crate) struct DeleteIntent {
    pub request_id: Uuid,
    pub generation: Uuid,
    pub sequence: u32,
    pub relative_key: String,
}
#[derive(Debug)]
pub(crate) struct ArtifactExpired;
impl std::fmt::Display for ArtifactExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("job result expired before publication")
    }
}
impl std::error::Error for ArtifactExpired {}
const MANIFEST_SEQUENCE: u32 = 4096;
const MAX_BYTES: u64 = 16 * 1024 * 1024;

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("CREATE TABLE job_artifacts(request_id TEXT NOT NULL REFERENCES requests(request_id),generation TEXT NOT NULL,sequence BIGINT NOT NULL CHECK(sequence BETWEEN 0 AND 4096),upload_token TEXT NOT NULL UNIQUE,relative_key TEXT NOT NULL UNIQUE,rows BIGINT NOT NULL CHECK(rows>=0),bytes BIGINT NOT NULL CHECK(bytes>0),sha256 TEXT NOT NULL CHECK(length(sha256)=64),state TEXT NOT NULL CHECK(state IN ('intended','uploaded','deleting','deleted')),created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),uploaded_at TIMESTAMPTZ,delete_after TIMESTAMPTZ,delete_attempts BIGINT NOT NULL DEFAULT 0,last_deleted_at TIMESTAMPTZ,PRIMARY KEY(request_id,generation,sequence)); CREATE INDEX job_artifact_cleanup ON job_artifacts(delete_after,request_id,generation,sequence) WHERE delete_after IS NOT NULL;")?;
    Ok(())
}
fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn lock_writer(
    tx: &mut Transaction<'_>,
    token: &ExecutionToken,
) -> Result<(String, crate::job_policy::AcceptedJobPolicy)> {
    let r=tx.query_one("SELECT r.namespace_id,r.state,j.policy_json,j.sql_completion,j.result_state,j.binding_generation,j.artifact_generation,r.worker_id,r.worker_boot_id FROM query_jobs j JOIN requests r USING(request_id) WHERE request_id=$1 FOR UPDATE OF j",&[&token.request_id.to_string()])?;
    ensure!(
        r.get::<_, String>(1) == "executing"
            && r.get::<_, i64>(5) == token.binding_generation
            && r.get::<_, String>(6) == token.artifact_generation.to_string()
            && r.get::<_, Option<String>>(7).as_deref() == Some(&token.epoch.worker_id)
            && r.get::<_, Option<String>>(8).as_deref() == Some(&token.epoch.boot_id.to_string()),
        "stale artifact writer"
    );
    ensure!(
        matches!(r.get::<_, &str>(4), "pending" | "writing"),
        "result writer closed"
    );
    Ok((r.get(0), serde_json::from_str(r.get::<_, &str>(2))?))
}
fn put_in(
    tx: &mut Transaction<'_>,
    token: &ExecutionToken,
    d: &BlobDescriptor,
    manifest: bool,
) -> Result<PutIntent> {
    ensure!(
        valid_hash(&d.sha256)
            && d.bytes > 0
            && d.bytes <= if manifest { 2 * 1024 * 1024 } else { MAX_BYTES }
            && (manifest || d.sequence < MANIFEST_SEQUENCE),
        "invalid bounded artifact descriptor"
    );
    let (namespace, policy) = lock_writer(tx, token)?;
    let total=tx.query_one("SELECT COUNT(*)::BIGINT,COALESCE(SUM(bytes),0)::BIGINT,COALESCE(SUM(rows),0)::BIGINT FROM job_artifacts WHERE request_id=$1 AND generation=$2 AND sequence<4096",&[&token.request_id.to_string(),&token.artifact_generation.to_string()])?;
    if !manifest {
        ensure!(
            total.get::<_, i64>(0) == i64::from(d.sequence)
                && (total.get::<_, i64>(1) as u64)
                    .checked_add(d.bytes)
                    .is_some_and(|v| v <= policy.limits.result_bytes)
                && (total.get::<_, i64>(2) as u64)
                    .checked_add(d.rows)
                    .is_some_and(|v| v <= policy.limits.result_rows),
            "artifact result limits exceeded"
        );
    } else {
        let r=tx.query_one("SELECT sql_completion,result_expires_at>clock_timestamp() FROM query_jobs WHERE request_id=$1",&[&token.request_id.to_string()])?;
        ensure!(
            r.get::<_, &str>(0) == "succeeded",
            "manifest requires checked SQL completion"
        );
        if r.get::<_, Option<bool>>(1) != Some(true) {
            return Err(ArtifactExpired.into());
        }
    }
    let sequence = if manifest {
        MANIFEST_SEQUENCE
    } else {
        d.sequence
    };
    let relative_key = format!(
        "{namespace}/{}/{}/{}",
        token.request_id,
        token.artifact_generation,
        if manifest {
            "manifest.json".to_string()
        } else {
            format!("chunk-{sequence}.arrow")
        }
    );
    let upload_token = Uuid::new_v4();
    tx.execute("INSERT INTO job_artifacts(request_id,generation,sequence,upload_token,relative_key,rows,bytes,sha256,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'intended')",&[&token.request_id.to_string(),&token.artifact_generation.to_string(),&i64::from(sequence),&upload_token.to_string(),&relative_key,&i64::try_from(d.rows)?,&i64::try_from(d.bytes)?,&d.sha256])?;
    tx.execute(
        "UPDATE query_jobs SET result_state='writing' WHERE request_id=$1",
        &[&token.request_id.to_string()],
    )?;
    Ok(PutIntent {
        request_id: token.request_id,
        generation: token.artifact_generation,
        sequence,
        upload_token,
        relative_key,
        bytes: d.bytes,
        sha256: d.sha256.clone(),
    })
}
pub(crate) fn intend_chunk(
    client: &mut Client,
    token: &ExecutionToken,
    d: &BlobDescriptor,
) -> Result<PutIntent> {
    let mut tx = client.transaction()?;
    jobs::bounded(&mut tx)?;
    let p = put_in(&mut tx, token, d, false)?;
    tx.commit()?;
    Ok(p)
}
pub(crate) fn intend_manifest(
    client: &mut Client,
    token: &ExecutionToken,
    bytes: u64,
    sha256: &str,
) -> Result<PutIntent> {
    let mut tx = client.transaction()?;
    jobs::bounded(&mut tx)?;
    let p = put_in(
        &mut tx,
        token,
        &BlobDescriptor {
            sequence: MANIFEST_SEQUENCE,
            rows: 0,
            bytes,
            sha256: sha256.into(),
        },
        true,
    )?;
    tx.commit()?;
    Ok(p)
}
pub(crate) fn ack_upload(
    client: &mut Client,
    token: &ExecutionToken,
    intent: &PutIntent,
) -> Result<()> {
    let mut tx = client.transaction()?;
    jobs::bounded(&mut tx)?;
    lock_writer(&mut tx, token)?;
    ensure!(
        intent.request_id == token.request_id && intent.generation == token.artifact_generation,
        "artifact token mismatch"
    );
    let n=tx.execute("UPDATE job_artifacts SET state='uploaded',uploaded_at=clock_timestamp() WHERE request_id=$1 AND generation=$2 AND sequence=$3 AND upload_token=$4 AND state='intended'",&[&intent.request_id.to_string(),&intent.generation.to_string(),&i64::from(intent.sequence),&intent.upload_token.to_string()])?;
    ensure!(n == 1, "artifact upload acknowledgement is stale");
    tx.commit()?;
    Ok(())
}
fn parts_in(tx: &mut Transaction<'_>, token: &ExecutionToken) -> Result<ManifestParts> {
    lock_writer(tx, token)?;
    let row = tx.query_one(
        "SELECT sql_completion,result_expires_at FROM query_jobs WHERE request_id=$1",
        &[&token.request_id.to_string()],
    )?;
    ensure!(
        row.get::<_, &str>(0) == "succeeded",
        "SQL completion has not been verified"
    );
    let expires_at = row
        .get::<_, Option<SystemTime>>(1)
        .ok_or_else(|| anyhow::anyhow!("result has no expiry"))?;
    let rows=tx.query("SELECT sequence,relative_key,rows,bytes,sha256,state FROM job_artifacts WHERE request_id=$1 AND generation=$2 AND sequence<4096 ORDER BY sequence LIMIT 4097",&[&token.request_id.to_string(),&token.artifact_generation.to_string()])?;
    ensure!(rows.len() <= 4096, "too many chunks");
    let mut chunks = Vec::with_capacity(rows.len());
    let (mut total_rows, mut total_bytes) = (0u64, 0u64);
    for (i, r) in rows.into_iter().enumerate() {
        ensure!(
            r.get::<_, i64>(0) == i as i64 && r.get::<_, &str>(5) == "uploaded",
            "artifact journal is incomplete"
        );
        let c = ChunkRef {
            sequence: i as u32,
            relative_key: r.get(1),
            rows: u64::try_from(r.get::<_, i64>(2))?,
            bytes: u64::try_from(r.get::<_, i64>(3))?,
            sha256: r.get(4),
        };
        total_rows = total_rows
            .checked_add(c.rows)
            .ok_or_else(|| anyhow::anyhow!("row count overflow"))?;
        total_bytes = total_bytes
            .checked_add(c.bytes)
            .ok_or_else(|| anyhow::anyhow!("byte count overflow"))?;
        chunks.push(c);
    }
    Ok(ManifestParts {
        request_id: token.request_id,
        generation: token.artifact_generation,
        chunks,
        rows: total_rows,
        bytes: total_bytes,
        expires_at,
    })
}
pub(crate) fn manifest_parts(client: &mut Client, token: &ExecutionToken) -> Result<ManifestParts> {
    let mut tx = client.transaction()?;
    jobs::bounded(&mut tx)?;
    let p = parts_in(&mut tx, token)?;
    tx.commit()?;
    Ok(p)
}
pub(crate) fn verified_manifest(
    client: &mut Client,
    token: &ExecutionToken,
    intent: &PutIntent,
) -> Result<VerifiedManifest> {
    let mut tx = client.transaction()?;
    jobs::bounded(&mut tx)?;
    let p = parts_in(&mut tx, token)?;
    ensure!(
        intent.request_id == token.request_id
            && intent.generation == token.artifact_generation
            && intent.sequence == MANIFEST_SEQUENCE,
        "manifest token mismatch"
    );
    ensure!(tx.query_opt("SELECT sequence FROM job_artifacts WHERE request_id=$1 AND generation=$2 AND sequence=4096 AND upload_token=$3 AND sha256=$4 AND state='uploaded'",&[&token.request_id.to_string(),&token.artifact_generation.to_string(),&intent.upload_token.to_string(),&intent.sha256])?.is_some(),"manifest upload is unconfirmed");
    tx.commit()?;
    Ok(VerifiedManifest {
        request_id: token.request_id,
        generation: token.artifact_generation,
        digest: intent.sha256.clone(),
        rows: p.rows,
        bytes: p.bytes,
    })
}
/// Resolve fresh policy in the caller's transaction, then authorize the exact object.
pub(crate) fn authorize_in(
    tx: &mut Transaction<'_>,
    access: &Access,
    id: Uuid,
    chunk: Option<u32>,
) -> Result<ReadObject, ApiError> {
    let row=jobs::api(tx.query_opt("SELECT r.owner_principal_id,j.result_state,j.result_expires_at<=clock_timestamp(),j.artifact_generation FROM requests r JOIN query_jobs j USING(request_id) WHERE r.request_id=$1 AND r.namespace_id=$2",&[&id.to_string(),&access.actor.namespace_id]))?.ok_or_else(||ApiError::new(failure::NOT_FOUND,"job not found"))?;
    access
        .require(
            Action::ResultRead,
            row.get::<_, Option<String>>(0).as_deref(),
        )
        .map_err(|_| ApiError::new(failure::NOT_FOUND, "job not found"))?;
    if row.get::<_, Option<bool>>(2) == Some(true) || row.get::<_, &str>(1) == "expired" {
        return Err(ApiError::new(failure::RESULT_EXPIRED, "job result expired"));
    }
    match row.get::<_, &str>(1) {
        "ready" => {}
        "pending" | "writing" => {
            return Err(ApiError::new(
                failure::RESULT_PENDING,
                "job result is pending",
            ));
        }
        _ => {
            return Err(ApiError::new(
                failure::RESULT_UNAVAILABLE,
                "job result is unavailable",
            ));
        }
    }
    let sequence = chunk.unwrap_or(MANIFEST_SEQUENCE);
    if chunk.is_some() && sequence >= MANIFEST_SEQUENCE {
        return Err(ApiError::new(failure::NOT_FOUND, "chunk not found"));
    }
    let r=jobs::api(tx.query_opt("SELECT relative_key,bytes,sha256 FROM job_artifacts WHERE request_id=$1 AND generation=$2 AND sequence=$3 AND state='uploaded'",&[&id.to_string(),&row.get::<_,String>(3),&i64::from(sequence)]))?.ok_or_else(||ApiError::new(failure::NOT_FOUND,"chunk not found"))?;
    Ok(ReadObject {
        request_id: id,
        generation: jobs::api(Uuid::parse_str(row.get::<_, &str>(3)))?,
        relative_key: r.get(0),
        bytes: jobs::api(u64::try_from(r.get::<_, i64>(1)))?,
        sha256: r.get(2),
    })
}
pub(crate) fn cleanup_in(tx: &mut Transaction<'_>, id: &str) -> Result<()> {
    tx.execute("UPDATE job_artifacts SET state='deleting',delete_after=COALESCE(delete_after,clock_timestamp()) WHERE request_id=$1 AND state IN ('intended','uploaded')",&[&id])?;
    Ok(())
}
/// No object listing: bounded metadata candidates, with a durable deletion tombstone.
pub(crate) fn deletion_candidates(
    client: &mut Client,
    lease: &crate::platform::Lease,
    config: &crate::platform::MaintenanceConfig,
    limit: u32,
) -> Result<Vec<DeleteIntent>> {
    let mut tx = crate::platform::fenced_transaction(client, lease, config)?;
    let expired=tx.query("SELECT request_id FROM query_jobs WHERE result_state='ready' AND result_expires_at<=clock_timestamp() ORDER BY result_expires_at,request_id LIMIT 64 FOR UPDATE SKIP LOCKED",&[])?;
    for r in expired {
        let id: String = r.get(0);
        tx.execute("UPDATE query_jobs SET result_state='expired',result_failure_kind='result_expired' WHERE request_id=$1",&[&id])?;
        cleanup_in(&mut tx, &id)?;
    }
    let rows=tx.query("SELECT request_id,generation,sequence,relative_key FROM job_artifacts WHERE delete_after<=clock_timestamp() ORDER BY delete_after,request_id,generation,sequence LIMIT $1",&[&i64::from(limit.min(64))])?;
    let intents = rows
        .into_iter()
        .map(|r| {
            Ok(DeleteIntent {
                request_id: Uuid::parse_str(r.get::<_, &str>(0))?,
                generation: Uuid::parse_str(r.get::<_, &str>(1))?,
                sequence: u32::try_from(r.get::<_, i64>(2))?,
                relative_key: r.get(3),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Reserve a retry time before IO. A permanently failing first key cannot
    // monopolize the next bounded pass, and abandoned batches remain retryable.
    for intent in &intents {
        crate::queue::lock_job_in(&mut tx, &intent.request_id.to_string())?;
        tx.execute("UPDATE job_artifacts SET delete_after=clock_timestamp()+interval '60 seconds',delete_attempts=delete_attempts+1 WHERE request_id=$1 AND generation=$2 AND sequence=$3",&[&intent.request_id.to_string(),&intent.generation.to_string(),&i64::from(intent.sequence)])?;
    }
    tx.commit()?;
    Ok(intents)
}
pub(crate) fn ack_delete(
    client: &mut Client,
    lease: &crate::platform::Lease,
    config: &crate::platform::MaintenanceConfig,
    intent: &DeleteIntent,
) -> Result<()> {
    let mut tx = crate::platform::fenced_transaction(client, lease, config)?;
    crate::queue::lock_job_in(&mut tx, &intent.request_id.to_string())?;
    tx.execute("UPDATE job_artifacts SET state='deleted',last_deleted_at=clock_timestamp(),delete_after=clock_timestamp()+interval '1 day' WHERE request_id=$1 AND generation=$2 AND sequence=$3 AND state IN ('deleting','deleted')",&[&intent.request_id.to_string(),&intent.generation.to_string(),&i64::from(intent.sequence)])?;
    tx.commit()?;
    Ok(())
}
