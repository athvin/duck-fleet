use crate::{ApiError, authz, credentials::Identity, failure, jobs};
use postgres::{Client, Transaction};
use uuid::Uuid;

pub(crate) const MAX_IMPORT_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct RequestMetadata {
    pub id: Uuid,
    pub destination: String,
    pub mode: String,
    pub format: String,
    pub bytes: u64,
    pub sha256: String,
    pub options: String,
}

impl RequestMetadata {
    fn validate(&self) -> Result<(), ApiError> {
        if self.destination.is_empty()
            || self.destination.len() > 1024
            || !matches!(self.mode.as_str(), "create" | "append")
            || !matches!(self.format.as_str(), "csv" | "parquet")
            || self.bytes > MAX_IMPORT_BYTES
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.options.len() > 4096
            || serde_json::from_str::<serde_json::Value>(&self.options)
                .ok()
                .is_none_or(|value| !value.is_object())
        {
            return Err(ApiError::bad("invalid import metadata"));
        }
        Ok(())
    }
}

pub(crate) struct Staged {
    pub relative_key: String,
    pub expires_at: String,
}

pub(crate) struct DeleteIntent {
    pub id: Uuid,
    pub relative_key: String,
}

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> anyhow::Result<()> {
    tx.batch_execute(
        "CREATE TABLE import_requests(
            request_id TEXT PRIMARY KEY,
            namespace_id TEXT NOT NULL,
            owner_principal_id TEXT,
            owner_principal TEXT NOT NULL,
            destination TEXT NOT NULL,
            mode TEXT NOT NULL CHECK(mode IN ('create','append')),
            format TEXT NOT NULL CHECK(format IN ('csv','parquet')),
            state TEXT NOT NULL CHECK(state IN ('uploading','staged','deleting','deleted')),
            bytes BIGINT NOT NULL CHECK(bytes>=0),
            sha256 TEXT NOT NULL,
            relative_key TEXT NOT NULL UNIQUE,
            options_json TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            expires_at TIMESTAMPTZ NOT NULL,
            delete_after TIMESTAMPTZ,
            delete_attempts BIGINT NOT NULL DEFAULT 0,
            last_deleted_at TIMESTAMPTZ
        );
        CREATE INDEX import_cleanup ON import_requests(delete_after,expires_at,request_id)
            WHERE state <> 'deleted';",
    )?;
    Ok(())
}

pub(crate) fn begin(
    client: &mut Client,
    identity: &Identity,
    request: &RequestMetadata,
) -> Result<Staged, ApiError> {
    request.validate()?;
    let mut tx = client.transaction().map_err(|_| ApiError::internal())?;
    jobs::bounded(&mut tx).map_err(|_| ApiError::internal())?;
    let access = authz::resolve_in(&mut tx, identity)?;
    if tx
        .query_opt(
            "SELECT request_id FROM import_requests WHERE request_id=$1",
            &[&request.id.to_string()],
        )
        .map_err(|_| ApiError::internal())?
        .is_some()
    {
        return Err(ApiError::new(
            failure::DUPLICATE_REQUEST,
            "import request ID already exists; inspect it before uploading again",
        ));
    }
    let relative_key = format!(
        "imports/{}/{}/source",
        access.actor.namespace_id, request.id
    );
    tx.execute(
        "INSERT INTO import_requests(request_id,namespace_id,owner_principal_id,owner_principal,destination,mode,format,state,bytes,sha256,relative_key,options_json,expires_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,'uploading',$8,$9,$10,$11,clock_timestamp()+interval '15 minutes')",
        &[
            &request.id.to_string(),
            &access.actor.namespace_id,
            &access.actor.principal_id,
            &access.actor.principal,
            &request.destination,
            &request.mode,
            &request.format,
            &i64::try_from(request.bytes).map_err(|_| ApiError::internal())?,
            &request.sha256,
            &relative_key,
            &request.options,
        ],
    )
    .map_err(|_| ApiError::internal())?;
    let expires_at: String = tx
        .query_one(
            "SELECT expires_at::text FROM import_requests WHERE request_id=$1",
            &[&request.id.to_string()],
        )
        .map_err(|_| ApiError::internal())?
        .get(0);
    tx.commit().map_err(|_| ApiError::internal())?;
    Ok(Staged {
        relative_key,
        expires_at,
    })
}

fn owner_update(
    tx: &mut Transaction<'_>,
    identity: &Identity,
    id: Uuid,
    sql: &str,
) -> Result<u64, ApiError> {
    let access = authz::resolve_in(tx, identity)?;
    tx.execute(
        sql,
        &[
            &id.to_string(),
            &access.actor.namespace_id,
            &access.actor.principal_id,
            &access.actor.principal,
        ],
    )
    .map_err(|_| ApiError::internal())
}

pub(crate) fn detail(
    client: &mut Client,
    identity: &Identity,
    id: Uuid,
) -> Result<serde_json::Value, ApiError> {
    let mut tx = client.transaction().map_err(|_| ApiError::internal())?;
    jobs::bounded(&mut tx).map_err(|_| ApiError::internal())?;
    let access = authz::resolve_in(&mut tx, identity)?;
    let row = tx
        .query_opt(
            "SELECT destination,mode,format,state,bytes,sha256,expires_at::text FROM import_requests
             WHERE request_id=$1 AND namespace_id=$2
               AND (owner_principal_id=$3 OR (owner_principal_id IS NULL AND owner_principal=$4))",
            &[
                &id.to_string(),
                &access.actor.namespace_id,
                &access.actor.principal_id,
                &access.actor.principal,
            ],
        )
        .map_err(|_| ApiError::internal())?
        .ok_or_else(|| ApiError::new(failure::NOT_FOUND, "import not found"))?;
    Ok(serde_json::json!({
        "request_id":id,
        "destination":row.get::<_,String>(0),
        "mode":row.get::<_,String>(1),
        "format":row.get::<_,String>(2),
        "state":row.get::<_,String>(3),
        "bytes":row.get::<_,i64>(4),
        "sha256":row.get::<_,String>(5),
        "expires_at":row.get::<_,String>(6),
    }))
}

pub(crate) fn mark_staged(
    client: &mut Client,
    identity: &Identity,
    id: Uuid,
) -> Result<(), ApiError> {
    let mut tx = client.transaction().map_err(|_| ApiError::internal())?;
    jobs::bounded(&mut tx).map_err(|_| ApiError::internal())?;
    let updated = owner_update(
        &mut tx,
        identity,
        id,
        "UPDATE import_requests SET state='staged',updated_at=clock_timestamp()
         WHERE request_id=$1 AND namespace_id=$2
           AND (owner_principal_id=$3 OR (owner_principal_id IS NULL AND owner_principal=$4))
           AND state='uploading'",
    )?;
    if updated != 1 {
        return Err(ApiError::new(failure::NOT_FOUND, "import not found"));
    }
    tx.commit().map_err(|_| ApiError::internal())
}

pub(crate) fn mark_cleanup(
    client: &mut Client,
    identity: &Identity,
    id: Uuid,
) -> Result<Option<String>, ApiError> {
    let mut tx = client.transaction().map_err(|_| ApiError::internal())?;
    jobs::bounded(&mut tx).map_err(|_| ApiError::internal())?;
    let access = authz::resolve_in(&mut tx, identity)?;
    let row = tx
        .query_opt(
            "SELECT relative_key,state FROM import_requests
             WHERE request_id=$1 AND namespace_id=$2
               AND (owner_principal_id=$3 OR (owner_principal_id IS NULL AND owner_principal=$4))
             FOR UPDATE",
            &[
                &id.to_string(),
                &access.actor.namespace_id,
                &access.actor.principal_id,
                &access.actor.principal,
            ],
        )
        .map_err(|_| ApiError::internal())?
        .ok_or_else(|| ApiError::new(failure::NOT_FOUND, "import not found"))?;
    if row.get::<_, &str>(1) == "deleted" {
        return Ok(None);
    }
    let key: String = row.get(0);
    tx.execute(
        "UPDATE import_requests SET state='deleting',delete_after=clock_timestamp(),updated_at=clock_timestamp() WHERE request_id=$1",
        &[&id.to_string()],
    )
    .map_err(|_| ApiError::internal())?;
    tx.commit().map_err(|_| ApiError::internal())?;
    Ok(Some(key))
}

pub(crate) fn mark_failed(client: &mut Client, id: Uuid) -> Result<(), ApiError> {
    client
        .execute(
            "UPDATE import_requests SET state='deleting',delete_after=clock_timestamp(),updated_at=clock_timestamp() WHERE request_id=$1 AND state='uploading'",
            &[&id.to_string()],
        )
        .map_err(|_| ApiError::internal())?;
    Ok(())
}

pub(crate) fn ack_delete(
    client: &mut Client,
    lease: &crate::platform::Lease,
    config: &crate::platform::MaintenanceConfig,
    intent: &DeleteIntent,
) -> anyhow::Result<()> {
    let mut tx = crate::platform::fenced_transaction(client, lease, config)?;
    tx.execute(
        "UPDATE import_requests SET state='deleted',last_deleted_at=clock_timestamp(),delete_after=clock_timestamp()+interval '1 day' WHERE request_id=$1",
        &[&intent.id.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn ack_delete_direct(client: &mut Client, id: Uuid) -> Result<(), ApiError> {
    client
        .execute(
            "UPDATE import_requests SET state='deleted',last_deleted_at=clock_timestamp(),delete_after=clock_timestamp()+interval '1 day' WHERE request_id=$1",
            &[&id.to_string()],
        )
        .map_err(|_| ApiError::internal())?;
    Ok(())
}

pub(crate) fn deletion_candidates(
    client: &mut Client,
    lease: &crate::platform::Lease,
    config: &crate::platform::MaintenanceConfig,
    limit: u32,
) -> anyhow::Result<Vec<DeleteIntent>> {
    let mut tx = crate::platform::fenced_transaction(client, lease, config)?;
    tx.execute(
        "UPDATE import_requests SET state='deleting',delete_after=clock_timestamp()
         WHERE state IN ('uploading','staged') AND expires_at<=clock_timestamp()",
        &[],
    )?;
    let rows = tx.query(
        "SELECT request_id,relative_key FROM import_requests
         WHERE state='deleting' AND delete_after<=clock_timestamp()
         ORDER BY delete_after,request_id LIMIT $1",
        &[&i64::from(limit.min(64))],
    )?;
    let intents = rows
        .into_iter()
        .map(|row| {
            Ok(DeleteIntent {
                id: Uuid::parse_str(row.get::<_, &str>(0))?,
                relative_key: row.get(1),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for intent in &intents {
        tx.execute(
            "UPDATE import_requests SET delete_after=clock_timestamp()+interval '60 seconds',delete_attempts=delete_attempts+1 WHERE request_id=$1",
            &[&intent.id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(intents)
}
