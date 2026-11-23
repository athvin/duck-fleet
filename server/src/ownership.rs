//! Durable gateway leases and fencing, separate from DuckDB worker ownership.
use crate::{ApiError, failure};
use postgres::{Client, Transaction};
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> anyhow::Result<()> {
    tx.batch_execute(
        "CREATE TABLE IF NOT EXISTS gateway_sessions (
            session_id TEXT PRIMARY KEY,
            gateway_id TEXT NOT NULL,
            gateway_epoch TEXT NOT NULL,
            lease_until TIMESTAMPTZ NOT NULL,
            worker_id TEXT NOT NULL,
            worker_boot_id TEXT NOT NULL,
            principal_id TEXT,
            placement TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS gateway_sessions_expiry ON gateway_sessions(lease_until);",
    )?;
    Ok(())
}

pub(crate) struct Registration<'a> {
    pub session_id: Uuid,
    pub gateway_id: &'a str,
    pub epoch: Uuid,
    pub worker_id: &'a str,
    pub boot_id: &'a str,
    pub principal_id: Option<&'a str>,
    pub placement: &'a Value,
}

pub(crate) fn register(
    tx: &mut Transaction<'_>,
    registration: &Registration<'_>,
) -> Result<(), ApiError> {
    tx.execute(
        "INSERT INTO gateway_sessions(session_id,gateway_id,gateway_epoch,lease_until,worker_id,worker_boot_id,principal_id,placement)
         VALUES($1,$2,$3,now()+interval '15 seconds',$4,$5,$6,$7)",
        &[&registration.session_id.to_string(),&registration.gateway_id,&registration.epoch.to_string(),&registration.worker_id,&registration.boot_id,&registration.principal_id,&registration.placement.to_string()],
    ).map_err(|_| ApiError::internal())?;
    Ok(())
}

pub(crate) fn renew(client: &mut Client, gateway_id: &str, epoch: Uuid) -> Result<u64, ApiError> {
    client
        .execute(
            "UPDATE gateway_sessions SET lease_until=now()+interval '15 seconds',updated_at=now()
         WHERE gateway_id=$1 AND gateway_epoch=$2 AND lease_until>now()",
            &[&gateway_id, &epoch.to_string()],
        )
        .map_err(|_| ApiError::internal())
}

pub(crate) fn close_permitted(
    client: &mut Client,
    session_id: Uuid,
    gateway_id: &str,
    epoch: Uuid,
) -> Result<(), ApiError> {
    let owner = client.query_opt(
        "SELECT gateway_id,gateway_epoch,lease_until>now() FROM gateway_sessions WHERE session_id=$1",
        &[&session_id.to_string()],
    ).map_err(|_| ApiError::internal())?;
    if owner.is_some_and(|row| {
        row.get::<_, String>(0) != gateway_id
            || row.get::<_, String>(1) != epoch.to_string()
            || !row.get::<_, bool>(2)
    }) {
        return Err(ApiError::new(
            failure::UNKNOWN_SESSION,
            "session is owned by another gateway or its lease expired; close through the live owner",
        ));
    }
    Ok(())
}

pub(crate) fn close(
    client: &mut Client,
    session_id: Uuid,
    gateway_id: &str,
    epoch: Uuid,
) -> Result<(), ApiError> {
    let changed = client.execute(
        "DELETE FROM gateway_sessions WHERE session_id=$1
           AND ((gateway_id=$2 AND gateway_epoch=$3 AND lease_until>now())
             OR EXISTS(SELECT 1 FROM queue_sessions WHERE session_id=$1 AND closed_at IS NOT NULL))",
        &[&session_id.to_string(),&gateway_id,&epoch.to_string()],
    ).map_err(|_| ApiError::internal())?;
    if changed != 1 {
        return Err(ApiError::new(
            failure::UNKNOWN_SESSION,
            "session ownership is stale or expired",
        ));
    }
    Ok(())
}

pub(crate) fn expired_workers(client: &mut Client, prefix: &str) -> Result<Vec<String>, ApiError> {
    let rows = client.query(
        "SELECT DISTINCT worker_id FROM gateway_sessions WHERE lease_until<=now() AND starts_with(worker_id,$1)
         UNION
         SELECT worker_id FROM queue_sessions q WHERE closed_at IS NULL AND starts_with(worker_id,$1)
           AND opened_at<now()-interval '30 seconds'
           AND NOT EXISTS(SELECT 1 FROM gateway_sessions g WHERE g.session_id=q.session_id)",
        &[&prefix],
    ).map_err(|_| ApiError::internal())?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

pub(crate) struct LiveOwner {
    pub worker_id: String,
    pub principal_id: Option<String>,
    pub placement: Value,
    pub operation: String,
}

pub(crate) fn live_workers(client: &mut Client, prefix: &str) -> Result<Vec<String>, ApiError> {
    let rows = client
        .query(
            "SELECT DISTINCT worker_id FROM gateway_sessions WHERE lease_until>now() AND starts_with(worker_id,$1)",
            &[&prefix],
        )
        .map_err(|_| ApiError::internal())?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

pub(crate) fn live_owner(
    client: &mut Client,
    session_id: Uuid,
) -> Result<Option<LiveOwner>, ApiError> {
    let row = client.query_opt(
        "SELECT g.worker_id,g.worker_boot_id,g.principal_id,g.placement,q.operation FROM gateway_sessions g
         JOIN queue_sessions q ON q.session_id=g.session_id
         WHERE g.session_id=$1 AND g.lease_until>now()",
        &[&session_id.to_string()],
    ).map_err(|_| ApiError::internal())?;
    Ok(row.map(|row| LiveOwner {
        worker_id: row.get(0),
        principal_id: row.get(2),
        placement: serde_json::from_str(row.get(3)).unwrap_or(Value::Null),
        operation: row.get(4),
    }))
}
