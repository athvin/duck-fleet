//! Provisioned-compute admission and lifecycle accounting, separate from SQL billing.
use crate::{ApiError, compute::Resources, failure};
use postgres::{Client, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

const LOCK: i64 = 4_283_107;

fn database_error(error: postgres::Error) -> ApiError {
    tracing::warn!(%error, "compute lifecycle database failure");
    ApiError::internal()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Policy {
    pub max_cpu: Option<u16>,
    pub max_memory_bytes: Option<u64>,
    pub max_disk_bytes: Option<u64>,
    pub max_principal_cpu: Option<u16>,
    pub max_principal_memory_bytes: Option<u64>,
    pub max_principal_disk_bytes: Option<u64>,
    pub idle_timeout_seconds: Option<u64>,
    pub max_lifetime_seconds: Option<u64>,
}

impl Policy {
    pub fn from_env() -> anyhow::Result<Self> {
        let policy: Self = serde_json::from_str(
            &std::env::var("DUCKFLEET_COMPUTE_POLICY_JSON").unwrap_or_else(|_| "{}".into()),
        )?;
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("max_memory_bytes", self.max_memory_bytes),
            ("max_disk_bytes", self.max_disk_bytes),
            (
                "max_principal_memory_bytes",
                self.max_principal_memory_bytes,
            ),
            ("max_principal_disk_bytes", self.max_principal_disk_bytes),
        ] {
            if let Some(bytes) = value {
                let resources = Resources::parse("1", &format!("{bytes}"), "1Gi")
                    .map_err(|reason| anyhow::anyhow!("{name}: {reason}"))?;
                anyhow::ensure!(resources.memory_bytes >= 256 * 1024 * 1024);
            }
        }
        for (name, value) in [
            ("idle_timeout_seconds", self.idle_timeout_seconds),
            ("max_lifetime_seconds", self.max_lifetime_seconds),
        ] {
            if let Some(seconds) = value {
                anyhow::ensure!(seconds > 0 && seconds <= 86_400, "{name} must be 1-86400");
            }
        }
        Ok(())
    }

    fn allows(&self, resources: Resources, principal: bool) -> bool {
        let (cpu, memory, disk) = if principal {
            (
                self.max_principal_cpu,
                self.max_principal_memory_bytes,
                self.max_principal_disk_bytes,
            )
        } else {
            (self.max_cpu, self.max_memory_bytes, self.max_disk_bytes)
        };
        cpu.is_none_or(|limit| resources.cpu <= limit)
            && memory.is_none_or(|limit| resources.memory_bytes <= limit)
            && disk.is_none_or(|limit| resources.disk_bytes <= limit)
    }
}

pub(crate) fn migrate(tx: &mut Transaction<'_>) -> anyhow::Result<()> {
    tx.batch_execute(
        "CREATE TABLE IF NOT EXISTS compute_lifecycles (
            lifecycle_id TEXT PRIMARY KEY,
            worker_id TEXT,
            boot_id TEXT,
            principal_id TEXT,
            pool TEXT NOT NULL,
            state TEXT NOT NULL,
            failure_kind TEXT,
            cpu BIGINT NOT NULL,
            memory_bytes BIGINT NOT NULL,
            disk_bytes BIGINT NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL,
            provisioning_started_at TIMESTAMPTZ,
            ready_at TIMESTAMPTZ,
            last_activity_at TIMESTAMPTZ NOT NULL,
            busy_since TIMESTAMPTZ,
            terminating_at TIMESTAMPTZ,
            terminated_at TIMESTAMPTZ,
            execution_ms BIGINT NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS compute_lifecycle_active ON compute_lifecycles(pool) WHERE terminated_at IS NULL;
        CREATE INDEX IF NOT EXISTS compute_lifecycle_owner ON compute_lifecycles(worker_id,boot_id);
        CREATE TABLE IF NOT EXISTS compute_reservations (
            lifecycle_id TEXT PRIMARY KEY,
            principal_id TEXT,
            pool TEXT NOT NULL,
            cpu BIGINT NOT NULL,
            memory_bytes BIGINT NOT NULL,
            disk_bytes BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            released_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS compute_reservations_active ON compute_reservations(pool) WHERE released_at IS NULL;
        CREATE INDEX IF NOT EXISTS compute_reservations_principal ON compute_reservations(principal_id) WHERE released_at IS NULL;
        CREATE TABLE IF NOT EXISTS compute_policies (
            scope TEXT PRIMARY KEY,
            policy TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );",
    )?;
    Ok(())
}

pub(crate) fn publish_policy(client: &mut Client, policy: &Policy) -> Result<(), ApiError> {
    let mut tx = client.transaction().map_err(database_error)?;
    tx.execute(
        "INSERT INTO compute_policies(scope,policy) VALUES('default',$1)
         ON CONFLICT(scope) DO UPDATE SET policy=$1,updated_at=now()",
        &[&serde_json::to_string(policy).map_err(|_| ApiError::internal())?],
    )
    .map_err(|_| ApiError::internal())?;
    tx.commit().map_err(database_error)
}

fn policy(tx: &mut Transaction<'_>) -> Result<Policy, ApiError> {
    let row = tx
        .query_opt(
            "SELECT policy FROM compute_policies WHERE scope='default'",
            &[],
        )
        .map_err(|_| ApiError::internal())?;
    match row {
        Some(row) => serde_json::from_str(row.get(0)).map_err(|_| ApiError::internal()),
        None => Ok(Policy::default()),
    }
}

fn check_single(policy: &Policy, resources: Resources, principal: bool) -> Result<(), ApiError> {
    if policy.allows(resources, principal) {
        Ok(())
    } else {
        Err(ApiError::new(
            failure::CAPACITY,
            if principal {
                "principal compute allowance exceeded before provisioning"
            } else {
                "deployment compute ceiling exceeded before provisioning"
            },
        ))
    }
}

pub(crate) fn reserve(
    tx: &mut Transaction<'_>,
    lifecycle_id: &str,
    principal_id: Option<&str>,
    pool: &str,
    resources: Resources,
) -> Result<(), ApiError> {
    tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&LOCK])
        .map_err(database_error)?;
    let existing = tx.query_opt(
        "SELECT l.lifecycle_id FROM compute_lifecycles l JOIN compute_reservations r USING(lifecycle_id)
         WHERE l.lifecycle_id=$1 AND l.principal_id IS NOT DISTINCT FROM $2
           AND l.pool=$3 AND l.cpu=$4 AND l.memory_bytes=$5 AND l.disk_bytes=$6
           AND l.terminated_at IS NULL AND r.released_at IS NULL FOR UPDATE OF l",
        &[&lifecycle_id, &principal_id, &pool, &i64::from(resources.cpu), &(resources.memory_bytes as i64), &(resources.disk_bytes as i64)],
    ).map_err(database_error)?;
    if existing.is_some() {
        return Ok(());
    }
    if tx
        .query_opt(
            "SELECT lifecycle_id FROM compute_lifecycles WHERE lifecycle_id=$1",
            &[&lifecycle_id],
        )
        .map_err(database_error)?
        .is_some()
    {
        return Err(ApiError::internal());
    }
    let policy = policy(tx)?;
    check_single(&policy, resources, false)?;
    check_single(&policy, resources, true)?;
    let totals = tx.query_one(
        "SELECT COALESCE(sum(cpu),0)::BIGINT,COALESCE(sum(memory_bytes),0)::BIGINT,COALESCE(sum(disk_bytes),0)::BIGINT FROM compute_reservations WHERE released_at IS NULL",
        &[],
    ).map_err(database_error)?;
    let total = Resources {
        cpu: u16::try_from(totals.get::<_, i64>(0))
            .unwrap_or(u16::MAX)
            .saturating_add(resources.cpu),
        memory_bytes: (totals.get::<_, i64>(1) as u64).saturating_add(resources.memory_bytes),
        disk_bytes: (totals.get::<_, i64>(2) as u64).saturating_add(resources.disk_bytes),
    };
    check_single(&policy, total, false)?;
    if let Some(principal_id) = principal_id {
        let totals = tx.query_one(
            "SELECT COALESCE(sum(cpu),0)::BIGINT,COALESCE(sum(memory_bytes),0)::BIGINT,COALESCE(sum(disk_bytes),0)::BIGINT FROM compute_reservations WHERE released_at IS NULL AND principal_id=$1",
            &[&principal_id],
        ).map_err(database_error)?;
        let total = Resources {
            cpu: u16::try_from(totals.get::<_, i64>(0))
                .unwrap_or(u16::MAX)
                .saturating_add(resources.cpu),
            memory_bytes: (totals.get::<_, i64>(1) as u64).saturating_add(resources.memory_bytes),
            disk_bytes: (totals.get::<_, i64>(2) as u64).saturating_add(resources.disk_bytes),
        };
        check_single(&policy, total, true)?;
    }
    tx.execute(
        "INSERT INTO compute_lifecycles(lifecycle_id,principal_id,pool,state,cpu,memory_bytes,disk_bytes,requested_at,provisioning_started_at,last_activity_at)
         VALUES($1,$2,$3,'provisioning',$4,$5,$6,now(),now(),now())",
        &[&lifecycle_id, &principal_id, &pool, &i64::from(resources.cpu), &(resources.memory_bytes as i64), &(resources.disk_bytes as i64)],
    ).map_err(database_error)?;
    tx.execute(
        "INSERT INTO compute_reservations(lifecycle_id,principal_id,pool,cpu,memory_bytes,disk_bytes,created_at)
         VALUES($1,$2,$3,$4,$5,$6,now())",
        &[&lifecycle_id, &principal_id, &pool, &i64::from(resources.cpu), &(resources.memory_bytes as i64), &(resources.disk_bytes as i64)],
    ).map_err(database_error)?;
    Ok(())
}

pub(crate) fn ready(
    client: &mut Client,
    lifecycle_id: &str,
    worker_id: &str,
    boot_id: &str,
) -> Result<(), ApiError> {
    let changed = client.execute(
        "UPDATE compute_lifecycles SET worker_id=$2,boot_id=$3,state='ready',ready_at=now(),last_activity_at=now()
         WHERE lifecycle_id=$1 AND terminated_at IS NULL",
        &[&lifecycle_id, &worker_id, &boot_id],
    ).map_err(database_error)?;
    if changed != 1 {
        let state = client
            .query_opt(
                "SELECT l.state,l.terminated_at IS NOT NULL,r.released_at IS NOT NULL
                 FROM compute_lifecycles l LEFT JOIN compute_reservations r USING(lifecycle_id)
                 WHERE l.lifecycle_id=$1",
                &[&lifecycle_id],
            )
            .ok()
            .flatten()
            .map(|row| {
                format!(
                    "{}; terminated={}; reservation_released={:?}",
                    row.get::<_, String>(0),
                    row.get::<_, bool>(1),
                    row.get::<_, Option<bool>>(2)
                )
            });
        tracing::warn!(
            lifecycle_id,
            worker_id,
            state,
            "compute lifecycle reservation is missing or terminated"
        );
        return Err(ApiError::internal());
    }
    Ok(())
}

pub(crate) fn activity(
    client: &mut Client,
    lifecycle_id: &str,
    busy: bool,
) -> Result<(), ApiError> {
    if busy {
        client.execute(
            "UPDATE compute_lifecycles SET state='busy',busy_since=COALESCE(busy_since,now()),last_activity_at=now()
             WHERE lifecycle_id=$1 AND terminated_at IS NULL",
            &[&lifecycle_id],
        ).map_err(database_error)?;
    } else {
        client.execute(
            "UPDATE compute_lifecycles SET state='ready',execution_ms=execution_ms+COALESCE(EXTRACT(epoch FROM now()-busy_since)*1000,0),busy_since=NULL,last_activity_at=now()
             WHERE lifecycle_id=$1 AND terminated_at IS NULL",
            &[&lifecycle_id],
        ).map_err(database_error)?;
    }
    Ok(())
}

pub(crate) fn finish(
    client: &mut Client,
    lifecycle_id: &str,
    failure_kind: Option<&str>,
) -> Result<(), ApiError> {
    let mut tx = client.transaction().map_err(database_error)?;
    tx.execute(
        "UPDATE compute_lifecycles SET state='terminated',failure_kind=COALESCE(failure_kind,$2),terminating_at=COALESCE(terminating_at,now()),terminated_at=now(),last_activity_at=now(),
            execution_ms=execution_ms+COALESCE(EXTRACT(epoch FROM now()-busy_since)*1000,0),busy_since=NULL
         WHERE lifecycle_id=$1 AND terminated_at IS NULL",
        &[&lifecycle_id, &failure_kind],
    ).map_err(database_error)?;
    tx.execute(
        "UPDATE compute_reservations SET released_at=now() WHERE lifecycle_id=$1 AND released_at IS NULL",
        &[&lifecycle_id],
    ).map_err(database_error)?;
    tx.commit().map_err(database_error)
}

pub(crate) fn finish_worker(client: &mut Client, worker_id: &str) -> Result<(), ApiError> {
    let mut tx = client.transaction().map_err(database_error)?;
    tx.execute(
        "UPDATE compute_lifecycles SET state='terminated',failure_kind='worker_restarted',terminating_at=COALESCE(terminating_at,now()),terminated_at=now(),last_activity_at=now(),
            execution_ms=execution_ms+COALESCE(EXTRACT(epoch FROM now()-busy_since)*1000,0),busy_since=NULL
         WHERE worker_id=$1 AND terminated_at IS NULL",
        &[&worker_id],
    ).map_err(database_error)?;
    tx.execute(
        "UPDATE compute_reservations SET released_at=now() WHERE released_at IS NULL AND lifecycle_id IN
            (SELECT lifecycle_id FROM compute_lifecycles WHERE worker_id=$1 AND state='terminated')",
        &[&worker_id],
    ).map_err(database_error)?;
    tx.commit().map_err(database_error)
}

pub(crate) fn report(
    client: &mut Client,
    rate: &crate::pricing::RateCard,
) -> Result<Vec<Value>, ApiError> {
    let rows = client.query(
        "SELECT lifecycle_id,worker_id,boot_id,principal_id,pool,state,failure_kind,cpu,memory_bytes,disk_bytes,
            requested_at::TEXT,provisioning_started_at::TEXT,ready_at::TEXT,last_activity_at::TEXT,terminating_at::TEXT,terminated_at::TEXT,execution_ms,
            GREATEST(EXTRACT(epoch FROM COALESCE(ready_at,now())-requested_at)*1000,0)::BIGINT,
            GREATEST(EXTRACT(epoch FROM COALESCE(terminated_at,now())-COALESCE(ready_at,requested_at))*1000-execution_ms,0)::BIGINT,
            (EXTRACT(epoch FROM COALESCE(terminated_at,now())-requested_at)*cpu)::FLOAT8,
            (EXTRACT(epoch FROM COALESCE(terminated_at,now())-requested_at)*memory_bytes/1073741824.0)::FLOAT8,
            GREATEST(EXTRACT(epoch FROM COALESCE(terminated_at,now())-COALESCE(ready_at,requested_at))*1000,0)::BIGINT
         FROM compute_lifecycles ORDER BY requested_at DESC LIMIT 500",
        &[],
    ).map_err(database_error)?;
    Ok(rows.iter().map(|row| {
        let state: String = row.get(5);
        let execution_ms: i64 = row.get(16);
        let cpu: i64 = row.get(7);
        let memory_bytes: i64 = row.get(8);
        let total_ms: i64 = row.get(21);
        let cpu_seconds: f64 = row.get(19);
        let gib_seconds: f64 = row.get(20);
        let estimated = crate::pricing::charge_at(
            rate.compute_micro_usd_per_unit_hour,
            crate::pricing::normalized_units(
                u16::try_from(cpu).unwrap_or(u16::MAX),
                u64::try_from(memory_bytes).unwrap_or(u64::MAX),
            ),
            u64::try_from(total_ms).unwrap_or(0),
        );
        json!({
            "lifecycle_id": row.get::<_,String>(0),
            "worker_id": row.get::<_,Option<String>>(1),
            "boot_id": row.get::<_,Option<String>>(2),
            "principal_id": row.get::<_,Option<String>>(3),
            "pool": row.get::<_,String>(4),
            "state": state,
            "failure_kind": row.get::<_,Option<String>>(6),
            "resources": {"cpu":cpu,"memory_bytes":memory_bytes,"disk_bytes":row.get::<_,i64>(9)},
            "requested_at": row.get::<_,String>(10),
            "provisioning_started_at": row.get::<_,Option<String>>(11),
            "ready_at": row.get::<_,Option<String>>(12),
            "last_activity_at": row.get::<_,String>(13),
            "terminating_at": row.get::<_,Option<String>>(14),
            "terminated_at": row.get::<_,Option<String>>(15),
            "execution_ms": execution_ms,
            "measured_usage": {
                "cpu_seconds": cpu_seconds,
                "gib_seconds": gib_seconds,
                "startup_ms": row.get::<_,i64>(17),
                "idle_ms": row.get::<_,i64>(18),
                "utilization": if total_ms > 0 {execution_ms as f64 / total_ms as f64} else {0.0},
            },
            "estimated_provider_cost": {
                "micro_usd": estimated,
                "rate_card_version": rate.version,
                "basis": "estimate_only_not_reconciled_provider_billing",
            },
        })
    }).collect())
}

pub(crate) fn deadlines(policy: &Policy) -> (Option<Duration>, Option<Duration>) {
    (
        policy.idle_timeout_seconds.map(Duration::from_secs),
        policy.max_lifetime_seconds.map(Duration::from_secs),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_checks_each_request_and_concurrent_totals() {
        let policy = Policy {
            max_cpu: Some(2),
            max_memory_bytes: Some(2 << 30),
            max_disk_bytes: Some(4 << 30),
            max_principal_cpu: Some(1),
            max_principal_memory_bytes: Some(1 << 30),
            max_principal_disk_bytes: Some(2 << 30),
            idle_timeout_seconds: Some(30),
            max_lifetime_seconds: Some(300),
        };
        let one = Resources {
            cpu: 1,
            memory_bytes: 1 << 30,
            disk_bytes: 2 << 30,
        };
        let two = Resources {
            cpu: 2,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
        };
        assert!(check_single(&policy, one, false).is_ok());
        assert!(check_single(&policy, two, false).is_ok());
        assert!(check_single(&policy, two, true).is_err());
        assert_eq!(
            deadlines(&policy),
            (
                Some(Duration::from_secs(30)),
                Some(Duration::from_secs(300))
            )
        );
    }

    #[test]
    fn policy_validation_rejects_unbounded_lifetimes() {
        let policy: Policy = serde_json::from_str("{\"idle_timeout_seconds\":0}").unwrap();
        assert!(policy.validate().is_err());
    }
}
