//! A caller disappearing must not free a database admission lease prematurely.
use crate::{
    ApiError,
    credentials::Identity,
    failure,
    metadata::StorageSchema,
    system_query::{Request, Service, SystemResult},
};
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::Instant};

struct Driver(JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Callers(Vec<JoinHandle<Result<SystemResult, ApiError>>>);
impl Drop for Callers {
    fn drop(&mut self) {
        for caller in &self.0 {
            caller.abort();
        }
    }
}
fn request() -> Request {
    Request::sql(
        "SELECT request_id FROM query_history WHERE request_id='system-lease-cancel'".into(),
    )
}
async fn waiting(tx: &tokio_postgres::Transaction<'_>, table: &str) -> Result<Vec<i32>> {
    // The lock holder stays in one transaction; refresh statistics explicitly.
    tx.query_one("SELECT pg_stat_clear_snapshot()", &[]).await?;
    Ok(tx.query(
        "SELECT pid FROM pg_stat_activity WHERE application_name='duckfleet-system' AND wait_event_type='Lock' AND state='active' AND position($1 IN query)>0 ORDER BY pid",
        &[&table],
    ).await?.iter().map(|row|row.get(0)).collect())
}
pub(super) async fn canceled_callers(
    admin_dsn: &str,
    schema: &str,
    identity: &Identity,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let storage = StorageSchema::validated(schema)
            .map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))?;
        let gateway_dsn = std::env::var("DUCKFLEET_GATEWAY_TEST_DSN")
            .context("system admission fixture requires gateway-role DSN")?;
        let service = Service::new(
            &gateway_dsn,
            Arc::from("system-admission-fixture-bootstrap"),
            storage,
        )
        .map_err(|e| anyhow::anyhow!("{}: {}", e.0.kind, e.1))?;
        let mut config: tokio_postgres::Config = admin_dsn.parse()?;
        config
            .connect_timeout(Duration::from_secs(3))
            .options("-c statement_timeout=3000 -c lock_timeout=500 -c idle_in_transaction_session_timeout=10000");
        let (mut db, connection) = config.connect(tokio_postgres::NoTls).await?;
        let _driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let tx = db.transaction().await?;
        // `schema` passed StorageSchema::validated; no caller SQL is interpolated.
        let table = format!("\"{schema}\".\"query_history\"");
        tx.batch_execute(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
            .await?;
        let mut callers = Callers(Vec::new());
        for _ in 0..2 {
            let service = service.clone();
            let identity = identity.clone();
            callers.0.push(tokio::spawn(async move {
                service.query(&identity, request()).await
            }));
        }
        let observe_until = Instant::now() + Duration::from_secs(2);
        let blocked = loop {
            let pids = waiting(&tx, &table).await?;
            if pids.len() == 2 {
                break pids;
            }
            anyhow::ensure!(
                Instant::now() < observe_until && callers.0.iter().all(|t| !t.is_finished()),
                "both system queries did not reach the held table lock before their bounded lock timeout (waiting={pids:?})"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        for caller in &callers.0 {
            caller.abort();
        }
        for caller in callers.0.drain(..) {
            anyhow::ensure!(
                caller.await.is_err_and(|error| error.is_cancelled()),
                "blocked caller completed instead of being canceled"
            );
        }
        for _ in 0..6 {
            match service.query(identity, request()).await {
                Err(error) if error.0 == failure::CAPACITY => {}
                Err(error) => anyhow::bail!(
                    "canceled database lease admitted a replacement ({})",
                    error.0.kind
                ),
                Ok(_) => anyhow::bail!("canceled database lease admitted a replacement"),
            }
            let pids = waiting(&tx, &table).await?;
            anyhow::ensure!(
                pids.iter().all(|pid| blocked.contains(pid)) && pids.len() <= 2,
                "replacement requests grew the blocked database population: {pids:?}"
            );
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        tx.rollback().await?;
        let recover_until = Instant::now() + Duration::from_secs(8);
        loop {
            match service.query(identity, request()).await {
                Ok(result) => {
                    drop(result);
                    break;
                }
                Err(error) if error.0 == failure::CAPACITY && Instant::now() < recover_until => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => anyhow::bail!(
                    "system admission did not recover after retired leases ({})",
                    error.0.kind
                ),
            }
        }
        // Acknowledged successful completion and response drop free the lease now.
        service.query(identity, request()).await.map_err(|error| {
            anyhow::anyhow!("{} after successful admission recovery", error.0.kind)
        })?;
        println!("PASS: canceled system callers retain bounded database leases, refuse replacements without backend growth, and recover after retirement");
        Ok(())
    })
    .await
    .context("system admission cancellation fixture exceeded 20 seconds")?
}
