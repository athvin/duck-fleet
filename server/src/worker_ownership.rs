//! Exclude overlapping processes for one stable recovery identity. Kubernetes can
//! replace a deleted pod while its predecessor is still draining, even when a
//! Deployment uses Recreate for rollouts. The dedicated connection outlives every
//! engine session and is never silently reconnected by the ledger client.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use postgres::{Client, NoTls};

pub(crate) struct Guard {
    client: Client,
    lost: bool,
}

impl Guard {
    /// Different identities do not block each other. A hash collision can only
    /// delay an unrelated startup, never grant two owners the same lock.
    pub fn try_claim(mut client: Client, worker_id: &str) -> Result<Option<Self>> {
        client.batch_execute("SET statement_timeout=3000")?;
        let owned: bool = client
            .query_one(
                "SELECT pg_try_advisory_lock(4283122,hashtext($1))",
                &[&worker_id],
            )?
            .get(0);
        Ok(owned.then_some(Self {
            client,
            lost: false,
        }))
    }

    pub fn acquire(dsn: &str, worker_id: &str, wait: Duration) -> Result<Self> {
        let redactor = crate::secrets::Redactor::from_env();
        let mut config: postgres::Config =
            redactor.guard("worker ownership configuration", dsn.parse())?;
        config.connect_timeout(Duration::from_secs(3));
        config.tcp_user_timeout(Duration::from_secs(3));
        config.keepalives_idle(Duration::from_secs(3));
        config.keepalives_interval(Duration::from_secs(1));
        config.keepalives_retries(2);
        let start = Instant::now();
        loop {
            let client = redactor.guard("worker ownership connection", config.connect(NoTls))?;
            if let Some(guard) =
                Self::try_claim(client, worker_id).context("claiming exclusive worker identity")?
            {
                return Ok(guard);
            }
            anyhow::ensure!(
                start.elapsed() < wait,
                "worker identity {worker_id} is still owned by another process; refusing startup recovery"
            );
            if start.elapsed() < Duration::from_secs(1) {
                tracing::info!(
                    worker_id,
                    "waiting for previous worker process to release its recovery identity"
                );
            }
            std::thread::sleep(
                Duration::from_millis(200).min(wait.saturating_sub(start.elapsed())),
            );
        }
    }

    /// Loss is terminal for this process. Reclaiming could race a replacement's
    /// recovery; readiness must fail and a fresh process must perform recovery.
    /// This guard does not assert that a lost database connection proves an
    /// in-flight DuckDB write did not commit.
    pub fn is_closed(&self) -> bool {
        self.lost || self.client.is_closed()
    }

    /// The synchronous client only polls its socket while executing a method.
    /// A cached is_closed check alone cannot notice a lost idle connection.
    pub fn check(&mut self) -> bool {
        if self.is_closed() || self.client.check_connection().is_err() {
            self.lost = true;
            false
        } else {
            true
        }
    }
}
