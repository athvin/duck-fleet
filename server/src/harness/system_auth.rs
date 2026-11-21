//! Real PostgreSQL parity between foreground and bounded async metadata auth.
use crate::{
    ApiError, authz,
    credentials::{self, Identity},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::time::Duration;

fn projection(access: Result<authz::Access, ApiError>) -> Value {
    match access {
        Ok(access) => {
            let metadata = access.metadata();
            json!({"actor":access.actor,"version":access.policy_version,
                "history":metadata.history,"usage":metadata.usage,"sql":metadata.sql,
                "profile":metadata.profile,
                "entry":access.require_metadata_access().map(|_| "allowed").unwrap_or_else(|e|e.0.kind)})
        }
        Err(error) => json!({"failure_kind":error.0.kind}),
    }
}
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn parity(
    dsn: &str,
    schema: &str,
    cases: &[Identity],
    digests: &[String],
) -> Result<()> {
    let schema = crate::metadata::StorageSchema::validated(schema)
        .map_err(|e| anyhow::anyhow!("{}", e.1))?;
    let options = format!(
        "-c search_path={} -c statement_timeout=3000 -c lock_timeout=500 -c idle_in_transaction_session_timeout=3000",
        schema.search_path()
    );
    let sync_dsn = dsn.to_owned();
    let sync_options = options.clone();
    let sync_cases = cases.to_vec();
    let sync_digests = digests.to_vec();
    let (expected, expected_tokens) = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut config: postgres::Config = sync_dsn.parse()?;
        config
            .connect_timeout(Duration::from_secs(3))
            .options(&sync_options);
        let mut db = config.connect(postgres::NoTls)?;
        let mut expected = Vec::new();
        for identity in &sync_cases {
            let mut tx = db.build_transaction().read_only(true).start()?;
            expected.push(projection(authz::resolve_in(&mut tx, identity)));
            tx.rollback()?;
        }
        let tokens = sync_digests
            .iter()
            .map(|hash| credentials::resolve(&mut db, hash))
            .collect::<Result<Vec<_>>>()?;
        Ok((expected, tokens))
    })
    .await??;
    let mut config: tokio_postgres::Config = dsn.parse()?;
    config
        .connect_timeout(Duration::from_secs(3))
        .options(&options);
    let (mut db, connection) = tokio::time::timeout(
        Duration::from_secs(4),
        config.connect(tokio_postgres::NoTls),
    )
    .await??;
    let driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    for (index, identity) in cases.iter().enumerate() {
        let actual = tokio::time::timeout(Duration::from_secs(4), async {
            let tx = db.build_transaction().read_only(true).start().await?;
            let value = projection(authz::resolve_async_in(&tx, identity).await);
            tx.rollback().await?;
            Ok::<_, tokio_postgres::Error>(value)
        })
        .await??;
        anyhow::ensure!(
            actual == expected[index],
            "sync/async authorization diverged at case {index}"
        );
    }
    for (index, digest) in digests.iter().enumerate() {
        let actual = tokio::time::timeout(Duration::from_secs(4), async {
            let tx = db.build_transaction().read_only(true).start().await?;
            let value = credentials::resolve_async_in(&tx, digest)
                .await
                .map_err(|e| anyhow::anyhow!("async credential lookup failed: {}", e.0.kind))?;
            tx.rollback().await?;
            Ok::<_, anyhow::Error>(value)
        })
        .await
        .context("async credential parity deadline")??;
        anyhow::ensure!(
            actual == expected_tokens[index],
            "sync/async credential lookup diverged at case {index}"
        );
    }
    drop(db);
    drop(driver);
    Ok(())
}
