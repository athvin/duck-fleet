//! EKS Pod Identity credentials for the engine's S3 secrets, fetched by the
//! worker process rather than by DuckDB.
//!
//! The engine runs with `enable_external_access=false` (see `engine_file_access`),
//! and DuckDB refuses to run its `credential_chain` provider once external access
//! is off. A secret created with `PROVIDER credential_chain, REFRESH auto` therefore
//! works until its first credentials expire, then fails every S3 request with
//! "The credential_chain provider cannot be used when external access is
//! disabled". A short-lived isolated pod never noticed. A persistent worker
//! stopped writing a few hours after it started.
//!
//! So the worker fetches the credentials itself from the Pod Identity agent and
//! creates each secret with explicit keys (`PROVIDER config`), which DuckDB
//! accepts after the lockdown. A background thread replaces the secrets well
//! before the credentials expire.

use crate::secrets::Redactor;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::Connection;
use std::time::Duration;

/// Replace credentials this long before they expire.
const REFRESH_AHEAD: Duration = Duration::from_secs(15 * 60);
/// Never wait longer than this between refreshes, whatever the expiry says.
const MAX_WAIT: Duration = Duration::from_secs(60 * 60);
/// After a failed refresh, try again this soon.
const RETRY: Duration = Duration::from_secs(30);

pub(crate) struct Credentials {
    key_id: String,
    secret: String,
    token: String,
    expiration: DateTime<Utc>,
}

impl Credentials {
    pub(crate) fn key_id(&self) -> &str {
        &self.key_id
    }

    fn values(&self) -> [String; 3] {
        [self.key_id.clone(), self.secret.clone(), self.token.clone()]
    }
}

/// One DuckDB S3 secret the worker keeps fresh: its name and the prefix it covers.
#[derive(Clone)]
pub(crate) struct Secret {
    pub(crate) name: &'static str,
    pub(crate) scope: String,
}

/// Parse the Pod Identity agent's response
/// (`{"AccessKeyId","SecretAccessKey","Token","Expiration"}`).
fn parse(body: &str) -> Result<Credentials> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("the credential response is not JSON")?;
    let text = |key: &str| -> Result<String> {
        value[key]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
            .with_context(|| format!("the credential response has no {key}"))
    };
    Ok(Credentials {
        key_id: text("AccessKeyId")?,
        secret: text("SecretAccessKey")?,
        token: text("Token")?,
        expiration: DateTime::parse_from_rfc3339(&text("Expiration")?)
            .context("the credential Expiration is not RFC 3339")?
            .with_timezone(&Utc),
    })
}

/// Fetch credentials from the agent named by the EKS-injected environment. The
/// authorization token file is re-read each time because EKS rotates it.
pub(crate) fn fetch(runtime: &tokio::runtime::Handle) -> Result<Credentials> {
    let uri = std::env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI")
        .context("AWS_CONTAINER_CREDENTIALS_FULL_URI is not set")?;
    let token_file = std::env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
        .context("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE is not set")?;
    let authorization = std::fs::read_to_string(&token_file)
        .context("reading the pod identity token file")?
        .trim()
        .to_owned();
    let body = runtime.block_on(async {
        let response = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?
            .get(&uri)
            .header("Authorization", authorization)
            .send()
            .await?
            .error_for_status()?;
        response.text().await
    });
    parse(&body.context("requesting pod identity credentials")?)
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `CREATE OR REPLACE SECRET` for every secret, with explicit keys. Credential-bearing,
/// so it only ever runs through a [`Redactor`] that knows these credentials.
pub(crate) fn secret_sql(secrets: &[Secret], region: &str, credentials: &Credentials) -> String {
    secret_sql_at(secrets, region, credentials, None)
}

/// [`secret_sql`] against a plain-HTTP, path-style endpoint, for the in-image
/// engine check's local S3 fixture. Production always uses AWS's own endpoint.
pub(crate) fn secret_sql_at(
    secrets: &[Secret],
    region: &str,
    credentials: &Credentials,
    endpoint: Option<&str>,
) -> String {
    let endpoint = endpoint
        .map(|endpoint| {
            format!(
                ", ENDPOINT {}, URL_STYLE 'path', USE_SSL false",
                literal(endpoint)
            )
        })
        .unwrap_or_default();
    secrets
        .iter()
        .map(|secret| {
            format!(
                "CREATE OR REPLACE SECRET {} (TYPE S3, KEY_ID {}, SECRET {}, SESSION_TOKEN {}, REGION {}, SCOPE {}{endpoint});",
                secret.name,
                literal(&credentials.key_id),
                literal(&credentials.secret),
                literal(&credentials.token),
                literal(region),
                literal(&secret.scope),
            )
        })
        .collect()
}

/// How long to wait before replacing credentials that expire at `expiration`.
fn wait_for(expiration: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    let remaining = (expiration - now).to_std().unwrap_or_default();
    remaining
        .saturating_sub(REFRESH_AHEAD)
        .clamp(RETRY, MAX_WAIT)
}

/// Install fresh secrets on `conn` and return when they expire.
pub(crate) fn install(
    conn: &Connection,
    runtime: &tokio::runtime::Handle,
    redactor: &Redactor,
    secrets: &[Secret],
    region: &str,
) -> Result<DateTime<Utc>> {
    let credentials = redactor.guard("fetching pod identity credentials", fetch(runtime))?;
    let redactor = redactor.with(credentials.values());
    redactor.guard(
        "installing the object store secrets",
        conn.execute_batch(&secret_sql(secrets, region, &credentials)),
    )?;
    Ok(credentials.expiration)
}

/// Keep the secrets fresh for the life of the process. `conn` shares the
/// engine's database, so a replaced secret applies to every session.
pub(crate) fn keep_fresh(
    conn: Connection,
    runtime: tokio::runtime::Handle,
    redactor: Redactor,
    secrets: Vec<Secret>,
    region: String,
    first_expiration: DateTime<Utc>,
) -> Result<()> {
    std::thread::Builder::new()
        .name("duckfleet-s3-credentials".into())
        .spawn(move || {
            let mut wait = wait_for(first_expiration, Utc::now());
            loop {
                std::thread::sleep(wait);
                wait = match install(&conn, &runtime, &redactor, &secrets, &region) {
                    Ok(expiration) => {
                        tracing::info!(%expiration, "refreshed object store credentials");
                        wait_for(expiration, Utc::now())
                    }
                    Err(error) => {
                        tracing::warn!(error = %format!("{error:#}"), "object store credential refresh failed; retrying");
                        RETRY
                    }
                };
            }
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(expiration: &str) -> Credentials {
        parse(&format!(
            r#"{{"AccessKeyId":"AKIATEST","SecretAccessKey":"se'cret","Token":"tok","Expiration":"{expiration}"}}"#
        ))
        .unwrap()
    }

    #[test]
    fn parses_the_agent_response_and_rejects_partial_ones() {
        let parsed = credentials("2026-11-27T18:00:00Z");
        assert_eq!(parsed.key_id, "AKIATEST");
        assert_eq!(parsed.expiration.to_rfc3339(), "2026-11-27T18:00:00+00:00");
        assert!(
            parse(
                r#"{"AccessKeyId":"A","SecretAccessKey":"S","Expiration":"2026-11-27T18:00:00Z"}"#
            )
            .is_err()
        );
        assert!(
            parse(r#"{"AccessKeyId":"A","SecretAccessKey":"S","Token":"T","Expiration":"soon"}"#)
                .is_err()
        );
        assert!(parse("not json").is_err());
    }

    #[test]
    fn secret_sql_is_explicit_and_quoted() {
        let sql = secret_sql(
            &[
                Secret {
                    name: "aws_s3",
                    scope: "s3://b/lake".into(),
                },
                Secret {
                    name: "import_results",
                    scope: "s3://b/results".into(),
                },
            ],
            "us-east-2",
            &credentials("2026-11-27T18:00:00Z"),
        );
        assert!(sql.starts_with("CREATE OR REPLACE SECRET aws_s3 (TYPE S3, KEY_ID 'AKIATEST', SECRET 'se''cret', SESSION_TOKEN 'tok', REGION 'us-east-2', SCOPE 's3://b/lake');"));
        assert!(sql.contains("CREATE OR REPLACE SECRET import_results"));
        assert!(!sql.contains("credential_chain"));
    }

    #[test]
    fn refreshes_ahead_of_expiry_within_bounds() {
        let now = DateTime::parse_from_rfc3339("2026-11-27T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let at = |minutes: i64| now + chrono::Duration::minutes(minutes);
        assert_eq!(wait_for(at(6 * 60), now), MAX_WAIT);
        assert_eq!(wait_for(at(45), now), Duration::from_secs(30 * 60));
        assert_eq!(wait_for(at(10), now), RETRY);
        assert_eq!(wait_for(at(-5), now), RETRY);
    }

    /// The bug this module exists for, against the real engine: after the lockdown
    /// a credential-chain secret cannot be used, and an explicit one can replace it.
    #[test]
    fn explicit_secrets_replace_after_the_engine_lockdown() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        if conn
            .execute_batch("SET extension_directory='/opt/duckdb/extensions'; LOAD httpfs;")
            .is_err()
        {
            eprintln!("skipped: httpfs is only bundled in the worker image");
            return Ok(());
        }
        let secrets = [Secret {
            name: "aws_s3",
            scope: "s3://b/lake".into(),
        }];
        let sql = secret_sql(&secrets, "us-east-2", &credentials("2026-11-27T18:00:00Z"));
        // The worker's order: installed at startup, locked, then refreshed.
        conn.execute_batch(&sql)?;
        conn.execute_batch(&crate::worker::engine_file_access(
            "s3://b/lake/",
            None,
            None,
        ))?;
        conn.execute_batch(&sql)?;
        let provider: String = conn.query_row(
            "SELECT provider FROM duckdb_secrets() WHERE name='aws_s3'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(provider, "config");
        Ok(())
    }
}
