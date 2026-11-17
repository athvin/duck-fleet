//! Issued credentials: per-user and per-service bearer tokens, individually
//! revocable, stored only as digests.
//!
//! The environment token (`DUCKFLEET_TOKEN`) does not go away — it becomes the
//! **bootstrap credential**: the operator-held secret that mints, lists, and
//! revokes everything else, and the only principal allowed to. A token is shown
//! exactly once, in the response that issued it; the database holds its SHA-256,
//! so neither a database read nor a backup discloses a usable secret.
use anyhow::Result;
use postgres::{Client, Transaction};
use serde_json::{Value, json};
use uuid::Uuid;

/// The header the gateway sets on the internal hop to a worker, naming the
/// principal it authenticated. Trusted only because that hop already requires the
/// shared internal token; a caller-supplied value never reaches it — the gateway
/// overwrites, and a worker addressed directly records `direct`.
pub(crate) const PRINCIPAL_HEADER: &str = "x-duckfleet-principal";
/// What the bootstrap credential authenticates as.
pub(crate) const BOOTSTRAP_PRINCIPAL: &str = "bootstrap-admin";

/// A credential at mint time. The only moment `token` exists in plaintext
/// anywhere but the caller's hands.
pub(crate) struct Issued {
    pub credential_id: String,
    pub principal: String,
    pub token: String,
}

pub(crate) fn migrate(transaction: &mut Transaction) -> Result<()> {
    transaction.batch_execute(
        "CREATE TABLE IF NOT EXISTS credentials (
            credential_id TEXT PRIMARY KEY,
            principal TEXT NOT NULL,
            token_sha256 TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ,
            revoked_at TIMESTAMPTZ
        );",
    )?;
    Ok(())
}

/// Hex SHA-256 of a presented token: the only form a token takes at rest or in a
/// lookup.
pub(crate) fn digest(token: &str) -> String {
    ring::digest::digest(&ring::digest::SHA256, token.as_bytes())
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// A principal a caller may name: short, printable, and never the bootstrap's own
/// name, so a history row saying `bootstrap-admin` can only mean the bootstrap.
pub(crate) fn valid_principal(principal: &str) -> bool {
    let trimmed = principal.trim();
    !trimmed.is_empty()
        && trimmed == principal
        && principal.len() <= 64
        && principal != BOOTSTRAP_PRINCIPAL
        && principal
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@'))
}

fn new_token() -> String {
    // 256 bits of v4 randomness under a greppable prefix, so a leaked token is
    // findable in a codebase the way cloud key prefixes are.
    format!("dfk_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub(crate) fn issue(
    client: &mut Client,
    principal: &str,
    expires_seconds: Option<i64>,
) -> Result<Issued> {
    anyhow::ensure!(
        valid_principal(principal),
        "principal must be 1-64 characters of [a-zA-Z0-9._@-], and not the bootstrap's own name"
    );
    if let Some(seconds) = expires_seconds {
        anyhow::ensure!(seconds >= 1, "expires_seconds must be at least 1");
    }
    let issued = Issued {
        credential_id: Uuid::new_v4().to_string(),
        principal: principal.to_owned(),
        token: new_token(),
    };
    client.execute(
        "INSERT INTO credentials (credential_id, principal, token_sha256, expires_at)
         VALUES ($1,$2,$3, CASE WHEN $4::bigint IS NULL THEN NULL
                                ELSE now() + ($4::bigint * interval '1 second') END)",
        &[
            &issued.credential_id,
            &issued.principal,
            &digest(&issued.token),
            &expires_seconds,
        ],
    )?;
    Ok(issued)
}

/// Marks a credential unusable. Idempotence is the caller's signal: false means
/// it was already revoked or never existed, which for an operator cleaning up
/// amounts to the same thing said honestly.
pub(crate) fn revoke(client: &mut Client, credential_id: &str) -> Result<bool> {
    let revoked = client.execute(
        "UPDATE credentials SET revoked_at = now()
         WHERE credential_id = $1 AND revoked_at IS NULL",
        &[&credential_id],
    )?;
    Ok(revoked > 0)
}

/// Every credential, without a byte of secret material — not even the digest,
/// which would let a listing be brute-forced offline.
pub(crate) fn list(client: &mut Client) -> Result<Value> {
    let rows = client.query(
        "SELECT credential_id, principal, created_at::text, expires_at::text, revoked_at::text
         FROM credentials ORDER BY created_at DESC",
        &[],
    )?;
    Ok(json!({
        "credentials": rows.iter().map(|row| json!({
            "credential_id": row.get::<_, String>(0),
            "principal": row.get::<_, String>(1),
            "created_at": row.get::<_, String>(2),
            "expires_at": row.get::<_, Option<String>>(3),
            "revoked_at": row.get::<_, Option<String>>(4),
        })).collect::<Vec<_>>()
    }))
}

/// The principal a presented token authenticates, or None: unknown, revoked and
/// expired all answer alike, so a probe learns nothing about which it was.
pub(crate) fn resolve(client: &mut Client, token_sha256: &str) -> Result<Option<String>> {
    Ok(client
        .query_opt(
            "SELECT principal FROM credentials
             WHERE token_sha256 = $1 AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > now())",
            &[&token_sha256],
        )?
        .map(|row| row.get(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_long_prefixed_and_never_repeat() {
        let first = new_token();
        let second = new_token();
        assert_ne!(first, second);
        for token in [&first, &second] {
            assert!(token.starts_with("dfk_"), "{token}");
            assert_eq!(token.len(), 4 + 64, "{token}");
            assert!(token[4..].chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn digests_are_stable_hex_and_token_free() {
        let token = "dfk_0123";
        let hashed = digest(token);
        assert_eq!(hashed, digest(token));
        assert_eq!(hashed.len(), 64);
        assert!(hashed.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(!hashed.contains(token));
    }

    #[test]
    fn principals_are_short_printable_and_never_the_bootstrap() {
        for ok in ["analyst", "etl-service", "a.b@example.com", "A_1"] {
            assert!(valid_principal(ok), "{ok}");
        }
        for bad in [
            "",
            "   ",
            " padded",
            "padded ",
            BOOTSTRAP_PRINCIPAL,
            "has space",
            "has\nnewline",
            &"x".repeat(65),
        ] {
            assert!(!valid_principal(bad), "{bad:?}");
        }
    }
}
