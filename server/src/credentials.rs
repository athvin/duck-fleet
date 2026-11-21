//! Issued credentials: per-user and per-service bearer tokens, individually
//! revocable, stored only as digests.
//!
//! The public environment token (`DUCKFLEET_TOKEN`) remains a gateway-only
//! bootstrap recovery credential. Named principals with credential-management
//! permission can also issue/list/revoke credentials through audited operations.
//! Workers receive a separate internal-service token. Issued bearer tokens are
//! shown only once; PostgreSQL stores a SHA-256 digest, never the bearer itself.

use anyhow::Result;
use postgres::{Client, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::SystemTime;
use uuid::Uuid;

/// The header the gateway sets on the internal hop to a worker, naming the
/// principal it authenticated. Trusted only because that hop already requires the
/// shared internal token; a caller-supplied value never reaches it — the gateway
/// overwrites, and a worker addressed directly records `direct`.
pub(crate) const PRINCIPAL_HEADER: &str = "x-duckfleet-principal";
/// Issued credential authenticated for this operation, never taken from SQL
/// context. Workers resolve this identifier again immediately before execution.
pub(crate) const CREDENTIAL_HEADER: &str = "x-duckfleet-credential-id";
pub(crate) const ACTOR_KIND_HEADER: &str = "x-duckfleet-actor-kind";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActorKind {
    Named,
    Bootstrap,
    Internal,
}
impl ActorKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Named => "named",
            Self::Bootstrap => "bootstrap",
            Self::Internal => "internal",
        }
    }
}
/// What the bootstrap credential authenticates as.
pub(crate) const BOOTSTRAP_PRINCIPAL: &str = "bootstrap-admin";

/// Authenticated identity without the bearer token or its digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub kind: ActorKind,
    pub principal: String,
    pub credential_id: Option<String>,
    pub expires_at: Option<SystemTime>,
}

impl Identity {
    pub fn bootstrap() -> Self {
        Self {
            kind: ActorKind::Bootstrap,
            principal: BOOTSTRAP_PRINCIPAL.into(),
            credential_id: None,
            expires_at: None,
        }
    }

    pub fn is_bootstrap(&self) -> bool {
        self.kind == ActorKind::Bootstrap
    }

    pub fn internal(principal: impl Into<String>) -> Self {
        Self {
            kind: ActorKind::Internal,
            principal: principal.into(),
            credential_id: None,
            expires_at: None,
        }
    }

    pub fn expired(&self) -> bool {
        self.expires_at.is_some_and(|at| at <= SystemTime::now())
    }
}

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

/// Whether a presented token equals a secret, compared without leaking how far the
/// two agree. Both are hashed to a fixed-length digest first — so the comparison's
/// timing cannot even reveal the secret's length — then verified byte-for-byte in
/// constant time. The bootstrap token's `==` check was timing-variable, which over
/// many probes narrows a shared secret a byte at a time.
pub(crate) fn tokens_match(presented: &str, secret: &str) -> bool {
    let presented = ring::digest::digest(&ring::digest::SHA256, presented.as_bytes());
    let secret = ring::digest::digest(&ring::digest::SHA256, secret.as_bytes());
    // Both digests are 32 bytes, so this loop runs a fixed number of times and folds
    // every byte into the accumulator before deciding — no early return on the first
    // mismatch, which is the whole point.
    let (presented, secret) = (presented.as_ref(), secret.as_ref());
    let mut difference = 0u8;
    for (a, b) in presented.iter().zip(secret.iter()) {
        difference |= a ^ b;
    }
    presented.len() == secret.len() && difference == 0
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
    let mut transaction = client.transaction()?;
    crate::authz::bounded(&mut transaction)?;
    let issued = issue_in(&mut transaction, principal, expires_seconds)?;
    transaction.commit()?;
    Ok(issued)
}

pub(crate) fn issue_in(
    client: &mut Transaction<'_>,
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
    let principal_id = crate::authz::ensure_principal_in(client, principal)?;
    client.execute(
        "INSERT INTO credentials (credential_id, principal, token_sha256, expires_at, principal_id)
         VALUES ($1,$2,$3, CASE WHEN $4::bigint IS NULL THEN NULL
                                ELSE now() + ($4::bigint * interval '1 second') END,$5)",
        &[
            &issued.credential_id,
            &issued.principal,
            &digest(&issued.token),
            &expires_seconds,
            &principal_id,
        ],
    )?;
    Ok(issued)
}

/// The principal a presented token authenticates, or None: unknown, revoked and
/// expired all answer alike, so a probe learns nothing about which it was.
const RESOLVE_TOKEN_SQL: &str = "SELECT credential_id, principal, expires_at FROM credentials
    WHERE token_sha256 = $1 AND revoked_at IS NULL
      AND (expires_at IS NULL OR expires_at > clock_timestamp())";

pub(crate) fn resolve(client: &mut Client, token_sha256: &str) -> Result<Option<Identity>> {
    Ok(
        read_identities(client, RESOLVE_TOKEN_SQL, &[&token_sha256])?
            .into_iter()
            .next(),
    )
}

/// The system-query caller owns the transaction, deadline and connection driver.
/// Share the token predicate and row decoder with foreground authentication; do
/// not cache, commit, or change the caller's read-only transaction settings here.
pub(crate) async fn resolve_async_in(
    transaction: &tokio_postgres::Transaction<'_>,
    token_sha256: &str,
) -> Result<Option<Identity>, crate::ApiError> {
    let row = transaction
        .query_opt(RESOLVE_TOKEN_SQL, &[&token_sha256])
        .await
        .map_err(api_error)?;
    Ok(row
        .map(identity)
        .transpose()
        .map_err(api_error)?
        .filter(|identity| !identity.expired()))
}

/// Authoritative admission check for an already authenticated connection/request.
pub(crate) fn resolve_id(client: &mut Client, credential_id: &str) -> Result<Option<Identity>> {
    Ok(read_identities(
        client,
        "SELECT credential_id, principal, expires_at FROM credentials
             WHERE credential_id = $1 AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > clock_timestamp())",
        &[&credential_id],
    )?
    .into_iter()
    .next())
}

/// One bounded query checks the lifecycle credentials of a worker's idle sessions.
pub(crate) fn resolve_ids(client: &mut Client, credential_ids: &[String]) -> Result<Vec<Identity>> {
    read_identities(
        client,
        "SELECT credential_id, principal, expires_at FROM credentials
             WHERE credential_id = ANY($1) AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > clock_timestamp())",
        &[&credential_ids],
    )
}

fn read_identities(
    client: &mut Client,
    sql: &str,
    parameters: &[&(dyn postgres::types::ToSql + Sync)],
) -> Result<Vec<Identity>> {
    // Workers reuse the ledger connection: scope the auth timeout/read-only mode
    // to this transaction, so lock contention cannot stall admission indefinitely
    // and subsequent ledger writes retain their original configuration.
    let mut transaction = client.build_transaction().read_only(true).start()?;
    transaction.batch_execute("SET LOCAL statement_timeout = 3000")?;
    let rows = transaction.query(sql, parameters)?;
    transaction.commit()?;
    Ok(rows
        .into_iter()
        .map(identity)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|identity| !identity.expired())
        .collect())
}

fn identity(row: postgres::Row) -> Result<Identity> {
    Ok(Identity {
        kind: ActorKind::Named,
        credential_id: Some(row.try_get(0)?),
        principal: row.try_get(1)?,
        expires_at: row.try_get(2)?,
    })
}

fn api_error(_: impl std::fmt::Display) -> crate::ApiError {
    crate::ApiError::new(
        crate::failure::UNAVAILABLE,
        "credential service unavailable",
    )
}
pub(crate) fn issue_authorized(
    client: &mut Client,
    actor: &Identity,
    principal: &str,
    expires: Option<i64>,
) -> Result<Issued, crate::ApiError> {
    if !valid_principal(principal) || expires.is_some_and(|v| v < 1) {
        return Err(crate::ApiError::bad(
            "invalid credential principal or expiry",
        ));
    }
    let mut tx = client.transaction().map_err(api_error)?;
    crate::authz::bounded(&mut tx).map_err(api_error)?;
    crate::authz::lock_actor_in(&mut tx, actor)?;
    let access = crate::authz::resolve_in(&mut tx, actor)?;
    if let Err(error) = crate::authz::require_in(
        &mut tx,
        &access,
        crate::authz::Action::CredentialManage,
        None,
    ) {
        tx.commit().map_err(api_error)?;
        return Err(error);
    }
    let issued = issue_in(&mut tx, principal, expires).map_err(api_error)?;
    crate::audit::append_in(
        &mut tx,
        &crate::audit::Event::new(
            &access,
            crate::authz::Action::CredentialManage,
            "credential",
            Some(&issued.credential_id),
            "allowed",
            "credential_issued",
            None,
        ),
    )
    .map_err(api_error)?;
    tx.commit().map_err(api_error)?;
    Ok(issued)
}
pub(crate) fn revoke_authorized(
    client: &mut Client,
    actor: &Identity,
    id: &str,
) -> Result<bool, crate::ApiError> {
    let mut tx = client.transaction().map_err(api_error)?;
    crate::authz::bounded(&mut tx).map_err(api_error)?;
    crate::authz::lock_actor_in(&mut tx, actor)?;
    let access = crate::authz::resolve_in(&mut tx, actor)?;
    if let Err(error) = crate::authz::require_in(
        &mut tx,
        &access,
        crate::authz::Action::CredentialManage,
        None,
    ) {
        tx.commit().map_err(api_error)?;
        return Err(error);
    }
    let changed=tx.execute("UPDATE credentials SET revoked_at=clock_timestamp() WHERE credential_id=$1 AND revoked_at IS NULL",&[&id]).map_err(api_error)?>0;
    crate::audit::append_in(
        &mut tx,
        &crate::audit::Event::new(
            &access,
            crate::authz::Action::CredentialManage,
            "credential",
            Some(id),
            "allowed",
            "credential_revoked",
            None,
        ),
    )
    .map_err(api_error)?;
    tx.commit().map_err(api_error)?;
    Ok(changed)
}
pub(crate) fn list_authorized(
    client: &mut Client,
    actor: &Identity,
    limit: i64,
    after: Option<&str>,
) -> Result<Value, crate::ApiError> {
    crate::authz::validate_page(limit, after)?;
    let mut tx = client.transaction().map_err(api_error)?;
    crate::authz::bounded(&mut tx).map_err(api_error)?;
    let access = crate::authz::resolve_in(&mut tx, actor)?;
    if let Err(error) = crate::authz::require_in(
        &mut tx,
        &access,
        crate::authz::Action::CredentialManage,
        None,
    ) {
        tx.commit().map_err(api_error)?;
        return Err(error);
    }
    let rows=tx.query("SELECT credential_id,principal,principal_id,created_at::text,expires_at::text,revoked_at::text FROM credentials WHERE ($1::text IS NULL OR credential_id>$1) ORDER BY credential_id LIMIT $2",&[&after,&(limit+1)]).map_err(api_error)?;
    let more = rows.len() > limit as usize;
    let page = &rows[..rows.len().min(limit as usize)];
    let value = json!({"next_after":if more{page.last().map(|r|r.get::<_,String>(0))}else{None},"credentials":page.iter().map(|r|json!({"credential_id":r.get::<_,String>(0),"principal":r.get::<_,String>(1),"principal_id":r.get::<_,Option<String>>(2),"created_at":r.get::<_,String>(3),"expires_at":r.get::<_,Option<String>>(4),"revoked_at":r.get::<_,Option<String>>(5)})).collect::<Vec<_>>()});
    tx.commit().map_err(api_error)?;
    Ok(value)
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
    fn tokens_match_accepts_the_secret_and_rejects_everything_else() {
        let secret = "duckfleet-local-development-token";
        assert!(tokens_match(secret, secret));
        // A prefix, a different-length string, and the empty string all fail —
        // nothing short of the whole secret matches.
        assert!(!tokens_match("duckfleet-local-development-toke", secret));
        assert!(!tokens_match("duckfleet-local-development-tokenX", secret));
        assert!(!tokens_match("", secret));
        assert!(!tokens_match(secret, ""));
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
