//! Immutable control-plane events. Runtime privileges are separately enforced by db_roles.
use crate::{
    ApiError,
    authz::{self, Access, Action, Actor},
    credentials::Identity,
    failure,
};
use anyhow::Result;
use postgres::{Client, Transaction};
use serde_json::{Value, json};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub actor: Actor,
    pub action: Action,
    pub target_type: String,
    pub target_id: Option<String>,
    pub decision: String,
    pub reason: String,
    pub policy_version: i64,
    pub correlation_id: Option<String>,
}
impl Event {
    pub fn new(
        access: &Access,
        action: Action,
        target_type: &str,
        target_id: Option<&str>,
        decision: &str,
        reason: &str,
        correlation_id: Option<&str>,
    ) -> Self {
        Self {
            actor: access.actor.clone(),
            action,
            target_type: target_type.into(),
            target_id: target_id.map(str::to_owned),
            decision: decision.into(),
            reason: reason.into(),
            policy_version: access.policy_version,
            correlation_id: correlation_id.map(str::to_owned),
        }
    }
}
pub(crate) fn migrate(tx: &mut Transaction<'_>) -> Result<()> {
    tx.batch_execute("CREATE TABLE audit_events(event_id TEXT PRIMARY KEY,occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),namespace_id TEXT NOT NULL,actor_principal_id TEXT,actor_kind TEXT NOT NULL,actor_principal TEXT NOT NULL,credential_id TEXT,action TEXT NOT NULL,target_type TEXT NOT NULL,target_id TEXT,decision TEXT NOT NULL,reason TEXT NOT NULL,policy_version BIGINT NOT NULL,correlation_id TEXT);
CREATE INDEX audit_events_time ON audit_events(namespace_id,occurred_at DESC,event_id);
CREATE FUNCTION immutable_audit_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit events are immutable'; END $$;
CREATE TRIGGER immutable_audit BEFORE UPDATE OR DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION immutable_audit_event();")?;
    Ok(())
}
fn validate_event(event: &Event) -> Result<()> {
    anyhow::ensure!(
        event.target_type.len() <= 64
            && event.target_id.as_ref().is_none_or(|s| s.len() <= 128)
            && event.reason.len() <= 128
            && event.correlation_id.as_ref().is_none_or(|s| s.len() <= 128),
        "invalid audit event bounds"
    );
    anyhow::ensure!(
        [
            "allowed",
            "denied",
            "started",
            "succeeded",
            "failed",
            "unknown"
        ]
        .contains(&event.decision.as_str()),
        "invalid audit decision"
    );
    Ok(())
}
const APPEND_SQL: &str = "INSERT INTO audit_events(event_id,namespace_id,actor_principal_id,actor_kind,actor_principal,credential_id,action,target_type,target_id,decision,reason,policy_version,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)";
pub(crate) fn append_in(tx: &mut Transaction<'_>, event: &Event) -> Result<()> {
    validate_event(event)?;
    tx.execute(
        APPEND_SQL,
        &[
            &Uuid::new_v4().to_string(),
            &event.actor.namespace_id,
            &event.actor.principal_id,
            &event.actor.kind.name(),
            &event.actor.principal,
            &event.actor.credential_id,
            &event.action.name(),
            &event.target_type,
            &event.target_id,
            &event.decision,
            &event.reason,
            &event.policy_version,
            &event.correlation_id,
        ],
    )?;
    Ok(())
}
pub(crate) async fn append_async_in(
    tx: &tokio_postgres::Transaction<'_>,
    event: &Event,
) -> Result<()> {
    validate_event(event)?;
    tx.execute(
        APPEND_SQL,
        &[
            &Uuid::new_v4().to_string(),
            &event.actor.namespace_id,
            &event.actor.principal_id,
            &event.actor.kind.name(),
            &event.actor.principal,
            &event.actor.credential_id,
            &event.action.name(),
            &event.target_type,
            &event.target_id,
            &event.decision,
            &event.reason,
            &event.policy_version,
            &event.correlation_id,
        ],
    )
    .await?;
    Ok(())
}
/// Accepted work already carries canonical attribution even when the credential
/// can no longer resolve. Never infer an actor from its label or a transport hint.
/// Version zero explicitly means no current authorization policy was resolved.
/// A savepoint keeps audit INSERT failure from changing a known terminal refusal.
pub(crate) fn append_accepted_denial_in(
    tx: &mut Transaction<'_>,
    request_id: &str,
    action: Action,
    reason: &str,
) -> Result<bool> {
    let mut audit_tx = tx.transaction()?;
    let Some(row)=audit_tx.query_opt("SELECT namespace_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id FROM requests WHERE request_id=$1 AND namespace_id IS NOT NULL AND actor_kind IS NOT NULL AND actor_principal IS NOT NULL",&[&request_id])? else {
        audit_tx.commit()?;
        return Ok(false);
    };
    let kind = match row.get::<_, &str>("actor_kind") {
        "named" => crate::credentials::ActorKind::Named,
        "bootstrap" => crate::credentials::ActorKind::Bootstrap,
        "internal" => crate::credentials::ActorKind::Internal,
        _ => anyhow::bail!("invalid retained actor kind"),
    };
    let actor = Actor {
        namespace_id: row.get("namespace_id"),
        principal_id: row.get("actor_principal_id"),
        principal: row.get("actor_principal"),
        credential_id: row.get("actor_credential_id"),
        kind,
    };
    anyhow::ensure!(
        Uuid::parse_str(&actor.namespace_id).is_ok(),
        "invalid retained actor namespace"
    );
    if kind == crate::credentials::ActorKind::Named {
        anyhow::ensure!(
            actor
                .principal_id
                .as_deref()
                .is_some_and(|id| Uuid::parse_str(id).is_ok())
                && actor
                    .credential_id
                    .as_deref()
                    .is_some_and(|id| Uuid::parse_str(id).is_ok()),
            "incomplete retained named actor"
        );
    } else {
        anyhow::ensure!(
            actor.principal_id.is_none() && actor.credential_id.is_none(),
            "invalid retained service actor"
        );
    }
    append_in(
        &mut audit_tx,
        &Event {
            actor,
            action,
            target_type: "request".into(),
            target_id: Some(request_id.into()),
            decision: "denied".into(),
            reason: reason.into(),
            policy_version: 0,
            correlation_id: Some(request_id.into()),
        },
    )?;
    audit_tx.commit()?;
    Ok(true)
}

pub(crate) fn append(
    client: &mut Client,
    access: &Access,
    action: Action,
    target: &str,
    decision: &str,
    reason: &str,
    correlation: Option<&str>,
) -> Result<()> {
    let mut tx = client.transaction()?;
    authz::bounded(&mut tx)?;
    append_in(
        &mut tx,
        &Event::new(
            access,
            action,
            "operation",
            Some(target),
            decision,
            reason,
            correlation,
        ),
    )?;
    tx.commit()?;
    Ok(())
}
pub(crate) fn list(
    client: &mut Client,
    identity: &Identity,
    limit: i64,
    before: Option<&str>,
) -> Result<Value, ApiError> {
    authz::validate_page(limit, before)?;
    if !(1..=500).contains(&limit) {
        return Err(ApiError::bad("limit must be 1-500"));
    }
    let err = |_: postgres::Error| ApiError::new(failure::UNAVAILABLE, "audit service unavailable");
    let mut tx = client.transaction().map_err(err)?;
    authz::bounded(&mut tx).map_err(|_| ApiError::internal())?;
    let access = authz::resolve_in(&mut tx, identity)?;
    if let Err(error) = authz::require_in(&mut tx, &access, Action::AuditRead, None) {
        tx.commit().map_err(err)?;
        return Err(error);
    }
    let rows=tx.query("SELECT event_id,occurred_at::text,actor_principal_id,actor_kind,actor_principal,credential_id,action,target_type,target_id,decision,reason,policy_version,correlation_id FROM audit_events WHERE namespace_id=$1 AND ($2::text IS NULL OR (occurred_at,event_id)<(SELECT occurred_at,event_id FROM audit_events WHERE namespace_id=$1 AND event_id=$2)) ORDER BY occurred_at DESC,event_id DESC LIMIT $3",&[&access.actor.namespace_id,&before,&(limit+1)]).map_err(err)?;
    let more = rows.len() > limit as usize;
    let page = &rows[..rows.len().min(limit as usize)];
    let next_before = if more {
        page.last().map(|r| r.get::<_, String>(0))
    } else {
        None
    };
    let events=page.iter().map(|r|json!({"event_id":r.get::<_,String>(0),"occurred_at":r.get::<_,String>(1),"actor_principal_id":r.get::<_,Option<String>>(2),"actor_kind":r.get::<_,String>(3),"actor_principal":r.get::<_,String>(4),"credential_id":r.get::<_,Option<String>>(5),"action":r.get::<_,String>(6),"target_type":r.get::<_,String>(7),"target_id":r.get::<_,Option<String>>(8),"decision":r.get::<_,String>(9),"reason":r.get::<_,String>(10),"policy_version":r.get::<_,i64>(11),"correlation_id":r.get::<_,Option<String>>(12)})).collect::<Vec<_>>();
    tx.commit().map_err(err)?;
    Ok(json!({"events":events,"next_before":next_before}))
}
