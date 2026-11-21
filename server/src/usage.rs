//! Usage ledger reads and the operator budget control.
//!
//! Like history reads, these use independent bounded PostgreSQL connections and
//! never enter the DuckDB worker queue, so the billing view stays answerable while
//! workers are busy or stopped. Reads run read-only; only the budget limits are
//! writable, and the usage events themselves are immutable in the database.
use crate::{
    ApiError, audit,
    authz::{self, Action, MetadataAccess},
    credentials::Identity,
    metadata::{self, Relation},
    metering, pricing,
};
use postgres::{Row, Transaction};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub(crate) struct Reader {
    reader: metadata::Reader,
    /// The card new work is priced with, reported on the budget position and used
    /// to settle legacy reservations that predate the stored-rate columns.
    rate: pricing::RateCard,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Filter {
    pub limit: i64,
    pub before: Option<i64>,
    pub execution_id: Option<String>,
    pub source: Option<String>,
}
impl Default for Filter {
    fn default() -> Self {
        Self {
            limit: 500,
            before: None,
            execution_id: None,
            source: None,
        }
    }
}
impl Filter {
    fn validate(&self) -> Result<(), ApiError> {
        if !(1..=500).contains(&self.limit) || self.before.is_some_and(|n| n <= 0) {
            return Err(ApiError::bad(
                "limit must be 1–500 and before must be positive",
            ));
        }
        if self
            .source
            .as_deref()
            .is_some_and(|s| !["worker", "recovery", "queue"].contains(&s))
        {
            return Err(ApiError::bad("unknown usage source"));
        }
        if self
            .execution_id
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 512)
        {
            return Err(ApiError::bad("execution_id must contain 1–512 bytes"));
        }
        Ok(())
    }
}

/// An absolute setting, not a delta: both fields replace the stored limit. Omitted
/// fields are left alone.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BudgetUpdate {
    pub prepaid_micro_usd: Option<i64>,
    pub max_job_micro_usd: Option<i64>,
}
impl BudgetUpdate {
    fn validate(&self) -> Result<(), ApiError> {
        if self.prepaid_micro_usd.is_none() && self.max_job_micro_usd.is_none() {
            return Err(ApiError::bad(
                "supply prepaid_micro_usd, max_job_micro_usd, or both",
            ));
        }
        // A negative limit is meaningless; zero is allowed so spending can be halted.
        if [self.prepaid_micro_usd, self.max_job_micro_usd]
            .iter()
            .any(|value| value.is_some_and(|v| v < 0))
        {
            return Err(ApiError::bad("budget limits cannot be negative"));
        }
        Ok(())
    }
}

const COLUMNS:&str="event_id,execution_id,meter_name,quantity,unit,resource_units,amount_micro_usd,rate_card_version,worker_id,
    CASE WHEN $4 OR ($5 AND owner_principal_id=$2) THEN context ELSE 'null' END,
    source,corrects,started_at::text,ended_at::text,observed_at::text,ledger_id,namespace_id,owner_principal_id,actor_principal_id,actor_kind,actor_principal,actor_credential_id";
fn record(row: &Row) -> Value {
    json!({
        "event_id":row.get::<_,String>(0),"execution_id":row.get::<_,String>(1),"meter_name":row.get::<_,String>(2),"quantity":row.get::<_,i64>(3),
        "unit":row.get::<_,String>(4),"resource_units":row.get::<_,i64>(5),"amount_micro_usd":row.get::<_,i64>(6),"rate_card_version":row.get::<_,String>(7),"worker_id":row.get::<_,String>(8),
        "context":serde_json::from_str::<Value>(row.get::<_,&str>(9)).unwrap_or(Value::Null),"source":row.get::<_,String>(10),"corrects":row.get::<_,Option<String>>(11),
        "started_at":row.get::<_,String>(12),"ended_at":row.get::<_,String>(13),"observed_at":row.get::<_,String>(14),"ledger_id":row.get::<_,i64>(15),
        "namespace_id":row.get::<_,Option<String>>(16),"owner_principal_id":row.get::<_,Option<String>>(17),"actor_principal_id":row.get::<_,Option<String>>(18),
        "actor_kind":row.get::<_,Option<String>>(19),"actor_principal":row.get::<_,Option<String>>(20),"actor_credential_id":row.get::<_,Option<String>>(21)
    })
}
pub(crate) fn events_in(
    tx: &mut Transaction<'_>,
    access: &MetadataAccess,
    filter: Filter,
) -> Result<Value, ApiError> {
    filter.validate()?;
    let plan = metadata::relation_plan(access, Relation::Usage)?;
    let rows=tx.query(&format!("SELECT {COLUMNS} FROM usage_events WHERE COALESCE(namespace_id,$1)=$1 AND ($3 OR owner_principal_id=$2)
        AND ($6::BIGINT IS NULL OR ledger_id<$6) AND ($7::TEXT IS NULL OR execution_id=$7) AND ($8::TEXT IS NULL OR source=$8) ORDER BY ledger_id DESC LIMIT $9"),
        &[&plan.namespace_id,&plan.principal_id,&plan.all,&plan.sql_all,&plan.sql_own,&filter.before,&filter.execution_id,&filter.source,&(filter.limit+1)])
        .map_err(|_|ApiError::internal())?;
    let mut events: Vec<_> = rows.iter().map(record).collect();
    let more = events.len() > filter.limit as usize;
    events.truncate(filter.limit as usize);
    let next = if more {
        events.last().map(|row| row["ledger_id"].clone())
    } else {
        None
    };
    Ok(json!({"events":events,"next_before":next}))
}
impl Reader {
    pub fn new(dsn: String, rate: pricing::RateCard) -> Self {
        Self {
            reader: metadata::Reader::new(dsn),
            rate,
        }
    }
    pub async fn events(&self, identity: Identity, filter: Filter) -> Result<Value, ApiError> {
        filter.validate()?;
        self.reader
            .run(identity, Action::UsageRead, None, move |tx, access| {
                events_in(tx, &access.metadata(), filter)
            })
            .await
    }
    pub async fn budget(&self, identity: Identity) -> Result<Value, ApiError> {
        let rate = self.rate.clone();
        self.reader
            .run(
                identity,
                Action::BudgetRead,
                Some("local".into()),
                move |tx, access| {
                    access.require(Action::BudgetRead, None)?;
                    metering::summary(tx, &rate)
                },
            )
            .await
    }
    pub async fn set_budget(
        &self,
        identity: Identity,
        update: BudgetUpdate,
    ) -> Result<Value, ApiError> {
        update.validate()?;
        let rate = self.rate.clone();
        let current = identity.clone();
        self.reader.run(identity,Action::BudgetManage,Some("local".into()),move|tx,access|{
            // Policy lock precedes the budget row. Management cannot commit
            // against a grant revoked while it waited for another policy writer.
            if let Some(id)=&access.actor.principal_id {tx.query_one("SELECT principal_id FROM principals WHERE principal_id=$1 FOR UPDATE",&[id]).map_err(|_|ApiError::internal())?;}
            tx.query_one("SELECT budget_id FROM budget WHERE budget_id='local' FOR UPDATE",&[]).map_err(|_|ApiError::internal())?;
            let access=authz::resolve_in(tx,&current)?;access.require(Action::BudgetManage,None)?;
            tx.execute("UPDATE budget SET prepaid_micro_usd=COALESCE($1,prepaid_micro_usd),max_job_micro_usd=COALESCE($2,max_job_micro_usd),updated_at=clock_timestamp() WHERE budget_id='local'",&[&update.prepaid_micro_usd,&update.max_job_micro_usd]).map_err(|_|ApiError::internal())?;
            audit::append_in(tx,&audit::Event::new(&access,Action::BudgetManage,"budget",Some("local"),"allowed","budget_limits_changed",None)).map_err(|_|ApiError::internal())?;
            metering::summary(tx,&rate)
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_filters_reject_invalid_values() {
        for input in [
            json!({"limit": 0}),
            json!({"limit": 501}),
            json!({"before": 0}),
            json!({"source": "made_up"}),
            json!({"execution_id": ""}),
        ] {
            assert!(
                serde_json::from_value::<Filter>(input)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        assert!(Filter::default().validate().is_ok());
        for source in ["worker", "recovery", "queue"] {
            assert!(
                serde_json::from_value::<Filter>(json!({"source":source}))
                    .unwrap()
                    .validate()
                    .is_ok()
            );
        }
        assert!(serde_json::from_value::<Filter>(json!({"unknown": true})).is_err());
    }

    #[test]
    fn budget_updates_require_a_sane_field() {
        for input in [
            json!({}),
            json!({"prepaid_micro_usd": -1}),
            json!({"max_job_micro_usd": -1}),
        ] {
            assert!(
                serde_json::from_value::<BudgetUpdate>(input)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        // Zero is legitimate: it stops further spending without deleting history.
        for input in [
            json!({"prepaid_micro_usd": 0}),
            json!({"max_job_micro_usd": 5_000}),
            json!({"prepaid_micro_usd": 1, "max_job_micro_usd": 1}),
        ] {
            assert!(
                serde_json::from_value::<BudgetUpdate>(input)
                    .unwrap()
                    .validate()
                    .is_ok()
            );
        }
        assert!(serde_json::from_value::<BudgetUpdate>(json!({"nope": 1})).is_err());
    }
}
