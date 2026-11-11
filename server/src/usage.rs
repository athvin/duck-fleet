//! Usage ledger reads and the operator budget control.
//!
//! Like history reads, these use independent bounded PostgreSQL connections and
//! never enter the DuckDB worker queue, so the billing view stays answerable while
//! workers are busy or stopped. Reads run read-only; only the budget limits are
//! writable, and the usage events themselves are immutable in the database.
use crate::{ApiError, failure, metering};
use postgres::{Client, NoTls, Row};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(crate) struct Reader {
    dsn: Arc<str>,
    capacity: Arc<Semaphore>,
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
            .is_some_and(|s| !["worker", "recovery"].contains(&s))
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

const COLUMNS: &str = "event_id,execution_id,meter_name,quantity,unit,resource_units,amount_micro_usd,rate_card_version,worker_id,context,source,corrects,started_at::text,ended_at::text,observed_at::text,ledger_id";
fn record(row: &Row) -> Value {
    json!({
        "event_id":row.get::<_, String>(0), "execution_id":row.get::<_, String>(1),
        "meter_name":row.get::<_, String>(2), "quantity":row.get::<_, i64>(3),
        "unit":row.get::<_, String>(4), "resource_units":row.get::<_, i64>(5),
        "amount_micro_usd":row.get::<_, i64>(6), "rate_card_version":row.get::<_, String>(7),
        "worker_id":row.get::<_, String>(8),
        "context":serde_json::from_str(row.get::<_, &str>(9)).unwrap_or(Value::Null),
        "source":row.get::<_, String>(10), "corrects":row.get::<_, Option<String>>(11),
        "started_at":row.get::<_, String>(12), "ended_at":row.get::<_, String>(13),
        "observed_at":row.get::<_, String>(14), "ledger_id":row.get::<_, i64>(15)
    })
}

impl Reader {
    pub fn new(dsn: String) -> Self {
        Self {
            dsn: dsn.into(),
            capacity: Arc::new(Semaphore::new(8)),
        }
    }
    async fn run<F>(&self, writable: bool, operation: F) -> Result<Value, ApiError>
    where
        F: FnOnce(&mut Client) -> Result<Value, ApiError> + Send + 'static,
    {
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "usage reader capacity reached"))?;
        let dsn = self.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.options(if writable {
                "-c statement_timeout=3000"
            } else {
                "-c default_transaction_read_only=on -c statement_timeout=3000"
            });
            let mut client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            operation(&mut client)
        })
        .await
        .map_err(|_| ApiError::internal())?
    }

    pub async fn events(&self, filter: Filter) -> Result<Value, ApiError> {
        filter.validate()?;
        self.run(false, move |client| {
            let rows = client
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM usage_events
                         WHERE ($1::bigint IS NULL OR ledger_id < $1)
                           AND ($2::text IS NULL OR execution_id=$2)
                           AND ($3::text IS NULL OR source=$3)
                         ORDER BY ledger_id DESC LIMIT $4"
                    ),
                    &[
                        &filter.before,
                        &filter.execution_id,
                        &filter.source,
                        &(filter.limit + 1),
                    ],
                )
                .map_err(|_| ApiError::internal())?;
            let mut events: Vec<Value> = rows.iter().map(record).collect();
            let has_more = events.len() > filter.limit as usize;
            events.truncate(filter.limit as usize);
            let next_before = if has_more {
                events.last().map(|e| e["ledger_id"].clone())
            } else {
                None
            };
            Ok(json!({"events":events, "next_before":next_before}))
        })
        .await
    }

    pub async fn budget(&self) -> Result<Value, ApiError> {
        self.run(false, metering::summary).await
    }

    pub async fn set_budget(&self, update: BudgetUpdate) -> Result<Value, ApiError> {
        update.validate()?;
        self.run(true, move |client| {
            // COALESCE leaves an omitted field at its stored value.
            client
                .execute(
                    "UPDATE budget SET prepaid_micro_usd=COALESCE($1,prepaid_micro_usd),
                     max_job_micro_usd=COALESCE($2,max_job_micro_usd), updated_at=now()
                     WHERE budget_id='local'",
                    &[&update.prepaid_micro_usd, &update.max_job_micro_usd],
                )
                .map_err(|_| ApiError::internal())?;
            metering::summary(client)
        })
        .await
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
