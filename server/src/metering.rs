//! Append-only usage ledger and prepaid budget admission.
//!
//! Three rules shape this module, all from the blueprint's metering section:
//!
//! 1. No execution path is unmetered. Admission reserves funds before SQL runs and
//!    settlement always writes exactly one usage event, including for failures,
//!    conflicts, timeouts, and work abandoned by a worker restart.
//! 2. The ledger is never edited. `usage_events` rejects `UPDATE` and `DELETE` in
//!    the database itself; a correction is a new row pointing at what it corrects.
//! 3. Reservations are serialized through the budget row, so two workers cannot
//!    both spend the last of the balance.
//!
//! Source telemetry (`query_history`) and the billable ledger stay separate tables
//! on purpose: diagnostics get rewritten as a query progresses, an invoice must not.
use crate::{ApiError, failure, pricing};
use anyhow::Result;
use postgres::{Client, Transaction};
use serde_json::{Value, json};

/// This harness bills one tenant, so the budget is a single known row.
const BUDGET_ID: &str = "local";
const METER: &str = "compute";

/// What admission granted: how long the statement may run, and what it reserved.
pub(crate) struct Admission {
    pub execution_id: String,
    pub units: i64,
    pub reserved_micro_usd: i64,
    /// The deadline actually enforced: the requested timeout, or less when the
    /// per-job cost cap or remaining balance would be reached first.
    pub deadline_seconds: u64,
    /// True when the budget, not the caller's timeout, set that deadline.
    pub limited_by_budget: bool,
}

/// The decision admission makes, separated from storage so every branch is unit
/// testable without a database.
pub(crate) fn decide(
    available_micro_usd: i64,
    max_job_micro_usd: i64,
    units: i64,
    timeout_seconds: u64,
) -> Result<(u64, i64), failure::Failure> {
    let ceiling = available_micro_usd.min(max_job_micro_usd);
    let affordable = pricing::affordable_seconds(units, ceiling);
    if affordable == 0 {
        // Distinguish "out of money" from "this cap is too low to run anything":
        // one is fixed by topping up, the other by changing the request or cap.
        return Err(if available_micro_usd <= max_job_micro_usd {
            failure::BUDGET_EXHAUSTED
        } else {
            failure::BUDGET_JOB_COST
        });
    }
    let deadline = timeout_seconds.min(affordable);
    Ok((deadline, pricing::charge_micro_usd(units, deadline * 1000)))
}

pub(crate) fn migrate(transaction: &mut Transaction, prepaid: i64, max_job: i64) -> Result<()> {
    transaction.batch_execute(
        "CREATE TABLE IF NOT EXISTS usage_events (
            event_id TEXT PRIMARY KEY,
            execution_id TEXT NOT NULL,
            meter_name TEXT NOT NULL,
            quantity BIGINT NOT NULL,
            unit TEXT NOT NULL,
            resource_units BIGINT NOT NULL,
            amount_micro_usd BIGINT NOT NULL,
            rate_card_version TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            context TEXT NOT NULL,
            source TEXT NOT NULL,
            corrects TEXT,
            started_at TIMESTAMPTZ NOT NULL,
            ended_at TIMESTAMPTZ NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ledger_id BIGSERIAL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS usage_events_order ON usage_events(ledger_id DESC);
        CREATE INDEX IF NOT EXISTS usage_events_execution ON usage_events(execution_id);
        CREATE OR REPLACE FUNCTION usage_events_append_only() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                RAISE EXCEPTION 'usage_events is append-only; insert a compensating event instead';
            END
            $body$;
        DROP TRIGGER IF EXISTS usage_events_immutable ON usage_events;
        CREATE TRIGGER usage_events_immutable BEFORE UPDATE OR DELETE ON usage_events
            FOR EACH STATEMENT EXECUTE FUNCTION usage_events_append_only();
        CREATE TABLE IF NOT EXISTS budget (
            budget_id TEXT PRIMARY KEY,
            prepaid_micro_usd BIGINT NOT NULL,
            max_job_micro_usd BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS reservations (
            execution_id TEXT PRIMARY KEY,
            reserved_micro_usd BIGINT NOT NULL,
            resource_units BIGINT NOT NULL,
            settled_micro_usd BIGINT,
            state TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            settled_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS reservations_held ON reservations(worker_id) WHERE state = 'held';",
    )?;
    // Seed once. A restart must not silently refill a spent balance.
    transaction.execute(
        "INSERT INTO budget (budget_id, prepaid_micro_usd, max_job_micro_usd)
         VALUES ($1,$2,$3) ON CONFLICT (budget_id) DO NOTHING",
        &[&BUDGET_ID, &prepaid, &max_job],
    )?;
    Ok(())
}

/// Settles reservations this worker abandoned by restarting, so no accepted
/// execution is left both unmetered and holding funds. The charge uses whatever
/// elapsed time the history row recorded; an execution that never reached its
/// terminal update is charged nothing, and the event says it came from recovery.
pub(crate) fn recover(transaction: &mut Transaction, worker_id: &str) -> Result<u64> {
    let orphans = transaction.query(
        "SELECT r.execution_id, r.reserved_micro_usd, r.resource_units,
                COALESCE(h.elapsed_ms,0), COALESCE(h.context,'{}')
         FROM reservations r LEFT JOIN query_history h ON h.query_id = r.execution_id
         WHERE r.state = 'held' AND r.worker_id = $1",
        &[&worker_id],
    )?;
    for row in &orphans {
        let execution_id: String = row.get(0);
        let reserved: i64 = row.get(1);
        let units: i64 = row.get(2);
        let elapsed_ms: i64 = row.get(3);
        let context: String = row.get(4);
        // Charge only what the history row proves ran, and never above the hold.
        let amount =
            pricing::charge_micro_usd(units, u64::try_from(elapsed_ms).unwrap_or(0)).min(reserved);
        record(
            transaction,
            &execution_id,
            units,
            elapsed_ms,
            amount,
            worker_id,
            &context,
            "recovery",
        )?;
        transaction.execute(
            "UPDATE reservations SET state='settled', settled_micro_usd=$2, settled_at=now()
             WHERE execution_id=$1",
            &[&execution_id, &amount],
        )?;
    }
    Ok(orphans.len() as u64)
}

/// Reserves funds for one execution. The budget row is locked for the duration, so
/// concurrent admissions on different workers cannot both pass on the same balance.
pub(crate) fn admit(
    client: &mut Client,
    execution_id: &str,
    units: i64,
    timeout_seconds: u64,
    worker_id: &str,
) -> Result<Admission, ApiError> {
    let mut transaction = client.transaction().map_err(|_| ApiError::internal())?;
    let limits = transaction
        .query_one(
            "SELECT prepaid_micro_usd, max_job_micro_usd FROM budget WHERE budget_id=$1 FOR UPDATE",
            &[&BUDGET_ID],
        )
        .map_err(|_| ApiError::internal())?;
    let prepaid: i64 = limits.get(0);
    let max_job: i64 = limits.get(1);
    let spent: i64 = transaction
        .query_one(
            "SELECT COALESCE(SUM(amount_micro_usd),0)::BIGINT FROM usage_events",
            &[],
        )
        .map_err(|_| ApiError::internal())?
        .get(0);
    let held: i64 = transaction
        .query_one(
            "SELECT COALESCE(SUM(reserved_micro_usd),0)::BIGINT FROM reservations WHERE state='held'",
            &[],
        )
        .map_err(|_| ApiError::internal())?
        .get(0);
    let (deadline_seconds, reserved_micro_usd) =
        decide(prepaid - spent - held, max_job, units, timeout_seconds).map_err(|failure| {
            ApiError::new(
                failure,
                format!(
                    "budget refused this execution: {} micro-USD remain, per-job cap is {max_job}",
                    prepaid - spent - held
                ),
            )
        })?;
    transaction
        .execute(
            "INSERT INTO reservations (execution_id, reserved_micro_usd, resource_units, state, worker_id)
             VALUES ($1,$2,$3,'held',$4)",
            &[&execution_id, &reserved_micro_usd, &units, &worker_id],
        )
        .map_err(|_| ApiError::internal())?;
    transaction.commit().map_err(|_| ApiError::internal())?;
    Ok(Admission {
        execution_id: execution_id.to_owned(),
        units,
        reserved_micro_usd,
        deadline_seconds,
        limited_by_budget: deadline_seconds < timeout_seconds,
    })
}

#[allow(clippy::too_many_arguments)]
fn record(
    transaction: &mut Transaction,
    execution_id: &str,
    units: i64,
    elapsed_ms: i64,
    amount_micro_usd: i64,
    worker_id: &str,
    context: &str,
    source: &str,
) -> Result<()> {
    // One event per execution per meter, keyed so a replayed settlement is a no-op.
    transaction.execute(
        "INSERT INTO usage_events (event_id, execution_id, meter_name, quantity, unit,
            resource_units, amount_micro_usd, rate_card_version, worker_id, context, source,
            started_at, ended_at)
         VALUES ($1,$2,$3,$4,'millisecond',$5,$6,$7,$8,$9,$10,
                 now() - ($4::BIGINT * interval '1 millisecond'), now())
         ON CONFLICT (event_id) DO NOTHING",
        &[
            &format!("{execution_id}:{METER}"),
            &execution_id,
            &METER,
            &elapsed_ms,
            &units,
            &amount_micro_usd,
            &pricing::RATE_CARD_VERSION,
            &worker_id,
            &context,
            &source,
        ],
    )?;
    Ok(())
}

/// Writes the usage event and releases the unused part of the reservation. Called
/// for every terminal execution, whatever its status.
pub(crate) fn settle(
    client: &mut Client,
    admission: &Admission,
    elapsed_ms: i64,
    worker_id: &str,
    context: &str,
) -> Result<i64> {
    let amount = pricing::charge_micro_usd(admission.units, u64::try_from(elapsed_ms).unwrap_or(0));
    let mut transaction = client.transaction()?;
    record(
        &mut transaction,
        &admission.execution_id,
        admission.units,
        elapsed_ms,
        amount,
        worker_id,
        context,
        "worker",
    )?;
    transaction.execute(
        "UPDATE reservations SET state='settled', settled_micro_usd=$2, settled_at=now()
         WHERE execution_id=$1 AND state='held'",
        &[&admission.execution_id, &amount],
    )?;
    transaction.commit()?;
    Ok(amount)
}

/// Current budget position, for the operator view.
pub(crate) fn summary(client: &mut Client) -> Result<Value, ApiError> {
    let limits = client
        .query_one(
            "SELECT prepaid_micro_usd, max_job_micro_usd FROM budget WHERE budget_id=$1",
            &[&BUDGET_ID],
        )
        .map_err(|_| ApiError::internal())?;
    let prepaid: i64 = limits.get(0);
    let max_job: i64 = limits.get(1);
    let totals = client
        .query_one(
            "SELECT (SELECT COALESCE(SUM(amount_micro_usd),0)::BIGINT FROM usage_events),
                    (SELECT COALESCE(SUM(reserved_micro_usd),0)::BIGINT FROM reservations WHERE state='held'),
                    (SELECT COUNT(*)::BIGINT FROM usage_events),
                    (SELECT COUNT(*)::BIGINT FROM reservations WHERE state='held')",
            &[],
        )
        .map_err(|_| ApiError::internal())?;
    let spent: i64 = totals.get(0);
    let held: i64 = totals.get(1);
    Ok(json!({
        "budget_id": BUDGET_ID,
        "prepaid_micro_usd": prepaid,
        "spent_micro_usd": spent,
        "held_micro_usd": held,
        "available_micro_usd": prepaid - spent - held,
        "max_job_micro_usd": max_job,
        "usage_events": totals.get::<_, i64>(2),
        "held_reservations": totals.get::<_, i64>(3),
        "rate_card_version": pricing::RATE_CARD_VERSION,
        "compute_micro_usd_per_unit_hour": pricing::COMPUTE_MICRO_USD_PER_UNIT_HOUR,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = pricing::COMPUTE_MICRO_USD_PER_UNIT_HOUR;

    #[test]
    fn a_comfortable_budget_grants_the_requested_timeout() {
        let (deadline, reserved) = decide(HOUR, HOUR, 1, 30).unwrap();
        assert_eq!(deadline, 30);
        assert_eq!(reserved, pricing::charge_micro_usd(1, 30_000));
    }

    #[test]
    fn the_per_job_cap_shortens_the_deadline_instead_of_refusing() {
        // Enough balance for an hour, but the cap only pays for 36 seconds.
        let (deadline, reserved) = decide(HOUR, 1_000, 1, 60).unwrap();
        assert_eq!(deadline, 36);
        assert!(reserved <= 1_000);
    }

    #[test]
    fn a_thin_balance_shortens_the_deadline_too() {
        let (deadline, reserved) = decide(1_000, HOUR, 1, 60).unwrap();
        assert_eq!(deadline, 36);
        assert!(reserved <= 1_000);
    }

    #[test]
    fn a_larger_class_gets_proportionally_less_time() {
        let (one, _) = decide(HOUR, 1_000, 1, 600).unwrap();
        let (two, _) = decide(HOUR, 1_000, 2, 600).unwrap();
        assert_eq!(one, 2 * two);
    }

    #[test]
    fn an_exhausted_balance_is_refused_and_safe_to_retry_later() {
        for available in [0, -5_000, 27] {
            let refusal = decide(available, HOUR, 1, 30).unwrap_err();
            assert_eq!(refusal, failure::BUDGET_EXHAUSTED);
            // Nothing ran, so sending it again once funded is legitimate.
            assert!(refusal.retry_safe);
        }
    }

    #[test]
    fn a_cap_too_small_for_one_second_blames_the_cap() {
        let refusal = decide(HOUR, 10, 1, 30).unwrap_err();
        assert_eq!(refusal, failure::BUDGET_JOB_COST);
        assert!(!refusal.retry_safe);
    }

    #[test]
    fn a_reservation_always_covers_what_the_deadline_can_cost() {
        for units in [1, 2, 32] {
            for available in [1_000, 50_000, HOUR, 10 * HOUR] {
                for timeout in [1, 30, 60] {
                    if let Ok((deadline, reserved)) = decide(available, available, units, timeout) {
                        assert!(deadline <= timeout);
                        assert!(
                            pricing::charge_micro_usd(units, deadline * 1000) <= reserved,
                            "under-reserved {units}/{available}/{timeout}"
                        );
                        assert!(reserved <= available);
                    }
                }
            }
        }
    }
}
