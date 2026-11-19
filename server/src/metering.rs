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
//!
//! The budget row carries `spent_micro_usd` and `held_micro_usd` accumulators so
//! admission never has to sum a ledger that, by rule 2, only ever grows. They are a
//! derived cache, not a second ledger: every writer updates them in the same
//! transaction as its ledger write, and each worker start recomputes them from the
//! true sums under the budget lock — so the one O(ledger) scan left runs per boot,
//! not per admission, and a delta missed by an older binary heals at the next start.
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
    rate: &pricing::RateCard,
) -> Result<(u64, i64), failure::Failure> {
    let ceiling = available_micro_usd.min(max_job_micro_usd);
    let affordable = rate.affordable_seconds(units, ceiling);
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
    Ok((deadline, rate.charge_micro_usd(units, deadline * 1000)))
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
        CREATE INDEX IF NOT EXISTS reservations_held ON reservations(worker_id) WHERE state = 'held';
        ALTER TABLE reservations ADD COLUMN IF NOT EXISTS rate_micro_usd_per_unit_hour BIGINT;
        ALTER TABLE reservations ADD COLUMN IF NOT EXISTS rate_card_version TEXT;
        ALTER TABLE budget ADD COLUMN IF NOT EXISTS spent_micro_usd BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE budget ADD COLUMN IF NOT EXISTS held_micro_usd BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE budget ADD COLUMN IF NOT EXISTS reconciled_at TIMESTAMPTZ;",
    )?;
    // Seed once. A restart must not silently refill a spent balance.
    transaction.execute(
        "INSERT INTO budget (budget_id, prepaid_micro_usd, max_job_micro_usd)
         VALUES ($1,$2,$3) ON CONFLICT (budget_id) DO NOTHING",
        &[&BUDGET_ID, &prepaid, &max_job],
    )?;
    Ok(())
}

/// Recomputes the budget accumulators from the ledger's true sums, under the budget
/// lock. Run once per worker start: taking the row lock *before* reading the sums
/// means any concurrent settle is either already visible to this snapshot or blocked
/// until after the overwrite, when it applies its own delta — no update is lost in
/// either order. This is also what heals a delta an older binary never applied.
pub(crate) fn reconcile(transaction: &mut Transaction) -> Result<()> {
    transaction.execute(
        "SELECT 1 FROM budget WHERE budget_id=$1 FOR UPDATE",
        &[&BUDGET_ID],
    )?;
    transaction.execute(
        "UPDATE budget SET
            spent_micro_usd = (SELECT COALESCE(SUM(amount_micro_usd),0) FROM usage_events),
            held_micro_usd = (SELECT COALESCE(SUM(reserved_micro_usd),0) FROM reservations WHERE state='held'),
            reconciled_at = now()
         WHERE budget_id=$1",
        &[&BUDGET_ID],
    )?;
    Ok(())
}

/// What one settlement moves on the budget row, decided by what its statements
/// actually changed: a replayed event (`inserted == 0`) spends nothing a second
/// time, and a hold someone else already released (`released == 0`) is not released
/// again. Counting rows rather than assuming is what keeps the accumulators exact
/// under crash-retry and concurrent recovery.
fn budget_deltas(inserted: u64, released: u64, amount: i64, reserved: i64) -> (i64, i64) {
    (
        if inserted == 1 { amount } else { 0 },
        if released == 1 { -reserved } else { 0 },
    )
}

fn apply_deltas(transaction: &mut Transaction, spent: i64, held: i64) -> Result<()> {
    if spent == 0 && held == 0 {
        return Ok(());
    }
    transaction.execute(
        "UPDATE budget SET spent_micro_usd = spent_micro_usd + $2,
            held_micro_usd = held_micro_usd + $3
         WHERE budget_id=$1",
        &[&BUDGET_ID, &spent, &held],
    )?;
    Ok(())
}

/// The price recovery puts on an orphan: the rate stored on the reservation at
/// admission, so a rate change between admission and recovery cannot reprice work,
/// with the current card standing in only for rows older than the stored columns.
/// Charges only what the history row proves ran, and never above the hold.
fn recovery_amount(
    stored_rate: Option<i64>,
    current: &pricing::RateCard,
    units: i64,
    elapsed_ms: i64,
    reserved: i64,
) -> i64 {
    let rate = stored_rate.unwrap_or(current.compute_micro_usd_per_unit_hour);
    pricing::charge_at(rate, units, u64::try_from(elapsed_ms).unwrap_or(0)).min(reserved)
}

/// Settles reservations this worker abandoned by restarting, so no accepted
/// execution is left both unmetered and holding funds. The charge uses whatever
/// elapsed time the history row recorded; an execution that never reached its
/// terminal update is charged nothing, and the event says it came from recovery.
pub(crate) fn recover(
    transaction: &mut Transaction,
    worker_id: &str,
    current: &pricing::RateCard,
) -> Result<u64> {
    let orphans = transaction.query(
        "SELECT r.execution_id, r.reserved_micro_usd, r.resource_units,
                COALESCE(h.elapsed_ms,0), COALESCE(h.context,'{}'),
                r.rate_micro_usd_per_unit_hour, r.rate_card_version
         FROM reservations r LEFT JOIN query_history h ON h.query_id = r.execution_id
         WHERE r.state = 'held' AND r.worker_id = $1",
        &[&worker_id],
    )?;
    let (mut spent_delta, mut held_delta) = (0i64, 0i64);
    for row in &orphans {
        let execution_id: String = row.get(0);
        let reserved: i64 = row.get(1);
        let units: i64 = row.get(2);
        let elapsed_ms: i64 = row.get(3);
        let context: String = row.get(4);
        let stored_rate: Option<i64> = row.get(5);
        let stored_version: Option<String> = row.get(6);
        let amount = recovery_amount(stored_rate, current, units, elapsed_ms, reserved);
        let inserted = record(
            transaction,
            &execution_id,
            units,
            elapsed_ms,
            amount,
            stored_version.as_deref().unwrap_or(&current.version),
            worker_id,
            &context,
            "recovery",
        )?;
        // `state='held'` guards a race with another settler of the same orphan: the
        // counts say which of us actually moved it, and the deltas follow the counts.
        let released = transaction.execute(
            "UPDATE reservations SET state='settled', settled_micro_usd=$2, settled_at=now()
             WHERE execution_id=$1 AND state='held'",
            &[&execution_id, &amount],
        )?;
        let (spent, held) = budget_deltas(inserted, released, amount, reserved);
        spent_delta += spent;
        held_delta += held;
    }
    apply_deltas(transaction, spent_delta, held_delta)?;
    Ok(orphans.len() as u64)
}

/// Marks one dead worker identity's `running` history rows `interrupted`, with the
/// shared unknown-commit wording. The worker calls this for itself at startup; the
/// gateway calls it for a session pod it is settling, because that pod's identity
/// never recurs and so no restart would ever reconcile its rows.
pub(crate) fn interrupt_abandoned_history(
    transaction: &mut Transaction,
    worker_id: &str,
) -> Result<u64> {
    Ok(transaction.execute(
        "UPDATE query_history SET status='interrupted', finished_at=now(),
            error=$2, failure_kind=$3, retry_safe=$4
         WHERE status='running' AND worker_id=$1",
        &[
            &worker_id,
            &failure::UNKNOWN_COMMIT_MESSAGE,
            &failure::INTERRUPTED.kind,
            &failure::INTERRUPTED.retry_safe,
        ],
    )?)
}

/// The prefix-wide form, for the gateway's startup pass: sound only after the pod
/// sweep, when every identity matching the prefix is provably dead. This is also
/// what repairs rows stranded before the per-settle reconciliation existed.
pub(crate) fn interrupt_abandoned_prefix(
    transaction: &mut Transaction,
    prefix: &str,
) -> Result<u64> {
    Ok(transaction.execute(
        "UPDATE query_history SET status='interrupted', finished_at=now(),
            error=$2, failure_kind=$3, retry_safe=$4
         WHERE status='running' AND worker_id LIKE $1 || '%'",
        &[
            &prefix,
            &failure::UNKNOWN_COMMIT_MESSAGE,
            &failure::INTERRUPTED.kind,
            &failure::INTERRUPTED.retry_safe,
        ],
    )?)
}

/// Worker identities that still hold funds and whose names begin with `prefix`.
///
/// For a session pod the identity is the pod's own name, so these are the holds that
/// no restart will ever settle: a persistent worker recovers its own when it comes
/// back, but a pod that has been deleted never comes back. The gateway is the only
/// process left that knows they existed.
pub(crate) fn held_matching(client: &mut Client, prefix: &str) -> Result<Vec<String>> {
    Ok(client
        .query(
            "SELECT DISTINCT worker_id FROM reservations
             WHERE state = 'held' AND worker_id LIKE $1 || '%'",
            &[&prefix],
        )?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

/// Reserves funds for one execution. The budget row is locked for the duration, so
/// concurrent admissions on different workers cannot both pass on the same balance.
pub(crate) fn admit(
    client: &mut Client,
    execution_id: &str,
    units: i64,
    timeout_seconds: u64,
    worker_id: &str,
    rate: &pricing::RateCard,
) -> Result<Admission, ApiError> {
    let mut transaction = client.transaction().map_err(|_| ApiError::internal())?;
    // The accumulators ride the same locked row as the limits, so admission reads
    // the whole position in one O(1) select however large the ledger has grown.
    let limits = transaction
        .query_one(
            "SELECT prepaid_micro_usd, max_job_micro_usd, spent_micro_usd, held_micro_usd
             FROM budget WHERE budget_id=$1 FOR UPDATE",
            &[&BUDGET_ID],
        )
        .map_err(|_| ApiError::internal())?;
    let prepaid: i64 = limits.get(0);
    let max_job: i64 = limits.get(1);
    let spent: i64 = limits.get(2);
    let held: i64 = limits.get(3);
    let (deadline_seconds, reserved_micro_usd) = decide(
        prepaid - spent - held,
        max_job,
        units,
        timeout_seconds,
        rate,
    )
    .map_err(|failure| {
        ApiError::new(
            failure,
            format!(
                "budget refused this execution: {} micro-USD remain, per-job cap is {max_job}",
                prepaid - spent - held
            ),
        )
    })?;
    // The rate rides on the reservation so settlement and recovery price this
    // execution at the card it was admitted under, whatever the card is by then.
    transaction
        .execute(
            "INSERT INTO reservations (execution_id, reserved_micro_usd, resource_units, state,
                worker_id, rate_micro_usd_per_unit_hour, rate_card_version)
             VALUES ($1,$2,$3,'held',$4,$5,$6)",
            &[
                &execution_id,
                &reserved_micro_usd,
                &units,
                &worker_id,
                &rate.compute_micro_usd_per_unit_hour,
                &rate.version,
            ],
        )
        .map_err(|_| ApiError::internal())?;
    apply_deltas(&mut transaction, 0, reserved_micro_usd).map_err(|_| ApiError::internal())?;
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
    rate_card_version: &str,
    worker_id: &str,
    context: &str,
    source: &str,
) -> Result<u64> {
    // One event per execution per meter, keyed so a replayed settlement is a no-op.
    // Returns the insert count so a caller can tell a first write from a replay.
    let inserted = transaction.execute(
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
            &rate_card_version,
            &worker_id,
            &context,
            &source,
        ],
    )?;
    Ok(inserted)
}

/// Writes the usage event and releases the unused part of the reservation. Called
/// for every terminal execution, whatever its status, by the process that admitted
/// it — so the process card and the admission's card are the same card.
pub(crate) fn settle(
    client: &mut Client,
    admission: &Admission,
    elapsed_ms: i64,
    worker_id: &str,
    context: &str,
    rate: &pricing::RateCard,
) -> Result<i64> {
    let amount = rate.charge_micro_usd(admission.units, u64::try_from(elapsed_ms).unwrap_or(0));
    let mut transaction = client.transaction()?;
    let inserted = record(
        &mut transaction,
        &admission.execution_id,
        admission.units,
        elapsed_ms,
        amount,
        &rate.version,
        worker_id,
        context,
        "worker",
    )?;
    let released = transaction.execute(
        "UPDATE reservations SET state='settled', settled_micro_usd=$2, settled_at=now()
         WHERE execution_id=$1 AND state='held'",
        &[&admission.execution_id, &amount],
    )?;
    let (spent, held) = budget_deltas(inserted, released, amount, admission.reserved_micro_usd);
    apply_deltas(&mut transaction, spent, held)?;
    transaction.commit()?;
    Ok(amount)
}

/// Current budget position, for the operator view. The rate reported is the card
/// this process prices *new* work with; recorded events carry their own.
pub(crate) fn summary(client: &mut Client, rate: &pricing::RateCard) -> Result<Value, ApiError> {
    // The accumulators, not fresh SUMs: the operator reads exactly the position the
    // admission gate enforces, and the two can never disagree in a report.
    let limits = client
        .query_one(
            "SELECT prepaid_micro_usd, max_job_micro_usd, spent_micro_usd, held_micro_usd
             FROM budget WHERE budget_id=$1",
            &[&BUDGET_ID],
        )
        .map_err(|_| ApiError::internal())?;
    let prepaid: i64 = limits.get(0);
    let max_job: i64 = limits.get(1);
    let spent: i64 = limits.get(2);
    let held: i64 = limits.get(3);
    let totals = client
        .query_one(
            "SELECT (SELECT COUNT(*)::BIGINT FROM usage_events),
                    (SELECT COUNT(*)::BIGINT FROM reservations WHERE state='held')",
            &[],
        )
        .map_err(|_| ApiError::internal())?;
    Ok(json!({
        "budget_id": BUDGET_ID,
        "prepaid_micro_usd": prepaid,
        "spent_micro_usd": spent,
        "held_micro_usd": held,
        "available_micro_usd": prepaid - spent - held,
        "max_job_micro_usd": max_job,
        "usage_events": totals.get::<_, i64>(0),
        "held_reservations": totals.get::<_, i64>(1),
        "rate_card_version": rate.version,
        "compute_micro_usd_per_unit_hour": rate.compute_micro_usd_per_unit_hour,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card() -> pricing::RateCard {
        pricing::dev()
    }
    const HOUR: i64 = 100_000;

    #[test]
    fn a_comfortable_budget_grants_the_requested_timeout() {
        let (deadline, reserved) = decide(HOUR, HOUR, 1, 30, &card()).unwrap();
        assert_eq!(deadline, 30);
        assert_eq!(reserved, card().charge_micro_usd(1, 30_000));
    }

    #[test]
    fn the_per_job_cap_shortens_the_deadline_instead_of_refusing() {
        // Enough balance for an hour, but the cap only pays for 36 seconds.
        let (deadline, reserved) = decide(HOUR, 1_000, 1, 60, &card()).unwrap();
        assert_eq!(deadline, 36);
        assert!(reserved <= 1_000);
    }

    #[test]
    fn a_thin_balance_shortens_the_deadline_too() {
        let (deadline, reserved) = decide(1_000, HOUR, 1, 60, &card()).unwrap();
        assert_eq!(deadline, 36);
        assert!(reserved <= 1_000);
    }

    #[test]
    fn a_larger_class_gets_proportionally_less_time() {
        let (one, _) = decide(HOUR, 1_000, 1, 600, &card()).unwrap();
        let (two, _) = decide(HOUR, 1_000, 2, 600, &card()).unwrap();
        assert_eq!(one, 2 * two);
    }

    #[test]
    fn an_exhausted_balance_is_refused_and_safe_to_retry_later() {
        for available in [0, -5_000, 27] {
            let refusal = decide(available, HOUR, 1, 30, &card()).unwrap_err();
            assert_eq!(refusal, failure::BUDGET_EXHAUSTED);
            // Nothing ran, so sending it again once funded is legitimate.
            assert!(refusal.retry_safe);
        }
    }

    #[test]
    fn a_cap_too_small_for_one_second_blames_the_cap() {
        let refusal = decide(HOUR, 10, 1, 30, &card()).unwrap_err();
        assert_eq!(refusal, failure::BUDGET_JOB_COST);
        assert!(!refusal.retry_safe);
    }

    #[test]
    fn recovery_prices_an_orphan_at_the_rate_it_was_admitted_under() {
        // The deployment's rate has since doubled; the orphan keeps its old price.
        let doubled = pricing::RateCard {
            version: "dev-2x".into(),
            compute_micro_usd_per_unit_hour: 2 * HOUR,
        };
        let at_old_rate = recovery_amount(Some(HOUR), &doubled, 1, 36_000, HOUR);
        assert_eq!(at_old_rate, 1_000);
        // A row from before the stored-rate columns is the only one priced at the
        // current card — there is nothing older to price it with.
        assert_eq!(recovery_amount(None, &doubled, 1, 36_000, HOUR), 2_000);
        // Whatever the rate says, the hold is the ceiling.
        assert_eq!(
            recovery_amount(Some(HOUR), &doubled, 1, 3_600_000, 5_000),
            5_000
        );
        // An execution history never saw ran is free, at any rate.
        assert_eq!(recovery_amount(Some(HOUR), &doubled, 1, 0, HOUR), 0);
    }

    #[test]
    fn a_doubled_rate_halves_the_deadline_a_budget_buys() {
        let doubled = pricing::RateCard {
            version: "dev-2x".into(),
            compute_micro_usd_per_unit_hour: 2 * HOUR,
        };
        let (at_dev, _) = decide(HOUR, HOUR, 1, 7200, &card()).unwrap();
        let (at_double, reserved) = decide(HOUR, HOUR, 1, 7200, &doubled).unwrap();
        assert_eq!(at_dev, 3600);
        assert_eq!(at_double, 1800);
        // And the reservation is priced by the same card as the deadline.
        assert_eq!(reserved, doubled.charge_micro_usd(1, 1800 * 1000));
    }

    #[test]
    fn budget_deltas_follow_the_row_counts_not_the_caller() {
        // A first settlement moves both accumulators.
        assert_eq!(budget_deltas(1, 1, 700, 1_000), (700, -1_000));
        // A replayed event spends nothing a second time.
        assert_eq!(budget_deltas(0, 1, 700, 1_000), (0, -1_000));
        // A hold someone else already released is not released again.
        assert_eq!(budget_deltas(1, 0, 700, 1_000), (700, 0));
        // A full replay moves nothing at all.
        assert_eq!(budget_deltas(0, 0, 700, 1_000), (0, 0));
        // A free execution still releases its hold.
        assert_eq!(budget_deltas(1, 1, 0, 1_000), (0, -1_000));
    }

    #[test]
    fn a_reservation_always_covers_what_the_deadline_can_cost() {
        for units in [1, 2, 32] {
            for available in [1_000, 50_000, HOUR, 10 * HOUR] {
                for timeout in [1, 30, 60] {
                    if let Ok((deadline, reserved)) =
                        decide(available, available, units, timeout, &card())
                    {
                        assert!(deadline <= timeout);
                        assert!(
                            card().charge_micro_usd(units, deadline * 1000) <= reserved,
                            "under-reserved {units}/{available}/{timeout}"
                        );
                        assert!(reserved <= available);
                    }
                }
            }
        }
    }
}
