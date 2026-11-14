//! Versioned rate card and exact money arithmetic.
//!
//! Money is integer micro-USD. A ledger must not accumulate binary floating-point
//! error, and every charge below is derived from integers only.
//!
//! The price is a development placeholder. The blueprint is explicit that no price
//! here has been validated against real infrastructure cost, so this exists to make
//! the metering path exercisable, not to propose what a customer should pay.

/// Bump this whenever a rate changes. Each usage event stores the version it was
/// priced with, so an old event keeps the price it was charged at.
pub(crate) const RATE_CARD_VERSION: &str = "local-dev-1";

/// Price of one normalized compute unit for one hour, in micro-USD ($0.10).
pub(crate) const COMPUTE_MICRO_USD_PER_UNIT_HOUR: i64 = 100_000;

const MILLIS_PER_HOUR: i128 = 3_600_000;
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;

/// Normalized billable size, `max(vCPU, GiB RAM / 4)`, as proposed in the
/// blueprint's billable-dimensions table. Memory-heavy classes therefore cost more
/// than their core count alone suggests. Never returns less than one.
pub(crate) fn normalized_units(cpu: u16, memory_bytes: u64) -> i64 {
    let by_memory = memory_bytes / (4 * BYTES_PER_GIB);
    i64::from(cpu)
        .max(i64::try_from(by_memory).unwrap_or(i64::MAX))
        .max(1)
}

/// Cost of holding `units` for `elapsed_ms`, rounded **up** to the next micro-USD.
///
/// Rounding up is deliberate: a budget that under-charges can be overrun, and the
/// blueprint asks for a safety margin rather than a favourable rounding mode.
pub(crate) fn charge_micro_usd(units: i64, elapsed_ms: u64) -> i64 {
    if units <= 0 {
        return 0;
    }
    let numerator =
        i128::from(units) * i128::from(elapsed_ms) * i128::from(COMPUTE_MICRO_USD_PER_UNIT_HOUR);
    // Both sides are non-negative here, so this is ceiling division.
    let rounded_up = (numerator + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR;
    i64::try_from(rounded_up).unwrap_or(i64::MAX)
}

/// Whole seconds of `units` that `micro_usd` can pay for, rounded **down**, so the
/// deadline derived from it can never cost more than the budget allows.
pub(crate) fn affordable_seconds(units: i64, micro_usd: i64) -> u64 {
    if units <= 0 || micro_usd <= 0 {
        return 0;
    }
    let seconds = i128::from(micro_usd) * 3600
        / (i128::from(units) * i128::from(COMPUTE_MICRO_USD_PER_UNIT_HOUR));
    u64::try_from(seconds).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_units_follow_the_blueprint_formula() {
        // The two local classes are CPU-bound at 1 GiB per vCPU.
        assert_eq!(normalized_units(1, BYTES_PER_GIB), 1);
        assert_eq!(normalized_units(2, 2 * BYTES_PER_GIB), 2);
        // A memory-heavy class bills on memory instead: 128 GiB / 4 = 32.
        assert_eq!(normalized_units(8, 128 * BYTES_PER_GIB), 32);
        // Never free, whatever the inputs claim.
        assert_eq!(normalized_units(0, 0), 1);
    }

    #[test]
    fn charges_round_up_so_a_budget_is_never_overrun() {
        // One unit-hour is exactly the rate.
        assert_eq!(
            charge_micro_usd(1, 3_600_000),
            COMPUTE_MICRO_USD_PER_UNIT_HOUR
        );
        assert_eq!(
            charge_micro_usd(2, 3_600_000),
            2 * COMPUTE_MICRO_USD_PER_UNIT_HOUR
        );
        // A single millisecond is not free: it rounds up to 1 micro-USD.
        assert_eq!(charge_micro_usd(1, 1), 1);
        // Zero elapsed time is the only zero charge.
        assert_eq!(charge_micro_usd(1, 0), 0);
        // 36 seconds of one unit is exactly 1% of the hourly rate.
        assert_eq!(charge_micro_usd(1, 36_000), 1_000);
    }

    #[test]
    fn affordable_seconds_round_down_and_invert_the_charge() {
        // A full hour's budget buys an hour, and no more.
        assert_eq!(affordable_seconds(1, COMPUTE_MICRO_USD_PER_UNIT_HOUR), 3600);
        assert_eq!(affordable_seconds(2, COMPUTE_MICRO_USD_PER_UNIT_HOUR), 1800);
        // Too little to buy a whole second.
        assert_eq!(affordable_seconds(1, 1), 0);
        assert_eq!(affordable_seconds(1, 0), 0);
        assert_eq!(affordable_seconds(0, 1_000_000), 0);
        // What it says is affordable must actually be affordable.
        for micro_usd in [1_000, 25_000, 100_000, 999_999] {
            for units in [1, 2, 32] {
                let seconds = affordable_seconds(units, micro_usd);
                assert!(charge_micro_usd(units, seconds * 1000) <= micro_usd);
            }
        }
    }
}
