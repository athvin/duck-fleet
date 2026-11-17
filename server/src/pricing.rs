//! Versioned rate card and exact money arithmetic.
//!
//! Money is integer micro-USD. A ledger must not accumulate binary floating-point
//! error, and every charge below is derived from integers only.
//!
//! The rate is deployment configuration with a development default. The blueprint is
//! explicit that no price here has been validated against real infrastructure cost;
//! the measured numbers (#18) are what an operator should put in these values, and
//! until then the default exists to make the metering path exercisable, not to
//! propose what a customer should pay.

const MILLIS_PER_HOUR: i128 = 3_600_000;
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;

/// The version recorded on events priced by the default rate. A deployment that
/// changes the rate must change the version with it, so an old event keeps the
/// price it was charged at.
const DEFAULT_VERSION: &str = "local-dev-1";
/// Default price of one normalized compute unit for one hour, in micro-USD ($0.10).
const DEFAULT_COMPUTE_MICRO_USD_PER_UNIT_HOUR: i64 = 100_000;

/// The rates a process prices new work with, resolved once at startup.
///
/// Admission copies the rate onto the reservation it creates, so settlement and
/// recovery price an execution at the card it was admitted under — a deployment
/// that changes its rates changes only work admitted afterwards, and the
/// append-only ledger is never repriced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RateCard {
    pub version: String,
    pub compute_micro_usd_per_unit_hour: i64,
}

impl RateCard {
    /// Reads `DUCKFLEET_RATE_CARD_VERSION` and
    /// `DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR`, defaulting to the development
    /// placeholder. Invalid configuration refuses to start rather than pricing a
    /// ledger with it.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_values(
            std::env::var("DUCKFLEET_RATE_CARD_VERSION").ok(),
            std::env::var("DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR").ok(),
        )
    }

    fn from_values(version: Option<String>, rate: Option<String>) -> anyhow::Result<Self> {
        let version = version.unwrap_or_else(|| DEFAULT_VERSION.to_owned());
        anyhow::ensure!(
            !version.trim().is_empty() && version.len() <= 64,
            "DUCKFLEET_RATE_CARD_VERSION must be 1-64 non-blank characters; \
             it names the rate every event was priced under"
        );
        let rate = match rate {
            None => DEFAULT_COMPUTE_MICRO_USD_PER_UNIT_HOUR,
            Some(text) => text
                .trim()
                .parse::<i64>()
                .ok()
                .filter(|r| *r >= 1)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR must be a whole number \
                     of micro-USD, at least 1: a zero rate would make every budget \
                     infinite ({text:?})"
                    )
                })?,
        };
        Ok(Self {
            version,
            compute_micro_usd_per_unit_hour: rate,
        })
    }

    /// Cost of holding `units` for `elapsed_ms` at this card's rate.
    pub fn charge_micro_usd(&self, units: i64, elapsed_ms: u64) -> i64 {
        charge_at(self.compute_micro_usd_per_unit_hour, units, elapsed_ms)
    }

    /// Whole seconds of `units` that `micro_usd` can pay for, rounded **down**, so
    /// the deadline derived from it can never cost more than the budget allows.
    pub fn affordable_seconds(&self, units: i64, micro_usd: i64) -> u64 {
        if units <= 0 || micro_usd <= 0 {
            return 0;
        }
        let seconds = i128::from(micro_usd) * 3600
            / (i128::from(units) * i128::from(self.compute_micro_usd_per_unit_hour));
        u64::try_from(seconds).unwrap_or(0)
    }
}

/// Normalized billable size, `max(vCPU, GiB RAM / 4)`, as proposed in the
/// blueprint's billable-dimensions table. Memory-heavy classes therefore cost more
/// than their core count alone suggests. Never returns less than one.
pub(crate) fn normalized_units(cpu: u16, memory_bytes: u64) -> i64 {
    let by_memory = memory_bytes / (4 * BYTES_PER_GIB);
    i64::from(cpu)
        .max(i64::try_from(by_memory).unwrap_or(i64::MAX))
        .max(1)
}

/// Cost of holding `units` for `elapsed_ms` at a given per-unit-hour rate, rounded
/// **up** to the next micro-USD.
///
/// Free-standing because recovery prices an orphaned reservation at the rate stored
/// on it when it was admitted, which need not be the rate this process runs with.
/// Rounding up is deliberate: a budget that under-charges can be overrun, and the
/// blueprint asks for a safety margin rather than a favourable rounding mode.
pub(crate) fn charge_at(rate_micro_usd_per_unit_hour: i64, units: i64, elapsed_ms: u64) -> i64 {
    if units <= 0 {
        return 0;
    }
    let numerator =
        i128::from(units) * i128::from(elapsed_ms) * i128::from(rate_micro_usd_per_unit_hour);
    // Both sides are non-negative here, so this is ceiling division.
    let rounded_up = (numerator + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR;
    i64::try_from(rounded_up).unwrap_or(i64::MAX)
}

/// The default card, for tests that need a concrete rate.
#[cfg(test)]
pub(crate) fn dev() -> RateCard {
    RateCard {
        version: DEFAULT_VERSION.to_owned(),
        compute_micro_usd_per_unit_hour: DEFAULT_COMPUTE_MICRO_USD_PER_UNIT_HOUR,
    }
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
        let card = dev();
        // One unit-hour is exactly the rate.
        assert_eq!(
            card.charge_micro_usd(1, 3_600_000),
            card.compute_micro_usd_per_unit_hour
        );
        assert_eq!(
            card.charge_micro_usd(2, 3_600_000),
            2 * card.compute_micro_usd_per_unit_hour
        );
        // A single millisecond is not free: it rounds up to 1 micro-USD.
        assert_eq!(card.charge_micro_usd(1, 1), 1);
        // Zero elapsed time is the only zero charge.
        assert_eq!(card.charge_micro_usd(1, 0), 0);
        // 36 seconds of one unit is exactly 1% of the hourly rate.
        assert_eq!(card.charge_micro_usd(1, 36_000), 1_000);
    }

    #[test]
    fn affordable_seconds_round_down_and_invert_the_charge() {
        let card = dev();
        let hour = card.compute_micro_usd_per_unit_hour;
        // A full hour's budget buys an hour, and no more.
        assert_eq!(card.affordable_seconds(1, hour), 3600);
        assert_eq!(card.affordable_seconds(2, hour), 1800);
        // Too little to buy a whole second.
        assert_eq!(card.affordable_seconds(1, 1), 0);
        assert_eq!(card.affordable_seconds(1, 0), 0);
        assert_eq!(card.affordable_seconds(0, 1_000_000), 0);
        // What it says is affordable must actually be affordable.
        for micro_usd in [1_000, 25_000, 100_000, 999_999] {
            for units in [1, 2, 32] {
                let seconds = card.affordable_seconds(units, micro_usd);
                assert!(card.charge_micro_usd(units, seconds * 1000) <= micro_usd);
            }
        }
    }

    #[test]
    fn the_configured_rate_prices_charges_and_deadlines_alike() {
        let double = RateCard {
            version: "test-2x".into(),
            compute_micro_usd_per_unit_hour: 200_000,
        };
        // Twice the rate: the same hour costs double, the same money buys half.
        assert_eq!(double.charge_micro_usd(1, 3_600_000), 200_000);
        assert_eq!(double.affordable_seconds(1, 100_000), 1800);
        assert_eq!(charge_at(200_000, 1, 36_000), 2_000);
    }

    #[test]
    fn unset_values_are_the_development_placeholder() {
        let card = RateCard::from_values(None, None).unwrap();
        assert_eq!(card, dev());
    }

    #[test]
    fn a_rate_that_cannot_price_a_ledger_refuses_to_start() {
        for rate in ["0", "-5", "0.10", "free", ""] {
            let refused = RateCard::from_values(None, Some(rate.into()));
            assert!(refused.is_err(), "accepted rate {rate:?}");
        }
        for version in ["", "   ", &"v".repeat(65)] {
            let refused = RateCard::from_values(Some(version.to_string()), None);
            assert!(refused.is_err(), "accepted version {version:?}");
        }
        let ok = RateCard::from_values(Some("prod-2026-11".into()), Some("250000".into())).unwrap();
        assert_eq!(ok.version, "prod-2026-11");
        assert_eq!(ok.compute_micro_usd_per_unit_hour, 250_000);
    }
}
