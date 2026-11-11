//! Tells a caller whether a failed request may be replayed.
//!
//! DuckLake detects write conflicts when a transaction commits, not while its
//! statements run, and it discards the losing transaction whole. That is the only
//! failure here whose outcome is known well enough to call a replay safe. Every
//! other failure either changed unknown state or needs the request fixed first,
//! so `retry_safe` stays false rather than guessing.
use axum::http::StatusCode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// Terminal state recorded in query history, or `None` when the request was
    /// rejected before it became a history record.
    pub status: Option<&'static str>,
    /// Machine-readable cause. Stable even if the message wording changes.
    pub kind: &'static str,
    /// True only when the engine proved this request left no partial effect.
    pub retry_safe: bool,
    pub http: StatusCode,
}

/// SQL the engine refused or could not run. Replaying it repeats the failure, and
/// a multi-statement request may already have applied earlier statements.
pub(crate) const SQL_ERROR: Failure = Failure {
    status: Some("failed"),
    kind: "sql_error",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// A DuckLake commit that lost to a transaction that committed first. The loser's
/// transaction is already gone, so the whole transaction can be replayed from
/// `BEGIN`. The engine leaves no transaction open; a `ROLLBACK` here fails.
pub(crate) const CONFLICT: Failure = Failure {
    status: Some("conflict"),
    kind: "transaction_conflict",
    retry_safe: true,
    http: StatusCode::CONFLICT,
};

/// The statement passed its deadline and was interrupted. An interrupted commit
/// may still have committed, so the outcome is unknown.
pub(crate) const TIMEOUT: Failure = Failure {
    status: Some("timed_out"),
    kind: "timeout",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// A caller interrupted the statement. Same unknown-commit caveat as a timeout.
pub(crate) const CANCELED: Failure = Failure {
    status: Some("canceled"),
    kind: "canceled",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// A worker restarted while this request was running. The record is reconciled at
/// startup rather than returned over HTTP, and whether it committed is unknown.
pub(crate) const INTERRUPTED: Failure = Failure {
    status: Some("interrupted"),
    kind: "interrupted",
    retry_safe: false,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

pub(crate) const UNAUTHORIZED: Failure = Failure {
    status: None,
    kind: "unauthorized",
    retry_safe: false,
    http: StatusCode::UNAUTHORIZED,
};

/// The request never reached the engine and needs correcting, not replaying.
pub(crate) const INVALID_REQUEST: Failure = Failure {
    status: None,
    kind: "invalid_request",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// The session is gone or expired. A caller must open a new one, and any write it
/// had open was rolled back when the connection was dropped.
pub(crate) const UNKNOWN_SESSION: Failure = Failure {
    status: None,
    kind: "unknown_session",
    retry_safe: false,
    http: StatusCode::NOT_FOUND,
};

pub(crate) const NOT_FOUND: Failure = Failure {
    status: None,
    kind: "not_found",
    retry_safe: false,
    http: StatusCode::NOT_FOUND,
};

/// This `request_id` was already accepted. The first attempt owns the outcome;
/// inspect its history record instead of sending it again under a new ID.
pub(crate) const DUPLICATE_REQUEST: Failure = Failure {
    status: None,
    kind: "duplicate_request",
    retry_safe: false,
    http: StatusCode::CONFLICT,
};

/// Rejected by a queue or session limit before any SQL ran, so the same request
/// may be sent again once capacity frees up.
pub(crate) const CAPACITY: Failure = Failure {
    status: None,
    kind: "capacity",
    retry_safe: true,
    http: StatusCode::TOO_MANY_REQUESTS,
};

/// A dependency failed or a worker disappeared mid-request. Whether the work
/// committed is unknown, which is exactly when an automatic replay is unsafe.
pub(crate) const UNAVAILABLE: Failure = Failure {
    status: None,
    kind: "unavailable",
    retry_safe: false,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

/// DuckDB's marker for a transaction that lost a commit race. DuckLake repeats it
/// in the catalog commit error, and the same text covers conflicting inserts,
/// deletes, updates, `ALTER`, and `CREATE`.
const CONFLICT_MARKER: &str = "Transaction conflict";

/// Classifies a failed execution. An interruption reason always wins: a statement
/// the platform stopped is reported as stopped, whatever the engine said about it.
pub(crate) fn classify(error: &str, reason: Option<&str>) -> Failure {
    match reason {
        Some("timed_out") => TIMEOUT,
        Some("canceled") => CANCELED,
        _ if error.contains(CONFLICT_MARKER) => CONFLICT,
        _ => SQL_ERROR,
    }
}

/// Resolves a stored history `failure_kind` back to its classification, so an
/// older record keeps the meaning it was written with.
pub(crate) fn by_kind(kind: &str) -> Option<Failure> {
    [
        SQL_ERROR,
        CONFLICT,
        TIMEOUT,
        CANCELED,
        INTERRUPTED,
        UNAUTHORIZED,
        INVALID_REQUEST,
        UNKNOWN_SESSION,
        NOT_FOUND,
        DUPLICATE_REQUEST,
        CAPACITY,
        UNAVAILABLE,
    ]
    .into_iter()
    .find(|failure| failure.kind == kind)
}

/// Terminal states a history record may hold, for filter validation.
pub(crate) const STATUSES: [&str; 7] = [
    "running",
    "succeeded",
    "failed",
    "conflict",
    "canceled",
    "timed_out",
    "interrupted",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim commit errors captured from DuckDB 1.5.4 with a DuckLake
    /// PostgreSQL catalog, one per conflicting write shape observed.
    const OBSERVED_CONFLICTS: [&str; 6] = [
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to insert into table with index \"93\" - but another transaction has deleted from it: Error code 1: Unknown error code",
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to delete from table with index \"95\" - but another transaction has inserted into it: Error code 1: Unknown error code",
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to delete from file with index \"94\" - but another transaction has deleted from it: Error code 1: Unknown error code",
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to alter table with index \"97\" - but another transaction has altered it: Error code 1: Unknown error code",
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to create table \"race_create\" in schema \"probe\" - but this table has been created by another transaction already: Error code 1: Unknown error code",
        "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to insert into table with index \"99\" - but another transaction has deleted from it: Error code 1: Unknown error code",
    ];

    #[test]
    fn observed_commit_conflicts_are_retry_safe() {
        for error in OBSERVED_CONFLICTS {
            let failure = classify(error, None);
            assert_eq!(failure, CONFLICT, "not classified as a conflict: {error}");
            assert!(failure.retry_safe);
            assert_eq!(failure.http, StatusCode::CONFLICT);
        }
    }

    #[test]
    fn ordinary_sql_errors_are_never_retry_safe() {
        for error in [
            "Catalog Error: Table with name missing_table does not exist!",
            "Binder Error: Referenced column \"missing_column\" not found",
            "Constraint Error: NOT NULL constraint failed",
            "TransactionContext Error: cannot rollback - no transaction is active",
            "Out of Memory Error: could not allocate block",
        ] {
            let failure = classify(error, None);
            assert_eq!(failure, SQL_ERROR, "misclassified: {error}");
            assert!(!failure.retry_safe);
        }
    }

    #[test]
    fn an_interruption_outranks_the_engine_message() {
        // A deadline can fire while a commit is conflicting. The platform stopped
        // this statement, and that leaves the commit outcome unknown.
        let conflict = OBSERVED_CONFLICTS[0];
        assert_eq!(classify(conflict, Some("timed_out")), TIMEOUT);
        assert_eq!(classify(conflict, Some("canceled")), CANCELED);
        assert!(!classify(conflict, Some("timed_out")).retry_safe);
    }

    #[test]
    fn only_proven_no_op_failures_are_retry_safe() {
        let safe: Vec<&str> = [
            SQL_ERROR,
            CONFLICT,
            TIMEOUT,
            CANCELED,
            INTERRUPTED,
            UNAUTHORIZED,
            INVALID_REQUEST,
            UNKNOWN_SESSION,
            NOT_FOUND,
            DUPLICATE_REQUEST,
            CAPACITY,
            UNAVAILABLE,
        ]
        .into_iter()
        .filter(|f| f.retry_safe)
        .map(|f| f.kind)
        .collect();
        assert_eq!(safe, ["transaction_conflict", "capacity"]);
    }

    #[test]
    fn stored_kinds_round_trip_and_statuses_are_known() {
        for failure in [CONFLICT, SQL_ERROR, TIMEOUT, CANCELED, INTERRUPTED] {
            assert_eq!(by_kind(failure.kind), Some(failure));
            assert!(STATUSES.contains(&failure.status.unwrap()));
        }
        assert_eq!(by_kind("invented_later"), None);
    }
}
