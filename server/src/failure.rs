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

/// The prepaid balance cannot cover even a one-second execution. No SQL ran, so
/// a funded retry may use a new attempt ID linked to this refused request ID.
pub(crate) const BUDGET_EXHAUSTED: Failure = Failure {
    status: Some("refused"),
    kind: "budget_exhausted",
    retry_safe: true,
    http: StatusCode::PAYMENT_REQUIRED,
};

/// The per-job cost cap is too low to buy a single second at this compute size.
/// Resending is pointless: raise the cap or ask for a smaller class.
pub(crate) const BUDGET_JOB_COST: Failure = Failure {
    status: Some("refused"),
    kind: "budget_job_cost",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// The statement was stopped because it reached the cost its budget allowed. Like
/// any interruption, a commit caught by it may still have committed.
pub(crate) const BUDGET_EXCEEDED: Failure = Failure {
    status: Some("budget_exceeded"),
    kind: "budget_exceeded",
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

/// The error text every reconciler writes on an interrupted record. One constant,
/// because a worker's own restart and the gateway settling a dead session pod are
/// the same fact to a reader, and the wording must not drift between them.
pub(crate) const UNKNOWN_COMMIT_MESSAGE: &str =
    "worker restarted; commit outcome may be unknown, do not blindly retry";

pub(crate) const UNAUTHORIZED: Failure = Failure {
    status: None,
    kind: "unauthorized",
    retry_safe: false,
    http: StatusCode::UNAUTHORIZED,
};

/// A valid credential asking for something only the bootstrap may do. Distinct
/// from `UNAUTHORIZED`: the caller is known, the request is understood, and
/// resending it with the same credential can never succeed.
pub(crate) const FORBIDDEN: Failure = Failure {
    status: None,
    kind: "forbidden",
    retry_safe: false,
    http: StatusCode::FORBIDDEN,
};

/// The request never reached the engine and needs correcting, not replaying.
pub(crate) const INVALID_REQUEST: Failure = Failure {
    status: None,
    kind: "invalid_request",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// The session is gone or expired. A caller must open a new one. Any write the lost
/// session had open can only be rolled back, never committed — immediately where the
/// session was closed, and at idle expiry where a gateway restart orphaned it on its
/// worker.
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

/// This attempt ID was already accepted. Durable request identity retains its
/// outcome independently of diagnostic history; this ID never executes again.
pub(crate) const DUPLICATE_REQUEST: Failure = Failure {
    status: None,
    kind: "duplicate_request",
    retry_safe: false,
    http: StatusCode::CONFLICT,
};

pub(crate) const REQUEST_ID_MISMATCH: Failure = Failure {
    status: None,
    kind: "request_id_mismatch",
    retry_safe: false,
    http: StatusCode::CONFLICT,
};

pub(crate) const INVALID_RETRY: Failure = Failure {
    status: None,
    kind: "invalid_retry",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};

/// Rejected by a queue or session limit before any SQL ran, so the same request
/// may be sent again once capacity frees up.
pub(crate) const CAPACITY: Failure = Failure {
    status: None,
    kind: "capacity",
    retry_safe: true,
    http: StatusCode::TOO_MANY_REQUESTS,
};

/// Durable admission refusals never authorize replay of session-bound SQL.
pub(crate) const QUEUE_CAPACITY: Failure = Failure {
    status: Some("refused"),
    kind: "queue_capacity",
    retry_safe: false,
    http: StatusCode::TOO_MANY_REQUESTS,
};
pub(crate) const SESSION_BUSY: Failure = Failure {
    status: Some("refused"),
    kind: "session_busy",
    retry_safe: false,
    http: StatusCode::CONFLICT,
};
pub(crate) const QUEUE_TIMEOUT: Failure = Failure {
    status: Some("expired"),
    kind: "queue_timeout",
    retry_safe: false,
    http: StatusCode::REQUEST_TIMEOUT,
};
pub(crate) const QUEUE_DRAINED: Failure = Failure {
    status: Some("refused"),
    kind: "queue_drained",
    retry_safe: false,
    http: StatusCode::SERVICE_UNAVAILABLE,
};
pub(crate) const SESSION_LOST: Failure = Failure {
    status: Some("refused"),
    kind: "session_lost",
    retry_safe: false,
    http: StatusCode::GONE,
};
pub(crate) const POLICY_UNAVAILABLE: Failure = Failure {
    status: Some("refused"),
    kind: "policy_unavailable",
    retry_safe: false,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

/// A recognised compute mode that this deployment cannot run yet. Distinct from
/// `INVALID_REQUEST` on purpose: the request is well formed and will become valid
/// once the mode is implemented, so a client can tell "ask differently" apart from
/// "ask later". Never downgraded to another mode, because silently running a model
/// on shared compute when isolation was requested would break the guarantee the
/// caller asked for.
pub(crate) const COMPUTE_UNAVAILABLE: Failure = Failure {
    status: None,
    kind: "compute_unavailable",
    retry_safe: true,
    http: StatusCode::NOT_IMPLEMENTED,
};

/// The worker has not attached its catalog yet, so the request never reached the
/// engine. Unlike `UNAVAILABLE` there is no partial effect to reason about: nothing
/// ran, which makes resending safe once the worker reports ready.
/// This worker is shutting down and will not take new sessions.
///
/// Distinct from `STARTING`, which means the same thing about readiness and the
/// opposite thing about waiting: a starting worker will serve this caller shortly, a
/// draining one never will. Retry-safe because no session was opened and no statement
/// ran, so the caller may open one elsewhere immediately.
pub(crate) const DRAINING: Failure = Failure {
    status: None,
    kind: "draining",
    retry_safe: true,
    http: StatusCode::SERVICE_UNAVAILABLE,
};
pub(crate) const STARTING: Failure = Failure {
    status: None,
    kind: "starting",
    retry_safe: true,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

/// Discovery refused a new session before forwarding any open. Unlike an
/// unavailable response after forwarding SQL, this has no unknown side effects.
pub(crate) const NO_READY_WORKER: Failure = Failure {
    status: None,
    kind: "no_ready_worker",
    retry_safe: true,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

/// A dependency failed or a worker disappeared mid-request. Whether the work
/// committed is unknown, which is exactly when an automatic replay is unsafe.
pub(crate) const UNAVAILABLE: Failure = Failure {
    status: None,
    kind: "unavailable",
    retry_safe: false,
    http: StatusCode::SERVICE_UNAVAILABLE,
};

/// Control-plane SQL is read-only and never enters a worker or its query ledger.
pub(crate) const SYSTEM_QUERY_UNSUPPORTED: Failure = Failure {
    status: None,
    kind: "system_query_unsupported",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};
pub(crate) const SYSTEM_QUERY_TOO_WIDE: Failure = Failure {
    status: None,
    kind: "system_query_too_wide",
    retry_safe: false,
    http: StatusCode::BAD_REQUEST,
};
pub(crate) const SYSTEM_QUERY_TIMEOUT: Failure = Failure {
    status: None,
    kind: "system_query_timeout",
    retry_safe: true,
    http: StatusCode::REQUEST_TIMEOUT,
};
pub(crate) const SYSTEM_QUERY_UNAVAILABLE: Failure = Failure {
    status: None,
    kind: "system_query_unavailable",
    retry_safe: true,
    http: StatusCode::SERVICE_UNAVAILABLE,
};
pub(crate) const SYSTEM_QUERY_RESULT: Failure = Failure {
    status: None,
    kind: "system_query_result",
    retry_safe: false,
    http: StatusCode::INTERNAL_SERVER_ERROR,
};
/// A well-formed isolated request that nothing could hold: no node fits it, or the
/// pod never became ready.
///
/// Separate from `UNAVAILABLE` because that is also what an internal failure returns,
/// where a statement may already have run, so it has to stay conservative about
/// retries. Here the failure happened before any session existed, so the request
/// provably left nothing behind — and unlike `COMPUTE_UNAVAILABLE`, which means this
/// deployment cannot isolate at all, capacity may well be different later.
pub(crate) const NOT_PLACEABLE: Failure = Failure {
    status: None,
    kind: "not_placeable",
    retry_safe: true,
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
        Some("budget_exceeded") => BUDGET_EXCEEDED,
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
        BUDGET_EXHAUSTED,
        BUDGET_JOB_COST,
        BUDGET_EXCEEDED,
        INTERRUPTED,
        UNAUTHORIZED,
        INVALID_REQUEST,
        UNKNOWN_SESSION,
        NOT_FOUND,
        DUPLICATE_REQUEST,
        REQUEST_ID_MISMATCH,
        INVALID_RETRY,
        CAPACITY,
        UNAVAILABLE,
        NO_READY_WORKER,
        QUEUE_CAPACITY,
        SESSION_BUSY,
        QUEUE_TIMEOUT,
        QUEUE_DRAINED,
        SESSION_LOST,
        POLICY_UNAVAILABLE,
        SYSTEM_QUERY_UNSUPPORTED,
        SYSTEM_QUERY_TOO_WIDE,
        SYSTEM_QUERY_TIMEOUT,
        SYSTEM_QUERY_UNAVAILABLE,
        SYSTEM_QUERY_RESULT,
    ]
    .into_iter()
    .find(|failure| failure.kind == kind)
}

/// Lifecycle states a history record may hold, for filter validation.
pub(crate) const STATUSES: [&str; 13] = [
    "running",
    "queued",
    "preparing",
    "executing",
    "expired",
    "succeeded",
    "failed",
    "conflict",
    "refused",
    "budget_exceeded",
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
            BUDGET_EXHAUSTED,
            BUDGET_JOB_COST,
            BUDGET_EXCEEDED,
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
        assert_eq!(
            safe,
            ["transaction_conflict", "budget_exhausted", "capacity"]
        );
    }

    #[test]
    fn stored_kinds_round_trip_and_statuses_are_known() {
        for failure in [
            CONFLICT,
            SQL_ERROR,
            TIMEOUT,
            CANCELED,
            INTERRUPTED,
            BUDGET_EXHAUSTED,
            BUDGET_JOB_COST,
            BUDGET_EXCEEDED,
            QUEUE_CAPACITY,
            SESSION_BUSY,
            QUEUE_TIMEOUT,
            QUEUE_DRAINED,
            SESSION_LOST,
            POLICY_UNAVAILABLE,
        ] {
            assert_eq!(by_kind(failure.kind), Some(failure));
            assert!(STATUSES.contains(&failure.status.unwrap()));
        }
        assert_eq!(by_kind("invented_later"), None);
    }
}
