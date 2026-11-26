#![doc = include_str!("../README.md")]

pub mod client;
pub mod protocol;
mod results;

pub use client::{Client, QueryResult, RemoteConnection, RemoteError};
pub use protocol::{ComputeRequest, JobRequest, JobResultRequest, Manifest, Mode, ResultFormat};
pub use reqwest::Method;

/// The SDK's error type: the server's classified failure, or a transport or
/// decode failure on the client side.
pub type Error = RemoteError;

/// An explicit DuckFleet session. Temporary tables, transactions, and the
/// selected compute live as long as the session; closing rolls back anything
/// uncommitted.
pub type Session = RemoteConnection;

use serde_json::{Value, json};
use std::time::Duration;

/// Stable classification of an [`Error`]. New server failure kinds map to
/// [`ErrorKind::Other`] until the SDK names them, so matching stays exhaustive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// DuckDB rejected the SQL (parse, binder, constraint, or runtime error).
    Sql,
    /// The statement exceeded its execution timeout.
    Timeout,
    /// The request waited longer than its queue timeout before compute was granted.
    QueueTimeout,
    /// The request was canceled.
    Canceled,
    /// DuckLake rejected a commit because a concurrent writer changed the same data.
    Conflict,
    /// The request ID was already used; the SQL was not replayed.
    DuplicateRequest,
    /// A usage budget refused the request before execution.
    BudgetRefused,
    /// The session's engine state is gone (worker restart, gateway restart, or expiry).
    SessionLost,
    /// The session already has outstanding work.
    SessionBusy,
    /// The connection failed after the request may have been sent: the outcome is unknown.
    TransportUncertain,
    /// Missing or invalid credentials.
    Unauthorized,
    /// The principal lacks the needed authority.
    Forbidden,
    /// The object does not exist or is not visible to this principal.
    NotFound,
    /// The request was malformed or outside supported bounds.
    InvalidRequest,
    /// Capacity, draining, or startup: the platform could not take the work now.
    Unavailable,
    /// A durable result is expired, pending, or failed publication.
    ResultUnavailable,
    /// The client could not decode or verify a response.
    Decode,
    /// Any other classified failure; inspect [`Error::failure_kind`].
    Other,
}

impl RemoteError {
    /// The stable classification of this failure.
    pub fn kind(&self) -> ErrorKind {
        match self.failure_kind.as_deref() {
            Some("sql_error") => ErrorKind::Sql,
            Some("timeout") => ErrorKind::Timeout,
            Some("queue_timeout") => ErrorKind::QueueTimeout,
            Some("canceled") => ErrorKind::Canceled,
            Some("transaction_conflict") => ErrorKind::Conflict,
            Some("duplicate_request" | "request_id_mismatch") => ErrorKind::DuplicateRequest,
            Some("budget_exceeded" | "budget_exhausted" | "budget_job_cost") => {
                ErrorKind::BudgetRefused
            }
            Some("session_lost" | "unknown_session" | "interrupted") => ErrorKind::SessionLost,
            Some("session_busy") => ErrorKind::SessionBusy,
            Some("transport_failure") => ErrorKind::TransportUncertain,
            Some("unauthorized") => ErrorKind::Unauthorized,
            Some("forbidden") => ErrorKind::Forbidden,
            Some("not_found") => ErrorKind::NotFound,
            Some("invalid_request" | "invalid_retry" | "unsupported_parameter") => {
                ErrorKind::InvalidRequest
            }
            Some(
                "capacity"
                | "compute_unavailable"
                | "draining"
                | "no_ready_worker"
                | "not_placeable"
                | "queue_capacity"
                | "queue_drained"
                | "starting"
                | "unavailable"
                | "jobs_unavailable"
                | "policy_unavailable",
            ) => ErrorKind::Unavailable,
            Some(
                "result_expired"
                | "result_pending"
                | "result_unavailable"
                | "result_fetch_unavailable",
            ) => ErrorKind::ResultUnavailable,
            Some("invalid_response" | "job_client_error") => ErrorKind::Decode,
            None if self.status == Some(401) => ErrorKind::Unauthorized,
            None if self.status == Some(403) => ErrorKind::Forbidden,
            None if self.status == Some(404) => ErrorKind::NotFound,
            _ => ErrorKind::Other,
        }
    }

    /// The server's `failure_kind`, verbatim.
    pub fn failure_kind(&self) -> Option<&str> {
        self.failure_kind.as_deref()
    }

    /// True only where the platform proved the request left no partial effect.
    /// The SDK never acts on it; resending is always the caller's decision.
    pub fn retry_safe(&self) -> bool {
        self.retry_safe
    }

    /// The request identity the failed work ran (or may have run) under.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// `accepted`, `not_accepted`, or `unknown`: whether the server took the work.
    pub fn acceptance(&self) -> Option<&str> {
        self.acceptance.as_deref()
    }

    /// The status route for reconciling an uncertain outcome.
    pub fn status_url(&self) -> Option<&str> {
        self.status_url.as_deref()
    }

    /// The HTTP status, when the server answered.
    pub fn http_status(&self) -> Option<u16> {
        self.status
    }
}

/// A typed bound parameter. Parameters are always sent separately from the SQL
/// text and bound by the engine. The wire carries JSON scalars: integers bind
/// as `BIGINT` (or `UBIGINT` above `i64::MAX`), floats as `DOUBLE`, and text
/// as `VARCHAR`. Bind decimals, dates, timestamps, and other types as text and
/// cast them in SQL, e.g. `?::DECIMAL(18,4)` or `?::TIMESTAMP`.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
    Text(String),
}

impl Param {
    /// The JSON wire value. Non-finite doubles have no exact representation
    /// and are refused before anything is sent.
    pub fn to_wire(&self) -> Result<Value, Error> {
        Ok(match self {
            Param::Null => Value::Null,
            Param::Bool(value) => json!(value),
            Param::Int(value) => json!(value),
            Param::UInt(value) => json!(value),
            Param::Double(value) => serde_json::Number::from_f64(*value)
                .map(Value::Number)
                .ok_or_else(|| {
                    local_error(
                        "unsupported_parameter",
                        "NaN and infinite doubles cannot be bound; bind them as text and cast in SQL",
                    )
                })?,
            Param::Text(value) => json!(value),
        })
    }
}

macro_rules! param_from {
    ($variant:ident, $target:ty, $($source:ty),+) => {
        $(impl From<$source> for Param {
            fn from(value: $source) -> Self {
                Param::$variant(<$target>::from(value))
            }
        })+
    };
}
param_from!(Int, i64, i8, i16, i32, i64, u8, u16, u32);
param_from!(UInt, u64, u64);
param_from!(Double, f64, f32, f64);
param_from!(Bool, bool, bool);
param_from!(Text, String, String, &str);

impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(value: Option<T>) -> Self {
        value.map_or(Param::Null, Into::into)
    }
}

/// How to open a session: a name for query history, optional explicit compute,
/// optional context fields, and how long to wait out transient open refusals
/// (draining, starting, no ready worker). Opening runs no SQL, so waiting is safe.
#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub name: String,
    pub compute: Option<ComputeRequest>,
    pub context: Option<Value>,
    pub open_wait: Duration,
}

impl SessionOptions {
    pub fn named(name: impl Into<String>) -> Self {
        SessionOptions {
            name: name.into(),
            compute: None,
            context: None,
            open_wait: Duration::from_secs(90),
        }
    }

    pub fn compute(mut self, compute: ComputeRequest) -> Self {
        self.compute = Some(compute);
        self
    }

    pub fn context(mut self, context: Value) -> Self {
        self.context = Some(context);
        self
    }

    pub fn open_wait(mut self, wait: Duration) -> Self {
        self.open_wait = wait;
        self
    }
}

/// Timeouts and identity for one statement. A caller-generated request ID is
/// the handle for reconciling an uncertain outcome.
#[derive(Clone, Debug)]
pub struct Statement {
    pub request_id: uuid::Uuid,
    pub timeout: Duration,
    pub queue_timeout: Duration,
}

impl Default for Statement {
    fn default() -> Self {
        Statement {
            request_id: uuid::Uuid::new_v4(),
            timeout: Duration::from_secs(30),
            queue_timeout: Duration::from_secs(protocol::DEFAULT_QUEUE_TIMEOUT),
        }
    }
}

impl Client {
    /// Opens an explicit session.
    pub async fn session(&self, options: SessionOptions) -> Result<Session, Error> {
        let compute = options
            .compute
            .map(|compute| {
                compute
                    .validate()
                    .map_err(|(_, message)| local_error("invalid_request", message))?;
                serde_json::to_value(compute)
                    .map_err(|_| local_error("invalid_request", "cannot encode compute request"))
            })
            .transpose()?;
        RemoteConnection::open_with(
            self.clone(),
            &options.name,
            compute,
            options.context,
            options.open_wait.as_secs(),
        )
        .await
    }

    /// Waits for a durable job to become terminal, polling its status every
    /// `poll`. The returned status document carries `status`, `failure_kind`,
    /// and result metadata. Waiting never resubmits or cancels the job; a lost
    /// status response is retry-safe because it is a read.
    pub async fn wait_job(
        &self,
        id: uuid::Uuid,
        poll: Duration,
        deadline: Duration,
    ) -> Result<Value, Error> {
        let started = std::time::Instant::now();
        loop {
            let status = self.job(id).await?;
            if status["state"] == "terminal" {
                return Ok(status);
            }
            if started.elapsed() >= deadline {
                return Err(RemoteError {
                    status: None,
                    failure_kind: Some("wait_deadline".into()),
                    retry_safe: true,
                    message: "the job is still running; wait again with the same job ID".into(),
                    request_id: Some(id.to_string().into()),
                    acceptance: Some("accepted".into()),
                    status_url: Some(format!("/v1/jobs/{id}").into()),
                });
            }
            tokio::time::sleep(poll).await;
        }
    }
}

impl RemoteConnection {
    /// Runs one statement with typed parameters and default timeouts.
    pub async fn query<I, P>(&self, sql: &str, parameters: I) -> Result<QueryResult, Error>
    where
        I: IntoIterator<Item = P>,
        P: Into<Param>,
    {
        self.query_with(sql, parameters, Statement::default()).await
    }

    /// Runs one statement with typed parameters, explicit timeouts, and a
    /// caller-chosen request ID.
    pub async fn query_with<I, P>(
        &self,
        sql: &str,
        parameters: I,
        statement: Statement,
    ) -> Result<QueryResult, Error>
    where
        I: IntoIterator<Item = P>,
        P: Into<Param>,
    {
        let parameters = parameters
            .into_iter()
            .map(|parameter| parameter.into().to_wire())
            .collect::<Result<Vec<_>, _>>()?;
        self.execute_with_timeouts_and_id(
            sql,
            parameters,
            statement.timeout.as_secs().max(1),
            statement.queue_timeout.as_secs().max(1),
            statement.request_id.to_string(),
        )
        .await
        .map(|(_, result)| result)
    }
}

fn local_error(kind: &str, message: impl Into<String>) -> Error {
    RemoteError {
        status: None,
        failure_kind: Some(kind.into()),
        retry_safe: true,
        message: message.into(),
        request_id: None,
        acceptance: Some("not_accepted".into()),
        status_url: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_are_typed_scalars_and_non_finite_doubles_are_refused() {
        assert_eq!(Param::from(7_i32).to_wire().unwrap(), json!(7));
        assert_eq!(Param::from(u64::MAX).to_wire().unwrap(), json!(u64::MAX));
        assert_eq!(Param::from(0.1_f64).to_wire().unwrap(), json!(0.1));
        assert_eq!(Param::from("x").to_wire().unwrap(), json!("x"));
        assert_eq!(Param::from(None::<i64>).to_wire().unwrap(), Value::Null);
        assert_eq!(Param::from(true).to_wire().unwrap(), json!(true));
        let error = Param::from(f64::NAN).to_wire().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidRequest);
        assert_eq!(error.acceptance(), Some("not_accepted"));
    }

    #[test]
    fn classified_failures_map_to_stable_kinds() {
        let error = |kind: Option<&str>, status: Option<u16>| RemoteError {
            status,
            failure_kind: kind.map(str::to_owned),
            retry_safe: false,
            message: String::new(),
            request_id: None,
            acceptance: None,
            status_url: None,
        };
        for (kind, expected) in [
            ("sql_error", ErrorKind::Sql),
            ("timeout", ErrorKind::Timeout),
            ("canceled", ErrorKind::Canceled),
            ("transaction_conflict", ErrorKind::Conflict),
            ("duplicate_request", ErrorKind::DuplicateRequest),
            ("budget_exhausted", ErrorKind::BudgetRefused),
            ("unknown_session", ErrorKind::SessionLost),
            ("session_busy", ErrorKind::SessionBusy),
            ("transport_failure", ErrorKind::TransportUncertain),
            ("result_expired", ErrorKind::ResultUnavailable),
            ("something_new", ErrorKind::Other),
        ] {
            assert_eq!(error(Some(kind), Some(400)).kind(), expected, "{kind}");
        }
        assert_eq!(error(None, Some(401)).kind(), ErrorKind::Unauthorized);
        assert_eq!(error(None, Some(500)).kind(), ErrorKind::Other);
    }
}
