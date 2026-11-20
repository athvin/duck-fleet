//! The whole client surface for DuckFleet: a small async HTTP + Arrow transport,
//! with no local DuckDB connection and no SQL retry path. It is the Rust
//! replacement for the former `clients/duckfleet-python` package — the dbt work
//! now runs through the forked dbt-core binary, so this is only what a caller
//! needs to reach the gateway directly from Rust (the `cli` role, and the
//! integration harness).
//!
//! The wire protocol is the gateway's: open a session, run statements against
//! it, read Arrow IPC back, and classify failures by the server's own
//! `{error, failure_kind, retry_safe}` shape. A request that dies in transport
//! is an *unknown* outcome, never permission to replay a write: the gateway
//! records every statement in `query_history` before executing it, so a
//! `transport_failure` can be resolved by reading it back by `request_id`.

use std::time::{Duration, Instant};

use arrow_ipc::reader::StreamReader;
use reqwest::{Client as HttpClient, Method, StatusCode};
use serde_json::{Value, json};

/// Longer than the server's maximum statement deadline, so a statement never
/// times out in transport before the gateway returns its classified verdict.
pub const STATEMENT_TIMEOUT: Duration = Duration::from_secs(75);

/// Covers the server's own ready budget with margin: opening an isolated session
/// can hold the request while a node is provisioned, and the client must wait for
/// the gateway's classified verdict rather than time out in transport first — a
/// transport timeout is unknown-outcome and would retry into a second pod (#90).
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(450);

/// Refusals a new session may simply wait out: a rollout drains the old gateway
/// (`draining`), replaces it (transport failures, or an unclassified 5xx from a
/// Service with no ready endpoint), and a worker may still be attaching its
/// catalog (`starting`). Opening a session runs no SQL, so waiting costs nothing
/// but time. Statements are never retried by this client under any
/// classification.
const OPEN_WAITABLE_KINDS: [&str; 3] = ["draining", "starting", "transport_failure"];

/// A failure the server classified — or one the transport could not deliver.
///
/// `retry_safe` repeats the server's judgement and is true only where the
/// platform proved the request left no partial effect. Acting on it is the
/// caller's choice: this transport never replays a statement by itself.
#[derive(Debug, Clone)]
pub struct RemoteError {
    pub status: Option<u16>,
    pub failure_kind: Option<String>,
    pub retry_safe: bool,
    pub message: String,
}

impl RemoteError {
    /// A request that dies in transport (reset, refused, timed out): whether it
    /// ran is unknown to the client, but not unknowable — `query_history` records
    /// it by `request_id`. Never retry-safe.
    fn transport(error: reqwest::Error) -> Self {
        RemoteError {
            status: None,
            failure_kind: Some("transport_failure".to_owned()),
            retry_safe: false,
            message: format!(
                "the request failed in transport before any response arrived, so whether it ran \
                 is unknown; check /v1/queries for its request_id before resending a write ({error})"
            ),
        }
    }

    fn decode(message: impl Into<String>) -> Self {
        RemoteError {
            status: None,
            failure_kind: Some("invalid_response".to_owned()),
            retry_safe: false,
            message: message.into(),
        }
    }

    fn is_kind(&self, kind: &str) -> bool {
        self.failure_kind.as_deref() == Some(kind)
    }
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RemoteError {}

/// A raw transport to one gateway, carrying the bearer token on every request.
#[derive(Clone)]
pub struct Client {
    endpoint: String,
    token: String,
    http: HttpClient,
}

impl Client {
    /// Builds the client. The process-wide rustls crypto provider must already be
    /// installed (it is, in `main` before any role runs); without it reqwest
    /// panics on build.
    pub fn new(endpoint: &str, token: &str) -> Result<Self, RemoteError> {
        let http = HttpClient::builder()
            .build()
            .map_err(RemoteError::transport)?;
        Ok(Client {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
            http,
        })
    }

    /// One request/response. Non-2xx is parsed into the server's classified
    /// `{error, failure_kind, retry_safe}` shape; a dropped connection becomes a
    /// `transport_failure`.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        payload: Option<&Value>,
        timeout: Duration,
    ) -> Result<Vec<u8>, RemoteError> {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.endpoint, path))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .timeout(timeout);
        if let Some(payload) = payload {
            request = request.body(serde_json::to_vec(payload).map_err(|e| {
                RemoteError::decode(format!("could not serialize request body: {e}"))
            })?);
        }
        let response = request.send().await.map_err(RemoteError::transport)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(RemoteError::transport)?;
        if status.is_success() {
            return Ok(bytes.to_vec());
        }
        Err(Self::classify(status, &bytes))
    }

    fn classify(status: StatusCode, bytes: &[u8]) -> RemoteError {
        let mut error = RemoteError {
            status: Some(status.as_u16()),
            failure_kind: None,
            retry_safe: false,
            message: String::from_utf8_lossy(bytes).into_owned(),
        };
        if let Ok(payload) = serde_json::from_slice::<Value>(bytes) {
            if let Some(message) = payload.get("error").and_then(Value::as_str) {
                error.message = message.to_owned();
            }
            error.failure_kind = payload
                .get("failure_kind")
                .and_then(Value::as_str)
                .map(str::to_owned);
            error.retry_safe = payload
                .get("retry_safe")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        }
        error
    }
}

/// One row set, column-oriented metadata plus rows of stringified cells (`None`
/// is SQL null). Stringifying keeps this transport free of per-type handling the
/// gateway already did; callers that need a typed value parse the column type.
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl QueryResult {
    fn from_arrow(bytes: &[u8]) -> Result<Self, RemoteError> {
        let reader = StreamReader::try_new(bytes, None)
            .map_err(|e| RemoteError::decode(format!("could not read the Arrow stream: {e}")))?;
        let schema = reader.schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let column_types: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch
                .map_err(|e| RemoteError::decode(format!("could not read a record batch: {e}")))?;
            let formatters: Vec<_> = batch
                .columns()
                .iter()
                .map(|array| {
                    arrow_cast::display::ArrayFormatter::try_new(
                        array.as_ref(),
                        &arrow_cast::display::FormatOptions::default(),
                    )
                })
                .collect::<Result<_, _>>()
                .map_err(|e| RemoteError::decode(format!("could not format a column: {e}")))?;
            for row in 0..batch.num_rows() {
                let mut cells = Vec::with_capacity(batch.num_columns());
                for (column, formatter) in batch.columns().iter().zip(&formatters) {
                    if column.is_null(row) {
                        cells.push(None);
                    } else {
                        cells.push(Some(formatter.value(row).to_string()));
                    }
                }
                rows.push(cells);
            }
        }
        Ok(QueryResult {
            columns,
            column_types,
            rows,
        })
    }
}

/// A session on one gateway: open it, run statements against it, close it.
/// Closing rolls back anything uncommitted, which is the server's behaviour and
/// the reason a failed load leaves no half-written table behind.
pub struct RemoteConnection {
    client: Client,
    context: Value,
    pub session_id: String,
    pub placement: Value,
    closed: bool,
}

impl RemoteConnection {
    /// Opens a session. `compute` is `None` (or an empty map) for the shared
    /// persistent tier; naming any of cpu/memory/disk asks for a dedicated pod of
    /// that size — the gateway derives the mode from presence, so there is no mode
    /// flag. The open is retried only while a refusal is one a new session may
    /// wait out, bounded by `open_retry_seconds` wall-clock.
    pub async fn open(
        endpoint: &str,
        token: &str,
        name: &str,
        compute: Option<Value>,
        context: Option<Value>,
        open_retry_seconds: u64,
    ) -> Result<Self, RemoteError> {
        let client = Client::new(endpoint, token)?;
        let mut context = match context {
            Some(Value::Object(map)) => Value::Object(map),
            _ => json!({}),
        };
        context["connection_name"] = json!(name);
        let compute = compute.unwrap_or_else(|| json!({}));
        let body = json!({ "compute": compute, "context": context });

        let deadline = Instant::now() + Duration::from_secs(open_retry_seconds);
        let info: Value = loop {
            match client
                .request(Method::POST, "/v1/sessions", Some(&body), OPEN_TIMEOUT)
                .await
            {
                Ok(bytes) => {
                    break serde_json::from_slice(&bytes).map_err(|e| {
                        RemoteError::decode(format!("could not parse the open response: {e}"))
                    })?;
                }
                Err(error) => {
                    let waitable = error
                        .failure_kind
                        .as_deref()
                        .map(|kind| OPEN_WAITABLE_KINDS.contains(&kind))
                        .unwrap_or(false)
                        || (error.failure_kind.is_none()
                            && matches!(error.status, Some(502..=504)));
                    if !waitable || Instant::now() >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        let session_id = info
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| RemoteError::decode("the open response carried no session_id"))?
            .to_owned();
        let placement = info.get("placement").cloned().unwrap_or(Value::Null);
        Ok(RemoteConnection {
            client,
            context,
            session_id,
            placement,
            closed: false,
        })
    }

    /// The session's path, `/v1/sessions/<id>` — the base for session-scoped raw
    /// requests the harness makes directly through [`Self::client`].
    pub fn path(&self) -> String {
        format!("/v1/sessions/{}", self.session_id)
    }

    /// The underlying transport, for callers (the harness) that issue raw requests
    /// against arbitrary gateway paths rather than only this session's statements.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Set a top-level field on the context sent with every statement, so a caller
    /// can tag its work (e.g. a workload marker) and find it again in history.
    pub fn set_context_field(&mut self, key: &str, value: Value) {
        if let Value::Object(map) = &mut self.context {
            map.insert(key.to_owned(), value);
        }
    }

    /// Like [`Self::execute`], but returns the `request_id` the statement ran
    /// under, so the caller can read it back from `/v1/queries/<id>`.
    pub async fn execute_tracked(
        &self,
        sql: &str,
        parameters: Vec<Value>,
    ) -> Result<(String, QueryResult), RemoteError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = json!({
            "request_id": request_id,
            "sql": sql,
            "parameters": parameters,
            "context": self.context,
        });
        let bytes = self
            .session_request(Method::POST, "/queries", Some(&body))
            .await?;
        Ok((request_id, QueryResult::from_arrow(&bytes)?))
    }

    /// A session-scoped request, with the one session-scoped failure explained:
    /// `unknown_session` is what a gateway restart or idle expiry looks like.
    async fn session_request(
        &self,
        method: Method,
        suffix: &str,
        payload: Option<&Value>,
    ) -> Result<Vec<u8>, RemoteError> {
        let path = format!("{}{}", self.path(), suffix);
        match self
            .client
            .request(method, &path, payload, STATEMENT_TIMEOUT)
            .await
        {
            Err(error) if error.is_kind("unknown_session") => Err(RemoteError {
                message: format!(
                    "{error} — this session no longer exists (the gateway restarted, or it sat \
                     idle past expiry); uncommitted work from it can only be rolled back, never \
                     committed. Open a new connection, and check /v1/queries by request_id before \
                     resending any write"
                ),
                ..error
            }),
            other => other,
        }
    }

    /// Runs one statement and returns its rows. `parameters` are always sent as
    /// bound values, never pasted into the SQL.
    pub async fn execute(
        &self,
        sql: &str,
        parameters: Vec<Value>,
    ) -> Result<QueryResult, RemoteError> {
        let body = json!({
            "request_id": uuid::Uuid::new_v4().to_string(),
            "sql": sql,
            "parameters": parameters,
            "context": self.context,
        });
        let bytes = self
            .session_request(Method::POST, "/queries", Some(&body))
            .await?;
        QueryResult::from_arrow(&bytes)
    }

    pub async fn commit(&self) -> Result<(), RemoteError> {
        self.execute("COMMIT", Vec::new()).await.map(|_| ())
    }

    pub async fn rollback(&self) -> Result<(), RemoteError> {
        self.execute("ROLLBACK", Vec::new()).await.map(|_| ())
    }

    pub async fn cancel(&self) -> Result<Value, RemoteError> {
        let bytes = self.session_request(Method::POST, "/cancel", None).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| RemoteError::decode(format!("could not parse the cancel response: {e}")))
    }

    /// Closes the session. A session already gone is what closing wanted; any
    /// other failure still raises, because the session has not been released.
    pub async fn close(&mut self) -> Result<(), RemoteError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        match self
            .client
            .request(Method::DELETE, &self.path(), None, STATEMENT_TIMEOUT)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.is_kind("unknown_session") => Ok(()),
            Err(error) => Err(error),
        }
    }
}
