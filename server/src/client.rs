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
//! retains each request identity before executing it, so a
//! `transport_failure` can be resolved by reading it back by `request_id`.

use std::time::{Duration, Instant};

use arrow_ipc::reader::StreamReader;
use reqwest::{Client as HttpClient, Method, StatusCode};
use serde_json::{Value, json};

/// Transport allowance for the default queue and execution deadlines. An explicit
/// longer queue may outlive this timeout; accepted work continues independently.
pub const STATEMENT_TIMEOUT: Duration = Duration::from_secs(75);

/// Covers the server's own ready budget with margin: opening an isolated session
/// can hold the request while a node is provisioned, and the client must wait for
/// the gateway's classified verdict rather than time out in transport first — a
/// transport timeout is unknown-outcome and would retry into a second pod (#90).
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(450);

/// Refusals a new session may simply wait out: a rollout drains the old gateway
/// (`draining`), replaces it (transport failures, or an unclassified 5xx from a
/// Service with no ready endpoint), and a worker may still be attaching its
/// catalog (`starting`), or discovery has no eligible worker yet
/// (`no_ready_worker`). Opening a session runs no SQL, so waiting costs nothing
/// but time. Statements are never retried by this client under any
/// classification.
const OPEN_WAITABLE_KINDS: [&str; 4] = [
    "draining",
    "starting",
    "no_ready_worker",
    "transport_failure",
];

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
    pub request_id: Option<Box<str>>,
    pub acceptance: Option<Box<str>>,
    pub status_url: Option<Box<str>>,
}

impl RemoteError {
    /// A request that dies in transport (reset, refused, timed out): whether it
    /// ran is unknown to the client; durable request identity records
    /// it by `request_id`. Never retry-safe.
    fn transport(error: reqwest::Error) -> Self {
        RemoteError {
            request_id: None,
            acceptance: None,
            status_url: None,
            status: None,
            failure_kind: Some("transport_failure".to_owned()),
            retry_safe: false,
            message: format!(
                "the request failed in transport before any response arrived, so whether it ran \
                 is unknown; check /v1/requests/<request_id> before resending a write ({error})"
            ),
        }
    }

    fn decode(message: impl Into<String>) -> Self {
        RemoteError {
            request_id: None,
            acceptance: None,
            status_url: None,
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
    /// Queries only the bounded metadata SQL service, without opening compute.
    /// BIGINT/DECIMAL cells remain JSON strings, preserving exact usage totals.
    pub async fn system_sql(
        &self,
        sql: &str,
        parameters: &[Value],
        bounds: Option<&Value>,
    ) -> Result<Value, RemoteError> {
        let bytes = self.request(Method::POST,"/v1/system/query",Some(&json!({"sql":sql,"parameters":parameters,"bounds":bounds,"contract_version":1,"format":"json"})),Duration::from_secs(5)).await.map_err(|mut error| {
            if error.status.is_none() && error.failure_kind.as_deref() == Some("transport_failure") {
                error.message = "system metadata request failed in transport; no warehouse SQL was submitted".into();
                error.retry_safe = true;
            }
            error
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|_| RemoteError::decode("invalid system query response"))
    }

    /// A standalone job has no client-owned engine session. The original ID
    /// survives ambiguous transport/commit outcomes; this method never resubmits.
    pub(crate) async fn submit_job(
        &self,
        request: &crate::job_policy::JobRequest,
    ) -> Result<Value, RemoteError> {
        let id = request.request_id;
        request.clone().validate().map_err(|error| {
            Self::job_error(
                id,
                RemoteError {
                    status: None,
                    failure_kind: Some(error.0.kind.into()),
                    retry_safe: error.0.retry_safe,
                    message: error.1,
                    request_id: None,
                    acceptance: Some("not_accepted".into()),
                    status_url: None,
                },
            )
        })?;
        let value = serde_json::to_value(request).map_err(|_| {
            Self::job_error(id, RemoteError::decode("could not encode job request"))
        })?;
        self.job_json(Method::POST, "/v1/jobs", Some(&value), id)
            .await
    }

    pub(crate) async fn job(&self, id: uuid::Uuid) -> Result<Value, RemoteError> {
        self.job_json(Method::GET, &format!("/v1/jobs/{id}"), None, id)
            .await
    }

    pub(crate) async fn cancel_job(&self, id: uuid::Uuid) -> Result<Value, RemoteError> {
        self.job_json(Method::POST, &format!("/v1/jobs/{id}/cancel"), None, id)
            .await
    }

    pub(crate) async fn job_result(&self, id: uuid::Uuid) -> Result<Value, RemoteError> {
        self.job_json(Method::GET, &format!("/v1/jobs/{id}/result"), None, id)
            .await
    }

    fn job_error(id: uuid::Uuid, mut error: RemoteError) -> RemoteError {
        error
            .request_id
            .get_or_insert_with(|| id.to_string().into());
        error.acceptance.get_or_insert_with(|| "unknown".into());
        error
            .status_url
            .get_or_insert_with(|| format!("/v1/jobs/{id}").into());
        error
    }

    async fn job_json(
        &self,
        method: Method,
        path: &str,
        payload: Option<&Value>,
        id: uuid::Uuid,
    ) -> Result<Value, RemoteError> {
        let submitting = method == Method::POST && path == "/v1/jobs";
        let reading = method == Method::GET;
        let mut request = self
            .http
            .request(method, format!("{}{}", self.endpoint, path))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(15));
        if let Some(payload) = payload {
            request = request.json(payload);
        }
        let result = async {
            let response = request.send().await.map_err(RemoteError::transport)?;
            let status = response.status();
            let body = Self::bounded_body(response, crate::artifacts::MAX_MANIFEST_BYTES).await?;
            if !status.is_success() {
                return Err(Self::classify(status, &body));
            }
            serde_json::from_slice(&body).map_err(|_| RemoteError::decode("invalid job response"))
        }
        .await;
        result.map_err(|mut error| {
            if !submitting && error.is_kind("transport_failure") {
                error.message = if reading {
                    "job retrieval failed in transport; retrieve the same job ID again; no SQL was submitted".into()
                } else {
                    "job cancellation response was lost; inspect the same job ID to see whether cancellation took effect; no SQL was submitted".into()
                };
                error.retry_safe = reading;
            }
            Self::job_error(id, error)
        })
    }

    async fn bounded_body(
        mut response: reqwest::Response,
        maximum: usize,
    ) -> Result<Vec<u8>, RemoteError> {
        if response
            .content_length()
            .is_some_and(|length| length > maximum as u64)
        {
            return Err(RemoteError::decode("job response exceeds its size limit"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(RemoteError::transport)? {
            if chunk.len() > maximum.saturating_sub(body.len()) {
                return Err(RemoteError::decode("job response exceeds its size limit"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Writes one immutable Arrow chunk incrementally. Only generated relative
    /// routes are used; manifest URLs can never redirect the bearer credential.
    pub(crate) async fn download_job_chunk<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        id: uuid::Uuid,
        chunk_id: &str,
        expected_bytes: u64,
        expected_sha256: &str,
        output: &mut W,
    ) -> Result<(), RemoteError> {
        use tokio::io::AsyncWriteExt;
        let result = async {
            if chunk_id.is_empty()
                || chunk_id.len() > 128
                || !chunk_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                || expected_bytes == 0
                || expected_bytes > 16 * 1024 * 1024
                || expected_sha256.len() != 64
                || !expected_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(RemoteError::decode("invalid job chunk descriptor"));
            }
            let mut response = self
                .http
                .get(format!(
                    "{}/v1/jobs/{id}/result/chunks/{chunk_id}",
                    self.endpoint
                ))
                .bearer_auth(&self.token)
                .timeout(STATEMENT_TIMEOUT)
                .send()
                .await
                .map_err(RemoteError::transport)?;
            let status = response.status();
            if !status.is_success() {
                let bytes = Self::bounded_body(response, 65536).await?;
                return Err(Self::classify(status, &bytes));
            }
            if response
                .content_length()
                .is_some_and(|n| n != expected_bytes)
            {
                return Err(RemoteError::decode(
                    "job chunk length differs from manifest",
                ));
            }
            let mut received = 0u64;
            let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
            while let Some(chunk) = response.chunk().await.map_err(RemoteError::transport)? {
                received = received
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| RemoteError::decode("job chunk length overflow"))?;
                if received > expected_bytes {
                    return Err(RemoteError::decode("job chunk exceeds manifest length"));
                }
                digest.update(&chunk);
                output
                    .write_all(&chunk)
                    .await
                    .map_err(|_| RemoteError::decode("cannot write job chunk output"))?;
            }
            let actual = digest
                .finish()
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            if received != expected_bytes || !actual.eq_ignore_ascii_case(expected_sha256) {
                return Err(RemoteError::decode(
                    "job chunk checksum or length differs from manifest",
                ));
            }
            output
                .flush()
                .await
                .map_err(|_| RemoteError::decode("cannot flush job chunk output"))?;
            Ok(())
        }
        .await;
        result.map_err(|mut error| {
            if error.status.is_none() && error.is_kind("transport_failure") {
                error.message =
                    "result download failed in transport; SQL was not submitted or retried".into();
            }
            Self::job_error(id, error)
        })
    }

    /// Builds the client. The process-wide rustls crypto provider must already be
    /// installed (it is, in `main` before any role runs); without it reqwest
    /// panics on build.
    pub fn new(endpoint: &str, token: &str) -> Result<Self, RemoteError> {
        let http = HttpClient::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
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
            request_id: None,
            acceptance: None,
            status_url: None,
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
            error.request_id = payload["request_id"].as_str().map(Into::into);
            error.acceptance = payload["acceptance"].as_str().map(Into::into);
            error.status_url = payload["status_url"].as_str().map(Into::into);
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
    /// Opens a session. `compute` is `None` (or an empty map) for the default
    /// persistent pool. Resource fields alone request a dedicated pod; explicit
    /// `mode` and `pool` select persistent resources or isolated compute. Selection
    /// is fixed for the session. The open is retried only while a refusal is one a new session may
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
        self.execute_with_queue_timeout(sql, parameters, crate::query::DEFAULT_QUEUE_TIMEOUT)
            .await
    }

    /// A queue allowance is independent of the SQL execution deadline. Transport
    /// loss retains the identity for status/cancellation; it never retries SQL.
    pub async fn execute_with_queue_timeout(
        &self,
        sql: &str,
        parameters: Vec<Value>,
        queue_timeout_seconds: u64,
    ) -> Result<(String, QueryResult), RemoteError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = json!({
            "request_id": request_id, "sql": sql, "parameters": parameters,
            "context": self.context, "queue_timeout_seconds": queue_timeout_seconds,
        });
        let result = self
            .session_request(Method::POST, "/queries", Some(&body))
            .await
            .and_then(|bytes| QueryResult::from_arrow(&bytes));
        result
            .map(|rows| (request_id.clone(), rows))
            .map_err(|mut error| {
                error
                    .request_id
                    .get_or_insert_with(|| request_id.clone().into());
                error
                    .status_url
                    .get_or_insert_with(|| format!("/v1/requests/{request_id}").into());
                error.acceptance.get_or_insert_with(|| "unknown".into());
                error
            })
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
                     committed. Open a new connection, and check /v1/requests/<request_id> before \
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
        self.execute_tracked(sql, parameters)
            .await
            .map(|(_, rows)| rows)
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
        match self
            .client
            .request(Method::DELETE, &self.path(), None, STATEMENT_TIMEOUT)
            .await
        {
            Ok(bytes) => {
                let confirmed = serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .is_some_and(|value| value["closed"] == true);
                if !confirmed {
                    return Err(RemoteError::decode("session closure is not confirmed"));
                }
                self.closed = true;
                Ok(())
            }
            Err(error) if error.is_kind("unknown_session") => {
                self.closed = true;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn job_submit_never_follows_redirects_and_preserves_original_identity() {
        use axum::{Json, Router, extract::State, routing::post};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let _ = rustls::crypto::ring::default_provider().install_default();
        let calls = Arc::new(AtomicUsize::new(0));
        let id = uuid::Uuid::new_v4();
        let app=Router::new().route("/v1/jobs",post(move |State(calls):State<Arc<AtomicUsize>>,Json(body):Json<Value>| async move {
            assert_eq!(body["request_id"],id.to_string());
            calls.fetch_add(1,Ordering::SeqCst);
            (StatusCode::TEMPORARY_REDIRECT,[("location","/replayed")],"")
        })).route("/replayed",post(|State(calls):State<Arc<AtomicUsize>>| async move {
            calls.fetch_add(100,Ordering::SeqCst); StatusCode::ACCEPTED
        })).with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let request =
            serde_json::from_value(json!({"request_id":id,"sql":"INSERT INTO t VALUES(1)"}))
                .unwrap();
        let error = Client::new(&endpoint, "test")
            .unwrap()
            .submit_job(&request)
            .await
            .unwrap_err();
        assert_eq!(error.status, Some(307));
        assert_eq!(error.request_id.as_deref(), Some(id.to_string().as_str()));
        assert_eq!(error.acceptance.as_deref(), Some("unknown"));
        assert!(!error.retry_safe);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        task.abort();
    }

    #[tokio::test]
    async fn job_download_checks_manifest_length_and_checksum_before_success() {
        use axum::{Router, routing::get};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let data = b"one bounded immutable chunk";
        let app = Router::new().route(
            "/v1/jobs/{id}/result/chunks/{chunk}",
            get(|| async { "one bounded immutable chunk" }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&endpoint, "test").unwrap();
        let id = uuid::Uuid::new_v4();
        let digest = ring::digest::digest(&ring::digest::SHA256, data)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let mut output = Vec::new();
        client
            .download_job_chunk(id, "0", data.len() as u64, &digest, &mut output)
            .await
            .unwrap();
        assert_eq!(output, data);
        output.clear();
        assert!(
            client
                .download_job_chunk(id, "0", data.len() as u64 - 1, &digest, &mut output)
                .await
                .is_err()
        );
        assert!(output.is_empty());
        assert!(
            client
                .download_job_chunk(id, "0", data.len() as u64, &"0".repeat(64), &mut output)
                .await
                .is_err()
        );
        output.clear();
        assert!(
            client
                .download_job_chunk(
                    id,
                    "../credentials",
                    data.len() as u64,
                    &digest,
                    &mut output
                )
                .await
                .is_err()
        );
        assert!(output.is_empty());
        task.abort();
    }

    #[test]
    fn classified_errors_preserve_identity_and_unknown_acceptance() {
        let id = uuid::Uuid::new_v4().to_string();
        let path = format!("/v1/requests/{id}");
        let error = Client::classify(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::to_vec(
                &json!({"error":"transport lost","failure_kind":"unavailable",
                "retry_safe":false,"request_id":id,"acceptance":"unknown","status_url":path}),
            )
            .unwrap(),
        );
        assert_eq!(error.request_id.as_deref(), Some(id.as_str()));
        assert_eq!(error.status_url.as_deref(), Some(path.as_str()));
        assert_eq!(error.acceptance.as_deref(), Some("unknown"));
        assert!(!error.retry_safe);
    }
}
