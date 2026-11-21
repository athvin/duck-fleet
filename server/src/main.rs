//! Local, single-tenant DuckFleet SQL service. All DuckDB work stays on one worker thread.
#[allow(dead_code)]
mod cli;
#[allow(dead_code)]
mod client;
mod compute;
mod credentials;
mod failure;
mod gateway;
mod harness;
mod history;
mod isolated;
mod kube;
mod metering;
mod pgwire;
mod pricing;
mod profiling;
mod secrets;
mod usage;
mod worker;
mod worker_status;
use compute::SessionRequest;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone)]
struct App {
    sender: mpsc::Sender<Envelope>,
    status: worker_status::Status,
    active: Arc<Mutex<Option<worker::Active>>>,
    token: Arc<str>,
    history: history::Reader,
    usage: usage::Reader,
    /// Set once the worker has attached its catalog. The HTTP server is bound before
    /// that happens so a slow catalog shows as not-ready rather than refusing
    /// connections, which a probe cannot tell apart from a crashed process.
    ready: Arc<AtomicBool>,
    /// Set when a termination signal arrives. Kept apart from `ready` because the two
    /// answer different questions: `ready` is what a probe reads, and this is what
    /// decides whether new work is admitted. A draining worker reports not-ready *and*
    /// refuses new sessions, while a starting one refuses everything and will later
    /// accept it.
    draining: Arc<AtomicBool>,
}

type Reply = Result<Output, ApiError>;
struct Envelope {
    op: Operation,
    reply: oneshot::Sender<Reply>,
    query_guard: Option<worker_status::QueryGuard>,
}
enum Operation {
    /// Request identity travels on the authenticated internal hop, independently
    /// of the caller's SQL context and the credential that opened a session.
    Open(SessionRequest, credentials::Identity),
    Close(Uuid),
    Query(Uuid, QueryRequest, credentials::Identity),
}
enum Output {
    Json(Value),
    Arrow(Vec<u8>, Uuid),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    request_id: Uuid,
    sql: String,
    #[serde(default)]
    parameters: Vec<Value>,
    #[serde(default = "default_context")]
    context: Value,
    #[serde(default = "default_timeout")]
    timeout_seconds: u64,
}
fn default_context() -> Value {
    json!({})
}
fn default_timeout() -> u64 {
    30
}

/// Resolves when the process is asked to stop.
///
/// SIGTERM is what Kubernetes sends; the previous shutdown waited on `ctrl_c` alone,
/// which is SIGINT, so on a pod deletion the graceful path never ran at all and the
/// worker kept admitting new sessions until it was killed. SIGINT is still handled
/// because that is what an operator running this locally sends.
/// How long a drain may take. Longer than the 60 seconds a statement may run, and
/// comfortably inside the 90-second `terminationGracePeriodSeconds` the chart sets, so
/// the process leaves of its own accord rather than being killed partway.
const DRAIN: std::time::Duration = std::time::Duration::from_secs(75);
/// How long the engine must stay idle before the drain is considered finished.
const SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

async fn terminating() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                // Refusing to serve over this would be worse than losing the graceful
                // path, so it is reported and the process carries on with SIGINT alone.
                tracing::error!(%error, "cannot listen for SIGTERM; drains will not be graceful");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Waits until `busy` has answered no for a sustained `SETTLE`, or `DRAIN` has passed.
///
/// On a worker, busy means a statement is executing; the settle window keeps a
/// session's next statement from being cut off in the gap between two of them. On the
/// gateway, busy means a request is in flight, and the same window covers a client
/// sending its next request on the session it already holds.
async fn drain_until_idle(busy: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + DRAIN;
    let mut idle_since = None;
    while tokio::time::Instant::now() < deadline {
        match (busy(), idle_since) {
            (true, _) => idle_since = None,
            (false, None) => idle_since = Some(tokio::time::Instant::now()),
            (false, Some(since)) if since.elapsed() >= SETTLE => break,
            (false, Some(_)) => {}
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Debug exposes nothing the client is not already sent: the message is the same
/// string `IntoResponse` returns, and it is scrubbed of credentials before it gets
/// here. It exists so assertions can report which failure was produced.
#[derive(Debug)]
struct ApiError(pub(crate) failure::Failure, pub(crate) String);
impl ApiError {
    fn new(failure: failure::Failure, message: impl Into<String>) -> Self {
        Self(failure, message.into())
    }
    fn bad(message: impl Into<String>) -> Self {
        Self(failure::INVALID_REQUEST, message.into())
    }
    fn internal() -> Self {
        Self(
            failure::UNAVAILABLE,
            "worker or execution history unavailable; do not automatically retry writes".into(),
        )
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // `retry_safe` is the machine-readable half of the message: true only where
        // the platform can prove the request left no partial effect behind.
        (
            self.0.http,
            Json(json!({
                "error": self.1,
                "failure_kind": self.0.kind,
                "retry_safe": self.0.retry_safe,
            })),
        )
            .into_response()
    }
}
impl IntoResponse for Output {
    fn into_response(self) -> Response {
        match self {
            Self::Json(value) => Json(value).into_response(),
            Self::Arrow(bytes, id) => (
                [
                    (
                        header::CONTENT_TYPE,
                        "application/vnd.apache.arrow.stream".to_owned(),
                    ),
                    (
                        header::HeaderName::from_static("x-query-id"),
                        id.to_string(),
                    ),
                ],
                Bytes::from(bytes),
            )
                .into_response(),
        }
    }
}

async fn authorize(
    State(token): State<Arc<str>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let valid = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|v| crate::credentials::tokens_match(v, &token));
    if !valid {
        return ApiError::new(failure::UNAUTHORIZED, "invalid bearer token").into_response();
    }
    next.run(request).await
}
async fn health(State(app): State<App>) -> Response {
    // Draining is reported before readiness, because a draining worker is also ready
    // and answering "ok" would keep it in the Service's endpoints while it shuts down.
    // Failing here is what takes it out of rotation.
    if app.draining.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "draining"})),
        )
            .into_response();
    }
    if app.ready.load(Ordering::Acquire) {
        Json(json!({"status": "ok"})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "starting"})),
        )
            .into_response()
    }
}
async fn dispatch(app: App, op: Operation) -> Reply {
    // The worker does not read its queue until the catalog is attached, so a request
    // accepted now would wait for a reply that cannot arrive. The queue has spare
    // capacity, so `try_send` below would accept it rather than reject it.
    if !app.ready.load(Ordering::Acquire) {
        return Err(ApiError::new(
            failure::STARTING,
            "worker is still attaching its catalog; no statement was executed",
        ));
    }
    let (reply, receiver) = oneshot::channel();
    let query_guard = matches!(&op, Operation::Query(..)).then(|| app.status.enqueue());
    // A rejected send never reached the engine, so this request may be sent again.
    app.sender
        .try_send(Envelope {
            op,
            reply,
            query_guard,
        })
        .map_err(|_| ApiError::new(failure::CAPACITY, "worker queue full or closed"))?;
    receiver.await.map_err(|_| ApiError::internal())?
}
async fn open(State(app): State<App>, headers: HeaderMap, body: Bytes) -> Reply {
    // Only new sessions are refused. Queries on sessions already open continue, which
    // is the drain: the grace period bounds how long that can take, so a client cannot
    // hold this worker open indefinitely by continuing to send work.
    if app.draining.load(Ordering::Acquire) {
        return Err(ApiError::new(
            failure::DRAINING,
            "this worker is shutting down and is not accepting new sessions; \
             no statement was executed",
        ));
    }
    // Empty body => default (persistent); malformed body => the gateway's own
    // classified error, not axum's raw 422.
    let request: SessionRequest = if body.is_empty() {
        SessionRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            ApiError::new(
                failure::INVALID_REQUEST,
                format!("invalid session request: {e}"),
            )
        })?
    };
    request.validate()?;
    let identity = worker_identity(&headers)?;
    dispatch(app, Operation::Open(request, identity)).await
}

/// Only the shared internal token can reach this handler. A named identity must
/// carry an issued credential ID, which the worker rechecks after dequeue; an
/// incomplete named identity must never silently become operator authority.
fn worker_identity(headers: &HeaderMap) -> Result<credentials::Identity, ApiError> {
    let denied = || {
        ApiError::new(
            failure::UNAUTHORIZED,
            "invalid internal credential identity",
        )
    };
    let principal = headers
        .get(credentials::PRINCIPAL_HEADER)
        .map(|value| value.to_str().map_err(|_| denied()))
        .transpose()?;
    let credential_id = headers
        .get(credentials::CREDENTIAL_HEADER)
        .map(|value| value.to_str().map_err(|_| denied()))
        .transpose()?;
    match (principal, credential_id) {
        (None, None) | (Some("direct"), None) => Ok(credentials::Identity {
            principal: "direct".into(),
            credential_id: None,
            expires_at: None,
        }),
        (Some(credentials::BOOTSTRAP_PRINCIPAL), None) => Ok(credentials::Identity::bootstrap()),
        (Some(principal), Some(id))
            if credentials::valid_principal(principal) && Uuid::parse_str(id).is_ok() =>
        {
            Ok(credentials::Identity {
                principal: principal.into(),
                credential_id: Some(id.into()),
                // The database supplies authoritative expiry at admission.
                expires_at: None,
            })
        }
        _ => Err(denied()),
    }
}
async fn info(State(app): State<App>) -> Reply {
    Ok(Output::Json(app.status.snapshot()))
}
async fn close(State(app): State<App>, Path(id): Path<Uuid>) -> Reply {
    dispatch(app, Operation::Close(id)).await
}
async fn history(State(app): State<App>, Query(filter): Query<history::Filter>) -> Reply {
    app.history.list(filter).await.map(Output::Json)
}
async fn query_detail(State(app): State<App>, Path(id): Path<Uuid>) -> Reply {
    app.history.detail(id).await.map(Output::Json)
}
async fn query(
    State(app): State<App>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Reply {
    if req.sql.trim().is_empty()
        || req.sql.len() > 262_144
        || !(1..=60).contains(&req.timeout_seconds)
    {
        return Err(ApiError::bad(
            "SQL must contain 1–262144 bytes; timeout must be 1–60 seconds",
        ));
    }
    if !req.context.is_object()
        || req.parameters.len() > 10_000
        || req.context.to_string().len() > 8192
    {
        return Err(ApiError::bad(
            "too many parameters or context exceeds 8192 bytes",
        ));
    }
    let identity = worker_identity(&headers)?;
    dispatch(app, Operation::Query(id, req, identity)).await
}
async fn usage_events(State(app): State<App>, Query(filter): Query<usage::Filter>) -> Reply {
    app.usage.events(filter).await.map(Output::Json)
}
async fn budget(State(app): State<App>) -> Reply {
    app.usage.budget().await.map(Output::Json)
}
async fn set_budget(State(app): State<App>, Json(update): Json<usage::BudgetUpdate>) -> Reply {
    app.usage.set_budget(update).await.map(Output::Json)
}
async fn cancel(State(app): State<App>, Path(id): Path<Uuid>, _: HeaderMap) -> Json<Value> {
    let canceled = worker::interrupt(&app.active, Some(id), None, "canceled");
    Json(json!({"interrupted_active_query": canceled}))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // Process-wide and before any HTTP client is built. reqwest is compiled without a
    // default crypto provider so the Kubernetes CA can be the only trusted root, and
    // in that configuration *every* client panics on build until one is installed —
    // including the gateway's plain HTTP client to the workers, which uses no TLS at
    // all. Installing it next to the one caller that needs TLS crashed on boot.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider was already installed"))?;
    // The `cli` role talks to a remote gateway and carries its own --token/
    // DUCKFLEET_TOKEN, so it runs before the server's own DUCKFLEET_TOKEN
    // requirement below (which a shell invocation of the CLI would not satisfy).
    if std::env::var("DUCKFLEET_ROLE").as_deref() == Ok("cli") {
        let args: Vec<String> = std::env::args().skip(1).collect();
        std::process::exit(cli::run(args).await);
    }
    // The integration harness, like the CLI, is a client of a remote gateway and
    // carries its own DUCKFLEET_URL/DUCKFLEET_TOKEN, so it runs before the server's
    // own token requirement below.
    if std::env::var("DUCKFLEET_ROLE").as_deref() == Ok("harness") {
        let args: Vec<String> = std::env::args().skip(1).collect();
        std::process::exit(harness::run(args).await);
    }
    let token = std::env::var("DUCKFLEET_TOKEN")?;
    anyhow::ensure!(
        token.len() >= 16,
        "DUCKFLEET_TOKEN must have at least 16 characters"
    );
    if std::env::var("DUCKFLEET_ROLE").as_deref() == Ok("gateway") {
        return gateway::serve(token).await;
    }
    let active = Arc::new(Mutex::new(None));
    let (sender, receiver) = mpsc::channel(64);
    let (failed_tx, failed_rx) = oneshot::channel();
    let ready = Arc::new(AtomicBool::new(false));
    let draining = Arc::new(AtomicBool::new(false));
    let status = worker_status::Status::new(
        std::env::var("DUCKFLEET_WORKER_ID").unwrap_or_default(),
        ready.clone(),
        draining.clone(),
    );
    let worker_active = active.clone();
    let status_for_drain = status.clone();
    let worker_ready = ready.clone();
    let worker_status = status.clone();
    // Startup waits for the catalog on a bounded budget, so reaching the error arm
    // means waiting will not help. Hand the failure back to `main` so the exit code
    // and the message still reflect it.
    let startup_failure = Arc::new(Mutex::new(None));
    let failure_slot = startup_failure.clone();
    let runtime = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("duckfleet-worker".into())
        .spawn(move || {
            match worker::Worker::new(
                worker_active,
                runtime,
                worker_ready.clone(),
                worker_status.clone(),
            ) {
                Ok(mut worker) => {
                    worker_status.initialize(worker.placement().clone());
                    worker_ready.store(true, Ordering::Release);
                    worker.run(receiver);
                }
                Err(error) => {
                    *failure_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                    let _ = failed_tx.send(());
                }
            }
        })?;
    let app = App {
        sender,
        status,
        active,
        token: token.into(),
        history: history::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: usage::Reader::new(
            std::env::var("PLATFORM_DSN")?,
            pricing::RateCard::from_env()?,
        ),
        ready,
        draining: draining.clone(),
    };
    let api = Router::new()
        .route("/v1/sessions", post(open))
        .route("/v1/sessions/{id}", axum::routing::delete(close))
        .route("/v1/sessions/{id}/queries", post(query))
        .route("/v1/sessions/{id}/cancel", post(cancel))
        .route("/v1/queries", get(history))
        .route("/v1/queries/{id}", get(query_detail))
        .route("/v1/usage", get(usage_events))
        .route("/v1/budget", get(budget).put(set_budget))
        .route("/v1/worker", get(info))
        .route_layer(middleware::from_fn_with_state(app.token.clone(), authorize));
    let router = Router::new()
        .route("/healthz", get(health))
        .merge(api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!(
        port = 8080,
        "DuckFleet local server listening; worker starting"
    );
    tokio::select! {
        served = axum::serve(listener, router).with_graceful_shutdown(async move {
            terminating().await;
            // Marked first, and the server deliberately keeps serving afterwards.
            //
            // Handing the signal straight to `with_graceful_shutdown` closes the
            // listener at once, which makes a drain invisible: a readiness probe gets
            // a refused connection instead of an answer, and the classified refusal
            // below can never be returned. The first version of this did exactly that,
            // and the harness caught `/healthz` being unreachable rather than saying
            // "draining". So the shutdown waits here while the pod is still answering.
            draining.store(true, Ordering::Release);
            tracing::info!(
                "termination signal received; refusing new sessions and finishing open work"
            );
            // Include queued SQL and admission/settlement, which remain work even
            // before or after the DuckDB interrupt slot is occupied.
            drain_until_idle(move || status_for_drain.has_queries())
            .await;
            tracing::info!("drain complete; shutting down");
        }) => served?,
        // Also reached if the worker thread dies after becoming ready, which drops the
        // sender. Serving on without an engine is worse than exiting and being replaced.
        _ = failed_rx => {
            return Err(startup_failure
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .unwrap_or_else(|| anyhow::anyhow!("worker thread stopped unexpectedly")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn internal_identity_requires_a_complete_named_credential() {
        let id = Uuid::new_v4().to_string();
        for (principal, credential) in [
            (Some("analyst"), None),
            (None, Some(id.as_str())),
            (Some("analyst"), Some("not-a-uuid")),
            (Some(""), None),
            (Some("bootstrap-admin"), Some(id.as_str())),
        ] {
            let mut headers = HeaderMap::new();
            if let Some(value) = principal {
                headers.insert(credentials::PRINCIPAL_HEADER, value.parse().unwrap());
            }
            if let Some(value) = credential {
                headers.insert(credentials::CREDENTIAL_HEADER, value.parse().unwrap());
            }
            assert!(
                worker_identity(&headers).is_err(),
                "{principal:?} {credential:?}"
            );
        }
        assert_eq!(
            worker_identity(&HeaderMap::new()).unwrap().principal,
            "direct"
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            credentials::PRINCIPAL_HEADER,
            "bootstrap-admin".parse().unwrap(),
        );
        assert!(worker_identity(&headers).unwrap().is_bootstrap());
        headers.insert(credentials::PRINCIPAL_HEADER, "analyst".parse().unwrap());
        headers.insert(credentials::CREDENTIAL_HEADER, id.parse().unwrap());
        let identity = worker_identity(&headers).unwrap();
        assert_eq!(identity.principal, "analyst");
        assert_eq!(identity.credential_id.as_deref(), Some(id.as_str()));
        assert!(identity.expires_at.is_none());
    }

    fn status_test_app() -> (App, mpsc::Receiver<Envelope>) {
        let (sender, receiver) = mpsc::channel(1);
        let ready = Arc::new(AtomicBool::new(true));
        let draining = Arc::new(AtomicBool::new(false));
        let status = worker_status::Status::new("small".into(), ready.clone(), draining.clone());
        status.initialize(json!({"worker_id":"small", "cpu":1}));
        (
            App {
                sender,
                status,
                active: Arc::new(Mutex::new(None)),
                token: "test-token".into(),
                history: history::Reader::new("postgres://unused".into()),
                usage: usage::Reader::new("postgres://unused".into(), pricing::dev()),
                ready,
                draining,
            },
            receiver,
        )
    }

    fn status_test_query() -> Operation {
        Operation::Query(
            Uuid::new_v4(),
            QueryRequest {
                request_id: Uuid::new_v4(),
                sql: "select 1".into(),
                parameters: vec![],
                context: json!({}),
                timeout_seconds: 30,
            },
            credentials::Identity::bootstrap(),
        )
    }

    #[tokio::test]
    async fn status_bypasses_a_full_queue_and_disconnect_preserves_work() {
        let (app, mut receiver) = status_test_app();
        let request = tokio::spawn(dispatch(app.clone(), status_test_query()));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while app.status.snapshot()["queued_queries"] != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // No worker is reading the channel: this is the full/blocked queue that
        // formerly prevented the information handler from replying.
        let Output::Json(snapshot) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            info(State(app.clone())),
        )
        .await
        .unwrap()
        .unwrap() else {
            panic!("expected JSON")
        };
        assert_eq!(snapshot["queued_queries"], 1);
        assert!(matches!(dispatch(app.clone(), status_test_query()).await,
            Err(ApiError(failure, _)) if failure.kind == "capacity"));
        assert_eq!(app.status.snapshot()["queued_queries"], 1);
        request.abort();
        assert!(matches!(request.await, Err(error) if error.is_cancelled()));
        assert_eq!(app.status.snapshot()["queued_queries"], 1);
        let envelope = receiver.recv().await.unwrap();
        let active = envelope.query_guard.unwrap().start();
        assert_eq!(app.status.snapshot()["queued_queries"], 0);
        assert_eq!(app.status.snapshot()["active_queries"], 1);
        assert!(envelope.reply.send(Ok(Output::Json(json!({})))).is_err());
        assert_eq!(app.status.snapshot()["active_queries"], 1);
        drop(active);
        assert!(!app.status.has_queries());
    }

    #[tokio::test]
    async fn a_lost_worker_queue_releases_admitted_and_refused_queries() {
        let (app, receiver) = status_test_app();
        let request = tokio::spawn(dispatch(app.clone(), status_test_query()));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while app.status.snapshot()["queued_queries"] != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(receiver);
        assert!(request.await.unwrap().is_err());
        assert!(!app.status.has_queries());
        assert!(dispatch(app.clone(), status_test_query()).await.is_err());
        assert!(!app.status.has_queries());
        app.ready.store(false, Ordering::Release);
        assert!(matches!(dispatch(app.clone(), status_test_query()).await,
            Err(ApiError(failure, _)) if failure.kind == "starting"));
        assert!(!app.status.has_queries());
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_drain_ends_at_the_settle_window_not_the_deadline() {
        let started = tokio::time::Instant::now();
        drain_until_idle(|| false).await;
        let elapsed = started.elapsed();
        assert!(elapsed >= SETTLE, "settled early: {elapsed:?}");
        assert!(
            elapsed < DRAIN,
            "an idle drain waited for the deadline: {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_drain_that_never_goes_idle_leaves_at_the_deadline() {
        let started = tokio::time::Instant::now();
        drain_until_idle(|| true).await;
        assert!(started.elapsed() >= DRAIN, "left before the deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn work_finishing_mid_drain_still_gets_its_settle_window() {
        // Busy for the first 50 polls — ten virtual seconds — then idle.
        let polls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        drain_until_idle(|| polls.fetch_add(1, Ordering::Relaxed) < 50).await;
        let elapsed = started.elapsed();
        let busy = std::time::Duration::from_secs(10);
        assert!(
            elapsed >= busy + SETTLE,
            "left before the work settled: {elapsed:?}"
        );
        assert!(
            elapsed < DRAIN,
            "waited for the deadline after going idle: {elapsed:?}"
        );
    }
}
