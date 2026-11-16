//! Local, single-tenant DuckFleet SQL service. All DuckDB work stays on one worker thread.
mod compute;
mod failure;
mod gateway;
mod history;
mod metering;
mod pricing;
mod profiling;
mod secrets;
mod usage;
mod worker;
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
    active: Arc<Mutex<Option<worker::Active>>>,
    token: Arc<str>,
    history: history::Reader,
    usage: usage::Reader,
    /// Set once the worker has attached its catalog. The HTTP server is bound before
    /// that happens so a slow catalog shows as not-ready rather than refusing
    /// connections, which a probe cannot tell apart from a crashed process.
    ready: Arc<AtomicBool>,
}

type Reply = Result<Output, ApiError>;
struct Envelope {
    op: Operation,
    reply: oneshot::Sender<Reply>,
}
enum Operation {
    Open(SessionRequest),
    Info,
    Close(Uuid),
    Query(Uuid, QueryRequest),
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
        .is_some_and(|v| v == token.as_ref());
    if !valid {
        return ApiError::new(failure::UNAUTHORIZED, "invalid bearer token").into_response();
    }
    next.run(request).await
}
async fn health(State(app): State<App>) -> Response {
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
    // A rejected send never reached the engine, so this request may be sent again.
    app.sender
        .try_send(Envelope { op, reply })
        .map_err(|_| ApiError::new(failure::CAPACITY, "worker queue full or closed"))?;
    receiver.await.map_err(|_| ApiError::internal())?
}
async fn open(State(app): State<App>, request: Option<Json<SessionRequest>>) -> Reply {
    let request = request.map(|Json(r)| r).unwrap_or_default();
    request.validate()?;
    dispatch(app, Operation::Open(request)).await
}
async fn info(State(app): State<App>) -> Reply {
    dispatch(app, Operation::Info).await
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
    dispatch(app, Operation::Query(id, req)).await
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
    let worker_active = active.clone();
    let worker_ready = ready.clone();
    // Startup waits for the catalog on a bounded budget, so reaching the error arm
    // means waiting will not help. Hand the failure back to `main` so the exit code
    // and the message still reflect it.
    let startup_failure = Arc::new(Mutex::new(None));
    let failure_slot = startup_failure.clone();
    let runtime = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("duckfleet-worker".into())
        .spawn(move || match worker::Worker::new(worker_active, runtime) {
            Ok(mut worker) => {
                worker_ready.store(true, Ordering::Release);
                worker.run(receiver);
            }
            Err(error) => {
                *failure_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                let _ = failed_tx.send(());
            }
        })?;
    let app = App {
        sender,
        active,
        token: token.into(),
        history: history::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: usage::Reader::new(std::env::var("PLATFORM_DSN")?),
        ready,
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
        served = axum::serve(listener, router).with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
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
