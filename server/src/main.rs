//! Local, single-tenant DuckFleet SQL service. All DuckDB work stays on one worker thread.
mod compute;
mod failure;
mod gateway;
mod history;
mod metering;
mod pricing;
mod profiling;
mod usage;
mod worker;
use compute::SessionRequest;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone)]
struct App {
    sender: mpsc::Sender<Envelope>,
    active: Arc<Mutex<Option<worker::Active>>>,
    token: Arc<str>,
    history: history::Reader,
    usage: usage::Reader,
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
async fn dispatch(app: App, op: Operation) -> Reply {
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
    let (ready_tx, ready_rx) = oneshot::channel();
    let worker_active = active.clone();
    let runtime = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("duckfleet-worker".into())
        .spawn(move || match worker::Worker::new(worker_active, runtime) {
            Ok(mut worker) => {
                let _ = ready_tx.send(Ok(()));
                worker.run(receiver);
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        })?;
    ready_rx.await??;
    let app = App {
        sender,
        active,
        token: token.into(),
        history: history::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: usage::Reader::new(std::env::var("PLATFORM_DSN")?),
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
        .route("/healthz", get(|| async { Json(json!({"status":"ok"})) }))
        .merge(api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!(port = 8080, "DuckFleet local server ready");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
