//! Local, single-tenant DuckFleet SQL service. All DuckDB work stays on one worker thread.
mod advise_cli;
mod advise_dbt;
mod artifacts;
#[allow(dead_code)]
mod audit;
mod authz;
mod autoscaling;
mod catalog;
mod cli;
mod cli_config;
#[allow(dead_code)]
mod client;
mod compute;
mod compute_lifecycle;
mod credentials;
mod db_roles;
mod dev;
mod engine_security;
mod engine_stream;
mod export_cli;
mod failure;
mod gateway;
mod harness;
mod history;
mod history_cli;
mod http_transport;
mod import_cli;
mod imports;
mod isolated;
mod job_artifacts;
mod job_policy;
mod jobs;
mod kube;
mod metadata;
mod metering;
mod ownership;
mod pgwire;
mod platform;
mod pod_credentials;
mod pools;
mod pricing;
mod profiling;
mod query;
mod query_shape;
mod queue;
mod queue_service;
mod requests;
mod secrets;
mod shell;
mod snapshot_cli;
mod sql_limits;
mod system_admission;
mod system_query;
mod system_result;
mod system_sql;
mod telemetry;
#[path = "ui_unavailable.rs"]
mod ui;
mod usage;
mod worker;
mod worker_ownership;
mod worker_status;
use compute::WorkerSessionRequest;
pub(crate) use query::QueryRequest;
use query::{Acceptance, Protocol};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
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
    queue: Arc<queue_service::Service>,
    status: worker_status::Status,
    token: Arc<str>,
    history: history::Reader,
    requests: requests::Reader,
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
}
enum Operation {
    OpenJob(jobs::BindingToken),
    /// Request identity travels on the authenticated internal hop, independently
    /// of the caller's SQL context and the credential that opened a session.
    Open(
        Box<WorkerSessionRequest>,
        credentials::Identity,
        Protocol,
        Uuid,
        authz::TrustedOperation,
    ),
}
enum Output {
    Json(Value),
    JsonResponse(StatusCode, Value),
    Arrow(Vec<u8>, Uuid),
}

pub(crate) fn configured_limit(
    name: &str,
    default: usize,
    maximum: usize,
) -> anyhow::Result<usize> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse::<usize>().ok(),
        Err(std::env::VarError::NotPresent) => Some(default),
        Err(_) => None,
    };
    value
        .filter(|value| (1..=maximum).contains(value))
        .ok_or_else(|| anyhow::anyhow!("{name} must be a whole number from 1 to {maximum}"))
}

/// Resolves when the process is asked to stop.
///
/// SIGTERM is what Kubernetes sends; the previous shutdown waited on `ctrl_c` alone,
/// which is SIGINT, so on a pod deletion the graceful path never ran at all and the
/// worker kept admitting new sessions until it was killed. SIGINT is still handled
/// because that is what an operator running this locally sends.
/// Bounded shutdown drain covering the longest admitted statement plus cleanup,
/// inside the chart's termination grace. Idle drains still finish after SETTLE.
const DRAIN: std::time::Duration =
    std::time::Duration::from_secs(crate::query::MAX_EXECUTION_TIMEOUT + 15);
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
            Self::JsonResponse(status, value) => (status, Json(value)).into_response(),
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
    // A rejected send never reached the engine, so this request may be sent again.
    app.sender
        .try_send(Envelope { op, reply })
        .map_err(|_| ApiError::new(failure::CAPACITY, "worker queue full or closed"))?;
    receiver.await.map_err(|_| ApiError::internal())?
}
async fn open_job(State(app): State<App>, body: Bytes) -> Reply {
    if app.draining.load(Ordering::Acquire) {
        return Err(ApiError::new(failure::DRAINING, "worker is draining"));
    }
    let token: jobs::BindingToken =
        serde_json::from_slice(&body).map_err(|_| ApiError::bad("invalid internal job binding"))?;
    dispatch(app, Operation::OpenJob(token)).await
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
    let request: WorkerSessionRequest = if body.is_empty() {
        WorkerSessionRequest::Direct(compute::SessionRequest::default())
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            ApiError::new(
                failure::INVALID_REQUEST,
                format!("invalid session request: {e}"),
            )
        })?
    };
    let identity = worker_identity(&headers)?;
    dispatch(
        app,
        Operation::Open(
            Box::new(request),
            identity,
            worker_protocol(&headers)?,
            open_span(&headers)?,
            worker_operation(&headers)?,
        ),
    )
    .await
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
    let kind = headers
        .get(credentials::ACTOR_KIND_HEADER)
        .map(|value| value.to_str().map_err(|_| denied()))
        .transpose()?;
    match (kind, principal, credential_id) {
        (None, None, None) => Ok(credentials::Identity::internal("direct")),
        (Some("internal"), Some("direct"), None) => Ok(credentials::Identity::internal("direct")),
        (Some("bootstrap"), Some(credentials::BOOTSTRAP_PRINCIPAL), None) => {
            Ok(credentials::Identity::bootstrap())
        }
        (Some("named"), Some(principal), Some(id))
            if credentials::valid_principal(principal) && Uuid::parse_str(id).is_ok() =>
        {
            Ok(credentials::Identity {
                kind: credentials::ActorKind::Named,
                principal: principal.into(),
                credential_id: Some(id.into()),
                expires_at: None,
            })
        }
        _ => Err(denied()),
    }
}
async fn info(State(app): State<App>) -> Reply {
    Ok(Output::Json(app.status.snapshot()))
}
async fn drain(State(app): State<App>) -> Reply {
    app.draining.store(true, Ordering::Release);
    app.queue.stop();
    Ok(Output::Json(app.status.snapshot()))
}
async fn close(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> Reply {
    let reason = match headers
        .get("x-duckfleet-close-reason")
        .and_then(|v| v.to_str().ok())
    {
        None => queue_service::CloseRequest::Explicit,
        Some("idle") => queue_service::CloseRequest::Idle,
        Some("credential") => {
            let credential_id = headers
                .get("x-duckfleet-close-credential-id")
                .map(|v| v.to_str().map(str::to_owned))
                .transpose()
                .map_err(|_| ApiError::bad("invalid close credential"))?;
            if credential_id
                .as_deref()
                .is_some_and(|id| Uuid::parse_str(id).is_err())
            {
                return Err(ApiError::bad("invalid close credential"));
            }
            let generation = headers
                .get("x-duckfleet-close-authority-generation")
                .map(|v| {
                    v.to_str()
                        .ok()
                        .and_then(|v| v.parse::<i64>().ok())
                        .filter(|v| *v >= 0)
                        .ok_or_else(|| ApiError::bad("invalid close authority generation"))
                })
                .transpose()?;
            queue_service::CloseRequest::Credential {
                credential_id,
                generation,
            }
        }
        Some(_) => return Err(ApiError::bad("invalid close reason")),
    };
    let actor = worker_identity(&headers)?;
    if !matches!(reason, queue_service::CloseRequest::Explicit)
        && actor.kind == credentials::ActorKind::Named
    {
        return Err(ApiError::new(
            failure::FORBIDDEN,
            "conditional cleanup requires internal authority",
        ));
    }
    app.queue.close(id, reason, actor).await
}
fn open_span(headers: &HeaderMap) -> Result<Uuid, ApiError> {
    headers.get("x-duckfleet-open-span-id").map_or_else(
        || Ok(Uuid::new_v4()),
        |value| {
            value
                .to_str()
                .ok()
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| ApiError::bad("invalid internal open span ID"))
        },
    )
}
fn worker_operation(headers: &HeaderMap) -> Result<authz::TrustedOperation, ApiError> {
    let operation = headers
        .get(authz::OPERATION_HEADER)
        .map(|v| {
            v.to_str()
                .ok()
                .and_then(|s| serde_json::from_str(s).ok())
                .ok_or_else(|| ApiError::bad("invalid trusted operation"))
        })
        .transpose()?
        .unwrap_or_default();
    authz::TrustedOperation::validate(&operation)?;
    Ok(operation)
}
fn worker_protocol(headers: &HeaderMap) -> Result<Protocol, ApiError> {
    match headers.get("x-duckfleet-protocol") {
        None => Ok(Protocol::Direct),
        Some(value) => match value.to_str().ok() {
            Some("http") => Ok(Protocol::Http),
            Some("pgwire") => Ok(Protocol::Pgwire),
            Some("platform") => Ok(Protocol::Platform),
            Some("direct") => Ok(Protocol::Direct),
            _ => Err(ApiError::bad("invalid internal protocol")),
        },
    }
}
async fn history(
    State(app): State<App>,
    Query(filter): Query<history::Filter>,
    headers: HeaderMap,
) -> Reply {
    app.history
        .list(worker_identity(&headers)?, filter)
        .await
        .map(Output::Json)
}
async fn query_detail(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> Reply {
    app.history
        .detail(worker_identity(&headers)?, id)
        .await
        .map(Output::Json)
}
async fn request_detail(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> Reply {
    let identity = worker_identity(&headers)?;
    app.requests.detail(identity, id).await.map(Output::Json)
}
async fn query(
    State(app): State<App>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Response {
    let request_id = req.request_id;
    let (acceptance, reply) = match worker_identity(&headers).and_then(|identity| {
        worker_operation(&headers)?;
        worker_protocol(&headers).map(|protocol| (identity, protocol))
    }) {
        Ok((identity, protocol)) => app.queue.submit(id, req, identity, protocol).await,
        Err(error) => (Acceptance::NotAccepted, Err(error)),
    };
    query_response(request_id, acceptance, reply)
}
fn query_response(id: Uuid, acceptance: Acceptance, reply: Reply) -> Response {
    let mut response = match reply {
        Err(error) => (
            error.0.http,
            Json(json!({"error":error.1, "failure_kind":error.0.kind,
            "retry_safe":error.0.retry_safe, "request_id":id, "acceptance":acceptance,
            "status_url":format!("/v1/requests/{id}")})),
        )
            .into_response(),
        Ok(Output::JsonResponse(status, mut value)) => {
            value["request_id"] = json!(id);
            value["acceptance"] = json!(acceptance);
            value["status_url"] = json!(format!("/v1/requests/{id}"));
            (status, Json(value)).into_response()
        }
        Ok(output) => output.into_response(),
    };
    response
        .headers_mut()
        .insert("x-query-id", id.to_string().parse().expect("UUID header"));
    response.headers_mut().insert(
        "x-query-status-url",
        format!("/v1/requests/{id}")
            .parse()
            .expect("UUID URL header"),
    );
    response.headers_mut().insert(
        "x-query-acceptance",
        acceptance.as_str().parse().expect("acceptance header"),
    );
    response
}
async fn request_cancel(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> Reply {
    app.queue.cancel(id, worker_identity(&headers)?).await
}
async fn usage_events(
    State(app): State<App>,
    Query(filter): Query<usage::Filter>,
    headers: HeaderMap,
) -> Reply {
    app.usage
        .events(worker_identity(&headers)?, filter)
        .await
        .map(Output::Json)
}
async fn budget(State(app): State<App>, headers: HeaderMap) -> Reply {
    app.usage
        .budget(worker_identity(&headers)?)
        .await
        .map(Output::Json)
}
async fn set_budget(
    State(app): State<App>,
    headers: HeaderMap,
    Json(update): Json<usage::BudgetUpdate>,
) -> Reply {
    app.usage
        .set_budget(worker_identity(&headers)?, update)
        .await
        .map(Output::Json)
}
async fn cancel(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> Reply {
    app.queue
        .cancel_session(id, worker_identity(&headers)?)
        .await
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
    if std::env::var("DUCKFLEET_ROLE").as_deref() == Ok("engine-security") {
        let args: Vec<String> = std::env::args().skip(1).collect();
        return engine_security::run(&args).await;
    }
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
    match std::env::var("DUCKFLEET_ROLE").as_deref() {
        Ok("migrate") => return platform::migrate_command().await,
        Ok("platform-maintain") => return platform::maintain_command().await,
        Ok("platform-grants") => return platform::grants_command().await,
        _ => {}
    }
    if std::env::var("DUCKFLEET_ROLE").as_deref() == Ok("gateway") {
        let token = std::env::var("DUCKFLEET_TOKEN")?;
        anyhow::ensure!(
            token.len() >= 16,
            "DUCKFLEET_TOKEN must have at least 16 characters"
        );
        return gateway::serve(token).await;
    }
    let token = std::env::var("DUCKFLEET_INTERNAL_TOKEN")?;
    anyhow::ensure!(
        token.len() >= 16,
        "DUCKFLEET_INTERNAL_TOKEN must have at least 16 characters"
    );
    anyhow::ensure!(
        std::env::var_os("DUCKFLEET_TOKEN").is_none(),
        "ordinary workers must not receive the public bootstrap token"
    );
    let active = Arc::new(Mutex::new(None));
    let queue_capacity = configured_limit("DUCKFLEET_WORKER_QUEUE_CAPACITY", 64, 4096)?;
    let worker_limits = query::WorkerLimits::from_env()?;
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let (failed_tx, failed_rx) = oneshot::channel();
    let ready = Arc::new(AtomicBool::new(false));
    let draining = Arc::new(AtomicBool::new(false));
    let status = worker_status::Status::new(
        std::env::var("DUCKFLEET_WORKER_ID").unwrap_or_default(),
        ready.clone(),
        draining.clone(),
        json!({
            "pool": if std::env::var("DUCKFLEET_ISOLATED").as_deref() == Ok("true") { None }
                else { Some(std::env::var("DUCKFLEET_POOL").unwrap_or_else(|_| "small".into())) },
            "max_sessions": configured_limit("DUCKFLEET_WORKER_MAX_SESSIONS", 32, 1024)?,
            "queue_capacity": queue_capacity,
            "max_queued_queries": worker_limits.max_queued_queries,
            "max_queued_bytes": worker_limits.max_queued_bytes,
        }),
    );
    let queue = queue_service::Service::new(
        std::env::var("PLATFORM_DSN")?,
        queue::WorkerEpoch {
            worker_id: std::env::var("DUCKFLEET_WORKER_ID")?,
            boot_id: status.boot_id(),
        },
        ready.clone(),
        draining.clone(),
        active.clone(),
        status.clone(),
    )?;
    let worker_queue = queue.clone();
    let queue_for_drain = queue.clone();
    let drain_active = active.clone();
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
                worker_queue,
                worker_limits,
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
        queue,
        status,
        token: token.into(),
        history: history::Reader::new(std::env::var("PLATFORM_DSN")?),
        requests: requests::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: usage::Reader::new(
            std::env::var("PLATFORM_DSN")?,
            pricing::RateCard::from_env()?,
        ),
        ready,
        draining: draining.clone(),
    };
    let api = Router::new()
        .route("/v1/internal/jobs/open", post(open_job))
        .route("/v1/sessions", post(open))
        .route("/v1/sessions/{id}", axum::routing::delete(close))
        .route("/v1/sessions/{id}/queries", post(query))
        .route("/v1/sessions/{id}/cancel", post(cancel))
        .route("/v1/queries", get(history))
        .route("/v1/queries/{id}", get(query_detail))
        .route("/v1/requests/{id}", get(request_detail))
        .route("/v1/requests/{id}/cancel", post(request_cancel))
        .route("/v1/usage", get(usage_events))
        .route("/v1/budget", get(budget).put(set_budget))
        .route("/v1/worker", get(info))
        .route_layer(middleware::from_fn_with_state(app.token.clone(), authorize));
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/v1/drain", post(drain))
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
            {
                let _active = drain_active.lock().unwrap_or_else(|e| e.into_inner());
                draining.store(true, Ordering::Release);
            }
            tracing::info!(
                "termination signal received; refusing intake, draining unstarted work, and finishing active SQL"
            );
            // Include queued SQL and admission/settlement, which remain work even
            // before or after the DuckDB interrupt slot is occupied.
            drain_until_idle(move || status_for_drain.has_queries() || queue_for_drain.has_waiting_queries())
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
    fn query_parameters_preserve_f64_bits_when_decoding_json() {
        for expected in [
            0.0012033694344163659_f64,
            1.2345678901234567,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::MAX,
            -0.0,
        ] {
            let body = serde_json::to_vec(&json!({
                "request_id": Uuid::nil(),
                "sql": "SELECT ?::DOUBLE",
                "parameters": [expected]
            }))
            .unwrap();
            let request: QueryRequest = serde_json::from_slice(&body).unwrap();
            let actual = request.parameters[0].as_f64().unwrap();
            assert_eq!(actual.to_bits(), expected.to_bits(), "{expected}");
        }
    }

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
        assert!(worker_identity(&headers).is_err());
        headers.insert(credentials::ACTOR_KIND_HEADER, "bootstrap".parse().unwrap());
        assert!(worker_identity(&headers).unwrap().is_bootstrap());
        headers.insert(credentials::PRINCIPAL_HEADER, "analyst".parse().unwrap());
        headers.insert(credentials::CREDENTIAL_HEADER, id.parse().unwrap());
        headers.insert(credentials::ACTOR_KIND_HEADER, "named".parse().unwrap());
        let identity = worker_identity(&headers).unwrap();
        assert_eq!(identity.principal, "analyst");
        assert_eq!(identity.credential_id.as_deref(), Some(id.as_str()));
        assert!(identity.expires_at.is_none());
        headers.insert(credentials::PRINCIPAL_HEADER, "direct".parse().unwrap());
        assert_eq!(
            worker_identity(&headers).unwrap().kind,
            credentials::ActorKind::Named
        );
        headers.insert(credentials::ACTOR_KIND_HEADER, "internal".parse().unwrap());
        assert!(worker_identity(&headers).is_err());
    }

    #[test]
    fn trusted_protocol_and_conditional_close_inputs_are_strict() {
        let mut headers = HeaderMap::new();
        assert_eq!(worker_protocol(&headers).unwrap(), Protocol::Direct);
        headers.insert("x-duckfleet-protocol", "pgwire".parse().unwrap());
        assert_eq!(worker_protocol(&headers).unwrap(), Protocol::Pgwire);
        headers.insert("x-duckfleet-protocol", "forged".parse().unwrap());
        assert!(worker_protocol(&headers).is_err());
    }

    #[tokio::test]
    async fn parsed_query_errors_keep_attempt_identity_and_acceptance() {
        let id = Uuid::new_v4();
        let response = query_response(id, Acceptance::Unknown, Err(ApiError::internal()));
        assert_eq!(response.headers()["x-query-id"], id.to_string());
        assert_eq!(
            response.headers()["x-query-status-url"],
            format!("/v1/requests/{id}")
        );
        let body = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["request_id"], id.to_string());
        assert_eq!(body["acceptance"], "unknown");
        assert_eq!(body["retry_safe"], false);
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
