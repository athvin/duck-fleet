//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError, authorize,
    compute::{Resources, SessionRequest},
    failure,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::header,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct Route {
    endpoint: String,
    touched: Instant,
    _permit: OwnedSemaphorePermit,
}
/// What a worker reports about itself. The worker reads its CPU and memory from its
/// own cgroup, so this is what the container actually got rather than what a label
/// claims it should have.
#[derive(Clone, Debug)]
struct WorkerInfo {
    endpoint: String,
    cpu: u16,
    memory_bytes: u64,
}
impl WorkerInfo {
    fn serves(&self, wanted: Resources) -> bool {
        self.cpu == wanted.cpu && self.memory_bytes == wanted.memory_bytes
    }
}

/// A discovery result, cached between requests.
///
/// `unreachable` is kept because "no worker serves this" and "the worker that does
/// is not answering" are different failures: the first is a deployment that lacks a
/// class, the second is one that is degraded. Collapsing them would report a missing
/// class every time a pod restarted.
#[derive(Clone, Debug)]
struct Fleet {
    workers: Vec<WorkerInfo>,
    unreachable: Vec<String>,
    discovered: Instant,
}

#[derive(Clone)]
struct Gateway {
    client: Client,
    token: Arc<str>,
    /// Worker endpoints, not classes. The gateway asks each one what it is, because
    /// it cannot list pods: both service accounts set
    /// `automountServiceAccountToken: false` and the deploy asserts DuckFleet cannot
    /// grant itself RBAC, so the workers are the only available authority.
    endpoints: Arc<Vec<String>>,
    fleet: Arc<Mutex<Option<Fleet>>>,
    routes: Arc<Mutex<HashMap<Uuid, Route>>>,
    capacity: Arc<Semaphore>,
    history: crate::history::Reader,
    usage: crate::usage::Reader,
}
impl Gateway {
    async fn worker_info(&self, endpoint: &str) -> Result<WorkerInfo, ApiError> {
        // Short relative to the statement timeout: discovery must not inherit the
        // deadline of the query it is about to route.
        let reported: Value = tokio::time::timeout(
            Duration::from_secs(5),
            self.send(endpoint, Method::GET, "/v1/worker", Bytes::new()),
        )
        .await
        .map_err(|_| ApiError::internal())??
        .error_for_status()
        .map_err(|_| ApiError::internal())?
        .json()
        .await
        .map_err(|_| ApiError::internal())?;
        let cpu = reported["cpu"].as_u64().ok_or_else(ApiError::internal)?;
        let memory_bytes = reported["memory_bytes"]
            .as_u64()
            .ok_or_else(ApiError::internal)?;
        Ok(WorkerInfo {
            endpoint: endpoint.to_owned(),
            cpu: u16::try_from(cpu).map_err(|_| ApiError::internal())?,
            memory_bytes,
        })
    }

    async fn discover(&self) -> Fleet {
        let mut workers = Vec::new();
        let mut unreachable = Vec::new();
        for endpoint in self.endpoints.iter() {
            match self.worker_info(endpoint).await {
                Ok(info) => workers.push(info),
                Err(_) => unreachable.push(endpoint.clone()),
            }
        }
        if !unreachable.is_empty() {
            tracing::warn!(?unreachable, "some workers did not report their resources");
        }
        Fleet {
            workers,
            unreachable,
            discovered: Instant::now(),
        }
    }

    /// Routes by requested resources, not by a configured class name.
    ///
    /// Discovery is lazy and cached rather than done at startup: requiring a worker
    /// to answer before the gateway can serve would reintroduce the startup coupling
    /// the catalog wait removed. A miss refreshes once, so a worker that was still
    /// starting when the table was built is not reported as a class that does not
    /// exist.
    async fn endpoint_for(&self, wanted: Resources) -> Result<String, ApiError> {
        const FRESH: Duration = Duration::from_secs(30);
        let cached = {
            let fleet = self.fleet.lock().await;
            fleet
                .clone()
                .filter(|f| f.discovered.elapsed() < FRESH && f.unreachable.is_empty())
        };
        if let Some(fleet) = cached
            && let Some(worker) = fleet.workers.iter().find(|w| w.serves(wanted))
        {
            return Ok(worker.endpoint.clone());
        }
        let fleet = self.discover().await;
        *self.fleet.lock().await = Some(fleet.clone());
        if let Some(worker) = fleet.workers.iter().find(|w| w.serves(wanted)) {
            return Ok(worker.endpoint.clone());
        }
        // Never falls back to a different size. Which failure it is matters: a
        // degraded fleet is worth retrying, a missing class is not.
        Err(if fleet.unreachable.is_empty() {
            ApiError::new(
                failure::UNAVAILABLE,
                format!(
                    "no worker provides cpu={}, memory={}; this deployment runs {:?}",
                    wanted.cpu,
                    wanted.memory(),
                    fleet
                        .workers
                        .iter()
                        .map(|w| format!("cpu={} memory_bytes={}", w.cpu, w.memory_bytes))
                        .collect::<Vec<_>>()
                ),
            )
        } else {
            ApiError::new(
                failure::UNAVAILABLE,
                format!(
                    "a worker serving cpu={}, memory={} is not reachable; no statement ran",
                    wanted.cpu,
                    wanted.memory()
                ),
            )
        })
    }
    async fn send(
        &self,
        endpoint: &str,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> Result<reqwest::Response, ApiError> {
        self.client
            .request(method, format!("{endpoint}{path}"))
            .bearer_auth(&self.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| ApiError::internal())
    }
    async fn route(&self, id: Uuid) -> Result<String, ApiError> {
        let mut routes = self.routes.lock().await;
        routes.retain(|_, r| r.touched.elapsed() < Duration::from_secs(300));
        let route = routes
            .get_mut(&id)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        route.touched = Instant::now();
        Ok(route.endpoint.clone())
    }
}
async fn response(upstream: reqwest::Response) -> Result<Response, ApiError> {
    let status = upstream.status();
    let mut headers = axum::http::HeaderMap::new();
    for name in [
        header::CONTENT_TYPE,
        header::HeaderName::from_static("x-query-id"),
    ] {
        if let Some(value) = upstream.headers().get(&name) {
            headers.insert(name, value.clone());
        }
    }
    let body = upstream.bytes().await.map_err(|_| ApiError::internal())?;
    Ok((status, headers, body).into_response())
}
async fn open(
    State(app): State<Gateway>,
    request: Option<Json<SessionRequest>>,
) -> Result<Response, ApiError> {
    let mut request = request.map(|Json(r)| r).unwrap_or_default();
    let placement = request.validate()?;
    request.compute = placement.resources.request();
    app.routes
        .lock()
        .await
        .retain(|_, r| r.touched.elapsed() < Duration::from_secs(300));
    let permit = app
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(failure::CAPACITY, "gateway session limit reached"))?;
    let endpoint = app.endpoint_for(placement.resources).await?;
    let body = serde_json::to_vec(&request).map_err(|_| ApiError::internal())?;
    let upstream = app
        .send(&endpoint, Method::POST, "/v1/sessions", body.into())
        .await?;
    if !upstream.status().is_success() {
        return response(upstream).await;
    }
    let info: Value = upstream.json().await.map_err(|_| ApiError::internal())?;
    let id = info
        .get("session_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(ApiError::internal)?;
    app.routes.lock().await.insert(
        id,
        Route {
            endpoint,
            touched: Instant::now(),
            _permit: permit,
        },
    );
    Ok(Json(info).into_response())
}
async fn query(
    State(app): State<Gateway>,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    response(
        app.send(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{id}/queries"),
            body,
        )
        .await?,
    )
    .await
}
async fn cancel(State(app): State<Gateway>, Path(id): Path<Uuid>) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    response(
        app.send(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{id}/cancel"),
            Bytes::new(),
        )
        .await?,
    )
    .await
}
async fn close(State(app): State<Gateway>, Path(id): Path<Uuid>) -> Result<Response, ApiError> {
    let endpoint = app.route(id).await?;
    let upstream = app
        .send(
            &endpoint,
            Method::DELETE,
            &format!("/v1/sessions/{id}"),
            Bytes::new(),
        )
        .await?;
    if upstream.status().is_success() {
        app.routes.lock().await.remove(&id);
    }
    response(upstream).await
}
async fn history(
    State(app): State<Gateway>,
    Query(filter): Query<crate::history::Filter>,
) -> Result<Json<Value>, ApiError> {
    app.history.list(filter).await.map(Json)
}
async fn query_detail(
    State(app): State<Gateway>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.history.detail(id).await.map(Json)
}
async fn usage_events(
    State(app): State<Gateway>,
    Query(filter): Query<crate::usage::Filter>,
) -> Result<Json<Value>, ApiError> {
    app.usage.events(filter).await.map(Json)
}
async fn budget(State(app): State<Gateway>) -> Result<Json<Value>, ApiError> {
    app.usage.budget().await.map(Json)
}
async fn set_budget(
    State(app): State<Gateway>,
    Json(update): Json<crate::usage::BudgetUpdate>,
) -> Result<Json<Value>, ApiError> {
    app.usage.set_budget(update).await.map(Json)
}
async fn workers(State(app): State<Gateway>) -> Result<Json<Value>, ApiError> {
    async fn info(app: &Gateway, endpoint: &str) -> Result<Value, ApiError> {
        app.send(endpoint, Method::GET, "/v1/worker", Bytes::new())
            .await?
            .error_for_status()
            .map_err(|_| ApiError::internal())?
            .json()
            .await
            .map_err(|_| ApiError::internal())
    }
    // Every configured worker must answer: this endpoint reports the fleet, and a
    // silently short list would read as a deployment that is smaller than it is.
    let mut reported = Vec::with_capacity(app.endpoints.len());
    for endpoint in app.endpoints.iter() {
        reported.push(info(&app, endpoint).await?);
    }
    Ok(Json(json!({"workers": reported})))
}

/// Worker endpoints, listed rather than named by class, so a deployment can run any
/// set of sizes without the gateway being taught about them.
fn worker_endpoints() -> anyhow::Result<Vec<String>> {
    let configured = std::env::var("DUCKFLEET_WORKER_URLS")?;
    let endpoints: Vec<String> = configured
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(
        !endpoints.is_empty(),
        "DUCKFLEET_WORKER_URLS must list at least one worker endpoint"
    );
    Ok(endpoints)
}

pub(crate) async fn serve(token: String) -> anyhow::Result<()> {
    let app = Gateway {
        client: Client::builder()
            .timeout(Duration::from_secs(75))
            .retry(reqwest::retry::never())
            .build()?,
        token: token.into(),
        endpoints: Arc::new(worker_endpoints()?),
        fleet: Arc::new(Mutex::new(None)),
        routes: Arc::new(Mutex::new(HashMap::new())),
        capacity: Arc::new(Semaphore::new(64)),
        history: crate::history::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: crate::usage::Reader::new(std::env::var("PLATFORM_DSN")?),
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
        .route("/v1/workers", get(workers))
        .route_layer(middleware::from_fn_with_state(app.token.clone(), authorize));
    let router = Router::new()
        .route(
            "/healthz",
            get(|| async { Json(json!({"status":"ok","role":"gateway"})) }),
        )
        .merge(api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    tracing::info!("DuckFleet gateway ready");
    axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8080").await?, router).await?;
    Ok(())
}
