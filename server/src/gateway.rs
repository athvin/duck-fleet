//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError, authorize,
    compute::{Mode, Resources, SessionRequest},
    failure,
    isolated::Isolation,
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
    /// The session pod to remove when this route goes away, for an isolated session.
    /// `None` for a persistent worker, which outlives every session it serves.
    pod: Option<String>,
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
    /// `None` where this deployment cannot create pods, which is most of them. The
    /// capability is absent rather than disabled: there is no code path that would
    /// serve an isolated request from a shared worker if it were off.
    isolation: Option<Arc<Isolation>>,
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
        self.expire().await;
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&id)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        route.touched = Instant::now();
        Ok(route.endpoint.clone())
    }

    /// Drops routes nothing has touched recently, releasing any session pod that
    /// went with them.
    ///
    /// Expiry used to be a plain `retain`, which is correct for a persistent worker
    /// because forgetting a route costs nothing. An isolated session owns a pod, so
    /// forgetting its route without releasing it would leave that pod billing until
    /// its deadline — a leak with a half-hour fuse rather than an unbounded one, but
    /// a leak nobody would see.
    async fn expire(&self) {
        let expired: Vec<String> = {
            let mut routes = self.routes.lock().await;
            let mut expired = Vec::new();
            routes.retain(|_, route| {
                if route.touched.elapsed() < Duration::from_secs(300) {
                    return true;
                }
                expired.extend(route.pod.take());
                false
            });
            expired
        };
        for pod in expired {
            tracing::warn!(pod, "releasing the pod of a session nobody closed");
            self.release(Some(pod)).await;
        }
    }

    /// Removes a session pod and settles whatever it was holding.
    ///
    /// Settlement runs on every path, not only the failures. Recovery touches only
    /// reservations still held, so a session that settled its own is unaffected —
    /// which means no caller of this has to work out which kind of ending it had.
    async fn release(&self, pod: Option<String>) {
        let (Some(pod), Some(isolation)) = (pod, self.isolation.as_ref()) else {
            return;
        };
        isolation.stop(&pod).await;
        match self.usage.settle_isolated(pod.clone()).await {
            Ok(0) => {}
            Ok(settled) => tracing::warn!(pod, settled, "settled reservations a session pod left"),
            // The startup sweep is the backstop for this, so the funds are not lost;
            // they stay held until a gateway restarts.
            Err(error) => {
                tracing::error!(?error, pod, "could not settle a session pod's reservations")
            }
        }
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
    // Carries the mode as well as the size, so an isolated session is recorded as
    // isolated rather than as whatever the pod would have assumed.
    request.compute = placement.request();
    app.expire().await;
    let permit = app
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(failure::CAPACITY, "gateway session limit reached"))?;
    // An isolated session gets a pod of its own; a persistent one gets whichever
    // worker already reports these resources. Neither substitutes for the other.
    let (endpoint, pod) = match placement.mode {
        Mode::Persistent => (app.endpoint_for(placement.resources).await?, None),
        Mode::Isolated => {
            let isolation = app
                .isolation
                .as_ref()
                .ok_or_else(crate::compute::unavailable)?;
            let started = isolation.start(placement.resources).await?;
            (started.endpoint, Some(started.name))
        }
    };
    // From here a session pod exists, so every exit below releases it. The worker's
    // own response is still relayed unchanged on the way out: it says why it refused,
    // and replacing that with a gateway error would lose the only useful detail.
    let body = serde_json::to_vec(&request).map_err(|_| ApiError::internal())?;
    let upstream = match app
        .send(&endpoint, Method::POST, "/v1/sessions", body.into())
        .await
    {
        Ok(upstream) => upstream,
        Err(error) => {
            app.release(pod).await;
            return Err(error);
        }
    };
    if !upstream.status().is_success() {
        let refused = response(upstream).await;
        app.release(pod).await;
        return refused;
    }
    let opened = upstream.json::<Value>().await.ok().and_then(|info| {
        let id = info
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())?;
        Some((id, info))
    });
    let Some((id, info)) = opened else {
        app.release(pod).await;
        return Err(ApiError::internal());
    };
    app.routes.lock().await.insert(
        id,
        Route {
            endpoint,
            touched: Instant::now(),
            pod,
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
        // The worker has already settled this session, so releasing the pod now
        // charges nothing further. Taken from the route rather than looked up, so a
        // second close cannot delete a pod a later session is using.
        let pod = app.routes.lock().await.remove(&id).and_then(|r| r.pod);
        app.release(pod).await;
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
    // Reported rather than left to be discovered by being refused. Whether this
    // deployment can create pods is a property of the deployment, and a caller
    // deciding whether to ask for isolation should not have to learn it from a 501.
    Ok(Json(json!({
        "workers": reported,
        "isolated_compute": app.isolation.is_some(),
    })))
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
        isolation: Isolation::configure()?.map(Arc::new),
    };
    // Before serving, so a pod this gateway is about to create can never be mistaken
    // for one a previous gateway abandoned.
    if let Some(isolation) = app.isolation.as_ref() {
        let holding = app
            .usage
            .held_isolated(crate::isolated::PREFIX)
            .await
            .unwrap_or_else(|error| {
                // Expected on a first boot, where the ledger tables do not exist yet:
                // a worker creates them, and the gateway does not wait for one. Not
                // fatal in any case — the pods below are still removed, and unread
                // funds stay held rather than being lost, so serving is the more
                // useful outcome. A warning, because an error here on every first
                // boot would teach an operator to ignore the real thing.
                tracing::warn!(
                    ?error,
                    "could not read which session pods hold funds; continuing without settling"
                );
                Vec::new()
            });
        isolation.sweep(&holding).await;
        for pod in holding {
            if let Ok(settled) = app.usage.settle_isolated(pod.clone()).await
                && settled > 0
            {
                tracing::warn!(
                    pod,
                    settled,
                    "settled reservations left by a gateway that is gone"
                );
            }
        }
        tracing::info!("isolated compute is available");
    }
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
