//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError,
    compute::{Mode, Resources, SessionRequest},
    credentials, failure,
    isolated::Isolation,
};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use postgres::NoTls;
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
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

/// Who a request authenticated as, attached by `authenticate` so handlers read a
/// fact the middleware established rather than re-deriving it.
#[derive(Clone)]
enum Principal {
    /// The environment token: the operator's bootstrap credential, and the only
    /// principal allowed to manage credentials.
    Bootstrap,
    Named(Arc<str>),
}
impl Principal {
    fn name(&self) -> &str {
        match self {
            Self::Bootstrap => credentials::BOOTSTRAP_PRINCIPAL,
            Self::Named(name) => name,
        }
    }
}

/// Resolves bearer tokens: the bootstrap by comparison, issued credentials by
/// digest lookup against the platform database, behind a short positive cache.
struct Authn {
    bootstrap: Arc<str>,
    dsn: String,
    /// digest → (principal, resolved at). Positive entries only, and cleared
    /// whole on any revocation, so on this gateway a revocation bites at once and
    /// `FRESH` only bounds how long a *deleted-underneath* database row lingers.
    cache: Mutex<HashMap<String, (Arc<str>, Instant)>>,
    /// Admission for lookups and credential management, separate from the history
    /// readers' pool so an auth storm cannot starve history or vice versa.
    lookups: Arc<Semaphore>,
}
impl Authn {
    const FRESH: Duration = Duration::from_secs(15);

    fn new(bootstrap: Arc<str>, dsn: String) -> Arc<Self> {
        Arc::new(Self {
            bootstrap,
            dsn,
            cache: Mutex::new(HashMap::new()),
            lookups: Arc::new(Semaphore::new(8)),
        })
    }

    async fn principal(&self, token: &str) -> Result<Principal, ApiError> {
        if token == self.bootstrap.as_ref() {
            return Ok(Principal::Bootstrap);
        }
        let hashed = credentials::digest(token);
        if let Some((name, resolved)) = self.cache.lock().await.get(&hashed)
            && resolved.elapsed() < Self::FRESH
        {
            return Ok(Principal::Named(name.clone()));
        }
        let lookup = hashed.clone();
        let resolved = self
            .with_db(false, move |client| credentials::resolve(client, &lookup))
            .await?;
        match resolved {
            Some(principal) => {
                let name: Arc<str> = principal.into();
                self.cache
                    .lock()
                    .await
                    .insert(hashed, (name.clone(), Instant::now()));
                Ok(Principal::Named(name))
            }
            // Unknown, revoked and expired answer alike: a probe learns nothing
            // about which it was, and none of them is worth a distinct code path.
            None => Err(ApiError::new(failure::UNAUTHORIZED, "invalid bearer token")),
        }
    }

    /// One blocking connection per call, like the history readers: credential
    /// traffic is cache-absorbed reads and rare writes, not a pool's workload.
    async fn with_db<T: Send + 'static>(
        &self,
        writable: bool,
        operation: impl FnOnce(&mut postgres::Client) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let permit = self
            .lookups
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "credential capacity reached"))?;
        let dsn = self.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.options(if writable {
                "-c statement_timeout=3000"
            } else {
                "-c default_transaction_read_only=on -c statement_timeout=3000"
            });
            let mut client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            operation(&mut client).map_err(|_| ApiError::internal())
        })
        .await
        .map_err(|_| ApiError::internal())?
    }
}

/// Authenticates every `/v1` request and attaches the principal.
async fn authenticate(
    State(authn): State<Arc<Authn>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    let Some(token) = presented else {
        return ApiError::new(failure::UNAUTHORIZED, "invalid bearer token").into_response();
    };
    match authn.principal(&token).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(refused) => refused.into_response(),
    }
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
    /// Set when a termination signal arrives, exactly as the worker does it: the
    /// probe reads it to leave the Service, and `open` reads it to refuse new
    /// sessions while requests already in flight run on.
    draining: Arc<AtomicBool>,
    /// Requests currently inside a `/v1` handler. What the drain waits on — the
    /// gateway has no engine slot to watch, so this count is its notion of busy.
    inflight: Arc<AtomicUsize>,
    authn: Arc<Authn>,
}

/// Counts a request in on construction and out on `Drop`.
///
/// Decrementing on `Drop` rather than after the handler returns is the difference
/// between a counter and a leak: a caller that disconnects has its future dropped
/// mid-await, and a drain waiting on a count that can never reach zero again would
/// always run to its deadline.
struct InFlight(Arc<AtomicUsize>);
impl InFlight {
    fn enter(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count.clone())
    }
}
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The refusal a draining gateway returns for a new session. The harness asserts the
/// kind, the retry safety, and the phrase — a caller telling "open elsewhere now"
/// apart from "something is broken" depends on all three, and this is deliberately
/// the same shape the worker returns while it drains.
fn draining_refusal() -> ApiError {
    ApiError::new(
        failure::DRAINING,
        "this gateway is shutting down and is not accepting new sessions; \
         no statement was executed",
    )
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
    Extension(principal): Extension<Principal>,
    request: Option<Json<SessionRequest>>,
) -> Result<Response, ApiError> {
    // Only new sessions are refused. Requests on sessions already open continue,
    // which is the drain: the grace period bounds how long that can take.
    if app.draining.load(Ordering::Acquire) {
        return Err(draining_refusal());
    }
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
    // The principal rides the internal hop as a header the worker records at open:
    // trusted because the hop requires the internal token, and set here rather than
    // taken from the body so a caller cannot name someone else.
    let body = serde_json::to_vec(&request).map_err(|_| ApiError::internal())?;
    let upstream = match app
        .client
        .request(Method::POST, format!("{endpoint}/v1/sessions"))
        .bearer_auth(&app.token)
        .header(header::CONTENT_TYPE, "application/json")
        .header(credentials::PRINCIPAL_HEADER, principal.name())
        .body(body)
        .send()
        .await
        .map_err(|_| ApiError::internal())
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
/// Runs one platform-owned statement through the ordinary execution machinery: a
/// session on a default-size worker, the statement, a close. Deliberately not a
/// side door — because it is a normal execution, it lands in history under the
/// real principal, is admitted and settled by the ledger, and a losing commit
/// race is classified `transaction_conflict`, retry-safe, like anyone else's.
async fn platform_statement(
    app: &Gateway,
    principal: &Principal,
    operation: &str,
    sql: String,
) -> Result<Response, ApiError> {
    if app.draining.load(Ordering::Acquire) {
        return Err(draining_refusal());
    }
    let endpoint = app.endpoint_for(Resources::default()).await?;
    let open = app
        .client
        .request(Method::POST, format!("{endpoint}/v1/sessions"))
        .bearer_auth(&app.token)
        .header(header::CONTENT_TYPE, "application/json")
        .header(credentials::PRINCIPAL_HEADER, principal.name())
        .body(json!({"compute": {}, "context": {"operation": operation}}).to_string())
        .send()
        .await
        .map_err(|_| ApiError::internal())?;
    if !open.status().is_success() {
        return response(open).await;
    }
    let session = open
        .json::<Value>()
        .await
        .ok()
        .and_then(|info| info["session_id"].as_str().map(str::to_owned))
        .ok_or_else(ApiError::internal)?;
    let result = app
        .send(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{session}/queries"),
            json!({
                "request_id": Uuid::new_v4(),
                "sql": sql,
                "timeout_seconds": 60,
                "context": {"operation": operation},
            })
            .to_string()
            .into(),
        )
        .await;
    // Closed on every path: the session was this handler's to clean up, and the
    // worker rolls back anything a failed statement left open.
    let _ = app
        .send(
            &endpoint,
            Method::DELETE,
            &format!("/v1/sessions/{session}"),
            Bytes::new(),
        )
        .await;
    response(result?).await
}

/// Snapshots of the lake, as the engine reports them. Readable by any principal:
/// listing destroys nothing and is how a caller decides what expiry would cost.
async fn lake_snapshots(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ApiError> {
    platform_statement(
        &app,
        &principal,
        "lake_snapshots",
        "SELECT snapshot_id, snapshot_time, schema_version, changes \
         FROM ducklake_snapshots('lake') ORDER BY snapshot_id"
            .to_owned(),
    )
    .await
}

/// The gateway as the pgwire front sees it: authenticate a credential, run
/// statements through an ordinary worker session under the named principal.
/// Nothing here is pgwire-shaped — which is the point; the front stays a
/// compatibility layer over machinery that neither knows nor cares it exists.
struct PgPlatform(Gateway);
impl crate::pgwire::Platform for PgPlatform {
    async fn authenticate(&self, token: String) -> Option<String> {
        self.0
            .authn
            .principal(&token)
            .await
            .ok()
            .map(|principal| principal.name().to_owned())
    }

    async fn open_session(&self, principal: String) -> anyhow::Result<crate::pgwire::Session> {
        let endpoint = self
            .0
            .endpoint_for(Resources::default())
            .await
            .map_err(|refusal| anyhow::anyhow!("{}", refusal.1))?;
        let opened = self
            .0
            .client
            .request(Method::POST, format!("{endpoint}/v1/sessions"))
            .bearer_auth(&self.0.token)
            .header(header::CONTENT_TYPE, "application/json")
            .header(credentials::PRINCIPAL_HEADER, &principal)
            .body(json!({"compute": {}, "context": {"operation": "pgwire"}}).to_string())
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("the worker did not answer"))?;
        anyhow::ensure!(
            opened.status().is_success(),
            "the worker refused the session"
        );
        let session_id = opened
            .json::<Value>()
            .await
            .ok()
            .and_then(|info| info["session_id"].as_str().map(str::to_owned))
            .ok_or_else(|| anyhow::anyhow!("the worker's answer had no session id"))?;
        Ok(crate::pgwire::Session {
            endpoint,
            session_id,
        })
    }

    async fn query(
        &self,
        session: &crate::pgwire::Session,
        sql: String,
    ) -> crate::pgwire::QueryOutcome {
        use crate::pgwire::QueryOutcome;
        let sent = self
            .0
            .send(
                &session.endpoint,
                Method::POST,
                &format!("/v1/sessions/{}/queries", session.session_id),
                json!({
                    "request_id": Uuid::new_v4(),
                    "sql": sql,
                    "timeout_seconds": 60,
                    "context": {"operation": "pgwire"},
                })
                .to_string()
                .into(),
            )
            .await;
        let upstream = match sent {
            Ok(upstream) => upstream,
            Err(refusal) => {
                return QueryOutcome::Refused {
                    kind: refusal.0.kind.to_owned(),
                    message: refusal.1,
                };
            }
        };
        if upstream.status().is_success() {
            match upstream.bytes().await {
                Ok(bytes) => QueryOutcome::Rows(bytes.to_vec()),
                Err(_) => QueryOutcome::Refused {
                    kind: "unavailable".to_owned(),
                    message: "the result did not arrive whole".to_owned(),
                },
            }
        } else {
            let refused = upstream.json::<Value>().await.unwrap_or_default();
            QueryOutcome::Refused {
                kind: refused["failure_kind"]
                    .as_str()
                    .unwrap_or("unavailable")
                    .to_owned(),
                message: refused["error"]
                    .as_str()
                    .unwrap_or("the worker refused the statement")
                    .to_owned(),
            }
        }
    }

    async fn close_session(&self, session: crate::pgwire::Session) {
        let _ = self
            .0
            .send(
                &session.endpoint,
                Method::DELETE,
                &format!("/v1/sessions/{}", session.session_id),
                Bytes::new(),
            )
            .await;
    }

    fn draining(&self) -> bool {
        self.0.draining.load(Ordering::Acquire)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceRequest {
    /// Expire snapshots older than this many days; `0` keeps only the current
    /// state, which is exactly as destructive as it sounds.
    #[serde(default)]
    expire_older_than_days: Option<u32>,
    #[serde(default)]
    compact: bool,
    #[serde(default)]
    cleanup: bool,
}

/// Lake maintenance, bootstrap-only: expiry forfeits time travel and cleanup
/// deletes files, which is the operator's call — a named principal keeping the
/// lake tidy should be handed the statements, not the lever.
async fn lake_maintenance(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<MaintenanceRequest>,
) -> Result<Response, ApiError> {
    admin_only(&principal)?;
    if request.expire_older_than_days.is_none() && !request.compact && !request.cleanup {
        return Err(ApiError::bad(
            "ask for at least one of expire_older_than_days, compact, or cleanup",
        ));
    }
    if request
        .expire_older_than_days
        .is_some_and(|days| days > 3650)
    {
        return Err(ApiError::bad("expire_older_than_days must be at most 3650"));
    }
    // Built from validated integers and booleans only — nothing caller-supplied
    // reaches the SQL as text.
    let mut statements = Vec::new();
    if let Some(days) = request.expire_older_than_days {
        statements.push(format!(
            "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '{days} days')"
        ));
    }
    if request.compact {
        statements.push("CALL ducklake_merge_adjacent_files('lake')".to_owned());
    }
    if request.cleanup {
        statements.push(match request.expire_older_than_days {
            Some(days) if days > 0 => format!(
                "CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '{days} days')"
            ),
            _ => "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)".to_owned(),
        });
    }
    platform_statement(&app, &principal, "lake_maintenance", statements.join(";\n")).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MintRequest {
    principal: String,
    #[serde(default)]
    expires_seconds: Option<i64>,
}

/// Credential management is the bootstrap's alone. A named credential that could
/// mint credentials could quietly become every principal at once.
fn admin_only(principal: &Principal) -> Result<(), ApiError> {
    match principal {
        Principal::Bootstrap => Ok(()),
        Principal::Named(_) => Err(ApiError::new(
            failure::FORBIDDEN,
            "only the bootstrap credential manages credentials",
        )),
    }
}

/// Issues a credential. The token in the response is the only time it exists in
/// plaintext anywhere the caller can reach — the database holds its digest.
async fn mint_credential(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<MintRequest>,
) -> Result<Json<Value>, ApiError> {
    admin_only(&principal)?;
    // Validated here so a bad request is the caller's 400, not a swallowed
    // internal error from the issuing transaction.
    if !credentials::valid_principal(&request.principal) {
        return Err(ApiError::bad(
            "principal must be 1-64 characters of [a-zA-Z0-9._@-], \
             and not the bootstrap's own name",
        ));
    }
    if request.expires_seconds.is_some_and(|seconds| seconds < 1) {
        return Err(ApiError::bad("expires_seconds must be at least 1"));
    }
    let issued = app
        .authn
        .with_db(true, move |client| {
            credentials::issue(client, &request.principal, request.expires_seconds)
        })
        .await?;
    Ok(Json(json!({
        "credential_id": issued.credential_id,
        "principal": issued.principal,
        "token": issued.token,
    })))
}

async fn list_credentials(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    admin_only(&principal)?;
    Ok(Json(app.authn.with_db(false, credentials::list).await?))
}

async fn revoke_credential(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Path(credential_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin_only(&principal)?;
    let revoked = app
        .authn
        .with_db(true, move |client| {
            credentials::revoke(client, &credential_id)
        })
        .await?;
    if !revoked {
        return Err(ApiError::new(
            failure::NOT_FOUND,
            "no such credential, or it is already revoked",
        ));
    }
    // The whole cache rather than one entry: revocation is rare, the cache is
    // small, and clearing it makes a revocation bite this gateway immediately
    // instead of after the cache window.
    app.authn.cache.lock().await.clear();
    Ok(Json(json!({"revoked": true})))
}

/// Wraps every `/v1` route so the drain can see work in flight.
///
/// The completion log line fires only for a request that *spanned* the signal:
/// one admitted during the drain — the refusal itself, a history read — completing
/// after it proves nothing about whether live work was waited for, and the harness
/// reads this line to assert exactly that ordering.
async fn track_inflight(
    State((draining, inflight)): State<(Arc<AtomicBool>, Arc<AtomicUsize>)>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let entered_draining = draining.load(Ordering::Acquire);
    let _guard = InFlight::enter(&inflight);
    let response = next.run(request).await;
    if !entered_draining && draining.load(Ordering::Acquire) {
        tracing::info!("a request in flight when the signal arrived has completed");
    }
    response
}

/// Draining is reported before the ok, for the same reason the worker does it: a
/// draining gateway is still answering, and "ok" would keep it in the Service's
/// endpoints while it shuts down. Failing here is what takes it out of rotation.
async fn health(State(app): State<Gateway>) -> Response {
    if app.draining.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "draining", "role": "gateway"})),
        )
            .into_response();
    }
    Json(json!({"status": "ok", "role": "gateway"})).into_response()
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
    // Configured and permitted are different questions, and only the API server can
    // answer the second: the Role that allows pod creation is applied outside this
    // release by an operator. Resolved here, once, so `isolated_compute` below reports
    // what this gateway can actually do.
    let isolation = match Isolation::configure()? {
        None => None,
        Some(isolation) => match isolation.permitted().await {
            Ok(()) => Some(Arc::new(isolation)),
            // Not fatal, deliberately. Everything else this gateway does still works,
            // and reporting the capability as absent refuses `isolated` with a reason
            // the caller can act on instead of failing their statement at pod-create
            // time. At error level because this deployment asked for a feature that is
            // not there — the message names the fix.
            Err(error) => {
                tracing::error!(
                    error = format!("{error:#}"),
                    "isolated compute is configured but this gateway is not permitted to \
                     create pods; serving without it"
                );
                None
            }
        },
    };
    let token: Arc<str> = token.into();
    let app = Gateway {
        client: Client::builder()
            .timeout(Duration::from_secs(75))
            .retry(reqwest::retry::never())
            .build()?,
        authn: Authn::new(token.clone(), std::env::var("PLATFORM_DSN")?),
        token,
        endpoints: Arc::new(worker_endpoints()?),
        fleet: Arc::new(Mutex::new(None)),
        routes: Arc::new(Mutex::new(HashMap::new())),
        capacity: Arc::new(Semaphore::new(64)),
        history: crate::history::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: crate::usage::Reader::new(
            std::env::var("PLATFORM_DSN")?,
            crate::pricing::RateCard::from_env()?,
        ),
        isolation,
        draining: Arc::new(AtomicBool::new(false)),
        inflight: Arc::new(AtomicUsize::new(0)),
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
        // After the sweep, so every session-pod identity is provably dead. Tolerant
        // for the same reason as the held read above: on a first boot the history
        // table does not exist yet, and there is nothing to repair in it either.
        match app
            .usage
            .reconcile_isolated_history(crate::isolated::PREFIX)
            .await
        {
            Ok(0) | Err(_) => {}
            Ok(repaired) => tracing::warn!(
                repaired,
                "marked interrupted the running records of session pods that are gone"
            ),
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
        .route(
            "/v1/credentials",
            get(list_credentials).post(mint_credential),
        )
        .route("/v1/lake/snapshots", get(lake_snapshots))
        .route("/v1/lake/maintenance", post(lake_maintenance))
        .route(
            "/v1/credentials/{id}",
            axum::routing::delete(revoke_credential),
        )
        .route_layer(middleware::from_fn_with_state(
            app.authn.clone(),
            authenticate,
        ))
        .route_layer(middleware::from_fn_with_state(
            (app.draining.clone(), app.inflight.clone()),
            track_inflight,
        ));
    // The pgwire front, where the deployment asked for one: a compatibility
    // listener over the same sessions, principals, and meters as everything else.
    if std::env::var("DUCKFLEET_PGWIRE").as_deref() == Ok("true") {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:5432").await?;
        tracing::info!(port = 5432, "pgwire front listening");
        tokio::spawn(crate::pgwire::serve(
            Arc::new(PgPlatform(app.clone())),
            listener,
        ));
    }
    let draining = app.draining.clone();
    let inflight = app.inflight.clone();
    let router = Router::new()
        .route("/healthz", get(health))
        .merge(api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    tracing::info!("DuckFleet gateway ready");
    axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8080").await?, router)
        .with_graceful_shutdown(async move {
            crate::terminating().await;
            // Marked first, and the server deliberately keeps serving afterwards —
            // handing the signal straight to `with_graceful_shutdown` closes the
            // listener at once, which is the trap the worker's drain documented: the
            // probe would see a refused connection instead of "draining", and the
            // classified refusal could never be returned.
            draining.store(true, Ordering::Release);
            tracing::info!(
                "termination signal received; refusing new sessions and finishing \
                 requests in flight"
            );
            crate::drain_until_idle(move || inflight.load(Ordering::Acquire) > 0).await;
            tracing::info!("drain complete; shutting down");
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The harness greps for each of these three, so the contract lives here too:
    /// weakening any one of them turns "open a session elsewhere now" into
    /// "something is wrong with this deployment".
    #[test]
    fn a_draining_gateway_refuses_sessions_in_the_worker_s_terms() {
        let refusal = draining_refusal();
        assert_eq!(refusal.0.kind, "draining");
        assert!(refusal.0.retry_safe);
        assert_eq!(refusal.0.http, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            refusal.1.contains("no statement was executed"),
            "{}",
            refusal.1
        );
    }

    #[test]
    fn overlapping_requests_are_counted_in_and_out() {
        let count = Arc::new(AtomicUsize::new(0));
        let first = InFlight::enter(&count);
        let second = InFlight::enter(&count);
        assert_eq!(count.load(Ordering::Acquire), 2);
        drop(first);
        assert_eq!(count.load(Ordering::Acquire), 1);
        drop(second);
        assert_eq!(count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn only_the_bootstrap_may_manage_credentials() {
        assert!(admin_only(&Principal::Bootstrap).is_ok());
        let refused = admin_only(&Principal::Named("analyst".into())).unwrap_err();
        assert_eq!(refused.0.kind, "forbidden");
        assert!(!refused.0.retry_safe);
        assert_eq!(refused.0.http, StatusCode::FORBIDDEN);
    }

    #[test]
    fn the_bootstrap_resolves_by_comparison_alone() {
        // No database behind this Authn; reaching for one would hang the test,
        // so passing proves the bootstrap path never needs it.
        let authn = Authn::new("the-bootstrap-token".into(), "host=nowhere".into());
        let resolved = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(authn.principal("the-bootstrap-token"))
            .unwrap();
        assert_eq!(resolved.name(), credentials::BOOTSTRAP_PRINCIPAL);
    }

    /// A caller that disconnects has its handler future dropped mid-await. The drain
    /// polls this count until it reaches zero, so a guard that survived cancellation
    /// without decrementing would hold every future drain to its full deadline.
    #[tokio::test]
    async fn a_cancelled_request_still_leaves_the_count() {
        let count = Arc::new(AtomicUsize::new(0));
        let entered = count.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            let _guard = InFlight::enter(&entered);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("the request never started");
        assert_eq!(count.load(Ordering::Acquire), 1);
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert_eq!(count.load(Ordering::Acquire), 0);
    }
}
