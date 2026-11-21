//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError,
    compute::{Mode, Resources, SessionRequest},
    credentials::{self, Identity},
    failure,
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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct Route {
    endpoint: String,
    touched: Instant,
    /// Who opened this session. Session IDs appear in history, which every
    /// credential can read, so possession of an ID proves nothing — only the
    /// opener (or the bootstrap) may run, cancel, or close the session.
    principal: Arc<str>,
    /// The last authorized request may rotate credentials without changing owner.
    identity: Identity,
    active: Arc<AtomicUsize>,
    closing: bool,
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
    ready: bool,
    active_queries: u64,
    queued_queries: u64,
    open_sessions: u64,
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
    reported: Vec<Value>,
}

/// How an attempt to open a persistent session ended: on a worker, or with the last
/// candidate's refusal, already shaped for relaying to the caller unchanged.
enum OpenOutcome {
    Opened {
        endpoint: String,
        upstream: reqwest::Response,
    },
    Refused(Response),
}

/// Whether a worker's refusal of an open is safe grounds to try its sibling: the
/// worker classified it, promised nothing ran, and named a condition that is about
/// this worker rather than the request — full, draining, or still starting. Not
/// every retry-safe kind qualifies: `budget_exhausted` is about the deployment's
/// funds and would refuse identically everywhere.
fn failover_worthy(status: StatusCode, refusal: &Value) -> bool {
    !status.is_success()
        && refusal["retry_safe"] == json!(true)
        && matches!(
            refusal["failure_kind"].as_str(),
            Some("capacity" | "draining" | "starting")
        )
}

/// Prefer the least execution pressure, then fewer open sessions. Stable sorting
/// preserves configured order on ties. Counts come from workers, so pgwire and
/// platform operations have exactly the same weight as HTTP clients.
fn ranked_endpoints(workers: &[WorkerInfo], wanted: Resources) -> Vec<String> {
    let mut matches: Vec<&WorkerInfo> = workers
        .iter()
        .filter(|w| w.ready && w.serves(wanted))
        .collect();
    matches.sort_by_key(|w| {
        (
            w.active_queries.saturating_add(w.queued_queries),
            w.open_sessions,
        )
    });
    matches.iter().map(|w| w.endpoint.clone()).collect()
}

const FLEET_FRESH: Duration = Duration::from_secs(1);

fn observed_at() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
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
    fn from_identity(identity: &Identity) -> Self {
        if identity.is_bootstrap() {
            Self::Bootstrap
        } else {
            Self::Named(identity.principal.clone().into())
        }
    }
    fn name(&self) -> &str {
        match self {
            Self::Bootstrap => credentials::BOOTSTRAP_PRINCIPAL,
            Self::Named(name) => name,
        }
    }
    /// Whether this caller may act on a session `owner` opened. The bootstrap may
    /// act on anyone's — it is the operator, and cancelling a runaway statement is
    /// its job — while a named principal is confined to its own.
    fn may_act_on(&self, owner: &str) -> bool {
        matches!(self, Self::Bootstrap) || self.name() == owner
    }
}

#[derive(Default)]
struct AuthCache {
    generation: u64,
    identities: HashMap<String, (Identity, Instant)>,
    digests: HashMap<String, String>,
}

impl AuthCache {
    fn get(&self, id: &str) -> Option<Identity> {
        self.identities.get(id).and_then(|(identity, resolved)| {
            (resolved.elapsed() < Authn::FRESH && !identity.expired()).then(|| identity.clone())
        })
    }

    /// Generation and publication share the same mutex. A lookup started before
    /// revocation cannot refill the cache after the revocation has cleared it.
    fn publish(
        &mut self,
        generation: u64,
        identity: Identity,
        started: Instant,
        digest: Option<String>,
    ) -> bool {
        if generation != self.generation || identity.expired() {
            return false;
        }
        let Some(id) = identity.credential_id.clone() else {
            return false;
        };
        self.identities
            .retain(|_, (identity, at)| at.elapsed() < Authn::FRESH && !identity.expired());
        if self.identities.len() >= 512
            && !self.identities.contains_key(&id)
            && let Some(oldest) = self
                .identities
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(id, _)| id.clone())
        {
            self.identities.remove(&oldest);
        }
        self.digests
            .retain(|_, id| self.identities.contains_key(id));
        self.identities.insert(id.clone(), (identity, started));
        if let Some(digest) = digest {
            self.digests.insert(digest, id);
        }
        true
    }

    fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.identities.clear();
        self.digests.clear();
    }
}

#[derive(Clone)]
enum AuthLookup {
    Digest(String),
    Id(String),
}

/// Resolves bearer tokens: the bootstrap by comparison, issued credentials by
/// digest lookup against the platform database, behind a short positive cache.
struct Authn {
    bootstrap: Arc<str>,
    dsn: String,
    /// Positive entries share credential identity across HTTP and pgwire checks.
    /// The generation also invalidates database lookups racing a revocation.
    cache: Mutex<AuthCache>,
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
            cache: Mutex::new(AuthCache::default()),
            lookups: Arc::new(Semaphore::new(8)),
        })
    }

    async fn authenticate(&self, token: &str) -> Result<Identity, ApiError> {
        if credentials::tokens_match(token, &self.bootstrap) {
            return Ok(Identity::bootstrap());
        }
        self.resolve(AuthLookup::Digest(credentials::digest(token)))
            .await
    }

    async fn revalidate(&self, identity: &Identity) -> Result<(), ApiError> {
        if identity.is_bootstrap() {
            return Ok(());
        }
        let id = identity
            .credential_id
            .as_ref()
            .ok_or_else(invalid_credential)?;
        if identity.expired() {
            return Err(invalid_credential());
        }
        let current = self.resolve(AuthLookup::Id(id.clone())).await?;
        if current.principal != identity.principal || current.expired() {
            return Err(invalid_credential());
        }
        Ok(())
    }

    async fn resolve(&self, lookup: AuthLookup) -> Result<Identity, ApiError> {
        // A burst of operator changes may invalidate a lookup repeatedly. Refuse
        // rather than return a stale result or wait without a bound.
        for _ in 0..3 {
            let started = Instant::now();
            let generation = {
                let cache = self.cache.lock().await;
                let id = match &lookup {
                    AuthLookup::Digest(digest) => cache.digests.get(digest),
                    AuthLookup::Id(id) => Some(id),
                };
                if let Some(identity) = id.and_then(|id| cache.get(id)) {
                    return Ok(identity);
                }
                cache.generation
            };
            let query = lookup.clone();
            let resolved = self
                .with_db(false, move |client| match query {
                    AuthLookup::Digest(digest) => credentials::resolve(client, &digest),
                    AuthLookup::Id(id) => credentials::resolve_id(client, &id),
                })
                .await?;
            let mut cache = self.cache.lock().await;
            if cache.generation != generation {
                continue;
            }
            let identity = resolved
                .filter(|identity| !identity.expired())
                .ok_or_else(invalid_credential)?;
            let digest = match &lookup {
                AuthLookup::Digest(digest) => Some(digest.clone()),
                AuthLookup::Id(_) => None,
            };
            if cache.publish(generation, identity.clone(), started, digest) {
                return Ok(identity);
            }
            return Err(invalid_credential());
        }
        Err(ApiError::new(
            failure::CAPACITY,
            "credential state is changing; retry authentication",
        ))
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

fn invalid_credential() -> ApiError {
    ApiError::new(failure::UNAUTHORIZED, "invalid platform credential")
}

fn authenticated_request(
    request: reqwest::RequestBuilder,
    identity: &Identity,
) -> reqwest::RequestBuilder {
    let request = request.header(credentials::PRINCIPAL_HEADER, &identity.principal);
    match &identity.credential_id {
        Some(id) => request.header(credentials::CREDENTIAL_HEADER, id),
        None => request,
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
    match authn.authenticate(&token).await {
        Ok(identity) => {
            request
                .extensions_mut()
                .insert(Principal::from_identity(&identity));
            request.extensions_mut().insert(identity);
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
    /// Last successful observations remain available for diagnosis, never routing.
    observations: Arc<Mutex<HashMap<String, Value>>>,
    /// Held for the duration of one `discover`, so concurrent cache misses collapse
    /// into a single concurrent fleet probe.
    discovering: Arc<Mutex<()>>,
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
    async fn worker_info(&self, endpoint: &str) -> Result<Value, ApiError> {
        // Bound the entire response, including decoding: headers alone are not a
        // successful observation when a broken worker stalls its response body.
        tokio::time::timeout(Duration::from_secs(5), async {
            self.send(endpoint, Method::GET, "/v1/worker", Bytes::new())
                .await?
                .error_for_status()
                .map_err(|_| ApiError::internal())?
                .json::<Value>()
                .await
                .map_err(|_| ApiError::internal())
        })
        .await
        .map_err(|_| ApiError::internal())?
    }

    async fn discover(&self) -> Fleet {
        let mut probes = tokio::task::JoinSet::new();
        for (index, endpoint) in self.endpoints.iter().cloned().enumerate() {
            let app = self.clone();
            probes.spawn(async move {
                let result = app.worker_info(&endpoint).await;
                (index, result, observed_at())
            });
        }
        let mut ordered: Vec<Option<(Value, u64)>> = vec![None; self.endpoints.len()];
        while let Some(joined) = probes.join_next().await {
            if let Ok((index, Ok(value), observed)) = joined {
                // A starting worker has no observed cgroup placement yet. It is
                // still reachable and should be shown as starting, not missing.
                if value.is_object() {
                    ordered[index] = Some((value, observed));
                }
            }
        }
        let mut workers = Vec::new();
        let mut unreachable = Vec::new();
        let mut reported = Vec::new();
        let mut observations = self.observations.lock().await;
        for (endpoint, value) in self.endpoints.iter().zip(ordered) {
            match value {
                Some((mut value, observed)) => {
                    value["endpoint"] = json!(endpoint);
                    value["last_success_at"] = json!(observed);
                    let cpu = value["cpu"].as_u64().and_then(|n| u16::try_from(n).ok());
                    if let (Some(cpu), Some(memory_bytes)) = (cpu, value["memory_bytes"].as_u64()) {
                        workers.push(WorkerInfo {
                            endpoint: endpoint.clone(),
                            cpu,
                            memory_bytes,
                            ready: value["ready"] == true
                                && value["draining"] == false
                                && matches!(value["state"].as_str(), Some("ready" | "busy"))
                                && ["active_queries", "queued_queries", "open_sessions"]
                                    .iter()
                                    .all(|key| value[key].as_u64().is_some()),
                            active_queries: value["active_queries"].as_u64().unwrap_or(0),
                            queued_queries: value["queued_queries"].as_u64().unwrap_or(0),
                            open_sessions: value["open_sessions"].as_u64().unwrap_or(0),
                        });
                    }
                    observations.insert(endpoint.clone(), value.clone());
                    reported.push(value);
                }
                None => {
                    unreachable.push(endpoint.clone());
                    let previous = observations.get(endpoint);
                    reported.push(json!({
                        "endpoint": endpoint,
                        "state": "unreachable",
                        "ready": false,
                        "last_success_at": previous.and_then(|v| v.get("last_success_at")),
                        "last_observation": previous,
                    }));
                }
            }
        }
        if !unreachable.is_empty() {
            tracing::warn!(?unreachable, "some workers did not report their resources");
        }
        Fleet {
            workers,
            unreachable,
            reported,
            discovered: Instant::now(),
        }
    }

    /// Shared by placement and inspection, with short-lived load observations.
    /// Unreachable workers do not discard healthy siblings' observations.
    async fn fleet_snapshot(&self) -> Fleet {
        let fresh = |fleet: &Option<Fleet>| {
            fleet
                .clone()
                .filter(|f| f.discovered.elapsed() < FLEET_FRESH)
        };
        if let Some(fleet) = fresh(&*self.fleet.lock().await) {
            return fleet;
        }
        let _flight = self.discovering.lock().await;
        if let Some(fleet) = fresh(&*self.fleet.lock().await) {
            return fleet;
        }
        let discovered = self.discover().await;
        *self.fleet.lock().await = Some(discovered.clone());
        discovered
    }

    /// New sessions use worker-observed pressure from every protocol. Existing
    /// sessions remain pinned even if another worker subsequently becomes idle.
    async fn endpoints_for(&self, wanted: Resources) -> Result<Vec<String>, ApiError> {
        let fleet = self.fleet_snapshot().await;
        let candidates = ranked_endpoints(&fleet.workers, wanted);
        if !candidates.is_empty() {
            return Ok(candidates);
        }
        // Excluding a worker from placement must preserve the classified open
        // refusal clients wait out during a rollout. No open was forwarded here.
        for report in &fleet.reported {
            let matching = report["cpu"].as_u64() == Some(u64::from(wanted.cpu))
                && report["memory_bytes"].as_u64() == Some(wanted.memory_bytes);
            let refusal = match report["state"].as_str() {
                Some("draining") if matching => Some(failure::DRAINING),
                Some("unavailable") if matching => Some(failure::NO_READY_WORKER),
                Some("starting") if matching || report.get("cpu").is_none() => {
                    Some(failure::STARTING)
                }
                _ => None,
            };
            if let Some(refusal) = refusal {
                return Err(ApiError::new(
                    refusal,
                    format!(
                        "a worker serving the requested resources is {}; no session was opened and no statement ran",
                        refusal.kind
                    ),
                ));
            }
        }
        Err(if fleet.unreachable.is_empty() {
            ApiError::new(
                failure::UNAVAILABLE,
                format!(
                    "no ready worker provides cpu={}, memory={}; this deployment reports {:?}",
                    wanted.cpu,
                    wanted.memory(),
                    fleet
                        .workers
                        .iter()
                        .map(|w| format!(
                            "cpu={} memory_bytes={} ready={}",
                            w.cpu, w.memory_bytes, w.ready
                        ))
                        .collect::<Vec<_>>()
                ),
            )
        } else {
            ApiError::new(
                failure::NO_READY_WORKER,
                format!(
                    "no reachable worker serves cpu={}, memory={}; no session was opened and no statement ran",
                    wanted.cpu,
                    wanted.memory()
                ),
            )
        })
    }

    /// Opens a session on the least-loaded worker providing the size, failing over
    /// to the next when the current one provably ran nothing: its own classified
    /// `retry_safe` refusal (full, draining, or still-starting), or a connection
    /// that never established — a stale cache entry for a worker that is gone sends
    /// no bytes, so no session was created. A transport error *after* connecting is
    /// never failed over: the open may have been received, and opening a second
    /// would leak the first.
    async fn open_persistent(
        &self,
        wanted: Resources,
        body: Vec<u8>,
        identity: &Identity,
    ) -> Result<OpenOutcome, ApiError> {
        let candidates = self.endpoints_for(wanted).await?;
        let total = candidates.len();
        for (position, endpoint) in candidates.into_iter().enumerate() {
            let last = position + 1 == total;
            self.authn.revalidate(identity).await?;
            let request = self
                .client
                .request(Method::POST, format!("{endpoint}/v1/sessions"))
                .bearer_auth(&self.token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone());
            let upstream = match authenticated_request(request, identity).send().await {
                Ok(upstream) => upstream,
                // `is_connect` is the connection-never-established case: nothing was
                // sent, so a worker that has gone away under a stale cache entry is
                // safe to skip for its sibling. Anything else may have been received.
                Err(error) if error.is_connect() && !last => {
                    tracing::info!(
                        endpoint,
                        "a worker was unreachable at connect; trying its sibling"
                    );
                    continue;
                }
                Err(_) => return Err(ApiError::internal()),
            };
            if upstream.status().is_success() {
                // Account for the known open without discarding other observations:
                // otherwise every open would re-probe an unreachable sibling. The
                // report remains the last sampled data; only ranking is adjusted.
                if let Some(fleet) = self.fleet.lock().await.as_mut()
                    && let Some(worker) = fleet.workers.iter_mut().find(|w| w.endpoint == endpoint)
                {
                    worker.open_sessions = worker.open_sessions.saturating_add(1);
                }
                return Ok(OpenOutcome::Opened { endpoint, upstream });
            }
            let status = upstream.status();
            let mut headers = axum::http::HeaderMap::new();
            if let Some(value) = upstream.headers().get(header::CONTENT_TYPE) {
                headers.insert(header::CONTENT_TYPE, value.clone());
            }
            let bytes = upstream.bytes().await.map_err(|_| ApiError::internal())?;
            let refusal: Value = serde_json::from_slice(&bytes).unwrap_or_default();
            if !last && failover_worthy(status, &refusal) {
                tracing::info!(
                    endpoint,
                    kind = refusal["failure_kind"].as_str().unwrap_or(""),
                    "a worker refused an open it provably did not start; trying its sibling"
                );
                continue;
            }
            // The last worker's own words, relayed unchanged: it says why it
            // refused, and a gateway paraphrase would lose the only useful detail.
            return Ok(OpenOutcome::Refused(
                (status, headers, bytes).into_response(),
            ));
        }
        // `endpoints_for` never returns an empty list.
        Err(ApiError::internal())
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

    async fn send_as(
        &self,
        endpoint: &str,
        method: Method,
        path: &str,
        body: Bytes,
        identity: &Identity,
    ) -> Result<reqwest::Response, ApiError> {
        // Middleware ran before body extraction and route/provisioning waits.
        // Recheck here; the worker also checks at dequeue, before any SQL runs.
        self.authn.revalidate(identity).await?;
        authenticated_request(
            self.client
                .request(method, format!("{endpoint}{path}"))
                .bearer_auth(&self.token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body),
            identity,
        )
        .send()
        .await
        .map_err(|_| ApiError::internal())
    }
    async fn route(&self, id: Uuid, caller: &Principal) -> Result<String, ApiError> {
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&id)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        // A 403 rather than a masked 404: the caller is a colleague, not an
        // adversary, and naming the refusal is this API's idiom. Checked before the
        // touch so a probing caller cannot keep someone else's session alive.
        if !caller.may_act_on(&route.principal) {
            return Err(ApiError::new(
                failure::FORBIDDEN,
                "this session belongs to another principal",
            ));
        }
        route.touched = Instant::now();
        Ok(route.endpoint.clone())
    }

    async fn query_route(
        &self,
        id: Uuid,
        identity: &Identity,
    ) -> Result<(String, InFlight), ApiError> {
        self.authn.revalidate(identity).await?;
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&id)
            .filter(|route| !route.closing)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        if !Principal::from_identity(identity).may_act_on(&route.principal) {
            return Err(ApiError::new(
                failure::FORBIDDEN,
                "this session belongs to another principal",
            ));
        }
        route.touched = Instant::now();
        if identity.principal == route.principal.as_ref() {
            route.identity = identity.clone();
        }
        Ok((route.endpoint.clone(), InFlight::enter(&route.active)))
    }

    /// Periodic, independent of new client traffic. Close on the worker before
    /// deleting an isolated pod, so cleanup never interrupts accepted active SQL.
    async fn expire(&self) {
        let candidates: Vec<_> = self
            .routes
            .lock()
            .await
            .iter()
            .filter(|(_, route)| route.active.load(Ordering::Acquire) == 0)
            .map(|(id, route)| (*id, route.identity.clone(), route.touched, route.closing))
            .collect();
        let ids: Vec<String> = candidates
            .iter()
            .filter_map(|(_, identity, _, _)| identity.credential_id.clone())
            .collect();
        let current = if ids.is_empty() {
            Ok(Vec::new())
        } else {
            self.authn
                .with_db(false, move |client| credentials::resolve_ids(client, &ids))
                .await
        };
        let mut retire = Vec::new();
        {
            let mut routes = self.routes.lock().await;
            for (id, identity, touched, closing) in candidates {
                // Failure to establish continuing authority closes named idle
                // sessions; it does not interrupt active statements.
                let invalid = identity.expired()
                    || (identity.credential_id.is_some()
                        && !current
                            .as_ref()
                            .is_ok_and(|valid| valid.iter().any(|item| item == &identity)));
                if !(closing || invalid || touched.elapsed() >= Duration::from_secs(300)) {
                    continue;
                }
                if let Some(route) = routes.get_mut(&id)
                    && route.identity == identity
                    && route.touched == touched
                    && route.active.load(Ordering::Acquire) == 0
                {
                    route.closing = true;
                    retire.push((id, route.endpoint.clone()));
                }
            }
        }
        let mut closes = tokio::task::JoinSet::new();
        for (id, endpoint) in retire {
            let app = self.clone();
            closes.spawn(async move {
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.send(
                        &endpoint,
                        Method::DELETE,
                        &format!("/v1/sessions/{id}"),
                        Bytes::new(),
                    ),
                )
                .await;
                // Retain failed closes for the next pass. Only an acknowledged
                // close proves that deleting this pod cannot cancel active SQL.
                if matches!(result, Ok(Ok(ref response)) if response.status().is_success()) {
                    let pod = app
                        .routes
                        .lock()
                        .await
                        .remove(&id)
                        .and_then(|route| route.pod);
                    // Pod deletion/settlement is bounded independently and cannot
                    // delay the next credential cleanup pass.
                    tokio::spawn(async move { app.release(pod).await });
                }
            });
        }
        while closes.join_next().await.is_some() {}
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
    Extension(identity): Extension<Identity>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Only new sessions are refused. Requests on sessions already open continue,
    // which is the drain: the grace period bounds how long that can take.
    if app.draining.load(Ordering::Acquire) {
        return Err(draining_refusal());
    }
    // An empty body is the default (persistent) request; a present-but-malformed
    // body is a client error in the gateway's own {error,failure_kind,retry_safe}
    // shape, not axum's raw 422 that bypasses the contract.
    let mut request: SessionRequest = if body.is_empty() {
        SessionRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            ApiError::new(
                failure::INVALID_REQUEST,
                format!("invalid session request: {e}"),
            )
        })?
    };
    let placement = request.validate()?;
    // Body extraction may have outlived authentication. Refuse before spending
    // resources on isolated provisioning, then check again before worker open.
    app.authn.revalidate(&identity).await?;
    // Carries the mode as well as the size, so an isolated session is recorded as
    // isolated rather than as whatever the pod would have assumed.
    request.compute = placement.request();
    let permit = app
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::new(failure::CAPACITY, "gateway session limit reached"))?;
    // The principal rides the internal hop as a header the worker records at open:
    // trusted because the hop requires the internal token, and set here rather than
    // taken from the body so a caller cannot name someone else.
    let body = serde_json::to_vec(&request).map_err(|_| ApiError::internal())?;
    // An isolated session gets a pod of its own; a persistent one gets the
    // least-loaded worker already reporting these resources, with a full or draining
    // worker failed over to its sibling. Neither mode substitutes for the other.
    let (endpoint, pod, upstream) = match placement.mode {
        Mode::Persistent => {
            match app
                .open_persistent(placement.resources, body, &identity)
                .await?
            {
                OpenOutcome::Opened { endpoint, upstream } => (endpoint, None, upstream),
                OpenOutcome::Refused(relay) => return Ok(relay),
            }
        }
        Mode::Isolated => {
            let isolation = app
                .isolation
                .as_ref()
                .ok_or_else(crate::compute::unavailable)?;
            let started = isolation.start(placement.resources).await?;
            // From here a session pod exists, so every exit below releases it. The
            // worker's own response is still relayed unchanged on the way out: it
            // says why it refused, and replacing that with a gateway error would
            // lose the only useful detail.
            let pod = Some(started.name);
            let upstream = match app
                .send_as(
                    &started.endpoint,
                    Method::POST,
                    "/v1/sessions",
                    body.into(),
                    &identity,
                )
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
            (started.endpoint, pod, upstream)
        }
    };
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
            principal: identity.principal.clone().into(),
            identity,
            active: Arc::new(AtomicUsize::new(0)),
            closing: false,
            pod,
            _permit: permit,
        },
    );
    Ok(Json(info).into_response())
}
async fn query(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (endpoint, guard) = app.query_route(id, &identity).await?;
    let inflight = InFlight::enter(&app.inflight);
    // A disconnected caller does not establish that the accepted work ended.
    tokio::spawn(async move {
        let _guard = guard;
        let _inflight = inflight;
        response(
            app.send_as(
                &endpoint,
                Method::POST,
                &format!("/v1/sessions/{id}/queries"),
                body,
                &identity,
            )
            .await?,
        )
        .await
    })
    .await
    .map_err(|_| ApiError::internal())?
}
async fn cancel(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id, &principal).await?;
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
async fn close(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id, &principal).await?;
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
    Extension(principal): Extension<Principal>,
    Json(update): Json<crate::usage::BudgetUpdate>,
) -> Result<Json<Value>, ApiError> {
    // Reading the balance stays open to every credential — it is how a caller
    // sizes a request — but the ceiling belongs to whoever answers for the bill.
    admin_only(&principal, "set budget limits")?;
    app.usage.set_budget(update).await.map(Json)
}
/// Runs one platform-owned statement through the ordinary execution machinery: a
/// session on a default-size worker, the statement, a close. Deliberately not a
/// side door — because it is a normal execution, it lands in history under the
/// real principal, is admitted and settled by the ledger, and a losing commit
/// race is classified `transaction_conflict`, retry-safe, like anyone else's.
async fn platform_statement(
    app: &Gateway,
    identity: &Identity,
    operation: &str,
    sql: String,
) -> Result<Response, ApiError> {
    if app.draining.load(Ordering::Acquire) {
        return Err(draining_refusal());
    }
    let body = json!({"compute": {}, "context": {"operation": operation}})
        .to_string()
        .into_bytes();
    let (endpoint, open) = match app
        .open_persistent(Resources::default(), body, identity)
        .await?
    {
        OpenOutcome::Opened { endpoint, upstream } => (endpoint, upstream),
        OpenOutcome::Refused(relay) => return Ok(relay),
    };
    let session = open
        .json::<Value>()
        .await
        .ok()
        .and_then(|info| info["session_id"].as_str().map(str::to_owned))
        .ok_or_else(ApiError::internal)?;
    let result = app
        .send_as(
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
            identity,
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
    Extension(identity): Extension<Identity>,
) -> Result<Response, ApiError> {
    platform_statement(
        &app,
        &identity,
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
    async fn authenticate(&self, token: String) -> Result<Identity, ApiError> {
        self.0.authn.authenticate(&token).await
    }

    async fn revalidate(&self, identity: &Identity) -> Result<(), ApiError> {
        self.0.authn.revalidate(identity).await
    }

    async fn open_session(&self, identity: &Identity) -> Result<crate::pgwire::Session, ApiError> {
        let body = json!({"compute": {}, "context": {"operation": "pgwire"}})
            .to_string()
            .into_bytes();
        let (endpoint, opened) = match self
            .0
            .open_persistent(Resources::default(), body, identity)
            .await?
        {
            OpenOutcome::Opened { endpoint, upstream } => (endpoint, upstream),
            OpenOutcome::Refused(reply) => {
                return Err(match reply.status() {
                    StatusCode::UNAUTHORIZED => invalid_credential(),
                    StatusCode::FORBIDDEN => {
                        ApiError::new(failure::FORBIDDEN, "session access refused")
                    }
                    _ => ApiError::new(failure::UNAVAILABLE, "the worker refused the session"),
                });
            }
        };
        let session_id = opened
            .json::<Value>()
            .await
            .ok()
            .and_then(|info| info["session_id"].as_str().map(str::to_owned))
            .ok_or_else(ApiError::internal)?;
        Ok(crate::pgwire::Session {
            endpoint,
            session_id,
        })
    }

    async fn query(
        &self,
        identity: &Identity,
        session: &crate::pgwire::Session,
        sql: String,
    ) -> crate::pgwire::QueryOutcome {
        use crate::pgwire::QueryOutcome;
        // Counted into the same in-flight tally the HTTP handlers use, so the
        // gateway's drain waits for a pgwire statement already running. The pgwire
        // listener sits outside the axum router that carries `track_inflight`, so
        // without this a rollout would close under a live wire-protocol query.
        let _inflight = InFlight::enter(&self.0.inflight);
        let sent = self
            .0
            .send_as(
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
                identity,
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
        // Do not let a busy unrelated worker keep a revoked socket alive. The
        // enqueued close and worker's periodic cleanup are the backstops.
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            self.0.send(
                &session.endpoint,
                Method::DELETE,
                &format!("/v1/sessions/{}", session.session_id),
                Bytes::new(),
            ),
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
    Extension(identity): Extension<Identity>,
    Json(request): Json<MaintenanceRequest>,
) -> Result<Response, ApiError> {
    admin_only(&principal, "run lake maintenance")?;
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
    platform_statement(&app, &identity, "lake_maintenance", statements.join(";\n")).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MintRequest {
    principal: String,
    #[serde(default)]
    expires_seconds: Option<i64>,
}

/// Operator-only actions. Credential management because a named credential that
/// could mint could quietly become every principal at once; budget writes because a
/// spend ceiling writable by the spenders it constrains is not a control; lake
/// maintenance because expiry and cleanup destroy what they tidy.
fn admin_only(principal: &Principal, what: &str) -> Result<(), ApiError> {
    match principal {
        Principal::Bootstrap => Ok(()),
        Principal::Named(_) => Err(ApiError::new(
            failure::FORBIDDEN,
            format!("only the bootstrap credential may {what}"),
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
    admin_only(&principal, "manage credentials")?;
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
    admin_only(&principal, "manage credentials")?;
    Ok(Json(app.authn.with_db(false, credentials::list).await?))
}

async fn revoke_credential(
    State(app): State<Gateway>,
    Extension(principal): Extension<Principal>,
    Path(credential_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin_only(&principal, "manage credentials")?;
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
    app.authn.cache.lock().await.invalidate();
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
    let fleet = app.fleet_snapshot().await;
    Ok(Json(json!({
        "workers": fleet.reported,
        "isolated_compute": app.isolation.is_some(),
        "observation_age_ms": u64::try_from(fleet.discovered.elapsed().as_millis()).unwrap_or(u64::MAX),
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
        observations: Arc::new(Mutex::new(HashMap::new())),
        discovering: Arc::new(Mutex::new(())),
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
    let cleanup = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            cleanup.expire().await;
        }
    });
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

    struct StatusServer {
        endpoint: String,
        reply: Arc<Mutex<(StatusCode, Value)>>,
        requests: Arc<AtomicUsize>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        close_status: Arc<Mutex<StatusCode>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for StatusServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn status_server(value: Value) -> StatusServer {
        let reply = Arc::new(Mutex::new((StatusCode::OK, value)));
        let requests = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let close_status = Arc::new(Mutex::new(StatusCode::OK));
        let handler_reply = reply.clone();
        let handler_requests = requests.clone();
        let handler_opens = opens.clone();
        let handler_closes = closes.clone();
        let handler_close_status = close_status.clone();
        let router = Router::new()
            .route(
                "/v1/worker",
                get(move || {
                    let reply = handler_reply.clone();
                    let requests = handler_requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        let (status, value) = reply.lock().await.clone();
                        (status, Json(value))
                    }
                }),
            )
            .route(
                "/v1/sessions",
                post(move || {
                    let opens = handler_opens.clone();
                    async move {
                        opens.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"session_id": Uuid::new_v4()}))
                    }
                }),
            )
            .route(
                "/v1/sessions/{id}",
                axum::routing::delete(move || {
                    let closes = handler_closes.clone();
                    let status = handler_close_status.clone();
                    async move {
                        closes.fetch_add(1, Ordering::SeqCst);
                        *status.lock().await
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        StatusServer {
            endpoint,
            reply,
            requests,
            opens,
            closes,
            close_status,
            task,
        }
    }

    fn status(state: &str) -> Value {
        let resources = Resources::default();
        json!({
            "worker_id": "test-worker",
            "worker_boot_id": "first-boot",
            "state": state,
            "ready": state == "ready",
            "draining": state == "draining",
            "cpu": resources.cpu,
            "memory_bytes": resources.memory_bytes,
            "active_queries": 0,
            "queued_queries": 0,
            "open_sessions": 0,
        })
    }

    fn test_gateway(endpoints: Vec<String>) -> Gateway {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let token: Arc<str> = "test-gateway-token".into();
        let dsn = "host=unused";
        Gateway {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(75))
                .retry(reqwest::retry::never())
                .build()
                .unwrap(),
            authn: Authn::new(token.clone(), dsn.into()),
            token,
            endpoints: Arc::new(endpoints),
            fleet: Arc::new(Mutex::new(None)),
            observations: Arc::new(Mutex::new(HashMap::new())),
            discovering: Arc::new(Mutex::new(())),
            routes: Arc::new(Mutex::new(HashMap::new())),
            capacity: Arc::new(Semaphore::new(64)),
            history: crate::history::Reader::new(dsn.into()),
            usage: crate::usage::Reader::new(
                dsn.into(),
                crate::pricing::RateCard {
                    version: "test".into(),
                    compute_micro_usd_per_unit_hour: 100_000,
                },
            ),
            isolation: None,
            draining: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn issued_identity() -> Identity {
        Identity {
            principal: "analyst".into(),
            credential_id: Some(Uuid::new_v4().to_string()),
            expires_at: Some(SystemTime::now() + Duration::from_secs(300)),
        }
    }

    #[test]
    fn revocation_prevents_a_previously_started_lookup_from_refilling_the_cache() {
        let mut cache = AuthCache::default();
        let identity = issued_identity();
        let lookup_generation = cache.generation;
        cache.invalidate();
        assert!(!cache.publish(
            lookup_generation,
            identity.clone(),
            Instant::now(),
            Some("old-digest".into())
        ));
        assert!(
            cache
                .get(identity.credential_id.as_ref().unwrap())
                .is_none()
        );
        assert!(cache.digests.is_empty());
        let replacement = issued_identity();
        assert!(cache.publish(
            cache.generation,
            replacement.clone(),
            Instant::now(),
            Some("new-digest".into())
        ));
        assert_eq!(
            cache.get(replacement.credential_id.as_ref().unwrap()),
            Some(replacement)
        );
        assert!(
            cache
                .get(identity.credential_id.as_ref().unwrap())
                .is_none()
        );
    }

    #[test]
    fn auth_cache_never_extends_credential_expiry_or_the_lookup_freshness_window() {
        let mut cache = AuthCache::default();
        let mut identity = issued_identity();
        identity.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
        assert!(!cache.publish(cache.generation, identity.clone(), Instant::now(), None));
        // Even a fresh positive entry must obey the credential's own deadline.
        let id = identity.credential_id.clone().unwrap();
        cache
            .identities
            .insert(id.clone(), (identity, Instant::now()));
        assert!(cache.get(&id).is_none());
        let valid = issued_identity();
        let id = valid.credential_id.clone().unwrap();
        assert!(cache.publish(cache.generation, valid, Instant::now() - Authn::FRESH, None));
        assert!(
            cache.get(&id).is_none(),
            "finishing a lookup restarted its cache window"
        );
    }

    #[tokio::test]
    async fn expired_credentials_are_refused_before_an_internal_request_is_sent() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let mut expired = issued_identity();
        expired.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
        let refused = app
            .send_as(
                &server.endpoint,
                Method::POST,
                "/v1/sessions",
                Bytes::from_static(b"{}"),
                &expired,
            )
            .await
            .unwrap_err();
        assert_eq!(refused.0.kind, "unauthorized");
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn internal_identity_headers_come_from_authentication_not_query_context() {
        let app = test_gateway(Vec::new());
        let identity = issued_identity();
        let request = authenticated_request(app.client.post("http://localhost/v1/sessions/s/queries")
            .json(&json!({"context": {"principal": "bootstrap-admin", "credential_id": "forged"}})), &identity)
            .build().unwrap();
        assert_eq!(request.headers()[credentials::PRINCIPAL_HEADER], "analyst");
        assert_eq!(
            request.headers()[credentials::CREDENTIAL_HEADER],
            identity.credential_id.unwrap()
        );
        let bootstrap = authenticated_request(
            app.client.post("http://localhost/v1/sessions"),
            &Identity::bootstrap(),
        )
        .build()
        .unwrap();
        assert!(
            !bootstrap
                .headers()
                .contains_key(credentials::CREDENTIAL_HEADER)
        );
    }

    async fn insert_test_route(
        app: &Gateway,
        endpoint: String,
        identity: Identity,
        touched: Instant,
    ) -> Uuid {
        let id = Uuid::new_v4();
        app.routes.lock().await.insert(
            id,
            Route {
                endpoint,
                principal: identity.principal.clone().into(),
                identity,
                touched,
                active: Arc::new(AtomicUsize::new(0)),
                closing: false,
                pod: None,
                _permit: app.capacity.clone().try_acquire_owned().unwrap(),
            },
        );
        id
    }

    #[tokio::test]
    async fn cleanup_waits_for_active_requests_and_a_successful_worker_close() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let id = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now() - Duration::from_secs(301),
        )
        .await;
        let active = app.routes.lock().await[&id].active.clone();
        let guard = InFlight::enter(&active);
        app.expire().await;
        assert_eq!(server.closes.load(Ordering::SeqCst), 0);
        drop(guard);
        *server.close_status.lock().await = StatusCode::SERVICE_UNAVAILABLE;
        app.expire().await;
        assert!(app.routes.lock().await[&id].closing);
        assert_eq!(app.capacity.available_permits(), 63);
        assert!(app.query_route(id, &Identity::bootstrap()).await.is_err());
        *server.close_status.lock().await = StatusCode::OK;
        app.expire().await;
        assert!(!app.routes.lock().await.contains_key(&id));
        assert_eq!(app.capacity.available_permits(), 64);
        assert_eq!(server.closes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_replacement_credential_preserves_principal_ownership_and_claims_cleanup() {
        let app = test_gateway(Vec::new());
        let old = issued_identity();
        let replacement = issued_identity();
        let id = insert_test_route(&app, "http://unused".into(), old, Instant::now()).await;
        {
            let mut cache = app.authn.cache.lock().await;
            let generation = cache.generation;
            assert!(cache.publish(generation, replacement.clone(), Instant::now(), None));
        }
        let (_, guard) = app.query_route(id, &replacement).await.unwrap();
        assert_eq!(app.routes.lock().await[&id].identity, replacement);
        app.expire().await;
        assert!(!app.routes.lock().await[&id].closing);
        drop(guard);
    }

    #[tokio::test]
    async fn a_partial_fleet_failure_preserves_the_last_successful_observation() {
        let healthy = status_server(status("ready")).await;
        let failing = status_server(status("ready")).await;
        let app = test_gateway(vec![healthy.endpoint.clone(), failing.endpoint.clone()]);
        let first = app.fleet_snapshot().await;
        let previous = first.reported[1].clone();
        assert!(previous["last_success_at"].as_u64().unwrap() > 0);

        failing.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        *app.fleet.lock().await = None;
        let Json(report) = workers(State(app.clone())).await.unwrap();
        assert_eq!(report["workers"].as_array().unwrap().len(), 2);
        assert_eq!(report["workers"][0]["state"], "ready");
        let unavailable = &report["workers"][1];
        assert_eq!(unavailable["state"], "unreachable");
        assert_eq!(unavailable["ready"], false);
        assert_eq!(unavailable["last_success_at"], previous["last_success_at"]);
        assert_eq!(unavailable["last_observation"], previous);
        assert_eq!(
            app.endpoints_for(Resources::default()).await.unwrap(),
            std::slice::from_ref(&healthy.endpoint)
        );

        // Consecutive failures retain the successful observation, not a nested
        // history of earlier failures or counters eligible for placement.
        *app.fleet.lock().await = None;
        let again = app.fleet_snapshot().await;
        assert_eq!(again.reported[1]["last_observation"], previous);
        assert_eq!(again.unreachable, std::slice::from_ref(&failing.endpoint));

        let mut restarted = status("ready");
        restarted["worker_boot_id"] = json!("second-boot");
        *failing.reply.lock().await = (StatusCode::OK, restarted);
        *app.fleet.lock().await = None;
        let recovered = app.fleet_snapshot().await;
        assert!(recovered.unreachable.is_empty());
        assert_eq!(recovered.reported[1]["worker_boot_id"], "second-boot");
        assert_eq!(recovered.reported[1]["worker_id"], previous["worker_id"]);
        assert!(recovered.reported[1].get("last_observation").is_none());
    }

    #[tokio::test]
    async fn fresh_negative_discovery_and_missing_sizes_are_cached() {
        let failing = status_server(status("ready")).await;
        failing.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        let app = test_gateway(vec![failing.endpoint.clone()]);
        let initial = app.fleet_snapshot().await;
        assert_eq!(initial.reported[0]["state"], "unreachable");
        assert!(initial.reported[0]["last_success_at"].is_null());
        assert!(initial.reported[0]["last_observation"].is_null());
        for _ in 0..3 {
            assert!(app.endpoints_for(Resources::default()).await.is_err());
        }
        assert_eq!(failing.requests.load(Ordering::SeqCst), 1);

        let healthy = status_server(status("ready")).await;
        let app = test_gateway(vec![healthy.endpoint.clone()]);
        let absent = Resources {
            cpu: 4,
            ..Resources::default()
        };
        for _ in 0..3 {
            assert!(app.endpoints_for(absent).await.is_err());
        }
        assert_eq!(healthy.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reachable_starting_and_draining_workers_are_reported_but_not_selected() {
        let starting = status_server(status("starting")).await;
        let mut draining_status = status("draining");
        // Readiness may still reflect completed startup while drain is active.
        draining_status["ready"] = json!(true);
        let draining = status_server(draining_status).await;
        let mut busy = status("ready");
        busy["active_queries"] = json!(1);
        busy["queued_queries"] = json!(4);
        let healthy = status_server(busy).await;
        let app = test_gateway(vec![
            starting.endpoint.clone(),
            draining.endpoint.clone(),
            healthy.endpoint.clone(),
        ]);
        let snapshot = app.fleet_snapshot().await;
        assert!(snapshot.unreachable.is_empty());
        assert_eq!(snapshot.reported[0]["state"], "starting");
        assert_eq!(snapshot.reported[1]["state"], "draining");
        assert_eq!(
            app.endpoints_for(Resources::default()).await.unwrap(),
            std::slice::from_ref(&healthy.endpoint)
        );
    }

    #[tokio::test]
    async fn incomplete_or_malformed_ready_reports_cannot_receive_sessions() {
        let malformed = status_server(status("ready")).await;
        let mut busy = status("ready");
        busy["state"] = json!("busy");
        busy["active_queries"] = json!(1);
        let healthy = status_server(busy).await;
        let app = test_gateway(vec![malformed.endpoint.clone(), healthy.endpoint.clone()]);
        let mut cases = Vec::new();
        for key in [
            "ready",
            "draining",
            "state",
            "active_queries",
            "queued_queries",
            "open_sessions",
        ] {
            let mut missing = status("ready");
            missing.as_object_mut().unwrap().remove(key);
            cases.push((format!("missing {key}"), missing));
        }
        for (key, value) in [
            ("ready", json!("true")),
            ("draining", json!("false")),
            ("state", json!("unknown")),
            ("state", json!("starting")),
            ("active_queries", json!(-1)),
            ("queued_queries", json!("0")),
            ("open_sessions", json!(null)),
        ] {
            let mut invalid = status("ready");
            invalid[key] = value;
            cases.push((format!("invalid {key}: {}", invalid[key]), invalid));
        }
        for (description, value) in cases {
            malformed.reply.lock().await.1 = value;
            *app.fleet.lock().await = None;
            assert_eq!(
                app.endpoints_for(Resources::default()).await.unwrap(),
                std::slice::from_ref(&healthy.endpoint),
                "{description} was treated as eligible idle capacity"
            );
        }
    }

    #[tokio::test]
    async fn sequential_opens_spread_without_reprobing_a_failed_sibling() {
        let first = status_server(status("ready")).await;
        let second = status_server(status("ready")).await;
        let failed = status_server(status("ready")).await;
        failed.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        let app = test_gateway(vec![
            first.endpoint.clone(),
            second.endpoint.clone(),
            failed.endpoint.clone(),
        ]);
        let initial = app.fleet_snapshot().await;
        assert_eq!(initial.unreachable, std::slice::from_ref(&failed.endpoint));
        // Keep this test within one cache generation independently of machine
        // scheduling. Discarding the cache after an open still triggers a probe.
        app.fleet.lock().await.as_mut().unwrap().discovered =
            Instant::now() + Duration::from_secs(60);
        let mut placements = Vec::new();
        for _ in 0..4 {
            match app
                .open_persistent(Resources::default(), b"{}".to_vec(), &Identity::bootstrap())
                .await
                .unwrap()
            {
                OpenOutcome::Opened { endpoint, upstream } => {
                    placements.push(endpoint);
                    assert!(upstream.json::<Value>().await.unwrap()["session_id"].is_string());
                }
                OpenOutcome::Refused(_) => panic!("healthy workers refused an open"),
            }
        }
        assert_eq!(
            placements,
            [
                first.endpoint.as_str(),
                second.endpoint.as_str(),
                first.endpoint.as_str(),
                second.endpoint.as_str(),
            ]
        );
        for server in [&first, &second, &failed] {
            assert_eq!(server.requests.load(Ordering::SeqCst), 1);
        }
        assert_eq!(first.opens.load(Ordering::SeqCst), 2);
        assert_eq!(second.opens.load(Ordering::SeqCst), 2);
        assert_eq!(failed.opens.load(Ordering::SeqCst), 0);
        let Json(report) = workers(State(app)).await.unwrap();
        // Local tie-breaker adjustments must not pretend to be worker samples.
        assert_eq!(report["workers"], json!(initial.reported));
    }

    #[tokio::test]
    async fn excluding_a_transient_worker_preserves_the_waitable_open_refusal() {
        let server = status_server(status("draining")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let error = app.endpoints_for(Resources::default()).await.unwrap_err();
        assert_eq!(error.0.kind, "draining");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);

        // A rollout can temporarily remove every Service endpoint between the
        // draining process and its replacement. This is still pre-admission.
        server.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        *app.fleet.lock().await = None;
        let error = app.endpoints_for(Resources::default()).await.unwrap_err();
        assert_eq!(error.0.kind, "no_ready_worker");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);
        *server.reply.lock().await = (StatusCode::OK, status("unavailable"));
        *app.fleet.lock().await = None;
        let error = app.endpoints_for(Resources::default()).await.unwrap_err();
        assert_eq!(error.0.kind, "no_ready_worker");
        assert!(error.0.retry_safe);

        let mut starting = status("starting");
        starting.as_object_mut().unwrap().remove("cpu");
        starting.as_object_mut().unwrap().remove("memory_bytes");
        server.reply.lock().await.1 = starting;
        *app.fleet.lock().await = None;
        let error = app.endpoints_for(Resources::default()).await.unwrap_err();
        assert_eq!(error.0.kind, "starting");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);

        server.reply.lock().await.1 = status("ready");
        *app.fleet.lock().await = None;
        assert_eq!(
            app.endpoints_for(Resources::default()).await.unwrap(),
            std::slice::from_ref(&server.endpoint)
        );
        // Unknown outcomes after forwarding retain their conservative meaning.
        assert!(!ApiError::internal().0.retry_safe);
    }

    #[tokio::test]
    async fn discovery_deadline_includes_a_stalled_response_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // A raw peer intentionally sends valid headers and an incomplete body.
        // This distinguishes the discovery bound from reqwest's 75-second bound.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sent, received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            let _ = sent.send(());
            std::future::pending::<()>().await;
            drop(stream);
        });
        let app = test_gateway(vec![endpoint.clone()]);
        let probe = tokio::spawn(async move { app.worker_info(&endpoint).await });
        received.await.unwrap();
        // Allow the local socket to deliver its headers before advancing time;
        // the test spends milliseconds rather than the discovery deadline.
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!probe.is_finished());
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        let outcome = tokio::time::timeout(Duration::from_secs(1), probe).await;
        server.abort();
        assert!(
            outcome
                .expect("discovery waited beyond its deadline")
                .unwrap()
                .is_err()
        );
    }

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
        assert!(admin_only(&Principal::Bootstrap, "manage credentials").is_ok());
        let refused =
            admin_only(&Principal::Named("analyst".into()), "set budget limits").unwrap_err();
        assert_eq!(refused.0.kind, "forbidden");
        assert!(!refused.0.retry_safe);
        assert_eq!(refused.0.http, StatusCode::FORBIDDEN);
        // The refusal names the action, so a caller knows which lever was refused.
        assert!(refused.1.contains("set budget limits"), "{}", refused.1);
    }

    #[test]
    fn failover_is_earned_only_by_a_worker_condition_that_ran_nothing() {
        // The three conditions that are about the worker, not the request.
        for kind in ["capacity", "draining", "starting"] {
            let refusal = json!({"failure_kind": kind, "retry_safe": true});
            assert!(
                failover_worthy(StatusCode::SERVICE_UNAVAILABLE, &refusal),
                "{kind}"
            );
        }
        // Retry-safe but about the deployment, not this worker: every sibling would
        // refuse identically, so trying one is noise.
        assert!(!failover_worthy(
            StatusCode::PAYMENT_REQUIRED,
            &json!({"failure_kind": "budget_exhausted", "retry_safe": true})
        ));
        // Not retry-safe, unclassified, and success are never failover grounds.
        assert!(!failover_worthy(
            StatusCode::BAD_REQUEST,
            &json!({"failure_kind": "invalid_request", "retry_safe": false})
        ));
        assert!(!failover_worthy(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({})
        ));
        assert!(!failover_worthy(
            StatusCode::OK,
            &json!({"failure_kind": "capacity", "retry_safe": true})
        ));
    }

    #[test]
    fn routing_uses_execution_pressure_before_sessions_and_excludes_unready_workers() {
        let small = Resources::default();
        let make = |endpoint: &str| WorkerInfo {
            endpoint: endpoint.into(),
            cpu: small.cpu,
            memory_bytes: small.memory_bytes,
            ready: true,
            active_queries: 0,
            queued_queries: 0,
            open_sessions: 0,
        };
        let mut workers = vec![
            make("first"),
            make("second"),
            make("other-size"),
            make("draining"),
        ];
        workers[2].cpu = 4;
        workers[3].ready = false;
        assert_eq!(ranked_endpoints(&workers, small), ["first", "second"]);
        // An idle pool with many sessions beats a busy worker with one session.
        workers[0].active_queries = 1;
        workers[0].open_sessions = 1;
        workers[1].open_sessions = 20;
        assert_eq!(ranked_endpoints(&workers, small), ["second", "first"]);
        // Queued work counts even between statements; open sessions break ties.
        workers[0].active_queries = 0;
        workers[0].queued_queries = 2;
        workers[1].active_queries = 1;
        assert_eq!(ranked_endpoints(&workers, small), ["second", "first"]);
        workers[0].queued_queries = 1;
        assert_eq!(ranked_endpoints(&workers, small), ["first", "second"]);
        workers[0].ready = false;
        workers[1].ready = false;
        assert!(ranked_endpoints(&workers, small).is_empty());
    }

    #[test]
    fn a_session_answers_only_to_its_owner_and_the_bootstrap() {
        let owner = Principal::Named("analyst".into());
        let intruder = Principal::Named("intruder".into());
        assert!(owner.may_act_on("analyst"));
        assert!(!intruder.may_act_on("analyst"));
        // The operator can cancel or close anyone's runaway session.
        assert!(Principal::Bootstrap.may_act_on("analyst"));
        // And nobody impersonates the bootstrap by name: minting refuses the name,
        // so a route owned by it can only have been opened with the real token.
        assert!(!owner.may_act_on(credentials::BOOTSTRAP_PRINCIPAL));
    }

    #[test]
    fn the_bootstrap_resolves_by_comparison_alone() {
        // No database behind this Authn; reaching for one would hang the test,
        // so passing proves the bootstrap path never needs it.
        let authn = Authn::new("the-bootstrap-token".into(), "host=nowhere".into());
        let resolved = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(authn.authenticate("the-bootstrap-token"))
            .unwrap();
        assert!(resolved.is_bootstrap());
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
