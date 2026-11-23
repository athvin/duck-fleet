//! Pins each session to one configured worker. It never retries or reroutes SQL.
use crate::{
    ApiError,
    authz::{self, Action, TrustedOperation},
    compute::{Authority, ForwardedSessionRequest, Mode, Placement, Resources, SessionRequest},
    credentials::{self, ActorKind, Identity},
    failure,
    isolated::Isolation,
    pools::PoolRegistry,
    query::{Acceptance, Protocol, QueryRequest},
    queue::{self, PolicyPublication},
};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
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
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

#[path = "gateway_jobs.rs"]
mod job_api;

#[derive(Clone)]
enum Cleanup {
    Credential(Option<String>),
    Idle,
}

struct Route {
    endpoint: String,
    touched: Instant,
    /// Immutable workload ownership; observation grants never transfer a session.
    owner_id: Option<String>,
    placement: Placement,
    operation: TrustedOperation,
    /// The last authorized request may rotate credentials without changing owner.
    identity: Identity,
    active: Arc<AtomicUsize>,
    closing: bool,
    cleanup: Option<Cleanup>,
    /// Keep the authorizing user on explicit-close retries; never promote a
    /// lost acknowledgement into an internal-service privilege.
    close_identity: Option<Identity>,
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
    disk_bytes: Option<u64>,
    pool: Option<String>,
    max_sessions: Option<u64>,
    ready: bool,
    active_queries: u64,
    queued_queries: u64,
    open_sessions: u64,
}
impl WorkerInfo {
    fn serves(&self, wanted: Resources, pool: &str, legacy: bool) -> bool {
        self.cpu == wanted.cpu
            && self.memory_bytes == wanted.memory_bytes
            && self.disk_bytes == Some(wanted.disk_bytes)
            && (self.pool.as_deref() == Some(pool) || (legacy && self.pool.is_none()))
    }

    fn session_limit(&self, configured: usize, legacy: bool) -> Option<u64> {
        let reported = self.max_sessions.or(legacy.then_some(32))?;
        (reported > 0 && (legacy || reported == configured as u64)).then_some(reported)
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
    revision: String,
    workers: Vec<WorkerInfo>,
    unreachable: Vec<String>,
    discovered: Instant,
    reported: Vec<Value>,
}

enum SessionOutcome {
    Opened {
        endpoint: String,
        id: Uuid,
        info: Value,
    },
    Refused(Response),
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
fn ranked_endpoints(
    workers: &[WorkerInfo],
    wanted: Resources,
    pool: &str,
    endpoints: &[String],
    max_sessions: usize,
    legacy: bool,
) -> Vec<String> {
    let mut matches: Vec<&WorkerInfo> = workers
        .iter()
        .filter(|w| {
            w.ready
                && endpoints.contains(&w.endpoint)
                && w.serves(wanted, pool, legacy)
                && w.session_limit(max_sessions, legacy)
                    .is_some_and(|limit| w.open_sessions < limit)
        })
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

fn system_token(headers: &axum::http::HeaderMap) -> Result<String, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty() && v.len() <= 8192)
        .map(str::to_owned)
        .ok_or_else(invalid_credential)
}
async fn system_query(
    State(app): State<Gateway>,
    ConnectInfo(connection): ConnectInfo<crate::http_transport::Connection>,
    request: axum::extract::Request,
) -> Result<crate::system_query::SystemResult, ApiError> {
    let token = system_token(request.headers())?;
    app.system
        .http(&token, request.into_body())
        .await
        .map(|result| result.with_http_delivery(&connection))
}
async fn system_schema(
    State(app): State<Gateway>,
    ConnectInfo(connection): ConnectInfo<crate::http_transport::Connection>,
    headers: axum::http::HeaderMap,
) -> Result<crate::system_query::SystemResult, ApiError> {
    let token = system_token(&headers)?;
    app.system
        .schema(&token)
        .await
        .map(|result| result.with_http_delivery(&connection))
}

fn authenticated_request(
    request: reqwest::RequestBuilder,
    identity: &Identity,
) -> reqwest::RequestBuilder {
    let kind = match identity.kind {
        ActorKind::Named => "named",
        ActorKind::Bootstrap => "bootstrap",
        ActorKind::Internal => "internal",
    };
    let request = request
        .header(credentials::PRINCIPAL_HEADER, &identity.principal)
        .header(credentials::ACTOR_KIND_HEADER, kind);
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
    pools: Arc<RwLock<Arc<PoolRegistry>>>,
    pools_file: Option<Arc<PathBuf>>,
    publishing: Arc<Mutex<()>>,
    controls: Arc<Semaphore>,
    artifact_store: Option<crate::artifacts::Store>,
    result_slots: Arc<Semaphore>,
    dispatcher_owner: Uuid,
    max_sessions: usize,
    fleet: Arc<Mutex<Option<Fleet>>>,
    /// Last successful observations remain available for diagnosis, never routing.
    observations: Arc<Mutex<HashMap<String, Value>>>,
    /// Held for the duration of one `discover`, so concurrent cache misses collapse
    /// into a single concurrent fleet probe.
    discovering: Arc<Mutex<()>>,
    routes: Arc<Mutex<HashMap<Uuid, Route>>>,
    capacity: Arc<Semaphore>,
    history: crate::history::Reader,
    requests: crate::requests::Reader,
    usage: crate::usage::Reader,
    system: crate::system_query::Service,
    /// `None` where this deployment cannot create pods, which is most of them. The
    /// capability is absent rather than disabled: there is no code path that would
    /// serve an isolated request from a shared worker if it were off.
    isolation: Option<Arc<Isolation>>,
    retired_pods: Arc<Mutex<HashMap<String, Instant>>>,
    rate: crate::pricing::RateCard,
    /// Set when a termination signal arrives, exactly as the worker does it: the
    /// probe reads it to leave the Service, and `open` reads it to refuse new
    /// sessions while requests already in flight run on.
    draining: Arc<AtomicBool>,
    /// Requests currently inside a `/v1` handler. What the drain waits on — the
    /// gateway has no engine slot to watch, so this count is its notion of busy.
    inflight: Arc<AtomicUsize>,
    authn: Arc<Authn>,
    /// Pure routing fixtures inject a resolved policy; production always reads
    /// the current database policy and persists external-operation audits.
    #[cfg(test)]
    resolved_access: Option<authz::Access>,
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
    fn registry(&self) -> Arc<PoolRegistry> {
        self.pools.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Bounded control capacity independent of authentication and engine waits.
    async fn control_db<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut postgres::Client) -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let permit = self
            .controls
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "control capacity reached"))?;
        let dsn = self.authn.dsn.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
            config.connect_timeout(Duration::from_secs(3));
            config.tcp_user_timeout(Duration::from_secs(3));
            config.keepalives_idle(Duration::from_secs(2));
            config.keepalives_interval(Duration::from_secs(1));
            config.keepalives_retries(2);
            let mut db = config.connect(NoTls).map_err(|_| ApiError::internal())?;
            db.batch_execute("SET statement_timeout='3s'; SET lock_timeout='250ms'; SET idle_in_transaction_session_timeout='3s'")
                .map_err(|_| ApiError::internal())?;
            operation(&mut db)
        }).await.map_err(|_| ApiError::internal())?
    }

    async fn authorize_action(
        &self,
        identity: &Identity,
        action: Action,
        owner: Option<String>,
    ) -> Result<authz::Access, ApiError> {
        #[cfg(test)]
        if let Some(access) = &self.resolved_access {
            access.require(action, owner.as_deref())?;
            return Ok(access.clone());
        }
        let identity = identity.clone();
        self.control_db(move |db| authz::authorize_action(db, &identity, action, owner.as_deref()))
            .await
    }

    async fn authorize_execution(
        &self,
        identity: &Identity,
        owner: &str,
        placement: &Placement,
        operation: &TrustedOperation,
    ) -> Result<authz::Access, ApiError> {
        #[cfg(test)]
        if let Some(access) = &self.resolved_access {
            access.require_execution(
                access.actor.principal_id.as_deref(),
                &serde_json::to_value(placement).map_err(|_| ApiError::internal())?,
                operation,
            )?;
            return Ok(access.clone());
        }
        let identity = identity.clone();
        let owner = owner.to_owned();
        let compute = serde_json::to_value(placement).map_err(|_| ApiError::internal())?;
        let operation = operation.clone();
        self.control_db(move |db| {
            authz::authorize_execution(db, &identity, &owner, &compute, &operation)
        })
        .await
    }

    async fn authorize_owned_execution(
        &self,
        identity: &Identity,
        owner: Option<String>,
        placement: &Placement,
        operation: &TrustedOperation,
    ) -> Result<authz::Access, ApiError> {
        let compute = serde_json::to_value(placement).map_err(|_| ApiError::internal())?;
        #[cfg(test)]
        if let Some(access) = &self.resolved_access {
            access.require_execution(owner.as_deref(), &compute, operation)?;
            return Ok(access.clone());
        }
        let identity = identity.clone();
        let operation = operation.clone();
        self.control_db(move |db| {
            authz::authorize_execution_for_owner(
                db,
                &identity,
                owner.as_deref(),
                &compute,
                &operation,
            )
        })
        .await
    }

    async fn start_execution_operation(
        &self,
        identity: &Identity,
        placement: &Placement,
        operation: &TrustedOperation,
        correlation: Uuid,
    ) -> Result<authz::Access, ApiError> {
        #[cfg(test)]
        if self.resolved_access.is_some() {
            return self
                .authorize_execution(identity, &identity.principal, placement, operation)
                .await;
        }
        let identity = identity.clone();
        let compute = serde_json::to_value(placement).map_err(|_| ApiError::internal())?;
        let operation = operation.clone();
        self.control_db(move |db| {
            let mut tx = db.transaction().map_err(|_| ApiError::internal())?;
            authz::bounded(&mut tx).map_err(|_| ApiError::internal())?;
            authz::lock_actor_in(&mut tx, &identity)?;
            let access = authz::resolve_in(&mut tx, &identity)?;
            let allowed = access.require_execution(
                access.actor.principal_id.as_deref(),
                &compute,
                &operation,
            );
            crate::audit::append_in(
                &mut tx,
                &crate::audit::Event::new(
                    &access,
                    operation.action(),
                    "session",
                    Some(&correlation.to_string()),
                    if allowed.is_ok() { "started" } else { "denied" },
                    "session_open",
                    Some(&correlation.to_string()),
                ),
            )
            .map_err(|_| ApiError::internal())?;
            tx.commit().map_err(|_| ApiError::internal())?;
            allowed?;
            Ok(access)
        })
        .await
    }

    async fn start_action_operation(
        &self,
        identity: &Identity,
        action: Action,
        target: &'static str,
        correlation: Uuid,
    ) -> Result<authz::Access, ApiError> {
        #[cfg(test)]
        if self.resolved_access.is_some() {
            return self.authorize_action(identity, action, None).await;
        }
        let identity = identity.clone();
        self.control_db(move |db| {
            let mut tx = db.transaction().map_err(|_| ApiError::internal())?;
            authz::bounded(&mut tx).map_err(|_| ApiError::internal())?;
            authz::lock_actor_in(&mut tx, &identity)?;
            let access = authz::resolve_in(&mut tx, &identity)?;
            let allowed = access.require(action, None);
            crate::audit::append_in(
                &mut tx,
                &crate::audit::Event::new(
                    &access,
                    action,
                    "operation",
                    Some(target),
                    if allowed.is_ok() { "started" } else { "denied" },
                    "operation_authorization",
                    Some(&correlation.to_string()),
                ),
            )
            .map_err(|_| ApiError::internal())?;
            tx.commit().map_err(|_| ApiError::internal())?;
            allowed?;
            Ok(access)
        })
        .await
    }

    async fn audit_outcome(
        &self,
        access: authz::Access,
        action: Action,
        target: &'static str,
        correlation: Uuid,
        succeeded: Option<bool>,
    ) {
        #[cfg(test)]
        if self.resolved_access.is_some() {
            return;
        }
        if let Err(error) = self
            .control_db(move |db| {
                crate::audit::append(
                    db,
                    &access,
                    action,
                    target,
                    match succeeded {
                        Some(true) => "succeeded",
                        Some(false) => "failed",
                        None => "unknown",
                    },
                    "operation_outcome",
                    Some(&correlation.to_string()),
                )
                .map_err(|_| ApiError::internal())
            })
            .await
        {
            tracing::error!(%correlation, kind=error.0.kind, "operation outcome audit was not confirmed; start event remains authoritative");
        }
    }

    async fn publish_policy(&self, registry: &PoolRegistry) -> Result<(), ApiError> {
        let policies = registry.admission_policies();
        let job_policies = registry.job_policies();
        let publication = PolicyPublication {
            publication_id: Uuid::new_v4(),
            digest: credentials::digest(
                &serde_json::to_string(&(&policies, &job_policies))
                    .map_err(|_| ApiError::internal())?,
            ),
            policies,
        };
        let pending = publication.clone();
        let published = self
            .control_db(move |db| {
                queue::publish_policies_with_jobs(db, &pending, &job_policies)
                    .map_err(|_| ApiError::internal())
            })
            .await;
        if published.is_ok() {
            return Ok(());
        }
        // A lost commit acknowledgment must be confirmed before the corresponding
        // registry becomes visible. Policy rows may already have changed.
        if self
            .control_db(move |db| {
                queue::publication_confirmed(db, &publication).map_err(|_| ApiError::internal())
            })
            .await
            .unwrap_or(false)
        {
            return Ok(());
        }
        Err(ApiError::new(
            failure::POLICY_UNAVAILABLE,
            "admission policy publication was not confirmed; registry retained, database policy may already have changed; inspect state before retrying reload",
        ))
    }

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

    async fn discover(&self, registry: &PoolRegistry) -> Fleet {
        let endpoints: Vec<String> = registry
            .pools
            .values()
            .flat_map(|pool| pool.endpoints.iter().cloned())
            .collect();
        let mut probes = tokio::task::JoinSet::new();
        for (index, endpoint) in endpoints.iter().cloned().enumerate() {
            let app = self.clone();
            probes.spawn(async move {
                let result = app.worker_info(&endpoint).await;
                (index, result, observed_at())
            });
        }
        let mut ordered: Vec<Option<(Value, u64)>> = vec![None; endpoints.len()];
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
        observations.retain(|endpoint, _| endpoints.contains(endpoint));
        for (endpoint, value) in endpoints.iter().zip(ordered) {
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
                            disk_bytes: value["disk_bytes"].as_u64(),
                            pool: value["pool"].as_str().map(str::to_owned),
                            max_sessions: value["max_sessions"].as_u64(),
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
            revision: registry.revision.clone(),
            workers,
            unreachable,
            reported,
            discovered: Instant::now(),
        }
    }

    /// Shared by placement and inspection, with short-lived load observations.
    /// Unreachable workers do not discard healthy siblings' observations.
    async fn fleet_snapshot(&self, registry: &PoolRegistry) -> Fleet {
        let fresh = |fleet: &Option<Fleet>| {
            fleet
                .clone()
                .filter(|f| f.revision == registry.revision && f.discovered.elapsed() < FLEET_FRESH)
        };
        if let Some(fleet) = fresh(&*self.fleet.lock().await) {
            return fleet;
        }
        let _flight = self.discovering.lock().await;
        if let Some(fleet) = fresh(&*self.fleet.lock().await) {
            return fleet;
        }
        let discovered = self.discover(registry).await;
        *self.fleet.lock().await = Some(discovered.clone());
        discovered
    }

    /// New sessions use worker-observed pressure from every protocol. Existing
    /// sessions remain pinned even if another worker subsequently becomes idle.
    async fn endpoints_for(
        &self,
        registry: &PoolRegistry,
        placement: &Placement,
    ) -> Result<Vec<String>, ApiError> {
        let name = placement.pool.as_deref().ok_or_else(ApiError::internal)?;
        let pool = registry.pools.get(name).ok_or_else(ApiError::internal)?;
        let fleet = self.fleet_snapshot(registry).await;
        let candidates = ranked_endpoints(
            &fleet.workers,
            placement.resources,
            name,
            &pool.endpoints,
            pool.max_sessions_per_worker,
            registry.legacy,
        );
        if !candidates.is_empty() {
            return Ok(candidates);
        }
        // A different pool's starting or failed worker cannot determine this
        // selection's error. An endpoint is never borrowed from another pool.
        for report in &fleet.reported {
            if !report["endpoint"].as_str().is_some_and(|endpoint| {
                pool.endpoints.iter().any(|candidate| candidate == endpoint)
            }) {
                continue;
            }
            let matching = report["pool"].as_str() == Some(name)
                || (registry.legacy && report["pool"].is_null());
            let resources = report["cpu"].as_u64() == Some(u64::from(placement.resources.cpu))
                && report["memory_bytes"].as_u64() == Some(placement.resources.memory_bytes)
                && report["disk_bytes"].as_u64() == Some(placement.resources.disk_bytes);
            let refusal = match report["state"].as_str() {
                Some("draining") if matching && resources => Some(failure::DRAINING),
                Some("starting") if matching => Some(failure::STARTING),
                _ => None,
            };
            if let Some(refusal) = refusal {
                return Err(ApiError::new(
                    refusal,
                    format!(
                        "a worker in pool {name} is {}; no session was opened and no statement ran",
                        refusal.kind
                    ),
                ));
            }
        }
        let full = fleet.workers.iter().any(|worker| {
            worker.ready
                && pool.endpoints.contains(&worker.endpoint)
                && worker.serves(placement.resources, name, registry.legacy)
                && worker
                    .session_limit(pool.max_sessions_per_worker, registry.legacy)
                    .is_some()
        });
        Err(ApiError::new(
            if full {
                failure::CAPACITY
            } else {
                failure::NO_READY_WORKER
            },
            format!(
                "no eligible worker is available in pool {name}; no session was opened and no statement ran"
            ),
        ))
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
        registry: &PoolRegistry,
        placement: &Placement,
        body: Vec<u8>,
        identity: &Identity,
        operation: &TrustedOperation,
        open_span: Uuid,
    ) -> Result<OpenOutcome, ApiError> {
        let candidates = self.endpoints_for(registry, placement).await?;
        let total = candidates.len();
        for (position, endpoint) in candidates.into_iter().enumerate() {
            let last = position + 1 == total;
            self.authorize_execution(identity, &identity.principal, placement, operation)
                .await?;
            let request = self
                .client
                .request(Method::POST, format!("{endpoint}/v1/sessions"))
                .bearer_auth(&self.token)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-duckfleet-open-span-id", open_span.to_string())
                .header(
                    "x-duckfleet-operation",
                    serde_json::to_string(operation).map_err(|_| ApiError::internal())?,
                )
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
        self.send_as_protocol(
            endpoint,
            method,
            path,
            body,
            identity,
            (Protocol::Http, &TrustedOperation::Sql),
        )
        .await
    }

    async fn send_as_protocol(
        &self,
        endpoint: &str,
        method: Method,
        path: &str,
        body: Bytes,
        identity: &Identity,
        execution: (Protocol, &TrustedOperation),
    ) -> Result<reqwest::Response, ApiError> {
        let (protocol, operation) = execution;
        let timeout = if method == Method::POST && path.ends_with("/queries") {
            let request: crate::QueryRequest = serde_json::from_slice(&body)
                .map_err(|_| ApiError::bad("invalid query request"))?;
            request.validate()?;
            request.transport_timeout()
        } else {
            Duration::from_secs(75)
        };
        // Middleware ran before body extraction and route/provisioning waits.
        // Recheck here; the worker also checks at dequeue, before any SQL runs.
        self.authn.revalidate(identity).await?;
        authenticated_request(
            self.client
                .request(method, format!("{endpoint}{path}"))
                .timeout(timeout)
                .bearer_auth(&self.token)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-duckfleet-protocol", protocol.name())
                .header(
                    "x-duckfleet-operation",
                    serde_json::to_string(operation).map_err(|_| ApiError::internal())?,
                )
                .body(body),
            identity,
        )
        .send()
        .await
        .map_err(|_| ApiError::internal())
    }
    async fn route(
        &self,
        id: Uuid,
        identity: &Identity,
        action: Action,
    ) -> Result<String, ApiError> {
        let owner = self
            .routes
            .lock()
            .await
            .get(&id)
            .map(|route| route.owner_id.clone())
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        self.authorize_action(identity, action, owner).await?;
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&id)
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        route.touched = Instant::now();
        Ok(route.endpoint.clone())
    }

    async fn query_route(
        &self,
        id: Uuid,
        identity: &Identity,
    ) -> Result<(String, InFlight), ApiError> {
        let (owner, placement, operation) = self
            .routes
            .lock()
            .await
            .get(&id)
            .filter(|route| !route.closing || route.cleanup.is_some())
            .map(|route| {
                (
                    route.owner_id.clone(),
                    route.placement.clone(),
                    route.operation.clone(),
                )
            })
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        self.authorize_owned_execution(identity, owner, &placement, &operation)
            .await?;
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&id)
            .filter(|route| !route.closing || route.cleanup.is_some())
            .ok_or_else(|| ApiError::new(failure::UNKNOWN_SESSION, "unknown or expired session"))?;
        route.touched = Instant::now();
        Ok((route.endpoint.clone(), InFlight::enter(&route.active)))
    }

    async fn close_backend(
        &self,
        id: Uuid,
        endpoint: &str,
        cleanup: Option<&Cleanup>,
        identity: Option<&Identity>,
    ) -> Result<(StatusCode, Value), ApiError> {
        let mut request = self
            .client
            .delete(format!("{endpoint}/v1/sessions/{id}"))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(5));
        match cleanup {
            Some(Cleanup::Credential(id)) => {
                request = request.header("x-duckfleet-close-reason", "credential");
                if let Some(id) = id {
                    request = request.header("x-duckfleet-close-credential-id", id);
                }
            }
            Some(Cleanup::Idle) => {
                request = request.header("x-duckfleet-close-reason", "idle");
            }
            None => {}
        }
        if let Some(identity) = identity {
            request = authenticated_request(request, identity);
        }
        let response = request.send().await.map_err(|_| ApiError::internal())?;
        let status = response.status();
        let value = response.json().await.map_err(|_| ApiError::internal())?;
        Ok((status, value))
    }

    /// Retire a route only after a close acknowledgment or durable closed marker.
    /// Pair the cached increment on open with a decrement so session churn cannot
    /// exhaust the sampled capacity while the worker itself is idle.
    async fn retire_route(&self, id: Uuid) -> Option<String> {
        let route = self.routes.lock().await.remove(&id)?;
        if route.pod.is_none()
            && let Some(fleet) = self.fleet.lock().await.as_mut()
            && let Some(worker) = fleet
                .workers
                .iter_mut()
                .find(|w| w.endpoint == route.endpoint)
        {
            worker.open_sessions = worker.open_sessions.saturating_sub(1);
        }
        route.pod
    }

    /// Protocol-owned sessions retain their capacity until the worker confirms
    /// connection closure and durable settlement, even after a transport timeout.
    async fn close_internal(&self, id: Uuid) {
        let endpoint = {
            let mut routes = self.routes.lock().await;
            let Some(route) = routes.get_mut(&id) else {
                return;
            };
            route.closing = true;
            route.cleanup = None;
            route.close_identity = None;
            route.endpoint.clone()
        };
        if let Ok((status, value)) = self.close_backend(id, &endpoint, None, None).await
            && status.is_success()
            && value["closed"] == true
        {
            let pod = self.retire_route(id).await;
            self.release(pod).await;
        }
    }

    /// A worker closes revoked-policy sessions independently. Reclaim the route
    /// only after its durable closed marker proves the connection ended; a policy
    /// denial snapshot or an unreachable worker is never proof of closure.
    async fn reap_closed_routes(&self) {
        #[cfg(test)]
        if self.resolved_access.is_some() {
            return;
        }
        let candidates: Vec<_> = self
            .routes
            .lock()
            .await
            .iter()
            .filter(|(_, route)| route.active.load(Ordering::Acquire) == 0)
            .map(|(id, _)| id.to_string())
            .collect();
        if candidates.is_empty() {
            return;
        }
        let closed = self.control_db(move |db| {
            db.query("SELECT session_id FROM queue_sessions WHERE session_id=ANY($1) AND closed_at IS NOT NULL ORDER BY closed_at,session_id LIMIT 64", &[&candidates])
                .map(|rows| rows.iter().filter_map(|r| Uuid::parse_str(r.get::<_,&str>(0)).ok()).collect::<Vec<_>>())
                .map_err(|_| ApiError::internal())
        }).await;
        let Ok(closed) = closed else {
            return;
        };
        let mut pods = Vec::new();
        for id in closed {
            if let Some(pod) = self.retire_route(id).await {
                pods.push(pod);
            }
        }
        // Deletion retries already use the bounded retired-pod lane. Record all
        // confirmed closed pods, then let that lane handle a small batch per tick.
        let mut retired = self.retired_pods.lock().await;
        for pod in pods {
            retired.entry(pod).or_insert_with(Instant::now);
        }
    }

    /// Periodic, independent of new client traffic. Close on the worker before
    /// deleting an isolated pod, so cleanup never interrupts accepted active SQL.
    async fn expire(&self) {
        self.reap_closed_routes().await;
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
                    if !route.closing {
                        route.cleanup = Some(if invalid {
                            Cleanup::Credential(identity.credential_id.clone())
                        } else {
                            Cleanup::Idle
                        });
                    }
                    route.closing = true;
                    retire.push((
                        id,
                        route.endpoint.clone(),
                        route.cleanup.clone(),
                        route.close_identity.clone(),
                    ));
                }
            }
        }
        let mut closes = tokio::task::JoinSet::new();
        for (id, endpoint, reason, identity) in retire {
            let app = self.clone();
            closes.spawn(async move {
                match app
                    .close_backend(id, &endpoint, reason.as_ref(), identity.as_ref())
                    .await
                {
                    Ok((status, value)) if status.is_success() && value["closed"] == true => {
                        let pod = app.retire_route(id).await;
                        tokio::spawn(async move { app.release(pod).await });
                    }
                    Ok((status, value))
                        if status.is_success() && value["authority_changed"] == true =>
                    {
                        // Accepted replacement credentials or more recent durable
                        // activity invalidate this conditional cleanup snapshot.
                        if let Some(route) = app.routes.lock().await.get_mut(&id)
                            && route.cleanup.is_some()
                        {
                            route.closing = false;
                            route.cleanup = None;
                            route.close_identity = None;
                        }
                    }
                    Ok((StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN, _)) => {
                        // The user lost authority before close was accepted.
                        // Drop only that same intent, leaving ordinary expiry and
                        // a newly authorized explicit request available.
                        if let Some(route) = app.routes.lock().await.get_mut(&id)
                            && route.close_identity == identity
                            && identity.is_some()
                        {
                            route.closing = false;
                            route.close_identity = None;
                        }
                    }
                    _ => {} // Keep the same conditional intent for the next pass.
                }
            });
        }
        while closes.join_next().await.is_some() {}
    }

    /// Removes a session pod and settles whatever it was holding.
    ///
    /// Settlement runs on every path, not only the failures. Recovery touches only
    /// reservations still held, so a session that settled its own is unaffected —
    /// Retire only a known ended or abandoned isolated session. Failed deletion
    /// or bookkeeping remains tracked independently of client routes for retry.
    async fn release(&self, pod: Option<String>) {
        let (Some(pod), Some(isolation)) = (pod, self.isolation.as_ref()) else {
            return;
        };
        self.retired_pods
            .lock()
            .await
            .entry(pod.clone())
            .or_insert_with(Instant::now);
        if isolation.stop_confirmed(&pod).await {
            self.recover_retired(pod).await;
        }
    }

    async fn recover_retired(&self, pod: String) {
        let worker = pod.clone();
        let rate = self.rate.clone();
        match self
            .control_db(move |db| {
                queue::recover_isolated(db, &worker, &rate).map_err(|_| ApiError::internal())
            })
            .await
        {
            Ok(recovered) => {
                self.retired_pods.lock().await.remove(&pod);
                if recovered > 0 {
                    tracing::warn!(
                        pod,
                        recovered,
                        "recovered work after confirmed isolated pod removal"
                    );
                }
            }
            Err(error) => tracing::error!(
                pod,
                kind = error.0.kind,
                "retired isolated work remains pending recovery"
            ),
        }
    }

    async fn retry_retired(&self) {
        let pods = {
            let mut retired = self.retired_pods.lock().await;
            let mut ordered: Vec<_> = retired.iter().map(|(id, at)| (*at, id.clone())).collect();
            ordered.sort();
            let pods: Vec<_> = ordered.into_iter().take(4).map(|(_, id)| id).collect();
            for id in &pods {
                retired.insert(id.clone(), Instant::now());
            }
            pods
        };
        let mut pending = tokio::task::JoinSet::new();
        for pod in pods {
            let app = self.clone();
            pending.spawn(async move {
                app.release(Some(pod)).await;
            });
        }
        while pending.join_next().await.is_some() {}
    }
}
async fn response(upstream: reqwest::Response) -> Result<Response, ApiError> {
    let status = upstream.status();
    let mut headers = axum::http::HeaderMap::new();
    for name in [
        header::CONTENT_TYPE,
        header::HeaderName::from_static("x-query-id"),
        header::HeaderName::from_static("x-query-status-url"),
        header::HeaderName::from_static("x-query-acceptance"),
    ] {
        if let Some(value) = upstream.headers().get(&name) {
            headers.insert(name, value.clone());
        }
    }
    let body = upstream.bytes().await.map_err(|_| ApiError::internal())?;
    Ok((status, headers, body).into_response())
}
impl Gateway {
    async fn open_session(
        &self,
        request: SessionRequest,
        identity: &Identity,
    ) -> Result<SessionOutcome, ApiError> {
        self.open_operation(request, identity, TrustedOperation::Sql)
            .await
    }

    async fn open_operation(
        &self,
        request: SessionRequest,
        identity: &Identity,
        operation: TrustedOperation,
    ) -> Result<SessionOutcome, ApiError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(draining_refusal());
        }
        request.validate()?;
        // One immutable snapshot governs selection, discovery, and forwarding even
        // if the operator reloads the registry while this open is awaiting a worker.
        let registry = self.registry();
        let authority = if identity.kind == ActorKind::Named {
            let authority_identity = identity.clone();
            self.control_db(move |db| authz::execution_authority(db, &authority_identity))
                .await?
        } else {
            Authority::Writer
        };
        let placement = registry.resolve_for(&request.compute, authority)?;
        self.authorize_execution(identity, &identity.principal, &placement, &operation)
            .await?;
        let open_span = Uuid::new_v4();
        let access = self
            .start_execution_operation(identity, &placement, &operation, open_span)
            .await?;
        let action = operation.action();
        let principal = identity.principal.clone();
        let scope = placement
            .pool
            .as_ref()
            .map(|pool| format!("persistent:{pool}"))
            .unwrap_or_else(|| "isolated".into());
        let started = Instant::now();
        if let Err(error) = self
            .control_db(move |db| {
                queue::begin_open(db, open_span, &principal, &scope)
                    .map_err(|_| ApiError::internal())
            })
            .await
        {
            tracing::warn!(%open_span, kind=error.0.kind, "could not persist session open start; gateway timing remains unavailable");
        }
        let result = self
            .open_resolved(request, identity, registry, placement, operation, open_span)
            .await;
        let session_id = match &result {
            Ok(SessionOutcome::Opened { id, .. }) => Some(*id),
            _ => None,
        };
        let error = match &result {
            Ok(SessionOutcome::Opened { .. }) => None,
            Ok(SessionOutcome::Refused(reply)) => {
                Some(format!("worker refused session: {}", reply.status()))
            }
            Err(error) => Some(error.0.kind.to_owned()),
        };
        let audit_result = match &result {
            Ok(SessionOutcome::Opened { .. }) => Some(true),
            Ok(SessionOutcome::Refused(_)) => Some(false),
            Err(_) => None,
        };
        self.audit_outcome(access, action, "session_open", open_span, audit_result)
            .await;
        let elapsed_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        if let Err(error) = self
            .control_db(move |db| {
                queue::finish_open(db, open_span, session_id, elapsed_ms, error.as_deref())
                    .map_err(|_| ApiError::internal())
            })
            .await
        {
            tracing::warn!(%open_span, kind=error.0.kind, "could not persist session open outcome; gateway timing remains unavailable");
        }
        result
    }

    async fn open_resolved(
        &self,
        request: SessionRequest,
        identity: &Identity,
        registry: Arc<PoolRegistry>,
        placement: Placement,
        operation: TrustedOperation,
        open_span: Uuid,
    ) -> Result<SessionOutcome, ApiError> {
        // Body extraction may have outlived authentication. Refuse before spending
        // resources on isolated provisioning, then check again before worker open.
        let access = self
            .authorize_execution(identity, &identity.principal, &placement, &operation)
            .await?;
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::new(failure::CAPACITY, "gateway session limit reached"))?;
        // The principal rides the internal hop as a header the worker records at open:
        // trusted because the hop requires the internal token, and set here rather than
        // taken from the body so a caller cannot name someone else.
        let body = serde_json::to_vec(&ForwardedSessionRequest {
            version: 1,
            request,
            resolved: placement.clone(),
            registry_revision: registry.revision.clone(),
        })
        .map_err(|_| ApiError::internal())?;
        // An isolated session gets a pod of its own; a persistent one gets the
        // least-loaded worker already reporting these resources, with a full or draining
        // worker failed over to its sibling. Neither mode substitutes for the other.
        let (endpoint, pod, upstream) = match placement.mode {
            Mode::Persistent => {
                match self
                    .open_persistent(&registry, &placement, body, identity, &operation, open_span)
                    .await?
                {
                    OpenOutcome::Opened { endpoint, upstream } => (endpoint, None, upstream),
                    OpenOutcome::Refused(relay) => return Ok(SessionOutcome::Refused(relay)),
                }
            }
            Mode::Isolated => {
                let isolation = self
                    .isolation
                    .as_ref()
                    .ok_or_else(crate::compute::unavailable)?;
                let started = isolation
                    .start(placement.resources, placement.authority)
                    .await?;
                // From here a session pod exists, so every exit below releases it. The
                // worker's own response is still relayed unchanged on the way out: it
                // says why it refused, and replacing that with a gateway error would
                // lose the only useful detail.
                let pod = Some(started.name);
                if let Err(error) = self
                    .authorize_execution(identity, &identity.principal, &placement, &operation)
                    .await
                {
                    self.release(pod).await;
                    return Err(error);
                }
                let open_request = self
                    .client
                    .post(format!("{}/v1/sessions", started.endpoint))
                    .bearer_auth(&self.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-duckfleet-open-span-id", open_span.to_string())
                    .header(
                        "x-duckfleet-operation",
                        serde_json::to_string(&operation).map_err(|_| ApiError::internal())?,
                    )
                    .body(body);
                let upstream = match authenticated_request(open_request, identity)
                    .send()
                    .await
                    .map_err(|_| ApiError::internal())
                {
                    Ok(upstream) => upstream,
                    Err(error) => {
                        self.release(pod).await;
                        return Err(error);
                    }
                };
                if !upstream.status().is_success() {
                    let refused = response(upstream).await;
                    self.release(pod).await;
                    return refused.map(SessionOutcome::Refused);
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
            self.release(pod).await;
            return Err(ApiError::internal());
        };
        self.routes.lock().await.insert(
            id,
            Route {
                endpoint: endpoint.clone(),
                touched: Instant::now(),
                owner_id: access.actor.principal_id,
                placement,
                operation,
                identity: identity.clone(),
                active: Arc::new(AtomicUsize::new(0)),
                closing: false,
                cleanup: None,
                close_identity: None,
                pod,
                _permit: permit,
            },
        );
        Ok(SessionOutcome::Opened { endpoint, id, info })
    }
}

async fn open(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request = if body.is_empty() {
        SessionRequest::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ApiError::bad(format!("invalid session request: {e}")))?
    };
    match app.open_session(request, &identity).await? {
        SessionOutcome::Opened { info, .. } => Ok(Json(info).into_response()),
        SessionOutcome::Refused(response) => Ok(response),
    }
}
fn query_refusal(error: ApiError, request_id: Uuid, acceptance: Acceptance) -> Response {
    let status_url = format!("/v1/requests/{request_id}");
    (
        error.0.http,
        [
            ("x-query-id", request_id.to_string()),
            ("x-query-status-url", status_url.clone()),
            ("x-query-acceptance", acceptance.as_str().to_owned()),
        ],
        Json(json!({"error":error.1,"failure_kind":error.0.kind,
        "retry_safe":error.0.retry_safe && acceptance != Acceptance::Unknown,"request_id":request_id,
        "acceptance":acceptance,"status_url":status_url})),
    )
        .into_response()
}

async fn query(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let parsed: QueryRequest =
        serde_json::from_slice(&body).map_err(|_| ApiError::bad("invalid query request"))?;
    let request_id = parsed.request_id;
    if let Err(error) = parsed.validate() {
        return Ok(query_refusal(error, request_id, Acceptance::NotAccepted));
    }
    let (endpoint, guard) = match app.query_route(id, &identity).await {
        Ok(route) => route,
        Err(error) => return Ok(query_refusal(error, request_id, Acceptance::NotAccepted)),
    };
    let inflight = InFlight::enter(&app.inflight);
    // A disconnected caller does not cancel accepted work. Durable status remains
    // authoritative even when the bounded synchronous transport stops waiting.
    let result = tokio::spawn(async move {
        let _guard = guard;
        let _inflight = inflight;
        match app
            .send_as(
                &endpoint,
                Method::POST,
                &format!("/v1/sessions/{id}/queries"),
                body,
                &identity,
            )
            .await
        {
            Ok(upstream) => response(upstream).await,
            Err(error) if error.0 == failure::UNAUTHORIZED => {
                Ok(query_refusal(error, request_id, Acceptance::NotAccepted))
            }
            Err(error) => Ok(query_refusal(error, request_id, Acceptance::Unknown)),
        }
    })
    .await;
    Ok(match result {
        Ok(Ok(response)) => response,
        _ => query_refusal(ApiError::internal(), request_id, Acceptance::Unknown),
    })
}

async fn request_cancel(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.authn.revalidate(&identity).await?;
    app.control_db(move |db| queue::request_cancel(db, id, &identity))
        .await
        .map(Json)
}

async fn cancel(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id, &identity, Action::QueryCancel).await?;
    response(
        app.send_as(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{id}/cancel"),
            Bytes::new(),
            &identity,
        )
        .await?,
    )
    .await
}
async fn close(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let endpoint = app.route(id, &identity, Action::SessionClose).await?;
    if let Some(route) = app.routes.lock().await.get_mut(&id) {
        route.closing = true;
        route.cleanup = None;
        route.close_identity = Some(identity.clone());
    }
    let (status, value) = app
        .close_backend(id, &endpoint, None, Some(&identity))
        .await?;
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        if let Some(route) = app.routes.lock().await.get_mut(&id)
            && route.close_identity.as_ref() == Some(&identity)
        {
            route.closing = false;
            route.close_identity = None;
        }
    } else if status.is_success() && value["closed"] == true {
        let pod = app.retire_route(id).await;
        app.release(pod).await;
    }
    Ok((status, Json(value)).into_response())
}

async fn history(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Query(filter): Query<crate::history::Filter>,
) -> Result<Json<Value>, ApiError> {
    app.history.list(identity, filter).await.map(Json)
}
async fn query_detail(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.history.detail(identity, id).await.map(Json)
}
async fn request_detail(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.requests.detail(identity, id).await.map(Json)
}
async fn usage_events(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Query(filter): Query<crate::usage::Filter>,
) -> Result<Json<Value>, ApiError> {
    app.usage.events(identity, filter).await.map(Json)
}
async fn budget(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    app.usage.budget(identity).await.map(Json)
}
async fn set_budget(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Json(update): Json<crate::usage::BudgetUpdate>,
) -> Result<Json<Value>, ApiError> {
    app.usage.set_budget(identity, update).await.map(Json)
}
/// Runs one platform-owned statement through the ordinary execution machinery: a
/// session on a default-size worker, the statement, a close. Deliberately not a
/// side door — because it is a normal execution, it lands in history under the
/// real principal, is admitted and settled by the ledger, and a losing commit
/// race is classified `transaction_conflict`, retry-safe, like anyone else's.
async fn platform_statement(
    app: &Gateway,
    identity: &Identity,
    operation: TrustedOperation,
) -> Result<Response, ApiError> {
    let sql = operation
        .canonical_sql()
        .ok_or_else(|| ApiError::bad("expected a fixed platform operation"))?;
    let request = SessionRequest::default();
    let (endpoint, session) = match app
        .open_operation(request, identity, operation.clone())
        .await?
    {
        SessionOutcome::Opened { endpoint, id, .. } => (endpoint, id),
        SessionOutcome::Refused(response) => return Ok(response),
    };
    let guard = match app.query_route(session, identity).await {
        Ok((_, guard)) => guard,
        Err(error) => {
            app.close_internal(session).await;
            return Err(error);
        }
    };
    let request_id = Uuid::new_v4();
    let access = match app
        .start_action_operation(
            identity,
            operation.action(),
            "platform_statement",
            request_id,
        )
        .await
    {
        Ok(access) => access,
        Err(error) => {
            drop(guard);
            app.close_internal(session).await;
            return Err(error);
        }
    };
    let result = app
        .send_as_protocol(
            &endpoint,
            Method::POST,
            &format!("/v1/sessions/{session}/queries"),
            json!({
                "request_id": request_id,
                "sql": sql,
                "timeout_seconds": 60,
            })
            .to_string()
            .into(),
            identity,
            (Protocol::Platform, &operation),
        )
        .await;
    drop(guard);
    app.close_internal(session).await;
    let result = match result {
        Ok(upstream) => response(upstream).await,
        Err(error) => Err(error),
    };
    let outcome = result
        .as_ref()
        .ok()
        .map(|reply| reply.status().is_success());
    app.audit_outcome(
        access,
        operation.action(),
        "platform_statement",
        request_id,
        outcome,
    )
    .await;
    result
}

async fn lake_snapshots(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
) -> Result<Response, ApiError> {
    platform_statement(&app, &identity, TrustedOperation::LakeSnapshots).await
}

/// The gateway as the pgwire front sees it: authenticate a credential, run
/// statements through an ordinary worker session under the named principal.
/// Nothing here is pgwire-shaped — which is the point; the front stays a
/// compatibility layer over machinery that neither knows nor cares it exists.
struct PgPlatform(Gateway);
impl crate::pgwire::Platform for PgPlatform {
    async fn authenticate(
        &self,
        token: String,
        mode: crate::pgwire::ConnectionMode,
    ) -> Result<Identity, ApiError> {
        match mode {
            crate::pgwire::ConnectionMode::Warehouse => self.0.authn.authenticate(&token).await,
            crate::pgwire::ConnectionMode::System => self.0.system.authenticate(&token).await,
        }
    }

    async fn revalidate(
        &self,
        identity: &Identity,
        mode: crate::pgwire::ConnectionMode,
    ) -> Result<(), ApiError> {
        if mode == crate::pgwire::ConnectionMode::System {
            return self.0.system.revalidate(identity).await;
        }
        let authority = if identity.kind == ActorKind::Named {
            let authority_identity = identity.clone();
            self.0
                .control_db(move |db| authz::execution_authority(db, &authority_identity))
                .await?
        } else {
            Authority::Writer
        };
        let placement = self
            .0
            .registry()
            .resolve_for(&SessionRequest::default().compute, authority)?;
        self.0
            .authorize_execution(
                identity,
                &identity.principal,
                &placement,
                &TrustedOperation::Sql,
            )
            .await?;
        Ok(())
    }

    async fn query_system(
        &self,
        identity: &Identity,
        request: crate::system_query::Request,
    ) -> Result<crate::system_query::SystemResult, ApiError> {
        let _inflight = InFlight::enter(&self.0.inflight);
        self.0.system.query(identity, request).await
    }

    async fn open_session(&self, identity: &Identity) -> Result<crate::pgwire::Session, ApiError> {
        let request = SessionRequest {
            context: json!({"operation": "pgwire"}),
            ..SessionRequest::default()
        };
        match self.0.open_session(request, identity).await? {
            SessionOutcome::Opened { endpoint, id, .. } => Ok(crate::pgwire::Session {
                endpoint,
                session_id: id.to_string(),
            }),
            SessionOutcome::Refused(reply) => Err(match reply.status() {
                StatusCode::UNAUTHORIZED => invalid_credential(),
                StatusCode::FORBIDDEN => {
                    ApiError::new(failure::FORBIDDEN, "session access refused")
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    ApiError::new(failure::CAPACITY, "session capacity reached")
                }
                _ => ApiError::new(failure::UNAVAILABLE, "the worker refused the session"),
            }),
        }
    }

    async fn query(
        &self,
        identity: &Identity,
        session: &crate::pgwire::Session,
        request_id: Uuid,
        sql: String,
    ) -> crate::pgwire::QueryOutcome {
        use crate::pgwire::QueryOutcome;
        // Counted into the same in-flight tally the HTTP handlers use, so the
        // gateway's drain waits for a pgwire statement already running. The pgwire
        // listener sits outside the axum router that carries `track_inflight`, so
        // without this a rollout would close under a live wire-protocol query.
        let _inflight = InFlight::enter(&self.0.inflight);
        let id = match Uuid::parse_str(&session.session_id) {
            Ok(id) => id,
            Err(_) => {
                return QueryOutcome::Refused {
                    kind: "unknown_session".into(),
                    message: "invalid session identity".into(),
                };
            }
        };
        let (endpoint, _guard) = match self.0.query_route(id, identity).await {
            Ok(route) => route,
            Err(error) => {
                return QueryOutcome::Refused {
                    kind: error.0.kind.into(),
                    message: error.1,
                };
            }
        };
        debug_assert_eq!(endpoint, session.endpoint);
        let sent = self
            .0
            .send_as_protocol(
                &endpoint,
                Method::POST,
                &format!("/v1/sessions/{}/queries", session.session_id),
                json!({
                    "request_id": request_id,
                    "sql": sql,
                    "timeout_seconds": 60,
                    "context": {"operation": "pgwire"},
                })
                .to_string()
                .into(),
                identity,
                (Protocol::Pgwire, &TrustedOperation::Sql),
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
        if let Ok(id) = Uuid::parse_str(&session.session_id) {
            self.0.close_internal(id).await;
        }
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

/// Fixed operations are independently authorized; supplied context cannot turn
/// arbitrary SQL into maintenance.
async fn lake_maintenance(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<MaintenanceRequest>,
) -> Result<Response, ApiError> {
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
    platform_statement(
        &app,
        &identity,
        TrustedOperation::LakeMaintenance {
            expire_older_than_days: request.expire_older_than_days,
            compact: request.compact,
            cleanup: request.cleanup,
        },
    )
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MintRequest {
    principal: String,
    #[serde(default)]
    expires_seconds: Option<i64>,
}

/// Issues a credential. The token in the response is the only time it exists in
/// plaintext anywhere the caller can reach — the database holds its digest.
async fn mint_credential(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<MintRequest>,
) -> Result<Json<Value>, ApiError> {
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
        .control_db(move |client| {
            credentials::issue_authorized(
                client,
                &identity,
                &request.principal,
                request.expires_seconds,
            )
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
    Extension(identity): Extension<Identity>,
    Query(filter): Query<ListFilter>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |client| {
        credentials::list_authorized(client, &identity, filter.limit, filter.after.as_deref())
    })
    .await
    .map(Json)
}

async fn revoke_credential(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(credential_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let revoked = app
        .control_db(move |client| credentials::revoke_authorized(client, &identity, &credential_id))
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

async fn list_principals(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Query(filter): Query<ListFilter>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |client| {
        authz::list_principals(client, &identity, filter.limit, filter.after.as_deref())
    })
    .await
    .map(Json)
}

async fn get_policy(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(principal): Path<String>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |client| authz::get_policy(client, &identity, &principal))
        .await
        .map(Json)
}

async fn set_policy(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(principal): Path<String>,
    Json(policy): Json<authz::PolicyUpdate>,
) -> Result<Json<Value>, ApiError> {
    let updated = app
        .control_db(move |client| authz::set_policy(client, &identity, &principal, &policy))
        .await?;
    app.authn.cache.lock().await.invalidate();
    Ok(Json(updated))
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuditFilter {
    limit: i64,
    before: Option<String>,
}
impl Default for AuditFilter {
    fn default() -> Self {
        Self {
            limit: 100,
            before: None,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ListFilter {
    limit: i64,
    after: Option<String>,
}
impl Default for ListFilter {
    fn default() -> Self {
        Self {
            limit: 100,
            after: None,
        }
    }
}
async fn audit_events(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Query(filter): Query<AuditFilter>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |client| {
        crate::audit::list(client, &identity, filter.limit, filter.before.as_deref())
    })
    .await
    .map(Json)
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

async fn workers(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    app.authorize_action(&identity, Action::FleetRead, None)
        .await?;
    let registry = app.registry();
    let fleet = app.fleet_snapshot(&registry).await;
    Ok(Json(json!({
        "workers": fleet.reported,
        "unreachable": fleet.unreachable,
        "registry_revision": registry.revision,
        "isolated_compute": app.isolation.is_some(),
        "observation_age_ms": u64::try_from(fleet.discovered.elapsed().as_millis()).unwrap_or(u64::MAX),
    })))
}

async fn pool_discovery(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    app.authorize_action(&identity, Action::FleetRead, None)
        .await?;
    let registry = app.registry();
    let fleet = app.fleet_snapshot(&registry).await;
    // Cleanup can outlive a registry entry. Count every retained closing route,
    // since its backend slot remains reserved until the worker acknowledges close.
    let (closing_sessions, closing_endpoints) = {
        let routes = app.routes.lock().await;
        let mut endpoints = BTreeMap::<String, usize>::new();
        for route in routes.values().filter(|route| route.closing) {
            *endpoints.entry(route.endpoint.clone()).or_default() += 1;
        }
        (endpoints.values().sum::<usize>(), endpoints)
    };
    let pools: Vec<Value> = registry.pools.iter().map(|(name, pool)| {
        let eligible = ranked_endpoints(&fleet.workers, pool.resources, name,
            &pool.endpoints, pool.max_sessions_per_worker, registry.legacy);
        let sampled: Vec<&WorkerInfo> = fleet.workers.iter().filter(|worker| {
            pool.endpoints.contains(&worker.endpoint)
                && worker.serves(pool.resources, name, registry.legacy)
        }).collect();
        json!({
            "name": name, "resources": pool.resources.describe(),
            "configured_workers": pool.endpoints.len(), "eligible_workers": eligible.len(),
            "sampled_workers": sampled.len(),
            "sampled_open_sessions": sampled.iter().fold(0u64, |total, worker| total.saturating_add(worker.open_sessions)),
            "unreachable_workers": fleet.unreachable.iter().filter(|endpoint| pool.endpoints.contains(endpoint)).count(),
            "max_sessions_per_worker": if registry.legacy { None } else { Some(pool.max_sessions_per_worker) },
            "worker_limits": sampled.iter().map(|worker| json!({
                "endpoint": worker.endpoint,
                "reported_max_sessions": worker.max_sessions,
                "effective_max_sessions": worker.session_limit(pool.max_sessions_per_worker, registry.legacy),
            })).collect::<Vec<_>>(),
            "endpoints": pool.endpoints,
            "admission": pool.admission,
        })
    }).collect();
    Ok(Json(json!({
        "default_pool": registry.default_pool, "revision": registry.revision,
        "reload_available": app.pools_file.is_some(), "pools": pools,
        "gateway_max_sessions": app.max_sessions,
        "gateway_open_sessions": app.max_sessions.saturating_sub(app.capacity.available_permits()),
        "gateway_closing_sessions": closing_sessions,
        "gateway_closing_endpoints": closing_endpoints,
        "isolated_compute": app.isolation.is_some(),
        "observation_age_ms": u64::try_from(fleet.discovered.elapsed().as_millis()).unwrap_or(u64::MAX),
    })))
}

async fn reload_pools(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    app.authorize_action(&identity, Action::FleetManage, None)
        .await?;
    // Keep serialization and the registry swap alive with the database write if
    // the HTTP caller disappears. A dropped waiter must not release the guard
    // while its blocking PostgreSQL publication can still commit.
    let inflight = InFlight::enter(&app.inflight);
    tokio::spawn(async move {
        let _inflight = inflight;
        reload_pool_registry(app, identity).await
    })
    .await
    .map_err(|_| ApiError::internal())?
}

async fn reload_pool_registry(app: Gateway, identity: Identity) -> Result<Json<Value>, ApiError> {
    let _publication_guard = app
        .publishing
        .clone()
        .try_lock_owned()
        .map_err(|_| ApiError::new(failure::CAPACITY, "pool reload already in progress"))?;
    let previous = app.registry();
    let path = app.pools_file.clone().ok_or_else(|| ApiError::bad(
        "pool reload requires DUCKFLEET_POOLS_FILE; inline and legacy configuration cannot reload"
    ))?;
    let candidate = tokio::task::spawn_blocking(move || -> anyhow::Result<PoolRegistry> {
        anyhow::ensure!(
            std::fs::metadata(path.as_ref())?.len() <= 1_048_576,
            "pool configuration exceeds one MiB"
        );
        previous.reloaded(&std::fs::read_to_string(path.as_ref())?)
    })
    .await
    .map_err(|_| ApiError::internal())?
    .map_err(|_| {
        ApiError::bad("invalid or unavailable pool configuration; previous registry retained")
    })?;
    let correlation = Uuid::new_v4();
    let access = app
        .start_action_operation(&identity, Action::FleetManage, "pool_reload", correlation)
        .await?;
    if let Err(error) = app.publish_policy(&candidate).await {
        app.audit_outcome(
            access,
            Action::FleetManage,
            "pool_reload",
            correlation,
            None,
        )
        .await;
        return Err(error);
    }
    let revision = candidate.revision.clone();
    *app.pools.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(candidate);
    // Existing routes retain their endpoint and session. A concurrent open retains
    // its complete old snapshot; a fleet cache also carries its registry revision.
    *app.fleet.lock().await = None;
    app.audit_outcome(
        access,
        Action::FleetManage,
        "pool_reload",
        correlation,
        Some(true),
    )
    .await;
    Ok(Json(json!({"reloaded": true, "revision": revision})))
}

pub(crate) async fn serve(token: String) -> anyhow::Result<()> {
    let (registry, pools_file) = PoolRegistry::load()?;
    let max_sessions = crate::configured_limit("DUCKFLEET_GATEWAY_MAX_SESSIONS", 64, 4096)?;
    let platform_dsn = std::env::var("PLATFORM_DSN")?;
    let maintenance_dsn = std::env::var("DUCKFLEET_MAINTENANCE_DSN").map_err(|_| {
        anyhow::anyhow!("DUCKFLEET_MAINTENANCE_DSN must name the separate maintainer database role")
    })?;
    let maintenance_config = crate::platform::MaintenanceConfig::from_env()?;
    let schema_wait = crate::platform::schema_wait()?;
    let schema_dsn = platform_dsn.clone();
    let maintenance_schema_dsn = maintenance_dsn.clone();
    let storage_schema = tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
        let mut db = crate::platform::wait_schema(&schema_dsn, schema_wait)?;
        crate::db_roles::validate_runtime(&mut db, crate::db_roles::RuntimeRole::Gateway)?;
        authz::validate_namespace(&mut db)?;
        let mut maintenance_db =
            crate::platform::wait_schema(&maintenance_schema_dsn, schema_wait)?;
        crate::db_roles::validate_runtime(
            &mut maintenance_db,
            crate::db_roles::RuntimeRole::Maintainer,
        )?;
        authz::validate_namespace(&mut maintenance_db)?;
        Ok(db.query_one("SELECT current_schema()", &[])?.get(0))
    })
    .await??;
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
    let bootstrap: Arc<str> = token.into();
    let internal = std::env::var("DUCKFLEET_INTERNAL_TOKEN").map_err(|_| {
        anyhow::anyhow!(
            "DUCKFLEET_INTERNAL_TOKEN must be separate from the public bootstrap credential"
        )
    })?;
    anyhow::ensure!(
        !internal.is_empty() && !credentials::tokens_match(&internal, &bootstrap),
        "DUCKFLEET_INTERNAL_TOKEN must be nonempty and different from DUCKFLEET_TOKEN"
    );
    let token: Arc<str> = internal.into();
    let system = crate::system_query::Service::new(
        &platform_dsn,
        bootstrap.clone(),
        crate::metadata::StorageSchema::validated(&storage_schema)
            .map_err(|e| anyhow::anyhow!("{}", e.1))?,
    )
    .map_err(|e| anyhow::anyhow!("{}", e.1))?;
    let app = Gateway {
        client: Client::builder()
            .timeout(Duration::from_secs(75))
            .retry(reqwest::retry::never())
            .build()?,
        authn: Authn::new(bootstrap, platform_dsn.clone()),
        system,
        token,
        pools: Arc::new(RwLock::new(Arc::new(registry))),
        pools_file: pools_file.map(Arc::new),
        publishing: Arc::new(Mutex::new(())),
        controls: Arc::new(Semaphore::new(8)),
        artifact_store: crate::artifacts::StoreConfig::from_env()?
            .map(|c| c.build())
            .transpose()?,
        result_slots: Arc::new(Semaphore::new(2)),
        dispatcher_owner: Uuid::new_v4(),
        max_sessions,
        fleet: Arc::new(Mutex::new(None)),
        observations: Arc::new(Mutex::new(HashMap::new())),
        discovering: Arc::new(Mutex::new(())),
        routes: Arc::new(Mutex::new(HashMap::new())),
        capacity: Arc::new(Semaphore::new(max_sessions)),
        history: crate::history::Reader::new(std::env::var("PLATFORM_DSN")?),
        requests: crate::requests::Reader::new(std::env::var("PLATFORM_DSN")?),
        usage: crate::usage::Reader::new(
            std::env::var("PLATFORM_DSN")?,
            crate::pricing::RateCard::from_env()?,
        ),
        isolation,
        retired_pods: Arc::new(Mutex::new(HashMap::new())),
        rate: crate::pricing::RateCard::from_env()?,
        draining: Arc::new(AtomicBool::new(false)),
        inflight: Arc::new(AtomicUsize::new(0)),
        #[cfg(test)]
        resolved_access: None,
    };
    app.publish_policy(&app.registry())
        .await
        .map_err(|error| anyhow::anyhow!("{}", error.1))?;
    // Before serving, so a pod this gateway is about to create can never be mistaken
    // for one a previous gateway abandoned.
    if let Some(isolation) = app.isolation.as_ref() {
        let orphaned = app
            .control_db(|db| {
                queue::orphan_workers(db, crate::isolated::PREFIX).map_err(|_| ApiError::internal())
            })
            .await
            .map_err(|error| {
                anyhow::anyhow!("cannot establish isolated recovery set: {}", error.1)
            })?;
        let job_pods = app
            .control_db(|db| crate::jobs::live_job_pods(db).map_err(|_| ApiError::internal()))
            .await
            .map_err(|_| anyhow::anyhow!("cannot establish durable job pod recovery set"))?
            .into_iter()
            .collect();
        let swept = isolation.sweep_excluding(&orphaned, &job_pods).await;
        app.retired_pods
            .lock()
            .await
            .extend(swept.pending.into_iter().map(|pod| (pod, Instant::now())));
        for pod in swept.confirmed_absent {
            app.retired_pods
                .lock()
                .await
                .entry(pod.clone())
                .or_insert_with(Instant::now);
            app.recover_retired(pod).await;
        }
        tracing::info!("isolated compute is available");
    }
    let api = Router::new()
        .route("/v1/jobs", post(job_api::submit))
        .route("/v1/jobs/{id}", get(job_api::detail))
        .route("/v1/jobs/{id}/cancel", post(request_cancel))
        .route("/v1/jobs/{id}/result", get(job_api::manifest))
        .route("/v1/jobs/{id}/result/chunks/{chunk}", get(job_api::chunk))
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
        .route("/v1/workers", get(workers))
        .route("/v1/pools", get(pool_discovery))
        .route("/v1/pools/reload", post(reload_pools))
        .route("/v1/principals", get(list_principals))
        .route(
            "/v1/principals/{principal}/policy",
            get(get_policy).put(set_policy),
        )
        .route("/v1/audit", get(audit_events))
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
    // This lane authenticates inside its owned read snapshot and absolute
    // deadline. It intentionally bypasses the warehouse authentication cache.
    let system_api = Router::new()
        .route("/v1/system/query", post(system_query))
        .route("/v1/system/schema", get(system_schema))
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
            tokio::join!(
                cleanup.expire(),
                cleanup.retry_retired(),
                cleanup.reconcile_job_pods()
            );
        }
    });
    let draining = app.draining.clone();
    let inflight = app.inflight.clone();
    let job_dispatcher = app.spawn_job_dispatcher();
    let router = Router::new()
        .route("/healthz", get(health))
        .merge(api)
        .merge(system_api)
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(app);
    let maintenance = crate::platform::spawn_maintenance(maintenance_dsn, maintenance_config);
    tracing::info!("DuckFleet gateway ready");
    let served = axum::serve(
        crate::http_transport::Listener::new(tokio::net::TcpListener::bind("0.0.0.0:8080").await?),
        router.into_make_service_with_connect_info::<crate::http_transport::Connection>(),
    )
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
    .await;
    maintenance.abort();
    job_dispatcher.abort();
    served?;
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
        close_ack: Arc<AtomicBool>,
        close_actors: Arc<Mutex<Vec<Value>>>,
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
        let close_ack = Arc::new(AtomicBool::new(true));
        let close_actors = Arc::new(Mutex::new(Vec::new()));
        let handler_close_actors = close_actors.clone();
        let handler_close_ack = close_ack.clone();
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
                "/v1/sessions/{id}/queries",
                post(|| async { Json(json!({"executed": true})) }),
            )
            .route(
                "/v1/sessions/{id}",
                axum::routing::delete(move |headers: axum::http::HeaderMap| {
                    let actors = handler_close_actors.clone();
                    let closes = handler_closes.clone();
                    let status = handler_close_status.clone();
                    let ack = handler_close_ack.clone();
                    async move {
                        closes.fetch_add(1, Ordering::SeqCst);
                        actors.lock().await.push(json!({
                            "kind": headers.get(credentials::ACTOR_KIND_HEADER).and_then(|v|v.to_str().ok()),
                            "principal": headers.get(credentials::PRINCIPAL_HEADER).and_then(|v|v.to_str().ok()),
                            "credential_id": headers.get(credentials::CREDENTIAL_HEADER).and_then(|v|v.to_str().ok()),
                        }));
                        (*status.lock().await, Json(json!({"closed":ack.load(Ordering::Acquire),"authority_changed":!ack.load(Ordering::Acquire)})))
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
            close_ack,
            close_actors,
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
            "disk_bytes": resources.disk_bytes,
            "pool": "small",
            "max_sessions": 32,
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
            resolved_access: Some(authz::Access::bootstrap_for_test()),
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(75))
                .retry(reqwest::retry::never())
                .build()
                .unwrap(),
            authn: Authn::new(token.clone(), dsn.into()),
            system: crate::system_query::Service::new(
                dsn,
                token.clone(),
                crate::metadata::StorageSchema::validated("public").unwrap(),
            )
            .unwrap(),
            token,
            pools: Arc::new(RwLock::new(Arc::new(
                PoolRegistry::from_json(
                    &json!({
                        "default_pool":"small", "pools":{"small":{
                            "cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":endpoints,
                            "max_sessions_per_worker":32
                        }}
                    })
                    .to_string(),
                )
                .unwrap(),
            ))),
            pools_file: None,
            publishing: Arc::new(Mutex::new(())),
            controls: Arc::new(Semaphore::new(8)),
            artifact_store: None,
            result_slots: Arc::new(Semaphore::new(2)),
            dispatcher_owner: Uuid::new_v4(),
            max_sessions: 64,
            fleet: Arc::new(Mutex::new(None)),
            observations: Arc::new(Mutex::new(HashMap::new())),
            discovering: Arc::new(Mutex::new(())),
            routes: Arc::new(Mutex::new(HashMap::new())),
            capacity: Arc::new(Semaphore::new(64)),
            history: crate::history::Reader::new(dsn.into()),
            requests: crate::requests::Reader::new(dsn.into()),
            usage: crate::usage::Reader::new(
                dsn.into(),
                crate::pricing::RateCard {
                    version: "test".into(),
                    compute_micro_usd_per_unit_hour: 100_000,
                },
            ),
            isolation: None,
            retired_pods: Arc::new(Mutex::new(HashMap::new())),
            rate: crate::pricing::RateCard {
                version: "test".into(),
                compute_micro_usd_per_unit_hour: 100_000,
            },
            draining: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn placement(resources: Resources) -> Placement {
        Placement {
            mode: Mode::Persistent,
            pool: Some("small".into()),
            resources,
            authority: crate::compute::Authority::Writer,
        }
    }

    #[tokio::test]
    async fn pool_membership_disk_and_capacity_are_required_for_routing() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        for (field, value, kind) in [
            ("pool", json!("another-pool"), "no_ready_worker"),
            (
                "disk_bytes",
                json!(Resources::default().disk_bytes * 2),
                "no_ready_worker",
            ),
            ("max_sessions", json!(0), "no_ready_worker"),
            ("max_sessions", json!(16), "no_ready_worker"),
            ("open_sessions", json!(32), "capacity"),
        ] {
            let mut report = status("ready");
            report[field] = value;
            server.reply.lock().await.1 = report;
            *app.fleet.lock().await = None;
            let error = app
                .endpoints_for(&app.registry(), &placement(Resources::default()))
                .await
                .unwrap_err();
            assert_eq!(error.0.kind, kind);
            assert!(error.0.retry_safe);
            assert_eq!(server.opens.load(Ordering::SeqCst), 0);
        }
        server.reply.lock().await.1 = status("ready");
        *app.fleet.lock().await = None;
        assert_eq!(
            app.endpoints_for(&app.registry(), &placement(Resources::default()))
                .await
                .unwrap(),
            std::slice::from_ref(&server.endpoint)
        );
    }

    #[tokio::test]
    async fn acknowledged_closes_release_cached_worker_capacity_once() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let identity = Identity::bootstrap();
        let mut ids = Vec::new();
        for _ in 0..2 {
            let SessionOutcome::Opened { id, .. } = app
                .open_session(SessionRequest::default(), &identity)
                .await
                .unwrap()
            else {
                panic!("open failed")
            };
            ids.push(id);
        }
        assert_eq!(
            app.fleet.lock().await.as_ref().unwrap().workers[0].open_sessions,
            2
        );
        // A refusal must leave the route and its capacity counted.
        *server.close_status.lock().await = StatusCode::SERVICE_UNAVAILABLE;
        app.close_internal(ids[0]).await;
        assert_eq!(
            app.fleet.lock().await.as_ref().unwrap().workers[0].open_sessions,
            2
        );
        *server.close_status.lock().await = StatusCode::OK;
        app.close_internal(ids[0]).await;
        app.close_internal(ids[0]).await;
        assert_eq!(
            app.fleet.lock().await.as_ref().unwrap().workers[0].open_sessions,
            1
        );
        let response = close(
            State(app.clone()),
            Extension(identity.clone()),
            Path(ids[1]),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            app.fleet.lock().await.as_ref().unwrap().workers[0].open_sessions,
            0
        );
        assert_eq!(app.capacity.available_permits(), 64);
    }

    #[tokio::test]
    async fn all_ingress_paths_share_the_backend_session_limit() {
        use crate::pgwire::Platform;
        let server = status_server(status("ready")).await;
        let mut app = test_gateway(vec![server.endpoint.clone()]);
        app.max_sessions = 1;
        app.capacity = Arc::new(Semaphore::new(1));
        let identity = Identity::bootstrap();
        let response = open(
            State(app.clone()),
            Extension(identity.clone()),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let http_id = *app.routes.lock().await.keys().next().unwrap();
        let pg = PgPlatform(app.clone());
        let refused = pg.open_session(&identity).await.err().unwrap();
        assert_eq!(refused.0.kind, "capacity");
        let refused = platform_statement(&app, &identity, TrustedOperation::LakeSnapshots)
            .await
            .unwrap_err();
        assert_eq!(refused.0.kind, "capacity");
        assert_eq!(server.opens.load(Ordering::SeqCst), 1);
        app.close_internal(http_id).await;
        let session = pg.open_session(&identity).await.unwrap();
        let refused = open(
            State(app.clone()),
            Extension(identity.clone()),
            Bytes::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(refused.0.kind, "capacity");
        assert_eq!(app.routes.lock().await.len(), 1);
        pg.close_session(session).await;
        assert_eq!(app.capacity.available_permits(), 1);
        let response = platform_statement(&app, &identity, TrustedOperation::LakeSnapshots)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(app.capacity.available_permits(), 1);
        assert!(app.routes.lock().await.is_empty());
        assert_eq!(server.opens.load(Ordering::SeqCst), 3);
        assert_eq!(server.closes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn reload_refuses_unconfirmed_policy_and_preserves_existing_routes() {
        let first = status_server(status("ready")).await;
        let mut named = status("ready");
        named["pool"] = json!("interactive");
        let second = status_server(named).await;
        let mut app = test_gateway(vec![first.endpoint.clone()]);
        let path = std::env::temp_dir().join(format!("duckfleet-pools-{}.json", Uuid::new_v4()));
        app.pools_file = Some(Arc::new(path.clone()));
        let identity = Identity::bootstrap();
        let SessionOutcome::Opened { id: original, .. } = app
            .open_session(SessionRequest::default(), &identity)
            .await
            .unwrap()
        else {
            panic!("initial open failed")
        };
        let previous = app.registry();
        let candidate = json!({"default_pool":"interactive","pools":{"interactive":{
            "cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":[second.endpoint],"max_sessions_per_worker":32
        }}});
        std::fs::write(&path, candidate.to_string()).unwrap();
        let named_identity = issued_identity();
        let mut denied_app = app.clone();
        let mut no_grants = authz::Access::bootstrap_for_test();
        no_grants.actor.kind = ActorKind::Named;
        no_grants.actor.principal = named_identity.principal.clone();
        no_grants.actor.principal_id = Some(Uuid::new_v4().to_string());
        no_grants.actor.credential_id = named_identity.credential_id.clone();
        denied_app.resolved_access = Some(no_grants);
        let denied = reload_pools(State(denied_app), Extension(named_identity))
            .await
            .unwrap_err();
        assert_eq!(denied.0.kind, "forbidden");
        assert_eq!(app.registry().revision, previous.revision);
        let error = reload_pools(State(app.clone()), Extension(identity.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.0.kind, "policy_unavailable");
        assert_eq!(app.registry().revision, previous.revision);
        let (endpoint, guard) = app.query_route(original, &identity).await.unwrap();
        assert_eq!(endpoint, first.endpoint);
        drop(guard);
        let current = app.registry();
        std::fs::write(&path, "{}").unwrap();
        assert!(
            reload_pools(State(app.clone()), Extension(identity))
                .await
                .is_err()
        );
        assert_eq!(app.registry().revision, current.revision);
        app.close_internal(original).await;
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn canceling_reload_waiter_keeps_serialization_until_metadata_work_finishes() {
        // A real PostgreSQL connection attempt remains in flight after the HTTP
        // waiter disappears. Exercise reload_pools itself, including its owned
        // task and blocking metadata lane, rather than a stand-in mutex helper.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (connected, accepted) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let backend = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = connected.send(());
            let _ = released.await;
            drop(socket);
            drop(listener);
        });
        let mut app = test_gateway(Vec::new());
        app.authn = Authn::new(
            app.token.clone(),
            format!("host=127.0.0.1 port={port} user=fixture dbname=fixture sslmode=disable"),
        );
        let path =
            std::env::temp_dir().join(format!("duckfleet-reload-cancel-{}.json", Uuid::new_v4()));
        std::fs::write(
            &path,
            json!({"default_pool":"small","pools":{"small":{
                "cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":[],
                "admission":{"max_running_queries":2}
            }}})
            .to_string(),
        )
        .unwrap();
        app.pools_file = Some(Arc::new(path.clone()));
        let previous = app.registry().revision.clone();
        let request_app = app.clone();
        let waiter = tokio::spawn(async move {
            reload_pools(State(request_app), Extension(Identity::bootstrap())).await
        });
        tokio::time::timeout(Duration::from_secs(2), accepted)
            .await
            .unwrap()
            .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            app.publishing.try_lock().is_err(),
            "canceled HTTP waiter released publication serialization"
        );
        assert_eq!(app.inflight.load(Ordering::Acquire), 1);
        let overlap = reload_pools(State(app.clone()), Extension(Identity::bootstrap()))
            .await
            .unwrap_err();
        assert_eq!(overlap.0, failure::CAPACITY);
        assert_eq!(app.registry().revision, previous);

        // The failed connection and failed independent readback leave the prior
        // registry in place, then release the lock and drain accounting.
        release.send(()).unwrap();
        backend.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if app.publishing.try_lock().is_ok() && app.inflight.load(Ordering::Acquire) == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(app.registry().revision, previous);
        assert_eq!(app.controls.available_permits(), 8);
        std::fs::remove_file(path).unwrap();
    }

    fn issued_identity() -> Identity {
        Identity {
            kind: ActorKind::Named,
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
        assert_eq!(request.headers()[credentials::ACTOR_KIND_HEADER], "named");
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
                owner_id: None,
                placement: app
                    .registry()
                    .resolve(&SessionRequest::default().compute)
                    .unwrap(),
                operation: TrustedOperation::Sql,
                identity,
                touched,
                active: Arc::new(AtomicUsize::new(0)),
                closing: false,
                cleanup: None,
                close_identity: None,
                pod: None,
                _permit: app.capacity.clone().try_acquire_owned().unwrap(),
            },
        );
        id
    }

    #[tokio::test]
    async fn explicit_http_close_retains_unconditional_intent_after_failure() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let id = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now(),
        )
        .await;
        *server.close_status.lock().await = StatusCode::SERVICE_UNAVAILABLE;
        let result = close(
            State(app.clone()),
            Extension(Identity::bootstrap()),
            Path(id),
        )
        .await
        .unwrap();
        assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(app.routes.lock().await[&id].closing);
        assert!(app.routes.lock().await[&id].cleanup.is_none());
        assert!(app.query_route(id, &Identity::bootstrap()).await.is_err());
        assert_eq!(app.capacity.available_permits(), 63);
        *server.close_status.lock().await = StatusCode::OK;
        app.expire().await;
        assert_eq!(app.capacity.available_permits(), 64);
    }

    #[tokio::test]
    async fn explicit_close_retries_preserve_the_authenticated_actor() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let actor = issued_identity();
        let id =
            insert_test_route(&app, server.endpoint.clone(), actor.clone(), Instant::now()).await;
        *server.close_status.lock().await = StatusCode::SERVICE_UNAVAILABLE;
        let result = close(State(app.clone()), Extension(actor.clone()), Path(id))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            app.routes.lock().await[&id].close_identity,
            Some(actor.clone())
        );
        *server.close_status.lock().await = StatusCode::OK;
        app.expire().await;
        let actors = server.close_actors.lock().await;
        assert_eq!(actors.len(), 2);
        for forwarded in actors.iter() {
            assert_eq!(forwarded["kind"], "named");
            assert_eq!(forwarded["principal"], actor.principal);
            assert_eq!(
                forwarded["credential_id"],
                actor.credential_id.as_deref().unwrap()
            );
        }
        assert!(!app.routes.lock().await.contains_key(&id));
    }

    #[tokio::test]
    async fn stale_conditional_cleanup_keeps_route_and_capacity() {
        let server = status_server(status("ready")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let id = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now() - Duration::from_secs(301),
        )
        .await;
        server.close_ack.store(false, Ordering::Release);
        app.expire().await;
        assert!(!app.routes.lock().await[&id].closing);
        assert_eq!(app.capacity.available_permits(), 63);
        assert!(app.query_route(id, &Identity::bootstrap()).await.is_ok());
        server.close_ack.store(true, Ordering::Release);
        app.close_internal(id).await;
        assert_eq!(app.capacity.available_permits(), 64);
    }

    #[tokio::test]
    async fn parsed_gateway_refusal_retains_id_before_forwarding() {
        let app = test_gateway(Vec::new());
        let id = Uuid::new_v4();
        let response = query(
            State(app),
            Extension(Identity::bootstrap()),
            Path(Uuid::new_v4()),
            json!({"request_id":id,"sql":"SELECT 1"}).to_string().into(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["x-query-id"], id.to_string());
        assert_eq!(response.headers()["x-query-acceptance"], "not_accepted");
        let bytes = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["request_id"], id.to_string());
        assert_eq!(body["acceptance"], "not_accepted");
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
        // Conditional cleanup must defer to the durable worker authority: an
        // accepted replacement token can remain valid while this ACK is missing.
        assert!(app.query_route(id, &Identity::bootstrap()).await.is_ok());
        *server.close_status.lock().await = StatusCode::OK;
        app.expire().await;
        assert!(!app.routes.lock().await.contains_key(&id));
        assert_eq!(app.capacity.available_permits(), 64);
        assert_eq!(server.closes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn closing_routes_remain_visible_after_endpoint_removal_until_close_is_acknowledged() {
        let server = status_server(status("ready")).await;
        // The current registry no longer lists this endpoint, but prior sessions
        // still have their pinned route and consume backend-session permits.
        let app = test_gateway(Vec::new());
        let first = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now(),
        )
        .await;
        let second = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now(),
        )
        .await;
        let active = insert_test_route(
            &app,
            server.endpoint.clone(),
            Identity::bootstrap(),
            Instant::now(),
        )
        .await;
        *server.close_status.lock().await = StatusCode::SERVICE_UNAVAILABLE;
        app.close_internal(first).await;
        app.close_internal(second).await;
        assert_eq!(app.capacity.available_permits(), 61);
        let Json(pending) = pool_discovery(State(app.clone()), Extension(Identity::bootstrap()))
            .await
            .unwrap();
        assert_eq!(pending["pools"][0]["configured_workers"], 0);
        assert_eq!(pending["gateway_open_sessions"], 3);
        assert_eq!(pending["gateway_closing_sessions"], 2);
        assert_eq!(
            pending["gateway_closing_endpoints"],
            json!({server.endpoint.clone(): 2})
        );
        assert!(app.routes.lock().await.contains_key(&first));
        assert!(app.routes.lock().await.contains_key(&second));

        *server.close_status.lock().await = StatusCode::OK;
        app.expire().await;
        assert_eq!(app.capacity.available_permits(), 63);
        assert!(!app.routes.lock().await.contains_key(&first));
        assert!(!app.routes.lock().await.contains_key(&second));
        assert!(app.routes.lock().await.contains_key(&active));
        let Json(cleaned) = pool_discovery(State(app.clone()), Extension(Identity::bootstrap()))
            .await
            .unwrap();
        assert_eq!(cleaned["gateway_open_sessions"], 1);
        assert_eq!(cleaned["gateway_closing_sessions"], 0);
        assert_eq!(cleaned["gateway_closing_endpoints"], json!({}));
        assert_eq!(server.closes.load(Ordering::SeqCst), 4);
        app.close_internal(active).await;
        assert_eq!(app.capacity.available_permits(), 64);
    }

    #[tokio::test]
    async fn routing_a_replacement_credential_does_not_claim_cleanup_before_admission() {
        let app = test_gateway(Vec::new());
        let old = issued_identity();
        let replacement = issued_identity();
        let id = insert_test_route(&app, "http://unused".into(), old.clone(), Instant::now()).await;
        {
            let mut cache = app.authn.cache.lock().await;
            let generation = cache.generation;
            assert!(cache.publish(generation, replacement.clone(), Instant::now(), None));
        }
        let (_, guard) = app.query_route(id, &replacement).await.unwrap();
        assert_eq!(app.routes.lock().await[&id].identity, old);
        app.expire().await;
        assert!(!app.routes.lock().await[&id].closing);
        drop(guard);
    }

    #[tokio::test]
    async fn a_partial_fleet_failure_preserves_the_last_successful_observation() {
        let healthy = status_server(status("ready")).await;
        let failing = status_server(status("ready")).await;
        let app = test_gateway(vec![healthy.endpoint.clone(), failing.endpoint.clone()]);
        let first = app.fleet_snapshot(&app.registry()).await;
        let previous = first.reported[1].clone();
        assert!(previous["last_success_at"].as_u64().unwrap() > 0);

        failing.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        *app.fleet.lock().await = None;
        let Json(report) = workers(State(app.clone()), Extension(Identity::bootstrap()))
            .await
            .unwrap();
        assert_eq!(report["workers"].as_array().unwrap().len(), 2);
        assert_eq!(report["workers"][0]["state"], "ready");
        let unavailable = &report["workers"][1];
        assert_eq!(unavailable["state"], "unreachable");
        assert_eq!(unavailable["ready"], false);
        assert_eq!(unavailable["last_success_at"], previous["last_success_at"]);
        assert_eq!(unavailable["last_observation"], previous);
        assert_eq!(
            app.endpoints_for(&app.registry(), &placement(Resources::default()))
                .await
                .unwrap(),
            std::slice::from_ref(&healthy.endpoint)
        );

        // Consecutive failures retain the successful observation, not a nested
        // history of earlier failures or counters eligible for placement.
        *app.fleet.lock().await = None;
        let again = app.fleet_snapshot(&app.registry()).await;
        assert_eq!(again.reported[1]["last_observation"], previous);
        assert_eq!(again.unreachable, std::slice::from_ref(&failing.endpoint));

        let mut restarted = status("ready");
        restarted["worker_boot_id"] = json!("second-boot");
        *failing.reply.lock().await = (StatusCode::OK, restarted);
        *app.fleet.lock().await = None;
        let recovered = app.fleet_snapshot(&app.registry()).await;
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
        let initial = app.fleet_snapshot(&app.registry()).await;
        assert_eq!(initial.reported[0]["state"], "unreachable");
        assert!(initial.reported[0]["last_success_at"].is_null());
        assert!(initial.reported[0]["last_observation"].is_null());
        for _ in 0..3 {
            assert!(
                app.endpoints_for(&app.registry(), &placement(Resources::default()))
                    .await
                    .is_err()
            );
        }
        assert_eq!(failing.requests.load(Ordering::SeqCst), 1);

        let healthy = status_server(status("ready")).await;
        let app = test_gateway(vec![healthy.endpoint.clone()]);
        let absent = Resources {
            cpu: 4,
            ..Resources::default()
        };
        for _ in 0..3 {
            assert!(
                app.endpoints_for(&app.registry(), &placement(absent))
                    .await
                    .is_err()
            );
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
        let snapshot = app.fleet_snapshot(&app.registry()).await;
        assert!(snapshot.unreachable.is_empty());
        assert_eq!(snapshot.reported[0]["state"], "starting");
        assert_eq!(snapshot.reported[1]["state"], "draining");
        assert_eq!(
            app.endpoints_for(&app.registry(), &placement(Resources::default()))
                .await
                .unwrap(),
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
                app.endpoints_for(&app.registry(), &placement(Resources::default()))
                    .await
                    .unwrap(),
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
        let initial = app.fleet_snapshot(&app.registry()).await;
        assert_eq!(initial.unreachable, std::slice::from_ref(&failed.endpoint));
        // Keep this test within one cache generation independently of machine
        // scheduling. Discarding the cache after an open still triggers a probe.
        app.fleet.lock().await.as_mut().unwrap().discovered =
            Instant::now() + Duration::from_secs(60);
        let mut placements = Vec::new();
        for _ in 0..4 {
            match app
                .open_persistent(
                    &app.registry(),
                    &placement(Resources::default()),
                    b"{}".to_vec(),
                    &Identity::bootstrap(),
                    &TrustedOperation::Sql,
                    Uuid::new_v4(),
                )
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
        let Json(report) = workers(State(app), Extension(Identity::bootstrap()))
            .await
            .unwrap();
        // Local tie-breaker adjustments must not pretend to be worker samples.
        assert_eq!(report["workers"], json!(initial.reported));
    }

    #[tokio::test]
    async fn excluding_a_transient_worker_preserves_the_waitable_open_refusal() {
        let server = status_server(status("draining")).await;
        let app = test_gateway(vec![server.endpoint.clone()]);
        let error = app
            .endpoints_for(&app.registry(), &placement(Resources::default()))
            .await
            .unwrap_err();
        assert_eq!(error.0.kind, "draining");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);

        // A rollout can temporarily remove every Service endpoint between the
        // draining process and its replacement. This is still pre-admission.
        server.reply.lock().await.0 = StatusCode::SERVICE_UNAVAILABLE;
        *app.fleet.lock().await = None;
        let error = app
            .endpoints_for(&app.registry(), &placement(Resources::default()))
            .await
            .unwrap_err();
        assert_eq!(error.0.kind, "no_ready_worker");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);
        *server.reply.lock().await = (StatusCode::OK, status("unavailable"));
        *app.fleet.lock().await = None;
        let error = app
            .endpoints_for(&app.registry(), &placement(Resources::default()))
            .await
            .unwrap_err();
        assert_eq!(error.0.kind, "no_ready_worker");
        assert!(error.0.retry_safe);

        let mut starting = status("starting");
        starting.as_object_mut().unwrap().remove("cpu");
        starting.as_object_mut().unwrap().remove("memory_bytes");
        server.reply.lock().await.1 = starting;
        *app.fleet.lock().await = None;
        let error = app
            .endpoints_for(&app.registry(), &placement(Resources::default()))
            .await
            .unwrap_err();
        assert_eq!(error.0.kind, "starting");
        assert!(error.0.retry_safe);
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);

        server.reply.lock().await.1 = status("ready");
        *app.fleet.lock().await = None;
        assert_eq!(
            app.endpoints_for(&app.registry(), &placement(Resources::default()))
                .await
                .unwrap(),
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
            disk_bytes: Some(small.disk_bytes),
            pool: Some("small".into()),
            max_sessions: Some(32),
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
        let endpoints: Vec<_> = workers
            .iter()
            .map(|worker| worker.endpoint.clone())
            .collect();
        workers[2].cpu = 4;
        workers[3].ready = false;
        assert_eq!(
            ranked_endpoints(&workers, small, "small", &endpoints, 32, false),
            ["first", "second"]
        );
        // An idle pool with many sessions beats a busy worker with one session.
        workers[0].active_queries = 1;
        workers[0].open_sessions = 1;
        workers[1].open_sessions = 20;
        assert_eq!(
            ranked_endpoints(&workers, small, "small", &endpoints, 32, false),
            ["second", "first"]
        );
        // Queued work counts even between statements; open sessions break ties.
        workers[0].active_queries = 0;
        workers[0].queued_queries = 2;
        workers[1].active_queries = 1;
        assert_eq!(
            ranked_endpoints(&workers, small, "small", &endpoints, 32, false),
            ["second", "first"]
        );
        workers[0].queued_queries = 1;
        assert_eq!(
            ranked_endpoints(&workers, small, "small", &endpoints, 32, false),
            ["first", "second"]
        );
        workers[0].ready = false;
        workers[1].ready = false;
        assert!(ranked_endpoints(&workers, small, "small", &endpoints, 32, false).is_empty());
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
