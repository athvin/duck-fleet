//! Bounded metadata actors independent of the serial DuckDB engine.
//! Durable queue rows own work; response waiters and wakeups never own permits.
use crate::{
    ApiError, Output, Reply,
    credentials::{self, Identity},
    failure,
    query::{Acceptance, Protocol, QueryRequest},
    queue::{self, CloseIntent, WorkerEpoch},
    worker, worker_status,
};
use axum::http::StatusCode;
use postgres::{Client, NoTls};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

const INTAKE_CAPACITY: usize = 16;
const CONTROL_CAPACITY: usize = 32;
const POLL: Duration = Duration::from_millis(250);
const AUTH_INTERVAL: Duration = Duration::from_secs(5);

type Job = Box<dyn FnOnce(&mut Option<Client>, &str) + Send>;
#[derive(Clone)]
struct Lane(mpsc::SyncSender<Job>);
impl Lane {
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Client) -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let (reply, receiver) = oneshot::channel();
        self.0
            .try_send(Box::new(move |client, dsn| {
                let result = connection(client, dsn).and_then(f);
                let _ = reply.send(result);
            }))
            .map_err(|_| {
                ApiError::new(
                    failure::CAPACITY,
                    "metadata lane full or closed; operation not submitted",
                )
            })?;
        receiver.await.map_err(|_| ApiError::internal())?
    }
}

fn connection<'a>(slot: &'a mut Option<Client>, dsn: &str) -> Result<&'a mut Client, ApiError> {
    if slot
        .as_mut()
        .is_none_or(|client| client.check_connection().is_err())
    {
        let mut config: postgres::Config = dsn.parse().map_err(|_| ApiError::internal())?;
        config.connect_timeout(Duration::from_secs(3));
        config.tcp_user_timeout(Duration::from_secs(3));
        config.keepalives_idle(Duration::from_secs(2));
        config.keepalives_interval(Duration::from_secs(1));
        config.keepalives_retries(2);
        let mut client = config.connect(NoTls).map_err(|_| ApiError::internal())?;
        // These connections serve bounded control transactions only, not the
        // foreground ledger. Preserve DSN options, including search_path.
        client.batch_execute("SET statement_timeout=3000; SET lock_timeout=1000; SET idle_in_transaction_session_timeout=3000")
            .map_err(|_| ApiError::internal())?;
        *slot = Some(client);
    }
    Ok(slot.as_mut().expect("connection was established"))
}

#[derive(Default)]
struct State {
    sessions: HashSet<Uuid>,
    closing: HashSet<Uuid>,
    close_ready: HashSet<Uuid>,
    close_waiters: HashMap<Uuid, Vec<oneshot::Sender<Reply>>>,
    queries: HashMap<Uuid, Waiting>,
    cancellations: HashMap<Uuid, Arc<AtomicBool>>,
}
struct Waiting {
    result: oneshot::Sender<Reply>,
    token: Uuid,
}

pub(crate) struct Service {
    pub epoch: WorkerEpoch,
    intake: Lane,
    control: Lane,
    state: Mutex<State>,
    pub wake: Notify,
    pub draining: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    stopped: AtomicBool,
    online: AtomicBool,
    active: Arc<Mutex<Option<worker::Active>>>,
    status: worker_status::Status,
}

impl Service {
    pub fn new(
        dsn: String,
        epoch: WorkerEpoch,
        ready: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
        active: Arc<Mutex<Option<worker::Active>>>,
        status: worker_status::Status,
    ) -> anyhow::Result<Arc<Self>> {
        let (intake_tx, intake_rx) = mpsc::sync_channel(INTAKE_CAPACITY);
        let (control_tx, control_rx) = mpsc::sync_channel(CONTROL_CAPACITY);
        let service = Arc::new(Self {
            epoch,
            intake: Lane(intake_tx),
            control: Lane(control_tx),
            state: Mutex::new(State::default()),
            wake: Notify::new(),
            draining,
            ready,
            stopped: AtomicBool::new(false),
            online: AtomicBool::new(false),
            active,
            status,
        });
        for (name, receiver, controls) in [
            ("duckfleet-intake", intake_rx, false),
            ("duckfleet-control", control_rx, true),
        ] {
            let weak = Arc::downgrade(&service);
            let dsn = dsn.clone();
            std::thread::Builder::new()
                .name(name.into())
                .spawn(move || run_lane(weak, receiver, dsn, controls))?;
        }
        Ok(service)
    }

    pub fn start(&self) {
        self.online.store(true, Ordering::Release);
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
    pub fn stop(&self) {
        let _active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        self.stopped.store(true, Ordering::Release);
        self.ready.store(false, Ordering::Release);
        self.wake.notify_one();
    }
    pub fn has_waiting_queries(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queries
            .is_empty()
    }
    pub fn register_local(&self, id: Uuid) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sessions
            .insert(id);
    }
    pub fn take_close_ready(&self) -> Vec<Uuid> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .close_ready
            .drain()
            .collect()
    }
    pub fn session_closed(&self, id: Uuid) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sessions.remove(&id);
        state.closing.remove(&id);
        state.close_ready.remove(&id);
        if let Some(waiters) = state.close_waiters.remove(&id) {
            for waiter in waiters {
                let _ = waiter.send(Ok(Output::Json(json!({"closed":true}))));
            }
        }
    }
    pub fn session_closing(&self, id: Uuid) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.sessions.contains(&id) || state.closing.contains(&id)
    }
    pub fn cancellation(&self, id: Uuid) -> Arc<AtomicBool> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancellations
            .entry(id)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone()
    }
    fn notify_cancel(&self, id: Uuid) {
        if let Some(canceled) = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancellations
            .get(&id)
        {
            canceled.store(true, Ordering::Release);
        }
        worker::interrupt(&self.active, None, Some(id), "canceled");
        self.wake.notify_one();
    }
    fn discard_waiter(&self, id: Uuid, token: Uuid) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .queries
            .get(&id)
            .is_some_and(|waiting| waiting.token == token)
        {
            state.queries.remove(&id);
        }
    }
    pub fn complete(&self, id: Uuid, result: Reply) {
        let waiting = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.cancellations.remove(&id);
            state.queries.remove(&id)
        };
        if let Some(waiting) = waiting {
            let _ = waiting.result.send(result);
        }
        self.wake.notify_one();
    }

    pub async fn submit(
        self: &Arc<Self>,
        session: Uuid,
        request: QueryRequest,
        actor: Identity,
        protocol: Protocol,
    ) -> (Acceptance, Reply) {
        if let Err(error) = request.validate() {
            return (Acceptance::NotAccepted, Err(error));
        }
        if !self.ready.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
            return (
                Acceptance::NotAccepted,
                Err(ApiError::new(
                    failure::STARTING,
                    "worker unavailable; no query submitted",
                )),
            );
        }
        if self.draining.load(Ordering::Acquire) {
            return (
                Acceptance::NotAccepted,
                Err(ApiError::new(
                    failure::DRAINING,
                    "worker is draining; no query submitted",
                )),
            );
        }
        let id = request.request_id;
        let (reply, receiver) = oneshot::channel();
        let token = Uuid::new_v4();
        let owns_waiter = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.sessions.contains(&session) {
                return (
                    Acceptance::NotAccepted,
                    Err(ApiError::new(failure::UNKNOWN_SESSION, "unknown session")),
                );
            }
            if state.queries.contains_key(&id) {
                false
            } else {
                // One durable outstanding request/session plus bounded intake.
                if state.queries.len() >= state.sessions.len() + INTAKE_CAPACITY {
                    return (
                        Acceptance::NotAccepted,
                        Err(ApiError::new(
                            failure::CAPACITY,
                            "submission waiters full; query not submitted",
                        )),
                    );
                }
                state.queries.insert(
                    id,
                    Waiting {
                        result: reply,
                        token,
                    },
                );
                true
            }
        };
        let pending = owns_waiter.then(|| PendingWaiter {
            service: Arc::downgrade(self),
            id,
            token,
            accepted: false,
        });
        let service = self.clone();
        let submission = self
            .intake
            .call(move |client| {
                let mut pending = pending;
                if service.stopped.load(Ordering::Acquire)
                    || service.draining.load(Ordering::Acquire)
                {
                    return Err(ApiError::new(
                        failure::DRAINING,
                        "worker is draining; query not submitted",
                    ));
                }
                let result =
                    queue::submit(client, &service.epoch, session, request, &actor, protocol)?;
                if matches!(result, queue::SubmitResult::Accepted { .. })
                    && let Some(pending) = pending.as_mut()
                {
                    pending.accepted = true;
                }
                service.wake.notify_one();
                Ok(result)
            })
            .await;
        match submission {
            Ok(queue::SubmitResult::Accepted {
                request_id,
                sequence,
                authority_generation,
            }) => {
                tracing::debug!(%request_id, sequence, authority_generation, "durable query accepted");
                if request_id != id {
                    self.stop();
                    return (Acceptance::Unknown, Err(ApiError::internal()));
                }
                if !owns_waiter {
                    return (Acceptance::Unknown, Err(ApiError::internal()));
                }
                (
                    Acceptance::Accepted,
                    receiver.await.unwrap_or_else(|_| Err(ApiError::internal())),
                )
            }
            result => match result {
                Ok(queue::SubmitResult::Duplicate(value)) => (
                    Acceptance::Accepted,
                    Ok(Output::JsonResponse(StatusCode::CONFLICT, value)),
                ),
                Ok(queue::SubmitResult::Refused { request_id, error }) => {
                    if request_id != id {
                        self.stop();
                        return (Acceptance::Unknown, Err(ApiError::internal()));
                    }
                    (Acceptance::Accepted, Err(error))
                }
                Err(error) => {
                    let acceptance = if error.0 == failure::UNAVAILABLE {
                        Acceptance::Unknown
                    } else {
                        Acceptance::NotAccepted
                    };
                    (acceptance, Err(error))
                }
                Ok(queue::SubmitResult::Accepted { .. }) => unreachable!(),
            },
        }
    }

    pub async fn cancel(self: &Arc<Self>, id: Uuid, actor: Identity) -> Reply {
        let service = self.clone();
        self.control
            .call(move |client| {
                let receipt = queue::request_cancel(client, id, &actor)?;
                if receipt["cancel_requested"].as_bool() == Some(true) {
                    service.notify_cancel(id);
                }
                Ok(Output::Json(receipt))
            })
            .await
    }

    fn register_close_waiter(
        &self,
        id: Uuid,
        waiter: oneshot::Sender<Reply>,
    ) -> Result<bool, ApiError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // A prior close can finish between the DB predicate and publication.
        // Never park a new waiter after its actual engine close ACK.
        if !state.sessions.contains(&id) {
            return Ok(false);
        }
        state.closing.insert(id);
        let waiters = state.close_waiters.entry(id).or_default();
        waiters.retain(|waiter| !waiter.is_closed());
        if waiters.len() >= CONTROL_CAPACITY {
            return Err(ApiError::new(failure::CAPACITY, "close already pending"));
        }
        waiters.push(waiter);
        self.wake.notify_one();
        Ok(true)
    }

    pub async fn close(self: &Arc<Self>, id: Uuid, reason: CloseRequest) -> Reply {
        let service = self.clone();
        let (waiter, receiver) = oneshot::channel();
        let closing = self
            .control
            .call(move |client| {
                let authority = queue::authorities(client, &service.epoch)
                    .map_err(|_| ApiError::internal())?
                    .into_iter()
                    .find(|row| row.session_id == id);
                let Some(authority) = authority else {
                    if service
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .sessions
                        .contains(&id)
                    {
                        return Err(ApiError::internal());
                    }
                    return Ok(false);
                };
                let intent = match reason {
                    CloseRequest::Explicit => CloseIntent::Explicit,
                    CloseRequest::Credential {
                        credential_id,
                        generation,
                    } => {
                        if authority.identity.credential_id != credential_id
                            || generation.is_some_and(|g| g != authority.generation)
                        {
                            return Ok(false);
                        }
                        CloseIntent::Credential {
                            credential_id,
                            authority_generation: authority.generation,
                        }
                    }
                    CloseRequest::Idle => {
                        if authority.outstanding.is_some()
                            || authority.touched_at.elapsed().unwrap_or_default()
                                < Duration::from_secs(300)
                        {
                            return Ok(false);
                        }
                        CloseIntent::Idle {
                            authority_generation: authority.generation,
                            touched_at: authority.touched_at,
                        }
                    }
                };
                let closing = queue::begin_close(client, id, &service.epoch, &intent)
                    .map_err(|_| ApiError::internal())?;
                if closing {
                    service.register_close_waiter(id, waiter)
                } else {
                    Ok(false)
                }
            })
            .await?;
        if closing {
            receiver.await.unwrap_or_else(|_| Err(ApiError::internal()))
        } else {
            let exists = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions
                .contains(&id);
            Ok(Output::Json(
                json!({"closed": !exists, "authority_changed": exists}),
            ))
        }
    }

    fn poll(&self, client: &mut Client, check_authorities: bool) -> anyhow::Result<()> {
        let report = queue::poll_control(
            client,
            &self.epoch,
            self.draining.load(Ordering::Acquire),
            32,
        )?;
        self.status.set_queue(report.counts);
        for id in report.cancel_active {
            self.notify_cancel(id);
        }
        for (id, value) in report.terminal {
            self.notify_cancel(id);
            let status = value
                .get("failure_kind")
                .and_then(Value::as_str)
                .and_then(failure::by_kind)
                .map_or(StatusCode::SERVICE_UNAVAILABLE, |failure| failure.http);
            self.complete(id, Ok(Output::JsonResponse(status, value)));
        }
        if !report.close_ready.is_empty() {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for id in report.close_ready {
                if state.sessions.contains(&id) {
                    state.closing.insert(id);
                    state.close_ready.insert(id);
                }
            }
            self.wake.notify_one();
        }
        if check_authorities {
            let authorities = queue::authorities(client, &self.epoch)?;
            let ids: Vec<_> = authorities
                .iter()
                .filter_map(|row| row.identity.credential_id.clone())
                .collect();
            let valid = credentials::resolve_ids(client, &ids).unwrap_or_else(|error| {
                // Fail closed for named cleanup authorities, while still using
                // the durable generation predicate before closing a connection.
                tracing::warn!(
                    cause = crate::secrets::Redactor::from_env().scrub(&format!("{error:#}")),
                    "credential cleanup lookup unavailable; closing named authorities conditionally"
                );
                Vec::new()
            });
            let started = Instant::now();
            for row in authorities {
                if row.closing {
                    continue;
                }
                let invalid = row.identity.credential_id.is_some()
                    && !valid.iter().any(|item| {
                        item.credential_id == row.identity.credential_id
                            && item.principal == row.identity.principal
                            && !item.expired()
                    });
                let intent = if invalid {
                    Some(CloseIntent::Credential {
                        credential_id: row.identity.credential_id,
                        authority_generation: row.generation,
                    })
                } else if row.outstanding.is_none()
                    && row.touched_at.elapsed().unwrap_or_default() >= Duration::from_secs(300)
                {
                    Some(CloseIntent::Idle {
                        authority_generation: row.generation,
                        touched_at: row.touched_at,
                    })
                } else {
                    None
                };
                if let Some(intent) = intent {
                    if queue::begin_close(client, row.session_id, &self.epoch, &intent)? {
                        self.state
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .closing
                            .insert(row.session_id);
                    }
                    if started.elapsed() >= Duration::from_millis(250) {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

struct PendingWaiter {
    service: Weak<Service>,
    id: Uuid,
    token: Uuid,
    accepted: bool,
}
impl Drop for PendingWaiter {
    fn drop(&mut self) {
        if !self.accepted
            && let Some(service) = self.service.upgrade()
        {
            service.discard_waiter(self.id, self.token);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CloseRequest {
    Explicit,
    Credential {
        credential_id: Option<String>,
        generation: Option<i64>,
    },
    Idle,
}

fn run_lane(service: Weak<Service>, receiver: mpsc::Receiver<Job>, dsn: String, controls: bool) {
    let mut client = None;
    let mut last_poll = Instant::now();
    let mut last_auth = Instant::now();
    loop {
        match receiver.recv_timeout(POLL) {
            Ok(job) => job(&mut client, &dsn),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        let Some(service) = service.upgrade() else {
            break;
        };
        if service.stopped.load(Ordering::Acquire) {
            break;
        }
        if controls && service.online.load(Ordering::Acquire) && last_poll.elapsed() >= POLL {
            let check_auth = last_auth.elapsed() >= AUTH_INTERVAL;
            let result = connection(&mut client, &dsn)
                .map_err(|error| anyhow::anyhow!("{error:?}"))
                .and_then(|client| service.poll(client, check_auth));
            if let Err(error) = result {
                service.ready.store(false, Ordering::Release);
                tracing::warn!(
                    cause = crate::secrets::Redactor::from_env().scrub(&format!("{error:#}")),
                    "queue control pass failed; keeping durable work pending"
                );
            } else {
                let _active = service.active.lock().unwrap_or_else(|e| e.into_inner());
                if !service.stopped.load(Ordering::Acquire) {
                    service.ready.store(true, Ordering::Release);
                }
            }
            last_poll = Instant::now();
            if check_auth {
                last_auth = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> Arc<Service> {
        let ready = Arc::new(AtomicBool::new(false));
        let draining = Arc::new(AtomicBool::new(false));
        let status =
            worker_status::Status::new("test".into(), ready.clone(), draining.clone(), json!({}));
        Service::new(
            "postgres://unused".into(),
            WorkerEpoch {
                worker_id: "test".into(),
                boot_id: status.boot_id(),
            },
            ready,
            draining,
            Arc::new(Mutex::new(None)),
            status,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn blocked_full_intake_does_not_consume_reserved_control_lane() {
        let service = service();
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        service
            .intake
            .0
            .send(Box::new(move |_, _| {
                let _ = entered.send(());
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            }))
            .unwrap();
        entered_rx.await.unwrap();
        for _ in 0..INTAKE_CAPACITY {
            service.intake.0.try_send(Box::new(|_, _| {})).unwrap();
        }
        assert!(service.intake.0.try_send(Box::new(|_, _| {})).is_err());
        let (controlled, controlled_rx) = oneshot::channel();
        service
            .control
            .0
            .try_send(Box::new(move |_, _| {
                let _ = controlled.send(());
            }))
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), controlled_rx).await;
        let _ = release.send(());
        result.unwrap().unwrap();
        service.stop();
    }

    #[test]
    fn disconnected_waiter_never_releases_accepted_work() {
        let service = service();
        let id = Uuid::new_v4();
        let token = Uuid::new_v4();
        let (result, receiver) = oneshot::channel();
        service
            .state
            .lock()
            .unwrap()
            .queries
            .insert(id, Waiting { result, token });
        let pending = PendingWaiter {
            service: Arc::downgrade(&service),
            id,
            token,
            accepted: true,
        };
        drop(receiver);
        drop(pending);
        assert!(service.state.lock().unwrap().queries.contains_key(&id));
        // A stale rejected submit cannot remove a newer in-flight waiter.
        service.discard_waiter(id, Uuid::new_v4());
        assert!(service.state.lock().unwrap().queries.contains_key(&id));
        service.complete(id, Ok(Output::Json(json!({}))));
        assert!(!service.state.lock().unwrap().queries.contains_key(&id));
        service.stop();
    }

    #[tokio::test]
    async fn repeated_close_does_not_park_a_waiter_after_engine_ack() {
        let service = service();
        let id = Uuid::new_v4();
        service.register_local(id);
        let (first, receiver) = oneshot::channel();
        assert!(service.register_close_waiter(id, first).unwrap());
        // The first close finishes after a repeat passed its DB predicate,
        // before that repeat can publish its local waiter.
        service.session_closed(id);
        assert!(
            matches!(receiver.await.unwrap().unwrap(), Output::Json(value) if value["closed"] == true)
        );
        let (late, receiver) = oneshot::channel();
        assert!(!service.register_close_waiter(id, late).unwrap());
        assert!(receiver.await.is_err());
        assert!(service.state.lock().unwrap().close_waiters.is_empty());
        service.stop();
    }

    #[test]
    fn rejected_or_failed_intake_reclaims_only_its_own_waiter() {
        let service = service();
        let id = Uuid::new_v4();
        let token = Uuid::new_v4();
        let (result, receiver) = oneshot::channel();
        service
            .state
            .lock()
            .unwrap()
            .queries
            .insert(id, Waiting { result, token });
        drop(receiver);
        drop(PendingWaiter {
            service: Arc::downgrade(&service),
            id,
            token,
            accepted: false,
        });
        assert!(service.state.lock().unwrap().queries.is_empty());
        service.stop();
    }
}
