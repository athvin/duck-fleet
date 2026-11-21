//! Exclude overlapping processes for one stable recovery identity. The ownership
//! connection lives on a separate thread so long DuckDB statements cannot stop
//! its heartbeat. PostgreSQL expires an orphan even when the dead pod's TCP
//! connection is not promptly reaped. Ownership is never silently reacquired.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use postgres::{Client, NoTls};

const HEARTBEAT: Duration = Duration::from_secs(2);
// A requested probe can follow one already-running 3s heartbeat and take 3s of
// its own. Missing this acknowledgement is terminal, even if the socket later
// becomes usable: a replacement may already have recovered this worker's state.
const PROBE_WAIT: Duration = Duration::from_secs(7);
const STOP_WAIT: Duration = Duration::from_secs(1);

enum Command {
    Probe(SyncSender<bool>),
    Stop,
}

#[derive(Default)]
struct Notification {
    callback: Option<Box<dyn Fn() + Send + Sync>>,
    fired: bool,
}

#[derive(Default)]
struct State {
    lost: AtomicBool,
    stopping: AtomicBool,
    notification: Mutex<Notification>,
}

impl State {
    fn lose(&self) {
        if !self.stopping.load(Ordering::Acquire) {
            // Publish before the callback: a worker holding its active-slot lock
            // can observe loss before the callback acquires that same lock.
            self.lost.store(true, Ordering::Release);
            self.notify();
        }
    }

    fn notify(&self) {
        let callback = {
            let mut notification = self
                .notification
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if notification.fired || !self.lost.load(Ordering::Acquire) {
                return;
            }
            let callback = notification.callback.take();
            if callback.is_some() {
                notification.fired = true;
            }
            callback
        };
        if let Some(callback) = callback {
            callback();
        }
    }
}

// Panic or unexpected thread exit must also fail closed. A normal Guard drop
// marks stopping before waking the thread and never invokes the loss callback.
struct OnExit(Arc<State>);
impl Drop for OnExit {
    fn drop(&mut self) {
        self.0.lose();
    }
}

fn heartbeat(mut client: Client, state: &State, commands: Receiver<Command>) {
    loop {
        if state.stopping.load(Ordering::Acquire) || state.lost.load(Ordering::Acquire) {
            break;
        }
        let probe = match commands.recv_timeout(HEARTBEAT) {
            Ok(Command::Stop) | Err(RecvTimeoutError::Disconnected) => break,
            Ok(Command::Probe(reply)) => Some(reply),
            Err(RecvTimeoutError::Timeout) => None,
        };
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        // An actual SQL statement unambiguously resets idle_session_timeout.
        // No transaction is left open on this connection, and no other code can
        // unlock/reconnect it while the worker still owns engine sessions.
        let alive = client.query_one("SELECT 1", &[]).is_ok();
        if !alive {
            state.lose();
        }
        if let Some(reply) = probe {
            let _ = reply.try_send(alive && !state.lost.load(Ordering::Acquire));
        }
        if !alive {
            break;
        }
    }
    // Dropping Client closes the session and releases the advisory lock. The
    // caller sends its completion acknowledgement only after this function and
    // Client's destructor return.
}

pub(crate) struct Guard {
    commands: SyncSender<Command>,
    state: Arc<State>,
    stopped: Receiver<()>,
    thread: Option<JoinHandle<()>>,
}

impl Guard {
    /// Different identities do not block each other. A hash collision can only
    /// delay an unrelated startup, never grant two owners the same lock.
    pub fn try_claim(mut client: Client, worker_id: &str) -> Result<Option<Self>> {
        client.batch_execute("SET statement_timeout=3000; SET idle_session_timeout=10000")?;
        let owned: bool = client
            .query_one(
                "SELECT pg_try_advisory_lock(4283122,hashtext($1))",
                &[&worker_id],
            )?
            .get(0);
        if !owned {
            return Ok(None);
        }
        let state = Arc::new(State::default());
        let background = state.clone();
        let (commands, receiver) = mpsc::sync_channel(1);
        let (finished, stopped) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("duckfleet-ownership".into())
            .spawn(move || {
                let exit = OnExit(background.clone());
                heartbeat(client, &background, receiver);
                drop(exit);
                let _ = finished.try_send(());
            })
            .context("starting worker ownership heartbeat")?;
        Ok(Some(Self {
            commands,
            state,
            stopped,
            thread: Some(thread),
        }))
    }

    pub fn acquire(dsn: &str, worker_id: &str, wait: Duration) -> Result<Self> {
        let redactor = crate::secrets::Redactor::from_env();
        let mut config: postgres::Config =
            redactor.guard("worker ownership configuration", dsn.parse())?;
        config.connect_timeout(Duration::from_secs(3));
        config.tcp_user_timeout(Duration::from_secs(3));
        config.keepalives(true);
        config.keepalives_idle(Duration::from_secs(3));
        config.keepalives_interval(Duration::from_secs(1));
        config.keepalives_retries(2);
        config.application_name(&format!("duckfleet-owner/{worker_id}"));
        let start = Instant::now();
        loop {
            let client = redactor.guard("worker ownership connection", config.connect(NoTls))?;
            if let Some(guard) =
                Self::try_claim(client, worker_id).context("claiming exclusive worker identity")?
            {
                return Ok(guard);
            }
            anyhow::ensure!(
                start.elapsed() < wait,
                "worker identity {worker_id} is still owned by another process; refusing startup recovery"
            );
            if start.elapsed() < Duration::from_secs(1) {
                tracing::info!(
                    worker_id,
                    "waiting for previous worker process to release its recovery identity"
                );
            }
            std::thread::sleep(
                Duration::from_millis(200).min(wait.saturating_sub(start.elapsed())),
            );
        }
    }

    /// Register the terminal-loss hook. Registration after loss invokes it
    /// immediately; repeated probes/notifications invoke it at most once. A
    /// graceful drop is not loss. Callbacks must finish promptly and must not
    /// recursively call check, which waits for the heartbeat thread.
    pub fn on_loss(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        let immediate = {
            let mut notification = self
                .state
                .notification
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if notification.fired {
                return;
            }
            let callback: Box<dyn Fn() + Send + Sync> = Box::new(callback);
            if self.state.lost.load(Ordering::Acquire) {
                // Claim late notification while holding the registration lock,
                // so the background exit cannot steal it and return from this
                // method before the promised immediate callback has run.
                notification.fired = true;
                notification.callback = None;
                Some(callback)
            } else {
                notification.callback = Some(callback);
                None
            }
        };
        if let Some(callback) = immediate {
            callback();
        }
    }

    /// Loss is terminal. This flag is also set by the independent heartbeat
    /// while the worker is busy, so checking it never waits for engine work.
    pub fn is_closed(&self) -> bool {
        self.state.lost.load(Ordering::Acquire)
    }

    /// Every execution boundary obtains a fresh server acknowledgement from the
    /// same owner connection. The request queue and acknowledgement wait are
    /// bounded; failure is latched and never causes ownership reconnection.
    pub fn check(&mut self) -> bool {
        if self.is_closed() {
            return false;
        }
        let (reply, response) = mpsc::sync_channel(1);
        if self.commands.try_send(Command::Probe(reply)).is_err()
            || !matches!(response.recv_timeout(PROBE_WAIT), Ok(true))
        {
            self.state.lose();
            return false;
        }
        !self.is_closed()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.state.stopping.store(true, Ordering::Release);
        let _ = self.commands.try_send(Command::Stop);
        // Never make process exit depend on a blackholed socket's destructor.
        // If shutdown stalls, detach the thread; server-side idle expiry is the
        // independent backstop. The guard remains the worker's last field so
        // engine sessions have already rolled back before this begins.
        if self.stopped.recv_timeout(STOP_WAIT).is_ok()
            && let Some(thread) = self.thread.take()
        {
            let _ = thread.join();
        }
    }
}
