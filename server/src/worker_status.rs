//! Process-local observations, independent of the DuckDB execution queue.
//!
//! Query guards belong to worker envelopes, not HTTP futures: a disconnected
//! caller does not remove work that the engine still has to handle.
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Status(Arc<Inner>);

struct Inner {
    worker_id: String,
    boot_id: Uuid,
    placement: OnceLock<Value>,
    ready: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    queued: HashMap<Uuid, Instant>,
    active: usize,
    sessions: usize,
}

pub(crate) struct QueryGuard {
    status: Status,
    stage: Stage,
}

enum Stage {
    Queued(Uuid),
    Active,
}

impl Status {
    pub fn new(worker_id: String, ready: Arc<AtomicBool>, draining: Arc<AtomicBool>) -> Self {
        Self(Arc::new(Inner {
            worker_id,
            boot_id: Uuid::new_v4(),
            placement: OnceLock::new(),
            ready,
            draining,
            counts: Mutex::new(Counts::default()),
        }))
    }

    pub fn boot_id(&self) -> Uuid {
        self.0.boot_id
    }

    /// Published once, only after all worker startup checks have succeeded.
    pub fn initialize(&self, placement: Value) {
        self.0
            .placement
            .set(placement)
            .expect("worker placement is initialized only once");
    }

    pub fn enqueue(&self) -> QueryGuard {
        let id = Uuid::new_v4();
        self.0
            .counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queued
            .insert(id, Instant::now());
        QueryGuard {
            status: self.clone(),
            stage: Stage::Queued(id),
        }
    }

    pub fn set_sessions(&self, count: usize) {
        self.0
            .counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sessions = count;
    }

    pub fn has_queries(&self) -> bool {
        let counts = self.0.counts.lock().unwrap_or_else(|e| e.into_inner());
        counts.active > 0 || !counts.queued.is_empty()
    }

    pub fn snapshot(&self) -> Value {
        let mut value = self.0.placement.get().cloned().unwrap_or_else(
            || json!({"worker_id": self.0.worker_id, "worker_boot_id": self.0.boot_id}),
        );
        let counts = self.0.counts.lock().unwrap_or_else(|e| e.into_inner());
        let draining = self.0.draining.load(Ordering::Acquire);
        let initialized = self.0.placement.get().is_some();
        let ready = initialized && self.0.ready.load(Ordering::Acquire);
        let state = if draining {
            "draining"
        } else if !initialized {
            "starting"
        } else if !ready {
            "unavailable"
        } else if counts.active > 0 {
            "busy"
        } else {
            "ready"
        };
        value["boot_id"] = json!(self.0.boot_id);
        value["state"] = json!(state);
        value["ready"] = json!(ready && !draining);
        value["draining"] = json!(draining);
        value["active_queries"] = json!(counts.active);
        value["queued_queries"] = json!(counts.queued.len());
        value["open_sessions"] = json!(counts.sessions);
        value["oldest_queued_ms"] = json!(
            counts
                .queued
                .values()
                .min()
                .map(|at| { u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX) })
        );
        value
    }
}

impl QueryGuard {
    /// Transition atomically so snapshots never lose an admitted query between
    /// the queue and the handler. Active includes admission and settlement.
    pub fn start(mut self) -> Self {
        let mut counts = self
            .status
            .0
            .counts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Stage::Queued(id) = self.stage {
            counts.queued.remove(&id);
            counts.active += 1;
            self.stage = Stage::Active;
        }
        drop(counts);
        self
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        let mut counts = self
            .status
            .0
            .counts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match self.stage {
            Stage::Queued(id) => {
                counts.queued.remove(&id);
            }
            Stage::Active => counts.active -= 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_preserves_placement_and_boot_identity() {
        let ready = Arc::new(AtomicBool::new(false));
        let draining = Arc::new(AtomicBool::new(false));
        let status = Status::new("small".into(), ready.clone(), draining.clone());
        let starting = status.snapshot();
        assert_eq!(starting["state"], "starting");
        assert_eq!(starting["ready"], false);
        status.initialize(json!({"worker_id":"small", "cpu":2, "worker_boot_id":status.boot_id()}));
        ready.store(true, Ordering::Release);
        assert_eq!(status.snapshot()["state"], "ready");
        let query = status.enqueue().start();
        assert_eq!(status.snapshot()["state"], "busy");
        status.set_sessions(3);
        draining.store(true, Ordering::Release);
        let drained = status.snapshot();
        assert_eq!(drained["state"], "draining");
        assert_eq!(drained["ready"], false);
        assert_eq!(drained["open_sessions"], 3);
        assert_eq!(drained["cpu"], 2);
        assert_eq!(drained["boot_id"], starting["boot_id"]);
        assert_eq!(drained["boot_id"], drained["worker_boot_id"]);
        draining.store(false, Ordering::Release);
        ready.store(false, Ordering::Release);
        assert_eq!(status.snapshot()["state"], "unavailable");
        drop(query);
        assert!(!status.has_queries());
        assert_ne!(
            Status::new("small".into(), ready, draining).boot_id(),
            status.boot_id()
        );
    }

    #[test]
    fn queue_age_tracks_remaining_work_and_guards_release_on_unwind() {
        let status = Status::new(
            "small".into(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
        );
        let first = status.enqueue();
        let second = status.enqueue();
        {
            let mut counts = status.0.counts.lock().unwrap();
            let Stage::Queued(id) = first.stage else {
                panic!("expected queued guard")
            };
            counts
                .queued
                .insert(id, Instant::now() - std::time::Duration::from_secs(5));
        }
        assert!(status.snapshot()["oldest_queued_ms"].as_u64().unwrap() >= 5000);
        let active = first.start();
        assert_eq!(status.snapshot()["active_queries"], 1);
        assert_eq!(status.snapshot()["queued_queries"], 1);
        assert!(status.snapshot()["oldest_queued_ms"].as_u64().unwrap() < 5000);
        drop(second);
        assert!(status.snapshot()["oldest_queued_ms"].is_null());
        let panicked = std::panic::catch_unwind(move || {
            let _active = active;
            panic!("simulated worker failure");
        });
        assert!(panicked.is_err());
        assert!(!status.has_queries());
    }
}
