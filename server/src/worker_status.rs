//! Process-local observations, independent of the DuckDB execution queue.
//!
//! Durable live-permit snapshots own pressure. HTTP waiter lifetime does not
//! release queued or executing work; metadata failure retains the last snapshot.
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Status(Arc<Inner>);

struct Inner {
    worker_id: String,
    boot_id: Uuid,
    placement: OnceLock<Value>,
    configured: Value,
    ready: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    queue: crate::queue::Counts,
    sessions: usize,
}

impl Status {
    pub fn new(
        worker_id: String,
        ready: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
        configured: Value,
    ) -> Self {
        Self(Arc::new(Inner {
            worker_id,
            boot_id: Uuid::new_v4(),
            placement: OnceLock::new(),
            configured,
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

    pub fn set_queue(&self, queue: crate::queue::Counts) {
        self.0
            .counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue = queue;
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
        counts.queue.queued + counts.queue.preparing + counts.queue.executing > 0
    }

    pub fn snapshot(&self) -> Value {
        let mut value = self.0.placement.get().cloned().unwrap_or_else(|| {
            let mut value = self.0.configured.clone();
            value["worker_id"] = json!(self.0.worker_id);
            value["worker_boot_id"] = json!(self.0.boot_id);
            value
        });
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
        } else if counts.queue.preparing + counts.queue.executing > 0 {
            "busy"
        } else {
            "ready"
        };
        value["boot_id"] = json!(self.0.boot_id);
        value["state"] = json!(state);
        value["ready"] = json!(ready && !draining);
        value["draining"] = json!(draining);
        value["active_queries"] = json!(counts.queue.preparing + counts.queue.executing);
        value["preparing_queries"] = json!(counts.queue.preparing);
        value["executing_queries"] = json!(counts.queue.executing);
        value["queued_bytes"] = json!(counts.queue.queued_bytes);
        value["queued_queries"] = json!(counts.queue.queued);
        value["open_sessions"] = json!(counts.sessions);
        value["oldest_queued_ms"] = json!(counts.queue.oldest_queued_ms);
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_preserves_placement_and_boot_identity() {
        let ready = Arc::new(AtomicBool::new(false));
        let draining = Arc::new(AtomicBool::new(false));
        let status = Status::new(
            "small".into(),
            ready.clone(),
            draining.clone(),
            json!({"pool":"interactive","max_sessions":16,"queue_capacity":8,"max_queued_queries":12,"max_queued_bytes":1048576}),
        );
        let starting = status.snapshot();
        assert_eq!(starting["state"], "starting");
        assert_eq!(starting["ready"], false);
        assert_eq!(starting["pool"], "interactive");
        assert_eq!(starting["max_sessions"], 16);
        assert_eq!(starting["queue_capacity"], 8);
        assert_eq!(starting["max_queued_queries"], 12);
        assert_eq!(starting["max_queued_bytes"], 1048576);
        status.initialize(json!({"worker_id":"small", "cpu":2, "worker_boot_id":status.boot_id()}));
        ready.store(true, Ordering::Release);
        assert_eq!(status.snapshot()["state"], "ready");
        status.set_queue(crate::queue::Counts {
            preparing: 1,
            ..Default::default()
        });
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
        status.set_queue(crate::queue::Counts::default());
        assert!(!status.has_queries());
        assert_ne!(
            Status::new("small".into(), ready, draining, json!({})).boot_id(),
            status.boot_id()
        );
    }

    #[test]
    fn pressure_comes_from_durable_permits_including_preparation() {
        let status = Status::new(
            "small".into(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
            json!({}),
        );
        status.set_queue(crate::queue::Counts {
            queued: 3,
            preparing: 1,
            executing: 0,
            oldest_queued_ms: Some(5000),
            queued_bytes: 4000,
        });
        let copy = status.clone();
        drop(copy); // Dropping an observer or HTTP waiter releases no capacity.
        let snapshot = status.snapshot();
        assert_eq!(snapshot["queued_queries"], 3);
        assert_eq!(snapshot["active_queries"], 1);
        assert_eq!(snapshot["oldest_queued_ms"], 5000);
        assert_eq!(snapshot["queued_bytes"], 4000);
        assert!(status.has_queries());
        status.set_queue(crate::queue::Counts::default());
        assert!(!status.has_queries());
        assert!(status.snapshot()["oldest_queued_ms"].is_null());
    }
}
