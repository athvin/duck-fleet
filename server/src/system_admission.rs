//! Admission survives cancellation of the caller waiting for a metadata read.
//!
//! Closing a PostgreSQL socket is not acknowledgement that its active statement
//! stopped. An abandoned operation therefore keeps its permit until the original
//! request deadline plus the configured three-second server statement timeout.
//! A confirmed COMMIT/ROLLBACK can release it sooner. This conservative cooldown
//! limits cancellation churn under the server-timeout/normal-transport assumption;
//! it is not proof of backend termination or a distributed execution fence.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::{
    runtime::Handle,
    sync::{Notify, OwnedSemaphorePermit},
    time::{Instant, sleep_until},
};

const SERVER_STATEMENT_TIMEOUT: Duration = Duration::from_secs(3);

struct State {
    idle: AtomicBool,
    changed: Notify,
}

/// A completion signal owns no admission permit. Clones can acknowledge the same
/// transaction, including after a caller has abandoned its waiting future.
#[derive(Clone)]
pub(crate) struct Completion(Arc<State>);

impl Completion {
    /// Call only after PostgreSQL acknowledges COMMIT/ROLLBACK, or when no command
    /// could have been dispatched. Returning a handled error, dropping a partial
    /// RowStream, and transmitting CancelRequest are not sufficient evidence.
    pub fn confirmed_idle(&self) {
        if !self.0.idle.swap(true, Ordering::AcqRel) {
            self.0.changed.notify_waiters();
        }
    }

    fn is_idle(&self) -> bool {
        self.0.idle.load(Ordering::Acquire)
    }

    async fn wait_for_idle(&self) {
        // Register before checking the persistent flag. A confirmation before
        // registration is observed by the flag; one after registration wakes us.
        // notify_waiters itself does not retain a notification for a future waiter.
        let changed = self.0.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.is_idle() {
            changed.await;
        }
    }
}

/// The sole owner of an acquired slot until safe release or result delivery.
/// Move this value into parser work so canceling its JoinHandle cannot free the
/// slot while that bounded blocking work is still running.
pub(crate) struct Permit {
    owned: Option<OwnedSemaphorePermit>,
    completion: Completion,
    release_at: Instant,
    runtime: Handle,
}

impl Permit {
    pub fn new(owned: OwnedSemaphorePermit, original_deadline: Instant) -> (Self, Completion) {
        let completion = Completion(Arc::new(State {
            idle: AtomicBool::new(false),
            changed: Notify::new(),
        }));
        (
            Self {
                owned: Some(owned),
                completion: completion.clone(),
                release_at: original_deadline + SERVER_STATEMENT_TIMEOUT,
                // The request creates the lease inside its runtime; retaining the
                // handle also makes Drop safe on its blocking parser thread.
                runtime: Handle::current(),
            },
            completion,
        )
    }

    /// Transfer the original slot into SystemResult after acknowledged database
    /// completion. The response then retains it until delivery or response drop.
    pub fn into_owned(mut self) -> OwnedSemaphorePermit {
        assert!(
            self.completion.is_idle(),
            "system result cannot take an unconfirmed database admission permit"
        );
        self.owned.take().expect("admission permit is owned once")
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Some(owned) = self.owned.take() else {
            return;
        };
        if self.completion.is_idle() || Instant::now() >= self.release_at {
            return; // `owned` releases here; no cleanup task is needed.
        }
        let completion = self.completion.clone();
        let release_at = self.release_at;
        // Exactly one task can retire this lease, and the task owns its existing
        // permit. Therefore outstanding cleanup is bounded by admitted slots,
        // rather than by the number of canceled HTTP/socket requests.
        self.runtime.spawn(async move {
            tokio::select! {
                _ = completion.wait_for_idle() => {}
                _ = sleep_until(release_at) => {}
            }
            drop(owned);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Semaphore;

    async fn acquire(slots: &Arc<Semaphore>, deadline: Instant) -> (Permit, Completion) {
        Permit::new(slots.clone().acquire_owned().await.unwrap(), deadline)
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_keeps_capacity_through_original_deadline_and_server_timeout() {
        let slots = Arc::new(Semaphore::new(1));
        let start = Instant::now();
        let (permit, _completion) = acquire(&slots, start + Duration::from_secs(3)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        drop(permit);
        tokio::task::yield_now().await;
        assert!(slots.clone().try_acquire_owned().is_err());
        // Dropping at t=2 does not start another full six-second window.
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(slots.available_permits(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn handled_partial_stream_error_without_acknowledgement_retains_its_slot() {
        let slots = Arc::new(Semaphore::new(2));
        let deadline = Instant::now() + Duration::from_secs(3);
        let (partial, _partial_completion) = acquire(&slots, deadline).await;
        let (finished, finished_completion) = acquire(&slots, deadline).await;
        // One operation consumed its result and committed. The other returned a
        // byte-limit/conversion error without draining its backend row stream.
        finished_completion.confirmed_idle();
        drop(finished);
        drop(partial);
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 1);
        let replacement = slots.clone().try_acquire_owned().unwrap();
        assert!(slots.clone().try_acquire_owned().is_err());
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 1);
        drop(replacement);
        assert_eq!(slots.available_permits(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_completion_releases_normally_without_cooldown() {
        let slots = Arc::new(Semaphore::new(1));
        let (permit, completion) = acquire(&slots, Instant::now() + Duration::from_secs(3)).await;
        completion.confirmed_idle();
        assert_eq!(slots.available_permits(), 0);
        drop(permit);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_before_cleanup_first_poll_cannot_be_lost() {
        let slots = Arc::new(Semaphore::new(1));
        let (permit, completion) = acquire(&slots, Instant::now() + Duration::from_secs(3)).await;
        drop(permit);
        // notify_waiters runs before the spawned task registers. The persistent
        // state must still let the task release without waiting for its timer.
        completion.confirmed_idle();
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_after_cleanup_waits_releases_early_once() {
        let slots = Arc::new(Semaphore::new(1));
        let (permit, completion) = acquire(&slots, Instant::now() + Duration::from_secs(3)).await;
        let second = completion.clone();
        drop(permit);
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        completion.confirmed_idle();
        second.confirmed_idle();
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 1);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn successful_result_transfer_keeps_slot_until_output_is_dropped() {
        let slots = Arc::new(Semaphore::new(1));
        let (permit, completion) = acquire(&slots, Instant::now() + Duration::from_secs(3)).await;
        completion.confirmed_idle();
        let output_permit = permit.into_owned();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(slots.available_permits(), 0);
        drop(output_permit);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn parser_work_keeps_ownership_even_beyond_the_cleanup_deadline() {
        let slots = Arc::new(Semaphore::new(1));
        let (permit, _completion) = acquire(&slots, Instant::now() + Duration::from_secs(3)).await;
        // Dropping a spawn_blocking JoinHandle does not stop its work. Exercise
        // that actual ownership path, including Drop on the blocking thread.
        let (release, wait) = std::sync::mpsc::sync_channel::<()>(1);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let parser = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(permit);
            finished.send(()).unwrap();
        });
        started.await.unwrap();
        drop(parser);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(slots.available_permits(), 0);
        release.send(()).unwrap();
        done.await.unwrap();
        assert_eq!(slots.available_permits(), 1);
    }
}
