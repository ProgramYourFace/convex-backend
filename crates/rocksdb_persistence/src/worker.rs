//! Stop-and-wait shared by the three background workers.
//!
//! # Why a deadline
//!
//! Teardown used to be three bare `JoinHandle::join()` calls. That is correct
//! only if every worker is guaranteed to return, and this crate's own design
//! says one of them is not: the health poller exists on a separate thread
//! precisely because `DBImpl::GetIntProperty` can block *permanently* on a
//! wedged volume. The backup worker is worse — it can be inside an
//! uninterruptible `open(2)` against a hung backup mount, which no signal
//! interrupts.
//!
//! An unbounded join on those threads turns "the volume wedged" into "the
//! process cannot exit", on the exact path the wedge escalation walks: the
//! health monitor signals shutdown, the backend drops its persistence, and the
//! drop blocks forever on the thread the wedge already stopped. Measured: with
//! a backup worker parked in the kernel, `drop` did not return in five seconds
//! out of five attempts.
//!
//! So a worker signals its own exit, and the caller *waits* for that signal
//! with a deadline rather than joining blind. A thread that misses the deadline
//! is detached and reported. The engine is then deliberately leaked rather than
//! closed — see [`Signal::wait_for_exit`]'s callers — because closing a
//! database out from under a thread still using it is worse than leaking it in
//! a process that is on its way out anyway.

use std::{
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Condvar,
        Mutex,
    },
    thread::JoinHandle,
    time::{
        Duration,
        Instant,
    },
};

/// The stop flag, the park, and the exit notification for one worker group.
pub(crate) struct Signal {
    stopped: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
    /// How many of this group's threads have left their loop.
    exited: Mutex<usize>,
    exited_wake: Condvar,
}

impl Signal {
    pub(crate) fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            lock: Mutex::new(()),
            wake: Condvar::new(),
            exited: Mutex::new(0),
            exited_wake: Condvar::new(),
        }
    }

    /// Waits one interval, or until stopped. Returns whether the caller should
    /// leave its loop.
    pub(crate) fn park(&self, interval: Duration) -> bool {
        {
            let guard = match self.lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Re-checked under the mutex the flag is set beneath. Without that,
            // a `stop()` racing the park is lost and the caller sleeps a full
            // interval past its own shutdown.
            let (guard, _) = self
                .wake
                .wait_timeout_while(guard, interval, |_| !self.stopped.load(Ordering::Relaxed))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(guard);
        }
        self.stopped.load(Ordering::Relaxed)
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    /// Whether `n` threads have already left their loops. Does not wait.
    #[cfg(test)]
    pub(crate) fn has_exited(&self, n: usize) -> bool {
        let guard = match self.exited.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard >= n
    }

    /// Asks the group to stop, without waiting.
    pub(crate) fn request_stop(&self) {
        {
            // Set under the same mutex the waiter re-checks it beneath, so the
            // notification cannot land in the gap before it parks.
            let guard = match self.lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            self.stopped.store(true, Ordering::Relaxed);
            drop(guard);
        }
        self.wake.notify_all();
    }

    /// Called by a worker as it leaves its loop.
    pub(crate) fn mark_exited(&self) {
        {
            let mut guard = match self.exited.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *guard += 1;
        }
        self.exited_wake.notify_all();
    }

    /// Waits for `n` threads to leave their loops, up to `timeout`.
    ///
    /// Returns whether they all did. A `false` means at least one thread is
    /// still inside a call that has not come back — on a wedged volume, one
    /// that may never come back — and the caller must not join it.
    pub(crate) fn wait_for_exit(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = match self.exited.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        while *guard < n {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, _) = self
                .exited_wake
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next;
        }
        true
    }
}

/// Stops a worker group and waits out its threads, bounded.
///
/// Returns whether every thread left. Handles for threads that did are joined,
/// which is immediate; the rest are detached, because joining a thread that has
/// not returned is the hang this module exists to avoid.
pub(crate) fn stop_and_wait(
    signal: &Signal,
    handles: Vec<JoinHandle<()>>,
    timeout: Duration,
    what: &str,
) -> bool {
    signal.request_stop();
    // A worker whose own tick is running this call cannot wait for itself. It
    // is inside its group's exit path already, so there is nothing to wait for.
    let current = std::thread::current().id();
    let (mine, others): (Vec<_>, Vec<_>) = handles
        .into_iter()
        .partition(|handle| handle.thread().id() == current);
    for handle in mine {
        drop(handle);
    }
    if others.is_empty() {
        return true;
    }
    if signal.wait_for_exit(others.len(), timeout) {
        for handle in others {
            let _ = handle.join();
        }
        true
    } else {
        tracing::error!(
            "the RocksDB {what} did not stop within {timeout:?}; detaching it rather than \
             blocking teardown, and leaving the database open so it cannot be closed underneath a \
             thread still using it"
        );
        for handle in others {
            drop(handle);
        }
        false
    }
}
