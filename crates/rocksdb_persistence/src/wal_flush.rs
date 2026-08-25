//! The background thread behind [`SyncMode::Interval`].
//!
//! In interval mode RocksDB is opened with `manual_wal_flush`, so a write puts
//! its WAL records in RocksDB's own buffer and returns without touching the
//! kernel. Something has to move them, and this is it: one thread per database,
//! calling `flush_wal(true)` — write the buffer through to the file, then fsync
//! it — on a fixed interval.
//!
//! The thread holds a [`Weak`] reference to the database, never a strong one.
//! A strong reference would keep the database alive for as long as the thread
//! ran, and the thread runs until the database is dropped, which is a cycle
//! that never collects. `upgrade` also fails as soon as the last strong
//! reference is gone, so the thread either gets a live database or gets
//! nothing, and can never observe one mid-drop.
//!
//! It does *not* make the whole teardown race-free. The thread holds a strong
//! reference for the duration of a tick, so an owner releasing the last other
//! reference inside that window leaves the worker holding the last one — and
//! dropping `Inner` then runs on this thread, reaching [`WalFlusher::stop`],
//! which is why `stop` refuses to join itself.

use std::{
    sync::{
        atomic::{
            AtomicBool,
            AtomicU32,
            Ordering,
        },
        Arc,
        Condvar,
        Mutex,
        Weak,
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    health::FlushClock,
    metrics,
    Inner,
};

pub(crate) struct WalFlusher {
    /// Set to stop the thread. Paired with `wake` so a stop does not have to
    /// wait out the remaining interval.
    stop: Arc<Signal>,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// When the log was last written through to disk. A failing flush leaves
    /// acknowledged writes in RocksDB's buffer, so this — not the presence of
    /// error logs — is the measurement of whether the mode's loss bound holds.
    clock: Arc<FlushClock>,
    /// Flushes that have failed since the last success. Distinguishes "the disk
    /// is slow" from "the disk is broken", which the elapsed clock alone
    /// cannot.
    failures: Arc<AtomicU32>,
}

struct Signal {
    stopped: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

impl WalFlusher {
    pub(crate) fn spawn(db: Weak<Inner>, interval: Duration) -> anyhow::Result<Self> {
        let stop = Arc::new(Signal {
            stopped: AtomicBool::new(false),
            lock: Mutex::new(()),
            wake: Condvar::new(),
        });
        let signal = stop.clone();
        let clock = Arc::new(FlushClock::new());
        let thread_clock = clock.clone();
        let failures = Arc::new(AtomicU32::new(0));
        let thread_failures = failures.clone();
        let handle = std::thread::Builder::new()
            .name("rocksdb-wal-flusher".to_string())
            .spawn(move || {
                loop {
                    {
                        let guard = match signal.lock.lock() {
                            Ok(guard) => guard,
                            // A poisoned lock means a previous holder panicked
                            // while holding nothing but a unit value. There is
                            // no state to be inconsistent, so carry on.
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        let _ = signal.wake.wait_timeout(guard, interval);
                    }
                    if signal.stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    // If the database is gone there is nothing left to flush,
                    // and nothing to flush it into.
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    let timer = metrics::wal_flush_timer();
                    match inner.db.flush_wal(true) {
                        Ok(()) => {
                            thread_clock.record_success();
                            thread_failures.store(0, Ordering::Relaxed);
                            timer.finish();
                        },
                        Err(e) => {
                            thread_failures.fetch_add(1, Ordering::Relaxed);
                            timer.finish_developer_error();
                            // Log rather than crash *here*: one failed flush is
                            // a transient, the next tick retries, and the
                            // writes are still buffered. A persistent failure
                            // is a different thing entirely — it means
                            // acknowledged writes are piling up unwritten — and
                            // the health monitor escalates that from
                            // `since_last_success`, which is a level rather
                            // than the absence of an event.
                            tracing::error!("rocksdb WAL flush failed: {e}");
                        },
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB WAL flusher: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
            clock,
            failures,
        })
    }

    /// Flushes that have failed since the last success.
    pub(crate) fn consecutive_failures(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }

    /// How long since the log was last written through to disk.
    pub(crate) fn since_last_success(&self) -> std::time::Duration {
        self.clock.since_last_success()
    }

    /// Stop the thread and wait for it. Idempotent — `shutdown` calls it so a
    /// flush cannot race the close, and `Drop` calls it again for the paths
    /// that never reach `shutdown`.
    pub(crate) fn stop(&self) {
        self.stop.stopped.store(true, Ordering::Relaxed);
        self.stop.wake.notify_all();
        let handle = match self.handle.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
            // The worker holds a strong `Arc<Inner>` for the duration of a
            // tick. If the owner released the last other reference in that
            // window, `Inner` is dropped *on this thread*, which reaches here —
            // and joining yourself deadlocks (the platform returns EDEADLK and
            // `join` panics). Signalling is enough in that case: the loop is
            // already unwinding.
            if handle.thread().id() == std::thread::current().id() {
                // Dropping the handle here would detach the thread, which is
                // fine — it is this thread, and it is already unwinding out of
                // the loop. Joining it would deadlock.
                drop(handle);
            } else {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for WalFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
