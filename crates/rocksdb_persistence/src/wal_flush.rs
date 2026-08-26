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
            AtomicU32,
            Ordering,
        },
        Arc,
        Mutex,
        Weak,
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    health::FlushClock,
    metrics,
    options,
    worker,
    Inner,
};

pub(crate) struct WalFlusher {
    /// Set to stop the thread. Paired with `wake` so a stop does not have to
    /// wait out the remaining interval.
    stop: Arc<worker::Signal>,
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

impl WalFlusher {
    pub(crate) fn spawn(db: Weak<Inner>, interval: Duration) -> anyhow::Result<Self> {
        let stop = Arc::new(worker::Signal::new());
        let signal = stop.clone();
        let clock = Arc::new(FlushClock::new());
        let thread_clock = clock.clone();
        let failures = Arc::new(AtomicU32::new(0));
        let thread_failures = failures.clone();
        let handle = std::thread::Builder::new()
            .name("rocksdb-wal-flusher".to_string())
            .spawn(move || {
                loop {
                    if signal.park(interval) {
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
                signal.mark_exited();
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

    /// Stops the thread and waits for it, bounded. Idempotent — `shutdown`
    /// calls it so a flush cannot race the close, and `Drop` calls it again for
    /// the paths that never reach `shutdown`.
    ///
    /// Returns whether the thread actually left. `false` means it is still
    /// inside `flush_wal` against a volume that has not answered, and the
    /// caller must not close the database underneath it.
    /// Whether every thread in this group has left its loop.
    ///
    /// Exposed so a test can assert the teardown contract — that dropping the
    /// owning handle stops the workers — without scanning process-wide thread
    /// names, which sees the other tests in the same binary and flakes.
    #[cfg(test)]
    pub(crate) fn has_stopped(&self) -> bool {
        self.stop.has_exited(1)
    }

    pub(crate) fn stop(&self) -> bool {
        let handles = match self.handle.lock() {
            Ok(mut guard) => guard.take().into_iter().collect(),
            Err(poisoned) => poisoned.into_inner().take().into_iter().collect(),
        };
        worker::stop_and_wait(
            &self.stop,
            handles,
            *options::SHUTDOWN_TIMEOUT,
            "write-ahead log flusher",
        )
    }
}

impl Drop for WalFlusher {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
