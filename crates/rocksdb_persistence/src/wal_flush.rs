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
//! that never collects. The `Weak` also makes the shutdown race safe rather
//! than merely unlikely: `upgrade` fails as soon as the last strong reference
//! is gone, so the thread either gets a live database or gets nothing, and can
//! never observe one mid-drop.

use std::{
    sync::{
        atomic::{
            AtomicBool,
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
    metrics,
    Inner,
};

pub(crate) struct WalFlusher {
    /// Set to stop the thread. Paired with `wake` so a stop does not have to
    /// wait out the remaining interval.
    stop: Arc<Signal>,
    handle: Mutex<Option<JoinHandle<()>>>,
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
                            timer.finish();
                        },
                        Err(e) => {
                            timer.finish_developer_error();
                            // Log rather than crash: the next tick retries, and
                            // the writes are still in RocksDB's buffer. A
                            // persistent failure here shows up as a growing
                            // gap in `rocksdb_wal_flush_seconds`.
                            tracing::error!("rocksdb WAL flush failed: {e}");
                        },
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB WAL flusher: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
        })
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
            let _ = handle.join();
        }
    }
}

impl Drop for WalFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
