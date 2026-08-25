//! The thread that notices when something has gone wrong quietly.
//!
//! Three failure modes here are silent by construction, and all three look
//! exactly like normal operation from the outside:
//!
//! - **A latched background error.** On ENOSPC or an SST checksum failure
//!   RocksDB puts the database into a read-only state and stays there. Every
//!   subsequent `write` fails, the process keeps serving, and nothing crashes.
//!   A Postgres deployment gets a pod that dies and a database another node can
//!   take over; an embedded one just fails every mutation, forever, until
//!   somebody looks. So a background error is escalated to the same
//!   [`ShutdownSignal`] the relational backends raise on lease loss.
//! - **A failing WAL flush in [`SyncMode::Interval`].** A write is acknowledged
//!   once its records are in RocksDB's buffer; the flusher moves them. If the
//!   flusher keeps failing, acknowledged-but-unwritten data accumulates without
//!   bound — not "up to one interval" as the mode advertises. Time since the
//!   last *successful* flush is therefore a durability measurement, and past a
//!   multiple of the interval it is escalated too.
//! - **A backup worker that has stopped.** Failures are events, and an event
//!   you can miss. Age is a level, and this publishes it on its own timer
//!   rather than the backup interval's, so the number keeps moving whether or
//!   not the backup worker is alive to move it.

use std::{
    path::PathBuf,
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
    time::{
        Duration,
        Instant,
    },
};

use common::shutdown::ShutdownSignal;

use crate::{
    backup,
    metrics,
    options::{
        self,
        SyncMode,
    },
    Inner,
};

/// How many intervals a WAL flush may go unmade before the process is stopped.
/// One missed flush is a transient; several in a row means the durability
/// contract this mode advertises is no longer being met.
const FLUSH_FAILURE_INTERVALS: u32 = 10;

pub(crate) struct HealthMonitor {
    stop: Arc<Signal>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct Signal {
    stopped: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

impl HealthMonitor {
    pub(crate) fn spawn(
        db: Weak<Inner>,
        shutdown: ShutdownSignal,
        backup_dir: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let stop = Arc::new(Signal {
            stopped: AtomicBool::new(false),
            lock: Mutex::new(()),
            wake: Condvar::new(),
        });
        let signal = stop.clone();
        let interval = *options::HEALTH_POLL_INTERVAL;
        let handle = std::thread::Builder::new()
            .name("rocksdb-health".to_string())
            .spawn(move || loop {
                {
                    let guard = match signal.lock.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    let _ = signal.wake.wait_timeout(guard, interval);
                }
                if signal.stopped.load(Ordering::Relaxed) {
                    break;
                }
                let Some(inner) = db.upgrade() else {
                    break;
                };
                check(&inner, &shutdown, backup_dir.as_deref());
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB health monitor: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(handle.into()),
        })
    }

    pub(crate) fn stop(&self) {
        self.stop.stopped.store(true, Ordering::Relaxed);
        self.stop.wake.notify_all();
        let handle = match self.handle.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
            if handle.thread().id() == std::thread::current().id() {
                std::mem::forget(handle);
            } else {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

fn check(inner: &Inner, shutdown: &ShutdownSignal, backup_dir: Option<&std::path::Path>) {
    if let Some(errors) = background_errors(inner)
        && errors > 0
    {
        metrics::log_background_errors(errors);
        // Read-only is not a state to keep serving from: the deployment's
        // recovery path is a restart onto the same volume, or a restore, and
        // neither begins while the process pretends to be healthy.
        shutdown.signal(anyhow::anyhow!(
            "RocksDB has latched {errors} background error(s) and is refusing writes; stopping so \
             the deployment restarts or fails over rather than failing every mutation"
        ));
        return;
    }

    if let SyncMode::Interval(interval) = inner.sync
        && let Some(flusher) = inner.wal_flusher.get()
    {
        let since = flusher.since_last_success();
        metrics::log_wal_flush_age(since.as_secs_f64());
        let budget = interval.saturating_mul(FLUSH_FAILURE_INTERVALS);
        if since > budget {
            shutdown.signal(anyhow::anyhow!(
                "the RocksDB write-ahead log has not been flushed for {:?}, past {} intervals; \
                 acknowledged writes are accumulating unwritten, so stopping rather than \
                 continuing to acknowledge them",
                since,
                FLUSH_FAILURE_INTERVALS,
            ));
            return;
        }
    }

    if let Some(dir) = backup_dir
        && let Ok(generations) = backup::list(dir)
        && let Some(newest) = generations.last()
    {
        metrics::log_backup_age(backup::age_seconds(newest.timestamp));
    }
}

fn background_errors(inner: &Inner) -> Option<u64> {
    // The property is per-column-family; any one of them latching stops writes
    // to the whole database, so the first non-zero answer is enough.
    for name in crate::keys::ALL_COLUMN_FAMILIES {
        let cf = inner.cf(name).ok()?;
        match inner
            .db
            .property_int_value_cf(&cf, rocksdb::properties::BACKGROUND_ERRORS)
        {
            Ok(Some(n)) if n > 0 => return Some(n),
            Ok(_) => {},
            Err(e) => {
                tracing::warn!("could not read RocksDB background-errors: {e}");
                return None;
            },
        }
    }
    Some(0)
}

/// Tracks when the write-ahead log was last successfully flushed, so a
/// persistently failing flusher is a measurable duration rather than an absent
/// metric.
pub(crate) struct FlushClock {
    last_success: Mutex<Instant>,
}

impl FlushClock {
    pub(crate) fn new() -> Self {
        Self {
            last_success: Mutex::new(Instant::now()),
        }
    }

    pub(crate) fn record_success(&self) {
        match self.last_success.lock() {
            Ok(mut guard) => *guard = Instant::now(),
            Err(poisoned) => *poisoned.into_inner() = Instant::now(),
        }
    }

    pub(crate) fn since_last_success(&self) -> Duration {
        let last = match self.last_success.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        };
        last.elapsed()
    }
}
