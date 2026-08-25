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
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
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

/// Multiple of the failure budget after which a flusher that has simply gone
/// quiet — no successes, no failures — is treated as dead.
const FLUSH_SILENCE_MULTIPLIER: u32 = 6;

/// Multiple of the stall timeout past which a blocked write is escalated
/// whatever the engine reports about itself. Backpressure that has not drained
/// in this long is indistinguishable from a hang from every angle that matters.
const STALL_ESCALATION_CEILING: u32 = 10;

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
        let started = Instant::now();
        let handle = std::thread::Builder::new()
            .name("rocksdb-health".to_string())
            .spawn(move || {
                let mut progress = Progress::new(Instant::now());
                loop {
                    {
                        let guard = match signal.lock.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        // Re-checked under the mutex; see the same pattern in
                        // `wal_flush`. Without it a `stop()` racing the park is
                        // lost and the caller blocks for a full poll interval.
                        let (guard, _) = signal
                            .wake
                            .wait_timeout_while(guard, interval, |_| {
                                !signal.stopped.load(Ordering::Relaxed)
                            })
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        drop(guard);
                    }
                    if signal.stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    check(
                        &inner,
                        &shutdown,
                        backup_dir.as_deref(),
                        started,
                        &mut progress,
                    );
                }
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB health monitor: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(handle.into()),
        })
    }

    pub(crate) fn stop(&self) {
        {
            // Set under the same mutex the waiter re-checks the flag beneath,
            // so the notification cannot be issued into the gap before it
            // parks.
            let guard = match self.stop.lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            self.stop.stopped.store(true, Ordering::Relaxed);
            drop(guard);
        }
        self.stop.wake.notify_all();
        let handle = match self.handle.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
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

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

fn check(
    inner: &Inner,
    shutdown: &ShutdownSignal,
    backup_dir: Option<&std::path::Path>,
    started: Instant,
    progress: &mut Progress,
) {
    let now = Instant::now();
    // Computed once per poll: `Progress` advances its own clock when it reads,
    // so calling the predicate twice would move the baseline mid-check.
    let backpressure = engine_is_applying_backpressure(inner, progress, now);
    // A write that cannot make progress does not fail — it blocks. On a full
    // volume RocksDB stalls the writer indefinitely rather than returning an
    // error, so the counter above stays at zero for as long as the write never
    // completes, and each stalled write parks a blocking-pool thread. The
    // oldest in-flight write is the signal that actually moves.
    let stalled = inner.write_watch.oldest_in_flight();
    metrics::log_oldest_write(stalled.map_or(0.0, |d| d.as_secs_f64()));
    if let Some(waiting) = stalled
        && waiting > *options::WRITE_STALL_TIMEOUT
    {
        // RocksDB also blocks writers *deliberately*, as backpressure: the
        // write buffer manager is built with `allow_stall`, and the L0
        // slowdown/stop triggers are at their defaults. That kind of stall
        // drains as compaction catches up, and killing the process for it would
        // turn an ingest burst into an outage. `is-write-stopped` says which
        // kind this is; only an unexplained one is a hang.
        // Past the ceiling nothing the engine reports buys any more time. A
        // stall this long is an outage whatever its cause, and the deployment's
        // recovery path cannot begin while the process still answers probes.
        let past_ceiling = waiting > *options::WRITE_STALL_TIMEOUT * STALL_ESCALATION_CEILING;
        if backpressure && !past_ceiling {
            tracing::warn!(
                "a RocksDB write has been in flight for {waiting:?}, but the engine reports \
                 deliberate write stalling — treating this as backpressure rather than a hang"
            );
        } else {
            shutdown.signal(anyhow::anyhow!(
                "a RocksDB write has been in flight for {waiting:?} with no background work \
                 completing, past the stall timeout. The engine blocks rather than failing when \
                 it cannot make progress — a full volume or a hung mount looks exactly like this \
                 — so stopping beats parking every writer forever"
            ));
            return;
        }
    }

    // Published on every poll, failing or not. A series that only appears once
    // the process is already being shut down is not something an alert can
    // watch — the same argument `log_backup_age` makes, applied here.
    let errors = background_errors(inner).unwrap_or(0);
    metrics::log_background_errors(errors);
    metrics::log_write_stopped(backpressure);
    if errors > 0 {
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
        // Two ways this ends badly, and they need different thresholds.
        //
        // Flushes *failing*: escalate once several in a row have failed. A
        // single fsync slower than the budget is ordinary on a contended volume
        // — at a 100 ms interval the budget is one second — so time alone would
        // be a self-inflicted outage.
        //
        // Flushes *stopping*: a thread that panicked, or is wedged inside
        // `flush_wal`, never increments the failure count, so the conjunction
        // above would stay silent forever while the mode kept acknowledging
        // writes that never reach the kernel. A much longer deadline catches
        // that without being trippable by a slow disk.
        let budget = interval.saturating_mul(FLUSH_FAILURE_INTERVALS);
        let failing = since > budget && flusher.consecutive_failures() >= FLUSH_FAILURE_INTERVALS;
        // With an absolute floor. At the interval the README uses as its worked
        // example — 100 ms — the multiplied budget is six seconds, and an fsync
        // tail that long on a throttled network volume is ordinary. The flusher
        // is *inside* `flush_wal` while that happens, so it records neither a
        // success nor a failure and this clock keeps climbing: without the
        // floor, a slow disk kills a healthy backend.
        let silence_deadline = budget
            .saturating_mul(FLUSH_SILENCE_MULTIPLIER)
            .max(*options::MIN_FLUSH_SILENCE);
        let stopped = since > silence_deadline;
        if failing || stopped {
            shutdown.signal(anyhow::anyhow!(
                "the RocksDB write-ahead log has not been flushed for {since:?} ({} failures \
                 since the last success); acknowledged writes are accumulating unwritten, so \
                 stopping rather than continuing to acknowledge them",
                flusher.consecutive_failures(),
            ));
            return;
        }
    }

    // Read from the worker's own record rather than listing the directory:
    // listing takes the backup directory lock, which the worker holds for the
    // whole of a backup, so polling it would fail backups and blind the gauge
    // during exactly the backups worth watching.
    if backup_dir.is_some()
        && let Some(worker) = inner.backup_worker.get()
    {
        // Emitted even when no generation exists yet, measured from process
        // start. A deployment whose backups have *never* worked — wrong path,
        // unwritable volume, a rejected ownership claim — is the case most
        // worth alerting on, and it is precisely the case where waiting for a
        // first generation would leave the series absent. An alert on a level
        // that is missing does not fire.
        let age = match worker.newest_backup_unix_secs() {
            Some(newest) => backup::age_seconds(newest),
            None => started.elapsed().as_secs_f64(),
        };
        metrics::log_backup_age(age);
    }
}

/// Reads the latched background error count.
///
/// `rocksdb.background-errors` is served per column family, but RocksDB only
/// ever increments it on the **default** one: `DBImpl` holds a single
/// `default_cf_internal_stats_`, and every `bg_error_count_` bump goes through
/// that. The five column families this backend defines are not the default one,
/// so asking any of them returns a permanent zero. Verified against a full
/// filesystem: the five read `0` while `default` read `3`.
///
/// `property_int_value` — no `_cf` — is the default family, so it is the only
/// call here that can ever be non-zero.
/// Whether the engine is deliberately holding writers back rather than hung.
///
/// `rocksdb.is-write-stopped` is `write_controller().IsStopped()`, and it does
/// not cover the stall this crate explicitly enables: the write buffer manager
/// is built with `allow_stall`, and `DBImpl::WriteBufferManagerStallWrites`
/// parks writers through `WriteThread::BeginWriteStall` without ever touching
/// the write controller. So the DB-wide memory stall — the first case the
/// escalation is meant to exclude — reads as zero there.
///
/// Running flushes and compactions are the missing signal — but only while they
/// are *finishing*. `num-running-*` reports that a job is **open**, which on a
/// wedged volume is permanently true: `BackgroundCallCompaction` increments the
/// counter before the job runs and decrements it only once its last file write
/// and fsync return, so a compaction blocked on a hung device pins the counter
/// above zero forever. Presence alone would therefore classify an indefinite
/// I/O hang as deliberate throttling and disable the escalation in exactly the
/// failure mode it exists for.
///
/// `rocksdb.current-super-version-number` advances when a flush or compaction
/// *completes* and installs a new superversion, so it separates "working
/// through a backlog" from "stuck holding a job open". A memory stall keeps
/// flushes completing and so keeps reading as backpressure, which is the case
/// this predicate was widened for in the first place.
fn engine_is_applying_backpressure(inner: &Inner, progress: &mut Progress, now: Instant) -> bool {
    let property = |name: &str| {
        inner
            .db
            .property_int_value(name)
            .ok()
            .flatten()
            .unwrap_or(0)
    };
    // Explicit throttling by the write controller. This drains by construction,
    // and needs no corroboration.
    if property("rocksdb.is-write-stopped") > 0 || property("rocksdb.actual-delayed-write-rate") > 0
    {
        return true;
    }
    let running = property("rocksdb.num-running-flushes") > 0
        || property("rocksdb.num-running-compactions") > 0;
    running && progress.completing_work(inner, now)
}

/// Tracks whether RocksDB's background machinery is still completing work.
///
/// Held across polls by the health thread, so "has anything finished lately" is
/// a measurement rather than an instantaneous reading.
pub(crate) struct Progress {
    last_super_version: Option<u64>,
    last_advance: Instant,
}

impl Progress {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            last_super_version: None,
            last_advance: now,
        }
    }

    /// Whether a flush or compaction has completed inside the stall budget.
    ///
    /// The window is the same budget the stall itself is measured against: a
    /// single legitimate compaction can easily outlast one poll interval, so
    /// requiring movement poll-to-poll would kill healthy backends under heavy
    /// merge load. Requiring movement within the stall timeout does not — it
    /// escalates only when *both* clocks have stopped, the writer's and the
    /// engine's.
    pub(crate) fn completing_work(&mut self, inner: &Inner, now: Instant) -> bool {
        // Per column family, and this backend's writes all land in named ones.
        // `property_int_value` — no `_cf` — reads the *default* family, whose
        // superversion never moves here, so it would report "nothing is
        // completing" on a perfectly healthy engine. This is the same per-CF
        // trap `background_errors` documents, inverted: that property is only
        // ever bumped on `default`, this one is only ever bumped off it.
        let current = crate::keys::ALL_COLUMN_FAMILIES
            .iter()
            .filter_map(|name| {
                let handle = inner.db.cf_handle(name)?;
                inner
                    .db
                    .property_int_value_cf(
                        &handle,
                        rocksdb::properties::CURRENT_SUPER_VERSION_NUMBER,
                    )
                    .ok()
                    .flatten()
            })
            .reduce(|a, b| a.saturating_add(b));
        if current.is_some() && current != self.last_super_version {
            self.last_super_version = current;
            self.last_advance = now;
        }
        now.saturating_duration_since(self.last_advance) < *options::WRITE_STALL_TIMEOUT
    }
}

fn background_errors(inner: &Inner) -> Option<u64> {
    match inner
        .db
        .property_int_value(rocksdb::properties::BACKGROUND_ERRORS)
    {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!("could not read RocksDB background-errors: {e}");
            None
        },
    }
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

/// Tracks how long the oldest in-flight write has been running.
///
/// RocksDB's response to a volume it cannot write to is to stall the writer,
/// not to return an error, and a stalled write never returns at all. That makes
/// it invisible to every error-shaped signal: no `Result` is produced, no
/// counter moves, and the process keeps answering health probes while its
/// blocking pool fills with parked writers. Duration is the only thing that
/// changes, so duration is what gets watched.
#[derive(Default)]
pub(crate) struct WriteWatch {
    in_flight: Mutex<BTreeMap<u64, Instant>>,
    next_id: AtomicU64,
}

impl WriteWatch {
    /// Marks a write as started. The returned guard clears it on drop, so a
    /// write that panics or is cancelled does not leave a phantom stall.
    pub(crate) fn begin(&self) -> WriteGuard<'_> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Recover from poisoning rather than skipping: a skipped *insert* is
        // merely a write this cannot see, but a skipped *remove* leaves an
        // entry that ages forever and eventually trips the stall shutdown on a
        // healthy process. Both halves recover, so neither can happen.
        let mut guard = match self.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(id, Instant::now());
        drop(guard);
        WriteGuard { watch: self, id }
    }

    /// How long the longest-running in-flight write has been going, if any.
    pub(crate) fn oldest_in_flight(&self) -> Option<Duration> {
        let guard = match self.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Ids are handed out before the timestamp is taken, so the lowest id is
        // not *strictly* the earliest instant. The skew is the width of one
        // `fetch_add`, which is irrelevant against a stall timeout measured in
        // minutes, and taking the minimum over every value would make this
        // O(n) on the health path for no gain.
        guard.values().next().map(Instant::elapsed)
    }
}

pub(crate) struct WriteGuard<'a> {
    watch: &'a WriteWatch,
    id: u64,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        let mut guard = match self.watch.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.remove(&self.id);
    }
}

#[cfg(test)]
mod progress_tests {
    use std::time::{
        Duration,
        Instant,
    };

    use super::Progress;
    use crate::{
        options,
        RocksDbPersistence,
    };

    /// The regression this exists for: classifying a stall as "deliberate"
    /// because a background job is *open* rather than *finishing*.
    ///
    /// `num-running-compactions` is incremented before a compaction runs and
    /// decremented only after its final fsync returns, so a job blocked on a
    /// hung volume pins it above zero indefinitely. Keying off presence alone
    /// therefore reported backpressure forever and disabled the stall
    /// escalation in precisely the failure mode it was written for. Progress
    /// has to be measured by completion, and completion has to time out.
    #[test]
    fn progress_goes_stale_when_nothing_completes() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
        let inner = &persistence.inner;

        let start = Instant::now();
        let mut progress = Progress::new(start);

        // First observation: fresh clock, inside the window.
        assert!(progress.completing_work(inner, start));

        // Nothing has completed since. Once the stall budget has elapsed with
        // the superversion unmoved, this must read false — that is what lets
        // `check` escalate a hang instead of excusing it.
        let stale = start + *options::WRITE_STALL_TIMEOUT + Duration::from_secs(1);
        assert!(
            !progress.completing_work(inner, stale),
            "an engine that has completed no background work for longer than the stall timeout \
             must not read as backpressure"
        );

        // A completed flush installs a new superversion, which restarts the
        // clock: a busy engine keeps reading as backpressure and is not killed.
        // The write matters — flushing an empty memtable is a no-op and
        // installs nothing.
        let globals = persistence.inner.cf(crate::keys::CF_GLOBALS)?;
        persistence
            .inner
            .db
            .put_cf(&globals, b"progress-test", b"1")?;
        persistence.inner.db.flush_cf(&globals)?;
        assert!(
            progress.completing_work(inner, stale),
            "a superversion advance must clear the stale reading"
        );
        Ok(())
    }
}
