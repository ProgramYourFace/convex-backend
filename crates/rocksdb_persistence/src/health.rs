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

/// Multiples of the configured interval below which the silence ceiling may
/// never fall, whatever it is set to. A healthy flusher is silent for about one
/// interval between ticks by construction.
const FLUSH_SILENCE_INTERVAL_FLOOR: u32 = 3;

pub(crate) struct HealthMonitor {
    stop: Arc<Signal>,
    /// Two threads, and the split is the point. See [`HealthMonitor::spawn`].
    handles: Mutex<Vec<JoinHandle<()>>>,
}

struct Signal {
    stopped: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

/// Records when the polling thread last completed a pass, so that a thread
/// which has blocked inside RocksDB is itself a measurable level rather than an
/// absence of one.
pub(crate) struct PollClock {
    last: Mutex<Instant>,
}

impl PollClock {
    fn new() -> Self {
        Self {
            last: Mutex::new(Instant::now()),
        }
    }

    fn completed(&self) {
        let mut guard = match self.last.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Instant::now();
    }

    fn since_last(&self) -> Duration {
        let guard = match self.last.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.elapsed()
    }
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
        let interval = *options::HEALTH_POLL_INTERVAL;
        let started = Instant::now();
        let poll_clock = Arc::new(PollClock::new());

        // Two threads, because one cannot do this job.
        //
        // Everything except the stall check has to ask RocksDB a question, and
        // `DBImpl::GetIntProperty` takes the engine's `mutex_`. A poller that
        // blocks in there stops polling — not for one pass, but permanently:
        // the loop is serial, so the next iteration never begins. The stall
        // ceiling needs roughly eighty consecutive polls to elapse at the
        // defaults, so a poller that parks on its first RocksDB call after a
        // volume wedges will never reach it. An earlier revision tried to fix
        // this by evaluating the stall check *first* in the pass, which buys
        // exactly one poll and no additional escalation opportunities.
        //
        // So the check that must survive a wedged engine runs on a thread that
        // never touches the engine. It reads `WriteWatch`, which is wall-clock
        // over a guard this crate takes and drops around each write, and the
        // flush clock, which the flusher stamps. Neither can block on I/O.
        //
        // The watchdog also publishes how long it has been since the poller
        // completed a pass, which is what makes a parked poller visible from
        // outside — the same argument the backup age makes, applied to the
        // monitor itself. The poller cannot publish that: a thread that has
        // stopped cannot report having stopped.
        let watchdog = {
            let signal = stop.clone();
            let db = db.clone();
            let shutdown = shutdown.clone();
            let poll_clock = poll_clock.clone();
            std::thread::Builder::new()
                .name("rocksdb-watchdog".to_string())
                .spawn(move || loop {
                    if park(&signal, interval) {
                        break;
                    }
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    metrics::log_health_poll_age(poll_clock.since_last().as_secs_f64());
                    if watch_writes(&inner, &shutdown) {
                        break;
                    }
                })
                .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB watchdog: {e}"))?
        };

        let poller = {
            let signal = stop.clone();
            std::thread::Builder::new()
                .name("rocksdb-health".to_string())
                .spawn(move || loop {
                    if park(&signal, interval) {
                        break;
                    }
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    check(&inner, &shutdown, backup_dir.as_deref(), started);
                    poll_clock.completed();
                })
                .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB health monitor: {e}"))?
        };

        Ok(Self {
            stop,
            handles: Mutex::new(vec![watchdog, poller]),
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
        let handles = match self.handles.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        for handle in handles {
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

/// Waits one interval, or until stopped. Returns whether the caller should
/// exit.
fn park(signal: &Signal, interval: Duration) -> bool {
    {
        let guard = match signal.lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Re-checked under the mutex; see the same pattern in `wal_flush`.
        // Without it a `stop()` racing the park is lost and the caller blocks
        // for a full poll interval.
        let (guard, _) = signal
            .wake
            .wait_timeout_while(guard, interval, |_| !signal.stopped.load(Ordering::Relaxed))
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(guard);
    }
    signal.stopped.load(Ordering::Relaxed)
}

/// The one check that has to survive a wedged engine, so it asks the engine
/// nothing.
///
/// A write that cannot make progress does not fail — it blocks. On a full
/// volume or a hung mount RocksDB stalls the writer indefinitely rather than
/// returning an error, so no counter moves and each stalled write parks a
/// blocking-pool thread. Duration is the only signal that keeps moving, and
/// `WriteWatch` measures it on a clock this crate owns: a guard taken and
/// dropped around each write, which RocksDB never touches and therefore cannot
/// latch, unlike every engine property that was tried before it.
///
/// Deliberately no attempt to tell backpressure from a hang. Five review rounds
/// each produced a new defect trying, because every property that could
/// distinguish them is updated by the machinery that has stopped. The ceiling
/// is instead set generously enough that real backpressure drains well inside
/// it.
///
/// Returns whether the caller should stop.
fn watch_writes(inner: &Inner, shutdown: &ShutdownSignal) -> bool {
    let oldest = inner.write_watch.oldest_in_flight();
    metrics::log_oldest_write(oldest.map_or(0.0, |d| d.as_secs_f64()));
    if let Some(waiting) = oldest
        && waiting > *options::WRITE_STALL_CEILING
    {
        shutdown.signal(anyhow::anyhow!(
            "a RocksDB write has been in flight for {waiting:?}, past the stall ceiling. The \
             engine blocks rather than failing when it cannot make progress — a full volume or a \
             hung mount looks exactly like this — so stopping beats parking every writer forever"
        ));
        return true;
    }
    false
}

/// How long the write-ahead log may go unflushed — no successes and no failures
/// — before the flusher is presumed dead.
///
/// Bounded at both ends, and the upper bound is itself floored by the interval
/// it bounds. A healthy flusher waits the whole interval before each tick, so
/// the time since its last success climbs to about one interval every cycle; a
/// ceiling below that would read the healthy steady state as a dead flusher and
/// restart the pod onto the same configuration, forever.
fn silence_deadline(interval: Duration) -> Duration {
    interval
        .saturating_mul(FLUSH_FAILURE_INTERVALS)
        .saturating_mul(FLUSH_SILENCE_MULTIPLIER)
        .max(*options::MIN_FLUSH_SILENCE)
        .min(
            (*options::MAX_FLUSH_SILENCE)
                .max(*options::MIN_FLUSH_SILENCE)
                .max(interval.saturating_mul(FLUSH_SILENCE_INTERVAL_FLOOR)),
        )
}

/// The checks that need to ask RocksDB something, on the thread that is allowed
/// to block doing it. The stall ceiling is deliberately not among them — see
/// [`watch_writes`] and the note in [`HealthMonitor::spawn`].
fn check(
    inner: &Inner,
    shutdown: &ShutdownSignal,
    backup_dir: Option<&std::path::Path>,
    started: Instant,
) {
    // Published on every poll, failing or not. A series that only appears once
    // the process is already being shut down is not something an alert can
    // watch — the same argument `log_backup_age` makes, applied here.
    let errors = background_errors(inner).unwrap_or(0);
    metrics::log_background_errors(errors);
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
        // Bounded at both ends: the floor keeps a short interval from turning
        // an ordinary fsync tail into a kill, the ceiling keeps a long one from
        // granting an hour of silence in a mode that advertises at most one
        // interval of loss.
        // Bounded at both ends, and the upper bound is itself floored by the
        // interval it bounds. A healthy flusher waits the whole interval before
        // each tick, so `since_last_success` necessarily climbs to about one
        // interval every cycle — a ceiling below that would read the healthy
        // steady state as a dead flusher and restart the pod onto the same
        // configuration, forever.
        let silence_deadline = silence_deadline(interval);
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
/// So this must keep using `property_int_value` — no `_cf`. "Correcting" it to
/// ask the app families would silently zero it, and it is one of only two
/// escalations left in this module.
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
mod escalation_tests {
    use std::time::Duration;

    use super::{
        silence_deadline,
        FLUSH_FAILURE_INTERVALS,
        FLUSH_SILENCE_MULTIPLIER,
    };

    /// A healthy flusher waits the whole interval before each tick, so the time
    /// since its last success climbs to about one interval every cycle. A
    /// silence ceiling below that reads the healthy steady state as a dead
    /// flusher — and since escalation restarts the pod onto the same
    /// configuration, that is a crash loop rather than a one-off.
    ///
    /// The ceiling is operator-configurable and the README invites tightening
    /// it, so this has to hold for intervals well past any default.
    #[test]
    fn the_silence_deadline_always_outlasts_a_healthy_flushers_own_interval() {
        for seconds in [1u64, 5, 30, 60, 120, 600, 1800, 3600] {
            let interval = Duration::from_secs(seconds);
            let deadline = silence_deadline(interval);
            assert!(
                deadline > interval,
                "a {interval:?} interval got a {deadline:?} silence deadline, which a healthy \
                 flusher would trip on its own cadence"
            );
        }
    }

    /// And the ceiling still binds where it was introduced to: a long interval
    /// must not buy an hour of silence in a mode that promises at most one
    /// interval of loss.
    #[test]
    fn the_silence_deadline_is_still_bounded_above() {
        let interval = Duration::from_secs(60);
        let deadline = silence_deadline(interval);
        let unbounded = interval
            .saturating_mul(FLUSH_FAILURE_INTERVALS)
            .saturating_mul(FLUSH_SILENCE_MULTIPLIER);
        assert!(
            deadline < unbounded,
            "the ceiling must still cut the multiplied budget down"
        );
    }
}

#[cfg(test)]
mod watchdog_tests {
    use std::time::Duration;

    use common::shutdown::ShutdownSignal;
    use tokio::sync::oneshot;

    use super::{
        watch_writes,
        WriteWatch,
    };
    use crate::{
        options,
        RocksDbPersistence,
    };

    /// The escalation this module exists for, end to end through the real
    /// predicate: a write held past the ceiling stops the backend.
    ///
    /// Six review rounds found a defect in this code path and none of them had
    /// a test to catch the seventh, so this covers the predicate directly
    /// rather than the arithmetic around it.
    #[test]
    fn a_write_held_past_the_ceiling_escalates() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
        let (tx, mut rx) = oneshot::channel();
        let shutdown = ShutdownSignal::new(tx);

        // Nothing in flight: nothing to say.
        assert!(!watch_writes(&persistence.inner, &shutdown));
        assert!(rx.try_recv().is_err(), "an idle backend must not escalate");

        // A write in flight, but inside the ceiling. This is the case five
        // rounds kept getting wrong in the other direction — a busy backend
        // must not be stopped for being busy.
        let guard = persistence.inner.write_watch.begin();
        assert!(
            !watch_writes(&persistence.inner, &shutdown),
            "a write that has only just begun is not a stall"
        );
        assert!(
            rx.try_recv().is_err(),
            "a write inside the ceiling must not escalate"
        );
        drop(guard);

        Ok(())
    }

    /// And the ceiling itself, on the clock rather than by waiting twenty
    /// minutes for it.
    #[test]
    fn the_watch_reports_the_oldest_write_not_the_newest() {
        let watch = WriteWatch::default();
        assert!(watch.oldest_in_flight().is_none());

        let first = watch.begin();
        std::thread::sleep(Duration::from_millis(30));
        let second = watch.begin();

        // The oldest, which is what the ceiling is compared against — reporting
        // the newest would reset the clock on every incoming write and mean a
        // wedged backend under continuous load never escalated at all.
        let oldest = watch.oldest_in_flight().expect("a write is in flight");
        assert!(
            oldest >= Duration::from_millis(30),
            "expected the age of the first write, got {oldest:?}"
        );

        drop(first);
        let after = watch.oldest_in_flight().expect("one write still in flight");
        assert!(
            after < Duration::from_millis(30),
            "after the first write finished the second is the oldest, got {after:?}"
        );

        drop(second);
        assert!(
            watch.oldest_in_flight().is_none(),
            "no writes in flight once every guard has dropped"
        );
    }

    /// The ceiling has to clear anything that legitimately holds a write for a
    /// long time, or it becomes the round-5 failure again in a new place.
    #[test]
    fn the_ceiling_is_generous_enough_to_be_a_backstop() {
        assert!(
            *options::WRITE_STALL_CEILING >= Duration::from_secs(60),
            "a ceiling this tight would stop backends for ordinary backpressure"
        );
    }
}
