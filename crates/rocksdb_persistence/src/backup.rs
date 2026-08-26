//! Backup, restore and verification, on RocksDB's `BackupEngine`.
//!
//! Postgres deployments get `pg_dump`, `pg_basebackup`, WAL archiving and
//! point-in-time recovery, and an operator who already knows how to drive them.
//! An embedded store has none of that by default: its durability story is the
//! volume, and a lost volume is lost data. This module is what replaces it.
//!
//! `BackupEngine` rather than `Checkpoint`, because it is the one that behaves
//! like a backup system rather than a snapshot primitive: numbered generations,
//! incremental file sharing so the *n*th backup writes only what changed since
//! *n-1*, a retention call, checksum verification, and a restore that does not
//! require the operator to know RocksDB's directory layout.
//!
//! # What this does not give you
//!
//! **Point-in-time recovery, and it cannot.** Postgres reaches an arbitrary
//! moment by replaying archived WAL segments onto a base backup. RocksDB
//! recycles or deletes its WAL once the corresponding memtables flush, so there
//! is nothing to archive and no `restore_to_timestamp`. The recovery point is
//! the last backup, and narrowing it means backing up more often. Where an
//! upstream log can replay into idempotent appliers, that log covers the gap
//! for the data that came through it — and only for that data.
//!
//! **Anything outside `Persistence`.** File storage and search index segments
//! live on the filesystem beside the database, not in it. See
//! `docs/proposals/005-backup-and-restore.md` §6.

use std::{
    path::{
        Path,
        PathBuf,
    },
    sync::{
        atomic::{
            AtomicI64,
            Ordering,
        },
        Arc,
        Mutex,
        Weak,
    },
    thread::JoinHandle,
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Context as _;
use rocksdb::backup::{
    BackupEngine,
    BackupEngineOptions,
    RestoreOptions,
};

use crate::{
    metrics,
    options,
    worker,
    Inner,
    OpenOptions,
    RocksDbPersistence,
};

/// One generation, as `BackupEngine` records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupInfo {
    /// The engine's own identifier for this generation, and the handle every
    /// other operation takes.
    pub backup_id: u32,
    /// Seconds since the Unix epoch, as recorded by the engine.
    pub timestamp: i64,
    /// Size of this generation's files. Incremental, so generations sharing an
    /// SST each report it.
    pub size_bytes: u64,
    /// How many files this generation references.
    pub num_files: u32,
}

/// An advisory lock over a backup directory, held for as long as an engine is
/// open against it.
///
/// RocksDB's own header is explicit that concurrent `BackupEngine`s on one
/// `backup_dir` are unspecified — `Write × Open` and `Write × Read` are listed
/// as *"unspec = Behavior is unspecified, including possibly trashing the
/// backup_dir"* — and it ships no lock of its own. Every entry point here opens
/// a read-write engine, and `purge_old_backups` runs on every worker tick, so
/// "the operator lists generations while the worker prunes" is a real sequence
/// that this makes an error instead of a corruption.
struct DirLock {
    /// Held only so the descriptor stays open: the `flock` lives on the fd and
    /// is released when it closes.
    _file: std::fs::File,
}

impl DirLock {
    fn acquire(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join("convex-backup.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        // flock is advisory and released by the kernel when the fd closes, so
        // a crashed holder does not strand the directory the way a lock file
        // whose presence is the lock would.
        let rc = unsafe {
            libc::flock(
                std::os::fd::AsRawFd::as_raw_fd(&file),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        };
        anyhow::ensure!(
            rc == 0,
            "another process is using the backup directory {}. Backups, restores and listings \
             must not overlap: RocksDB does not define what concurrent backup engines do to a \
             directory, up to and including destroying it.",
            dir.display(),
        );
        Ok(Self { _file: file })
    }
}

// No `Drop` impl: closing the file descriptor releases the `flock`, which
// `File`'s own `Drop` does. The lock file is left in place so the next acquirer
// reuses it rather than racing to create it.

/// A backup engine and the directory lock it is only valid under.
struct LockedEngine {
    engine: BackupEngine,
    _lock: DirLock,
}

/// Name of the file that records which database owns a backup directory.
const OWNER_FILE: &str = "convex-backup-owner";

/// Records, or checks, which database a backup directory belongs to.
///
/// Two cells pointed at one `ROCKSDB_BACKUP_DIR` — a shared volume, a templated
/// env var, a copy-pasted manifest — would interleave their generations into
/// one chain. `purge_old_backups` would then prune the *other* database's
/// generations, and a restore would return whichever database happened to run
/// last. The directory lock prevents simultaneous corruption; it does nothing
/// about this, and the failure is silent until someone restores.
fn claim_directory(dir: &Path, identity: &str) -> anyhow::Result<()> {
    let marker = dir.join(OWNER_FILE);
    match std::fs::read_to_string(&marker) {
        Ok(existing) => {
            let existing = existing.trim();
            anyhow::ensure!(
                existing == identity,
                "{} holds backups of a different database ({existing}, not {identity}). Two \
                 databases must not share a backup directory: their generations interleave, \
                 retention prunes the wrong ones, and a restore returns whichever wrote last.",
                dir.display(),
            );
            Ok(())
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::write(&marker, identity)
                .with_context(|| format!("failed to write {}", marker.display()))?;
            Ok(())
        },
        Err(e) => {
            Err(anyhow::Error::from(e).context(format!("failed to read {}", marker.display())))
        },
    }
}

fn open_locked_engine(dir: &Path) -> anyhow::Result<LockedEngine> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create backup directory {}", dir.display()))?;
    let lock = DirLock::acquire(dir)?;
    Ok(LockedEngine {
        engine: open_engine(dir)?,
        _lock: lock,
    })
}

fn open_engine(dir: &Path) -> anyhow::Result<BackupEngine> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create backup directory {}", dir.display()))?;
    let env = rocksdb::Env::new().context("failed to create a RocksDB environment")?;
    let opts = BackupEngineOptions::new(dir)
        .with_context(|| format!("invalid backup directory {}", dir.display()))?;
    BackupEngine::open(&opts, &env)
        .with_context(|| format!("failed to open the backup engine at {}", dir.display()))
}

fn info_of(engine: &BackupEngine) -> Vec<BackupInfo> {
    let mut out: Vec<BackupInfo> = engine
        .get_backup_info()
        .into_iter()
        .map(|i| BackupInfo {
            backup_id: i.backup_id,
            timestamp: i.timestamp,
            size_bytes: i.size,
            num_files: i.num_files,
        })
        .collect();
    out.sort_by_key(|i| i.backup_id);
    out
}

/// Lists the generations in a backup directory. Safe to call while the database
/// that produced them is running.
pub fn list(dir: &Path) -> anyhow::Result<Vec<BackupInfo>> {
    // Deliberately not `create_dir_all`: `list /typo` should say the directory
    // does not exist, not create it and report an empty chain.
    anyhow::ensure!(dir.exists(), "no backup directory at {}", dir.display());
    Ok(info_of(&open_locked_engine(dir)?.engine))
}

/// Checks that a generation's files are present and the size RocksDB expects.
///
/// **Not a checksum.** The C API's `VerifyBackup` defaults
/// `verify_with_checksum` to false and the binding exposes no way to turn it
/// on, so this compares existence and byte length only. Bit rot on the
/// destination, or a filesystem that padded rather than truncated, passes.
/// [`rehearse`] is the check that reads the bytes back.
pub fn verify(dir: &Path, backup_id: u32) -> anyhow::Result<()> {
    open_locked_engine(dir)?
        .engine
        .verify_backup(backup_id)
        .with_context(|| format!("backup {backup_id} failed verification"))
}

/// Restores a generation into `db_dir`, which must not be open.
///
/// RocksDB holds a directory lock for as long as a database is open, and a
/// restore rewrites the directory underneath it, so this cannot run against a
/// live backend. Stop the process first.
pub fn restore(dir: &Path, db_dir: &Path, backup_id: Option<u32>) -> anyhow::Result<()> {
    // Refuse to write into a directory that already holds something. There is
    // no reliable way to ask whether another process has RocksDB's directory
    // lock — the `LOCK` file exists either way, and the only way to find out is
    // to take it, which is exactly what must not happen here. Requiring an
    // empty target sidesteps the question and rules out the worse mistake of
    // restoring over a live database. Move the old directory aside first; that
    // also leaves you something to go back to.
    if db_dir.exists() {
        let mut entries = std::fs::read_dir(db_dir)
            .with_context(|| format!("failed to read {}", db_dir.display()))?;
        anyhow::ensure!(
            entries.next().is_none(),
            "{} is not empty. Restore into a fresh directory and swap it in, so that a running \
             database is never written underneath and the current one stays recoverable.",
            db_dir.display(),
        );
    }
    restore_into(dir, db_dir, backup_id)
}

/// The restore itself, without the empty-target precondition.
///
/// Split out for [`rehearse`], which restores into a directory it constructed
/// itself and so has no pre-existing content to protect.
fn restore_into(dir: &Path, db_dir: &Path, backup_id: Option<u32>) -> anyhow::Result<()> {
    let mut locked = open_locked_engine(dir)?;
    let engine = &mut locked.engine;
    let opts = RestoreOptions::default();
    let result = match backup_id {
        Some(id) => engine
            .restore_from_backup(db_dir, db_dir, &opts, id)
            .with_context(|| format!("failed to restore backup {id}")),
        None => engine
            .restore_from_latest_backup(db_dir, db_dir, &opts)
            .context("failed to restore the latest backup"),
    };
    if result.is_err() {
        // A restore that failed part-way — disk full, killed — leaves a
        // populated directory, which the emptiness precondition would then
        // refuse on the retry, wedging the operator at the worst moment.
        // Clearing it is safe: it was empty before this call, so nothing here
        // predates the attempt.
        if let Err(e) = std::fs::remove_dir_all(db_dir) {
            tracing::warn!(
                "could not clear {} after a failed restore: {e}. Remove it before retrying.",
                db_dir.display(),
            );
        }
    }
    result
}

/// Restores a generation into a scratch directory and opens it.
///
/// A backup nobody has restored is not a backup. `verify` checks file sizes;
/// this checks the thing an operator actually needs to know — that a database
/// restored from this backup opens and reads. It costs a full copy of the data,
/// so it belongs on a schedule of its own rather than after every backup.
pub fn rehearse(
    dir: &Path,
    scratch: &Path,
    backup_id: Option<u32>,
) -> anyhow::Result<(BackupInfo, crate::ReadCheck)> {
    let generations = list(dir)?;
    let target = match backup_id {
        Some(id) => generations
            .iter()
            .find(|i| i.backup_id == id)
            .copied()
            .with_context(|| format!("no backup {id} in {}", dir.display()))?,
        None => *generations
            .last()
            .with_context(|| format!("no backups in {}", dir.display()))?,
    };
    verify(dir, target.backup_id)?;

    // Never delete a path the operator named. The previous attempt marked a
    // scratch directory as "mine to clear" — but a marker is a *permanent*
    // deletion grant on that path, and a directory that hosted a rehearsal last
    // year is a directory this command will happily empty today, live database
    // and all. A flag would be no better; the dangerous invocation would carry
    // it.
    //
    // Instead the rehearsal works inside a directory it names itself, and
    // removes only that. `scratch` is created if absent and otherwise left
    // exactly as found — whatever else is in it is none of this command's
    // business.
    std::fs::create_dir_all(scratch)
        .with_context(|| format!("failed to create {}", scratch.display()))?;
    let restored = scratch.join(format!(
        "convex-rehearsal-{}-{}",
        target.backup_id,
        std::process::id()
    ));
    if restored.exists() {
        // Only reachable for a path this process just constructed, so clearing
        // it cannot touch anything else.
        std::fs::remove_dir_all(&restored)
            .with_context(|| format!("failed to clear {}", restored.display()))?;
    }
    std::fs::create_dir_all(&restored)?;
    restore_into(dir, &restored, Some(target.backup_id))?;

    // Opening exercises the manifest and every column family descriptor; the
    // scan then forces real iterators over real data rather than a bare open.
    // No background work: this process may be running in the backend's own
    // environment, where `ROCKSDB_BACKUP_DIR` is set. A worker attached to a
    // scratch database would write generations *of the scratch database* into
    // the production backup chain and then prune the real ones away.
    let persistence = RocksDbPersistence::open_with(
        &restored,
        OpenOptions {
            background: false,
            ..OpenOptions::default()
        },
    )
    .context("the restored database did not open")?;
    let read = persistence
        .inner
        .verify_readable()
        .context("the restored database opened but could not be read")?;
    drop(persistence);
    // Leave nothing behind on success. A failed rehearsal keeps its directory
    // for inspection; the next run's name differs by pid, so it never collides.
    if let Err(e) = std::fs::remove_dir_all(&restored) {
        tracing::warn!("could not clean up {}: {e}", restored.display());
    }
    Ok((target, read))
}

impl RocksDbPersistence {
    /// Takes a new backup generation into `dir` and prunes to `keep`
    /// generations.
    ///
    /// Memtables are flushed first, so the backup does not depend on replaying
    /// a WAL to be complete. With `atomic_flush` on — which this backend always
    /// sets — that flush is a consistent cut across every column family, so a
    /// backup can never hold an index entry whose document it missed.
    pub fn backup(&self, dir: &Path, keep: usize) -> anyhow::Result<BackupInfo> {
        backup_inner(&self.inner, dir, keep, None)
    }
}

fn backup_inner(
    inner: &Inner,
    dir: &Path,
    keep: usize,
    stop: Option<&worker::Signal>,
) -> anyhow::Result<BackupInfo> {
    // Checked between phases, not inside them. A generation cannot be torn in
    // half safely, but the gaps between claiming the directory, writing,
    // verifying and pruning are all safe points — and teardown waits out
    // whatever phase is running, so a worker that ignored the stop flag made
    // shutdown as slow as a whole backup. Measured at ~200ms per 60 MiB on
    // local disk, which the module's own docs describe as minutes in
    // production; a `terminationGracePeriodSeconds` shorter than that turns an
    // orderly exit into a SIGKILL, losing the acknowledged writes the closing
    // WAL flush exists to protect.
    let interrupted = || {
        stop.is_some_and(|signal| signal.is_stopped())
            .then(|| anyhow::anyhow!("backup abandoned: the worker was asked to stop"))
    };
    anyhow::ensure!(!inner.secondary, "a secondary instance cannot take backups");
    let timer = metrics::backup_timer();
    // Ownership first, before a read-write engine is opened: opening one can
    // run RocksDB's own garbage collection over a directory that turns out to
    // belong to a different database.
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create backup directory {}", dir.display()))?;
    let identity = inner.identity()?;
    let _claim_lock = DirLock::acquire(dir)?;
    claim_directory(dir, &identity)?;
    drop(_claim_lock);
    if let Some(e) = interrupted() {
        return Err(e);
    }
    let mut locked = open_locked_engine(dir)?;
    let engine = &mut locked.engine;
    if let Err(e) = engine.create_new_backup_flush(&inner.db, true) {
        timer.finish_developer_error();
        return Err(anyhow::Error::from(e).context("failed to create a backup"));
    }
    let latest = *info_of(engine)
        .last()
        .context("backup reported success but recorded no generation")?;

    // Verify before pruning, never after. `create_new_backup_flush` returning
    // `Ok` says RocksDB wrote the files; it does not say the destination kept
    // them. Pruning on the strength of an unverified generation is how the last
    // known-good ones age out behind a silently bad new one.
    //
    // This is a weaker gate than it looks: `verify_backup` checks sizes, not
    // checksums (see `verify`). It catches a truncated or missing file, which
    // is the common destination failure, and not a corrupted one. A scheduled
    // `rehearse` is what covers the rest.
    if let Err(e) = engine.verify_backup(latest.backup_id) {
        timer.finish_developer_error();
        return Err(anyhow::Error::from(e).context(format!(
            "backup {} was written but failed verification; keeping older generations",
            latest.backup_id,
        )));
    }

    // `purge_old_backups` is also what triggers RocksDB's `GarbageCollect`, so
    // `keep = 0` does not merely retain everything — it also leaves orphaned
    // files from an interrupted backup behind forever. Retain a very large
    // number instead of none, so collection still runs.
    let keep = if keep == 0 { u32::MAX as usize } else { keep };
    if let Err(e) = engine.purge_old_backups(keep) {
        // The backup is written and verified; failing to prune is not a reason
        // to report it as lost.
        tracing::warn!("failed to purge old backups in {}: {e}", dir.display());
    }
    timer.finish();
    metrics::log_backup(latest.size_bytes, latest.num_files);
    Ok(latest)
}

// ---------------------------------------------------------------------------
// The periodic worker
// ---------------------------------------------------------------------------

/// What the periodic backup worker was told to do.
#[derive(Clone, Debug)]
pub struct BackupConfig {
    /// Where generations are written. One database per directory: the worker
    /// claims it on first use and refuses a directory another database owns.
    pub dir: PathBuf,
    /// How long to wait between generations.
    pub interval: Duration,
    /// How many generations to retain. Zero keeps every one.
    pub keep: usize,
}

impl BackupConfig {
    /// Reads the configuration from the environment, or `None` if
    /// `ROCKSDB_BACKUP_DIR` is unset.
    ///
    /// Unset means off, deliberately. A backend that silently starts writing
    /// backups into an unconfigured path is worse than one that does nothing.
    pub fn from_env() -> Option<Self> {
        let dir = std::env::var("ROCKSDB_BACKUP_DIR").ok()?;
        let dir = dir.trim();
        if dir.is_empty() {
            return None;
        }
        Some(Self {
            dir: PathBuf::from(dir),
            interval: *options::BACKUP_INTERVAL,
            keep: *options::BACKUP_KEEP,
        })
    }
}

pub(crate) struct BackupWorker {
    stop: Arc<worker::Signal>,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Unix seconds of the newest generation, or 0 if none is known yet.
    ///
    /// The health monitor reads this instead of listing the directory. Listing
    /// takes the directory lock, which the worker holds for the whole of a
    /// backup — so a monitor polling every 15 seconds would fail the hourly
    /// backup it collided with, and would stop publishing the backup-age gauge
    /// for exactly as long as a backup was running.
    newest: Arc<AtomicI64>,
}

impl BackupWorker {
    /// Spawns the periodic worker.
    ///
    /// Holds a [`Weak`] for the same reason the WAL flusher does: a strong
    /// reference would keep the database alive for as long as the thread ran,
    /// and the thread runs until the database is dropped.
    pub(crate) fn spawn(db: Weak<Inner>, config: BackupConfig) -> anyhow::Result<Self> {
        // Seeded once, before the worker starts, so the gauge is meaningful
        // from boot rather than only after the first interval elapses.
        let newest = Arc::new(AtomicI64::new(
            list(&config.dir)
                .ok()
                .and_then(|g| g.last().map(|i| i.timestamp))
                .unwrap_or(0),
        ));
        let thread_newest = newest.clone();
        let stop = Arc::new(worker::Signal::new());
        let signal = stop.clone();
        tracing::info!(
            "rocksdb backups: every {}s into {}, keeping {}",
            config.interval.as_secs(),
            config.dir.display(),
            config.keep,
        );
        let handle = std::thread::Builder::new()
            .name("rocksdb-backup".to_string())
            .spawn(move || {
                loop {
                    if signal.park(config.interval) {
                        break;
                    }
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    match backup_inner(&inner, &config.dir, config.keep, Some(&signal)) {
                        Ok(info) => {
                            thread_newest.store(info.timestamp, Ordering::Relaxed);
                            tracing::info!(
                                "rocksdb backup {} written: {} files, {} MiB",
                                info.backup_id,
                                info.num_files,
                                info.size_bytes >> 20,
                            );
                        },
                        Err(e) => {
                            // Log rather than crash: the next tick retries, and
                            // the database is unaffected. A backup system that
                            // fails quietly is the one failure mode worse than
                            // not having one, so this also drives the age gauge
                            // below, which is what an alert should watch.
                            tracing::error!("rocksdb backup failed: {e:#}");
                            metrics::log_backup_failure();
                        },
                    }
                }
                signal.mark_exited();
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB backup worker: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
            newest,
        })
    }

    /// Unix seconds of the newest generation the worker knows about, or `None`
    /// before the first one exists.
    pub(crate) fn newest_backup_unix_secs(&self) -> Option<i64> {
        match self.newest.load(Ordering::Relaxed) {
            0 => None,
            secs => Some(secs),
        }
    }

    /// Stops the thread and waits for it, bounded. Idempotent.
    ///
    /// Returns whether the thread actually left. `false` means it is still
    /// inside a backup — an uninterruptible `open(2)` against a hung backup
    /// mount is the case that motivated the deadline — and the caller must not
    /// close the database underneath it.
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
            "backup worker",
        )
    }
}

impl Drop for BackupWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub(crate) fn age_seconds(timestamp: i64) -> f64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(timestamp);
    (now - timestamp).max(0) as f64
}

#[cfg(test)]
pub(crate) mod testing {
    use std::path::Path;

    /// Takes the backup-directory lock and returns a guard, so a test can hold
    /// it the way a running backup or restore does.
    pub(crate) fn lock_backup_dir(dir: &Path) -> anyhow::Result<impl Sized + use<>> {
        std::fs::create_dir_all(dir)?;
        super::DirLock::acquire(dir)
    }
}
