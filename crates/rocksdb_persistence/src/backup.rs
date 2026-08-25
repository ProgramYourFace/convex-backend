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
    Inner,
    OpenOptions,
    RocksDbPersistence,
};

/// One generation, as `BackupEngine` records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupInfo {
    pub backup_id: u32,
    /// Seconds since the Unix epoch, as recorded by the engine.
    pub timestamp: i64,
    pub size_bytes: u64,
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
    file: std::fs::File,
    path: PathBuf,
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
        Ok(Self { file, path })
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        let _ = std::os::fd::AsRawFd::as_raw_fd(&self.file);
        // Closing the file releases the flock; the path is left in place so the
        // next acquirer reuses it rather than racing on create.
        let _ = &self.path;
    }
}

/// A backup engine and the directory lock it is only valid under.
struct LockedEngine {
    engine: BackupEngine,
    _lock: DirLock,
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
    Ok(info_of(&open_locked_engine(dir)?.engine))
}

/// Checks that a generation's files are present and the size RocksDB expects.
///
/// This is a checksum, not a rehearsal: it says the files are intact, not that
/// a database restored from them opens. Use [`rehearse`] for that.
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
    let mut locked = open_locked_engine(dir)?;
    let engine = &mut locked.engine;
    let opts = RestoreOptions::default();
    match backup_id {
        Some(id) => engine
            .restore_from_backup(db_dir, db_dir, &opts, id)
            .with_context(|| format!("failed to restore backup {id}")),
        None => engine
            .restore_from_latest_backup(db_dir, db_dir, &opts)
            .context("failed to restore the latest backup"),
    }
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

    if scratch.exists() {
        std::fs::remove_dir_all(scratch)
            .with_context(|| format!("failed to clear {}", scratch.display()))?;
    }
    std::fs::create_dir_all(scratch)?;
    restore(dir, scratch, Some(target.backup_id))?;

    // Opening exercises the manifest and every column family descriptor; the
    // scan then forces real iterators over real data rather than a bare open.
    // No background work: this process may be running in the backend's own
    // environment, where `ROCKSDB_BACKUP_DIR` is set. A worker attached to a
    // scratch database would write generations *of the scratch database* into
    // the production backup chain and then prune the real ones away.
    let persistence = RocksDbPersistence::open_with(
        scratch,
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
        backup_inner(&self.inner, dir, keep)
    }
}

fn backup_inner(inner: &Inner, dir: &Path, keep: usize) -> anyhow::Result<BackupInfo> {
    anyhow::ensure!(!inner.secondary, "a secondary instance cannot take backups");
    let timer = metrics::backup_timer();
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
    if let Err(e) = engine.verify_backup(latest.backup_id) {
        timer.finish_developer_error();
        return Err(anyhow::Error::from(e).context(format!(
            "backup {} was written but failed verification; keeping older generations",
            latest.backup_id,
        )));
    }

    if keep > 0
        && let Err(e) = engine.purge_old_backups(keep)
    {
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

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub dir: PathBuf,
    pub interval: Duration,
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
        Some(Self::for_dir(PathBuf::from(dir)))
    }

    /// The same configuration for an explicitly chosen directory.
    ///
    /// A process that opens several databases has to name a directory per
    /// database: generations are numbered per directory and carry no record of
    /// which database wrote them, so sharing one would interleave two chains
    /// and let each database's pruning delete the other's generations.
    pub fn for_dir(dir: PathBuf) -> Self {
        Self {
            dir,
            interval: *options::BACKUP_INTERVAL,
            keep: *options::BACKUP_KEEP,
        }
    }
}

pub(crate) struct BackupWorker {
    stop: Arc<Signal>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct Signal {
    stopped: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

impl BackupWorker {
    /// Spawns the periodic worker.
    ///
    /// Holds a [`Weak`] for the same reason the WAL flusher does: a strong
    /// reference would keep the database alive for as long as the thread ran,
    /// and the thread runs until the database is dropped.
    pub(crate) fn spawn(db: Weak<Inner>, config: BackupConfig) -> anyhow::Result<Self> {
        let stop = Arc::new(Signal {
            stopped: AtomicBool::new(false),
            lock: Mutex::new(()),
            wake: Condvar::new(),
        });
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
                    {
                        let guard = match signal.lock.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        let _ = signal.wake.wait_timeout(guard, config.interval);
                    }
                    if signal.stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    let Some(inner) = db.upgrade() else {
                        break;
                    };
                    match backup_inner(&inner, &config.dir, config.keep) {
                        Ok(info) => tracing::info!(
                            "rocksdb backup {} written: {} files, {} MiB",
                            info.backup_id,
                            info.num_files,
                            info.size_bytes >> 20,
                        ),
                        Err(e) => {
                            // Log rather than crash: the next tick retries, and
                            // the database is unaffected. A backup system that
                            // fails quietly is the one failure mode worse than
                            // not having one, so this also drives the age gauge
                            // below, which is what an alert should watch.
                            tracing::error!("rocksdb backup failed: {e:#}");
                            metrics::log_backup_failure(&inner.instance);
                        },
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn the RocksDB backup worker: {e}"))?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
        })
    }

    /// Stop the thread and wait for it. Idempotent.
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
                std::mem::forget(handle);
            } else {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for BackupWorker {
    fn drop(&mut self) {
        self.stop();
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
    pub(crate) fn lock_backup_dir(dir: &Path) -> anyhow::Result<impl Drop + use<>> {
        std::fs::create_dir_all(dir)?;
        super::DirLock::acquire(dir)
    }
}
