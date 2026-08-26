//! A [`Persistence`] implementation backed by an embedded RocksDB store.
//!
//! # Why this exists
//!
//! Convex's storage schema is already an append-only, multi-version log: a
//! document update writes a new `(id, ts)` revision plus a new
//! `(index_id, key, ts)` index entry, and never modifies a row in place. The
//! relational backends store that log on top of B-tree pages that *are* updated
//! in place, paying leaf lookups, page splits, checkpoint full-page writes and
//! vacuum for a workload that never overwrites anything — in a separate
//! process, across a socket. An LSM tree's on-disk model is that log.
//!
//! Convex also asks very little of its storage layer. Every read through
//! [`PersistenceReader`] is explicitly timestamp-scoped, so **Convex implements
//! MVCC in its own data model and needs none from the engine**. What is left is
//! ordered keys, range scans, point gets and atomic durable batches.
//!
//! Nothing above the [`Persistence`] trait changes: the committer, the index
//! cache, retention, subscriptions, streaming export, and the search and vector
//! indexes are untouched, and the Convex developer API is identical.
//!
//! # Layout
//!
//! See [`keys`] for the column families and the encodings, including why index
//! keys are escaped before a timestamp is concatenated onto them.
//!
//! # Semantics that differ from the relational backends
//!
//! * **Uniqueness is detected, not enforced.** [`ConflictStrategy::Error`] is
//!   free in a B-tree, which gets it from a primary key inside the transaction;
//!   an LSM silently shadows an existing key instead. It is checked here with
//!   one bloom-filtered point get per row written — but that get happens
//!   *before* the batch, not inside it, so it is a detector, not a constraint:
//!   two concurrent writes naming the same `(ts, id)` can both probe clean and
//!   both apply, the second shadowing the first. That is weaker than Postgres
//!   and worth stating plainly.
//!
//!   It holds anyway on the path that matters, for a reason outside this
//!   crate: commits are serialized through a single committer that assigns
//!   strictly increasing timestamps, so no two commits can name the same
//!   `(ts, id)` in the first place, and `check_generated_ids` rejects reused
//!   document ids a layer above. The check earns its keep on
//!   `Database::initialize`, which writes bootstrap rows outside that path.
//!   `ROCKSDB_CHECK_CONFLICTS=false` gives up the detection.
//! * **No lease.** The relational backends fence a stolen leadership with two
//!   extra statements per write. An embedded store has no such concept: the
//!   process holding the directory lock is the writer. That is a stronger
//!   guarantee for a single-node deployment, but it means one writer per data
//!   directory must be guaranteed externally, and there is no failover through
//!   a shared database — recovery is restore-from-backup plus the WAL.
//!
//! # Threading
//!
//! RocksDB is synchronous. Every call into it runs on a blocking pool thread,
//! so no operation here occupies an async worker. Documents are serialized on
//! the caller's thread, as the Postgres backend also does.

// `futures_async_stream::try_stream` builds the paged read streams; it lowers
// to coroutines, as the other persistence backends' streaming readers do.
#![feature(coroutines)]
#![feature(yield_expr)]
#![warn(missing_docs)]

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use common::{
    document::ResolvedDocument,
    index::IndexEntry,
    persistence::{
        ConflictStrategy,
        DocumentLogEntry,
        Persistence,
        PersistenceGlobalKey,
        PersistenceIndexEntry,
        PersistenceReader,
        PersistenceTableSize,
    },
    runtime::tokio_spawn_blocking,
    shutdown::ShutdownSignal,
    types::{
        IndexId,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        TabletId,
    },
};
use rocksdb::{
    ColumnFamilyDescriptor,
    DBWithThreadMode,
    MultiThreaded,
    WriteBatch,
    WriteOptions,
};
use serde_json::Value as JsonValue;

pub mod backup;
pub mod codec;
pub mod keys;
mod memory;
mod metrics;
pub mod options;
mod reader;

#[cfg(test)]
mod adversarial;
#[cfg(test)]
mod tests;

use keys::{
    ALL_COLUMN_FAMILIES,
    CF_DLOG,
    CF_DOCS,
    CF_DTAB,
    CF_GLOBALS,
    CF_IDX,
};

pub(crate) type Db = DBWithThreadMode<MultiThreaded>;

/// Key under which a database's backup identity lives in the `globals` column
/// family. Namespaced away from every `PersistenceGlobalKey`.
const IDENTITY_KEY: &[u8] = b"__convex_rocksdb_backup_identity";

/// Key under which the on-disk layout version lives, in the same column family.
const FORMAT_VERSION_KEY: &[u8] = b"__convex_rocksdb_format_version";

/// Version of the on-disk layout this build writes and understands.
///
/// The key encodings in [`keys`] — the escaping, the descending-timestamp
/// convention, the set of column families — are a format, and a format with no
/// version is one that opens an incompatible directory cleanly and returns
/// wrong answers. Bump this whenever any of them changes.
const FORMAT_VERSION: u64 = 1;

/// What a read-back check actually managed to decode.
#[derive(Default, Debug, Clone, Copy)]
pub struct ReadCheck {
    /// Rows read across every column family.
    pub rows: usize,
    /// Rows that decoded as a document revision.
    pub documents: usize,
    /// Rows that decoded as an index entry.
    pub index_entries: usize,
}

/// How to open a database.
///
/// A struct rather than a run of positional flags because these are two
/// unrelated decisions — durability and failure escalation — and an
/// `open(path, true, None)` at a call site says neither.
pub struct OpenOptions {
    /// Durability mode. `None` takes it from the environment.
    pub sync: Option<options::SyncMode>,
    /// Where to report a latched background error. Without one, a database
    /// that has stopped accepting writes keeps the process alive.
    pub shutdown: Option<ShutdownSignal>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            sync: None,
            shutdown: None,
        }
    }
}

/// An embedded RocksDB [`Persistence`].
pub struct RocksDbPersistence {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) db: Db,
    newly_created: bool,
    /// A secondary instance reads a primary owned by another process, and has
    /// to be told to catch up before it can see recent writes.
    secondary: bool,
    path: PathBuf,
    /// Held for the lifetime of the database so the shared block cache and
    /// memtable budget outlive the column families that reference them.
    _cache: rocksdb::Cache,
    _write_buffer_manager: rocksdb::WriteBufferManager,
    /// Resolved once when the database is opened, rather than read from the
    /// environment per write, so a single process can hold databases in
    /// different modes — which is what makes the mode testable.
    sync: options::SyncMode,
    /// Where to report a database that has stopped accepting writes.
    shutdown: Option<ShutdownSignal>,
    /// Engine writes that have failed since the last one that succeeded.
    consecutive_write_failures: std::sync::atomic::AtomicU32,
    /// Latched by the first `shutdown()`, so a second is a no-op rather than an
    /// error. See the note there.
    shutdown_done: std::sync::atomic::AtomicBool,
    /// A secondary's scratch directory, owned here so it is removed when the
    /// reader using it goes away. Held rather than leaked: the caller cannot
    /// drop it earlier without pulling the ground out from under an open
    /// engine, and leaking it accumulates a directory per reader in TMPDIR
    /// across restarts.
    _secondary_scratch: Option<tempfile::TempDir>,
}

impl RocksDbPersistence {
    /// Open (or create) a database at `path` with the environment's settings
    /// and no fatal-error escalation.
    ///
    /// Prefer [`RocksDbPersistence::open_with`] in a deployment: without a
    /// [`ShutdownSignal`] a latched background error — a full disk, a corrupt
    /// SST — leaves the process serving and failing every write indefinitely.
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    /// Open (or create) a database in an explicit [`options::SyncMode`],
    /// ignoring the environment. The mode is a durability contract, so it is
    /// worth being able to state it in code rather than hope a variable was
    /// set.
    pub fn new_with_sync_mode(path: &Path, sync: options::SyncMode) -> anyhow::Result<Self> {
        Self::open_with(
            path,
            OpenOptions {
                sync: Some(sync),
                ..OpenOptions::default()
            },
        )
    }

    /// Open (or create) a database with explicit options.
    pub fn open_with(path: &Path, opts: OpenOptions) -> anyhow::Result<Self> {
        Self::open(path, true, opts)
    }

    /// Whether an interval WAL flusher is running.

    /// Open a read-only view of a database another process has open for
    /// writing.
    ///
    /// RocksDB allows exactly one writer per directory, so a second reader
    /// cannot simply open the same path. A *secondary* instance is the
    /// supported way to do this: it reads the primary's files without taking
    /// the write lock, and catches up to the primary's log on demand — which
    /// [`Inner::refresh`] does before every read.
    ///
    /// The instance needs its own writable directory for its bookkeeping; it
    /// holds no user data, and is created here so its lifetime can be tied to
    /// the reader that uses it rather than leaked by the caller.
    ///
    /// Per *reader*, not per process: two readers in one process sharing a
    /// directory would corrupt each other's catch-up state, and in a container
    /// the backend is usually PID 1, so a pid-derived path would also be
    /// identical across restarts and silently reuse a dead process's state.
    pub fn new_secondary(path: &Path) -> anyhow::Result<Self> {
        let scratch = tempfile::Builder::new()
            .prefix("convex-rocksdb-secondary-")
            .tempdir()?;
        let secondary_path = scratch.path().to_path_buf();
        Self::open_secondary(path, &secondary_path, Some(scratch))
    }

    /// As [`Self::new_secondary`], but into a caller-provided directory whose
    /// lifetime the caller manages. Used by tests, which want a path they can
    /// inspect.
    pub fn new_secondary_in(path: &Path, secondary_path: &Path) -> anyhow::Result<Self> {
        Self::open_secondary(path, secondary_path, None)
    }

    fn open_secondary(
        path: &Path,
        secondary_path: &Path,
        scratch: Option<tempfile::TempDir>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.join("CURRENT").exists(),
            "no RocksDB database at {}",
            path.display(),
        );
        std::fs::create_dir_all(secondary_path)?;
        let shared = options::build(false, options::SyncMode::Every);
        let cfs: Vec<_> = ALL_COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options::column_family(name, &shared)))
            .collect();
        let db = Db::open_cf_descriptors_as_secondary(&shared.db, path, secondary_path, cfs)
            .with_context(|| format!("failed to open RocksDB secondary at {}", path.display()))?;
        db.try_catch_up_with_primary()?;
        check_format_version(&db, path, false)?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                newly_created: false,
                secondary: true,
                path: path.to_path_buf(),
                _cache: shared.cache,
                _write_buffer_manager: shared.write_buffer_manager,
                // A secondary instance never writes, so it has no WAL to flush.
                sync: options::SyncMode::Every,
                shutdown: None,
                consecutive_write_failures: std::sync::atomic::AtomicU32::new(0),
                shutdown_done: std::sync::atomic::AtomicBool::new(false),
                _secondary_scratch: scratch,
            }),
        })
    }

    fn open(path: &Path, create_if_missing: bool, opts: OpenOptions) -> anyhow::Result<Self> {
        let sync = opts.sync.unwrap_or_else(options::SyncMode::current);
        // RocksDB writes a CURRENT file as the last step of creating a
        // database, so its absence is the reliable "nothing here yet" signal —
        // more so than the directory existing, which a mount or a failed
        // earlier attempt can also produce.
        let directory_is_new = !path.join("CURRENT").exists();
        if directory_is_new && !create_if_missing {
            anyhow::bail!("no RocksDB database at {}", path.display());
        }

        let shared = options::build(create_if_missing, sync);
        let cfs: Vec<_> = ALL_COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options::column_family(name, &shared)))
            .collect();

        let db = Db::open_cf_descriptors(&shared.db, path, cfs)
            .with_context(|| format!("failed to open RocksDB at {}", path.display()))?;

        // Freshness is a property of the *data*, not of the directory. RocksDB
        // writes `CURRENT` as the last step of creating a database, before
        // Convex has written a single row, so a process killed anywhere inside
        // `Database::initialize` — an OOM, an eviction, a node reboot — would
        // reopen to a directory that exists and a database that is empty. Its
        // caller would then skip initialization and fail forever on the missing
        // bootstrap tables, with no way out but deleting the volume. Postgres
        // asks the same question of the data (`SELECT 1 FROM documents LIMIT
        // 1`), and recovers from this automatically.
        let newly_created = {
            let dlog = db
                .cf_handle(CF_DLOG)
                .context("missing column family dlog just after opening")?;
            let mut iter = db.raw_iterator_cf(&dlog);
            iter.seek_to_first();
            iter.status()?;
            !iter.valid()
        };

        check_format_version(&db, path, create_if_missing)?;

        tracing::info!(
            "opened RocksDB persistence at {} ({})",
            path.display(),
            match (directory_is_new, newly_created) {
                (true, _) => "created",
                (false, true) => "existing but empty; will be initialized",
                (false, false) => "existing",
            },
        );

        let inner = Arc::new(Inner {
            db,
            newly_created,
            secondary: false,
            path: path.to_path_buf(),
            _cache: shared.cache,
            _write_buffer_manager: shared.write_buffer_manager,
            sync,
            shutdown: opts.shutdown,
            consecutive_write_failures: std::sync::atomic::AtomicU32::new(0),
            shutdown_done: std::sync::atomic::AtomicBool::new(false),
            _secondary_scratch: None,
        });

        Ok(Self { inner })
    }
}

/// Stops the background threads and flushes the write-ahead log before
/// `Inner`'s fields — `db` among them — are dropped.
///
/// `Persistence::shutdown` is the intended teardown, but nothing in the tree
/// calls it: `Database::shutdown` stops the committer and its workers and
/// returns, and no relational backend overrides `shutdown` either, so the hook
/// was never wired up. Dropping the handle is the closest thing to a teardown
/// that runs, so it has to be the safe one.
///
/// It is not guaranteed to run either. `local_backend` installs no SIGTERM
/// handler, so a Kubernetes pod stop terminates the process with no destructor
/// executing at all — which under `SyncMode::Interval` costs up to one interval
/// on every rolling restart. Under the default `SyncMode::Every` nothing is
/// lost. Wiring a signal handler to `Persistence::shutdown` is an upstream gap
/// affecting every backend, not just this one. Without this, `db` — the
/// first field — would close while the workers were still running, and in
/// `SyncMode::Interval` the WAL buffer would be discarded rather than written.
/// Refuses a directory whose on-disk layout this build does not understand, and
/// stamps the version on one that has none.
///
/// Shared by the primary and secondary open paths: a secondary reads the same
/// files with the same encodings, and `connect_persistence_reader` is exactly
/// where a stale binary is most likely to be pointed at a newer volume.
fn check_format_version(db: &Db, path: &Path, stamp_if_missing: bool) -> anyhow::Result<()> {
    let globals = db
        .cf_handle(CF_GLOBALS)
        .context("missing column family globals just after opening")?;
    match db.get_cf(&globals, FORMAT_VERSION_KEY)? {
        Some(raw) => {
            let found: u64 = String::from_utf8_lossy(&raw)
                .trim()
                .parse()
                .with_context(|| format!("unreadable format version in {}", path.display()))?;
            anyhow::ensure!(
                found == FORMAT_VERSION,
                "{} was written in RocksDB layout version {found}, and this build understands \
                 {FORMAT_VERSION}. Opening it would read the data with the wrong key encodings.",
                path.display(),
            );
        },
        // Absent on a database just created, and on one written before
        // versioning existed — the same layout, so stamping it is correct
        // rather than a migration. A secondary never writes.
        None if stamp_if_missing => {
            // Synced, unlike an ordinary write, because losing it is not
            // losing data: the next boot would see an unstamped database and
            // stamp it again, which is harmless here but is the same pattern as
            // the identity below, where it is not.
            let mut sync = rocksdb::WriteOptions::default();
            sync.set_sync(true);
            db.put_cf_opt(
                &globals,
                FORMAT_VERSION_KEY,
                FORMAT_VERSION.to_string(),
                &sync,
            )?;
        },
        None => {},
    }
    Ok(())
}

impl Inner {
    /// Runs one engine write, escalating if the engine has stopped accepting
    /// writes altogether rather than rejecting this one.
    ///
    /// RocksDB latches read-only on a background error — a full disk, an SST
    /// checksum failure — and stays there. Every subsequent write fails, the
    /// process keeps serving, and nothing crashes. A Postgres deployment gets a
    /// pod that dies and a database another node can take over; an embedded one
    /// would fail every mutation indefinitely until somebody looked.
    ///
    /// The signal is **consecutive failures across independent writes**, not
    /// any property RocksDB exposes. An earlier version gated on
    /// `rocksdb.background-errors` and was dead code for the case it was
    /// written for: that counter is bumped in exactly two places, both of them
    /// a failed background *flush* or *compaction*
    /// (`db_impl_compaction_flush.cc`), and never by a foreground write or by
    /// `ErrorHandler::SetBGError`. On ENOSPC the failing write never reaches
    /// the memtable, so no flush is ever scheduled and the counter stays zero
    /// while every write fails forever. Measured on an 8 MiB tmpfs: 20 rounds
    /// of failing writes, `background-errors` reading `0` throughout, no
    /// signal raised — and still refusing writes after 7 MiB was freed.
    ///
    /// Counting failures asks the question directly, needs no property read on
    /// any path, and cannot be tripped by one bad write: the counter resets on
    /// every success, so reaching the threshold means this many *in a row* got
    /// nothing through.
    fn engine_write<T>(
        &self,
        what: &str,
        f: impl FnOnce() -> Result<T, rocksdb::Error>,
    ) -> anyhow::Result<T> {
        match f() {
            Ok(value) => {
                self.consecutive_write_failures
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                Ok(value)
            },
            Err(e) => {
                let failures = self
                    .consecutive_write_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                if failures >= *options::WRITE_FAILURES_TO_ESCALATE
                    && let Some(shutdown) = &self.shutdown
                {
                    shutdown.signal(anyhow::anyhow!(
                        "{failures} consecutive RocksDB writes have failed (most recently {what}: \
                         {e}); the engine is refusing writes, so stopping to let the deployment \
                         restart or fail over rather than failing every mutation from here on"
                    ));
                }
                Err(e.into())
            },
        }
    }

    pub(crate) fn cf(&self, name: &str) -> anyhow::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow::anyhow!("missing column family {name}"))
    }

    /// Reads every column family end to end, decoding what it finds, and
    /// refuses a database that holds nothing.
    ///
    /// Exists for [`crate::backup::rehearse`]. Opening a database exercises
    /// only its manifest and column family descriptors, and counting rows
    /// exercises only the iterators — neither says the bytes are the bytes that
    /// went in. So every document is parsed, every index entry is decoded, and
    /// an empty result is an error: a backup of nothing restores and scans
    /// perfectly, which is exactly the false pass a rehearsal exists to catch.
    pub(crate) fn verify_readable(&self) -> anyhow::Result<ReadCheck> {
        let mut check = ReadCheck::default();
        for name in keys::ALL_COLUMN_FAMILIES {
            let cf = self.cf(name)?;
            let mut iter = self.db.raw_iterator_cf(&cf);
            iter.seek_to_first();
            while iter.valid() {
                let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
                    break;
                };
                match name {
                    // Parsing the body is the point: a document above the blob
                    // threshold lives in a separate file, and a key-only or
                    // length-only scan would never prove it came back.
                    keys::CF_DLOG => {
                        let (ts, id) = keys::parse_dlog_key(key)?;
                        codec::decode_document(id.table(), value).with_context(|| {
                            format!("document {id}@{ts} did not decode after the restore")
                        })?;
                        check.documents += 1;
                    },
                    keys::CF_IDX => {
                        keys::parse_idx_key(key)?;
                        codec::decode_index_entry(value)
                            .context("index entry did not decode after the restore")?;
                        check.index_entries += 1;
                    },
                    _ => {},
                }
                check.rows += 1;
                iter.next();
            }
            iter.status()
                .with_context(|| format!("failed to scan column family {name}"))?;
        }
        anyhow::ensure!(
            check.documents > 0,
            "the restored database is readable but empty. A backup of an empty database restores \
             and scans perfectly, so this is reported as a failure rather than a pass.",
        );
        Ok(check)
    }

    /// A stable name for this database, for tying a backup directory to the
    /// database that produced it.
    ///
    /// Stored in the `globals` column family rather than taken from RocksDB's
    /// `IDENTITY` file, because `IDENTITY` is **not** among the files a backup
    /// captures: `GetLiveFilesStorageInfo` never lists it, so a restored
    /// database mints a fresh UUID on its first open. Using it would lock a
    /// restored database out of its own backup chain — every backup failing
    /// from the moment of recovery onward, which is exactly when a deployment
    /// can least afford to stop taking them. A key in `globals` is copied with
    /// the rest of the data and comes back unchanged.
    ///
    /// The key is namespaced so it cannot collide with a
    /// `PersistenceGlobalKey`, and Convex only ever reads globals by known
    /// key, so it stays invisible above the trait.
    pub(crate) fn identity(&self) -> anyhow::Result<String> {
        let globals = self.cf(CF_GLOBALS)?;
        if let Some(bytes) = self.db.get_cf(&globals, IDENTITY_KEY)? {
            return Ok(String::from_utf8(bytes)?);
        }
        // First call on this database: mint one and keep it. Derived from the
        // engine's own id when it has one, so two databases created in the same
        // instant cannot collide.
        let minted = std::fs::read_to_string(self.path.join("IDENTITY"))
            .map(|id| id.trim().to_string())
            .unwrap_or_else(|_| format!("path:{}", self.path.display()));
        // Synced regardless of the configured mode. This row is what a backup
        // chain matches a database against, and it is minted from RocksDB's
        // `IDENTITY` file — which a restore regenerates. Lose the row to a
        // crash and the next boot mints a *different* value, at which point
        // every backup into the existing directory fails as "a different
        // database". That lockout is the exact failure this row was added to
        // prevent, so it must not be the one write that was still in a buffer.
        let mut sync = rocksdb::WriteOptions::default();
        sync.set_sync(true);
        self.db
            .put_cf_opt(&globals, IDENTITY_KEY, minted.as_bytes(), &sync)?;
        Ok(minted)
    }

    /// A snapshot to read a whole page against, or `None` on a secondary.
    ///
    /// RocksDB rejects `ReadOptions::snapshot` on a secondary instance outright
    /// — `NewErrorIterator(Status::NotSupported("snapshot not supported in
    /// secondary mode"))` — so passing one turns every paged read into an
    /// error. Nothing is lost by omitting it there: a secondary's view of the
    /// primary's files only advances when `try_catch_up_with_primary` is
    /// called, which [`Inner::refresh`] does once at the start of a page and
    /// never during one. The view is therefore already frozen for the page's
    /// duration, which is the property the snapshot buys on a primary.
    pub(crate) fn read_snapshot(&self) -> Option<rocksdb::SnapshotWithThreadMode<'_, Db>> {
        (!self.secondary).then(|| self.db.snapshot())
    }

    /// Bring a secondary instance up to date with its primary. A no-op on the
    /// primary itself.
    pub(crate) fn refresh(&self) -> anyhow::Result<()> {
        if self.secondary {
            self.db.try_catch_up_with_primary()?;
        }
        Ok(())
    }

    fn write_options(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        // When this is on, RocksDB coalesces concurrent writers into a single
        // write group and syncs the shared WAL once for all of them — which is
        // exactly the shape of Convex's committer, which issues up to
        // `COMMITTER_MAX_CONCURRENT_WRITE_BATCHES` writes at a time.
        opts.set_sync(self.sync.sync_each_write());
        opts
    }
}

/// A write encoded into owned bytes, ready to hand to a blocking thread.
struct PendingWrite {
    /// `(dlog key, encoded document, docs key, dtab key)`
    documents: Vec<(
        [u8; keys::DOC_KEY_LEN],
        Vec<u8>,
        [u8; keys::DOC_KEY_LEN],
        [u8; keys::DOC_KEY_LEN],
    )>,
    /// `(idx key, encoded entry)`
    indexes: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PendingWrite {
    /// Serialize documents on the caller's thread, as the Postgres backend also
    /// does, so the blocking pool only does I/O.
    fn encode(
        documents: &[DocumentLogEntry],
        indexes: &[PersistenceIndexEntry],
    ) -> anyhow::Result<Self> {
        let mut encoded_documents = Vec::with_capacity(documents.len());
        for entry in documents {
            if let Some(doc) = &entry.value {
                anyhow::ensure!(
                    entry.id == doc.id_with_table_id(),
                    "document log entry id {} does not match its document {}",
                    entry.id,
                    doc.id_with_table_id(),
                );
            }
            encoded_documents.push((
                keys::dlog_key(entry.ts, entry.id),
                codec::encode_document(&entry.value, entry.prev_ts)?,
                keys::docs_key(entry.ts, entry.id),
                keys::dtab_key(entry.ts, entry.id),
            ));
        }
        let encoded_indexes = indexes
            .iter()
            .map(|entry| {
                (
                    keys::idx_key(entry.index_id, &entry.key.0, entry.ts),
                    codec::encode_index_entry(entry.value),
                )
            })
            .collect();
        Ok(Self {
            documents: encoded_documents,
            indexes: encoded_indexes,
        })
    }
}

impl Inner {
    fn apply_write(&self, write: PendingWrite, strategy: ConflictStrategy) -> anyhow::Result<()> {
        let dlog = self.cf(CF_DLOG)?;
        let docs = self.cf(CF_DOCS)?;
        let dtab = self.cf(CF_DTAB)?;
        let idx = self.cf(CF_IDX)?;

        if strategy == ConflictStrategy::Error && *options::CHECK_CONFLICTS {
            let _timer = metrics::conflict_check_timer();
            // The relational backends get this from a primary key. Here it is
            // one batched, bloom-filtered lookup per write; on the commit path
            // every key is new, so this is a filter miss and no I/O.
            let probes: Vec<_> = write
                .documents
                .iter()
                .map(|(k, ..)| (&dlog, &k[..]))
                .chain(write.indexes.iter().map(|(k, _)| (&idx, &k[..])))
                .collect();
            let n_documents = write.documents.len();
            // `multi_get_cf` reads committed state, so it cannot see the batch's
            // own rows: two entries naming the same key would both probe clean
            // and the later would silently shadow the earlier. Postgres's
            // `insert_document` is a plain INSERT with no ON CONFLICT clause and
            // so raises a primary-key violation for that input. The keys are
            // already materialised, so catching it here is a set insert per key.
            let mut seen = std::collections::BTreeSet::new();
            for (cf_is_dlog, key) in write
                .documents
                .iter()
                .map(|(k, ..)| (true, &k[..]))
                .chain(write.indexes.iter().map(|(k, _)| (false, &k[..])))
            {
                if !seen.insert((cf_is_dlog, key)) {
                    if cf_is_dlog {
                        let (ts, id) = keys::parse_dlog_key(key)?;
                        anyhow::bail!("document {id} appears twice at timestamp {ts} in one write");
                    }
                    let (index_id, dup_key, ts) = keys::parse_idx_key(key)?;
                    anyhow::bail!(
                        "index entry for {index_id:?} key {:?} appears twice at timestamp {ts} in \
                         one write",
                        dup_key.0,
                    );
                }
            }
            for (i, result) in self.db.multi_get_cf(probes).into_iter().enumerate() {
                if result?.is_some() {
                    if i < n_documents {
                        let (ts, id) = keys::parse_dlog_key(&write.documents[i].0)?;
                        anyhow::bail!("document {id} already exists at timestamp {ts}");
                    }
                    let (index_id, key, ts) =
                        keys::parse_idx_key(&write.indexes[i - n_documents].0)?;
                    anyhow::bail!(
                        "index entry for {index_id:?} key {:?} already exists at timestamp {ts}",
                        key.0,
                    );
                }
            }
        }

        let mut batch = WriteBatch::default();
        for (dlog_key, value, docs_key, dtab_key) in &write.documents {
            batch.put_cf(&dlog, dlog_key, value);
            batch.put_cf(&docs, docs_key, []);
            batch.put_cf(&dtab, dtab_key, []);
        }
        for (key, value) in &write.indexes {
            batch.put_cf(&idx, key, value);
        }

        let _timer = metrics::write_timer();
        self.engine_write("a document batch", || {
            self.db.write_opt(batch, &self.write_options())
        })
    }

    /// Delete every revision of `id` at or before `ts`, across all three
    /// document column families.
    ///
    /// Versions of one document are contiguous in `docs` and sort newest-first,
    /// so the ones to drop are a suffix of that run.
    fn delete_document_revisions(
        &self,
        batch: &mut WriteBatch,
        id: InternalDocumentId,
        ts: Timestamp,
    ) -> anyhow::Result<usize> {
        let docs = self.cf(CF_DOCS)?;
        let dlog = self.cf(CF_DLOG)?;
        let dtab = self.cf(CF_DTAB)?;

        let prefix = keys::docs_prefix(id);
        let mut iter = self.db.raw_iterator_cf(&docs);
        iter.seek(keys::docs_key(ts, id));
        let mut deleted = 0;
        while iter.valid() {
            let Some(key) = iter.key() else { break };
            if !key.starts_with(&prefix) {
                break;
            }
            let (found_ts, found_id) = keys::parse_docs_key(key)?;
            debug_assert_eq!(found_id, id);
            debug_assert!(found_ts <= ts);
            batch.delete_cf(&docs, key);
            batch.delete_cf(&dlog, keys::dlog_key(found_ts, id));
            batch.delete_cf(&dtab, keys::dtab_key(found_ts, id));
            deleted += 1;
            iter.next();
        }
        iter.status()?;
        Ok(deleted)
    }
}

#[async_trait]
impl Persistence for RocksDbPersistence {
    fn is_fresh(&self) -> bool {
        self.inner.newly_created
    }

    fn reader(&self) -> Arc<dyn PersistenceReader> {
        Arc::new(RocksDbPersistence {
            inner: self.inner.clone(),
        })
    }

    async fn write<'a>(
        &self,
        documents: &'a [DocumentLogEntry],
        indexes: &'a [PersistenceIndexEntry],
        conflict_strategy: ConflictStrategy,
    ) -> anyhow::Result<()> {
        if documents.is_empty() && indexes.is_empty() {
            return Ok(());
        }
        metrics::log_write(documents.len(), indexes.len());
        let write = PendingWrite::encode(documents, indexes)?;
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_write", move || {
            // Timed from inside the blocking task, because the thing being
            // watched for is a write that never returns: RocksDB stalls a
            // writer it cannot make progress for instead of failing it, and a
            // parked writer holds this thread until the volume recovers or the
            // process is stopped.
            inner.apply_write(write, conflict_strategy)
        })
        .await
        .context("rocksdb write task panicked")?
    }

    async fn write_persistence_global(
        &self,
        key: PersistenceGlobalKey,
        value: JsonValue,
    ) -> anyhow::Result<()> {
        let encoded = serde_json::to_vec(&value)?;
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_write_global", move || -> anyhow::Result<()> {
            let globals = inner.cf(CF_GLOBALS)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(&globals, String::from(key).as_bytes(), &encoded);
            inner.engine_write("a persistence global", || {
                inner.db.write_opt(batch, &inner.write_options())
            })?;
            Ok(())
        })
        .await
        .context("rocksdb global write task panicked")?
    }

    async fn load_index_chunk(
        &self,
        cursor: Option<IndexEntry>,
        chunk_size: usize,
    ) -> anyhow::Result<Vec<IndexEntry>> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_load_index_chunk", move || {
            inner.load_index_chunk(cursor, chunk_size)
        })
        .await
        .context("rocksdb index chunk task panicked")?
    }

    async fn delete_index_entries(&self, entries: Vec<IndexEntry>) -> anyhow::Result<usize> {
        let inner = self.inner.clone();
        tokio_spawn_blocking(
            "rocksdb_delete_index_entries",
            move || -> anyhow::Result<usize> {
                let idx = inner.cf(CF_IDX)?;

                // Retention deletes every version of a key at or before a
                // timestamp, and can name several expired versions of the same
                // key in one call. Collapsing to the highest timestamp per key
                // first keeps the returned count equal to the number of rows
                // actually removed, which is what the relational backends
                // report: their `DELETE ... WHERE a OR b` counts a row once
                // however many clauses match it.
                let mut highest_expired: BTreeMap<(IndexId, Vec<u8>), Timestamp> = BTreeMap::new();
                for entry in entries {
                    let mut full_key = entry.key_prefix;
                    if let Some(suffix) = entry.key_suffix {
                        full_key.extend_from_slice(&suffix);
                    }
                    highest_expired
                        .entry((entry.index_id, full_key))
                        .and_modify(|ts| *ts = (*ts).max(entry.ts))
                        .or_insert(entry.ts);
                }

                let mut batch = WriteBatch::default();
                let mut deleted = 0;
                for ((index_id, full_key), ts) in highest_expired {
                    // Versions sort newest-first, so the expired ones are a
                    // contiguous suffix of the key's run.
                    let prefix = keys::idx_key_prefix(index_id, &full_key);
                    let mut iter = inner.db.raw_iterator_cf(&idx);
                    iter.seek(keys::idx_key(index_id, &full_key, ts));
                    while iter.valid() {
                        let Some(key) = iter.key() else { break };
                        if !key.starts_with(&prefix) {
                            break;
                        }
                        batch.delete_cf(&idx, key);
                        deleted += 1;
                        iter.next();
                    }
                    iter.status()?;
                }
                inner.engine_write("a persistence global", || {
                    inner.db.write_opt(batch, &inner.write_options())
                })?;
                metrics::log_index_entries_deleted(deleted);
                Ok(deleted)
            },
        )
        .await
        .context("rocksdb index delete task panicked")?
    }

    async fn delete(
        &self,
        documents: Vec<(Timestamp, InternalDocumentId)>,
    ) -> anyhow::Result<usize> {
        let inner = self.inner.clone();
        tokio_spawn_blocking(
            "rocksdb_delete_documents",
            move || -> anyhow::Result<usize> {
                // Collapse to the highest expired timestamp per document: one
                // id can be named more than once across the input, and each
                // revision must count once — which is what Postgres's
                // `DELETE ... WHERE (a) OR (b)` does, since a row matched by
                // two clauses is still one deleted row.
                let mut highest_expired: BTreeMap<InternalDocumentId, Timestamp> = BTreeMap::new();
                for (ts, id) in documents {
                    highest_expired
                        .entry(id)
                        .and_modify(|existing| *existing = (*existing).max(ts))
                        .or_insert(ts);
                }

                let mut batch = WriteBatch::default();
                let mut deleted = 0;
                for (id, ts) in highest_expired {
                    deleted += inner.delete_document_revisions(&mut batch, id, ts)?;
                }
                inner.engine_write("an index-entry delete", || {
                    inner.db.write_opt(batch, &inner.write_options())
                })?;
                metrics::log_documents_deleted(deleted);
                Ok(deleted)
            },
        )
        .await
        .context("rocksdb document delete task panicked")?
    }

    async fn delete_tablet_documents(
        &self,
        tablet_id: TabletId,
        chunk_size: usize,
    ) -> anyhow::Result<usize> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_delete_tablet", move || -> anyhow::Result<usize> {
            // Same collapse as `delete_index_entries`: one document can be
            // named more than once, and each revision must count once.
            let docs = inner.cf(CF_DOCS)?;
            let (lower, upper) = keys::tablet_bounds(tablet_id);

            // Matches the relational backends: take up to `chunk_size` rows,
            // then remove every revision of the documents they belong to, so a
            // document is never left half-deleted across chunks.
            let mut ids = BTreeSet::new();
            let mut iter = inner.db.raw_iterator_cf(&docs);
            iter.seek(&lower);
            let mut scanned = 0;
            while iter.valid() && scanned < chunk_size {
                let Some(key) = iter.key() else { break };
                if !upper.is_empty() && key >= &upper[..] {
                    break;
                }
                let (_, id) = keys::parse_docs_key(key)?;
                ids.insert(id);
                scanned += 1;
                iter.next();
            }
            iter.status()?;

            let mut batch = WriteBatch::default();
            let mut deleted = 0;
            for id in ids {
                deleted += inner.delete_document_revisions(&mut batch, id, Timestamp::MAX)?;
            }
            inner.engine_write("a document delete", || {
                inner.db.write_opt(batch, &inner.write_options())
            })?;
            metrics::log_documents_deleted(deleted);
            Ok(deleted)
        })
        .await
        .context("rocksdb tablet delete task panicked")?
    }

    async fn finish_loading(&self) -> anyhow::Result<()> {
        // A bulk import leaves everything in L0. Flushing here turns the
        // memtables into files so the first reads after an import are not
        // served by scanning a large write buffer.
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_finish_loading", move || -> anyhow::Result<()> {
            // Deliberately no `write_watch` guard. `flush_cf` uses RocksDB's
            // default `FlushOptions`, whose `allow_write_stall = false` waits
            // for L0 to fall below the slowdown trigger — and "a bulk import
            // leaves everything in L0" is this function's own premise, so the
            // wait can be long and is healthy. Counting it as an in-flight
            // write would let a large import trip the stall ceiling and stop a
            // backend that is doing exactly what it was asked to.
            //
            // Nothing is lost: the guard exists so a *stalled acknowledged
            // write* is visible, and this is neither acknowledged nor a write.
            //
            // Every family, explicitly. `atomic_flush` does *not* widen a
            // single-family flush: `DBImpl::Flush` passes a one-element
            // candidate list into `SelectColumnFamiliesForAtomicFlush`, so
            // flushing `dlog` leaves the other four memtables where they are.
            // (The backup path's flush is different, and genuinely does cover
            // every family — `GetLiveFilesStorageInfo` selects with no
            // candidate list at all.) This loop is doing the work, not
            // repeating it.
            for name in ALL_COLUMN_FAMILIES {
                let cf = inner.cf(name)?;
                inner.engine_write("a flush", || inner.db.flush_cf(&cf))?;
            }
            Ok(())
        })
        .await
        .context("rocksdb flush task panicked")?
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_shutdown", move || -> anyhow::Result<()> {
            // Idempotent, because the trait's default is: the relational
            // backends inherit a no-op, so a deployment that wires SIGTERM to
            // `shutdown()` alongside an existing teardown path can call this
            // twice and must not get an error on the second. It would:
            // `cancel_all_background_work` sets RocksDB's `shutting_down_`
            // flag, after which every `flush_cf` returns `ShutdownInProgress`.
            if inner
                .shutdown_done
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(());
            }
            inner.db.flush_wal(true)?;
            for name in ALL_COLUMN_FAMILIES {
                let cf = inner.cf(name)?;
                inner.engine_write("a flush", || inner.db.flush_cf(&cf))?;
            }
            // Let in-flight compactions finish rather than leaving a large L0
            // for the next boot to recover through, but do not wait forever.
            let mut wait = rocksdb::WaitForCompactOptions::default();
            // RocksDB takes this bound in microseconds, not as a Duration.
            wait.set_timeout(options::SHUTDOWN_TIMEOUT.as_micros() as u64);
            wait.set_flush(true);
            if let Err(e) = inner.db.wait_for_compact(&wait) {
                tracing::warn!("rocksdb compactions still running at shutdown: {e}");
            }
            inner.db.cancel_all_background_work(true);
            // Latched only now: the guard exists so a *completed* shutdown can
            // be repeated, not so a failed one can be papered over. Setting it
            // before the flush made every retry of a shutdown that died at
            // ENOSPC return Ok having written nothing.
            inner
                .shutdown_done
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
        .context("rocksdb shutdown task panicked")?
    }
}

impl RocksDbPersistence {
    /// On-disk size per column family, for operational visibility.
    pub async fn table_sizes(&self) -> anyhow::Result<Vec<PersistenceTableSize>> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_table_sizes", move || -> anyhow::Result<_> {
            let mut out = Vec::new();
            for name in ALL_COLUMN_FAMILIES {
                let cf = inner.cf(name)?;
                let property = |key: &str| -> u64 {
                    inner
                        .db
                        .property_int_value_cf(&cf, key)
                        .ok()
                        .flatten()
                        .unwrap_or(0)
                };
                out.push(PersistenceTableSize {
                    table_name: name.to_string(),
                    data_bytes: property("rocksdb.live-sst-files-size")
                        + property("rocksdb.total-blob-file-size"),
                    // An LSM has no separable index structure to measure — the
                    // index and filter blocks are inside the SST files already
                    // counted above. Reporting anything here would be
                    // double-counting; this used to report
                    // `estimate-table-readers-mem`, which is memory, not disk.
                    index_bytes: 0,
                    row_count: Some(property("rocksdb.estimate-num-keys")),
                });
            }
            Ok(out)
        })
        .await
        .context("rocksdb table size task panicked")?
    }

    /// Path this database was opened from.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }
}

/// Resolves document bodies for coordinates an index or table scan produced.
///
/// `read_opts` must carry the *same* snapshot the scan's iterator used.
/// Without it the two phases read two different points in time: the iterator
/// pins a sequence number for its lifetime, a default-options `multi_get` reads
/// the latest state, and a retention delete landing between them removes the
/// body of a row the iterator had already returned. The scan then fails with
/// "missing its body" or "dangling index reference" — for data that was
/// perfectly consistent at the snapshot it claimed to be reading. The
/// relational backends cannot hit this because they resolve the body in the
/// same statement, at one MVCC snapshot.
pub(crate) fn multi_get_documents(
    inner: &Inner,
    coordinates: &[(Timestamp, InternalDocumentId)],
    read_opts: &rocksdb::ReadOptions,
) -> anyhow::Result<
    BTreeMap<(Timestamp, InternalDocumentId), (Option<ResolvedDocument>, Option<Timestamp>)>,
> {
    if coordinates.is_empty() {
        return Ok(BTreeMap::new());
    }
    let dlog = inner.cf(CF_DLOG)?;
    let encoded: Vec<_> = coordinates
        .iter()
        .map(|(ts, id)| keys::dlog_key(*ts, *id))
        .collect();
    let results = inner
        .db
        .multi_get_cf_opt(encoded.iter().map(|key| (&dlog, &key[..])), read_opts);

    let mut out = BTreeMap::new();
    for ((ts, id), result) in coordinates.iter().zip(results) {
        let Some(bytes) = result? else { continue };
        let decoded = codec::decode_document(id.table(), &bytes)
            .with_context(|| format!("failed to decode document {id} at {ts}"))?;
        out.insert((*ts, *id), (decoded.value, decoded.prev_ts));
    }
    Ok(out)
}
