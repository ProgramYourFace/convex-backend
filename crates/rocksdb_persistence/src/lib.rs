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
mod health;
pub mod keys;
mod memory;
mod metrics;
pub mod options;
mod reader;
mod wal_flush;

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

/// The gauge label for a database opened without an explicit instance name: the
/// last component of its directory, which for a single-tenant backend is the
/// data directory's own name and for anything else is at least unique within
/// the process.
fn default_instance_label(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// What a read-back check actually managed to decode.
#[derive(Default, Debug, Clone, Copy)]
pub struct ReadCheck {
    pub rows: usize,
    pub documents: usize,
    pub index_entries: usize,
}

/// How to open a database.
///
/// A struct rather than a run of positional flags because these are unrelated
/// decisions — durability, failure escalation, whether this process owns the
/// database's background work, and how this database shares the process with
/// any others — and an `open(path, true, false, None)` at a call site says none
/// of that.
#[derive(Default)]
pub struct OpenOptions {
    /// Durability mode. `None` takes it from the environment.
    pub sync: Option<options::SyncMode>,
    /// Where to report a latched background error. Without one, a database
    /// that has stopped accepting writes keeps the process alive.
    pub shutdown: Option<ShutdownSignal>,
    /// Whether to run background work — the interval WAL flusher, the periodic
    /// backup worker and the health monitor. False for short-lived opens such
    /// as a restore rehearsal, which must not attach either to the production
    /// backup chain or to a shutdown signal.
    pub background: bool,
    /// Which deployment this database belongs to, used to label the
    /// per-database gauges (backup age, WAL flush age).
    ///
    /// Those gauges are levels, not rates, so N unlabelled databases in one
    /// process would each overwrite the others and the series would report
    /// whichever wrote last. `None` labels them with the directory name, which
    /// is right for the one database a single-tenant backend opens.
    pub instance: Option<String>,
    /// Where the periodic backup worker writes, overriding
    /// `ROCKSDB_BACKUP_DIR`.
    ///
    /// A process with several databases MUST set this per database:
    /// `BackupEngine` generations are numbered per directory with no notion of
    /// which database produced them, so two databases pointed at one directory
    /// interleave their generations and each one's `purge_old_backups` deletes
    /// the other's.
    pub backup_dir: Option<PathBuf>,
    /// Per-database RocksDB knobs. Defaults defer to the environment, which is
    /// what a single-tenant backend wants; see [`options::DbTuning`].
    pub tuning: options::DbTuning,
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
    /// Label for this database's own gauges. See [`OpenOptions::instance`].
    pub(crate) instance: String,
    /// Held for the lifetime of the database so the shared block cache and
    /// memtable budget outlive the column families that reference them.
    _cache: rocksdb::Cache,
    _write_buffer_manager: rocksdb::WriteBufferManager,
    /// Resolved once when the database is opened, rather than read from the
    /// environment per write, so a single process can hold databases in
    /// different modes — which is what makes the mode testable.
    sync: options::SyncMode,
    /// Present only in [`options::SyncMode::Interval`]. Populated after the
    /// `Arc<Inner>` exists, because the thread needs a `Weak` back to it.
    wal_flusher: std::sync::OnceLock<wal_flush::WalFlusher>,
    /// Present only when `ROCKSDB_BACKUP_DIR` is set. Same `Weak` reasoning.
    backup_worker: std::sync::OnceLock<backup::BackupWorker>,
    /// Present on any primary opened with a shutdown signal. Same `Weak`
    /// reasoning.
    health: std::sync::OnceLock<health::HealthMonitor>,
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

    /// Open a read-only view of a database another process has open for
    /// writing.
    ///
    /// RocksDB allows exactly one writer per directory, so a second reader
    /// cannot simply open the same path. A *secondary* instance is the
    /// supported way to do this: it reads the primary's files without taking
    /// the write lock, and catches up to the primary's log on demand — which
    /// [`Inner::refresh`] does before every read.
    ///
    /// `secondary_path` needs its own writable directory for the instance's
    /// own bookkeeping; it holds no user data.
    pub fn new_secondary(path: &Path, secondary_path: &Path) -> anyhow::Result<Self> {
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
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                newly_created: false,
                secondary: true,
                instance: default_instance_label(path),
                path: path.to_path_buf(),
                _cache: shared.cache,
                _write_buffer_manager: shared.write_buffer_manager,
                // A secondary instance never writes, so it has no WAL to flush.
                sync: options::SyncMode::Every,
                wal_flusher: std::sync::OnceLock::new(),
                backup_worker: std::sync::OnceLock::new(),
                health: std::sync::OnceLock::new(),
            }),
        })
    }

    fn open(path: &Path, create_if_missing: bool, opts: OpenOptions) -> anyhow::Result<Self> {
        let sync = opts.sync.unwrap_or_else(options::SyncMode::current);
        // RocksDB writes a CURRENT file as the last step of creating a
        // database, so its absence is the reliable "nothing here yet" signal —
        // more so than the directory existing, which a mount or a failed
        // earlier attempt can also produce.
        let newly_created = !path.join("CURRENT").exists();
        if newly_created && !create_if_missing {
            anyhow::bail!("no RocksDB database at {}", path.display());
        }

        let shared = options::build_with(create_if_missing, sync, opts.tuning);
        let cfs: Vec<_> = ALL_COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options::column_family(name, &shared)))
            .collect();

        let db = Db::open_cf_descriptors(&shared.db, path, cfs)
            .with_context(|| format!("failed to open RocksDB at {}", path.display()))?;

        tracing::info!(
            "opened RocksDB persistence at {} ({})",
            path.display(),
            if newly_created { "fresh" } else { "existing" },
        );

        let inner = Arc::new(Inner {
            db,
            newly_created,
            secondary: false,
            instance: opts
                .instance
                .clone()
                .unwrap_or_else(|| default_instance_label(path)),
            path: path.to_path_buf(),
            _cache: shared.cache,
            _write_buffer_manager: shared.write_buffer_manager,
            sync,
            wal_flusher: std::sync::OnceLock::new(),
            backup_worker: std::sync::OnceLock::new(),
            health: std::sync::OnceLock::new(),
        });

        if opts.background
            && let options::SyncMode::Interval(interval) = sync
        {
            let flusher = wal_flush::WalFlusher::spawn(Arc::downgrade(&inner), interval)?;
            let _ = inner.wal_flusher.set(flusher);
        }

        // A backup directory is only ever taken from the environment, and only
        // when the caller wants background work at all. `rocksdb-backup` opens
        // databases too, and a rehearsal or a restore run in the backend's own
        // environment must not attach a worker that would write generations of
        // a scratch database into the production backup chain.
        let backup_config = opts
            .background
            .then(|| match &opts.backup_dir {
                Some(dir) => Some(backup::BackupConfig::for_dir(dir.clone())),
                None => backup::BackupConfig::from_env(),
            })
            .flatten();
        if let Some(config) = backup_config.clone() {
            let worker = backup::BackupWorker::spawn(Arc::downgrade(&inner), config)?;
            let _ = inner.backup_worker.set(worker);
        }

        if opts.background
            && let Some(shutdown) = opts.shutdown
        {
            let monitor = health::HealthMonitor::spawn(
                Arc::downgrade(&inner),
                shutdown,
                backup_config.map(|c| c.dir),
                inner.instance.clone(),
            )?;
            let _ = inner.health.set(monitor);
        }

        Ok(Self { inner })
    }
}

/// Stops the background threads and flushes the write-ahead log before
/// `Inner`'s fields — `db` among them — are dropped.
///
/// `Persistence::shutdown` is the intended teardown, but nothing in the tree
/// calls it: `Database::shutdown` stops the committer and its workers and
/// returns, and no relational backend overrides `shutdown` either, so the hook
/// was never wired up. Dropping the handle is therefore the path production
/// actually takes, and it has to be the safe one. Without this, `db` — the
/// first field — would close while the workers were still running, and in
/// `SyncMode::Interval` the WAL buffer would be discarded rather than written.
impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(monitor) = self.health.get() {
            monitor.stop();
        }
        if let Some(worker) = self.backup_worker.get() {
            worker.stop();
        }
        if let Some(flusher) = self.wal_flusher.get() {
            flusher.stop();
        }
        // Only interval mode can be holding acknowledged writes in RocksDB's
        // buffer; the other modes have already handed them to the kernel.
        if matches!(self.sync, options::SyncMode::Interval(_))
            && !self.secondary
            && let Err(e) = self.db.flush_wal(true)
        {
            tracing::error!("failed to flush the RocksDB WAL while closing: {e}");
        }
    }
}

impl Inner {
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
        self.db.write_opt(batch, &self.write_options())?;
        Ok(())
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
            inner.db.write_opt(batch, &inner.write_options())?;
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
                inner.db.write_opt(batch, &inner.write_options())?;
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
                // Same collapse as `delete_index_entries`: one document can be
                // named more than once, and each revision must count once.
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
                inner.db.write_opt(batch, &inner.write_options())?;
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
            inner.db.write_opt(batch, &inner.write_options())?;
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
            for name in ALL_COLUMN_FAMILIES {
                inner.db.flush_cf(&inner.cf(name)?)?;
            }
            Ok(())
        })
        .await
        .context("rocksdb flush task panicked")?
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_shutdown", move || -> anyhow::Result<()> {
            // Stop the background threads before touching the database, so
            // neither a timed flush nor a backup can land in the middle of the
            // close below. The `flush_wal` that follows covers whatever the
            // flusher had not yet written.
            if let Some(monitor) = inner.health.get() {
                monitor.stop();
            }
            if let Some(worker) = inner.backup_worker.get() {
                worker.stop();
            }
            if let Some(flusher) = inner.wal_flusher.get() {
                flusher.stop();
            }
            inner.db.flush_wal(true)?;
            for name in ALL_COLUMN_FAMILIES {
                inner.db.flush_cf(&inner.cf(name)?)?;
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
                    index_bytes: property("rocksdb.estimate-table-readers-mem"),
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

/// Reconstruct documents for a set of `(ts, id)` pairs with one batched lookup.
///
/// `index_scan` and the `previous_revisions` family all resolve a list of
/// document coordinates into bodies; batching turns what would be one point get
/// per row into a single `multi_get`.
pub(crate) fn multi_get_documents(
    inner: &Inner,
    coordinates: &[(Timestamp, InternalDocumentId)],
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
        .multi_get_cf(encoded.iter().map(|key| (&dlog, &key[..])));

    let mut out = BTreeMap::new();
    for ((ts, id), result) in coordinates.iter().zip(results) {
        let Some(bytes) = result? else { continue };
        let decoded = codec::decode_document(id.table(), &bytes)
            .with_context(|| format!("failed to decode document {id} at {ts}"))?;
        out.insert((*ts, *id), (decoded.value, decoded.prev_ts));
    }
    Ok(out)
}
