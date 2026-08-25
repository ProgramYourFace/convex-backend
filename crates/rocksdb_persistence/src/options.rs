//! RocksDB configuration and the knobs that tune it.
//!
//! Knobs are read with `cmd_util::env::env_config`, the same mechanism the rest
//! of the backend uses, so they are set as ordinary environment variables and
//! this crate needs no entry in `common::knobs`.

use std::{
    sync::LazyLock,
    time::Duration,
};

use cmd_util::env::env_config;
use rocksdb::{
    BlockBasedOptions,
    Cache,
    DBCompressionType,
    Options,
    WriteBufferManager,
};

use crate::keys::{
    CF_DLOG,
    CF_IDX,
};

/// Whether `Persistence::write` fsyncs the write-ahead log before returning.
///
/// On by default: Convex's contract is that a returned write is durable, and a
/// Postgres backend at its default `synchronous_commit` behaves the same way.
/// Turning it off trades recent writes on host loss for throughput, and is only
/// safe where an upstream log can replay into idempotent appliers.
///
/// Ignored when [`SYNC_INTERVAL`] is set — see [`SyncMode`].
pub static SYNC_WRITES: LazyLock<bool> = LazyLock::new(|| env_config("ROCKSDB_SYNC_WRITES", true));

/// Flush and fsync the write-ahead log on this interval instead of on every
/// write. Zero, the default, disables it.
pub static SYNC_INTERVAL: LazyLock<Option<Duration>> =
    LazyLock::new(|| match env_config("ROCKSDB_SYNC_INTERVAL_MS", 0u64) {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    });

/// How the write-ahead log reaches disk.
///
/// The three modes are a durability/throughput curve, not three ways of
/// spelling the same thing. What each one loses is different in kind, so the
/// choice belongs to whoever knows what is upstream of the database:
///
/// | Mode | On process crash | On host loss |
/// |---|---|---|
/// | [`Every`](SyncMode::Every) | nothing | nothing |
/// | [`Interval`](SyncMode::Interval) | up to one interval | up to one interval |
/// | [`Never`](SyncMode::Never) | nothing | unbounded — whatever the OS had not written |
///
/// [`Interval`](SyncMode::Interval)'s bound holds under two conditions worth
/// stating, because neither is automatic. Dropping the database handle flushes
/// the buffer, so an orderly teardown loses nothing — but
/// `Persistence::shutdown` has no caller anywhere in the backend, for any
/// storage engine, so an exit that never drops the handle is a crash as far as
/// this mode is concerned. And a flush that keeps *failing* accumulates
/// acknowledged-but-unwritten data without bound, which is why
/// [`crate::health`] escalates a stale flush clock to a process shutdown rather
/// than trusting the interval alone.
///
/// `Interval` is the only mode that can lose a write the *process* never got a
/// chance to hand to the kernel: it turns on RocksDB's manual WAL flush, so
/// records sit in RocksDB's own buffer until the flusher thread moves them.
/// `Never` still writes through to the page cache on every write, so it
/// survives a crash of this process and only loses data if the machine goes
/// down — but with no bound on how much.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync before `write` returns. RocksDB coalesces concurrent writers into
    /// one write group and syncs the shared WAL once for the group.
    Every,
    /// Flush and fsync the WAL on a timer, on a background thread.
    Interval(Duration),
    /// Leave it to the operating system.
    Never,
}

impl SyncMode {
    /// Resolved from the environment once, at first use.
    pub fn current() -> Self {
        // An explicit interval wins: asking for a timed flush and a per-write
        // fsync at once is contradictory, and the interval is the more
        // specific request.
        match (*SYNC_INTERVAL, *SYNC_WRITES) {
            (Some(interval), _) => Self::Interval(interval),
            (None, true) => Self::Every,
            (None, false) => Self::Never,
        }
    }

    /// Whether `WriteOptions::set_sync` should be on for this mode.
    pub fn sync_each_write(&self) -> bool {
        matches!(self, Self::Every)
    }
}

/// Whether `ConflictStrategy::Error` is enforced on every write.
///
/// The relational backends get this free from a primary key; an LSM has no
/// unique constraint, so enforcing it costs one bloom-filtered point get per
/// key written. On by default so the backend's semantics match Postgres.
/// See the crate docs for what is lost by turning it off.
pub static CHECK_CONFLICTS: LazyLock<bool> =
    LazyLock::new(|| env_config("ROCKSDB_CHECK_CONFLICTS", true));

/// Fraction of the container's memory limit to give the block cache when
/// `ROCKSDB_BLOCK_CACHE_BYTES` is not set, as a percentage.
///
/// Deliberately not half. The backend that hosts this crate also runs V8
/// isolates, whose heaps are the other large consumer in the process, and
/// RocksDB's own compaction buffers, iterators and WAL buffers sit outside the
/// cache. A quarter leaves room for all of it; raise it on a deployment whose
/// working set matters more than its isolate headroom.
pub static BLOCK_CACHE_PERCENT: LazyLock<u64> =
    LazyLock::new(|| env_config::<u64>("ROCKSDB_BLOCK_CACHE_PERCENT", 25).clamp(1, 90));

/// Floor for the derived cache size. Below this RocksDB thrashes on its own
/// index and filter blocks, and a cache that small is worse than useless.
const MIN_DERIVED_CACHE_BYTES: usize = 64 << 20;

/// Ceiling for the derived cache size. A very large host does not mean a very
/// large cache is wanted by default; past this, set it explicitly.
const MAX_DERIVED_CACHE_BYTES: usize = 4 << 30;

/// Fallback when no cgroup memory limit applies to this process. Physical
/// memory is only ever used to *cap* a cgroup limit, never as a substitute for
/// one, so an unconstrained host gets this flat value regardless of its size —
/// set `ROCKSDB_BLOCK_CACHE_BYTES` there.
const DEFAULT_CACHE_BYTES: usize = 512 << 20;

/// Shared block cache across every column family, in bytes.
///
/// This is the backend's RocksDB memory budget, not just its read cache:
/// memtable memory is charged against it too (see [`build`]), so cached data,
/// index and filter blocks and unflushed writes all come out of this one
/// number. Steady-state usage runs somewhat above it, because compaction
/// buffers, iterators and WAL buffers are outside the cache.
///
/// Unset, it is derived from the container's memory limit
/// ([`crate::memory::container_limit_bytes`]) rather than from a constant,
/// because RocksDB reads no cgroup limit and a fixed default that fits a
/// generous host will get the backend OOM-killed on a small one.
pub static BLOCK_CACHE_BYTES: LazyLock<usize> = LazyLock::new(|| {
    if let Some(explicit) = explicit_cache_bytes() {
        tracing::info!(
            "rocksdb block cache: {} MiB (ROCKSDB_BLOCK_CACHE_BYTES)",
            explicit >> 20
        );
        return explicit;
    }
    match crate::memory::container_limit_bytes() {
        Some(limit) => {
            let derived = ((limit / 100) * *BLOCK_CACHE_PERCENT) as usize;
            let clamped = derived.clamp(MIN_DERIVED_CACHE_BYTES, MAX_DERIVED_CACHE_BYTES);
            tracing::info!(
                "rocksdb block cache: {} MiB ({}% of a {} MiB memory limit)",
                clamped >> 20,
                *BLOCK_CACHE_PERCENT,
                limit >> 20,
            );
            clamped
        },
        None => {
            tracing::info!(
                "rocksdb block cache: {} MiB (no memory limit found; set \
                 ROCKSDB_BLOCK_CACHE_BYTES to size this deliberately)",
                DEFAULT_CACHE_BYTES >> 20,
            );
            DEFAULT_CACHE_BYTES
        },
    }
});

/// `env_config` needs a default to compare against, which would make "unset"
/// indistinguishable from "set to the default". The cache size has to tell
/// those apart, so it is read directly.
fn explicit_cache_bytes() -> Option<usize> {
    match std::env::var("ROCKSDB_BLOCK_CACHE_BYTES") {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(bytes) if bytes > 0 => Some(bytes),
            _ => {
                tracing::warn!("ignoring invalid ROCKSDB_BLOCK_CACHE_BYTES={raw:?}");
                None
            },
        },
        Err(_) => None,
    }
}

/// Ceiling on memtable memory across every column family, in bytes.
///
/// A share of [`BLOCK_CACHE_BYTES`] rather than memory on top of it, so the
/// default is a quarter of the cache. Letting memtables grow into the whole
/// budget would evict every data, index and filter block — including the bloom
/// filters the uniqueness check depends on — and turn reads into I/O exactly
/// when writes are heaviest.
pub static WRITE_BUFFER_BYTES: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_WRITE_BUFFER_BYTES", *BLOCK_CACHE_BYTES / 4));

/// Per-column-family memtable size, in bytes.
pub static MEMTABLE_BYTES: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_MEMTABLE_BYTES", 64 << 20));

/// Background flush and compaction threads. Zero means available parallelism.
///
/// RocksDB's default `Env` is a process-wide singleton whose low- and
/// high-priority thread pools are shared by every database opened against it,
/// and `max_background_jobs` grows those pools to the largest value any
/// database asked for rather than to their sum. So this is a PROCESS budget
/// even when several databases are open, and a host running many databases does
/// not need to divide it — see [`DbTuning::background_jobs`] for when it should
/// anyway.
pub static BACKGROUND_JOBS: LazyLock<i32> =
    LazyLock::new(|| env_config("ROCKSDB_BACKGROUND_JOBS", 0));

/// Table-file descriptors a database may hold open. `-1`, the default, means
/// unlimited, which is right for the one database a single-tenant backend opens
/// and wrong for a process that opens many: file descriptors are a per-PROCESS
/// resource, so N unlimited databases race each other to `EMFILE`. A host that
/// opens several databases passes a bounded value per database through
/// [`DbTuning::max_open_files`].
pub static MAX_OPEN_FILES: LazyLock<i32> =
    LazyLock::new(|| env_config("ROCKSDB_MAX_OPEN_FILES", -1));

/// Documents at or above this many bytes are stored outside the LSM tree, in
/// blob files, so that compacting the `dlog` column family does not rewrite
/// large document bodies over and over. Zero disables key-value separation.
pub static BLOB_THRESHOLD_BYTES: LazyLock<u64> =
    LazyLock::new(|| env_config("ROCKSDB_BLOB_THRESHOLD_BYTES", 4096u64));

/// Rows read per page by the streaming read paths. Each page is one
/// `spawn_blocking` hop, so this trades syscall overhead against how long a
/// blocking thread is held.
pub static SCAN_PAGE_ROWS: LazyLock<usize> =
    LazyLock::new(|| env_config::<usize>("ROCKSDB_SCAN_PAGE_ROWS", 1024).max(1));

/// How often the periodic backup worker takes a generation. Only consulted
/// when `ROCKSDB_BACKUP_DIR` is set.
pub static BACKUP_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    Duration::from_secs(env_config::<u64>("ROCKSDB_BACKUP_INTERVAL_SECONDS", 3600).max(60))
});

/// Generations retained before `purge_old_backups`. Zero disables pruning,
/// which grows without bound — a deliberate choice rather than a default.
pub static BACKUP_KEEP: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_BACKUP_KEEP", 24usize));

/// How often the health monitor polls for a latched background error, a
/// write-ahead log that has stopped being flushed, and the age of the newest
/// backup. Short enough that a stuck database is noticed in seconds, not the
/// hour a backup interval would impose.
pub static HEALTH_POLL_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    Duration::from_secs(env_config::<u64>("ROCKSDB_HEALTH_POLL_SECONDS", 15).max(1))
});

/// How long `shutdown` waits for background compactions to settle.
pub static SHUTDOWN_TIMEOUT: LazyLock<Duration> =
    LazyLock::new(|| Duration::from_secs(env_config("ROCKSDB_SHUTDOWN_TIMEOUT_SECONDS", 30u64)));

fn default_background_jobs() -> i32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4)
}

/// The memory RocksDB is allowed to use, for the whole PROCESS.
///
/// [`BLOCK_CACHE_BYTES`] is derived from the container's memory limit, so it is
/// a statement about the process, not about one database. A process that opens
/// several databases — a host serving several tenants, each with its own store
/// — must therefore give them ONE cache between them: N caches each sized at a
/// quarter of the container would oversubscribe memory by N and get the backend
/// OOM-killed, which is precisely the failure the derived sizing exists to
/// prevent.
///
/// Both objects are refcounted handles in RocksDB; cloning them shares the
/// underlying structure. For the single database a single-tenant backend opens,
/// sharing is indistinguishable from not sharing.
struct SharedMemory {
    cache: Cache,
    write_buffer_manager: WriteBufferManager,
}

/// The process's one block cache and one write-buffer manager.
///
/// Memtable memory is charged against the cache
/// (`new_write_buffer_manager_with_cache`), so this single number bounds cached
/// blocks AND unflushed writes across every database in the process.
static SHARED_MEMORY: LazyLock<SharedMemory> = LazyLock::new(|| {
    let cache = Cache::new_lru_cache(*BLOCK_CACHE_BYTES);
    let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
        *WRITE_BUFFER_BYTES,
        true,
        cache.clone(),
    );
    SharedMemory {
        cache,
        write_buffer_manager,
    }
});

/// Per-database knobs a process that opens several databases needs to set
/// independently of the process-wide ones.
///
/// Every field is `None` by default and falls back to the corresponding
/// environment knob, so a single-tenant backend behaves exactly as it did
/// before this struct existed.
#[derive(Clone, Copy, Debug, Default)]
pub struct DbTuning {
    /// Overrides [`BACKGROUND_JOBS`]. The thread pools are shared process-wide
    /// (see that knob), so the reason to lower this per database is not thread
    /// count but *concurrency per store*: a value of N lets one database occupy
    /// N of the shared threads at once, and leaving every database at the
    /// host's full parallelism lets one compacting tenant monopolise the
    /// pool.
    pub background_jobs: Option<i32>,
    /// Overrides [`MAX_OPEN_FILES`]. Descriptors are a process resource; a host
    /// opening many databases must divide its `RLIMIT_NOFILE` between them
    /// rather than let each one take what it likes.
    pub max_open_files: Option<i32>,
    /// Overrides [`MEMTABLE_BYTES`], the per-column-family memtable size.
    ///
    /// The shared write-buffer manager already bounds TOTAL memtable memory, so
    /// this cannot overcommit; what it controls is the shape of that bound.
    /// Left at the single-tenant default, N databases × 5 column families
    /// all want 64 MiB and the manager answers by force-flushing whichever
    /// memtable is largest, turning a healthy budget into a stream of
    /// premature flushes. A host with many databases wants a smaller
    /// per-family target.
    pub memtable_bytes: Option<usize>,
}

impl DbTuning {
    fn background_jobs(&self) -> i32 {
        match self.background_jobs.unwrap_or(*BACKGROUND_JOBS) {
            0 => default_background_jobs(),
            n => n,
        }
    }

    fn max_open_files(&self) -> i32 {
        self.max_open_files.unwrap_or(*MAX_OPEN_FILES)
    }

    fn memtable_bytes(&self) -> usize {
        self.memtable_bytes.unwrap_or(*MEMTABLE_BYTES)
    }
}

/// Options shared by the database and every column family.
pub struct RocksOptions {
    /// Database-wide options.
    pub db: Options,
    /// Block cache shared by every column family — and, since
    /// [`SHARED_MEMORY`], by every other database in this process.
    pub cache: Cache,
    /// Held so the manager outlives the column families that reference it.
    pub write_buffer_manager: WriteBufferManager,
    /// Carried through to [`column_family`], which needs the memtable size.
    tuning: DbTuning,
}

/// Build the database-wide options and the objects they reference.
pub fn build(create_if_missing: bool, sync: SyncMode) -> RocksOptions {
    build_with(create_if_missing, sync, DbTuning::default())
}

/// [`build`], with per-database overrides for the knobs a multi-database
/// process must set itself.
pub fn build_with(create_if_missing: bool, sync: SyncMode, tuning: DbTuning) -> RocksOptions {
    let cache = SHARED_MEMORY.cache.clone();
    let write_buffer_manager = SHARED_MEMORY.write_buffer_manager.clone();

    let mut db = Options::default();
    db.create_if_missing(create_if_missing);
    db.create_missing_column_families(create_if_missing);

    // A single `Persistence::write` spans every column family, and Convex
    // depends on it landing atomically — an index entry whose document did not
    // survive recovery is a dangling reference. Atomic flush makes the column
    // families' memtables flush as a unit so recovery restores a consistent
    // cut across all of them.
    db.set_atomic_flush(true);

    db.set_max_background_jobs(tuning.background_jobs());
    db.set_write_buffer_manager(&write_buffer_manager);
    // Smooth writeback rather than dumping whole files at the page cache.
    db.set_bytes_per_sync(1 << 20);
    db.set_wal_bytes_per_sync(1 << 20);
    match sync {
        SyncMode::Interval(interval) => {
            // Hold WAL records in RocksDB's buffer and let the flusher thread
            // move them, so a run of writes costs one fsync rather than one
            // each.
            db.set_manual_wal_flush(true);
            tracing::info!(
                "rocksdb WAL sync: every {}ms on a background thread",
                interval.as_millis(),
            );
        },
        SyncMode::Every => tracing::info!("rocksdb WAL sync: on every write"),
        SyncMode::Never => {
            tracing::info!("rocksdb WAL sync: left to the operating system")
        },
    }
    db.set_max_total_wal_size(1 << 30);
    db.set_keep_log_file_num(8);
    db.set_max_open_files(tuning.max_open_files());
    // Recover as much of a torn tail as is consistent, then carry on. The
    // alternative, `AbsoluteConsistency`, refuses to open at all after an
    // unclean shutdown that left a partial record.
    db.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);

    RocksOptions {
        db,
        cache,
        write_buffer_manager,
        tuning,
    }
}

/// Per-column-family options.
///
/// `dlog` holds document bodies and is the target of `index_scan`'s join, so it
/// gets key-value separation and a bloom filter. `docs`, `dtab` and `idx` hold
/// keys with empty or tiny values.
pub fn column_family(name: &str, shared: &RocksOptions) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(shared.tuning.memtable_bytes());
    opts.set_max_write_buffer_number(3);
    opts.set_min_write_buffer_number_to_merge(1);
    opts.set_write_buffer_manager(&shared.write_buffer_manager);

    // Dynamic level sizing keeps space amplification near 1.1x instead of the
    // ~2x a statically-sized level tree drifts to under a write-heavy load.
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);

    // Cheap compression where compaction is hottest, stronger compression where
    // data comes to rest.
    opts.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ]);

    let mut block = BlockBasedOptions::default();
    block.set_block_cache(&shared.cache);
    block.set_block_size(16 << 10);
    // Index and filter blocks live in the cache rather than pinned in RAM, so
    // total memory stays bounded by the cache size as the database grows past
    // it. The top level stays pinned so hot lookups keep their filter.
    block.set_cache_index_and_filter_blocks(true);
    block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block.set_format_version(5);

    // `dlog` and `docs` are read by point lookup, where a filter turns a miss
    // into no I/O at all. `check_generated_ids` runs such a lookup for every
    // newly generated document id on every commit, and nearly all of them miss.
    // `idx` is read by range seek, which filters cannot short-circuit, and
    // `dtab`/`globals` are scanned.
    if matches!(name, crate::keys::CF_DLOG | crate::keys::CF_DOCS) {
        block.set_bloom_filter(10.0, false);
    }
    opts.set_block_based_table_factory(&block);

    if name == CF_DLOG && *BLOB_THRESHOLD_BYTES > 0 {
        // Document bodies are the only large values in the store. Keeping them
        // out of the LSM tree stops compaction from rewriting them at every
        // level, which is most of the write amplification on this workload.
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(*BLOB_THRESHOLD_BYTES);
        opts.set_blob_compression_type(DBCompressionType::Zstd);
        opts.set_enable_blob_gc(true);
        opts.set_blob_gc_age_cutoff(0.25);
    }

    if name == CF_IDX {
        // Index scans walk one key's versions and then skip to the next key, so
        // they benefit from larger blocks and lose nothing to the absence of a
        // whole-key filter.
        opts.set_optimize_filters_for_hits(true);
    }

    opts
}
