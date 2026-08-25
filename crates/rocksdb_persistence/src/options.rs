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
/// Turning it off trades a bounded window of recent writes on host loss for
/// throughput, and is only safe where an upstream log can replay into
/// idempotent appliers.
pub static SYNC_WRITES: LazyLock<bool> = LazyLock::new(|| env_config("ROCKSDB_SYNC_WRITES", true));

/// Whether `ConflictStrategy::Error` is enforced on every write.
///
/// The relational backends get this free from a primary key; an LSM has no
/// unique constraint, so enforcing it costs one bloom-filtered point get per
/// key written. On by default so the backend's semantics match Postgres.
/// See the crate docs for what is lost by turning it off.
pub static CHECK_CONFLICTS: LazyLock<bool> =
    LazyLock::new(|| env_config("ROCKSDB_CHECK_CONFLICTS", true));

/// Shared block cache across every column family, in bytes.
pub static BLOCK_CACHE_BYTES: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_BLOCK_CACHE_BYTES", 512 << 20));

/// Ceiling on memtable memory across every column family, in bytes.
pub static WRITE_BUFFER_BYTES: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_WRITE_BUFFER_BYTES", 512 << 20));

/// Per-column-family memtable size, in bytes.
pub static MEMTABLE_BYTES: LazyLock<usize> =
    LazyLock::new(|| env_config("ROCKSDB_MEMTABLE_BYTES", 64 << 20));

/// Background flush and compaction threads. Zero means available parallelism.
pub static BACKGROUND_JOBS: LazyLock<i32> =
    LazyLock::new(|| env_config("ROCKSDB_BACKGROUND_JOBS", 0));

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

/// How long `shutdown` waits for background compactions to settle.
pub static SHUTDOWN_TIMEOUT: LazyLock<Duration> =
    LazyLock::new(|| Duration::from_secs(env_config("ROCKSDB_SHUTDOWN_TIMEOUT_SECONDS", 30u64)));

fn background_jobs() -> i32 {
    match *BACKGROUND_JOBS {
        0 => std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4),
        n => n,
    }
}

/// Options shared by the database and every column family.
pub struct RocksOptions {
    /// Database-wide options.
    pub db: Options,
    /// Block cache shared by every column family.
    pub cache: Cache,
    /// Held so the manager outlives the column families that reference it.
    pub write_buffer_manager: WriteBufferManager,
}

/// Build the database-wide options and the objects they reference.
pub fn build(create_if_missing: bool) -> RocksOptions {
    let cache = Cache::new_lru_cache(*BLOCK_CACHE_BYTES);
    let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
        *WRITE_BUFFER_BYTES,
        true,
        cache.clone(),
    );

    let mut db = Options::default();
    db.create_if_missing(create_if_missing);
    db.create_missing_column_families(create_if_missing);

    // A single `Persistence::write` spans every column family, and Convex
    // depends on it landing atomically — an index entry whose document did not
    // survive recovery is a dangling reference. Atomic flush makes the column
    // families' memtables flush as a unit so recovery restores a consistent
    // cut across all of them.
    db.set_atomic_flush(true);

    db.set_max_background_jobs(background_jobs());
    db.set_write_buffer_manager(&write_buffer_manager);
    // Smooth writeback rather than dumping whole files at the page cache.
    db.set_bytes_per_sync(1 << 20);
    db.set_wal_bytes_per_sync(1 << 20);
    db.set_max_total_wal_size(1 << 30);
    db.set_keep_log_file_num(8);
    db.set_max_open_files(-1);
    // Recover as much of a torn tail as is consistent, then carry on. The
    // alternative, `AbsoluteConsistency`, refuses to open at all after an
    // unclean shutdown that left a partial record.
    db.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);

    RocksOptions {
        db,
        cache,
        write_buffer_manager,
    }
}

/// Per-column-family options.
///
/// `dlog` holds document bodies and is the target of `index_scan`'s join, so it
/// gets key-value separation and a bloom filter. `docs`, `dtab` and `idx` hold
/// keys with empty or tiny values.
pub fn column_family(name: &str, shared: &RocksOptions) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(*MEMTABLE_BYTES);
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
