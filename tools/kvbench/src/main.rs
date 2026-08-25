//! Convex storage-shape benchmark.
//!
//! Compares candidate storage engines on the *exact* write and read shapes that
//! `convex-backend`'s `Persistence` / `PersistenceReader` traits demand, rather
//! than on synthetic `fillrandom` numbers. Nothing here is Convex code; it is a
//! faithful model of the physical work Convex asks its store to do.
//!
//! # Write shape
//!
//! From `IndexRegistry::index_updates` (crates/indexing/src/index_registry.rs)
//! plus the Postgres DDL (crates/postgres/src/sql.rs), one Convex document write
//! produces:
//!
//!   * one `documents` row, and
//!   * one `indexes` row per index on the table — `by_id` and `by_creation_time`
//!     always, plus every user index.
//!
//! Postgres additionally maintains two secondary indexes on `documents`
//! (`(table_id, id, ts)` and `(table_id, ts, id)`). In a KV store those are
//! explicit keyspaces, so the *physical* work is the same: with 3 indexes on the
//! table, 6 keys per document write.
//!
//! # Key encodings
//!
//! Convex does MVCC in its data model, not in the storage engine: every
//! `PersistenceReader` method takes an explicit `read_timestamp`. So the store
//! needs only ordered keys, range scans, point gets and atomic batches — no
//! engine-level MVCC. Encoding the timestamp descending inside the key turns
//! "newest version at or before `read_ts`" into a single forward seek:
//!
//!   doc  : table_id[16] ‖ id[16]  ‖ !ts[8]        -> document bytes
//!   dlog : ts[8]        ‖ table_id[16] ‖ id[16]   -> ()
//!   dtab : table_id[16] ‖ ts[8]  ‖ id[16]         -> ()
//!   idx  : index_id[16] ‖ key[..] ‖ !ts[8]        -> tag[1] ‖ table_id[16] ‖ id[16]
//!
//! where `!ts == u64::MAX - ts`, big-endian.
//!
//! # Read shapes measured
//!
//! * `index_scan` — the query in `crates/sqlite/src/lib.rs:157-177`: for each
//!   distinct key in a range, the newest version at or before `read_ts`, skipping
//!   tombstones.
//! * `previous_revisions` — `crates/postgres/src/sql.rs:930+`: given
//!   `(table_id, id, ts)`, the newest revision strictly before `ts`.
//!
//! # Usage
//!
//!   kvbench [--docs N] [--batch N] [--indexes N] [--value-bytes N]
//!           [--durable|--relaxed] [--engines a,b,c] [--dir PATH]

use std::error::Error;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type R<T> = Result<T, Box<dyn Error>>;

/// RocksDB is behind a cargo feature: it pulls in a C++ toolchain and libclang,
/// which not every environment has. `--features rocksdb` enables it.
#[cfg(feature = "rocksdb")]
const DEFAULT_ENGINES: &[&str] = &["fjall", "rocksdb", "redb", "sqlite"];
#[cfg(not(feature = "rocksdb"))]
const DEFAULT_ENGINES: &[&str] = &["fjall", "redb", "sqlite"];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    docs: usize,
    /// Documents per atomic write — one `Persistence::write` call.
    /// Convex's default `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS` is 64.
    batch: usize,
    /// Indexes on the table, including `by_id` and `by_creation_time`.
    indexes: usize,
    value_bytes: usize,
    /// fsync on every batch, versus letting the OS buffer it. The aa-app cell
    /// runs Postgres with `synchronous_commit=off`, so `relaxed` is the
    /// like-for-like comparison against production today.
    durable: bool,
    scans: usize,
    scan_limit: usize,
    point_gets: usize,
    dir: PathBuf,
    engines: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            docs: 200_000,
            batch: 64,
            indexes: 3,
            value_bytes: 400,
            durable: true,
            scans: 2_000,
            scan_limit: 64,
            point_gets: 20_000,
            dir: PathBuf::from("/tmp/kvbench"),
            engines: DEFAULT_ENGINES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Workload generation
// ---------------------------------------------------------------------------

/// Deterministic PRNG — the same document stream reaches every engine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const ID_LEN: usize = 16;
/// Devices the synthetic fleet reports from. Index keys cluster by device, the
/// way an IoT telemetry index does.
const DEVICES: u64 = 4_096;

fn id_bytes(n: u64) -> [u8; ID_LEN] {
    let mut b = [0u8; ID_LEN];
    b[..8].copy_from_slice(&n.to_be_bytes());
    b[8..].copy_from_slice(&(n.wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_be_bytes());
    b
}

fn desc_ts(ts: u64) -> [u8; 8] {
    (u64::MAX - ts).to_be_bytes()
}

/// One document write, expanded into the physical keys it produces.
struct DocWrite {
    table_id: [u8; ID_LEN],
    id: [u8; ID_LEN],
    ts: u64,
    device: u64,
    value: Vec<u8>,
}

const TABLE_ID: [u8; ID_LEN] = [0x11; ID_LEN];

fn index_id(i: usize) -> [u8; ID_LEN] {
    let mut b = [0xAAu8; ID_LEN];
    b[15] = i as u8;
    b
}

impl DocWrite {
    fn doc_key(&self) -> Vec<u8> {
        let mut k = Vec::with_capacity(40);
        k.extend_from_slice(&self.table_id);
        k.extend_from_slice(&self.id);
        k.extend_from_slice(&desc_ts(self.ts));
        k
    }

    fn dlog_key(&self) -> Vec<u8> {
        let mut k = Vec::with_capacity(40);
        k.extend_from_slice(&self.ts.to_be_bytes());
        k.extend_from_slice(&self.table_id);
        k.extend_from_slice(&self.id);
        k
    }

    fn dtab_key(&self) -> Vec<u8> {
        let mut k = Vec::with_capacity(40);
        k.extend_from_slice(&self.table_id);
        k.extend_from_slice(&self.ts.to_be_bytes());
        k.extend_from_slice(&self.id);
        k
    }

    /// Index 0 is `by_id` (key = the document id). Index 1 is
    /// `by_creation_time` (key = an f64 creation time, then the id). Indexes 2+
    /// are user indexes on `(device, ts)` — the shape an IoT telemetry index has.
    fn index_key(&self, i: usize) -> Vec<u8> {
        let mut k = Vec::with_capacity(64);
        k.extend_from_slice(&index_id(i));
        match i {
            0 => k.extend_from_slice(&self.id),
            1 => {
                k.extend_from_slice(&(self.ts as f64).to_be_bytes());
                k.extend_from_slice(&self.id);
            },
            _ => {
                k.extend_from_slice(&id_bytes(self.device));
                k.extend_from_slice(&self.ts.to_be_bytes());
                k.extend_from_slice(&self.id);
            },
        }
        k.extend_from_slice(&desc_ts(self.ts));
        k
    }

    /// The `indexes` row value: a liveness tag plus the document pointer.
    fn index_value(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + 2 * ID_LEN);
        v.push(0); // 0 = live, 1 = tombstone
        v.extend_from_slice(&self.table_id);
        v.extend_from_slice(&self.id);
        v
    }

    /// The `index_id ‖ key` prefix, without the trailing descending timestamp —
    /// what a scan compares to detect a new distinct key.
    fn index_key_prefix_len(&self, i: usize) -> usize {
        self.index_key(i).len() - 8
    }
}

fn generate(cfg: &Config) -> Vec<DocWrite> {
    let mut rng = Rng(0x5EED_1234_ABCD_9876);
    let mut out = Vec::with_capacity(cfg.docs);
    for n in 0..cfg.docs {
        let device = rng.below(DEVICES);
        // Convex timestamps are monotonic per commit; every document in a batch
        // shares its commit timestamp.
        let ts = 1_700_000_000_000_000u64 + (n / cfg.batch) as u64 * 1_000;
        let mut value = vec![0u8; cfg.value_bytes];
        for (j, b) in value.iter_mut().enumerate() {
            *b = ((rng.next() >> (j % 8 * 8)) & 0xFF) as u8;
        }
        out.push(DocWrite {
            table_id: TABLE_ID,
            id: id_bytes(n as u64 + 1),
            ts,
            device,
            value,
        });
    }
    out
}

// ---------------------------------------------------------------------------
// Engine abstraction
// ---------------------------------------------------------------------------

trait Engine {
    fn name(&self) -> &'static str;
    /// One `Persistence::write` call: atomic across all keyspaces, durable when
    /// the config says so.
    fn write(&mut self, batch: &[DocWrite], cfg: &Config) -> R<()>;
    /// `index_scan`: for each distinct key in `[lo, hi)`, the newest version at
    /// or before `read_ts`, skipping tombstones. Returns rows yielded.
    fn index_scan(&self, idx: usize, lo: &[u8], hi: &[u8], read_ts: u64, limit: usize) -> R<usize>;
    /// `previous_revisions`: the newest revision of `(table_id, id)` strictly
    /// before `before_ts`.
    fn prev_revision(&self, id: &[u8; ID_LEN], before_ts: u64) -> R<Option<usize>>;
    fn flush(&mut self) -> R<()>;
    fn disk_bytes(&self) -> u64;
}

fn dir_size(p: &Path) -> u64 {
    let mut total = 0;
    let Ok(rd) = std::fs::read_dir(p) else { return 0 };
    for e in rd.flatten() {
        let Ok(m) = e.metadata() else { continue };
        total += if m.is_dir() { dir_size(&e.path()) } else { m.len() };
    }
    total
}

fn fresh(dir: &Path, name: &str) -> R<PathBuf> {
    let p = dir.join(name);
    let _ = std::fs::remove_dir_all(&p);
    let _ = std::fs::remove_file(&p);
    std::fs::create_dir_all(dir)?;
    Ok(p)
}

// ---------------------------------------------------------------------------
// fjall — LSM, MIT
// ---------------------------------------------------------------------------

struct Fjall {
    db: fjall::Database,
    doc: fjall::Keyspace,
    dlog: fjall::Keyspace,
    dtab: fjall::Keyspace,
    idx: fjall::Keyspace,
    path: PathBuf,
}

impl Fjall {
    fn open(dir: &Path) -> R<Self> {
        use fjall::KeyspaceCreateOptions;
        let path = fresh(dir, "fjall")?;
        let db = fjall::Database::builder(&path).open()?;
        let doc = db.keyspace("doc", KeyspaceCreateOptions::default)?;
        let dlog = db.keyspace("dlog", KeyspaceCreateOptions::default)?;
        let dtab = db.keyspace("dtab", KeyspaceCreateOptions::default)?;
        let idx = db.keyspace("idx", KeyspaceCreateOptions::default)?;
        Ok(Self { db, doc, dlog, dtab, idx, path })
    }
}

impl Engine for Fjall {
    fn name(&self) -> &'static str {
        "fjall"
    }

    fn write(&mut self, batch: &[DocWrite], cfg: &Config) -> R<()> {
        let mode = if cfg.durable {
            Some(fjall::PersistMode::SyncAll)
        } else {
            Some(fjall::PersistMode::Buffer)
        };
        let mut wb = self.db.batch().durability(mode);
        for d in batch {
            wb.insert(&self.doc, d.doc_key(), d.value.clone());
            wb.insert(&self.dlog, d.dlog_key(), &[][..]);
            wb.insert(&self.dtab, d.dtab_key(), &[][..]);
            for i in 0..cfg.indexes {
                wb.insert(&self.idx, d.index_key(i), d.index_value());
            }
        }
        wb.commit()?;
        Ok(())
    }

    fn index_scan(&self, idx: usize, lo: &[u8], hi: &[u8], read_ts: u64, limit: usize) -> R<usize> {
        let mut yielded = 0;
        let mut last_prefix: Option<Vec<u8>> = None;
        let _ = idx;
        for guard in self.idx.range(lo.to_vec()..hi.to_vec()) {
            let (k, v) = guard.into_inner()?;
            if k.len() < 8 {
                continue;
            }
            let prefix = &k[..k.len() - 8];
            // Versions of one key run newest-first, so a forward walk meets a
            // key's newest version first. Skip versions newer than `read_ts`,
            // take the first one at or before it, then skip the rest of that
            // key's run — one entry read per key.
            if last_prefix.as_deref() == Some(prefix) {
                continue;
            }
            let ts = u64::MAX - u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
            if ts > read_ts {
                continue; // too new — keep walking this key's versions
            }
            last_prefix = Some(prefix.to_vec());
            if v.first() == Some(&0) {
                yielded += 1;
                if yielded >= limit {
                    break;
                }
            }
        }
        Ok(yielded)
    }

    fn prev_revision(&self, id: &[u8; ID_LEN], before_ts: u64) -> R<Option<usize>> {
        let mut seek = Vec::with_capacity(40);
        seek.extend_from_slice(&TABLE_ID);
        seek.extend_from_slice(id);
        let prefix_len = seek.len();
        seek.extend_from_slice(&desc_ts(before_ts.saturating_sub(1)));
        let mut end = seek[..prefix_len].to_vec();
        end.extend_from_slice(&[0xFFu8; 8]);
        for guard in self.doc.range(seek..=end) {
            let (_, v) = guard.into_inner()?;
            return Ok(Some(v.len()));
        }
        Ok(None)
    }

    fn flush(&mut self) -> R<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    fn disk_bytes(&self) -> u64 {
        dir_size(&self.path)
    }
}

// ---------------------------------------------------------------------------
// RocksDB — LSM, Apache-2.0 / GPLv2
// ---------------------------------------------------------------------------

#[cfg(feature = "rocksdb")]
struct Rocks {
    db: rocksdb::DB,
    path: PathBuf,
}

#[cfg(feature = "rocksdb")]
impl Rocks {
    fn open(dir: &Path) -> R<Self> {
        use rocksdb::{ColumnFamilyDescriptor, Options};
        let path = fresh(dir, "rocksdb")?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.increase_parallelism(4);
        let cf = |n: &str| ColumnFamilyDescriptor::new(n, Options::default());
        let db = rocksdb::DB::open_cf_descriptors(
            &opts,
            &path,
            vec![cf("doc"), cf("dlog"), cf("dtab"), cf("idx")],
        )?;
        Ok(Self { db, path })
    }
}

#[cfg(feature = "rocksdb")]
impl Engine for Rocks {
    fn name(&self) -> &'static str {
        "rocksdb"
    }

    fn write(&mut self, batch: &[DocWrite], cfg: &Config) -> R<()> {
        let doc = self.db.cf_handle("doc").ok_or("cf doc")?;
        let dlog = self.db.cf_handle("dlog").ok_or("cf dlog")?;
        let dtab = self.db.cf_handle("dtab").ok_or("cf dtab")?;
        let idx = self.db.cf_handle("idx").ok_or("cf idx")?;
        let mut wb = rocksdb::WriteBatch::default();
        for d in batch {
            wb.put_cf(&doc, d.doc_key(), &d.value);
            wb.put_cf(&dlog, d.dlog_key(), []);
            wb.put_cf(&dtab, d.dtab_key(), []);
            for i in 0..cfg.indexes {
                wb.put_cf(&idx, d.index_key(i), d.index_value());
            }
        }
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(cfg.durable);
        self.db.write_opt(wb, &wo)?;
        Ok(())
    }

    fn index_scan(&self, _idx: usize, lo: &[u8], hi: &[u8], read_ts: u64, limit: usize) -> R<usize> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};
        let cf = self.db.cf_handle("idx").ok_or("cf idx")?;
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(hi.to_vec());
        let it = self
            .db
            .iterator_cf_opt(&cf, ro, IteratorMode::From(lo, Direction::Forward));
        let mut yielded = 0;
        let mut last_prefix: Option<Vec<u8>> = None;
        for kv in it {
            let (k, v) = kv?;
            if k.len() < 8 {
                continue;
            }
            let prefix = &k[..k.len() - 8];
            if last_prefix.as_deref() == Some(prefix) {
                continue;
            }
            let ts = u64::MAX - u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
            if ts > read_ts {
                continue;
            }
            last_prefix = Some(prefix.to_vec());
            if v.first() == Some(&0) {
                yielded += 1;
                if yielded >= limit {
                    break;
                }
            }
        }
        Ok(yielded)
    }

    fn prev_revision(&self, id: &[u8; ID_LEN], before_ts: u64) -> R<Option<usize>> {
        use rocksdb::{Direction, IteratorMode};
        let cf = self.db.cf_handle("doc").ok_or("cf doc")?;
        let mut seek = Vec::with_capacity(40);
        seek.extend_from_slice(&TABLE_ID);
        seek.extend_from_slice(id);
        let prefix_len = seek.len();
        seek.extend_from_slice(&desc_ts(before_ts.saturating_sub(1)));
        let mut it = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&seek, Direction::Forward));
        if let Some(kv) = it.next() {
            let (k, v) = kv?;
            if k.len() >= prefix_len && &k[..prefix_len] == &seek[..prefix_len] {
                return Ok(Some(v.len()));
            }
        }
        Ok(None)
    }

    fn flush(&mut self) -> R<()> {
        self.db.flush()?;
        Ok(())
    }

    fn disk_bytes(&self) -> u64 {
        dir_size(&self.path)
    }
}

// ---------------------------------------------------------------------------
// redb — copy-on-write B-tree, MIT/Apache-2.0
// ---------------------------------------------------------------------------

const T_DOC: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("doc");
const T_DLOG: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("dlog");
const T_DTAB: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("dtab");
const T_IDX: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("idx");

struct Redb {
    db: redb::Database,
    path: PathBuf,
}

impl Redb {
    fn open(dir: &Path) -> R<Self> {
        let path = fresh(dir, "redb.db")?;
        let db = redb::Database::create(&path)?;
        let tx = db.begin_write()?;
        {
            tx.open_table(T_DOC)?;
            tx.open_table(T_DLOG)?;
            tx.open_table(T_DTAB)?;
            tx.open_table(T_IDX)?;
        }
        tx.commit()?;
        Ok(Self { db, path })
    }
}

impl Engine for Redb {
    fn name(&self) -> &'static str {
        "redb"
    }

    fn write(&mut self, batch: &[DocWrite], cfg: &Config) -> R<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(if cfg.durable {
            redb::Durability::Immediate
        } else {
            redb::Durability::Eventual
        });
        {
            let mut doc = tx.open_table(T_DOC)?;
            let mut dlog = tx.open_table(T_DLOG)?;
            let mut dtab = tx.open_table(T_DTAB)?;
            let mut idx = tx.open_table(T_IDX)?;
            for d in batch {
                doc.insert(d.doc_key().as_slice(), d.value.as_slice())?;
                dlog.insert(d.dlog_key().as_slice(), [].as_slice())?;
                dtab.insert(d.dtab_key().as_slice(), [].as_slice())?;
                for i in 0..cfg.indexes {
                    idx.insert(d.index_key(i).as_slice(), d.index_value().as_slice())?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn index_scan(&self, _idx: usize, lo: &[u8], hi: &[u8], read_ts: u64, limit: usize) -> R<usize> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(T_IDX)?;
        let mut yielded = 0;
        let mut last_prefix: Option<Vec<u8>> = None;
        for kv in t.range(lo..hi)? {
            let (k, v) = kv?;
            let k = k.value();
            if k.len() < 8 {
                continue;
            }
            let prefix = &k[..k.len() - 8];
            if last_prefix.as_deref() == Some(prefix) {
                continue;
            }
            let ts = u64::MAX - u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
            if ts > read_ts {
                continue;
            }
            last_prefix = Some(prefix.to_vec());
            if v.value().first() == Some(&0) {
                yielded += 1;
                if yielded >= limit {
                    break;
                }
            }
        }
        Ok(yielded)
    }

    fn prev_revision(&self, id: &[u8; ID_LEN], before_ts: u64) -> R<Option<usize>> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(T_DOC)?;
        let mut seek = Vec::with_capacity(40);
        seek.extend_from_slice(&TABLE_ID);
        seek.extend_from_slice(id);
        let prefix_len = seek.len();
        seek.extend_from_slice(&desc_ts(before_ts.saturating_sub(1)));
        let mut end = seek[..prefix_len].to_vec();
        end.extend_from_slice(&[0xFFu8; 8]);
        for kv in t.range(seek.as_slice()..=end.as_slice())? {
            let (_, v) = kv?;
            return Ok(Some(v.value().len()));
        }
        Ok(None)
    }

    fn flush(&mut self) -> R<()> {
        Ok(())
    }

    fn disk_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// SQLite — the relational baseline, using Convex's own schema
//
// Schema copied from crates/sqlite/src/lib.rs (DOCUMENTS_INIT / INDEXES_INIT):
// the same primary keys and secondary indexes Convex asks a relational engine
// for. This is the control: same process, same disk, no network — so any
// difference against the KV engines is the storage model alone.
// ---------------------------------------------------------------------------

struct Sqlite {
    conn: rusqlite::Connection,
    path: PathBuf,
}

impl Sqlite {
    fn open(dir: &Path, durable: bool) -> R<Self> {
        let path = fresh(dir, "convex.sqlite")?;
        let conn = rusqlite::Connection::open(&path)?;
        conn.execute_batch(&format!(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous={};
             CREATE TABLE documents (
                id BLOB NOT NULL, ts INTEGER NOT NULL, table_id BLOB NOT NULL,
                json_value BLOB NULL, deleted INTEGER NOT NULL, prev_ts INTEGER,
                PRIMARY KEY (ts, table_id, id));
             CREATE INDEX documents_by_table_and_id ON documents (table_id, id, ts);
             CREATE INDEX documents_by_table_ts_and_id ON documents (table_id, ts, id);
             CREATE TABLE indexes (
                index_id BLOB NOT NULL, ts INTEGER NOT NULL, key BLOB NOT NULL,
                deleted INTEGER NOT NULL, table_id BLOB NULL, document_id BLOB NULL,
                PRIMARY KEY (index_id, key, ts));",
            if durable { "FULL" } else { "OFF" }
        ))?;
        Ok(Self { conn, path })
    }
}

impl Engine for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn write(&mut self, batch: &[DocWrite], cfg: &Config) -> R<()> {
        let tx = self.conn.transaction()?;
        {
            let mut doc = tx.prepare_cached(
                "INSERT INTO documents (id, ts, table_id, json_value, deleted, prev_ts) \
                 VALUES (?, ?, ?, ?, 0, NULL)",
            )?;
            let mut idx = tx.prepare_cached(
                "INSERT INTO indexes (index_id, ts, key, deleted, table_id, document_id) \
                 VALUES (?, ?, ?, 0, ?, ?)",
            )?;
            for d in batch {
                doc.execute(rusqlite::params![
                    &d.id[..],
                    d.ts as i64,
                    &d.table_id[..],
                    &d.value[..]
                ])?;
                for i in 0..cfg.indexes {
                    // The `indexes` row carries the index key without the
                    // trailing descending timestamp — `ts` is its own column
                    // here, exactly as in Convex's relational schema.
                    let full = d.index_key(i);
                    let key = &full[ID_LEN..d.index_key_prefix_len(i)];
                    idx.execute(rusqlite::params![
                        &index_id(i)[..],
                        d.ts as i64,
                        key,
                        &d.table_id[..],
                        &d.id[..]
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn index_scan(&self, idx: usize, lo: &[u8], hi: &[u8], read_ts: u64, limit: usize) -> R<usize> {
        // The query from crates/sqlite/src/lib.rs:157-177, verbatim in shape.
        let mut stmt = self.conn.prepare_cached(
            "SELECT B.key FROM (
                SELECT index_id, key, MAX(ts) as max_ts FROM indexes
                WHERE index_id = ?1 AND ts <= ?2 AND key >= ?3 AND key < ?4
                GROUP BY index_id, key
             ) A
             JOIN indexes B ON B.deleted = 0 AND A.index_id = B.index_id
                AND A.key = B.key AND A.max_ts = B.ts
             LEFT JOIN documents C ON B.ts = C.ts AND B.table_id = C.table_id
                AND B.document_id = C.id
             ORDER BY B.key ASC LIMIT ?5",
        )?;
        let n = stmt
            .query_map(
                rusqlite::params![
                    &index_id(idx)[..],
                    read_ts as i64,
                    &lo[ID_LEN..],
                    &hi[ID_LEN..],
                    limit as i64
                ],
                |_| Ok(()),
            )?
            .count();
        Ok(n)
    }

    fn prev_revision(&self, id: &[u8; ID_LEN], before_ts: u64) -> R<Option<usize>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT json_value FROM documents WHERE table_id = ?1 AND id = ?2 AND ts < ?3 \
             ORDER BY ts DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(rusqlite::params![&TABLE_ID[..], &id[..], before_ts as i64])?;
        match rows.next()? {
            Some(r) => {
                let v: Vec<u8> = r.get(0)?;
                Ok(Some(v.len()))
            },
            None => Ok(None),
        }
    }

    fn flush(&mut self) -> R<()> {
        self.conn.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        Ok(())
    }

    fn disk_bytes(&self) -> u64 {
        let mut n = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        for suffix in ["-wal", "-shm"] {
            let p = self.path.with_extension(format!("sqlite{suffix}"));
            n += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
        n
    }
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

struct Latencies(Vec<u64>);

impl Latencies {
    fn pct(&mut self, p: f64) -> Duration {
        if self.0.is_empty() {
            return Duration::ZERO;
        }
        self.0.sort_unstable();
        let i = (((self.0.len() - 1) as f64) * p).round() as usize;
        Duration::from_nanos(self.0[i])
    }
}

struct Report {
    engine: &'static str,
    write_docs_per_s: f64,
    write_rows_per_s: f64,
    batch_p50: Duration,
    batch_p99: Duration,
    scans_per_s: f64,
    scan_p99: Duration,
    /// Total rows the scan phase yielded. Every engine runs the identical
    /// sequence of scans, so these must agree — if they do not, the engines are
    /// not answering the same question and the timings are meaningless.
    scan_rows: usize,
    point_gets_per_s: f64,
    disk_mb: f64,
}

fn run(mut eng: Box<dyn Engine>, docs: &[DocWrite], cfg: &Config) -> R<Report> {
    let rows_per_doc = (3 + cfg.indexes) as f64;

    // --- write ---------------------------------------------------------
    let mut lat = Latencies(Vec::with_capacity(docs.len() / cfg.batch + 1));
    let t0 = Instant::now();
    for chunk in docs.chunks(cfg.batch) {
        let t = Instant::now();
        eng.write(chunk, cfg)?;
        lat.0.push(t.elapsed().as_nanos() as u64);
    }
    let write_elapsed = t0.elapsed().as_secs_f64();
    eng.flush()?;

    // --- index_scan ----------------------------------------------------
    // Scan a single device's slice of the user index — the shape a
    // `withIndex(q => q.eq('deviceId', d))` query produces.
    let mut rng = Rng(0xC0FF_EE00_1234_5678);
    let read_ts = docs.last().map(|d| d.ts).unwrap_or(0);
    let mut scan_lat = Latencies(Vec::with_capacity(cfg.scans));
    let mut scan_rows = 0usize;
    let t0 = Instant::now();
    for _ in 0..cfg.scans {
        let device = rng.below(DEVICES);
        let mut lo = Vec::with_capacity(40);
        lo.extend_from_slice(&index_id(2));
        lo.extend_from_slice(&id_bytes(device));
        let mut hi = lo.clone();
        hi.extend_from_slice(&[0xFFu8; 24]);
        let t = Instant::now();
        scan_rows += eng.index_scan(2, &lo, &hi, read_ts, cfg.scan_limit)?;
        scan_lat.0.push(t.elapsed().as_nanos() as u64);
    }
    let scan_elapsed = t0.elapsed().as_secs_f64();

    // --- previous_revisions --------------------------------------------
    let t0 = Instant::now();
    for _ in 0..cfg.point_gets {
        let n = rng.below(docs.len() as u64) + 1;
        eng.prev_revision(&id_bytes(n), read_ts)?;
    }
    let point_elapsed = t0.elapsed().as_secs_f64();

    Ok(Report {
        engine: eng.name(),
        write_docs_per_s: docs.len() as f64 / write_elapsed,
        write_rows_per_s: docs.len() as f64 * rows_per_doc / write_elapsed,
        batch_p50: lat.pct(0.50),
        batch_p99: lat.pct(0.99),
        scans_per_s: cfg.scans as f64 / scan_elapsed,
        scan_p99: scan_lat.pct(0.99),
        scan_rows,
        point_gets_per_s: cfg.point_gets as f64 / point_elapsed,
        disk_mb: eng.disk_bytes() as f64 / (1024.0 * 1024.0),
    })
}

fn ms(d: Duration) -> String {
    format!("{:.2}", d.as_secs_f64() * 1000.0)
}

fn main() -> R<()> {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        let mut val = || -> String {
            i += 1;
            args.get(i).cloned().unwrap_or_default()
        };
        match a {
            "--docs" => cfg.docs = val().parse()?,
            "--batch" => cfg.batch = val().parse()?,
            "--indexes" => cfg.indexes = val().parse()?,
            "--value-bytes" => cfg.value_bytes = val().parse()?,
            "--scans" => cfg.scans = val().parse()?,
            "--point-gets" => cfg.point_gets = val().parse()?,
            "--dir" => cfg.dir = PathBuf::from(val()),
            "--engines" => cfg.engines = val().split(',').map(|s| s.trim().to_string()).collect(),
            "--durable" => cfg.durable = true,
            "--relaxed" => cfg.durable = false,
            "--help" | "-h" => {
                println!("{}", HELP);
                return Ok(());
            },
            other => return Err(format!("unknown flag {other}").into()),
        }
        i += 1;
    }

    println!(
        "convex storage-shape benchmark\n\
         docs={} batch={} indexes={} value={}B rows/doc={} durability={}\n\
         dir={}\n",
        cfg.docs,
        cfg.batch,
        cfg.indexes,
        cfg.value_bytes,
        3 + cfg.indexes,
        if cfg.durable { "fsync per batch" } else { "no fsync (OS buffer)" },
        cfg.dir.display(),
    );

    let docs = generate(&cfg);
    let mut reports = Vec::new();

    for name in &cfg.engines {
        let eng: Box<dyn Engine> = match name.as_str() {
            "fjall" => Box::new(Fjall::open(&cfg.dir)?),
            #[cfg(feature = "rocksdb")]
            "rocksdb" => Box::new(Rocks::open(&cfg.dir)?),
            #[cfg(not(feature = "rocksdb"))]
            "rocksdb" => return Err("rocksdb engine not compiled in; rebuild with --features rocksdb".into()),
            "redb" => Box::new(Redb::open(&cfg.dir)?),
            "sqlite" => Box::new(Sqlite::open(&cfg.dir, cfg.durable)?),
            other => return Err(format!("unknown engine {other}").into()),
        };
        eprint!("running {name} ... ");
        let t = Instant::now();
        let r = run(eng, &docs, &cfg)?;
        eprintln!("{:.1}s", t.elapsed().as_secs_f64());
        reports.push(r);
    }

    // Every engine ran the identical scan sequence. If the row counts differ,
    // they are not answering the same question and no timing below means
    // anything — say so loudly rather than printing a pretty table.
    let agree = reports
        .iter()
        .all(|r| r.scan_rows == reports[0].scan_rows);

    let mut out = String::new();
    writeln!(
        out,
        "{:<9} {:>11} {:>11} {:>9} {:>9} {:>10} {:>9} {:>11} {:>9} {:>10}",
        "engine",
        "docs/s",
        "rows/s",
        "p50 ms",
        "p99 ms",
        "scans/s",
        "p99 ms",
        "prevrev/s",
        "disk MB",
        "scan rows"
    )?;
    writeln!(out, "{}", "-".repeat(107))?;
    for r in &reports {
        writeln!(
            out,
            "{:<9} {:>11.0} {:>11.0} {:>9} {:>9} {:>10.0} {:>9} {:>11.0} {:>9.1} {:>10}",
            r.engine,
            r.write_docs_per_s,
            r.write_rows_per_s,
            ms(r.batch_p50),
            ms(r.batch_p99),
            r.scans_per_s,
            ms(r.scan_p99),
            r.point_gets_per_s,
            r.disk_mb,
            r.scan_rows,
        )?;
    }
    println!("\n{out}");
    if !agree {
        return Err("engines disagree on scan row counts — the scan timings above \
                    compare different work and must not be used"
            .into());
    }
    if reports.iter().any(|r| r.scan_rows == 0) {
        return Err("scan phase yielded no rows — the scan range or seek is wrong".into());
    }
    Ok(())
}

const HELP: &str = "\
kvbench — compare storage engines on Convex's write and read shapes

  --docs N          documents to write            (default 200000)
  --batch N         documents per atomic write    (default 64, = COMMITTER_MAX_WRITE_BATCH_DOCUMENTS)
  --indexes N       indexes on the table          (default 3, = by_id + by_creation_time + one user index)
  --value-bytes N   document size                 (default 400)
  --scans N         index_scan iterations         (default 2000)
  --point-gets N    previous_revisions iterations (default 20000)
  --durable         fsync on every batch          (default)
  --relaxed         no fsync — what Postgres with synchronous_commit=off is doing
  --engines a,b,c   subset of fjall,rocksdb,redb,sqlite
  --dir PATH        scratch directory             (default /tmp/kvbench)
";
