//! Adversarial differential tests: a brute-force model of the `Persistence`
//! contract, run against the RocksDB backend on randomised workloads.

#![allow(clippy::needless_range_loop)]

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
};

use common::{
    document::{
        CreationTime,
        ResolvedDocument,
    },
    index::{
        IndexEntry,
        IndexKeyBytes,
    },
    interval::{
        End,
        Interval,
        StartIncluded,
    },
    obj,
    persistence::{
        ConflictStrategy,
        DocumentLogEntry,
        DocumentPrevTsQuery,
        NoopRetentionValidator,
        Persistence,
        PersistenceIndexEntry,
        RetentionValidator,
        TimestampRange,
    },
    query::Order,
    types::{
        IndexId,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        ResolvedDocumentId,
        TabletId,
    },
};
use futures::TryStreamExt;
use value::{
    DeveloperDocumentId,
    InternalId,
    TableNumber,
};

use crate::{
    keys,
    RocksDbPersistence,
};

const TABLE_NUMBER: u32 = 7;
const TABLET: u8 = 3;

fn tablet() -> TabletId {
    TabletId(InternalId([TABLET; keys::ID_LEN]))
}

fn internal_id(n: u32) -> InternalId {
    let mut bytes = [0u8; keys::ID_LEN];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    InternalId(bytes)
}

fn doc_id(n: u32) -> InternalDocumentId {
    InternalDocumentId::new(tablet(), internal_id(n))
}

fn index_id(n: u8) -> IndexId {
    IndexId(InternalId([0xA0 | n; keys::ID_LEN]))
}

fn ts(n: u64) -> Timestamp {
    Timestamp::try_from(n).unwrap()
}

fn document(n: u32, body: &str) -> anyhow::Result<ResolvedDocument> {
    let id = ResolvedDocumentId {
        tablet_id: tablet(),
        developer_id: DeveloperDocumentId::new(
            TableNumber::try_from(TABLE_NUMBER)?,
            internal_id(n),
        ),
    };
    Ok(ResolvedDocument::new(
        id,
        CreationTime::try_from(1.0)?,
        obj!("body" => body)?,
    )?)
}

fn body_of(doc: &ResolvedDocument) -> String {
    let raw = doc
        .value()
        .get::<str>("body")
        .expect("document has no body field")
        .to_string();
    raw.trim_matches('"').to_string()
}

fn validator() -> Arc<dyn RetentionValidator> {
    Arc::new(NoopRetentionValidator)
}

/// Deterministic xorshift so a failure is reproducible from the seed alone.
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

    fn below(&mut self, n: usize) -> usize {
        (self.next() % (n as u64)) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

/// Index keys chosen to break naive encodings: empty, zero bytes, keys that are
/// proper prefixes of others, saturated bytes, and keys at and past
/// `MAX_INDEX_KEY_PREFIX_LEN` that share a truncated prefix.
fn key_pool() -> Vec<Vec<u8>> {
    let long = vec![0x41u8; common::index::MAX_INDEX_KEY_PREFIX_LEN];
    let mut pool: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 0, 0],
        vec![0, 0xFF],
        vec![0, 0xFF, 0],
        vec![1],
        vec![1, 0],
        vec![1, 0, 0],
        vec![1, 0, 0xFF],
        vec![1, 2],
        vec![1, 2, 3],
        vec![2],
        vec![0xFE, 0xFF],
        vec![0xFF],
        vec![0xFF, 0],
        vec![0xFF, 0xFF],
        vec![0xFF; 9],
    ];
    // Keys that share the 2500-byte truncated prefix: `IndexEntry` orders these
    // by sha256, storage orders them by bytes, and `index_scan` must still
    // return them in full-key order.
    pool.push(long.clone());
    for suffix in [
        vec![0u8],
        vec![0u8, 0u8],
        vec![0u8, 0xFFu8],
        vec![1u8],
        vec![0xFFu8],
        vec![0x41u8; 40],
    ] {
        let mut k = long.clone();
        k.extend_from_slice(&suffix);
        pool.push(k);
    }
    pool.sort();
    pool.dedup();
    pool
}

/// Everything written so far, as a plain list. Queries are answered by brute
/// force over it.
#[derive(Default)]
struct Model {
    /// (ts, id) -> (value, prev_ts)
    documents: BTreeMap<(Timestamp, InternalDocumentId), (Option<String>, Option<Timestamp>)>,
    /// (index_id, key, ts) -> value
    indexes: BTreeMap<(IndexId, Vec<u8>, Timestamp), Option<InternalDocumentId>>,
}

impl Model {
    fn document_log(&self, range: TimestampRange, order: Order) -> Vec<(u64, u32)> {
        let mut rows: Vec<_> = self
            .documents
            .keys()
            .filter(|(t, _)| range.contains(*t))
            .map(|(t, id)| {
                (
                    u64::from(*t),
                    u32::from_be_bytes(id.internal_id()[..4].try_into().unwrap()),
                )
            })
            .collect();
        rows.sort();
        if order == Order::Desc {
            rows.reverse();
        }
        rows
    }

    fn index_scan(
        &self,
        index: IndexId,
        read_ts: Timestamp,
        interval: &Interval,
        order: Order,
    ) -> Vec<(Vec<u8>, u64, String)> {
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        for (idx, key, _) in self.indexes.keys() {
            if *idx == index && interval.contains(key) {
                keys.insert(key.clone());
            }
        }
        let mut out = Vec::new();
        for key in keys {
            let newest = self
                .indexes
                .iter()
                .filter(|((idx, k, t), _)| *idx == index && k == &key && *t <= read_ts)
                .max_by_key(|((_, _, t), _)| *t);
            let Some(((_, _, t), value)) = newest else {
                continue;
            };
            let Some(id) = value else { continue };
            let (body, _) = self
                .documents
                .get(&(*t, *id))
                .expect("index entry points at a missing document");
            let body = body.clone().expect("index entry points at a tombstone");
            out.push((key, u64::from(*t), body));
        }
        out.sort();
        if order == Order::Desc {
            out.reverse();
        }
        out
    }

    fn index_entries(&self) -> Vec<IndexEntry> {
        let mut out: Vec<IndexEntry> = self
            .indexes
            .iter()
            .map(|((idx, key, t), value)| {
                IndexEntry::from_index_key(IndexKeyBytes(key.clone()), *idx, *t, value.is_none())
            })
            .collect();
        out.sort();
        out
    }

    fn previous_revision(
        &self,
        id: InternalDocumentId,
        at: Timestamp,
    ) -> Option<(Timestamp, Option<String>, Option<Timestamp>)> {
        self.documents
            .iter()
            .filter(|((t, i), _)| *i == id && *t < at)
            .max_by_key(|((t, _), _)| *t)
            .map(|((t, _), (body, prev))| (*t, body.clone(), *prev))
    }
}

/// A randomised history: several timestamps, at each a random set of documents
/// created, updated or deleted, with the matching index entries.
struct Workload {
    model: Model,
    max_ts: u64,
    keys: Vec<Vec<u8>>,
}

async fn build(
    persistence: &RocksDbPersistence,
    seed: u64,
    commits: usize,
) -> anyhow::Result<Workload> {
    let mut rng = Rng(seed | 1);
    let keys = key_pool();
    let mut model = Model::default();
    // Document i owns key i, in index 1. Index 2 is a decoy that must never
    // leak into index 1's scans.
    let mut last_ts: BTreeMap<u32, Timestamp> = BTreeMap::new();
    let mut live: BTreeSet<u32> = BTreeSet::new();
    let mut now = 1u64;
    for _ in 0..commits {
        let mut documents = Vec::new();
        let mut indexes = Vec::new();
        let touched = 1 + rng.below(keys.len().min(6));
        let mut chosen = BTreeSet::new();
        for _ in 0..touched {
            chosen.insert(rng.below(keys.len()) as u32);
        }
        for n in chosen {
            let t = ts(now);
            let prev = last_ts.get(&n).copied();
            let deleting = live.contains(&n) && rng.chance(35);
            if deleting {
                documents.push(DocumentLogEntry {
                    ts: t,
                    id: doc_id(n),
                    value: None,
                    prev_ts: prev,
                });
                indexes.push(PersistenceIndexEntry {
                    ts: t,
                    index_id: index_id(1),
                    key: IndexKeyBytes(keys[n as usize].clone()),
                    value: None,
                });
                model.documents.insert((t, doc_id(n)), (None, prev));
                model
                    .indexes
                    .insert((index_id(1), keys[n as usize].clone(), t), None);
                live.remove(&n);
            } else {
                let body = format!("d{n}@{now}");
                documents.push(DocumentLogEntry {
                    ts: t,
                    id: doc_id(n),
                    value: Some(document(n, &body)?),
                    prev_ts: prev,
                });
                indexes.push(PersistenceIndexEntry {
                    ts: t,
                    index_id: index_id(1),
                    key: IndexKeyBytes(keys[n as usize].clone()),
                    value: Some(doc_id(n)),
                });
                model.documents.insert((t, doc_id(n)), (Some(body), prev));
                model
                    .indexes
                    .insert((index_id(1), keys[n as usize].clone(), t), Some(doc_id(n)));
                live.insert(n);
            }
            last_ts.insert(n, t);
        }
        // A decoy entry in another index at the same key bytes, so a scan that
        // ran past its index's bounds would pick it up.
        if rng.chance(40) {
            let n = rng.below(keys.len());
            let t = ts(now);
            indexes.push(PersistenceIndexEntry {
                ts: t,
                index_id: index_id(2),
                key: IndexKeyBytes(keys[n].clone()),
                value: None,
            });
            model
                .indexes
                .insert((index_id(2), keys[n].clone(), t), None);
        }
        persistence
            .write(&documents, &indexes, ConflictStrategy::Error)
            .await?;
        now += 1 + (rng.below(3) as u64);
    }
    Ok(Workload {
        model,
        max_ts: now,
        keys,
    })
}

fn interval_of(start: &[u8], end: Option<&[u8]>) -> Interval {
    Interval {
        start: StartIncluded(start.to_vec().into()),
        end: match end {
            Some(end) => End::Excluded(end.to_vec().into()),
            None => End::Unbounded,
        },
    }
}

struct Fixture {
    persistence: RocksDbPersistence,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
        Ok(Self {
            persistence,
            _dir: dir,
        })
    }
}

// ---------------------------------------------------------------------------
// index_scan
// ---------------------------------------------------------------------------

/// Every distinct key, every read timestamp, both orders, several page sizes,
/// and intervals drawn from the key pool's own boundaries — against a
/// brute-force model. Index keys here include ones that are proper prefixes of
/// others, keys containing `0x00`, and keys past `MAX_INDEX_KEY_PREFIX_LEN`
/// that share a truncated prefix (which is where the relational backends need
/// an explicit re-sort).
#[tokio::test(flavor = "multi_thread")]
async fn index_scan_matches_a_brute_force_model() -> anyhow::Result<()> {
    for seed in [1u64, 2, 3, 5, 8, 13] {
        let f = Fixture::new()?;
        let w = build(&f.persistence, seed, 30).await?;
        let reader = f.persistence.reader();
        let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);

        // Bounds drawn from real keys, their neighbours, and nothing at all.
        let mut bounds: Vec<Option<Vec<u8>>> = vec![None];
        for k in &w.keys {
            bounds.push(Some(k.clone()));
            let mut plus = k.clone();
            plus.push(0);
            bounds.push(Some(plus));
            if !k.is_empty() {
                bounds.push(Some(k[..k.len() - 1].to_vec()));
            }
        }

        for _ in 0..120 {
            let start = bounds[rng.below(bounds.len())].clone().unwrap_or_default();
            let end = bounds[rng.below(bounds.len())].clone();
            let interval = interval_of(&start, end.as_deref());
            if interval.is_empty() {
                continue;
            }
            let read_ts = ts(rng.below(w.max_ts as usize + 2) as u64);
            let order = if rng.chance(50) {
                Order::Asc
            } else {
                Order::Desc
            };
            let size_hint = [1usize, 2, 3, 7, 64][rng.below(5)];

            let rows: Vec<_> = reader
                .index_scan(
                    index_id(1),
                    tablet(),
                    read_ts,
                    &interval,
                    order,
                    size_hint,
                    validator(),
                )
                .try_collect()
                .await?;
            let actual: Vec<(Vec<u8>, u64, String)> = rows
                .into_iter()
                .map(|(k, d)| (k.0, u64::from(d.ts), body_of(&d.value)))
                .collect();
            let expected = w.model.index_scan(index_id(1), read_ts, &interval, order);
            assert_eq!(
                actual, expected,
                "seed {seed}: index_scan mismatch at read_ts {read_ts}, order {order:?}, \
                 size_hint {size_hint}, interval {interval:?}",
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// document log
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn document_log_matches_a_brute_force_model() -> anyhow::Result<()> {
    for seed in [1u64, 7, 21] {
        let f = Fixture::new()?;
        let w = build(&f.persistence, seed, 25).await?;
        let reader = f.persistence.reader();
        let mut rng = Rng(seed ^ 0xDEADBEEF | 1);

        for _ in 0..80 {
            let a = rng.below(w.max_ts as usize + 3) as u64;
            let b = rng.below(w.max_ts as usize + 3) as u64;
            let (lo, hi) = (a.min(b), a.max(b));
            // Deliberately include inverted and empty ranges.
            let range = if rng.chance(15) {
                TimestampRange::new(ts(hi)..ts(lo))
            } else {
                TimestampRange::new(ts(lo)..ts(hi))
            };
            let order = if rng.chance(50) {
                Order::Asc
            } else {
                Order::Desc
            };
            let page_size = [1u32, 2, 5, 100][rng.below(4)];

            let entries: Vec<_> = reader
                .load_documents(range, order, page_size, validator())
                .try_collect()
                .await?;
            let actual: Vec<(u64, u32)> = entries
                .iter()
                .map(|e| {
                    (
                        u64::from(e.ts),
                        u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap()),
                    )
                })
                .collect();
            let expected = w.model.document_log(range, order);
            assert_eq!(
                actual, expected,
                "seed {seed}: load_documents mismatch for {range:?} {order:?} page {page_size}",
            );

            // Bodies and prev_ts have to survive too.
            for e in &entries {
                let n = u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap());
                let (body, prev) = w.model.documents.get(&(e.ts, doc_id(n))).unwrap();
                assert_eq!(e.value.as_ref().map(body_of), *body);
                assert_eq!(e.prev_ts, *prev);
            }

            // Same query restricted to the single tablet everything lives in
            // must be identical.
            let from_table: Vec<_> = reader
                .load_documents_from_table(tablet(), range, order, page_size, validator())
                .try_collect()
                .await?;
            let from_table: Vec<(u64, u32)> = from_table
                .into_iter()
                .map(|e| {
                    (
                        u64::from(e.ts),
                        u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap()),
                    )
                })
                .collect();
            assert_eq!(from_table, expected, "seed {seed}: per-table log mismatch");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// load_index_chunk
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn load_index_chunk_matches_a_brute_force_model() -> anyhow::Result<()> {
    for seed in [4u64, 9, 16] {
        let f = Fixture::new()?;
        let w = build(&f.persistence, seed, 25).await?;
        let expected = w.model.index_entries();

        for chunk_size in [1usize, 2, 3, 7, 1000] {
            let mut cursor = None;
            let mut got = Vec::new();
            loop {
                let chunk = f
                    .persistence
                    .load_index_chunk(cursor.clone(), chunk_size)
                    .await?;
                if chunk.is_empty() {
                    break;
                }
                cursor = chunk.last().cloned();
                got.extend(chunk);
                assert!(
                    got.len() <= expected.len() + 1,
                    "seed {seed}: load_index_chunk is not making progress"
                );
            }
            assert_eq!(
                got, expected,
                "seed {seed}: load_index_chunk mismatch at chunk_size {chunk_size}",
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// previous revisions
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn previous_revisions_match_a_brute_force_model() -> anyhow::Result<()> {
    for seed in [11u64, 23] {
        let f = Fixture::new()?;
        let w = build(&f.persistence, seed, 25).await?;
        let reader = f.persistence.reader();

        let mut queries = BTreeSet::new();
        for (t, id) in w.model.documents.keys() {
            queries.insert((*id, *t));
        }
        // Also ask about timestamps that hold no revision at all.
        for n in 0..w.keys.len() as u32 {
            for t in [0u64, 1, w.max_ts, w.max_ts + 1] {
                queries.insert((doc_id(n), ts(t)));
            }
        }

        let got = reader
            .previous_revisions(queries.clone(), validator())
            .await?;
        for (id, at) in &queries {
            let expected = w.model.previous_revision(*id, *at);
            match (got.get(&(*id, *at)), expected) {
                (None, None) => {},
                (Some(entry), Some((t, body, prev))) => {
                    assert_eq!(u64::from(entry.ts), u64::from(t), "seed {seed}: prev ts");
                    assert_eq!(entry.value.as_ref().map(body_of), body, "seed {seed}: body");
                    assert_eq!(entry.prev_ts, prev, "seed {seed}: prev_ts");
                },
                (a, b) => {
                    panic!("seed {seed}: previous_revisions mismatch for {id}@{at}: {a:?} vs {b:?}")
                },
            }
        }

        // Exact-coordinate lookups.
        let mut exact = BTreeSet::new();
        for ((t, id), (_, prev)) in &w.model.documents {
            if let Some(prev) = prev {
                exact.insert(DocumentPrevTsQuery {
                    id: *id,
                    ts: *t,
                    prev_ts: *prev,
                });
            }
            // A coordinate that does not exist.
            exact.insert(DocumentPrevTsQuery {
                id: *id,
                ts: *t,
                prev_ts: ts(u64::from(*t) + 1000),
            });
        }
        let got = reader
            .previous_revisions_of_documents(exact.clone(), validator())
            .await?;
        for query in &exact {
            let expected = w.model.documents.get(&(query.prev_ts, query.id));
            match (got.get(query), expected) {
                (None, None) => {},
                (Some(entry), Some((body, prev))) => {
                    assert_eq!(entry.ts, query.prev_ts);
                    assert_eq!(entry.value.as_ref().map(body_of), *body);
                    assert_eq!(entry.prev_ts, *prev);
                },
                (a, b) => {
                    panic!("seed {seed}: exact revision mismatch for {query:?}: {a:?} vs {b:?}")
                },
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// retention deletes, interleaved with the reads above
// ---------------------------------------------------------------------------

/// Retention deletes every revision at or before a timestamp; afterwards the
/// remaining data must still answer every query the same way the model does.
#[tokio::test(flavor = "multi_thread")]
async fn retention_deletes_leave_the_model_intact() -> anyhow::Result<()> {
    for seed in [6u64, 17] {
        let f = Fixture::new()?;
        let mut w = build(&f.persistence, seed, 30).await?;
        let cut = ts(w.max_ts / 2);

        // Index retention: hand over every expired version, the same way
        // `LeaderRetentionManager` does, including the same key more than once.
        let expired: Vec<IndexEntry> = w
            .model
            .index_entries()
            .into_iter()
            .filter(|e| e.ts <= cut)
            .collect();
        let expected_deleted = expired.len();
        let deleted = f.persistence.delete_index_entries(expired.clone()).await?;
        assert_eq!(
            deleted, expected_deleted,
            "seed {seed}: index retention deleted the wrong number of rows",
        );
        w.model.indexes.retain(|(_, _, t), _| *t > cut);

        // Document retention.
        let expired_docs: Vec<(Timestamp, InternalDocumentId)> = w
            .model
            .documents
            .keys()
            .filter(|(t, _)| *t <= cut)
            .map(|(t, id)| (*t, *id))
            .collect();
        let expected_deleted = expired_docs.len();
        let deleted = f.persistence.delete(expired_docs).await?;
        assert_eq!(
            deleted, expected_deleted,
            "seed {seed}: document retention deleted the wrong number of rows",
        );
        w.model.documents.retain(|(t, _), _| *t > cut);

        // Deleting again is a no-op.
        assert_eq!(f.persistence.delete(vec![]).await?, 0);

        let reader = f.persistence.reader();
        let all = TimestampRange::all();
        let entries: Vec<_> = reader
            .load_documents(all, Order::Asc, 3, validator())
            .try_collect()
            .await?;
        let actual: Vec<(u64, u32)> = entries
            .iter()
            .map(|e| {
                (
                    u64::from(e.ts),
                    u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(actual, w.model.document_log(all, Order::Asc));

        assert_eq!(
            f.persistence.load_index_chunk(None, 10_000).await?,
            w.model.index_entries(),
            "seed {seed}: surviving index entries",
        );

        // Index scans at timestamps still inside retention must be unchanged.
        for read_ts in (u64::from(cut) + 1)..=w.max_ts {
            let rows: Vec<_> = reader
                .index_scan(
                    index_id(1),
                    tablet(),
                    ts(read_ts),
                    &Interval::all(),
                    Order::Asc,
                    2,
                    validator(),
                )
                .try_collect()
                .await?;
            let actual: Vec<(Vec<u8>, u64, String)> = rows
                .into_iter()
                .map(|(k, d)| (k.0, u64::from(d.ts), body_of(&d.value)))
                .collect();
            let expected =
                w.model
                    .index_scan(index_id(1), ts(read_ts), &Interval::all(), Order::Asc);
            assert_eq!(
                actual, expected,
                "seed {seed}: index_scan after retention at {read_ts}",
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

/// Many writers and readers at once, on one database, for long enough to cross
/// several memtable flushes. Looking for a panic, a deadlock, or a lost write.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_and_readers_lose_nothing() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let persistence = Arc::new(RocksDbPersistence::new(&dir.path().join("db"))?);

    const WRITERS: u32 = 8;
    const PER_WRITER: u32 = 40;

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let persistence = persistence.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                let n = w * PER_WRITER + i;
                // Timestamps are globally unique, as the committer guarantees.
                let t = ts(u64::from(n) + 1);
                let key = format!("k{n:06}");
                persistence
                    .write(
                        &[DocumentLogEntry {
                            ts: t,
                            id: doc_id(n),
                            value: Some(document(n, &format!("v{n}"))?),
                            prev_ts: None,
                        }],
                        &[PersistenceIndexEntry {
                            ts: t,
                            index_id: index_id(1),
                            key: IndexKeyBytes(key.into_bytes()),
                            value: Some(doc_id(n)),
                        }],
                        ConflictStrategy::Error,
                    )
                    .await?;
            }
            anyhow::Ok(())
        }));
    }
    // Readers run against a moving target; they must never error or panic.
    for _ in 0..4 {
        let persistence = persistence.clone();
        tasks.push(tokio::spawn(async move {
            let reader = persistence.reader();
            for _ in 0..30 {
                let _: Vec<_> = reader
                    .load_documents(TimestampRange::all(), Order::Asc, 3, validator())
                    .try_collect()
                    .await?;
                let _: Vec<_> = reader
                    .index_scan(
                        index_id(1),
                        tablet(),
                        ts(u64::from(WRITERS * PER_WRITER) + 1),
                        &Interval::all(),
                        Order::Desc,
                        2,
                        validator(),
                    )
                    .try_collect()
                    .await?;
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }

    let reader = persistence.reader();
    let entries: Vec<_> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 64, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), (WRITERS * PER_WRITER) as usize);
    let rows: Vec<_> = reader
        .index_scan(
            index_id(1),
            tablet(),
            ts(u64::from(WRITERS * PER_WRITER) + 1),
            &Interval::all(),
            Order::Asc,
            8,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(rows.len(), (WRITERS * PER_WRITER) as usize);
    Ok(())
}

// ---------------------------------------------------------------------------
// Specific suspicions
// ---------------------------------------------------------------------------

/// `ConflictStrategy::Error` is a primary key in the relational backends, so a
/// batch naming one `(ts, id)` twice is rejected outright. Here the check is a
/// point get taken *before* the batch, which cannot see the batch's own rows.
#[tokio::test]
async fn duplicate_keys_inside_one_batch_under_conflict_error() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let t = ts(10);
    let result = f
        .persistence
        .write(
            &[
                DocumentLogEntry {
                    ts: t,
                    id: doc_id(1),
                    value: Some(document(1, "first")?),
                    prev_ts: None,
                },
                DocumentLogEntry {
                    ts: t,
                    id: doc_id(1),
                    value: Some(document(1, "second")?),
                    prev_ts: None,
                },
            ],
            &[],
            ConflictStrategy::Error,
        )
        .await;
    let reader = f.persistence.reader();
    let entries: Vec<_> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    println!(
        "duplicate-in-batch write result: {:?}; rows stored: {:?}",
        result.as_ref().err().map(|e| e.to_string()),
        entries
            .iter()
            .map(|e| e.value.as_ref().map(body_of))
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// An empty or inverted `TimestampRange` — which `RepeatablePersistence`
/// produces whenever a caller's range is disjoint from the snapshot bound —
/// must return nothing rather than everything.
#[tokio::test]
async fn inverted_and_empty_timestamp_ranges_return_nothing() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    for n in 1..5u32 {
        f.persistence
            .write(
                &[DocumentLogEntry {
                    ts: ts(u64::from(n) * 10),
                    id: doc_id(n),
                    value: Some(document(n, "v")?),
                    prev_ts: None,
                }],
                &[],
                ConflictStrategy::Error,
            )
            .await?;
    }
    let reader = f.persistence.reader();
    for range in [
        TimestampRange::empty(),
        TimestampRange::new(ts(30)..ts(10)),
        TimestampRange::new(ts(10)..ts(10)),
        TimestampRange::all().intersect(TimestampRange::new(ts(100)..ts(200))),
    ] {
        for order in [Order::Asc, Order::Desc] {
            let entries: Vec<_> = reader
                .load_documents(range, order, 2, validator())
                .try_collect()
                .await?;
            assert!(
                entries.is_empty(),
                "range {range:?} order {order:?} returned {} rows",
                entries.len()
            );
            let entries: Vec<_> = reader
                .load_documents_from_table(tablet(), range, order, 2, validator())
                .try_collect()
                .await?;
            assert!(
                entries.is_empty(),
                "per-table range {range:?} order {order:?} returned {} rows",
                entries.len()
            );
        }
    }
    Ok(())
}

/// `delete_tablet_documents` is called in a loop until it reports zero. It must
/// terminate, remove every revision, and leave the index entries alone — which
/// is what the relational backends do.
#[tokio::test]
async fn delete_tablet_documents_terminates_and_leaves_indexes() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let w = build(&f.persistence, 42, 20).await?;
    let before = w.model.index_entries();

    let mut total = 0;
    loop {
        let n = f.persistence.delete_tablet_documents(tablet(), 3).await?;
        if n == 0 {
            break;
        }
        total += n;
        assert!(total <= w.model.documents.len() + 1, "not making progress");
    }
    assert_eq!(total, w.model.documents.len());

    let reader = f.persistence.reader();
    let entries: Vec<_> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert!(entries.is_empty(), "documents survived the tablet delete");
    assert_eq!(
        f.persistence.load_index_chunk(None, 100_000).await?,
        before,
        "tablet delete must not touch index entries",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Retention moving under a live scan
// ---------------------------------------------------------------------------

/// Document retention and `delete_tablet_documents` remove rows continuously,
/// with no reference to the scans in flight. A page cursor that seeks and steps
/// unconditionally skips the row *after* a deleted cursor row. This drives a
/// paged `index_scan` one row at a time while deletes land between pages, and
/// asserts that every key the deletes did not touch is still returned, in
/// order, exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn index_scan_is_not_derailed_by_deletes_between_pages() -> anyhow::Result<()> {
    use futures::StreamExt;

    let f = Fixture::new()?;
    const KEYS: u32 = 60;
    // Three versions of every key: 10, 20, 30. All live at 30.
    for t in [10u64, 20, 30] {
        let mut documents = Vec::new();
        let mut indexes = Vec::new();
        for n in 0..KEYS {
            documents.push(DocumentLogEntry {
                ts: ts(t),
                id: doc_id(n),
                value: Some(document(n, &format!("v{n}@{t}"))?),
                prev_ts: if t == 10 { None } else { Some(ts(t - 10)) },
            });
            indexes.push(PersistenceIndexEntry {
                ts: ts(t),
                index_id: index_id(1),
                key: IndexKeyBytes(format!("k{n:03}").into_bytes()),
                value: Some(doc_id(n)),
            });
        }
        f.persistence
            .write(&documents, &indexes, ConflictStrategy::Error)
            .await?;
    }

    let reader = f.persistence.reader();
    // size_hint 1, so every row is its own page and a delete can land between
    // any two of them.
    let mut stream = reader.index_scan(
        index_id(1),
        tablet(),
        ts(30),
        &Interval::all(),
        Order::Asc,
        1,
        validator(),
    );

    let mut seen: Vec<String> = Vec::new();
    while let Some(row) = stream.next().await {
        let (key, _) = row?;
        seen.push(String::from_utf8(key.0)?);

        if seen.len() == 5 {
            // Real document retention: every revision at or before 20 goes,
            // including the ones this scan has already walked past.
            let expired: Vec<IndexEntry> = (0..KEYS)
                .flat_map(|n| {
                    [10u64, 20].into_iter().map(move |t| {
                        IndexEntry::from_index_key(
                            IndexKeyBytes(format!("k{n:03}").into_bytes()),
                            index_id(1),
                            ts(t),
                            false,
                        )
                    })
                })
                .collect();
            f.persistence.delete_index_entries(expired).await?;
            let docs: Vec<_> = (0..KEYS)
                .flat_map(|n| [10u64, 20].into_iter().map(move |t| (ts(t), doc_id(n))))
                .collect();
            f.persistence.delete(docs).await?;
        }
        if seen.len() == 10 {
            // A table drop: the *live* revisions of the last ten keys vanish
            // mid-scan, cursor group included.
            let expired: Vec<IndexEntry> = (KEYS - 10..KEYS)
                .map(|n| {
                    IndexEntry::from_index_key(
                        IndexKeyBytes(format!("k{n:03}").into_bytes()),
                        index_id(1),
                        ts(30),
                        false,
                    )
                })
                .collect();
            f.persistence.delete_index_entries(expired).await?;
        }
    }

    // Strictly increasing: no duplicates, no reordering.
    let mut sorted = seen.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted, seen, "keys were repeated or came back out of order");

    // Every key the deletes never touched must still be there.
    for n in 0..KEYS - 10 {
        let key = format!("k{n:03}");
        assert!(
            seen.contains(&key),
            "{key} was silently skipped by a delete landing between pages; got {seen:?}",
        );
    }
    Ok(())
}

/// The same shape for the document log, but with the deletes chosen randomly
/// rather than at fixed points, and over many seeds.
#[tokio::test(flavor = "multi_thread")]
async fn document_log_is_not_derailed_by_deletes_between_pages() -> anyhow::Result<()> {
    use futures::StreamExt;

    for seed in [3u64, 19, 71] {
        let f = Fixture::new()?;
        const N: u32 = 80;
        for n in 0..N {
            f.persistence
                .write(
                    &[DocumentLogEntry {
                        ts: ts(u64::from(n) + 1),
                        id: doc_id(n),
                        value: Some(document(n, "v")?),
                        prev_ts: None,
                    }],
                    &[],
                    ConflictStrategy::Error,
                )
                .await?;
        }
        let mut rng = Rng(seed | 1);
        // Everything at or below this is fair game for retention; everything
        // above it must survive and be returned.
        let safe_from = u64::from(N) / 2;

        let reader = f.persistence.reader();
        let mut stream = reader.load_documents(TimestampRange::all(), Order::Asc, 1, validator());
        let mut seen: Vec<u64> = Vec::new();
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            seen.push(u64::from(entry.ts));
            if rng.chance(30) {
                let victim = 1 + (rng.below(safe_from as usize) as u64);
                f.persistence
                    .delete(vec![(ts(victim), doc_id((victim - 1) as u32))])
                    .await?;
            }
        }
        let mut sorted = seen.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted, seen, "seed {seed}: log rows repeated or reordered");
        for t in safe_from + 1..=u64::from(N) {
            assert!(
                seen.contains(&t),
                "seed {seed}: revision at {t} was skipped although nothing deleted it",
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Everything written has to still be there, and answer identically, after the
/// process that wrote it is gone. Run once with a clean `shutdown()` and once
/// with the handle simply dropped, which is what a rolling restart actually
/// does.
#[tokio::test(flavor = "multi_thread")]
async fn a_reopened_database_answers_identically() -> anyhow::Result<()> {
    for clean in [true, false] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let model;
        let max_ts;
        {
            let persistence = RocksDbPersistence::new(&path)?;
            let w = build(&persistence, 12345, 30).await?;
            model = w.model;
            max_ts = w.max_ts;
            if clean {
                persistence.shutdown().await?;
            }
        }

        let reopened = RocksDbPersistence::new(&path)?;
        assert!(!reopened.is_fresh(), "a database with data is not fresh");
        let reader = reopened.reader();

        let all = TimestampRange::all();
        let entries: Vec<_> = reader
            .load_documents(all, Order::Asc, 7, validator())
            .try_collect()
            .await?;
        let actual: Vec<(u64, u32)> = entries
            .iter()
            .map(|e| {
                (
                    u64::from(e.ts),
                    u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(
            actual,
            model.document_log(all, Order::Asc),
            "clean={clean}: document log after reopen",
        );
        for e in &entries {
            let n = u32::from_be_bytes(e.id.internal_id()[..4].try_into().unwrap());
            let (body, prev) = model.documents.get(&(e.ts, doc_id(n))).unwrap();
            assert_eq!(e.value.as_ref().map(body_of), *body, "clean={clean}: body");
            assert_eq!(e.prev_ts, *prev, "clean={clean}: prev_ts");
        }

        for read_ts in 0..=max_ts {
            for order in [Order::Asc, Order::Desc] {
                let rows: Vec<_> = reader
                    .index_scan(
                        index_id(1),
                        tablet(),
                        ts(read_ts),
                        &Interval::all(),
                        order,
                        3,
                        validator(),
                    )
                    .try_collect()
                    .await?;
                let actual: Vec<(Vec<u8>, u64, String)> = rows
                    .into_iter()
                    .map(|(k, d)| (k.0, u64::from(d.ts), body_of(&d.value)))
                    .collect();
                assert_eq!(
                    actual,
                    model.index_scan(index_id(1), ts(read_ts), &Interval::all(), order),
                    "clean={clean}: index_scan at {read_ts} {order:?} after reopen",
                );
            }
        }

        assert_eq!(
            reopened.load_index_chunk(None, 100_000).await?,
            model.index_entries(),
            "clean={clean}: index entries after reopen",
        );

        // Every column family agrees about which revisions exist: `docs` and
        // `dtab` are the only witnesses that a `dlog` row is reachable, and a
        // write that landed in one but not the others would show up here.
        let by_table: Vec<_> = reader
            .load_documents_from_table(tablet(), all, Order::Asc, 7, validator())
            .try_collect()
            .await?;
        assert_eq!(
            by_table.len(),
            entries.len(),
            "clean={clean}: dtab and dlog disagree about how many revisions exist",
        );
        let mut prevs = BTreeSet::new();
        for ((t, id), (_, prev)) in &model.documents {
            if let Some(prev) = prev {
                prevs.insert((*id, *t));
                let _ = prev;
            }
        }
        let got = reader
            .previous_revisions(prevs.clone(), validator())
            .await?;
        for (id, at) in &prevs {
            let expected = model.previous_revision(*id, *at);
            assert_eq!(
                got.get(&(*id, *at)).map(|e| u64::from(e.ts)),
                expected.map(|(t, ..)| u64::from(t)),
                "clean={clean}: previous revision of {id}@{at} after reopen",
            );
        }
    }
    Ok(())
}

/// Writers, retention deletes and readers all at once. Looking for a panic, a
/// deadlock, a poisoned lock, or a read that errors because two column families
/// disagreed mid-delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writes_deletes_and_reads_can_all_run_at_once() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let persistence = Arc::new(RocksDbPersistence::new(&dir.path().join("db"))?);
    const N: u32 = 400;

    // How far the writer has got. Retention only ever touches timestamps well
    // below this, which is the invariant `min_snapshot_ts` gives the real one.
    let written_up_to = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let writer = {
        let persistence = persistence.clone();
        let written_up_to = written_up_to.clone();
        tokio::spawn(async move {
            for n in 0..N {
                let t = ts(u64::from(n) + 1);
                persistence
                    .write(
                        &[DocumentLogEntry {
                            ts: t,
                            id: doc_id(n % 50),
                            value: Some(document(n % 50, "v")?),
                            prev_ts: if n >= 50 {
                                Some(ts(u64::from(n) - 49))
                            } else {
                                None
                            },
                        }],
                        &[PersistenceIndexEntry {
                            ts: t,
                            index_id: index_id(1),
                            key: IndexKeyBytes(format!("k{:03}", n % 50).into_bytes()),
                            value: Some(doc_id(n % 50)),
                        }],
                        ConflictStrategy::Overwrite,
                    )
                    .await?;
                written_up_to.store(u64::from(n) + 1, std::sync::atomic::Ordering::SeqCst);
            }
            anyhow::Ok(())
        })
    };
    let retention = {
        let persistence = persistence.clone();
        let written_up_to = written_up_to.clone();
        tokio::spawn(async move {
            for _ in 0..40u64 {
                let cut = written_up_to
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .saturating_sub(60);
                // Index entries first, then documents — the order
                // `LeaderRetentionManager` uses, and the only order in which a
                // live index entry never outlives the document it names.
                let expired: Vec<IndexEntry> = (1..=cut)
                    .map(|t| {
                        IndexEntry::from_index_key(
                            IndexKeyBytes(format!("k{:03}", (t as u32 - 1) % 50).into_bytes()),
                            index_id(1),
                            ts(t),
                            false,
                        )
                    })
                    .collect();
                if !expired.is_empty() {
                    persistence.delete_index_entries(expired).await?;
                }
                let docs: Vec<_> = (1..=cut)
                    .map(|t| (ts(t), doc_id((t as u32 - 1) % 50)))
                    .collect();
                if !docs.is_empty() {
                    persistence.delete(docs).await?;
                }
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        })
    };
    let mut readers = Vec::new();
    for _ in 0..4 {
        let persistence = persistence.clone();
        readers.push(tokio::spawn(async move {
            let reader = persistence.reader();
            for _ in 0..40 {
                // Reads may legitimately return fewer rows as retention runs,
                // but they must never fail.
                let _: Vec<_> = reader
                    .load_documents(TimestampRange::all(), Order::Desc, 3, validator())
                    .try_collect()
                    .await?;
                let _: Vec<_> = reader
                    .index_scan(
                        index_id(1),
                        tablet(),
                        ts(u64::from(N)),
                        &Interval::all(),
                        Order::Asc,
                        2,
                        validator(),
                    )
                    .try_collect()
                    .await?;
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        }));
    }
    writer.await??;
    retention.await??;
    for r in readers {
        r.await??;
    }

    // The newest revision of every document survived retention and is still
    // reachable through the index.
    let reader = persistence.reader();
    let rows: Vec<_> = reader
        .index_scan(
            index_id(1),
            tablet(),
            ts(u64::from(N)),
            &Interval::all(),
            Order::Asc,
            8,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 50, "every live key must still resolve");
    Ok(())
}

// ---------------------------------------------------------------------------
// Health monitor: why the engine's own properties cannot classify a stall
// ---------------------------------------------------------------------------

/// The measurement behind removing the stall *classifier* from the health
/// monitor.
///
/// Five review rounds tried to separate "deliberately backpressured" from
/// "wedged" using `num-running-flushes` and `num-running-compactions`, and each
/// attempt was wrong in a new way. This shows why that was not four coding
/// mistakes: on a database being written to continuously, the engine reports a
/// background job open only a fraction of the time, so an instantaneous reading
/// of those properties says very little about whether it is working.
///
/// Not a correctness test. It samples properties this crate no longer reads,
/// and a failure would mean a design argument was weaker than believed rather
/// than that anything is broken. It also costs six seconds of real I/O, so it
/// is opt-in: `cargo test -p rocksdb_persistence -- --ignored`.
#[ignore = "a measurement supporting a design decision, not a correctness check"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_backpressure_predicate_is_true_under_ordinary_load() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let persistence = Arc::new(RocksDbPersistence::new(&dir.path().join("db"))?);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let persistence = persistence.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            // Big-ish bodies so memtables fill and compaction actually runs.
            let body = "x".repeat(2000);
            let mut t = 1u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut documents = Vec::new();
                let mut indexes = Vec::new();
                for i in 0..32u32 {
                    let n = (t as u32).wrapping_mul(32).wrapping_add(i) % 5000;
                    documents.push(DocumentLogEntry {
                        ts: ts(t),
                        id: doc_id(n),
                        value: Some(document(n, &body)?),
                        prev_ts: None,
                    });
                    indexes.push(PersistenceIndexEntry {
                        ts: ts(t),
                        index_id: index_id(1),
                        key: IndexKeyBytes(format!("k{n:06}-{t}").into_bytes()),
                        value: Some(doc_id(n)),
                    });
                    t += 1;
                }
                persistence
                    .write(&documents, &indexes, ConflictStrategy::Overwrite)
                    .await?;
            }
            anyhow::Ok(())
        })
    };

    let mut samples = 0u32;
    let mut masked = 0u32;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    while std::time::Instant::now() < deadline {
        let p = |name: &str| {
            persistence
                .inner
                .db
                .property_int_value(name)
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        // The predicate as it was originally written: a background job being
        // *open* counted as backpressure. Sampled here only to show how often
        // that reading is available to mask a stall on a healthy database.
        let by_presence = p("rocksdb.is-write-stopped") > 0
            || p("rocksdb.actual-delayed-write-rate") > 0
            || p("rocksdb.num-running-flushes") > 0
            || p("rocksdb.num-running-compactions") > 0;
        samples += 1;
        if by_presence {
            masked += 1;
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await??;

    assert!(
        masked * 2 < samples,
        "a background job was open in {masked}/{samples} samples; if the engine really did report \
         a job open most of the time it is busy, the classifier this measurement was taken to \
         discredit would have been defensible"
    );
    println!(
        "a background job was open in {masked}/{samples} samples ({:.1}%) on a healthy, busy \
         database",
        100.0 * f64::from(masked) / f64::from(samples),
    );
    Ok(())
}
