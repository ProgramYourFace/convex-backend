//! Behavioural tests for the RocksDB persistence layer.
//!
//! These exercise the [`Persistence`] and [`PersistenceReader`] contract the
//! way the database itself uses it: multi-version documents read at explicit
//! timestamps, index scans that must resolve to the newest version at or before
//! a snapshot, retention deletes, and durability across a reopen. The key
//! encodings have their own unit and property tests in [`crate::keys`].

use std::{
    collections::BTreeSet,
    sync::Arc,
};

use common::{
    document::{
        CreationTime,
        ResolvedDocument,
    },
    index::IndexKeyBytes,
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
        PersistenceGlobalKey,
        PersistenceIndexEntry,
        PersistenceReader,
        RetentionValidator,
        TimestampRange,
    },
    query::Order,
    shutdown::ShutdownSignal,
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
use rocksdb::backup::{
    BackupEngine,
    BackupEngineOptions,
    RestoreOptions,
};
use value::{
    DeveloperDocumentId,
    InternalId,
    TableNumber,
};

use crate::{
    backup,
    keys,
    keys::CF_GLOBALS,
    options::{
        self,
        SyncMode,
    },
    OpenOptions,
    RocksDbPersistence,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const TABLE_NUMBER: u32 = 7;

pub(crate) fn tablet(n: u8) -> TabletId {
    TabletId(InternalId([n; keys::ID_LEN]))
}

fn internal_id(n: u32) -> InternalId {
    let mut bytes = [0u8; keys::ID_LEN];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    InternalId(bytes)
}

pub(crate) fn doc_id(tablet_n: u8, n: u32) -> InternalDocumentId {
    InternalDocumentId::new(tablet(tablet_n), internal_id(n))
}

pub(crate) fn index_id(n: u8) -> IndexId {
    IndexId(InternalId([0xA0 | n; keys::ID_LEN]))
}

pub(crate) fn ts(n: u64) -> Timestamp {
    Timestamp::try_from(n).unwrap()
}

pub(crate) fn document(tablet_n: u8, n: u32, body: &str) -> anyhow::Result<ResolvedDocument> {
    let id = ResolvedDocumentId {
        tablet_id: tablet(tablet_n),
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

fn entry(
    tablet_n: u8,
    n: u32,
    at: u64,
    body: &str,
    prev_ts: Option<u64>,
) -> anyhow::Result<DocumentLogEntry> {
    Ok(DocumentLogEntry {
        ts: ts(at),
        id: doc_id(tablet_n, n),
        value: Some(document(tablet_n, n, body)?),
        prev_ts: prev_ts.map(ts),
    })
}

fn tombstone(tablet_n: u8, n: u32, at: u64, prev_ts: Option<u64>) -> DocumentLogEntry {
    DocumentLogEntry {
        ts: ts(at),
        id: doc_id(tablet_n, n),
        value: None,
        prev_ts: prev_ts.map(ts),
    }
}

fn index_entry(
    idx: u8,
    key: &[u8],
    at: u64,
    value: Option<InternalDocumentId>,
) -> PersistenceIndexEntry {
    PersistenceIndexEntry {
        ts: ts(at),
        index_id: index_id(idx),
        key: IndexKeyBytes(key.to_vec()),
        value,
    }
}

fn body_of(doc: &ResolvedDocument) -> String {
    doc.value()
        .get::<str>("body")
        .expect("document has no body field")
        .to_string()
}

fn validator() -> Arc<dyn RetentionValidator> {
    Arc::new(NoopRetentionValidator)
}

fn interval(start: &[u8], end: Option<&[u8]>) -> Interval {
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

    fn reader(&self) -> Arc<dyn PersistenceReader> {
        self.persistence.reader()
    }

    async fn write(
        &self,
        documents: &[DocumentLogEntry],
        indexes: &[PersistenceIndexEntry],
    ) -> anyhow::Result<()> {
        self.persistence
            .write(documents, indexes, ConflictStrategy::Error)
            .await
    }

    async fn scan(
        &self,
        idx: u8,
        tablet_n: u8,
        read_ts: u64,
        interval: Interval,
        order: Order,
    ) -> anyhow::Result<Vec<(Vec<u8>, String)>> {
        let rows: Vec<_> = self
            .reader()
            .index_scan(
                index_id(idx),
                tablet(tablet_n),
                ts(read_ts),
                &interval,
                order,
                8,
                validator(),
            )
            .try_collect()
            .await?;
        Ok(rows
            .into_iter()
            .map(|(key, doc)| (key.0, body_of(&doc.value)))
            .collect())
    }

    async fn log(
        &self,
        range: TimestampRange,
        order: Order,
    ) -> anyhow::Result<Vec<(u64, InternalDocumentId)>> {
        let entries: Vec<_> = self
            .reader()
            .load_documents(range, order, 2, validator())
            .try_collect()
            .await?;
        Ok(entries.into_iter().map(|e| (e.ts.into(), e.id)).collect())
    }
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn documents_round_trip_through_the_log() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "first", None)?,
            entry(1, 2, 10, "second", None)?,
            entry(1, 1, 20, "updated", Some(10))?,
        ],
        &[],
    )
    .await?;

    let entries: Vec<_> = f
        .reader()
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 3);
    assert_eq!(body_of(entries[0].value.as_ref().unwrap()), "\"first\"");
    assert_eq!(entries[2].ts, ts(20));
    assert_eq!(entries[2].prev_ts, Some(ts(10)));
    Ok(())
}

#[tokio::test]
async fn tombstones_survive_the_round_trip() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "alive", None)?,
            tombstone(1, 1, 20, Some(10)),
        ],
        &[],
    )
    .await?;

    let entries: Vec<_> = f
        .reader()
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 2);
    assert!(entries[1].value.is_none());
    assert_eq!(entries[1].prev_ts, Some(ts(10)));
    Ok(())
}

#[tokio::test]
async fn document_log_respects_range_order_and_paging() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let mut writes = Vec::new();
    for i in 0..7u64 {
        writes.push(entry(1, i as u32, 10 + i, "x", None)?);
    }
    f.write(&writes, &[]).await?;

    // Page size is 2 in `Fixture::log`, so this crosses several page boundaries.
    let asc = f.log(TimestampRange::all(), Order::Asc).await?;
    assert_eq!(
        asc.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        vec![10, 11, 12, 13, 14, 15, 16]
    );

    let desc = f.log(TimestampRange::all(), Order::Desc).await?;
    assert_eq!(
        desc.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        vec![16, 15, 14, 13, 12, 11, 10]
    );

    let windowed = f
        .log(TimestampRange::new(ts(12)..ts(15)), Order::Asc)
        .await?;
    assert_eq!(
        windowed.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        vec![12, 13, 14]
    );
    Ok(())
}

#[tokio::test]
async fn document_log_can_be_restricted_to_one_tablet() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "t1", None)?,
            entry(2, 1, 11, "t2", None)?,
            entry(1, 2, 12, "t1 again", None)?,
        ],
        &[],
    )
    .await?;

    let entries: Vec<_> = f
        .reader()
        .load_documents_from_table(tablet(1), TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.id.table() == tablet(1)));
    assert_eq!(entries[0].ts, ts(10));
    assert_eq!(entries[1].ts, ts(12));
    // Bodies come from a second lookup on this path, so check they arrived.
    assert_eq!(body_of(entries[1].value.as_ref().unwrap()), "\"t1 again\"");
    Ok(())
}

// ---------------------------------------------------------------------------
// Index scans
// ---------------------------------------------------------------------------

#[tokio::test]
async fn index_scan_returns_the_newest_version_at_or_before_the_snapshot() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
            entry(1, 1, 30, "v3", Some(20))?,
        ],
        &[
            index_entry(1, b"k", 10, Some(id)),
            index_entry(1, b"k", 20, Some(id)),
            index_entry(1, b"k", 30, Some(id)),
        ],
    )
    .await?;

    for (read_ts, expected) in [
        (10, "\"v1\""),
        (15, "\"v1\""),
        (20, "\"v2\""),
        (99, "\"v3\""),
    ] {
        let rows = f
            .scan(1, 1, read_ts, interval(b"", None), Order::Asc)
            .await?;
        assert_eq!(
            rows,
            vec![(b"k".to_vec(), expected.to_string())],
            "at {read_ts}"
        );
    }

    // Before any version exists, the key is simply absent.
    let rows = f.scan(1, 1, 5, interval(b"", None), Order::Asc).await?;
    assert!(rows.is_empty());
    Ok(())
}

#[tokio::test]
async fn index_scan_skips_keys_whose_newest_version_is_a_tombstone() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.write(
        &[entry(1, 1, 10, "v1", None)?],
        &[
            index_entry(1, b"k", 10, Some(id)),
            index_entry(1, b"k", 20, None),
        ],
    )
    .await?;

    // Visible before the delete, gone after it.
    assert_eq!(
        f.scan(1, 1, 10, interval(b"", None), Order::Asc)
            .await?
            .len(),
        1
    );
    assert!(f
        .scan(1, 1, 20, interval(b"", None), Order::Asc)
        .await?
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn index_scan_honours_interval_bounds_and_order() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let mut documents = Vec::new();
    let mut indexes = Vec::new();
    for (i, key) in [b"a".as_slice(), b"b", b"c", b"d"].iter().enumerate() {
        documents.push(entry(1, i as u32, 10, "x", None)?);
        indexes.push(index_entry(1, key, 10, Some(doc_id(1, i as u32))));
    }
    f.write(&documents, &indexes).await?;

    let asc = f
        .scan(1, 1, 10, interval(b"b", Some(b"d")), Order::Asc)
        .await?;
    assert_eq!(
        asc.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![b"b".to_vec(), b"c".to_vec()]
    );

    let desc = f
        .scan(1, 1, 10, interval(b"b", Some(b"d")), Order::Desc)
        .await?;
    assert_eq!(
        desc.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![b"c".to_vec(), b"b".to_vec()]
    );

    let unbounded = f.scan(1, 1, 10, interval(b"c", None), Order::Asc).await?;
    assert_eq!(
        unbounded.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![b"c".to_vec(), b"d".to_vec()]
    );
    Ok(())
}

/// The case the key escaping exists for: one index key a proper prefix of
/// another. Without escaping these sort the wrong way round once a timestamp is
/// concatenated on, and the scan silently returns the wrong rows.
#[tokio::test]
async fn index_scan_orders_prefix_keys_correctly() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let keys: [&[u8]; 5] = [b"", b"a", b"a\x00", b"ab", b"b"];
    let mut documents = Vec::new();
    let mut indexes = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        documents.push(entry(1, i as u32, 10, "x", None)?);
        indexes.push(index_entry(1, key, 10, Some(doc_id(1, i as u32))));
    }
    f.write(&documents, &indexes).await?;

    let rows = f.scan(1, 1, 10, interval(b"", None), Order::Asc).await?;
    let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
    expected.sort();
    assert_eq!(
        rows.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        expected
    );
    Ok(())
}

#[tokio::test]
async fn index_scan_pages_across_many_keys() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let mut documents = Vec::new();
    let mut indexes = Vec::new();
    for i in 0..50u32 {
        documents.push(entry(1, i, 10, "x", None)?);
        indexes.push(index_entry(1, &i.to_be_bytes(), 10, Some(doc_id(1, i))));
    }
    f.write(&documents, &indexes).await?;

    // `Fixture::scan` uses a size hint of 8, so this crosses many pages.
    let asc = f.scan(1, 1, 10, interval(b"", None), Order::Asc).await?;
    assert_eq!(asc.len(), 50);
    let mut keys: Vec<_> = asc.iter().map(|(k, _)| k.clone()).collect();
    let sorted = {
        let mut s = keys.clone();
        s.sort();
        s
    };
    assert_eq!(keys, sorted, "ascending scan must be sorted");

    let desc = f.scan(1, 1, 10, interval(b"", None), Order::Desc).await?;
    assert_eq!(desc.len(), 50);
    keys.reverse();
    assert_eq!(
        desc.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        keys
    );
    Ok(())
}

#[tokio::test]
async fn index_scan_ignores_other_indexes() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[entry(1, 1, 10, "x", None)?],
        &[
            index_entry(1, b"k", 10, Some(doc_id(1, 1))),
            index_entry(2, b"k", 10, Some(doc_id(1, 1))),
        ],
    )
    .await?;
    assert_eq!(
        f.scan(1, 1, 10, interval(b"", None), Order::Asc)
            .await?
            .len(),
        1
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Previous revisions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn previous_revisions_walks_back_one_version() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
            entry(1, 1, 30, "v3", Some(20))?,
        ],
        &[],
    )
    .await?;

    let id = doc_id(1, 1);
    let out = f
        .reader()
        .previous_revisions(BTreeSet::from([(id, ts(30)), (id, ts(20))]), validator())
        .await?;
    assert_eq!(out.len(), 2);
    assert_eq!(out[&(id, ts(30))].ts, ts(20));
    assert_eq!(
        body_of(out[&(id, ts(30))].value.as_ref().unwrap()),
        "\"v2\""
    );
    assert_eq!(out[&(id, ts(20))].ts, ts(10));

    // Nothing precedes the first revision.
    let none = f
        .reader()
        .previous_revisions(BTreeSet::from([(id, ts(10))]), validator())
        .await?;
    assert!(none.is_empty());
    Ok(())
}

#[tokio::test]
async fn previous_revisions_of_documents_reads_exact_coordinates() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
        ],
        &[],
    )
    .await?;

    let id = doc_id(1, 1);
    let query = DocumentPrevTsQuery {
        id,
        ts: ts(20),
        prev_ts: ts(10),
    };
    let out = f
        .reader()
        .previous_revisions_of_documents(BTreeSet::from([query]), validator())
        .await?;
    assert_eq!(body_of(out[&query].value.as_ref().unwrap()), "\"v1\"");

    // A coordinate that does not exist is absent rather than an error.
    let missing = DocumentPrevTsQuery {
        id,
        ts: ts(20),
        prev_ts: ts(11),
    };
    let out = f
        .reader()
        .previous_revisions_of_documents(BTreeSet::from([missing]), validator())
        .await?;
    assert!(out.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// Conflicts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conflict_strategy_error_rejects_a_duplicate_document() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(&[entry(1, 1, 10, "v1", None)?], &[]).await?;
    let err = f
        .write(&[entry(1, 1, 10, "again", None)?], &[])
        .await
        .expect_err("duplicate (ts, id) must be rejected");
    assert!(
        err.to_string().contains("already exists"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn conflict_strategy_error_rejects_a_duplicate_index_entry() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.write(
        &[entry(1, 1, 10, "v1", None)?],
        &[index_entry(1, b"k", 10, Some(id))],
    )
    .await?;
    let err = f
        .persistence
        .write(
            &[],
            &[index_entry(1, b"k", 10, Some(id))],
            ConflictStrategy::Error,
        )
        .await
        .expect_err("duplicate index entry must be rejected");
    assert!(
        err.to_string().contains("already exists"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn conflict_strategy_overwrite_replaces_in_place() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(&[entry(1, 1, 10, "v1", None)?], &[]).await?;
    f.persistence
        .write(
            &[entry(1, 1, 10, "replaced", None)?],
            &[],
            ConflictStrategy::Overwrite,
        )
        .await?;

    let entries: Vec<_> = f
        .reader()
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(body_of(entries[0].value.as_ref().unwrap()), "\"replaced\"");
    Ok(())
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_removes_revisions_at_or_before_a_timestamp() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
            entry(1, 1, 30, "v3", Some(20))?,
        ],
        &[],
    )
    .await?;

    let deleted = f.persistence.delete(vec![(ts(20), doc_id(1, 1))]).await?;
    assert_eq!(deleted, 2, "revisions at 10 and 20 should be removed");

    let remaining = f.log(TimestampRange::all(), Order::Asc).await?;
    assert_eq!(
        remaining.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
        vec![30]
    );

    // The per-tablet index has to be cleaned up alongside the log, or a
    // tablet-scoped scan would resolve a body that is no longer there.
    let by_table: Vec<_> = f
        .reader()
        .load_documents_from_table(tablet(1), TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(by_table.len(), 1);
    Ok(())
}

#[tokio::test]
async fn delete_index_entries_removes_versions_at_or_before_a_timestamp() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
            entry(1, 1, 30, "v3", Some(20))?,
        ],
        &[
            index_entry(1, b"k", 10, Some(id)),
            index_entry(1, b"k", 20, Some(id)),
            index_entry(1, b"k", 30, Some(id)),
        ],
    )
    .await?;

    let chunk = f.persistence.load_index_chunk(None, 100).await?;
    assert_eq!(chunk.len(), 3);
    let expired: Vec<_> = chunk.into_iter().filter(|e| e.ts <= ts(20)).collect();
    let deleted = f.persistence.delete_index_entries(expired).await?;
    assert_eq!(deleted, 2);

    let remaining = f.persistence.load_index_chunk(None, 100).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].ts, ts(30));
    Ok(())
}

/// Retention hands over every expired version it found, so the same key can
/// appear more than once in one call. The count must be rows removed, not
/// clauses matched — which is what `DELETE ... WHERE a OR b` reports.
#[tokio::test]
async fn deleting_the_same_key_twice_counts_each_row_once() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.write(
        &[
            entry(1, 1, 10, "v1", None)?,
            entry(1, 1, 20, "v2", Some(10))?,
            entry(1, 1, 30, "v3", Some(20))?,
        ],
        &[
            index_entry(1, b"k", 10, Some(id)),
            index_entry(1, b"k", 20, Some(id)),
            index_entry(1, b"k", 30, Some(id)),
        ],
    )
    .await?;

    // Both expired versions of `k` name the same two rows between them.
    let expired: Vec<_> = f
        .persistence
        .load_index_chunk(None, 100)
        .await?
        .into_iter()
        .filter(|e| e.ts <= ts(20))
        .collect();
    assert_eq!(expired.len(), 2);
    assert_eq!(f.persistence.delete_index_entries(expired).await?, 2);

    // Same for documents: two coordinates, three revisions, two removed.
    let deleted = f
        .persistence
        .delete(vec![(ts(10), doc_id(1, 1)), (ts(20), doc_id(1, 1))])
        .await?;
    assert_eq!(deleted, 2);
    assert_eq!(f.log(TimestampRange::all(), Order::Asc).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn delete_tablet_documents_removes_whole_documents_in_chunks() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let mut writes = Vec::new();
    for i in 0..5u32 {
        writes.push(entry(1, i, 10 + i as u64, "x", None)?);
        writes.push(entry(1, i, 100 + i as u64, "y", Some(10 + i as u64))?);
    }
    writes.push(entry(2, 0, 200, "other tablet", None)?);
    f.write(&writes, &[]).await?;

    let mut total = 0;
    loop {
        let deleted = f.persistence.delete_tablet_documents(tablet(1), 3).await?;
        total += deleted;
        if deleted == 0 {
            break;
        }
    }
    assert_eq!(total, 10, "both revisions of all five documents");

    let remaining = f.log(TimestampRange::all(), Order::Asc).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].1.table(), tablet(2));
    Ok(())
}

#[tokio::test]
async fn load_index_chunk_pages_in_order() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    let mut documents = Vec::new();
    let mut indexes = Vec::new();
    for (i, key) in [b"a".as_slice(), b"b", b"c"].iter().enumerate() {
        for version in 0..3u64 {
            let at = 10 + i as u64 * 10 + version;
            documents.push(entry(1, (i * 3 + version as usize) as u32, at, "x", None)?);
            indexes.push(index_entry(1, key, at, Some(id)));
        }
    }
    f.write(&documents, &indexes).await?;

    let all = f.persistence.load_index_chunk(None, 100).await?;
    assert_eq!(all.len(), 9);
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted, "chunks must arrive in IndexEntry order");

    // Paging with a cursor must cover the set exactly once.
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let chunk = f.persistence.load_index_chunk(cursor.clone(), 2).await?;
        if chunk.is_empty() {
            break;
        }
        cursor = chunk.last().cloned();
        paged.extend(chunk);
    }
    assert_eq!(paged, all);
    Ok(())
}

// ---------------------------------------------------------------------------
// Globals, freshness and durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persistence_globals_round_trip() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    assert!(f
        .reader()
        .get_persistence_global(PersistenceGlobalKey::MaxRepeatableTimestamp)
        .await?
        .is_none());
    f.persistence
        .write_persistence_global(
            PersistenceGlobalKey::MaxRepeatableTimestamp,
            serde_json::json!(1234),
        )
        .await?;
    assert_eq!(
        f.reader()
            .get_persistence_global(PersistenceGlobalKey::MaxRepeatableTimestamp)
            .await?,
        Some(serde_json::json!(1234)),
    );
    Ok(())
}

#[tokio::test]
async fn max_ts_covers_both_the_log_and_the_repeatable_timestamp() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    assert_eq!(f.reader().max_ts().await?, None);

    f.write(&[entry(1, 1, 10, "v1", None)?], &[]).await?;
    assert_eq!(f.reader().max_ts().await?, Some(ts(10)));

    // A repeatable timestamp ahead of the log wins; one behind it does not.
    f.persistence
        .write_persistence_global(
            PersistenceGlobalKey::MaxRepeatableTimestamp,
            serde_json::json!(50),
        )
        .await?;
    assert_eq!(f.reader().max_ts().await?, Some(ts(50)));

    f.write(&[entry(1, 2, 60, "v2", None)?], &[]).await?;
    assert_eq!(f.reader().max_ts().await?, Some(ts(60)));
    Ok(())
}

#[tokio::test]
async fn a_new_database_is_fresh_and_a_reopened_one_is_not() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");

    let persistence = RocksDbPersistence::new(&path)?;
    assert!(persistence.is_fresh());
    persistence
        .write(
            &[entry(1, 1, 10, "durable", None)?],
            &[],
            ConflictStrategy::Error,
        )
        .await?;
    persistence.shutdown().await?;
    drop(persistence);

    let reopened = RocksDbPersistence::new(&path)?;
    assert!(!reopened.is_fresh());
    let entries: Vec<_> = reopened
        .reader()
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(body_of(entries[0].value.as_ref().unwrap()), "\"durable\"");
    Ok(())
}

#[tokio::test]
async fn writes_survive_a_reopen_without_a_clean_shutdown() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    {
        let persistence = RocksDbPersistence::new(&path)?;
        let id = doc_id(1, 1);
        persistence
            .write(
                &[entry(1, 1, 10, "v1", None)?],
                &[index_entry(1, b"k", 10, Some(id))],
                ConflictStrategy::Error,
            )
            .await?;
        // Dropped without `shutdown`, so recovery must come from the WAL.
    }

    let reopened = RocksDbPersistence::new(&path)?;
    let rows: Vec<_> = reopened
        .reader()
        .index_scan(
            index_id(1),
            tablet(1),
            ts(10),
            &interval(b"", None),
            Order::Asc,
            8,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "the index entry and its document must both recover"
    );
    Ok(())
}

/// `BackupEngine` is the mechanism `docs/proposals/005-backup-and-restore.md`
/// proposes building on, and it rests on two assumptions worth checking rather
/// than believing: that a backup covers every column family, and that it covers
/// the blob files that documents above `ROCKSDB_BLOB_THRESHOLD_BYTES` are
/// stored in. A backup that silently skipped either would restore a database
/// that opens, reads, and is missing data.
#[tokio::test]
async fn backup_engine_round_trips_every_column_family_and_blob_files() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let backup_path = dir.path().join("backup");

    // A document comfortably past the 4 KiB blob threshold, so its body is
    // stored outside the LSM tree, plus a small one that is not.
    let big_body = "x".repeat(64 * 1024);
    {
        let persistence = RocksDbPersistence::new(&db_path)?;
        persistence
            .write(
                &[
                    entry(1, 1, 10, &big_body, None)?,
                    entry(1, 2, 11, "small", None)?,
                ],
                &[
                    index_entry(1, b"big", 10, Some(doc_id(1, 1))),
                    index_entry(1, b"small", 11, Some(doc_id(1, 2))),
                ],
                ConflictStrategy::Error,
            )
            .await?;
        persistence
            .write_persistence_global(
                PersistenceGlobalKey::MaxRepeatableTimestamp,
                serde_json::json!(4321),
            )
            .await?;

        let mut engine = BackupEngine::open(
            &BackupEngineOptions::new(&backup_path)?,
            &rocksdb::Env::new()?,
        )?;
        // `true` flushes memtables first, so the backup does not depend on
        // replaying a WAL to be complete.
        engine.create_new_backup_flush(&persistence.inner.db, true)?;
        let info = engine.get_backup_info();
        assert_eq!(info.len(), 1, "one backup should have been recorded");
        engine.verify_backup(info[0].backup_id)?;
        persistence.shutdown().await?;
    }

    // Destroy the original, so nothing below can be served by leftovers.
    std::fs::remove_dir_all(&db_path)?;

    let mut engine = BackupEngine::open(
        &BackupEngineOptions::new(&backup_path)?,
        &rocksdb::Env::new()?,
    )?;
    engine.restore_from_latest_backup(&db_path, &db_path, &RestoreOptions::default())?;

    let restored = RocksDbPersistence::new(&db_path)?;
    let reader = restored.reader();

    // `dlog` — including the blob-stored body.
    let entries: Vec<_> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(entries.len(), 2, "both documents must survive the restore");
    assert_eq!(
        body_of(entries[0].value.as_ref().unwrap()).len(),
        big_body.len() + 2,
        "the blob-stored body must come back whole, not truncated or empty"
    );
    assert_eq!(body_of(entries[1].value.as_ref().unwrap()), "\"small\"");

    // `idx` — the index entries and their join back into `dlog`.
    let rows: Vec<_> = reader
        .index_scan(
            index_id(1),
            tablet(1),
            ts(11),
            &interval(b"", None),
            Order::Asc,
            8,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 2, "index entries must survive the restore");

    // `docs` — the id-ordered revision index behind `previous_revisions`.
    let previous = reader
        .previous_revisions(
            std::collections::BTreeSet::from([(doc_id(1, 1).into(), ts(11))]),
            validator(),
        )
        .await?;
    assert_eq!(
        previous.len(),
        1,
        "the revision index must survive the restore"
    );

    // `globals`. Written as 4321, which is past the log's own maximum, so
    // `max_ts` returning it proves the global came back rather than being
    // recomputed from the documents.
    assert_eq!(
        reader
            .get_persistence_global(PersistenceGlobalKey::MaxRepeatableTimestamp)
            .await?,
        Some(serde_json::json!(4321)),
    );
    assert_eq!(reader.max_ts().await?, Some(ts(4321)));
    Ok(())
}

/// The whole operator lifecycle in one pass: take generations, prune to a
/// retention bound, rehearse a restore, and restore for real.
#[tokio::test]
async fn backup_lifecycle_generations_retention_rehearsal_and_restore() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let backup_dir = dir.path().join("backup");

    let persistence = RocksDbPersistence::new(&db_path)?;
    // Four generations, each adding a document, so a restore of generation N
    // can be told apart from a restore of generation N+1.
    for i in 1..=4u32 {
        let id = doc_id(1, i);
        let at = u64::from(i) * 10;
        persistence
            .write(
                &[entry(1, i, at, &format!("v{i}"), None)?],
                &[index_entry(1, format!("k{i}").as_bytes(), at, Some(id))],
                ConflictStrategy::Error,
            )
            .await?;
        // Keep 2, so the first two generations are pruned as the last two land.
        let info = persistence.backup(&backup_dir, 2)?;
        assert_eq!(info.backup_id, i, "generations are always-increasing");
    }

    let generations = backup::list(&backup_dir)?;
    assert_eq!(
        generations.iter().map(|g| g.backup_id).collect::<Vec<_>>(),
        vec![3, 4],
        "retention keeps the newest two and prunes the rest"
    );

    // A rehearsal proves the backup opens and reads, which verification alone
    // does not.
    let scratch = dir.path().join("scratch");
    let (rehearsed, read) = backup::rehearse(&backup_dir, &scratch, None)?;
    assert_eq!(rehearsed.backup_id, 4);
    assert_eq!(
        read.documents, 4,
        "the rehearsal must decode every document, not merely count rows"
    );
    assert_eq!(read.index_entries, 4);
    // Rehearsing again must work: the scratch directory is reused, not appended
    // to, or the second run of a nightly job fails.
    backup::rehearse(&backup_dir, &scratch, None)?;

    // Restoring an *older* retained generation gives that generation's state,
    // which is the whole point of keeping more than one.
    persistence.shutdown().await?;
    drop(persistence);
    let restored_path = dir.path().join("restored");
    backup::restore(&backup_dir, &restored_path, Some(3))?;

    let restored = RocksDbPersistence::new(&restored_path)?;
    let entries: Vec<_> = restored
        .reader()
        .load_documents(TimestampRange::all(), Order::Asc, 16, validator())
        .try_collect()
        .await?;
    assert_eq!(
        entries.len(),
        3,
        "generation 3 held three documents, not the four that exist now"
    );
    Ok(())
}

/// Restoring over a directory that already holds data would write into a
/// database that may be open, and destroys the only other copy. Refuse.
#[tokio::test]
async fn restore_refuses_a_non_empty_target() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let backup_dir = dir.path().join("backup");
    {
        let persistence = RocksDbPersistence::new(&db_path)?;
        persistence
            .write(
                &[entry(1, 1, 10, "v1", None)?],
                &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
                ConflictStrategy::Error,
            )
            .await?;
        persistence.backup(&backup_dir, 4)?;
        persistence.shutdown().await?;
    }

    let err = backup::restore(&backup_dir, &db_path, None)
        .expect_err("restoring over a populated directory must fail");
    assert!(
        format!("{err}").contains("not empty"),
        "the error should say why: {err}"
    );
    Ok(())
}

/// A live backup and a stopped-writer backup share one chain.
///
/// This is the ordinary operational mix: a CronJob backs up through a secondary
/// while the backend runs, and an operator occasionally takes one by hand with
/// the writer stopped. Both must land in the same directory as successive
/// generations, because the backup directory is claimed by database identity —
/// and a secondary cannot mint that identity, so it has to already match.
///
/// An earlier revision asserted the opposite of this test: that "a secondary
/// instance has no WAL and no business writing a backup". The premise was
/// wrong. A secondary tails the primary's WAL, which is exactly what makes a
/// live backup complete.
#[tokio::test]
async fn live_and_stopped_backups_share_a_chain() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let backup_dir = dir.path().join("backup");

    let primary = RocksDbPersistence::new(&db_path)?;
    primary
        .write(
            &[entry(1, 1, 10, "v1", None)?],
            &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
            ConflictStrategy::Error,
        )
        .await?;

    // Live, through a secondary, while the primary holds the lock.
    let secondary = RocksDbPersistence::new_secondary_in(&db_path, &dir.path().join("secondary"))?;
    secondary.backup(&backup_dir, 4)?;

    // Stopped, by the writer itself.
    primary.backup(&backup_dir, 4)?;

    let generations = backup::list(&backup_dir)?;
    assert_eq!(
        generations.len(),
        2,
        "both backups belong to the same chain, so the directory should hold two generations"
    );
    Ok(())
}

/// Document retention deletes rows while exports and index backfills are
/// scanning. A page cursor that seeks-and-steps skips the row *after* a deleted
/// cursor row, which is a revision silently missing from an export or an index
/// update silently missing from a backfill.
#[tokio::test]
async fn load_documents_does_not_skip_a_row_when_the_cursor_row_is_deleted() -> anyhow::Result<()> {
    for order in [Order::Asc, Order::Desc] {
        let f = Fixture::new()?;
        for i in 1..=6u32 {
            let at = u64::from(i) * 10;
            f.persistence
                .write(
                    &[entry(1, i, at, &format!("v{i}"), None)?],
                    &[],
                    ConflictStrategy::Error,
                )
                .await?;
        }

        // Take one page, then delete the row the cursor names.
        let reader = f.persistence.reader();
        let mut stream =
            Box::pin(reader.load_documents(TimestampRange::all(), order, 2, validator()));
        let mut seen = Vec::new();
        for _ in 0..2 {
            let entry = stream
                .try_next()
                .await?
                .expect("two rows in the first page");
            seen.push(u64::from(entry.ts));
        }
        let cursor_ts = *seen.last().unwrap();
        let cursor_n = (cursor_ts / 10) as u32;
        f.persistence
            .delete(vec![(ts(cursor_ts), doc_id(1, cursor_n))])
            .await?;

        while let Some(entry) = stream.try_next().await? {
            seen.push(u64::from(entry.ts));
        }

        // Deleting the cursor row may drop it from the results — it is gone.
        // Every *other* row must still be there.
        let mut expected: Vec<u64> = (1..=6u64).map(|i| i * 10).collect();
        if order == Order::Desc {
            expected.reverse();
        }
        expected.retain(|t| *t != cursor_ts || seen.contains(t));
        assert_eq!(
            seen, expected,
            "{order:?} scan lost a row after its cursor row was deleted"
        );
    }
    Ok(())
}

/// `IndexEntry` sorts on a 2500-byte prefix and breaks ties on
/// `sha256(full_key)`, which is uncorrelated with the byte order the iterator
/// produces. Emitting storage order would both violate the contract and make
/// the `entry > cursor` resume filter discard entries the scan never revisits.
#[tokio::test]
async fn load_index_chunk_orders_keys_that_share_a_truncated_prefix() -> anyhow::Result<()> {
    use common::index::MAX_INDEX_KEY_PREFIX_LEN;
    use value::sha256::Sha256;

    let f = Fixture::new()?;
    // Two keys agreeing on the whole truncated prefix, differing past it. The
    // suffixes are not arbitrary: `IndexEntry` breaks the tie on
    // `sha256(full_key)`, so the pair has to be one where sha256 order
    // *disagrees* with byte order. Suffixes 1 and 2 do not (this test was
    // originally written with them and passed vacuously); 1 and 3 do.
    let mut a = vec![b'z'; MAX_INDEX_KEY_PREFIX_LEN];
    let mut b = a.clone();
    a.push(1);
    b.push(3);
    assert!(a < b, "a is the byte-order-smaller key");
    assert!(
        Sha256::hash(&a).as_ref() > Sha256::hash(&b).as_ref(),
        "the point of this test is a pair whose IndexEntry order is the reverse of storage order; \
         if this ever stops holding, pick different suffixes rather than deleting the assertion",
    );

    for (i, key) in [a.clone(), b.clone()].into_iter().enumerate() {
        let n = i as u32 + 1;
        f.persistence
            .write(
                &[entry(1, n, 10, "v", None)?],
                &[index_entry(1, &key, 10, Some(doc_id(1, n)))],
                ConflictStrategy::Error,
            )
            .await?;
    }

    let all = f.persistence.load_index_chunk(None, 16).await?;
    assert_eq!(all.len(), 2, "both entries are in the index");
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted, "entries must be emitted in IndexEntry order");

    // Page one entry at a time. The entry that sorts second in `IndexEntry`
    // order sits *first* in storage, so resuming at the cursor's own storage
    // position would start past it and it would never be emitted.
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let chunk = f.persistence.load_index_chunk(cursor.clone(), 1).await?;
        let Some(entry) = chunk.into_iter().next() else {
            break;
        };
        cursor = Some(entry.clone());
        paged.push(entry);
        assert!(paged.len() <= 4, "paging is not terminating");
    }
    assert_eq!(
        paged, sorted,
        "paging one at a time must yield every entry, in IndexEntry order"
    );
    Ok(())
}

/// A rehearsal exists to catch a backup that restores but is wrong. A backup of
/// an empty database restores and scans perfectly, so an empty read-back is the
/// one result that must not be reported as success.
#[tokio::test]
async fn rehearse_refuses_an_empty_database() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let backup_dir = dir.path().join("backup");
    {
        // Opened and backed up without ever being written to.
        let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
        persistence.backup(&backup_dir, 4)?;
        persistence.shutdown().await?;
    }
    let err = backup::rehearse(&backup_dir, &dir.path().join("scratch"), None)
        .expect_err("an empty restore must not pass as a rehearsal");
    assert!(format!("{err:#}").contains("empty"), "{err:#}");
    Ok(())
}

/// RocksDB defines no behaviour for two backup engines on one directory —
/// its own header says the result may include trashing the directory — and
/// `purge_old_backups` runs on every scheduled backup, so an operator listing
/// generations while the worker prunes is a real sequence.
#[tokio::test]
async fn backup_directory_refuses_concurrent_use() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let backup_dir = dir.path().join("backup");
    let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
    persistence
        .write(
            &[entry(1, 1, 10, "v1", None)?],
            &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
            ConflictStrategy::Error,
        )
        .await?;
    persistence.backup(&backup_dir, 4)?;

    // Hold the lock the way a running operation does, then contend for it.
    let held = backup::testing::lock_backup_dir(&backup_dir)?;
    let err = backup::list(&backup_dir).expect_err("a second holder must be refused");
    assert!(format!("{err:#}").contains("another process"), "{err:#}");
    drop(held);

    // Released, so the directory is usable again.
    assert_eq!(backup::list(&backup_dir)?.len(), 1);
    Ok(())
}

/// Retention can move under a scan. Every reader must consult the validator
/// *after* reading a page and *before* handing it to the caller, so a snapshot
/// that fell out of retention mid-scan is never returned. Every other test uses
/// a validator that always approves, which cannot show this.
#[tokio::test]
async fn a_rejecting_retention_validator_stops_every_read_path() -> anyhow::Result<()> {
    struct Rejecting;
    #[async_trait::async_trait]
    impl RetentionValidator for Rejecting {
        fn optimistic_validate_snapshot(&self, _ts: Timestamp) -> anyhow::Result<()> {
            Ok(())
        }

        async fn validate_snapshot(&self, _ts: Timestamp) -> anyhow::Result<()> {
            anyhow::bail!("snapshot is out of retention")
        }

        async fn validate_document_snapshot(&self, _ts: Timestamp) -> anyhow::Result<()> {
            anyhow::bail!("document snapshot is out of retention")
        }

        async fn min_snapshot_ts(&self) -> anyhow::Result<common::types::RepeatableTimestamp> {
            anyhow::bail!("unused")
        }

        async fn min_document_snapshot_ts(
            &self,
        ) -> anyhow::Result<common::types::RepeatableTimestamp> {
            anyhow::bail!("unused")
        }
    }

    let f = Fixture::new()?;
    let id = doc_id(1, 1);
    f.persistence
        .write(
            &[entry(1, 1, 10, "v1", None)?],
            &[index_entry(1, b"k", 10, Some(id))],
            ConflictStrategy::Error,
        )
        .await?;

    let rejecting: Arc<dyn RetentionValidator> = Arc::new(Rejecting);
    let reader = f.persistence.reader();

    let documents: anyhow::Result<Vec<_>> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 8, rejecting.clone())
        .try_collect()
        .await;
    assert!(documents.is_err(), "load_documents yielded a rejected page");

    let rows: anyhow::Result<Vec<_>> = reader
        .index_scan(
            index_id(1),
            tablet(1),
            ts(10),
            &interval(b"", None),
            Order::Asc,
            8,
            rejecting.clone(),
        )
        .try_collect()
        .await;
    assert!(rows.is_err(), "index_scan yielded a rejected page");

    let previous = reader
        .previous_revisions(BTreeSet::from([(id, ts(10))]), rejecting.clone())
        .await;
    assert!(
        previous.is_err(),
        "previous_revisions yielded a rejected read"
    );
    Ok(())
}

/// A rehearsal must never delete anything at the path an operator named. The
/// earlier design marked a scratch directory as "mine to clear", which is a
/// permanent deletion grant: a directory that hosted a rehearsal once is a
/// directory the command will empty forever after, live database and all.
/// It now works inside a directory it names itself and removes only that.
#[tokio::test]
async fn rehearse_leaves_everything_at_the_scratch_path_alone() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let backup_dir = dir.path().join("backup");
    {
        let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
        persistence
            .write(
                &[entry(1, 1, 10, "v1", None)?],
                &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
                ConflictStrategy::Error,
            )
            .await?;
        persistence.backup(&backup_dir, 4)?;
        persistence.shutdown().await?;
    }

    // Stand in for a data volume: a live-looking database and a user file.
    let volume = dir.path().join("volume");
    std::fs::create_dir_all(volume.join("db"))?;
    std::fs::create_dir_all(volume.join("file_storage"))?;
    std::fs::write(volume.join("db/CURRENT"), "live")?;
    std::fs::write(volume.join("file_storage/blob1"), "user upload")?;

    // Rehearse into it twice — the second run is the one the old marker scheme
    // turned into a deletion.
    backup::rehearse(&backup_dir, &volume, None)?;
    backup::rehearse(&backup_dir, &volume, None)?;

    assert_eq!(
        std::fs::read_to_string(volume.join("db/CURRENT"))?,
        "live",
        "the rehearsal must not have touched the database at that path"
    );
    assert_eq!(
        std::fs::read_to_string(volume.join("file_storage/blob1"))?,
        "user upload",
        "nor anything else under it"
    );
    // And it cleans up after itself rather than accumulating.
    let leftovers: Vec<_> = std::fs::read_dir(&volume)?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("convex-rehearsal-"))
        .collect();
    assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
    Ok(())
}

/// `is_fresh` decides whether the caller runs `Database::initialize`. RocksDB
/// writes `CURRENT` as the *last* step of creating a database, before Convex
/// has written a row, so deriving freshness from the directory means a process
/// killed anywhere inside initialization reopens to "not fresh" and an empty
/// database — skipping initialization forever and failing on the missing
/// bootstrap tables, with no recovery but deleting the volume.
#[tokio::test]
async fn an_empty_database_is_still_fresh_after_a_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    {
        // Created, then killed before writing anything — an OOM or an eviction
        // partway through initialization.
        let persistence = RocksDbPersistence::new(&path)?;
        assert!(persistence.is_fresh());
    }
    assert!(path.join("CURRENT").exists(), "the directory now exists");

    let reopened = RocksDbPersistence::new(&path)?;
    assert!(
        reopened.is_fresh(),
        "an empty database must still be fresh, or initialization never runs"
    );

    // One row is enough to stop being fresh.
    reopened
        .write(
            &[entry(1, 1, 10, "v1", None)?],
            &[],
            ConflictStrategy::Error,
        )
        .await?;
    reopened.shutdown().await?;
    drop(reopened);
    assert!(
        !RocksDbPersistence::new(&path)?.is_fresh(),
        "a database with data is not fresh"
    );
    Ok(())
}

/// Two databases sharing one backup directory interleave their generations,
/// so retention prunes the wrong ones and a restore returns whichever wrote
/// last. Silent until someone restores, so it is refused at backup time.
#[tokio::test]
async fn a_backup_directory_belongs_to_one_database() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let backup_dir = dir.path().join("backup");

    let first = RocksDbPersistence::new(&dir.path().join("a"))?;
    first
        .write(&[entry(1, 1, 10, "a", None)?], &[], ConflictStrategy::Error)
        .await?;
    first.backup(&backup_dir, 4)?;

    let second = RocksDbPersistence::new(&dir.path().join("b"))?;
    second
        .write(&[entry(1, 2, 20, "b", None)?], &[], ConflictStrategy::Error)
        .await?;
    let err = second
        .backup(&backup_dir, 4)
        .expect_err("a second database must not write into the same chain");
    assert!(format!("{err:#}").contains("different database"), "{err:#}");

    // The first database is still free to keep using it.
    first.backup(&backup_dir, 4)?;
    assert_eq!(backup::list(&backup_dir)?.len(), 2);
    Ok(())
}

/// A key whose newest version at the read timestamp is a tombstone advances the
/// scan without producing a row, so a page bounded only by rows could walk an
/// entire index in one blocking call. The budget that prevents that must still
/// resume correctly — stopping early with an empty page and no cursor would
/// report a scan as finished when it was not.
#[tokio::test]
async fn index_scan_over_many_tombstones_makes_progress_and_resumes() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    // 400 keys, every one deleted at ts 20, then one live key past them all.
    for i in 0..400u32 {
        let key = format!("k{i:04}");
        f.persistence
            .write(
                &[entry(1, i, 10, "v", None)?],
                &[index_entry(1, key.as_bytes(), 10, Some(doc_id(1, i)))],
                ConflictStrategy::Error,
            )
            .await?;
        f.persistence
            .write(
                &[],
                &[index_entry(1, key.as_bytes(), 20, None)],
                ConflictStrategy::Error,
            )
            .await?;
    }
    f.persistence
        .write(
            &[entry(1, 9999, 10, "live", None)?],
            &[index_entry(1, b"zzzz", 10, Some(doc_id(1, 9999)))],
            ConflictStrategy::Error,
        )
        .await?;

    // One row requested, so the budget is small — the scan must still reach the
    // live key past 400 tombstones rather than reporting an empty result.
    let rows: Vec<_> = f
        .persistence
        .reader()
        .index_scan(
            index_id(1),
            tablet(1),
            ts(20),
            &interval(b"", None),
            Order::Asc,
            1,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "the live key past the tombstones must be found"
    );
    assert_eq!(rows[0].0 .0, b"zzzz".to_vec());
    Ok(())
}

/// A secondary instance is the only way to read a database another process has
/// open, and RocksDB rejects `ReadOptions::snapshot` on one outright. Every
/// paged read has to work through it, which nothing checked before.
#[tokio::test]
async fn a_secondary_instance_can_actually_read() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let primary = RocksDbPersistence::new(&path)?;
    let id = doc_id(1, 1);
    primary
        .write(
            &[entry(1, 1, 10, "v1", None)?],
            &[index_entry(1, b"k", 10, Some(id))],
            ConflictStrategy::Error,
        )
        .await?;

    let secondary = RocksDbPersistence::new_secondary_in(&path, &dir.path().join("secondary"))?;
    let reader = secondary.reader();

    let documents: Vec<_> = reader
        .load_documents(TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(
        documents.len(),
        1,
        "load_documents must work on a secondary"
    );

    let from_table: Vec<_> = reader
        .load_documents_from_table(tablet(1), TimestampRange::all(), Order::Asc, 8, validator())
        .try_collect()
        .await?;
    assert_eq!(from_table.len(), 1, "load_documents_from_table must work");

    let rows: Vec<_> = reader
        .index_scan(
            index_id(1),
            tablet(1),
            ts(10),
            &interval(b"", None),
            Order::Asc,
            8,
            validator(),
        )
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 1, "index_scan must work on a secondary");

    let previous = reader
        .previous_revisions(BTreeSet::from([(id, ts(10))]), validator())
        .await?;
    assert!(previous.is_empty(), "no earlier revision exists");

    assert_eq!(reader.max_ts().await?, Some(ts(10)));
    Ok(())
}

/// A restored database has to be able to keep backing up into the chain it came
/// from. RocksDB's own `IDENTITY` is not carried by a backup, so anything keyed
/// on it locks the restored database out at exactly the moment it is
/// recovering.
#[tokio::test]
async fn a_restored_database_can_continue_its_backup_chain() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let backup_dir = dir.path().join("backup");
    {
        let original = RocksDbPersistence::new(&dir.path().join("original"))?;
        original
            .write(
                &[entry(1, 1, 10, "v1", None)?],
                &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
                ConflictStrategy::Error,
            )
            .await?;
        original.backup(&backup_dir, 8)?;
        original.shutdown().await?;
    }

    let restored_path = dir.path().join("restored");
    backup::restore(&backup_dir, &restored_path, None)?;
    let restored = RocksDbPersistence::new(&restored_path)?;
    restored
        .write(
            &[entry(1, 2, 20, "v2", None)?],
            &[index_entry(1, b"k2", 20, Some(doc_id(1, 2)))],
            ConflictStrategy::Error,
        )
        .await?;

    // The whole point: this must not be refused as "a different database".
    restored.backup(&backup_dir, 8)?;
    assert_eq!(
        backup::list(&backup_dir)?.len(),
        2,
        "the restored database must extend its own chain"
    );
    Ok(())
}

/// The environment resolves to exactly one mode, and an explicit interval wins
/// over `ROCKSDB_SYNC_WRITES` rather than the two silently fighting.
#[test]
fn sync_mode_sync_each_write_only_in_every() {
    assert!(SyncMode::Every.sync_each_write());
    assert!(!SyncMode::Never.sync_each_write());
}

#[tokio::test]
async fn empty_writes_are_accepted() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    f.write(&[], &[]).await?;
    assert!(f.log(TimestampRange::all(), Order::Asc).await?.is_empty());
    Ok(())
}

/// `Persistence::shutdown` must be idempotent, because the trait's default is:
/// the relational backends inherit a no-op. `cancel_all_background_work` sets
/// RocksDB's `shutting_down_` flag, after which every `flush_cf` fails with
/// `ShutdownInProgress` — so a deployment wiring SIGTERM to `shutdown()`
/// alongside an existing teardown path used to get an error on the second call.
#[tokio::test]
async fn shutdown_is_idempotent() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;
    persistence.shutdown().await?;
    persistence
        .shutdown()
        .await
        .expect("a second shutdown must succeed");
    persistence
        .shutdown()
        .await
        .expect("and a third, for good measure");
    Ok(())
}

/// The idempotence guard latches *before* the work it guards, so a shutdown
/// that fails part-way is reported as a success by every later call — while
/// nothing was ever flushed. A caller that retries a failed shutdown (the only
/// sensible response) is told the log is on disk when it is not.
#[tokio::test]
async fn a_failed_shutdown_must_not_report_success_on_retry() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let persistence = RocksDbPersistence::new(&dir.path().join("db"))?;

    // Put the engine in the state the commit message describes: once
    // `shutting_down_` is set, every `flush_cf` returns ShutdownInProgress.
    // Standing in for any mid-shutdown failure — ENOSPC on the WAL fsync is
    // the one that matters in production.
    persistence.inner.db.cancel_all_background_work(true);

    let first = persistence.shutdown().await;
    assert!(
        first.is_err(),
        "precondition: this shutdown must fail part-way"
    );

    let second = persistence.shutdown().await;
    assert!(
        second.is_err(),
        "a retry after a failed shutdown must not report success: the guard latched before the \
         flush, so this returns Ok having flushed nothing"
    );
    Ok(())
}

/// The escalation must fire when the engine stops accepting writes, and must
/// not fire for writes that merely fail.
///
/// This had no coverage at all, and the version it replaced could not have
/// fired: it gated on `rocksdb.background-errors`, which RocksDB bumps only on
/// a failed background flush or compaction. On ENOSPC the failing write never
/// reaches the memtable, so no flush is scheduled and that counter stays zero
/// while every write fails forever — measured on a full tmpfs at 20 rounds with
/// no signal raised.
///
/// Driven through `Inner::engine_write` with a synthetic failure rather than by
/// filling a real volume, so it runs anywhere and asserts the policy rather
/// than the operating system.
#[tokio::test]
async fn repeated_engine_write_failures_escalate() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let persistence = RocksDbPersistence::open_with(
        &dir.path().join("db"),
        OpenOptions {
            sync: None,
            shutdown: Some(ShutdownSignal::new(tx)),
        },
    )?;
    let inner = &persistence.inner;

    let fail = || {
        inner.engine_write("a synthetic failure", || {
            // A real engine error, produced without needing a full volume:
            // opening a checkpoint at a path that already exists fails.
            Err::<(), _>(synthetic_engine_error())
        })
    };

    // Below the threshold: a write that fails is not a database that has
    // stopped accepting writes.
    for _ in 0..(*options::WRITE_FAILURES_TO_ESCALATE - 1) {
        assert!(fail().is_err());
    }
    assert!(
        rx.try_recv().is_err(),
        "a run of failures below the threshold must not stop the backend"
    );

    // A success clears the run, so the count is consecutive rather than
    // cumulative — the distinction that keeps a transient error from ever
    // accumulating into a shutdown.
    let globals = inner.cf(CF_GLOBALS)?;
    inner.engine_write("a real write", || {
        inner.db.put_cf(&globals, b"escalation-test", b"1")
    })?;
    for _ in 0..(*options::WRITE_FAILURES_TO_ESCALATE - 1) {
        assert!(fail().is_err());
    }
    assert!(
        rx.try_recv().is_err(),
        "a success must reset the run, so this second partial run must not escalate either"
    );

    // And over the threshold, it stops.
    for _ in 0..*options::WRITE_FAILURES_TO_ESCALATE {
        assert!(fail().is_err());
    }
    let reported = rx
        .try_recv()
        .expect("the backend must stop once the engine is refusing writes");
    assert!(
        reported.to_string().contains("refusing writes"),
        "unexpected escalation message: {reported}"
    );
    Ok(())
}

/// A genuine , for tests that need a failing engine write
/// without a failing device. `rocksdb::Error::new` is private to the binding.
fn synthetic_engine_error() -> rocksdb::Error {
    rocksdb::DB::destroy(
        &rocksdb::Options::default(),
        "/proc/self/definitely-not-a-database",
    )
    .expect_err("destroying a database under /proc must fail")
}

/// A backup taken while the primary is live must contain every acknowledged
/// write, including ones the primary has never flushed.
///
/// This is the supported way to back up a running deployment, so it is worth
/// stating what makes it work. RocksDB allows one writer, so a scheduled job
/// cannot open the data directory read-write while the backend holds it. A
/// secondary opens without the lock and tails the primary's write-ahead log —
/// and under `SyncMode::Every` an acknowledged write is in that log before
/// `write` returns. So the recovery point is the moment of the backup, not the
/// moment the secondary opened, and not the last flush.
///
/// The writes below are deliberately never flushed: they exist only in the
/// primary's memtable and its WAL when the backup is taken. Restoring through
/// the real `restore` path — whose `RestoreOptions` differ from a hand-rolled
/// one — is the point of the test.
#[tokio::test]
async fn a_backup_of_a_live_primary_captures_unflushed_writes() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let backup_dir = dir.path().join("backups");
    let primary = RocksDbPersistence::new(&db_path)?;

    let batch = |from: u32, n: u32| -> anyhow::Result<Vec<DocumentLogEntry>> {
        (from..from + n)
            .map(|i| {
                Ok(DocumentLogEntry {
                    ts: ts(u64::from(i) + 1),
                    id: doc_id(1, i),
                    value: Some(document(1, i, "body")?),
                    prev_ts: None,
                })
            })
            .collect()
    };

    primary
        .write(&batch(0, 100)?, &[], ConflictStrategy::Error)
        .await?;

    // Opened before the rest of the writes, as a long-running sidecar would be.
    let secondary = RocksDbPersistence::new_secondary(&db_path)?;

    // Acknowledged after the secondary opened, and never flushed.
    primary
        .write(&batch(100, 100)?, &[], ConflictStrategy::Error)
        .await?;

    // The whole operation a CronJob performs.
    secondary.backup(&backup_dir, 3)?;

    let restored_dir = dir.path().join("restored");
    backup::restore(&backup_dir, &restored_dir, None)?;
    let restored = RocksDbPersistence::new(&restored_dir)?;
    let check = restored.inner.verify_readable()?;
    assert_eq!(
        check.documents, 200,
        "a live backup must include writes the primary acknowledged but never flushed; got {} of \
         200",
        check.documents
    );
    Ok(())
}

/// `SyncMode::Never` must still survive a crash of *this* process.
///
/// The mode trades away host loss, not process loss: RocksDB still writes
/// through to the page cache on every write, so only the machine going down
/// loses data. Nothing tested that, and `new_with_sync_mode` — the only way to
/// state the mode in code rather than hope an environment variable was set —
/// had no callers at all.
#[tokio::test]
async fn never_sync_survives_a_process_crash() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    {
        let persistence = RocksDbPersistence::new_with_sync_mode(&path, SyncMode::Never)?;
        persistence
            .write(
                &[entry(1, 1, 10, "v1", None)?],
                &[index_entry(1, b"k", 10, Some(doc_id(1, 1)))],
                ConflictStrategy::Error,
            )
            .await?;
        // Deliberately no shutdown: dropping the handle is the closest thing to
        // a teardown that a killed process gets.
    }
    let reopened = RocksDbPersistence::new(&path)?;
    let check = reopened.inner.verify_readable()?;
    assert_eq!(
        check.documents, 1,
        "a write acknowledged under `Never` must survive reopening the database"
    );
    assert_eq!(check.index_entries, 1);
    Ok(())
}
