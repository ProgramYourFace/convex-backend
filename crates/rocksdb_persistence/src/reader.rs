//! The read side: paged scans over the column families described in
//! [`crate::keys`].
//!
//! Every scan is paged. A page is fetched on a blocking thread, the retention
//! validator is consulted, and only then are its rows yielded — the same order
//! the Postgres backend uses, so a snapshot that falls out of retention
//! mid-scan is never handed to the caller.
//!
//! Paging holds no engine snapshot, so rows can be deleted between pages —
//! document retention does it continuously, and `delete_tablet_documents` does
//! it on a table drop with no reference to the retention window at all. What
//! makes that safe is not that deletion cannot happen but that **every cursor
//! is a value, not a position**: a resume seeks to the cursor's key and steps
//! past it only if the seek actually landed on it. A seek-and-step would skip
//! the row *after* a deleted cursor row, which is a revision silently missing
//! from an export or an index update silently missing from a backfill. The
//! relational backends get the same property from `(ts, table_id, id) > (…)`.
//!
//! Concurrent commits land at higher timestamps, outside every range being
//! scanned, so they cannot appear mid-page.

use std::{
    cmp,
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use common::{
    index::{
        IndexEntry,
        IndexKeyBytes,
    },
    interval::{
        End,
        Interval,
    },
    persistence::{
        DocumentLogEntry,
        DocumentPrevTsQuery,
        DocumentStream,
        IndexStream,
        LatestDocument,
        PersistenceGlobalKey,
        PersistenceReader,
        RetentionValidator,
        TimestampRange,
    },
    query::Order,
    runtime::{
        tokio_spawn_blocking,
        CoopStreamExt,
    },
    types::{
        IndexId,
        PersistenceVersion,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        TabletId,
    },
};
use futures::StreamExt;
use futures_async_stream::try_stream;
use serde::Deserialize as _;
use serde_json::Value as JsonValue;

use crate::{
    codec,
    keys::{
        self,
        CF_DLOG,
        CF_DOCS,
        CF_DTAB,
        CF_GLOBALS,
        CF_IDX,
    },
    metrics,
    multi_get_documents,
    options,
    Inner,
    RocksDbPersistence,
};

/// One page of a document-log scan.
struct DocumentPage {
    entries: Vec<DocumentLogEntry>,
    /// Storage key of the last row, to resume from. `None` when the scan is
    /// exhausted.
    cursor: Option<Vec<u8>>,
}

/// One page of an index scan.
struct IndexPage {
    rows: Vec<(IndexKeyBytes, LatestDocument)>,
    /// `index_id ‖ esc(key)` of the last key resolved, to resume past.
    cursor: Option<Vec<u8>>,
}

/// Ceiling on how many index entries may share one truncated key prefix before
/// [`Inner::load_index_chunk`] gives up. Reaching it needs many distinct index
/// keys that agree on their first `MAX_INDEX_KEY_PREFIX_LEN` bytes, each with
/// its own version run. Failing loudly beats buffering without bound.
const MAX_INDEX_ENTRY_GROUP: usize = 65_536;

/// How many index keys one page may examine per row it is asked to produce.
///
/// Tombstoned keys advance the iterator without producing a row, so without a
/// ceiling a page over a mass-deleted table is a scan of the whole index on one
/// blocking thread. Generous enough that ordinary tombstone density never
/// truncates a page early; the cursor makes an early return correct anyway.
const KEYS_PER_ROW_BUDGET: usize = 64;

/// `ReadOptions` pinned to `snapshot`, for resolving document bodies at the
/// same point in time the scan's iterator is walking.
///
/// `None` — a secondary instance, which RocksDB does not let take snapshots —
/// yields plain options. See [`crate::Inner::read_snapshot`].
fn snapshot_read_opts(
    snapshot: Option<&rocksdb::SnapshotWithThreadMode<'_, crate::Db>>,
) -> rocksdb::ReadOptions {
    let mut opts = rocksdb::ReadOptions::default();
    if let Some(snapshot) = snapshot {
        opts.set_snapshot(snapshot);
    }
    opts
}

impl Inner {
    /// Read a page of the document log, ordered by timestamp.
    ///
    /// Without a tablet the scan walks `dlog`, whose values are the documents
    /// themselves, so nothing extra is fetched. With a tablet it walks `dtab`
    /// — the per-table timestamp index — and resolves the bodies in one
    /// batched lookup.
    fn document_page(
        &self,
        tablet_id: Option<TabletId>,
        range: TimestampRange,
        order: Order,
        limit: usize,
        cursor: Option<Vec<u8>>,
    ) -> anyhow::Result<DocumentPage> {
        self.refresh()?;
        let (cf_name, lower, upper) = match tablet_id {
            None => (
                CF_DLOG,
                keys::dlog_ts_lower(range.min_timestamp_inclusive()).to_vec(),
                keys::dlog_ts_upper(range.max_timestamp_exclusive()).to_vec(),
            ),
            Some(tablet) => {
                let (lo, hi) = keys::dtab_bounds(
                    tablet,
                    range.min_timestamp_inclusive(),
                    range.max_timestamp_exclusive(),
                );
                (CF_DTAB, lo, hi)
            },
        };
        let cf = self.cf(cf_name)?;

        // One snapshot for both phases of this page: the keys the iterator
        // walks, and the bodies resolved for them afterwards. See
        // `multi_get_documents` for what reading them at two different points
        // in time does.
        let snapshot = self.read_snapshot();
        let mut read_opts = snapshot_read_opts(snapshot.as_ref());
        read_opts.set_iterate_lower_bound(lower.clone());
        read_opts.set_iterate_upper_bound(upper);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);

        match (&cursor, order) {
            // Step past the cursor row only when the seek actually landed on
            // it. A seek is a position, not a value: if the cursor row was
            // deleted between pages — which document retention and
            // `delete_tablet_documents` both do, concurrently and without
            // reference to this scan — the seek lands on the *next* row
            // instead, and stepping again would skip it silently. Comparing
            // against the cursor makes the resume value-based, which is what
            // the relational backends get from `(ts, table_id, id) > (…)`.
            (Some(cursor), Order::Asc) => {
                iter.seek(cursor);
                if iter.valid() && iter.key() == Some(cursor.as_slice()) {
                    iter.next();
                }
            },
            (Some(cursor), Order::Desc) => {
                iter.seek_for_prev(cursor);
                if iter.valid() && iter.key() == Some(cursor.as_slice()) {
                    iter.prev();
                }
            },
            (None, Order::Asc) => iter.seek(&lower),
            (None, Order::Desc) => iter.seek_to_last(),
        }

        let mut coordinates = Vec::with_capacity(limit);
        let mut inline = Vec::with_capacity(if tablet_id.is_none() { limit } else { 0 });
        let mut last_key = None;
        while iter.valid() && coordinates.len() < limit {
            let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
                break;
            };
            let (ts, id) = match tablet_id {
                None => keys::parse_dlog_key(key)?,
                Some(_) => keys::parse_dtab_key(key)?,
            };
            if tablet_id.is_none() {
                let decoded = codec::decode_document(id.table(), value)
                    .with_context(|| format!("failed to decode document {id} at {ts}"))?;
                inline.push(DocumentLogEntry {
                    ts,
                    id,
                    value: decoded.value,
                    prev_ts: decoded.prev_ts,
                });
            }
            coordinates.push((ts, id));
            last_key = Some(key.to_vec());
            match order {
                Order::Asc => iter.next(),
                Order::Desc => iter.prev(),
            }
        }
        iter.status()?;

        let entries = if tablet_id.is_none() {
            inline
        } else {
            let bodies =
                multi_get_documents(self, &coordinates, &snapshot_read_opts(snapshot.as_ref()))?;
            coordinates
                .iter()
                .map(|(ts, id)| {
                    let (value, prev_ts) = bodies.get(&(*ts, *id)).cloned().ok_or_else(|| {
                        anyhow::anyhow!("document log entry for {id} at {ts} is missing its body")
                    })?;
                    Ok(DocumentLogEntry {
                        ts: *ts,
                        id: *id,
                        value,
                        prev_ts,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        };

        // A short page means the range is exhausted.
        let cursor = if entries.len() < limit {
            None
        } else {
            last_key
        };
        Ok(DocumentPage { entries, cursor })
    }

    /// Read a page of an index scan: for each distinct key in the interval, the
    /// newest version at or before `read_timestamp`, skipping tombstones.
    ///
    /// Versions of a key sort newest-first, so resolving one key is a single
    /// seek to `key ‖ !read_timestamp` — the relational backends need a
    /// `DISTINCT ON` with a sort for the same answer.
    fn index_page(
        &self,
        index_id: IndexId,
        tablet_id: TabletId,
        read_timestamp: Timestamp,
        interval: &Interval,
        order: Order,
        limit: usize,
        cursor: Option<Vec<u8>>,
    ) -> anyhow::Result<IndexPage> {
        self.refresh()?;
        let idx = self.cf(CF_IDX)?;
        let lower = keys::idx_lower_bound(index_id, &interval.start.0);
        let upper = match &interval.end {
            End::Excluded(end) => keys::idx_upper_bound(index_id, Some(&end[..])),
            End::Unbounded => keys::idx_upper_bound(index_id, None),
        };

        // One snapshot for the index walk and the body resolution that follows
        // it — see `multi_get_documents`.
        let snapshot = self.read_snapshot();
        let mut read_opts = snapshot_read_opts(snapshot.as_ref());
        read_opts.set_iterate_lower_bound(lower.clone());
        if !upper.is_empty() {
            read_opts.set_iterate_upper_bound(upper.clone());
        }
        let mut iter = self.db.raw_iterator_cf_opt(&idx, read_opts);

        match (&cursor, order) {
            // The cursor is a whole key's prefix, and every version of it has
            // already been considered, so resume past the run entirely.
            (Some(cursor), Order::Asc) => iter.seek(keys::successor(cursor)),
            (Some(cursor), Order::Desc) => iter.seek_for_prev(cursor),
            (None, Order::Asc) => iter.seek(&lower),
            (None, Order::Desc) => iter.seek_to_last(),
        }

        let mut resolved: Vec<(IndexKeyBytes, Timestamp, InternalDocumentId)> =
            Vec::with_capacity(limit);
        let mut last_prefix = None;
        // Bounded by keys *examined* as well as rows produced. A key whose
        // newest version at `read_timestamp` is a tombstone advances the
        // iterator without producing a row, so a limit on rows alone lets one
        // page walk an entire index — a table that has just been mass-deleted
        // is exactly that shape. That would hold a blocking thread for the
        // length of the whole scan and skip the retention validator for all of
        // it. The relational backends batch on rows fetched, deletes included.
        let mut examined = 0;
        while iter.valid()
            && resolved.len() < limit
            && examined < limit.saturating_mul(KEYS_PER_ROW_BUDGET)
        {
            let Some(key) = iter.key() else { break };
            examined += 1;
            anyhow::ensure!(
                key.len() > keys::TS_LEN,
                "malformed index key of {} bytes",
                key.len()
            );
            let prefix = key[..key.len() - keys::TS_LEN].to_vec();

            // Jump straight to this key's newest version at or before the read
            // timestamp; versions newer than it sort ahead and are skipped.
            iter.seek(keys::idx_seek_at_prefix(&prefix, read_timestamp));
            if iter.valid()
                && let (Some(key), Some(value)) = (iter.key(), iter.value())
                && key.starts_with(&prefix)
            {
                let (found_index_id, index_key, ts) = keys::parse_idx_key(key)?;
                anyhow::ensure!(
                    found_index_id == index_id,
                    "index scan crossed from {index_id:?} into {found_index_id:?}",
                );
                debug_assert!(ts <= read_timestamp);
                // A tombstone means the key is absent at this snapshot, which
                // is not an error — it is simply not part of the result.
                if !codec::index_entry_is_deleted(value) {
                    let document_id = codec::decode_index_entry(value)?.ok_or_else(|| {
                        anyhow::anyhow!("index entry claimed a value but carried none")
                    })?;
                    anyhow::ensure!(
                        document_id.table() == tablet_id,
                        "index {index_id:?} on tablet {tablet_id:?} points at {document_id}",
                    );
                    resolved.push((index_key, ts, document_id));
                }
            }
            last_prefix = Some(prefix.clone());

            // Move to the next distinct key, whichever direction we are going.
            match order {
                Order::Asc => iter.seek(keys::successor(&prefix)),
                Order::Desc => iter.seek_for_prev(&prefix),
            }
        }
        iter.status()?;

        let coordinates: Vec<_> = resolved.iter().map(|(_, ts, id)| (*ts, *id)).collect();
        let bodies =
            multi_get_documents(self, &coordinates, &snapshot_read_opts(snapshot.as_ref()))?;
        let mut rows = Vec::with_capacity(resolved.len());
        for (index_key, ts, document_id) in resolved {
            let (value, prev_ts) = bodies.get(&(ts, document_id)).cloned().ok_or_else(|| {
                anyhow::anyhow!("dangling index reference to {document_id} at {ts}")
            })?;
            let value = value.ok_or_else(|| {
                anyhow::anyhow!("index reference to deleted document {document_id} at {ts}")
            })?;
            rows.push((index_key, LatestDocument { ts, value, prev_ts }));
        }

        // Whether to resume is "did the iterator run out", not "did the page
        // fill". Those coincided while rows were the only bound; with the
        // examined-key budget above, a page can stop early with room to spare,
        // and reporting `None` there would tell the caller the scan was
        // finished when it was not.
        let cursor = iter.valid().then_some(last_prefix).flatten();
        Ok(IndexPage { rows, cursor })
    }

    pub(crate) fn load_index_chunk(
        &self,
        cursor: Option<IndexEntry>,
        chunk_size: usize,
    ) -> anyhow::Result<Vec<IndexEntry>> {
        self.refresh()?;
        let idx = self.cf(CF_IDX)?;
        let mut iter = self.db.raw_iterator_cf(&idx);
        match &cursor {
            // Resume at the start of the cursor's *prefix group*, not at the
            // cursor's own key.
            //
            // Within a group, `IndexEntry` order is `sha256(full_key)` order,
            // which has no relationship to storage order. So an entry that
            // belongs after the cursor in `IndexEntry` order — and therefore
            // still owes the caller an appearance — can sit *before* the
            // cursor's key in storage. Seeking to the cursor's own key would
            // start past it and it would never be emitted at all. Seeking to
            // the group start rescans the group and the `entry > cursor` filter
            // below picks up exactly what is still owed.
            //
            // `idx_lower_bound` on the truncated prefix is at or before every
            // key in the group, so at worst this rescans a little more than
            // needed; the filter makes that free of duplicates.
            Some(entry) => iter.seek(keys::idx_lower_bound(entry.index_id, &entry.key_prefix)),
            None => iter.seek_to_first(),
        }

        // This method's contract is `IndexEntry` order, which is *not* storage
        // order. `IndexEntry` sorts by `key_prefix` — only the first
        // `MAX_INDEX_KEY_PREFIX_LEN` bytes — and breaks ties on
        // `sha256(full_key)`, which has no relationship to the byte order the
        // iterator produces. The two agree for every key short enough to be its
        // own prefix, and disagree for keys that share a truncated prefix.
        //
        // So entries are grouped by `key_prefix`: across groups, storage order
        // already is `IndexEntry` order, and within a group the whole group is
        // sorted. That keeps the buffer bounded by the number of entries
        // sharing one truncated prefix rather than by the size of the index,
        // and it is also what makes the cursor filter below sound — filtering
        // `entry > cursor` against a stream that is not in `IndexEntry` order
        // discards entries the scan will never come back for.
        let mut out = Vec::with_capacity(chunk_size);
        let mut group: Vec<IndexEntry> = Vec::new();
        let mut group_prefix: Option<Vec<u8>> = None;
        // Draining stops as soon as `out` is full, and whatever is left in the
        // group is simply dropped — which is correct only because the resume
        // above rescans the whole group. Emitting a prefix of the sorted group
        // and letting the next call re-derive the rest is what keeps the two
        // halves consistent.
        let flush = |group: &mut Vec<IndexEntry>, out: &mut Vec<IndexEntry>| {
            group.sort();
            for entry in group.drain(..) {
                if out.len() >= chunk_size {
                    break;
                }
                if cursor.as_ref().is_none_or(|cursor| entry > *cursor) {
                    out.push(entry);
                }
            }
        };

        while iter.valid() && out.len() < chunk_size {
            let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
                break;
            };
            let (index_id, index_key, ts) = keys::parse_idx_key(key)?;
            let entry = IndexEntry::from_index_key(
                index_key,
                index_id,
                ts,
                codec::index_entry_is_deleted(value),
            );
            let prefix = entry.key_prefix.clone();
            if group_prefix.as_ref().is_some_and(|p| *p != prefix) {
                flush(&mut group, &mut out);
            }
            group_prefix = Some(prefix);
            group.push(entry);
            anyhow::ensure!(
                group.len() <= MAX_INDEX_ENTRY_GROUP,
                "more than {MAX_INDEX_ENTRY_GROUP} index entries share one truncated key prefix; \
                 they cannot be ordered without buffering all of them",
            );
            iter.next();
        }
        iter.status()?;
        flush(&mut group, &mut out);
        Ok(out)
    }

    fn get_global(&self, key: PersistenceGlobalKey) -> anyhow::Result<Option<JsonValue>> {
        self.refresh()?;
        let globals = self.cf(CF_GLOBALS)?;
        let key_str = String::from(key);
        let Some(bytes) = self.db.get_cf(&globals, key_str.as_bytes())? else {
            return Ok(None);
        };
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        // Shapes nest far deeper than serde_json's default recursion limit,
        // matching what the SQLite backend does for the same values.
        deserializer.disable_recursion_limit();
        let value = JsonValue::deserialize(&mut deserializer)
            .with_context(|| format!("invalid JSON at persistence key {key_str}"))?;
        deserializer.end()?;
        Ok(Some(value))
    }

    /// Newest revision of each `(id, ts)` strictly before `ts`.
    fn previous_revisions_inner(
        &self,
        ids: BTreeSet<(InternalDocumentId, Timestamp)>,
    ) -> anyhow::Result<BTreeMap<(InternalDocumentId, Timestamp), DocumentLogEntry>> {
        self.refresh()?;
        let docs = self.cf(CF_DOCS)?;
        // One snapshot across the revision walk and the body resolution below.
        let snapshot = self.read_snapshot();
        let mut iter = self
            .db
            .raw_iterator_cf_opt(&docs, snapshot_read_opts(snapshot.as_ref()));

        // Locate each predecessor first, then fetch all the bodies at once.
        let mut located = Vec::with_capacity(ids.len());
        for (id, ts) in ids {
            // No revision can precede `Timestamp::MIN`.
            let Some(seek) = keys::docs_seek_before(id, ts) else {
                continue;
            };
            let prefix = keys::docs_prefix(id);
            iter.seek(seek);
            if iter.valid()
                && let Some(key) = iter.key()
                && key.starts_with(&prefix)
            {
                let (prev_ts, found_id) = keys::parse_docs_key(key)?;
                debug_assert_eq!(found_id, id);
                located.push((id, ts, prev_ts));
            }
        }
        iter.status()?;

        let coordinates: Vec<_> = located.iter().map(|(id, _, prev)| (*prev, *id)).collect();
        let bodies =
            multi_get_documents(self, &coordinates, &snapshot_read_opts(snapshot.as_ref()))?;
        let mut out = BTreeMap::new();
        for (id, ts, prev_ts) in located {
            let (value, prev_prev_ts) = bodies.get(&(prev_ts, id)).cloned().ok_or_else(|| {
                anyhow::anyhow!("revision of {id} at {prev_ts} is missing its body")
            })?;
            out.insert(
                (id, ts),
                DocumentLogEntry {
                    ts: prev_ts,
                    id,
                    value,
                    prev_ts: prev_prev_ts,
                },
            );
        }
        Ok(out)
    }

    /// Exact revisions at the given `prev_ts` coordinates.
    fn exact_revisions_inner(
        &self,
        ids: BTreeSet<DocumentPrevTsQuery>,
    ) -> anyhow::Result<BTreeMap<DocumentPrevTsQuery, DocumentLogEntry>> {
        self.refresh()?;
        let queries: Vec<_> = ids.into_iter().collect();
        let coordinates: Vec<_> = queries.iter().map(|q| (q.prev_ts, q.id)).collect();
        let snapshot = self.read_snapshot();
        let bodies =
            multi_get_documents(self, &coordinates, &snapshot_read_opts(snapshot.as_ref()))?;
        let mut out = BTreeMap::new();
        for query in queries {
            let Some((value, prev_prev_ts)) = bodies.get(&(query.prev_ts, query.id)).cloned()
            else {
                continue;
            };
            out.insert(
                query,
                DocumentLogEntry {
                    ts: query.prev_ts,
                    id: query.id,
                    value,
                    prev_ts: prev_prev_ts,
                },
            );
        }
        Ok(out)
    }

    fn max_committed_ts(&self) -> anyhow::Result<Option<Timestamp>> {
        self.refresh()?;
        let dlog = self.cf(CF_DLOG)?;
        let mut iter = self.db.raw_iterator_cf(&dlog);
        iter.seek_to_last();
        iter.status()?;
        let Some(key) = iter.key() else {
            return Ok(None);
        };
        Ok(Some(keys::parse_dlog_key(key)?.0))
    }
}

#[try_stream(ok = DocumentLogEntry, error = anyhow::Error)]
async fn stream_document_log(
    inner: Arc<Inner>,
    tablet_id: Option<TabletId>,
    range: TimestampRange,
    order: Order,
    page_size: usize,
    retention_validator: Arc<dyn RetentionValidator>,
) {
    let timer = metrics::load_documents_timer();
    let mut cursor = None;
    let mut total = 0;
    loop {
        let inner = inner.clone();
        let page = tokio_spawn_blocking("rocksdb_document_page", move || {
            inner.document_page(tablet_id, range, order, page_size, cursor)
        })
        .await
        .context("rocksdb document page task panicked")??;

        // Confirm the snapshot is still within retention before releasing
        // anything read from it, exactly as the Postgres backend does.
        retention_validator
            .validate_document_snapshot(range.min_timestamp_inclusive())
            .await?;

        total += page.entries.len();
        for entry in page.entries {
            yield entry;
        }
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    metrics::finish_load_documents_timer(timer, total);
}

#[try_stream(ok = (IndexKeyBytes, LatestDocument), error = anyhow::Error)]
async fn stream_index_scan(
    inner: Arc<Inner>,
    index_id: IndexId,
    tablet_id: TabletId,
    read_timestamp: Timestamp,
    interval: Interval,
    order: Order,
    page_size: usize,
    retention_validator: Arc<dyn RetentionValidator>,
) {
    let timer = metrics::index_scan_timer();
    let mut cursor = None;
    let mut total = 0;
    loop {
        let inner = inner.clone();
        let interval = interval.clone();
        let page = tokio_spawn_blocking("rocksdb_index_page", move || {
            inner.index_page(
                index_id,
                tablet_id,
                read_timestamp,
                &interval,
                order,
                page_size,
                cursor,
            )
        })
        .await
        .context("rocksdb index page task panicked")??;

        retention_validator
            .validate_snapshot(read_timestamp)
            .await?;

        total += page.rows.len();
        for row in page.rows {
            yield row;
        }
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    metrics::finish_index_scan_timer(timer, total);
}

#[async_trait]
impl PersistenceReader for RocksDbPersistence {
    fn load_documents(
        &self,
        range: TimestampRange,
        order: Order,
        page_size: u32,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> DocumentStream<'_> {
        stream_document_log(
            self.inner.clone(),
            None,
            range,
            order,
            page_size.max(1) as usize,
            retention_validator,
        )
        // A page is yielded from a buffer with no await between its rows, so a
        // consumer that awaits nothing else can run through Tokio's
        // cooperative budget and starve tasks sharing the worker. The
        // relational backends wrap their streams the same way.
        .cooperative()
        .boxed()
    }

    fn load_documents_from_table(
        &self,
        tablet_id: TabletId,
        range: TimestampRange,
        order: Order,
        page_size: u32,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> DocumentStream<'_> {
        stream_document_log(
            self.inner.clone(),
            Some(tablet_id),
            range,
            order,
            page_size.max(1) as usize,
            retention_validator,
        )
        // A page is yielded from a buffer with no await between its rows, so a
        // consumer that awaits nothing else can run through Tokio's
        // cooperative budget and starve tasks sharing the worker. The
        // relational backends wrap their streams the same way.
        .cooperative()
        .boxed()
    }

    async fn previous_revisions(
        &self,
        ids: BTreeSet<(InternalDocumentId, Timestamp)>,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> anyhow::Result<BTreeMap<(InternalDocumentId, Timestamp), DocumentLogEntry>> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let min_ts = ids.iter().map(|(_, ts)| *ts).min();
        let inner = self.inner.clone();
        let out = tokio_spawn_blocking("rocksdb_previous_revisions", move || {
            inner.previous_revisions_inner(ids)
        })
        .await
        .context("rocksdb previous revisions task panicked")??;
        if let Some(min_ts) = min_ts {
            retention_validator
                .validate_document_snapshot(min_ts)
                .await?;
        }
        Ok(out)
    }

    async fn previous_revisions_of_documents(
        &self,
        ids: BTreeSet<DocumentPrevTsQuery>,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> anyhow::Result<BTreeMap<DocumentPrevTsQuery, DocumentLogEntry>> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let min_ts = ids.iter().map(|q| q.ts).min();
        let inner = self.inner.clone();
        let out = tokio_spawn_blocking("rocksdb_exact_revisions", move || {
            inner.exact_revisions_inner(ids)
        })
        .await
        .context("rocksdb exact revisions task panicked")??;
        if let Some(min_ts) = min_ts {
            retention_validator
                .validate_document_snapshot(min_ts)
                .await?;
        }
        Ok(out)
    }

    fn index_scan(
        &self,
        index_id: IndexId,
        tablet_id: TabletId,
        read_timestamp: Timestamp,
        interval: &Interval,
        order: Order,
        size_hint: usize,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> IndexStream<'_> {
        stream_index_scan(
            self.inner.clone(),
            index_id,
            tablet_id,
            read_timestamp,
            interval.clone(),
            order,
            size_hint.clamp(1, *options::SCAN_PAGE_ROWS),
            retention_validator,
        )
        // A page is yielded from a buffer with no await between its rows, so a
        // consumer that awaits nothing else can run through Tokio's
        // cooperative budget and starve tasks sharing the worker. The
        // relational backends wrap their streams the same way.
        .cooperative()
        .boxed()
    }

    async fn get_persistence_global(
        &self,
        key: PersistenceGlobalKey,
    ) -> anyhow::Result<Option<JsonValue>> {
        let inner = self.inner.clone();
        tokio_spawn_blocking("rocksdb_get_global", move || inner.get_global(key))
            .await
            .context("rocksdb global read task panicked")?
    }

    async fn max_ts(&self) -> anyhow::Result<Option<Timestamp>> {
        let inner = self.inner.clone();
        let max_committed =
            tokio_spawn_blocking("rocksdb_max_ts", move || inner.max_committed_ts())
                .await
                .context("rocksdb max_ts task panicked")??;
        let max_repeatable = self
            .get_persistence_global(PersistenceGlobalKey::MaxRepeatableTimestamp)
            .await?
            .map(Timestamp::try_from)
            .transpose()?;
        // `None` sorts below `Some`, which is what we want: either bound alone
        // is a valid answer when the other is absent.
        Ok(cmp::max(max_committed, max_repeatable))
    }

    fn version(&self) -> PersistenceVersion {
        PersistenceVersion::V5
    }

    async fn table_size_stats(
        &self,
    ) -> anyhow::Result<Vec<common::persistence::PersistenceTableSize>> {
        self.table_sizes().await
    }
}
