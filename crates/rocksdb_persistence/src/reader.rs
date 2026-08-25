//! The read side: paged scans over the column families described in
//! [`crate::keys`].
//!
//! Every scan is paged. A page is fetched on a blocking thread, the retention
//! validator is consulted, and only then are its rows yielded — the same order
//! the Postgres backend uses, so a snapshot that falls out of retention
//! mid-scan is never handed to the caller.
//!
//! Paging without an engine snapshot is safe because of how Convex writes: a
//! `(ts, id)` row is written once and only ever removed by retention, which
//! cannot touch anything the retention validator has approved. Concurrent
//! commits land at higher timestamps, outside every range being scanned. The
//! relational backends page the same way, for the same reason.

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
    runtime::tokio_spawn_blocking,
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

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_iterate_lower_bound(lower.clone());
        read_opts.set_iterate_upper_bound(upper);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);

        match (&cursor, order) {
            (Some(cursor), Order::Asc) => {
                iter.seek(cursor);
                // The cursor is the last row already returned, so step past it.
                if iter.valid() {
                    iter.next();
                }
            },
            (Some(cursor), Order::Desc) => {
                iter.seek_for_prev(cursor);
                if iter.valid() {
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
            let bodies = multi_get_documents(self, &coordinates)?;
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

        let mut read_opts = rocksdb::ReadOptions::default();
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
        while iter.valid() && resolved.len() < limit {
            let Some(key) = iter.key() else { break };
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
        let bodies = multi_get_documents(self, &coordinates)?;
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

        let cursor = if rows.len() < limit {
            None
        } else {
            last_prefix
        };
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
            // Start at the cursor's own key: later versions of it may still be
            // ahead of the cursor in `IndexEntry` order.
            Some(entry) => {
                let mut full_key = entry.key_prefix.clone();
                if let Some(suffix) = &entry.key_suffix {
                    full_key.extend_from_slice(suffix);
                }
                iter.seek(keys::idx_key_prefix(entry.index_id, &full_key));
            },
            None => iter.seek_to_first(),
        }

        // `IndexEntry` orders by timestamp ascending within a key, while
        // storage orders newest-first, so each key's run is buffered and
        // reversed before it is emitted.
        let mut out = Vec::with_capacity(chunk_size);
        let mut run: Vec<IndexEntry> = Vec::new();
        let mut run_prefix: Option<Vec<u8>> = None;
        let flush = |run: &mut Vec<IndexEntry>, out: &mut Vec<IndexEntry>| {
            run.reverse();
            for entry in run.drain(..) {
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
            let prefix = key[..key.len().saturating_sub(keys::TS_LEN)].to_vec();
            if run_prefix.as_ref().is_some_and(|p| *p != prefix) {
                flush(&mut run, &mut out);
            }
            run_prefix = Some(prefix);
            let (index_id, index_key, ts) = keys::parse_idx_key(key)?;
            run.push(IndexEntry::from_index_key(
                index_key,
                index_id,
                ts,
                codec::index_entry_is_deleted(value),
            ));
            iter.next();
        }
        iter.status()?;
        flush(&mut run, &mut out);
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
        let mut iter = self.db.raw_iterator_cf(&docs);

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
        let bodies = multi_get_documents(self, &coordinates)?;
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
        let bodies = multi_get_documents(self, &coordinates)?;
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
