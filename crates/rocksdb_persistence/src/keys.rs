//! Key encodings for the RocksDB persistence layer.
//!
//! Convex does MVCC in its own data model — every `PersistenceReader` method
//! takes an explicit timestamp — so the storage engine only has to provide
//! ordered keys. All the versioning lives in the encodings below.
//!
//! # Column families
//!
//! ```text
//! dlog    ts[8] ‖ tablet[16] ‖ id[16]            -> encoded document
//! docs    tablet[16] ‖ id[16] ‖ !ts[8]           -> ()
//! dtab    tablet[16] ‖ ts[8] ‖ id[16]            -> ()
//! idx     index_id[16] ‖ esc(index_key) ‖ !ts[8] -> encoded index entry
//! globals key                                    -> JSON
//! ```
//!
//! This mirrors the Postgres schema exactly (`crates/postgres/src/sql.rs`):
//! `dlog` is the `documents` heap under its primary key `(ts, table_id, id)`,
//! and `docs` / `dtab` are its two secondary indexes
//! (`documents_by_table_and_id`, `documents_by_table_ts_and_id`). The document
//! body lives in `dlog` so that `index_scan`'s join — which already knows
//! `(ts, tablet, id)` from the index entry — is a single point get, and so that
//! the timestamp-ordered scans behind `load_documents` and streaming export
//! read their values sequentially.
//!
//! # Descending timestamps
//!
//! `!ts` is `u64::MAX - ts`, big-endian, so a key's versions sort newest-first.
//! "The newest version at or before `read_ts`" then becomes a single forward
//! seek instead of the `DISTINCT ON` + `GROUP BY` + join that the relational
//! backends need. Retention's "delete every version at or before `ts`" likewise
//! becomes one contiguous range.
//!
//! # Why index keys are escaped
//!
//! `idx` concatenates a variable-length index key with a fixed-length
//! timestamp, and naive concatenation does not preserve order: with raw bytes,
//! `[1,2] ‖ !ts` sorts *after* `[1,2,3] ‖ !ts` because `0xFF… > 0x03`, while
//! `[1,2] < [1,2,3]`. Index keys are not guaranteed prefix-free, so the
//! variable-length component is escaped into a self-terminating,
//! order-preserving form first. See [`escape_into`].

use common::{
    index::IndexKeyBytes,
    types::{
        IndexId,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        TabletId,
    },
};
use value::InternalId;

/// Length of an `InternalId`, and therefore of a `TabletId` or an `IndexId`.
pub const ID_LEN: usize = 16;
/// Length of an encoded timestamp.
pub const TS_LEN: usize = 8;
/// `dlog`, `docs` and `dtab` keys are all one timestamp and two ids.
pub const DOC_KEY_LEN: usize = TS_LEN + ID_LEN + ID_LEN;

/// The document log, ordered by timestamp; holds the document bodies.
pub const CF_DLOG: &str = "dlog";
/// Document revisions ordered by id, newest first.
pub const CF_DOCS: &str = "docs";
/// Document revisions ordered by tablet then timestamp.
pub const CF_DTAB: &str = "dtab";
/// Database index entries, newest version of each key first.
pub const CF_IDX: &str = "idx";
/// Persistence globals, keyed by name.
pub const CF_GLOBALS: &str = "globals";

/// Every column family, in the order they are created.
pub const ALL_COLUMN_FAMILIES: [&str; 5] = [CF_DLOG, CF_DOCS, CF_DTAB, CF_IDX, CF_GLOBALS];

/// Big-endian timestamp, ascending.
#[inline]
fn ts_asc(ts: Timestamp) -> [u8; TS_LEN] {
    u64::from(ts).to_be_bytes()
}

/// Big-endian complement of a timestamp, so that larger timestamps sort first.
#[inline]
fn ts_desc(ts: Timestamp) -> [u8; TS_LEN] {
    (u64::MAX - u64::from(ts)).to_be_bytes()
}

#[inline]
fn read_ts_asc(bytes: &[u8]) -> anyhow::Result<Timestamp> {
    let arr: [u8; TS_LEN] = bytes.try_into()?;
    Ok(Timestamp::try_from(u64::from_be_bytes(arr))?)
}

#[inline]
fn read_ts_desc(bytes: &[u8]) -> anyhow::Result<Timestamp> {
    let arr: [u8; TS_LEN] = bytes.try_into()?;
    Ok(Timestamp::try_from(u64::MAX - u64::from_be_bytes(arr))?)
}

#[inline]
fn read_id(bytes: &[u8]) -> anyhow::Result<InternalId> {
    let arr: [u8; ID_LEN] = bytes.try_into()?;
    Ok(InternalId(arr))
}

// ---------------------------------------------------------------------------
// dlog: ts ‖ tablet ‖ id
// ---------------------------------------------------------------------------

/// Key of a document body in `dlog`.
pub fn dlog_key(ts: Timestamp, id: InternalDocumentId) -> [u8; DOC_KEY_LEN] {
    let mut k = [0u8; DOC_KEY_LEN];
    k[..TS_LEN].copy_from_slice(&ts_asc(ts));
    k[TS_LEN..TS_LEN + ID_LEN].copy_from_slice(&id.table().0[..]);
    k[TS_LEN + ID_LEN..].copy_from_slice(&id.internal_id()[..]);
    k
}

/// Recover the coordinates a `dlog` key encodes.
pub fn parse_dlog_key(k: &[u8]) -> anyhow::Result<(Timestamp, InternalDocumentId)> {
    anyhow::ensure!(
        k.len() == DOC_KEY_LEN,
        "malformed dlog key of {} bytes",
        k.len()
    );
    let ts = read_ts_asc(&k[..TS_LEN])?;
    let tablet = TabletId(read_id(&k[TS_LEN..TS_LEN + ID_LEN])?);
    let id = read_id(&k[TS_LEN + ID_LEN..])?;
    Ok((ts, InternalDocumentId::new(tablet, id)))
}

/// Lower bound (inclusive) of the `dlog` range starting at `ts`.
pub fn dlog_ts_lower(ts: Timestamp) -> [u8; TS_LEN] {
    ts_asc(ts)
}

/// Upper bound (exclusive) of the `dlog` range ending before `ts`.
pub fn dlog_ts_upper(ts: Timestamp) -> [u8; TS_LEN] {
    ts_asc(ts)
}

// ---------------------------------------------------------------------------
// docs: tablet ‖ id ‖ !ts
// ---------------------------------------------------------------------------

/// Key of one revision in `docs`.
pub fn docs_key(ts: Timestamp, id: InternalDocumentId) -> [u8; DOC_KEY_LEN] {
    let mut k = [0u8; DOC_KEY_LEN];
    k[..ID_LEN].copy_from_slice(&id.table().0[..]);
    k[ID_LEN..2 * ID_LEN].copy_from_slice(&id.internal_id()[..]);
    k[2 * ID_LEN..].copy_from_slice(&ts_desc(ts));
    k
}

/// The `tablet ‖ id` prefix shared by every revision of a document.
pub fn docs_prefix(id: InternalDocumentId) -> [u8; 2 * ID_LEN] {
    let mut k = [0u8; 2 * ID_LEN];
    k[..ID_LEN].copy_from_slice(&id.table().0[..]);
    k[ID_LEN..].copy_from_slice(&id.internal_id()[..]);
    k
}

/// Seek target for "the newest revision of `id` strictly before `ts`".
///
/// Versions sort newest-first, so seeking to `!(ts - 1)` lands directly on the
/// predecessor when one exists.
///
/// `Timestamp::MIN` has no predecessor. `!MIN` is all-`0xFF`, which is the
/// *largest* suffix a revision can carry rather than a value past the run, so
/// seeking there would wrongly land on the `MIN` revision itself. The seek
/// target is `None` for that case: no older revision can exist, so there is
/// nothing to seek to.
pub fn docs_seek_before(id: InternalDocumentId, ts: Timestamp) -> Option<[u8; DOC_KEY_LEN]> {
    let pred = ts.pred_opt()?;
    let mut k = [0u8; DOC_KEY_LEN];
    k[..2 * ID_LEN].copy_from_slice(&docs_prefix(id));
    k[2 * ID_LEN..].copy_from_slice(&ts_desc(pred));
    Some(k)
}

/// Recover the coordinates a `docs` key encodes.
pub fn parse_docs_key(k: &[u8]) -> anyhow::Result<(Timestamp, InternalDocumentId)> {
    anyhow::ensure!(
        k.len() == DOC_KEY_LEN,
        "malformed docs key of {} bytes",
        k.len()
    );
    let tablet = TabletId(read_id(&k[..ID_LEN])?);
    let id = read_id(&k[ID_LEN..2 * ID_LEN])?;
    let ts = read_ts_desc(&k[2 * ID_LEN..])?;
    Ok((ts, InternalDocumentId::new(tablet, id)))
}

// ---------------------------------------------------------------------------
// dtab: tablet ‖ ts ‖ id
// ---------------------------------------------------------------------------

/// Key of one revision in `dtab`.
pub fn dtab_key(ts: Timestamp, id: InternalDocumentId) -> [u8; DOC_KEY_LEN] {
    let mut k = [0u8; DOC_KEY_LEN];
    k[..ID_LEN].copy_from_slice(&id.table().0[..]);
    k[ID_LEN..ID_LEN + TS_LEN].copy_from_slice(&ts_asc(ts));
    k[ID_LEN + TS_LEN..].copy_from_slice(&id.internal_id()[..]);
    k
}

/// Recover the coordinates a `dtab` key encodes.
pub fn parse_dtab_key(k: &[u8]) -> anyhow::Result<(Timestamp, InternalDocumentId)> {
    anyhow::ensure!(
        k.len() == DOC_KEY_LEN,
        "malformed dtab key of {} bytes",
        k.len()
    );
    let tablet = TabletId(read_id(&k[..ID_LEN])?);
    let ts = read_ts_asc(&k[ID_LEN..ID_LEN + TS_LEN])?;
    let id = read_id(&k[ID_LEN + TS_LEN..])?;
    Ok((ts, InternalDocumentId::new(tablet, id)))
}

/// Bounds of one tablet's slice of `dtab`, restricted to `[lower, upper)`.
pub fn dtab_bounds(tablet: TabletId, lower: Timestamp, upper: Timestamp) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(ID_LEN + TS_LEN);
    lo.extend_from_slice(&tablet.0[..]);
    lo.extend_from_slice(&ts_asc(lower));
    let mut hi = Vec::with_capacity(ID_LEN + TS_LEN);
    hi.extend_from_slice(&tablet.0[..]);
    hi.extend_from_slice(&ts_asc(upper));
    (lo, hi)
}

/// Bounds covering every entry for `tablet`.
pub fn tablet_bounds(tablet: TabletId) -> (Vec<u8>, Vec<u8>) {
    let lo = tablet.0[..].to_vec();
    (lo.clone(), successor(&lo))
}

// ---------------------------------------------------------------------------
// idx: index_id ‖ esc(index_key) ‖ !ts
// ---------------------------------------------------------------------------

/// Escape `key` into an order-preserving, self-terminating form.
///
/// `0x00` becomes `0x00 0xFF`; the sequence is terminated by `0x00 0x00`. The
/// terminator sorts below every escaped continuation — a literal zero escapes
/// to `0x00 0xFF` and any other byte is itself non-zero — so whenever `a` is a
/// proper prefix of `b`, `escape(a) < escape(b)`. Concatenating a fixed-length
/// suffix afterwards therefore preserves `(key, suffix)` ordering.
pub fn escape_into(key: &[u8], out: &mut Vec<u8>) {
    out.reserve(key.len() + 2);
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// Escape without the terminator, for use as a range bound.
///
/// A bound built this way sits exactly between the escaped forms of the keys
/// below it and the escaped form of `key` itself, which is what makes it a
/// correct inclusive lower bound and exclusive upper bound.
fn escape_body_into(key: &[u8], out: &mut Vec<u8>) {
    out.reserve(key.len());
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
}

/// Reverse [`escape_into`], returning the key and the number of bytes consumed.
pub fn unescape(escaped: &[u8]) -> anyhow::Result<(Vec<u8>, usize)> {
    let mut out = Vec::with_capacity(escaped.len());
    let mut i = 0;
    while i < escaped.len() {
        let b = escaped[i];
        if b != 0 {
            out.push(b);
            i += 1;
            continue;
        }
        let next = *escaped
            .get(i + 1)
            .ok_or_else(|| anyhow::anyhow!("truncated escape sequence"))?;
        match next {
            0x00 => return Ok((out, i + 2)),
            0xFF => {
                out.push(0);
                i += 2;
            },
            other => anyhow::bail!("invalid escape byte 0x{other:02x}"),
        }
    }
    anyhow::bail!("unterminated escaped key")
}

/// Key of one version of an index entry.
pub fn idx_key(index_id: IndexId, key: &[u8], ts: Timestamp) -> Vec<u8> {
    let mut k = Vec::with_capacity(ID_LEN + key.len() + 2 + TS_LEN);
    k.extend_from_slice(&index_id.0[..]);
    escape_into(key, &mut k);
    k.extend_from_slice(&ts_desc(ts));
    k
}

/// Recover the index, key and timestamp an `idx` key encodes.
pub fn parse_idx_key(k: &[u8]) -> anyhow::Result<(IndexId, IndexKeyBytes, Timestamp)> {
    anyhow::ensure!(
        k.len() > ID_LEN + TS_LEN,
        "malformed idx key of {} bytes",
        k.len()
    );
    let index_id = IndexId(read_id(&k[..ID_LEN])?);
    let (key, consumed) = unescape(&k[ID_LEN..])?;
    anyhow::ensure!(
        ID_LEN + consumed + TS_LEN == k.len(),
        "idx key has {} trailing bytes after the escaped key",
        k.len() - ID_LEN - consumed,
    );
    let ts = read_ts_desc(&k[ID_LEN + consumed..])?;
    Ok((index_id, IndexKeyBytes(key), ts))
}

/// The `index_id ‖ esc(key)` prefix shared by every version of one index key.
pub fn idx_key_prefix(index_id: IndexId, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(ID_LEN + key.len() + 2);
    k.extend_from_slice(&index_id.0[..]);
    escape_into(key, &mut k);
    k
}

/// Inclusive lower bound for an index scan starting at raw key `start`.
pub fn idx_lower_bound(index_id: IndexId, start: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(ID_LEN + start.len());
    k.extend_from_slice(&index_id.0[..]);
    escape_body_into(start, &mut k);
    k
}

/// Exclusive upper bound for an index scan ending before raw key `end`.
pub fn idx_upper_bound(index_id: IndexId, end: Option<&[u8]>) -> Vec<u8> {
    match end {
        Some(end) => idx_lower_bound(index_id, end),
        None => successor(&index_id.0[..]),
    }
}

/// Seek target for "the newest version of a key at or before `ts`", given that
/// key's `index_id ‖ esc(key)` prefix.
///
/// Versions sort newest-first, so the first entry at or after this target is
/// the answer whenever it still carries `prefix`.
pub fn idx_seek_at_prefix(prefix: &[u8], ts: Timestamp) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + TS_LEN);
    k.extend_from_slice(prefix);
    k.extend_from_slice(&ts_desc(ts));
    k
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The smallest byte string strictly greater than every string with `prefix`.
///
/// An all-`0xFF` prefix has no successor within the same length; the empty
/// vector returned then means "unbounded", which every call site treats as the
/// end of the column family.
pub fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last != 0xFF {
            out.push(last + 1);
            return out;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use common::types::Timestamp;
    use proptest::prelude::*;
    use value::{
        InternalDocumentId,
        InternalId,
        TabletId,
    };

    use super::*;

    fn ts(n: u64) -> Timestamp {
        Timestamp::try_from(n).unwrap()
    }

    fn doc_id(tablet: u8, id: u8) -> InternalDocumentId {
        InternalDocumentId::new(
            TabletId(InternalId([tablet; ID_LEN])),
            InternalId([id; ID_LEN]),
        )
    }

    fn index_id(n: u8) -> IndexId {
        IndexId(InternalId([n; ID_LEN]))
    }

    /// Escaping has to be order-preserving, or an index scan silently returns
    /// the wrong rows. This is the property the whole `idx` layout rests on.
    fn assert_escape_order(a: &[u8], b: &[u8]) {
        let (mut ea, mut eb) = (Vec::new(), Vec::new());
        escape_into(a, &mut ea);
        escape_into(b, &mut eb);
        assert_eq!(
            ea.cmp(&eb),
            a.cmp(b),
            "escaping reordered {a:?} vs {b:?} -> {ea:?} vs {eb:?}",
        );
    }

    #[test]
    fn escape_preserves_order_on_prefixes() {
        // The case naive concatenation gets wrong: one key a proper prefix of
        // another, with a high-byte suffix behind it.
        assert_escape_order(&[1, 2], &[1, 2, 3]);
        assert_escape_order(&[], &[0]);
        assert_escape_order(&[0], &[0, 1]);
        assert_escape_order(&[0, 0], &[0, 0xFF]);
        assert_escape_order(&[0xFF], &[0xFF, 0]);
    }

    #[test]
    fn naive_concatenation_would_have_been_wrong() {
        // Documents why the escaping exists: without it, `[1,2] ‖ !ts` sorts
        // after `[1,2,3] ‖ !ts` even though `[1,2] < [1,2,3]`.
        let ts_bytes = ts_desc(ts(1));
        let mut naive_short = vec![1u8, 2];
        naive_short.extend_from_slice(&ts_bytes);
        let mut naive_long = vec![1u8, 2, 3];
        naive_long.extend_from_slice(&ts_bytes);
        assert_eq!(naive_short.cmp(&naive_long), Ordering::Greater);

        // The real encoding gets it right.
        let short = idx_key(index_id(1), &[1, 2], ts(1));
        let long = idx_key(index_id(1), &[1, 2, 3], ts(1));
        assert_eq!(short.cmp(&long), Ordering::Less);
    }

    #[test]
    fn idx_key_roundtrips() {
        for key in [
            vec![],
            vec![0],
            vec![1, 2, 3],
            vec![0, 0xFF, 0, 0],
            vec![0xFF; 40],
        ] {
            let encoded = idx_key(index_id(7), &key, ts(12345));
            let (id, parsed, parsed_ts) = parse_idx_key(&encoded).unwrap();
            assert_eq!(id, index_id(7));
            assert_eq!(parsed.0, key);
            assert_eq!(parsed_ts, ts(12345));
        }
    }

    #[test]
    fn idx_versions_sort_newest_first() {
        let key = [1u8, 2, 3];
        let old = idx_key(index_id(1), &key, ts(10));
        let new = idx_key(index_id(1), &key, ts(20));
        assert!(new < old, "newer versions must sort first");
    }

    #[test]
    fn idx_bounds_bracket_the_interval() {
        let id = index_id(3);
        let lo = idx_lower_bound(id, &[1, 2]);
        let hi = idx_upper_bound(id, Some(&[1, 4]));

        // Inside the interval.
        for key in [vec![1u8, 2], vec![1, 2, 0], vec![1, 3], vec![1, 3, 9]] {
            let k = idx_key(id, &key, ts(5));
            assert!(k >= lo && k < hi, "{key:?} should be inside [lo, hi)");
        }
        // Outside it.
        for key in [
            vec![1u8, 1, 9],
            vec![1u8],
            vec![1, 4],
            vec![1, 4, 0],
            vec![2],
        ] {
            let k = idx_key(id, &key, ts(5));
            assert!(k < lo || k >= hi, "{key:?} should be outside [lo, hi)");
        }
    }

    #[test]
    fn idx_unbounded_upper_stops_at_the_next_index() {
        let hi = idx_upper_bound(index_id(3), None);
        assert!(idx_key(index_id(3), &[0xFF; 64], ts(0)) < hi);
        assert!(idx_key(index_id(4), &[], ts(0)) >= hi);
    }

    #[test]
    fn doc_keys_roundtrip() {
        let id = doc_id(2, 9);
        let t = ts(777);
        assert_eq!(parse_dlog_key(&dlog_key(t, id)).unwrap(), (t, id));
        assert_eq!(parse_docs_key(&docs_key(t, id)).unwrap(), (t, id));
        assert_eq!(parse_dtab_key(&dtab_key(t, id)).unwrap(), (t, id));
    }

    #[test]
    fn docs_versions_sort_newest_first() {
        let id = doc_id(1, 1);
        assert!(docs_key(ts(20), id) < docs_key(ts(10), id));
    }

    #[test]
    fn docs_seek_before_lands_on_the_predecessor() {
        let id = doc_id(1, 1);
        let seek = docs_seek_before(id, ts(20)).unwrap();
        // Revisions strictly older than 20 are at or after the seek target...
        assert!(docs_key(ts(19), id).as_slice() >= seek.as_slice());
        assert!(docs_key(ts(0), id).as_slice() >= seek.as_slice());
        // ...and revisions at or after 20 are before it.
        assert!(docs_key(ts(20), id).as_slice() < seek.as_slice());
        assert!(docs_key(ts(21), id).as_slice() < seek.as_slice());
    }

    #[test]
    fn docs_seek_before_min_timestamp_has_no_target() {
        // `!MIN` is all-0xFF — the largest suffix a revision can carry, not a
        // value past the run — so a naive `!(ts - 1)` would land on the MIN
        // revision itself and report it as its own predecessor.
        assert_eq!(docs_seek_before(doc_id(1, 1), Timestamp::MIN), None);
    }

    #[test]
    fn dlog_orders_by_timestamp_across_tablets() {
        assert!(dlog_key(ts(1), doc_id(9, 9)) < dlog_key(ts(2), doc_id(0, 0)));
    }

    #[test]
    fn successor_handles_saturated_prefixes() {
        assert_eq!(successor(&[1, 2]), vec![1, 3]);
        assert_eq!(successor(&[1, 0xFF]), vec![2]);
        assert_eq!(successor(&[0xFF, 0xFF]), Vec::<u8>::new());
        assert_eq!(successor(&[]), Vec::<u8>::new());
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn escape_is_order_preserving(a in prop::collection::vec(any::<u8>(), 0..24),
                                      b in prop::collection::vec(any::<u8>(), 0..24)) {
            assert_escape_order(&a, &b);
        }

        #[test]
        fn escape_roundtrips(key in prop::collection::vec(any::<u8>(), 0..64)) {
            let mut escaped = Vec::new();
            escape_into(&key, &mut escaped);
            let (decoded, consumed) = unescape(&escaped).unwrap();
            prop_assert_eq!(decoded, key);
            prop_assert_eq!(consumed, escaped.len());
        }

        /// The composite ordering the `idx` column family depends on: sort by
        /// key ascending, then by timestamp descending.
        #[test]
        fn idx_key_orders_by_key_then_reverse_ts(
            a in prop::collection::vec(any::<u8>(), 0..24),
            b in prop::collection::vec(any::<u8>(), 0..24),
            ts_a in 0u64..1_000_000,
            ts_b in 0u64..1_000_000,
        ) {
            let ka = idx_key(index_id(1), &a, ts(ts_a));
            let kb = idx_key(index_id(1), &b, ts(ts_b));
            let expected = a.cmp(&b).then(ts_b.cmp(&ts_a));
            prop_assert_eq!(ka.cmp(&kb), expected);
        }
    }
}
