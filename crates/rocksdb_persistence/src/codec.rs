//! Value encodings for the RocksDB persistence layer.
//!
//! Documents are stored as the same JSON the Postgres and SQLite backends
//! write (`doc.value().json_serialize()`), so a database can be dumped and
//! reloaded across backends without a re-encoding step, and so there is exactly
//! one document serialization in the tree to keep correct. RocksDB compresses
//! the JSON on the way to disk.
//!
//! Everything that is already in the key — the timestamp, the tablet, the
//! document id — is omitted from the value.

use common::{
    document::ResolvedDocument,
    types::Timestamp,
    value::{
        ConvexValue,
        InternalDocumentId,
    },
};
use value::{
    InternalId,
    TabletId,
};

use crate::keys::ID_LEN;

const FLAG_HAS_VALUE: u8 = 1 << 0;
const FLAG_HAS_PREV_TS: u8 = 1 << 1;

// ---------------------------------------------------------------------------
// dlog values: the document body
// ---------------------------------------------------------------------------

/// `[flags][prev_ts: 8 if present][json]`
///
/// A tombstone has no value and, in the usual case, no body at all.
pub fn encode_document(
    value: &Option<ResolvedDocument>,
    prev_ts: Option<Timestamp>,
) -> anyhow::Result<Vec<u8>> {
    let json = match value {
        Some(doc) => Some(doc.value().json_serialize()?),
        None => None,
    };
    let mut flags = 0u8;
    if json.is_some() {
        flags |= FLAG_HAS_VALUE;
    }
    if prev_ts.is_some() {
        flags |= FLAG_HAS_PREV_TS;
    }
    let mut out = Vec::with_capacity(1 + 8 + json.as_ref().map_or(0, |j| j.len()));
    out.push(flags);
    if let Some(prev_ts) = prev_ts {
        out.extend_from_slice(&u64::from(prev_ts).to_be_bytes());
    }
    if let Some(json) = json {
        out.extend_from_slice(json.as_bytes());
    }
    Ok(out)
}

/// A document body recovered from `dlog`.
pub struct DecodedDocument {
    /// `None` for a tombstone.
    pub value: Option<ResolvedDocument>,
    /// Timestamp of the previous revision, if this is not the first.
    pub prev_ts: Option<Timestamp>,
}

/// Reverse [`encode_document`].
pub fn decode_document(tablet: TabletId, bytes: &[u8]) -> anyhow::Result<DecodedDocument> {
    let flags = *bytes
        .first()
        .ok_or_else(|| anyhow::anyhow!("empty document value"))?;
    let mut rest = &bytes[1..];

    let prev_ts = if flags & FLAG_HAS_PREV_TS != 0 {
        anyhow::ensure!(rest.len() >= 8, "document value truncated before prev_ts");
        let (head, tail) = rest.split_at(8);
        rest = tail;
        Some(Timestamp::try_from(u64::from_be_bytes(head.try_into()?))?)
    } else {
        None
    };

    let value = if flags & FLAG_HAS_VALUE != 0 {
        let json: serde_json::Value = serde_json::from_slice(rest)
            .map_err(|e| anyhow::anyhow!("invalid document JSON: {e}"))?;
        let value: ConvexValue = json.try_into()?;
        Some(ResolvedDocument::from_database(tablet, value)?)
    } else {
        None
    };

    Ok(DecodedDocument { value, prev_ts })
}

// ---------------------------------------------------------------------------
// idx values: the document this index entry points at
// ---------------------------------------------------------------------------

/// `[flags][tablet: 16][id: 16]`, or just `[flags]` for a tombstone.
pub fn encode_index_entry(value: Option<InternalDocumentId>) -> Vec<u8> {
    match value {
        Some(id) => {
            let mut out = Vec::with_capacity(1 + 2 * ID_LEN);
            out.push(FLAG_HAS_VALUE);
            out.extend_from_slice(&id.table().0[..]);
            out.extend_from_slice(&id.internal_id()[..]);
            out
        },
        None => vec![0u8],
    }
}

/// Reverse [`encode_index_entry`].
pub fn decode_index_entry(bytes: &[u8]) -> anyhow::Result<Option<InternalDocumentId>> {
    let flags = *bytes
        .first()
        .ok_or_else(|| anyhow::anyhow!("empty index value"))?;
    if flags & FLAG_HAS_VALUE == 0 {
        return Ok(None);
    }
    anyhow::ensure!(
        bytes.len() == 1 + 2 * ID_LEN,
        "index value of {} bytes, expected {}",
        bytes.len(),
        1 + 2 * ID_LEN,
    );
    let tablet_bytes: [u8; ID_LEN] = bytes[1..1 + ID_LEN].try_into()?;
    let id_bytes: [u8; ID_LEN] = bytes[1 + ID_LEN..].try_into()?;
    Ok(Some(InternalDocumentId::new(
        TabletId(InternalId(tablet_bytes)),
        InternalId(id_bytes),
    )))
}

/// Whether an index entry is a tombstone, without decoding the pointer.
///
/// `index_scan` checks this for every version it walks past, so it stays on the
/// first byte.
#[inline]
pub fn index_entry_is_deleted(bytes: &[u8]) -> bool {
    bytes
        .first()
        .is_none_or(|flags| flags & FLAG_HAS_VALUE == 0)
}

#[cfg(test)]
mod tests {
    use value::InternalId;

    use super::*;

    fn doc_id(tablet: u8, id: u8) -> InternalDocumentId {
        InternalDocumentId::new(
            TabletId(InternalId([tablet; ID_LEN])),
            InternalId([id; ID_LEN]),
        )
    }

    #[test]
    fn index_entry_roundtrips() {
        let id = doc_id(3, 4);
        let encoded = encode_index_entry(Some(id));
        assert!(!index_entry_is_deleted(&encoded));
        assert_eq!(decode_index_entry(&encoded).unwrap(), Some(id));

        let tombstone = encode_index_entry(None);
        assert!(index_entry_is_deleted(&tombstone));
        assert_eq!(decode_index_entry(&tombstone).unwrap(), None);
    }

    #[test]
    fn tombstone_document_roundtrips() {
        let encoded = encode_document(&None, None).unwrap();
        let decoded = decode_document(TabletId(InternalId([1; ID_LEN])), &encoded).unwrap();
        assert!(decoded.value.is_none());
        assert!(decoded.prev_ts.is_none());
    }

    #[test]
    fn tombstone_with_prev_ts_roundtrips() {
        let prev = Timestamp::try_from(42u64).unwrap();
        let encoded = encode_document(&None, Some(prev)).unwrap();
        let decoded = decode_document(TabletId(InternalId([1; ID_LEN])), &encoded).unwrap();
        assert!(decoded.value.is_none());
        assert_eq!(decoded.prev_ts, Some(prev));
    }

    #[test]
    fn empty_value_is_rejected_rather_than_silently_decoded() {
        assert!(decode_document(TabletId(InternalId([1; ID_LEN])), &[]).is_err());
        assert!(decode_index_entry(&[]).is_err());
    }
}
