use std::ops::Bound;

use slatedb::ValueDeletable;

use crate::error::SlateDbError;

type KeyBound = Bound<Vec<u8>>;
type KeyBounds = (KeyBound, KeyBound);

/// A half-open or closed byte-key range used by scan APIs.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct KeyRange {
    /// Inclusive or exclusive lower bound. `None` means unbounded.
    pub start: Option<Vec<u8>>,
    /// Whether `start` is inclusive when present.
    pub start_inclusive: bool,
    /// Inclusive or exclusive upper bound. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// Whether `end` is inclusive when present.
    pub end_inclusive: bool,
}

impl KeyRange {
    pub(crate) fn into_bounds(self) -> Result<KeyBounds, SlateDbError> {
        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(SlateDbError::RangeStartGreaterThanEnd);
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(SlateDbError::EmptyRange);
                }
                _ => {}
            }
        }

        Ok((
            match self.start {
                Some(start) if self.start_inclusive => Bound::Included(start),
                Some(start) => Bound::Excluded(start),
                None => Bound::Unbounded,
            },
            match self.end {
                Some(end) if self.end_inclusive => Bound::Included(end),
                Some(end) => Bound::Excluded(end),
                None => Bound::Unbounded,
            },
        ))
    }
}

/// Metadata returned by a successful write.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    /// Sequence number assigned to the write.
    pub seqnum: u64,
    /// Creation timestamp assigned to the write.
    pub create_ts: i64,
}

impl From<slatedb::WriteHandle> for WriteHandle {
    fn from(value: slatedb::WriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}

/// A key/value pair together with the row version metadata that produced it.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    /// Row key.
    pub key: Vec<u8>,
    /// Row value bytes.
    pub value: Vec<u8>,
    /// Sequence number of the row version.
    pub seq: u64,
    /// Creation timestamp of the row version.
    pub create_ts: i64,
    /// Expiration timestamp, if the row has a TTL.
    pub expire_ts: Option<i64>,
}

impl From<slatedb::KeyValue> for KeyValue {
    fn from(value: slatedb::KeyValue) -> Self {
        Self {
            key: value.key.to_vec(),
            value: value.value.to_vec(),
            seq: value.seq,
            create_ts: value.create_ts,
            expire_ts: value.expire_ts,
        }
    }
}

/// Kind of row entry stored in WAL iteration results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RowEntryKind {
    /// A regular value row.
    Value,
    /// A delete tombstone.
    Tombstone,
    /// A merge operand row.
    Merge,
}

/// A raw row entry returned from WAL inspection.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowEntry {
    /// Encoded row kind.
    pub kind: RowEntryKind,
    /// Row key.
    pub key: Vec<u8>,
    /// Row value for value and merge entries. `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Sequence number of the entry.
    pub seq: u64,
    /// Creation timestamp if present in the WAL entry.
    pub create_ts: Option<i64>,
    /// Expiration timestamp if present in the WAL entry.
    pub expire_ts: Option<i64>,
}

impl From<slatedb::RowEntry> for RowEntry {
    fn from(entry: slatedb::RowEntry) -> Self {
        let (kind, value) = match entry.value {
            ValueDeletable::Value(value) => (RowEntryKind::Value, Some(value.to_vec())),
            ValueDeletable::Tombstone => (RowEntryKind::Tombstone, None),
            ValueDeletable::Merge(value) => (RowEntryKind::Merge, Some(value.to_vec())),
        };

        Self {
            kind,
            key: entry.key.to_vec(),
            value,
            seq: entry.seq,
            create_ts: entry.create_ts,
            expire_ts: entry.expire_ts,
        }
    }
}
