use std::ops::Bound;

use slatedb::ValueDeletable;

use crate::error::SlateDbError;

type KeyBound = Bound<Vec<u8>>;
type KeyBounds = (KeyBound, KeyBound);

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct KeyRange {
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end: Option<Vec<u8>>,
    pub end_inclusive: bool,
}

impl KeyRange {
    pub(crate) fn into_bounds(self) -> Result<KeyBounds, SlateDbError> {
        if self.start.as_ref().is_some_and(|start| start.is_empty()) {
            return Err(SlateDbError::EmptyRangeStart);
        }
        if self.end.as_ref().is_some_and(|end| end.is_empty()) {
            return Err(SlateDbError::EmptyRangeEnd);
        }

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

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    pub seqnum: u64,
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

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub create_ts: i64,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RowEntryKind {
    Value,
    Tombstone,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowEntry {
    pub kind: RowEntryKind,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub seq: u64,
    pub create_ts: Option<i64>,
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
