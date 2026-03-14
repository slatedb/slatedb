use std::ops::Bound;

use slatedb::{KeyValue, RowEntry, ValueDeletable, WriteHandle};

use crate::error::FfiSlateDbError;

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiKeyRange {
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end: Option<Vec<u8>>,
    pub end_inclusive: bool,
}

impl FfiKeyRange {
    pub(crate) fn into_bounds(self) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), FfiSlateDbError> {
        if self.start.as_ref().is_some_and(|start| start.is_empty()) {
            return Err(FfiSlateDbError::EmptyRangeStart);
        }
        if self.end.as_ref().is_some_and(|end| end.is_empty()) {
            return Err(FfiSlateDbError::EmptyRangeEnd);
        }

        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(FfiSlateDbError::RangeStartGreaterThanEnd);
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(FfiSlateDbError::EmptyRange);
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
pub struct FfiWriteHandle {
    pub seqnum: u64,
    pub create_ts: i64,
}

impl FfiWriteHandle {
    pub(crate) fn from_core(value: WriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub create_ts: i64,
    pub expire_ts: Option<i64>,
}

impl FfiKeyValue {
    pub(crate) fn from_core(value: KeyValue) -> Self {
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
pub enum FfiRowEntryKind {
    Value,
    Tombstone,
    Merge,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiRowEntry {
    pub kind: FfiRowEntryKind,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub seq: u64,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

impl FfiRowEntry {
    pub(crate) fn from_core(entry: RowEntry) -> Self {
        let (kind, value) = match entry.value {
            ValueDeletable::Value(value) => (FfiRowEntryKind::Value, Some(value.to_vec())),
            ValueDeletable::Tombstone => (FfiRowEntryKind::Tombstone, None),
            ValueDeletable::Merge(value) => (FfiRowEntryKind::Merge, Some(value.to_vec())),
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
