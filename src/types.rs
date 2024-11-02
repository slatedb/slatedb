use bytes::Bytes;

use crate::row_codec::RowFlags;

/// Represents a key-value pair known not to be a tombstone.
#[derive(Debug, Clone)]
pub struct KeyValue {
    #[allow(dead_code)]
    pub key: Bytes,
    #[allow(dead_code)]
    pub value: Bytes,
}

/// Represents a key-value pair that may be a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub struct RowEntry {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub flags: RowFlags,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

impl RowEntry {
    pub fn new(key: Bytes, value: Option<Bytes>) -> Self {
        let flags = match &value {
            Some(_) => RowFlags::default(),
            None => RowFlags::Tombstone,
        };
        let value = match value {
            Some(v) => ValueDeletable::Value(v),
            None => ValueDeletable::Tombstone,
        };
        Self {
            key,
            value,
            flags,
            create_ts: None,
            expire_ts: None,
        }
    }

    pub fn with_create_ts(mut self, ts: i64) -> Self {
        self.create_ts = Some(ts);
        self
    }

    pub fn with_expire_ts(mut self, ts: i64) -> Self {
        self.expire_ts = Some(ts);
        self
    }
}

/// The metadata associated with a `KeyValueDeletable`
#[derive(Debug, Clone, PartialEq)]
pub struct RowAttributes {
    pub ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

/// Represents a value that may be a tombstone.
/// Equivalent to `Option<Bytes>`, but used internally
/// to prevent type confusion between `None` indicating
/// that a key does not exist, and `Tombstone` indicating
/// that the key exists but has a tombstone value.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueDeletable {
    Value(Bytes),
    Tombstone,
}

impl ValueDeletable {
    pub fn into_option(self) -> Option<Bytes> {
        match self {
            ValueDeletable::Value(v) => Some(v),
            ValueDeletable::Tombstone => None,
        }
    }

    pub fn as_option(&self) -> Option<&Bytes> {
        match self {
            ValueDeletable::Value(v) => Some(v),
            ValueDeletable::Tombstone => None,
        }
    }
}
