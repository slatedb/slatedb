use bytes::Bytes;

/// Represents a key-value pair known not to be a tombstone.
#[derive(Debug, Clone)]
#[cfg(test)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

/// Represents a key-value pair that may be a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub struct RowEntry {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub seq: u64,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

impl RowEntry {
    #[allow(unused)]
    pub fn new(
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Self {
        let value = match value {
            Some(v) => ValueDeletable::Value(v),
            None => ValueDeletable::Tombstone,
        };
        Self {
            key,
            value,
            seq,
            create_ts,
            expire_ts,
        }
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
    Merge(Bytes),
    Tombstone,
}

impl ValueDeletable {
    pub fn len(&self) -> usize {
        match self {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
            ValueDeletable::Tombstone => 0,
        }
    }

    #[deprecated]
    pub fn into_option(self) -> Option<Bytes> {
        match self {
            ValueDeletable::Value(v) => Some(v),
            ValueDeletable::Merge(_) => todo!(),
            ValueDeletable::Tombstone => None,
        }
    }
}
