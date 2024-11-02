use bytes::Bytes;

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
pub struct KeyValueDeletable {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub attributes: RowAttributes,
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
