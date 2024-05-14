use bytes::Bytes;

/// Represents a key-value pair known not to be a tombstone.
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

/// Represents a key-value pair that may be a tombstone.
#[derive(Debug, Clone)]
pub struct KeyValueDeletable {
    pub key: Bytes,
    pub value: ValueDeletable,
}

/// Represents a value that may be a tombstone.
/// Equivalent to `Option<Bytes>`, but used internally
/// to prevent type confusion between `None` indicating
/// that a key does not exist, and `Tombstone` indicating
/// that the key exists but has a tombstone value.
#[derive(Debug, Clone)]
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
}
