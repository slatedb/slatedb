use bytes::Bytes;

/// Represents a key-value pair known not to be a tombstone.
#[derive(Debug)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

/// Represents a key-value pair that may be a tombstone.
#[derive(Debug)]
pub struct KVEntry {
    pub key: Bytes,
    pub value: KVValue,
}

/// Represents a value that may be a tombstone.
/// Equivalent to `Option<Bytes>`, but used internally
/// to prevent type confusion between `None` indicating
/// that a key does not exist, and `Tombstone` indicating
/// that the key exists but has a tombstone value.
#[derive(Debug)]
pub enum KVValue {
    Value(Bytes),
    Tombstone,
}
