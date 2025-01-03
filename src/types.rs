use bytes::Bytes;

/// Represents a key-value pair known not to be a tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

impl From<(&[u8], &[u8])> for KeyValue {
    fn from(record: (&[u8], &[u8])) -> Self {
        let key = Bytes::copy_from_slice(record.0);
        let value = Bytes::copy_from_slice(record.1);
        KeyValue { key, value }
    }
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
    pub fn new(
        key: Bytes,
        value: ValueDeletable,
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Self {
        Self {
            key,
            value,
            seq,
            create_ts,
            expire_ts,
        }
    }

    pub fn estimated_size(&self) -> usize {
        let mut size = self.key.len();
        match &self.value {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => size += v.len(),
            ValueDeletable::Tombstone => {}
        }
        // Add size for sequence number
        size += std::mem::size_of::<u64>();
        // Add size for timestamps
        if self.create_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        if self.expire_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        size
    }

    #[cfg(test)]
    pub fn new_value(key: &[u8], value: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub fn new_tombstone(key: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Tombstone,
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub fn with_create_ts(&self, create_ts: i64) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            seq: self.seq,
            create_ts: Some(create_ts),
            expire_ts: self.expire_ts,
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
}
