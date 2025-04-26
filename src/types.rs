use bytes::Bytes;

/// Represents a key-value pair known not to be a tombstone.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

impl<K, V> From<(&K, &V)> for KeyValue
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    fn from(record: (&K, &V)) -> Self {
        let key = Bytes::copy_from_slice(record.0.as_ref());
        let value = Bytes::copy_from_slice(record.1.as_ref());
        KeyValue { key, value }
    }
}

/// Represents a key-value pair that may be a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowEntry {
    pub(crate) key: Bytes,
    pub(crate) value: ValueDeletable,
    pub(crate) seq: u64,
    pub(crate) create_ts: Option<i64>,
    pub(crate) expire_ts: Option<i64>,
}

impl RowEntry {
    pub(crate) fn new(
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
        let mut size = self.key.len() + self.value.len();
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
    pub(crate) fn new_value(key: &[u8], value: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_merge(key: &[u8], value: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Merge(Bytes::copy_from_slice(value)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_tombstone(key: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Tombstone,
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_create_ts(&self, create_ts: i64) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            seq: self.seq,
            create_ts: Some(create_ts),
            expire_ts: self.expire_ts,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_expire_ts(&self, expire_ts: i64) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            seq: self.seq,
            create_ts: self.create_ts,
            expire_ts: Some(expire_ts),
        }
    }
}

/// The metadata associated with a `KeyValueDeletable`
/// TODO: can be removed
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowAttributes {
    pub(crate) ts: Option<i64>,
    pub(crate) expire_ts: Option<i64>,
}

/// Represents a value that may be a tombstone.
/// Equivalent to `Option<Bytes>`, but used internally
/// to prevent type confusion between `None` indicating
/// that a key does not exist, and `Tombstone` indicating
/// that the key exists but has a tombstone value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueDeletable {
    Value(Bytes),
    Merge(Bytes),
    Tombstone,
}

impl ValueDeletable {
    pub(crate) fn len(&self) -> usize {
        match self {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
            ValueDeletable::Tombstone => 0,
        }
    }

    pub(crate) fn as_bytes(&self) -> Option<Bytes> {
        match self {
            ValueDeletable::Value(v) => Some(v.clone()),
            ValueDeletable::Merge(_) => {
                unimplemented!("MergeOperator is not yet fully implemented")
            }
            ValueDeletable::Tombstone => None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        match self {
            ValueDeletable::Value(_) => false,
            ValueDeletable::Merge(_) => false,
            ValueDeletable::Tombstone => true,
        }
    }
}
