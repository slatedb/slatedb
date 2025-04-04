use crate::error::SlateDBError;
use crate::iter::IterationOrder::{Ascending, Descending};
use crate::types::RowEntry;
use crate::types::{KeyValue, ValueDeletable};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IterationOrder {
    Ascending,
    Descending,
}

impl IterationOrder {
    /// Compare two values using the iteration order.
    /// Return true if `a` should be ordered before `b`, and false otherwise
    pub(crate) fn precedes<T: PartialOrd>(&self, a: T, b: T) -> bool {
        match self {
            Ascending => a < b,
            Descending => b < a,
        }
    }
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12
pub trait KeyValueIterator {
    /// Returns the next non-deleted key-value pair in the iterator.
    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        loop {
            let entry = self.next_entry().await?;
            if let Some(kv) = entry {
                match kv.value {
                    ValueDeletable::Value(v) => {
                        return Ok(Some(KeyValue {
                            key: kv.key,
                            value: v,
                        }))
                    }
                    ValueDeletable::Merge(_) => todo!(),
                    ValueDeletable::Tombstone => continue,
                }
            } else {
                return Ok(None);
            }
        }
    }

    /// Returns the next entry in the iterator, which may be a key-value pair or
    /// a tombstone of a deleted key-value pair.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError>;

    /// Return the order of this iterator
    fn order(&self) -> IterationOrder {
        Ascending
    }
}

pub(crate) trait SeekToKey {
    /// Seek to the next (inclusive) key
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError>;
}
