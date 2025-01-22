use crate::error::SlateDBError;
use crate::types::RowEntry;
use crate::types::{KeyValue, ValueDeletable};

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
                            key: kv.key.into(),
                            value: v.into(),
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
}

pub(crate) trait SeekToKey {
    /// Seek to the next (inclusive) key
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError>;
}
