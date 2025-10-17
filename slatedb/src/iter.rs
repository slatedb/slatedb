use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::types::RowEntry;
use crate::types::{KeyValue, ValueDeletable};

#[derive(Clone, Copy, Debug)]
pub(crate) enum IterationOrder {
    Ascending,
    #[allow(dead_code)]
    Descending,
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12

#[async_trait]
pub trait KeyValueIterator: Send + Sync {
    /// Performs any expensive initialization required before regular iteration.
    ///
    /// This method should be idempotent and can be called multiple times, only
    /// the first initialization should perform expensive operations.
    async fn init(&mut self) -> Result<(), SlateDBError>;

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
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling next_entry and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError>;

    /// Seek to the next (inclusive) key
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling seek and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError>;
}

/// Initializes the iterator contained in the option, propagating `None` unchanged.
pub async fn init_optional_iterator<T: KeyValueIterator>(
    iter: Option<T>,
) -> Result<Option<T>, SlateDBError> {
    match iter {
        Some(mut it) => {
            it.init().await?;
            Ok(Some(it))
        }
        None => Ok(None),
    }
}

#[async_trait]
impl<'a> KeyValueIterator for Box<dyn KeyValueIterator + 'a> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.as_mut().init().await
    }

    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        self.as_mut().next().await
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.as_mut().next_entry().await
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.as_mut().seek(next_key).await
    }
}

pub(crate) struct EmptyIterator;

impl EmptyIterator {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl KeyValueIterator for EmptyIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(None)
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}
