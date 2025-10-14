use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::is_not_expired;

/// An iterator adapter that applies a user-provided mapping function to each
/// [`RowEntry`] yielded by the underlying iterator.
///
/// # Examples
/// ```ignore
/// // Example usage: wrap an iterator to transform every value to uppercase.
/// let map_iter = MapIterator::new(inner_iter, Box::new(|mut entry| {
///     entry.value = entry.value.to_uppercase();
///     entry
/// }));
/// ```
pub(crate) struct MapIterator<T: KeyValueIterator> {
    /// The underlying iterator providing [`RowEntry`] elements.
    iterator: T,
    /// The mapping function applied to each row entry.
    f: Box<dyn Fn(RowEntry) -> RowEntry + Send + Sync>,
}

impl<T: KeyValueIterator> MapIterator<T> {
    pub(crate) fn new(iterator: T, f: Box<dyn Fn(RowEntry) -> RowEntry + Send + Sync>) -> Self {
        Self { iterator, f }
    }

    pub(crate) fn new_with_ttl_now(iterator: T, ttl_now: i64) -> Self {
        let f = Box::new(move |entry: RowEntry| {
            if is_not_expired(&entry, ttl_now) {
                entry
            } else {
                RowEntry {
                    key: entry.key,
                    value: ValueDeletable::Tombstone,
                    seq: entry.seq,
                    create_ts: entry.create_ts,
                    expire_ts: None,
                }
            }
        });
        Self::new(iterator, f)
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for MapIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let next_entry = self.iterator.next_entry().await?;
        if let Some(entry) = next_entry {
            Ok(Some((self.f)(entry)))
        } else {
            Ok(None)
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.iterator.seek(next_key).await
    }
}
