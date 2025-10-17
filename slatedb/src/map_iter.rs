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
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.iterator.init().await
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestIterator;
    use bytes::Bytes;

    #[tokio::test]
    async fn should_apply_mapping_function_to_each_entry() {
        // given: an iterator with entries and a mapping function that modifies keys
        let iter = TestIterator::new()
            .with_entry(b"key1", b"value1", 1)
            .with_entry(b"key2", b"value2", 2);

        let map_fn = Box::new(|mut entry: RowEntry| {
            entry.key = Bytes::from(format!("mapped_{}", String::from_utf8_lossy(&entry.key)));
            entry
        });

        // when: creating a map iterator
        let mut map_iter = MapIterator::new(iter, map_fn);

        // then: entries should have mapped keys
        let entry1 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry1.key, Bytes::from("mapped_key1"));
        assert_eq!(entry1.value, ValueDeletable::Value(Bytes::from("value1")));

        let entry2 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry2.key, Bytes::from("mapped_key2"));
        assert_eq!(entry2.value, ValueDeletable::Value(Bytes::from("value2")));

        assert!(map_iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_return_none_when_underlying_iterator_is_empty() {
        // given: an empty iterator and a mapping function
        let iter = TestIterator::new();
        let map_fn = Box::new(|entry: RowEntry| entry);

        // when: creating a map iterator
        let mut map_iter = MapIterator::new(iter, map_fn);

        // then: should return None immediately
        assert!(map_iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_convert_expired_entries_to_tombstones_with_ttl() {
        // given: an iterator with entries having different expiration times
        let iter = TestIterator::new()
            .with_row_entry(RowEntry::new_value(b"key1", b"value1", 1).with_expire_ts(100)) // not expired
            .with_row_entry(RowEntry::new_value(b"key2", b"value2", 2).with_expire_ts(50)) // expired
            .with_row_entry(RowEntry::new_value(b"key3", b"value3", 3).with_expire_ts(150)); // not expired

        // when: creating a map iterator with TTL now = 80
        let mut map_iter = MapIterator::new_with_ttl_now(iter, 80);

        // then: expired entry should be converted to tombstone
        let entry1 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry1.key, Bytes::from("key1"));
        assert_eq!(entry1.value, ValueDeletable::Value(Bytes::from("value1")));
        assert_eq!(entry1.expire_ts, Some(100));

        let entry2 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry2.key, Bytes::from("key2"));
        assert_eq!(entry2.value, ValueDeletable::Tombstone);
        assert_eq!(entry2.expire_ts, None); // cleared on tombstone

        let entry3 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry3.key, Bytes::from("key3"));
        assert_eq!(entry3.value, ValueDeletable::Value(Bytes::from("value3")));
        assert_eq!(entry3.expire_ts, Some(150));
    }

    #[tokio::test]
    async fn should_not_modify_entries_without_expiration_when_using_ttl() {
        // given: an iterator with entries without expiration timestamps
        let iter = TestIterator::new()
            .with_entry(b"key1", b"value1", 1)
            .with_entry(b"key2", b"value2", 2);

        // when: creating a map iterator with TTL
        let mut map_iter = MapIterator::new_with_ttl_now(iter, 100);

        // then: entries should pass through unchanged
        let entry1 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry1.key, Bytes::from("key1"));
        assert_eq!(entry1.value, ValueDeletable::Value(Bytes::from("value1")));
        assert_eq!(entry1.expire_ts, None);

        let entry2 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry2.key, Bytes::from("key2"));
        assert_eq!(entry2.value, ValueDeletable::Value(Bytes::from("value2")));
        assert_eq!(entry2.expire_ts, None);
    }

    #[tokio::test]
    async fn should_preserve_sequence_numbers_when_mapping() {
        // given: an iterator with entries having specific sequence numbers
        let iter = TestIterator::new()
            .with_entry(b"key1", b"value1", 42)
            .with_entry(b"key2", b"value2", 100);

        let map_fn = Box::new(|mut entry: RowEntry| {
            // Modify value but keep seq
            if let ValueDeletable::Value(v) = entry.value {
                entry.value = ValueDeletable::Value(Bytes::from(format!(
                    "modified_{}",
                    String::from_utf8_lossy(&v)
                )));
            }
            entry
        });

        // when: creating a map iterator
        let mut map_iter = MapIterator::new(iter, map_fn);

        // then: sequence numbers should be preserved
        let entry1 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry1.seq, 42);
        assert_eq!(
            entry1.value,
            ValueDeletable::Value(Bytes::from("modified_value1"))
        );

        let entry2 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(entry2.seq, 100);
        assert_eq!(
            entry2.value,
            ValueDeletable::Value(Bytes::from("modified_value2"))
        );
    }

    #[tokio::test]
    async fn should_handle_tombstone_entries() {
        // given: an iterator with mixed value and tombstone entries
        let iter = TestIterator::new()
            .with_row_entry(RowEntry::new_value(b"key1", b"value1", 1))
            .with_row_entry(RowEntry::new_tombstone(b"key2", 2))
            .with_row_entry(RowEntry::new_value(b"key3", b"value3", 3));

        let map_fn = Box::new(|entry: RowEntry| entry);

        // when: creating a map iterator
        let mut map_iter = MapIterator::new(iter, map_fn);

        // then: should preserve tombstone entries
        let result1 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(result1.key, Bytes::from("key1"));
        assert!(matches!(result1.value, ValueDeletable::Value(_)));

        let result2 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(result2.key, Bytes::from("key2"));
        assert_eq!(result2.value, ValueDeletable::Tombstone);

        let result3 = map_iter.next_entry().await.unwrap().unwrap();
        assert_eq!(result3.key, Bytes::from("key3"));
        assert!(matches!(result3.value, ValueDeletable::Value(_)));
    }
}
