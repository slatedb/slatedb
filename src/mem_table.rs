use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::{KVEntry, KVValue};
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::Notify;

pub(crate) struct MemTable {
    pub(crate) map: Arc<SkipMap<Bytes, Option<Bytes>>>,
    pub(crate) flush_notify: Arc<Notify>,
}

type MemTableRange<'a> = Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Option<Bytes>>;

pub struct MemTableIterator<'a>(MemTableRange<'a>);

impl<'a> KeyValueIterator for MemTableIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<KVEntry>, SlateDBError> {
        Ok(self.0.next().map(|entry| {
            let v = match entry.value() {
                Some(v) => KVValue::Value(v.clone()),
                None => KVValue::Tombstone,
            };

            KVEntry {
                key: entry.key().clone(),
                value: v,
            }
        }))
    }
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            flush_notify: Arc::new(Notify::new()),
        }
    }

    /// Get the value for a given key.
    /// Returns None if the key is not in the memtable at all,
    /// Some(None) if the key is in the memtable but has a tombstone value,
    /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
    pub(crate) fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        let bounds = (Bound::Unbounded, Bound::Unbounded);
        MemTableIterator(self.map.range(bounds))
    }

    #[allow(dead_code)] // will be used in #8
    pub(crate) fn range_from(&self, start: &[u8]) -> MemTableIterator {
        let bounds = (
            Bound::Included(Bytes::copy_from_slice(start)),
            Bound::Unbounded,
        );
        MemTableIterator(self.map.range(bounds))
    }

    /// Puts a value, returning as soon as the value is written to the memtable but before
    /// it is flushed to durable storage.
    #[allow(dead_code)] // will be used in #8
    pub(crate) fn put_optimistic(&self, key: &[u8], value: &[u8]) {
        self.map.insert(
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
        );
    }

    /// Puts a value and waits for the value to be flushed to durable storage.
    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
        );
        self.flush_notify.notified().await;
    }

    pub(crate) async fn delete(&self, key: &[u8]) {
        self.map.insert(Bytes::copy_from_slice(key), None);
        self.flush_notify.notified().await;
    }

    #[allow(dead_code)]
    pub(crate) fn delete_optimistic(&self, key: &[u8]) {
        self.map.insert(Bytes::copy_from_slice(key), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memtable_iter() {
        let table = MemTable::new();
        table.put_optimistic(b"abc333", b"value3");
        table.put_optimistic(b"abc111", b"value1");
        table.put_optimistic(b"abc555", b"value5");
        table.put_optimistic(b"abc444", b"value4");
        table.put_optimistic(b"abc222", b"value2");

        let mut iter = table.iter();
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc111".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc222".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc333".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc444".as_slice());
        assert_eq!(kv.value, b"value4".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc555".as_slice());
        assert_eq!(kv.value, b"value5".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_range_from_existing_key() {
        let table = MemTable::new();
        table.put_optimistic(b"abc333", b"value3");
        table.put_optimistic(b"abc111", b"value1");
        table.put_optimistic(b"abc555", b"value5");
        table.put_optimistic(b"abc444", b"value4");
        table.put_optimistic(b"abc222", b"value2");

        let mut iter = table.range_from(b"abc333");
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc333".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc444".as_slice());
        assert_eq!(kv.value, b"value4".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc555".as_slice());
        assert_eq!(kv.value, b"value5".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_range_from_nonexisting_key() {
        let table = MemTable::new();
        table.put_optimistic(b"abc333", b"value3");
        table.put_optimistic(b"abc111", b"value1");
        table.put_optimistic(b"abc555", b"value5");
        table.put_optimistic(b"abc444", b"value4");
        table.put_optimistic(b"abc222", b"value2");

        let mut iter = table.range_from(b"abc345");
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc444".as_slice());
        assert_eq!(kv.value, b"value4".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc555".as_slice());
        assert_eq!(kv.value, b"value5".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_iter_delete() {
        let table = MemTable::new();
        table.put_optimistic(b"abc333", b"value3");
        table.delete_optimistic(b"abc333");

        let mut iter = table.iter();
        assert!(iter.next().await.unwrap().is_none());
    }
}
