use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::{KeyValueDeletable, ValueDeletable};
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::Notify;

pub(crate) struct KVTable {
    map: SkipMap<Bytes, ValueDeletable>,
    flush_notify: Arc<Notify>,
}

pub(crate) struct WritableKVTable {
    table: Arc<KVTable>,
    size: usize,
}

pub(crate) struct ImmutableMemtable {
    last_wal_id: u64,
    table: Arc<KVTable>,
}

pub(crate) struct ImmutableWal {
    id: u64,
    table: Arc<KVTable>,
}

type MemTableRange<'a> = Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, ValueDeletable>;

pub struct MemTableIterator<'a>(MemTableRange<'a>);

impl<'a> KeyValueIterator for MemTableIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
        Ok(self.0.next().map(|entry| KeyValueDeletable {
            key: entry.key().clone(),
            value: entry.value().clone(),
        }))
    }
}

impl ImmutableMemtable {
    pub(crate) fn new(table: WritableKVTable, last_wal_id: u64) -> Self {
        Self {
            table: table.table,
            last_wal_id,
        }
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }

    pub(crate) fn last_wal_id(&self) -> u64 {
        self.last_wal_id
    }
}

impl ImmutableWal {
    pub(crate) fn new(id: u64, table: WritableKVTable) -> Self {
        Self {
            id,
            table: table.table,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }
}

impl WritableKVTable {
    pub(crate) fn new() -> Self {
        Self {
            table: Arc::new(KVTable::new()),
            size: 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn table(&self) -> &Arc<KVTable> {
        &self.table
    }

    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) {
        self.maybe_subtract_old_val_from_size(key);
        self.size = self.size + key.len() + value.len();
        self.table.put(key, value)
    }

    pub(crate) fn delete(&mut self, key: &[u8]) {
        self.maybe_subtract_old_val_from_size(key);
        self.size += key.len();
        self.table.delete(key);
    }

    fn maybe_subtract_old_val_from_size(&mut self, key: &[u8]) {
        if let Some(old_deletable) = self.table.get(key) {
            self.size = self.size
                - key.len()
                - match old_deletable {
                    ValueDeletable::Tombstone => 0,
                    ValueDeletable::Value(old) => old.len(),
                }
        }
    }
}

impl KVTable {
    fn new() -> Self {
        Self {
            map: SkipMap::new(),
            flush_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Get the value for a given key.
    /// Returns None if the key is not in the memtable at all,
    /// Some(None) if the key is in the memtable but has a tombstone value,
    /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueDeletable> {
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
    fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(
            Bytes::copy_from_slice(key),
            ValueDeletable::Value(Bytes::copy_from_slice(value)),
        );
    }

    fn delete(&self, key: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), ValueDeletable::Tombstone);
    }

    pub(crate) async fn await_flush(&self) {
        self.flush_notify.notified().await;
    }

    pub(crate) fn notify_flush(&self) {
        self.flush_notify.notify_waiters()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memtable_iter() {
        let mut table = WritableKVTable::new();
        table.put(b"abc333", b"value3");
        table.put(b"abc111", b"value1");
        table.put(b"abc555", b"value5");
        table.put(b"abc444", b"value4");
        table.put(b"abc222", b"value2");

        let mut iter = table.table().iter();
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
        let mut table = WritableKVTable::new();
        table.put(b"abc333", b"value3");
        table.put(b"abc111", b"value1");
        table.put(b"abc555", b"value5");
        table.put(b"abc444", b"value4");
        table.put(b"abc222", b"value2");

        let mut iter = table.table().range_from(b"abc333");
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
        let mut table = WritableKVTable::new();
        table.put(b"abc333", b"value3");
        table.put(b"abc111", b"value1");
        table.put(b"abc555", b"value5");
        table.put(b"abc444", b"value4");
        table.put(b"abc222", b"value2");

        let mut iter = table.table().range_from(b"abc345");
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
        let mut table = WritableKVTable::new();
        table.put(b"abc333", b"value3");
        table.delete(b"abc333");

        let mut iter = table.table().iter();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_track_sz() {
        let mut table = WritableKVTable::new();

        table.put(b"abc333", b"val1");
        assert_eq!(table.size(), 10);

        table.put(b"def456", b"blablabla");
        assert_eq!(table.size(), 25);

        table.put(b"def456", b"blabla");
        assert_eq!(table.size(), 22);

        table.delete(b"abc333");
        assert_eq!(table.size(), 18)
    }
}
