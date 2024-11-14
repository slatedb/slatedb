use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use tokio::sync::watch;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::{RowAttributes, RowEntry, ValueDeletable};

pub(crate) struct KVTable {
    map: SkipMap<Bytes, ValueWithAttributes>,
    is_durable_tx: watch::Sender<bool>,
    is_durable_rx: watch::Receiver<bool>,
    size: AtomicUsize,
}

pub(crate) struct WritableKVTable {
    table: Arc<KVTable>,
}

pub(crate) struct ImmutableMemtable {
    last_wal_id: u64,
    table: Arc<KVTable>,
    is_flushed_tx: watch::Sender<bool>,
    is_flushed_rx: watch::Receiver<bool>,
}

pub(crate) struct ImmutableWal {
    id: u64,
    table: Arc<KVTable>,
}

type MemTableRange<'a> = Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, ValueWithAttributes>;

pub struct MemTableIterator<'a>(MemTableRange<'a>);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValueWithAttributes {
    pub(crate) value: ValueDeletable,
    pub(crate) attrs: RowAttributes,
}

impl<'a> KeyValueIterator for MemTableIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }
}

impl<'a> MemTableIterator<'a> {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        self.0.next().map(|entry| RowEntry {
            key: entry.key().clone(),
            value: entry.value().value.clone(),
            seq: 0,
            create_ts: entry.value().attrs.ts,
            expire_ts: entry.value().attrs.expire_ts,
        })
    }
}

impl ImmutableMemtable {
    pub(crate) fn new(table: WritableKVTable, last_wal_id: u64) -> Self {
        let (is_flushed_tx, is_flushed_rx) = watch::channel(false);
        Self {
            table: table.table,
            last_wal_id,
            is_flushed_tx,
            is_flushed_rx,
        }
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }

    pub(crate) fn last_wal_id(&self) -> u64 {
        self.last_wal_id
    }

    pub(crate) async fn await_flush_to_l0(&self) {
        let mut rx = self.is_flushed_rx.clone();
        while !*rx.borrow_and_update() {
            rx.changed().await.expect("watch channel closed");
        }
    }

    pub(crate) fn notify_flush_to_l0(&self) {
        self.is_flushed_tx.send(true).expect("watch channel closed");
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
        }
    }

    pub(crate) fn table(&self) -> &Arc<KVTable> {
        &self.table
    }

    pub(crate) fn put(&mut self, key: Bytes, value: Bytes, attrs: RowAttributes) {
        self.table.put(key, value, attrs)
    }

    pub(crate) fn delete(&mut self, key: Bytes, attrs: RowAttributes) {
        self.table.delete(key, attrs);
    }

    pub(crate) fn size(&self) -> usize {
        self.table.size()
    }
}

impl KVTable {
    fn new() -> Self {
        let (is_durable_tx, is_durable_rx) = watch::channel(false);
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
            is_durable_tx,
            is_durable_rx,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get the value for a given key.
    /// Returns None if the key is not in the memtable at all,
    /// Some(None) if the key is in the memtable but has a tombstone value,
    /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueWithAttributes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        let bounds = (Bound::Unbounded, Bound::Unbounded);
        MemTableIterator(self.map.range(bounds))
    }

    #[allow(dead_code)] // will be used in #8
    pub(crate) fn range_from(&self, start: Bytes) -> MemTableIterator {
        let bounds = (Bound::Included(start), Bound::Unbounded);
        MemTableIterator(self.map.range(bounds))
    }

    /// Puts a value, returning as soon as the value is written to the memtable but before
    /// it is flushed to durable storage.
    fn put(&self, key: Bytes, value: Bytes, attrs: RowAttributes) {
        self.maybe_subtract_old_val_from_size(key.clone());
        self.size.fetch_add(
            key.len() + value.len() + sizeof_attributes(&attrs),
            Ordering::Relaxed,
        );
        self.map.insert(
            key,
            ValueWithAttributes {
                value: ValueDeletable::Value(value),
                attrs,
            },
        );
    }

    fn delete(&self, key: Bytes, attrs: RowAttributes) {
        self.maybe_subtract_old_val_from_size(key.clone());
        self.size.fetch_add(key.len(), Ordering::Relaxed);
        self.map.insert(
            key,
            ValueWithAttributes {
                value: ValueDeletable::Tombstone,
                attrs,
            },
        );
    }

    fn maybe_subtract_old_val_from_size(&self, key: Bytes) {
        if let Some(old_deletable) = self.get(&key) {
            let old_size = key.len()
                + match old_deletable.value {
                    ValueDeletable::Tombstone => 0,
                    ValueDeletable::Value(old) => old.len(),
                }
                + sizeof_attributes(&old_deletable.attrs);
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
    }

    pub(crate) async fn await_durable(&self) {
        let mut rx = self.is_durable_rx.clone();
        while !*rx.borrow_and_update() {
            rx.changed().await.expect("watch channel closed");
        }
    }

    pub(crate) fn notify_durable(&self) {
        self.is_durable_tx.send(true).expect("watch channel closed");
    }
}

fn sizeof_attributes(attrs: &RowAttributes) -> usize {
    attrs.ts.map(|_| 8).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::gen_attrs;

    #[tokio::test]
    async fn test_memtable_iter() {
        let mut table = WritableKVTable::new();
        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"value3"),
            gen_attrs(1),
        );
        table.put(
            Bytes::from_static(b"abc111"),
            Bytes::from_static(b"value1"),
            gen_attrs(2),
        );
        table.put(
            Bytes::from_static(b"abc555"),
            Bytes::from_static(b"value5"),
            gen_attrs(3),
        );
        table.put(
            Bytes::from_static(b"abc444"),
            Bytes::from_static(b"value4"),
            gen_attrs(4),
        );
        table.put(
            Bytes::from_static(b"abc222"),
            Bytes::from_static(b"value2"),
            gen_attrs(5),
        );

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
    async fn test_memtable_iter_entry_attrs() {
        let mut table = WritableKVTable::new();
        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"value3"),
            gen_attrs(1),
        );
        table.put(
            Bytes::from_static(b"abc111"),
            Bytes::from_static(b"value1"),
            gen_attrs(2),
        );

        let mut iter = table.table().iter();
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc111".as_slice());
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc333".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_range_from_existing_key() {
        let mut table = WritableKVTable::new();
        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"value3"),
            gen_attrs(1),
        );
        table.put(
            Bytes::from_static(b"abc111"),
            Bytes::from_static(b"value1"),
            gen_attrs(2),
        );
        table.put(
            Bytes::from_static(b"abc555"),
            Bytes::from_static(b"value5"),
            gen_attrs(3),
        );
        table.put(
            Bytes::from_static(b"abc444"),
            Bytes::from_static(b"value4"),
            gen_attrs(4),
        );
        table.put(
            Bytes::from_static(b"abc222"),
            Bytes::from_static(b"value2"),
            gen_attrs(5),
        );

        let mut iter = table.table().range_from(Bytes::from_static(b"abc333"));
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
        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"value3"),
            gen_attrs(1),
        );
        table.put(
            Bytes::from_static(b"abc111"),
            Bytes::from_static(b"value1"),
            gen_attrs(2),
        );
        table.put(
            Bytes::from_static(b"abc555"),
            Bytes::from_static(b"value5"),
            gen_attrs(3),
        );
        table.put(
            Bytes::from_static(b"abc444"),
            Bytes::from_static(b"value4"),
            gen_attrs(4),
        );
        table.put(
            Bytes::from_static(b"abc222"),
            Bytes::from_static(b"value2"),
            gen_attrs(5),
        );

        let mut iter = table.table().range_from(Bytes::from_static(b"abc345"));
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
        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"value3"),
            gen_attrs(1),
        );
        table.delete(Bytes::from_static(b"abc333"), gen_attrs(2));

        let mut iter = table.table().iter();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memtable_track_sz() {
        let mut table = WritableKVTable::new();

        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"val1"),
            gen_attrs(1),
        );
        assert_eq!(table.table.size(), 18);

        table.put(
            Bytes::from_static(b"def456"),
            Bytes::from_static(b"blablabla"),
            RowAttributes {
                ts: None,
                expire_ts: None,
            },
        );
        assert_eq!(table.table.size(), 33);

        table.put(
            Bytes::from_static(b"def456"),
            Bytes::from_static(b"blabla"),
            gen_attrs(3),
        );
        assert_eq!(table.table.size(), 38);

        table.delete(Bytes::from_static(b"abc333"), gen_attrs(4));
        assert_eq!(table.table.size(), 26)
    }
}
