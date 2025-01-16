use std::cell::Cell;
use std::collections::VecDeque;
use std::ops::{RangeBounds, RangeFull};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{KeyValueIterator, SeekToKey};
use crate::merge_iterator::MergeIterator;
use crate::types::{RowAttributes, RowEntry, ValueDeletable};
use crate::utils::WatchableOnceCell;

pub(crate) struct KVTable {
    map: SkipMap<Bytes, ValueWithAttributes>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    size: AtomicUsize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    last_tick: AtomicI64,
}

pub(crate) struct WritableKVTable {
    table: Arc<KVTable>,
}

pub(crate) struct ImmutableMemtable {
    last_wal_id: u64,
    table: Arc<KVTable>,
    flushed: WatchableOnceCell<Result<(), SlateDBError>>,
}

pub(crate) struct ImmutableWal {
    id: u64,
    table: Arc<KVTable>,
}

type MemTableRange<'a, T> = Range<'a, Bytes, T, Bytes, ValueWithAttributes>;

pub(crate) struct MemTableIterator<'a, T: RangeBounds<Bytes>>(MemTableRange<'a, T>);

pub(crate) struct VecDequeKeyValueIterator {
    rows: VecDeque<RowEntry>,
}

impl VecDequeKeyValueIterator {
    pub(crate) fn new(rows: VecDeque<RowEntry>) -> Self {
        Self { rows }
    }

    pub(crate) async fn materialize_range(
        tables: VecDeque<Arc<KVTable>>,
        range: BytesRange,
    ) -> Result<Self, SlateDBError> {
        let memtable_iters = tables.iter().map(|t| t.range(range.clone())).collect();
        let mut merge_iter = MergeIterator::new(memtable_iters).await?;
        let mut rows = VecDeque::new();

        while let Some(row_entry) = merge_iter.next_entry().await? {
            rows.push_back(row_entry.clone());
        }

        Ok(VecDequeKeyValueIterator::new(rows))
    }
}

impl KeyValueIterator for VecDequeKeyValueIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.rows.pop_front())
    }
}

impl SeekToKey for VecDequeKeyValueIterator {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let front = self.rows.front();
            if front.is_some_and(|record| record.key < next_key) {
                self.rows.pop_front();
            } else {
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValueWithAttributes {
    pub(crate) value: ValueDeletable,
    pub(crate) attrs: RowAttributes,
}

impl<T: RangeBounds<Bytes>> KeyValueIterator for MemTableIterator<'_, T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }
}

impl<T: RangeBounds<Bytes>> MemTableIterator<'_, T> {
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
        Self {
            table: table.table,
            last_wal_id,
            flushed: WatchableOnceCell::new(),
        }
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }

    pub(crate) fn last_wal_id(&self) -> u64 {
        self.last_wal_id
    }

    pub(crate) async fn await_flush_to_l0(&self) -> Result<(), SlateDBError> {
        self.flushed.reader().await_value().await
    }

    pub(crate) fn notify_flush_to_l0(&self, result: Result<(), SlateDBError>) {
        self.flushed.write(result);
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
        self.table
            .put_or_delete(key, ValueDeletable::Value(value), attrs)
    }

    pub(crate) fn delete(&mut self, key: Bytes, attrs: RowAttributes) {
        self.table
            .put_or_delete(key, ValueDeletable::Tombstone, attrs);
    }

    pub(crate) fn size(&self) -> usize {
        self.table.size()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

impl KVTable {
    fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
            durable: WatchableOnceCell::new(),
            last_tick: AtomicI64::new(i64::MIN),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub(crate) fn last_tick(&self) -> i64 {
        self.last_tick.load(SeqCst)
    }

    /// Get the value for a given key.
    /// Returns None if the key is not in the memtable at all,
    /// Some(None) if the key is in the memtable but has a tombstone value,
    /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueWithAttributes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator<RangeFull> {
        self.range(..)
    }

    pub(crate) fn range<T: RangeBounds<Bytes>>(&self, range: T) -> MemTableIterator<T> {
        MemTableIterator(self.map.range(range))
    }

    /// Inserts a value, returning as soon as the value is written to the memtable but before
    /// it is flushed to durable storage.
    fn put_or_delete(&self, key: Bytes, value: ValueDeletable, attrs: RowAttributes) {
        let key_len = key.len();
        self.size.fetch_add(
            key_len + value.len() + sizeof_attributes(&attrs),
            Ordering::Relaxed,
        );

        // it is safe to use fetch_max here to update the last tick
        // because the monotonicity is enforced when generating the clock tick
        // (see [crate::utils::MonotonicClock::now])
        attrs.ts.map(|tick| self.last_tick.fetch_max(tick, SeqCst));

        let previous_size = Cell::new(None);
        self.map.compare_insert(
            key,
            ValueWithAttributes { value, attrs },
            |previous_value| {
                // Optimistically calculate the size of the previous value.
                let size =
                    key_len + previous_value.value.len() + sizeof_attributes(&previous_value.attrs);
                // `compare_fn` might be called multiple times in case of concurrent
                // writes to the same key, so we use `Cell` to avoid substracting
                // the size multiple times. The last call will set the correct size.
                previous_size.set(Some(size));
                true
            },
        );
        if let Some(size) = previous_size.take() {
            self.size.fetch_sub(size, Ordering::Relaxed);
        }
    }

    pub(crate) async fn await_durable(&self) -> Result<(), SlateDBError> {
        self.durable.reader().await_value().await
    }

    pub(crate) fn notify_durable(&self, result: Result<(), SlateDBError>) {
        self.durable.write(result);
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

        let mut iter = table.table().range(Bytes::from_static(b"abc333")..);
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

        let mut iter = table.table().range(Bytes::from_static(b"abc345")..);
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

        assert_eq!(table.table.size(), 0);
        table.put(
            Bytes::from_static(b"first"),
            Bytes::from_static(b"foo"),
            gen_attrs(1),
        );
        assert_eq!(table.table.size(), 16); // first(5) + foo(3) + attrs(8)

        // ensure that multiple deletes keep the table size stable
        for ts in 2..5 {
            table.delete(Bytes::from_static(b"first"), gen_attrs(ts));
            assert_eq!(table.table.size(), 13); // first(5) + attrs(8)
        }

        table.put(
            Bytes::from_static(b"abc333"),
            Bytes::from_static(b"val1"),
            gen_attrs(1),
        );
        assert_eq!(table.table.size(), 31); // 13 + abc333(6) + val1(4) + attrs(8)

        table.put(
            Bytes::from_static(b"def456"),
            Bytes::from_static(b"blablabla"),
            RowAttributes {
                ts: None,
                expire_ts: None,
            },
        );
        assert_eq!(table.table.size(), 46); // 31 + def456(6) + blablabla(9) + attrs(0)

        table.put(
            Bytes::from_static(b"def456"),
            Bytes::from_static(b"blabla"),
            gen_attrs(3),
        );
        assert_eq!(table.table.size(), 51); // 46 - blablabla(9) + blabla(6) - attrs(0) + attrs(8)

        table.delete(Bytes::from_static(b"abc333"), gen_attrs(4));
        assert_eq!(table.table.size(), 47) // 51 - val1(4)
    }
}
