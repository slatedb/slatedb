use std::cell::Cell;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::WatchableOnceCell;

/// Memtable may contains multiple versions of a single user key, with a monotonically increasing sequence number.
#[derive(Debug, Clone, Eq, PartialEq)]
struct LookupKey {
    user_key: Bytes,
    seq: u64,
}

impl LookupKey {
    pub fn new(user_key: Bytes, seq: u64) -> Self {
        Self { user_key, seq }
    }
}

impl Ord for LookupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // TODO: should we put the higher seq number first?
        (&self.user_key, self.seq).cmp(&(&other.user_key, other.seq))
    }
}

impl PartialOrd for LookupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct KVTable {
    map: SkipMap<LookupKey, RowEntry>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    size: AtomicUsize,
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

type MemTableIterInner<'a> =
    Range<'a, LookupKey, (Bound<LookupKey>, Bound<LookupKey>), LookupKey, RowEntry>;

pub struct MemTableIterator<'a>(MemTableIterInner<'a>);

impl<'a> KeyValueIterator for MemTableIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }
}

impl MemTableIterator<'_> {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        self.0.next().map(|entry| entry.value().clone())
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

    pub(crate) fn put(&mut self, row: RowEntry) {
        self.table.put(row)
    }

    pub(crate) fn size(&self) -> usize {
        self.table.size()
    }
}

impl KVTable {
    fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
            durable: WatchableOnceCell::new(),
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
    pub(crate) fn get(&self, key: &[u8]) -> Option<RowEntry> {
        let start_key = LookupKey::new(Bytes::from(key.to_vec()), 0);
        let end_key = LookupKey::new(Bytes::from(key.to_vec()), u64::MAX);
        let bounds = (Bound::Included(start_key), Bound::Included(end_key));
        self.map
            .range(bounds)
            .next_back()
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        let bounds = (Bound::Unbounded, Bound::Unbounded);
        MemTableIterator(self.map.range(bounds))
    }

    #[allow(dead_code)] // will be used in #8
    pub(crate) fn range_from(&self, start: Bytes) -> MemTableIterator {
        let bounds = (Bound::Included(LookupKey::new(start, 0)), Bound::Unbounded);
        MemTableIterator(self.map.range(bounds))
    }

    /// Inserts a value, returning as soon as the value is written to the memtable but before
    /// it is flushed to durable storage.
    fn put(&self, row: RowEntry) {
        self.size.fetch_add(row.estimated_size(), Ordering::Relaxed);
        let lookup_key = LookupKey::new(row.key.clone(), row.seq);
        let previous_size = Cell::new(None);
        // TODO: memtable is considered as append only, so i suppose we do not need consider removing the previous row here
        self.map.compare_insert(lookup_key, row, |previous_row| {
            // Optimistically calculate the size of the previous value.
            // `compare_fn` might be called multiple times in case of concurrent
            // writes to the same key, so we use `Cell` to avoid substracting
            // the size multiple times. The last call will set the correct size.
            previous_size.set(Some(previous_row.estimated_size()));
            true
        });
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

#[cfg(test)]
mod tests {
    use crate::types::ValueDeletable;

    use super::*;

    #[tokio::test]
    async fn test_memtable_iter() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc111"),
            value: ValueDeletable::Value(Bytes::from_static(b"value1")),
            create_ts: None,
            expire_ts: None,
            seq: 2,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc555"),
            value: ValueDeletable::Value(Bytes::from_static(b"value5")),
            create_ts: None,
            expire_ts: None,
            seq: 3,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc444"),
            value: ValueDeletable::Value(Bytes::from_static(b"value4")),
            create_ts: None,
            expire_ts: None,
            seq: 4,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc222"),
            value: ValueDeletable::Value(Bytes::from_static(b"value2")),
            create_ts: None,
            expire_ts: None,
            seq: 5,
        });

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
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc111"),
            value: ValueDeletable::Value(Bytes::from_static(b"value1")),
            create_ts: None,
            expire_ts: None,
            seq: 2,
        });

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
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc111"),
            value: ValueDeletable::Value(Bytes::from_static(b"value1")),
            create_ts: None,
            expire_ts: None,
            seq: 2,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc555"),
            value: ValueDeletable::Value(Bytes::from_static(b"value5")),
            create_ts: None,
            expire_ts: None,
            seq: 3,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc444"),
            value: ValueDeletable::Value(Bytes::from_static(b"value4")),
            create_ts: None,
            expire_ts: None,
            seq: 4,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc222"),
            value: ValueDeletable::Value(Bytes::from_static(b"value2")),
            create_ts: None,
            expire_ts: None,
            seq: 5,
        });

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
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc111"),
            value: ValueDeletable::Value(Bytes::from_static(b"value1")),
            create_ts: None,
            expire_ts: None,
            seq: 2,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc555"),
            value: ValueDeletable::Value(Bytes::from_static(b"value5")),
            create_ts: None,
            expire_ts: None,
            seq: 3,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc444"),
            value: ValueDeletable::Value(Bytes::from_static(b"value4")),
            create_ts: None,
            expire_ts: None,
            seq: 4,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc222"),
            value: ValueDeletable::Value(Bytes::from_static(b"value2")),
            create_ts: None,
            expire_ts: None,
            seq: 5,
        });

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
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Tombstone,
            create_ts: None,
            expire_ts: None,
            seq: 2,
        });

        let value = table.table().get(b"abc333").map(|entry| entry.value);
        assert_eq!(value, Some(ValueDeletable::Tombstone));

        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"value3")),
            create_ts: None,
            expire_ts: None,
            seq: 3,
        });

        let value = table.table().get(b"abc333").map(|entry| entry.value);
        assert_eq!(
            value,
            Some(ValueDeletable::Value(Bytes::from_static(b"value3")))
        );
    }

    #[tokio::test]
    async fn test_memtable_track_sz() {
        let mut table = WritableKVTable::new();

        assert_eq!(table.table.size(), 0);
        table.put(RowEntry {
            key: Bytes::from_static(b"first"),
            value: ValueDeletable::Value(Bytes::from_static(b"foo")),
            create_ts: Some(1),
            expire_ts: None,
            seq: 1,
        });
        assert_eq!(table.table.size(), 24);

        // ensure that multiple deletes keep the table size stable
        for ts in 2..5 {
            table.put(RowEntry {
                key: Bytes::from_static(b"first"),
                value: ValueDeletable::Tombstone,
                create_ts: Some(ts),
                expire_ts: None,
                seq: 1,
            });
            assert_eq!(table.table.size(), 21);
        }

        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Value(Bytes::from_static(b"val1")),
            create_ts: Some(1),
            expire_ts: None,
            seq: 1,
        });
        assert_eq!(table.table.size(), 47);

        table.put(RowEntry {
            key: Bytes::from_static(b"def456"),
            value: ValueDeletable::Value(Bytes::from_static(b"blablabla")),
            create_ts: None,
            expire_ts: None,
            seq: 1,
        });
        assert_eq!(table.table.size(), 70);

        table.put(RowEntry {
            key: Bytes::from_static(b"def456"),
            value: ValueDeletable::Value(Bytes::from_static(b"blabla")),
            create_ts: Some(3),
            expire_ts: None,
            seq: 3,
        });
        assert_eq!(table.table.size(), 98);

        table.put(RowEntry {
            key: Bytes::from_static(b"abc333"),
            value: ValueDeletable::Tombstone,
            create_ts: Some(4),
            expire_ts: None,
            seq: 4,
        });
        assert_eq!(table.table.size(), 120);
    }
}
