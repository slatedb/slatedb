use std::cell::Cell;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{KeyValueIterator, SeekToKey};
use crate::merge_iterator::MergeIterator;
use crate::types::RowEntry;
use crate::utils::WatchableOnceCell;

/// Memtable may contains multiple versions of a single user key, with a monotonically increasing sequence number.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct KVTableInternalKey {
    user_key: Bytes,
    seq: u64,
}

impl KVTableInternalKey {
    pub fn new(user_key: Bytes, seq: u64) -> Self {
        Self { user_key, seq }
    }
}

impl Ord for KVTableInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(self.seq.cmp(&other.seq).reverse())
    }
}

impl PartialOrd for KVTableInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KVTableInternalKeyRange {
    start_bound: Bound<KVTableInternalKey>,
    end_bound: Bound<KVTableInternalKey>,
}

impl RangeBounds<KVTableInternalKey> for KVTableInternalKeyRange {
    fn start_bound(&self) -> Bound<&KVTableInternalKey> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> Bound<&KVTableInternalKey> {
        self.end_bound.as_ref()
    }
}

impl From<BytesRange> for KVTableInternalKeyRange {
    fn from(range: BytesRange) -> Self {
        let start_bound = match range.start_bound() {
            Bound::Included(key) => Bound::Included(KVTableInternalKey::new(key.clone(), u64::MAX)),
            Bound::Excluded(key) => Bound::Included(KVTableInternalKey::new(key.clone(), u64::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(KVTableInternalKey::new(key.clone(), 0)),
            Bound::Excluded(key) => Bound::Included(KVTableInternalKey::new(key.clone(), 0)),
            Bound::Unbounded => Bound::Unbounded,
        };
        Self {
            start_bound,
            end_bound,
        }
    }
}

pub(crate) struct KVTable {
    map: SkipMap<KVTableInternalKey, RowEntry>,
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

pub(crate) struct MemTableIterator<'a, T: RangeBounds<KVTableInternalKey>> {
    /// The kv table internal key range is considered as wider than the user key range since it includes sequence
    /// numbers. For example, with keys ("key001", seq=1), ("key002", seq=2), ("key002", seq=3), ("key003", seq=4),
    /// if the user specifies a range of Excluded("key002"), we cannot directly create
    /// a KVTableInternalKeyRange that equivant with Excluded("key002") that filter out all the sequence numbers.
    /// We have to store the original user key range with an additional filter to handle the Excluded case.
    user_key_range: BytesRange,
    /// `inner` is the Iterator impl of SkipMap, which is the underlying data structure of MemTable.
    inner: Range<'a, KVTableInternalKey, T, KVTableInternalKey, RowEntry>,
}

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
            if front.map_or(false, |record| record.key < next_key) {
                self.rows.pop_front();
            } else {
                return Ok(());
            }
        }
    }
}

impl<'a, T: RangeBounds<KVTableInternalKey>> KeyValueIterator for MemTableIterator<'a, T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }
}

impl<T: RangeBounds<KVTableInternalKey>> MemTableIterator<'_, T> {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        for entry in self.inner.by_ref() {
            if self.user_key_range.contains(&entry.key().user_key) {
                return Some(entry.value().clone());
            }
        }
        None
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
        self.table.put(row);
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
        let user_key = Bytes::from(key.to_vec());
        let range = KVTableInternalKeyRange::from(BytesRange::new(
            Bound::Included(user_key.clone()),
            Bound::Included(user_key),
        ));
        self.map
            .range(range)
            .next()
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator<KVTableInternalKeyRange> {
        self.range(BytesRange::from(..))
    }

    pub(crate) fn range(&self, range: BytesRange) -> MemTableIterator<KVTableInternalKeyRange> {
        MemTableIterator {
            user_key_range: range.clone(),
            inner: self.map.range(KVTableInternalKeyRange::from(range)),
        }
    }

    fn put(&self, row: RowEntry) {
        self.size.fetch_add(row.estimated_size(), Ordering::Relaxed);
        let internal_key = KVTableInternalKey::new(row.key.clone(), row.seq);
        let previous_size = Cell::new(None);

        self.map.compare_insert(internal_key, row, |previous_row| {
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
    use super::*;
    use crate::test_utils::assert_iterator;

    #[tokio::test]
    async fn test_memtable_iter() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc222", b"value2", 5),
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_entry_attrs() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc333", b"value3", 1),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_existing_key() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));

        let mut iter = table
            .table()
            .range(BytesRange::from(Bytes::from_static(b"abc333")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_nonexisting_key() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));

        let mut iter = table
            .table()
            .range(BytesRange::from(Bytes::from_static(b"abc334")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_delete() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_tombstone(b"abc333", 2));
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));

        // in merge iterator, it should only return one entry
        let iter = table.table().iter();
        let mut merge_iter = MergeIterator::new(VecDeque::from(vec![iter]))
            .await
            .unwrap();
        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_tombstone(b"abc333", 2),
                RowEntry::new_value(b"abc444", b"value4", 4),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_track_sz() {
        let mut table = WritableKVTable::new();

        assert_eq!(table.table.size(), 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1));
        assert_eq!(table.table.size(), 16);

        table.put(RowEntry::new_tombstone(b"first", 2));
        assert_eq!(table.table.size(), 29);

        table.put(RowEntry::new_tombstone(b"first", 2));
        assert_eq!(table.table.size(), 29);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1));
        assert_eq!(table.table.size(), 47);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2));
        assert_eq!(table.table.size(), 70);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3));
        assert_eq!(table.table.size(), 90);

        table.put(RowEntry::new_tombstone(b"abc333", 4));
        assert_eq!(table.table.size(), 104);
    }
}
