use std::cell::Cell;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;

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

/// Convert a user key range to a memtable internal key range. The internal key range should contain all the sequence
/// numbers for the given user key in the range. This is used for iterating over the memtable in [`KVTable::range`].
///
/// Please note that the sequence number is ordered in reverse, given a user key range (`key001`..=`key001`), the first
/// sequence number in this range is u64::MAX, and the last sequence number is 0. The output range should be
/// `(key001, u64::MAX) ..= (key001, 0)`.
impl From<BytesRange> for KVTableInternalKeyRange {
    fn from(range: BytesRange) -> Self {
        let start_bound = match range.start_bound() {
            Bound::Included(key) => Bound::Included(KVTableInternalKey::new(key.clone(), u64::MAX)),
            Bound::Excluded(key) => Bound::Excluded(KVTableInternalKey::new(key.clone(), 0)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(KVTableInternalKey::new(key.clone(), 0)),
            Bound::Excluded(key) => Bound::Excluded(KVTableInternalKey::new(key.clone(), u64::MAX)),
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
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    last_tick: AtomicI64,
    /// the sequence number of the most recent operation on this KVTable
    last_seq: Mutex<Option<u64>>,
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
    table: Arc<KVTable>,
}

pub(crate) struct MemTableIterator<'a, T: RangeBounds<KVTableInternalKey>> {
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
            if front.is_some_and(|record| record.key < next_key) {
                self.rows.pop_front();
            } else {
                return Ok(());
            }
        }
    }
}

impl<T: RangeBounds<KVTableInternalKey>> KeyValueIterator for MemTableIterator<'_, T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }
}

impl<T: RangeBounds<KVTableInternalKey>> MemTableIterator<'_, T> {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        self.inner.next().map(|entry| entry.value().clone())
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
    pub(crate) fn new(table: WritableKVTable) -> Self {
        Self { table: table.table }
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
            last_seq: Mutex::new(None),
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

    pub(crate) fn last_seq(&self) -> Option<u64> {
        *self.last_seq.lock()
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
            inner: self.map.range(KVTableInternalKeyRange::from(range)),
        }
    }

    fn put(&self, row: RowEntry) {
        self.size.fetch_add(row.estimated_size(), Ordering::Relaxed);
        let internal_key = KVTableInternalKey::new(row.key.clone(), row.seq);
        let previous_size = Cell::new(None);

        // it is safe to use fetch_max here to update the last tick
        // because the monotonicity is enforced when generating the clock tick
        // (see [crate::utils::MonotonicClock::now])
        if let Some(create_ts) = row.create_ts {
            self.last_tick
                .fetch_max(create_ts, atomic::Ordering::SeqCst);
        }
        // update the last seq number if it is greater than the current last seq
        {
            let mut last_seq = self.last_seq.lock();
            if last_seq.is_none() || row.seq > last_seq.unwrap() {
                *last_seq = Some(row.seq);
            }
        }

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
    use rstest::rstest;

    #[tokio::test]
    async fn test_memtable_iter() {
        let mut table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"abc333", b"value3", 1));
        table.put(RowEntry::new_value(b"abc111", b"value1", 2));
        table.put(RowEntry::new_value(b"abc555", b"value5", 3));
        table.put(RowEntry::new_value(b"abc444", b"value4", 4));
        table.put(RowEntry::new_value(b"abc222", b"value2", 5));
        assert_eq!(table.table().last_seq(), Some(5));

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

    #[rstest]
    #[case(
        BytesRange::from(..),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Unbounded,
        },
        vec![KVTableInternalKey::new(Bytes::from_static(b"abc111"), 1)],
        vec![]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc111")..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(KVTableInternalKey::new(Bytes::from_static(b"abc111"), u64::MAX)),
            end_bound: Bound::Included(KVTableInternalKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            KVTableInternalKey::new(Bytes::from_static(b"abc111"), 1),
            KVTableInternalKey::new(Bytes::from_static(b"abc222"), 2),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), 3),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), 0),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![KVTableInternalKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc222")..Bytes::from_static(b"abc444")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(KVTableInternalKey::new(Bytes::from_static(b"abc222"), u64::MAX)),
            end_bound: Bound::Excluded(KVTableInternalKey::new(Bytes::from_static(b"abc444"), u64::MAX)),
        },
        vec![
            KVTableInternalKey::new(Bytes::from_static(b"abc222"), 1),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), 2),
        ],
        vec![
            KVTableInternalKey::new(Bytes::from_static(b"abc444"), 0),
            KVTableInternalKey::new(Bytes::from_static(b"abc444"), u64::MAX),
            KVTableInternalKey::new(Bytes::from_static(b"abc555"), u64::MAX),
        ]
    )]
    #[case(
        BytesRange::from(..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Included(KVTableInternalKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            KVTableInternalKey::new(Bytes::from_static(b"abc111"), 1),
            KVTableInternalKey::new(Bytes::from_static(b"abc222"), 2),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), 3),
            KVTableInternalKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![KVTableInternalKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    fn test_from_internal_key_range(
        #[case] range: BytesRange,
        #[case] expected: KVTableInternalKeyRange,
        #[case] should_contains: Vec<KVTableInternalKey>,
        #[case] should_not_contains: Vec<KVTableInternalKey>,
    ) {
        let range = KVTableInternalKeyRange::from(range);
        assert_eq!(range, expected);
        for key in should_contains {
            assert!(range.contains(&key));
        }
        for key in should_not_contains {
            assert!(!range.contains(&key));
        }
    }
}
