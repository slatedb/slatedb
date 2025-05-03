use std::cell::Cell;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
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
impl<T: RangeBounds<Bytes>> From<T> for KVTableInternalKeyRange {
    fn from(range: T) -> Self {
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
    map: Arc<SkipMap<KVTableInternalKey, RowEntry>>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    entries_size_in_bytes: AtomicUsize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    last_tick: AtomicI64,
    /// the sequence number of the most recent operation on this KVTable
    last_seq: AtomicU64,
}

pub(crate) struct KVTableMetadata {
    pub(crate) entry_num: usize,
    pub(crate) entries_size_in_bytes: usize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    #[allow(dead_code)]
    pub(crate) last_tick: i64,
    /// the sequence number of the most recent operation on this KVTable
    #[allow(dead_code)]
    pub(crate) last_seq: u64,
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

#[self_referencing]
pub(crate) struct MemTableIteratorInner<T: RangeBounds<KVTableInternalKey>> {
    map: Arc<SkipMap<KVTableInternalKey, RowEntry>>,
    /// `inner` is the Iterator impl of SkipMap, which is the underlying data structure of MemTable.
    #[borrows(map)]
    #[not_covariant]
    inner: Range<'this, KVTableInternalKey, T, KVTableInternalKey, RowEntry>,
    ordering: IterationOrder,
    item: Option<RowEntry>,
}
pub(crate) type MemTableIterator = MemTableIteratorInner<KVTableInternalKeyRange>;

#[async_trait]
impl KeyValueIterator for MemTableIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let front = self.borrow_item().clone();
            if front.is_some_and(|record| record.key < next_key) {
                self.next_entry_sync();
            } else {
                return Ok(());
            }
        }
    }
}

impl MemTableIterator {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        let ans = self.borrow_item().clone();
        let next_entry = match self.borrow_ordering() {
            IterationOrder::Ascending => self.with_inner_mut(|inner| inner.next()),
            IterationOrder::Descending => self.with_inner_mut(|inner| inner.next_back()),
        };

        let cloned_entry = next_entry.map(|entry| entry.value().clone());
        self.with_item_mut(|item| *item = cloned_entry);

        ans
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

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        self.table.metadata()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }
}

impl KVTable {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            entries_size_in_bytes: AtomicUsize::new(0),
            durable: WatchableOnceCell::new(),
            last_tick: AtomicI64::new(i64::MIN),
            last_seq: AtomicU64::new(0),
        }
    }

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        let entry_num = self.map.len();
        let entries_size_in_bytes = self.entries_size_in_bytes.load(Ordering::Relaxed);
        let last_tick = self.last_tick.load(SeqCst);
        let last_seq = self.last_seq.load(SeqCst);
        KVTableMetadata {
            entry_num,
            entries_size_in_bytes,
            last_tick,
            last_seq,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn last_tick(&self) -> i64 {
        self.last_tick.load(SeqCst)
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let last_seq = self.last_seq.load(SeqCst);
            Some(last_seq)
        }
    }

    /// Get the value for a given key.
    /// Returns None if the key is not in the memtable at all,
    /// Some(None) if the key is in the memtable but has a tombstone value,
    /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
    pub(crate) fn get(&self, key: &[u8], max_seq: Option<u64>) -> Option<RowEntry> {
        let user_key = Bytes::from(key.to_vec());
        let range = KVTableInternalKeyRange::from(BytesRange::new(
            Bound::Included(user_key.clone()),
            Bound::Included(user_key),
        ));
        self.map
            .range(range)
            .find(|entry| {
                if let Some(max_seq) = max_seq {
                    entry.key().seq <= max_seq
                } else {
                    true
                }
            })
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        self.range_ascending(..)
    }

    pub(crate) fn range_ascending<T: RangeBounds<Bytes>>(&self, range: T) -> MemTableIterator {
        self.range(range, IterationOrder::Ascending)
    }

    pub(crate) fn range<T: RangeBounds<Bytes>>(
        &self,
        range: T,
        ordering: IterationOrder,
    ) -> MemTableIterator {
        let internal_range = KVTableInternalKeyRange::from(range);
        let mut iterator = MemTableIteratorInnerBuilder {
            map: self.map.clone(),
            inner_builder: |map| map.range(internal_range),
            ordering,
            item: None,
        }
        .build();
        iterator.next_entry_sync();
        iterator
    }

    fn put(&self, row: RowEntry) {
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
        self.last_seq.fetch_max(row.seq, atomic::Ordering::SeqCst);

        let row_size = row.estimated_size();
        self.map.compare_insert(internal_key, row, |previous_row| {
            // Optimistically calculate the size of the previous value.
            // `compare_fn` might be called multiple times in case of concurrent
            // writes to the same key, so we use `Cell` to avoid subtracting
            // the size multiple times. The last call will set the correct size.
            previous_size.set(Some(previous_row.estimated_size()));
            true
        });
        if let Some(size) = previous_size.take() {
            self.entries_size_in_bytes
                .fetch_sub(size, Ordering::Relaxed);
            self.entries_size_in_bytes
                .fetch_add(row_size, Ordering::Relaxed);
        } else {
            self.entries_size_in_bytes
                .fetch_add(row_size, Ordering::Relaxed);
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
    use std::collections::VecDeque;

    use super::*;
    use crate::merge_iterator::MergeIterator;
    use crate::proptest_util::{arbitrary, sample};
    use crate::test_utils::assert_iterator;
    use crate::{proptest_util, test_utils};
    use rstest::rstest;
    use tokio::runtime::Runtime;

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
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc333")..));
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
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc334")..));
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
    async fn test_memtable_track_sz_and_num() {
        let mut table = WritableKVTable::new();
        let mut metadata = table.table().metadata();

        assert_eq!(metadata.entry_num, 0);
        assert_eq!(metadata.entries_size_in_bytes, 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 1);
        assert_eq!(metadata.entries_size_in_bytes, 16);

        table.put(RowEntry::new_tombstone(b"first", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 2);
        assert_eq!(metadata.entries_size_in_bytes, 29);

        table.put(RowEntry::new_tombstone(b"first", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 2);
        assert_eq!(metadata.entries_size_in_bytes, 29);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 3);
        assert_eq!(metadata.entries_size_in_bytes, 47);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 4);
        assert_eq!(metadata.entries_size_in_bytes, 70);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 5);
        assert_eq!(metadata.entries_size_in_bytes, 90);

        table.put(RowEntry::new_tombstone(b"abc333", 4));
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 6);
        assert_eq!(metadata.entries_size_in_bytes, 104);
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

    #[test]
    fn should_iterate_arbitrary_range() {
        let mut runner = proptest_util::runner::new(file!(), None);
        let runtime = Runtime::new().unwrap();
        let sample_table = sample::table(runner.rng(), 500, 10);

        let mut kv_table = WritableKVTable::new();
        let mut seq = 1;
        for (key, value) in &sample_table {
            let row_entry = RowEntry::new_value(key, value, seq);
            kv_table.put(row_entry);
            seq += 1;
        }

        runner
            .run(
                &(arbitrary::nonempty_range(10), arbitrary::iteration_order()),
                |(range, ordering)| {
                    let mut kv_iter = kv_table.table.range(range.clone(), ordering);

                    runtime.block_on(test_utils::assert_ranged_kv_scan(
                        &sample_table,
                        &range,
                        ordering,
                        &mut kv_iter,
                    ));
                    Ok(())
                },
            )
            .unwrap();
    }
}
