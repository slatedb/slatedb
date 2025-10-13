use crate::batch::WriteBatchIterator;
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::filter_iterator::FilterIterator;
use crate::iter::{EmptyIterator, KeyValueIterator};
use crate::mem_table::MemTableIterator;
use crate::merge_iterator::MergeIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use crate::types::KeyValue;

use bytes::Bytes;
use parking_lot::Mutex;
use std::ops::RangeBounds;
use std::sync::Arc;

/// [`DbIteratorRangeTracker'] is used to track the range of keys accessed by a [`DbIterator`].  For
/// Serializable Snapshot Isolation, we need to track the read keys during the transaction to detect
/// read-write conflicts with recent committed transactions.
///
/// A naive implementation is to maintain a set of read keys, but this may suffers phantom read conflicts.
/// For example, if transaction A reads a range `["key01", "key10"]` and transaction B writes to `"key05"`
/// which falls within that range, we cannot detect this conflict and abort one of the transactions to
/// maintain serializability.
///
/// To mitigate this, we could use a range tracker to track the range of keys accessed by the iterator,
/// and check if the write key falls within the range.
///
/// A [`DbIteratorRangeTracker`] can be passed to [`DbIterator`] optionally. If it's passed, you can retrieve
/// the range of keys scanned by [`DbIterator`] from it.
#[derive(Debug)]
pub struct DbIteratorRangeTracker {
    inner: Mutex<DbIteratorRangeTrackerInner>,
}

#[derive(Debug)]
struct DbIteratorRangeTrackerInner {
    first_key: Option<Bytes>,
    last_key: Option<Bytes>,
    has_data: bool,
}

impl DbIteratorRangeTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(DbIteratorRangeTrackerInner {
                first_key: None,
                last_key: None,
                has_data: false,
            }),
        }
    }

    pub fn track_key(&self, key: &Bytes) {
        let mut inner = self.inner.lock();

        inner.first_key = Some(match &inner.first_key {
            Some(first) if key < first => key.clone(),
            Some(first) => first.clone(),
            None => key.clone(),
        });

        inner.last_key = Some(match &inner.last_key {
            Some(last) if key > last => key.clone(),
            Some(last) => last.clone(),
            None => key.clone(),
        });

        inner.has_data = true;
    }

    pub fn get_range(&self) -> Option<BytesRange> {
        let inner = self.inner.lock();
        match (&inner.first_key, &inner.last_key) {
            (Some(first), Some(last)) => {
                use std::ops::Bound;
                Some(BytesRange::from((
                    Bound::Included(first.clone()),
                    Bound::Included(last.clone()),
                )))
            }
            _ => None,
        }
    }

    pub fn has_data(&self) -> bool {
        self.inner.lock().has_data
    }
}

pub struct DbIterator<'a> {
    range: BytesRange,
    iter: MergeIterator<'a>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
    range_tracker: Option<Arc<DbIteratorRangeTracker>>,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        range: BytesRange,
        write_batch_iter: Option<WriteBatchIterator<'a>>,
        mem_iters: impl IntoIterator<Item = MemTableIterator>,
        l0_iters: impl IntoIterator<Item = SstIterator<'a>>,
        sr_iters: impl IntoIterator<Item = SortedRunIterator<'a>>,
        max_seq: Option<u64>,
        range_tracker: Option<Arc<DbIteratorRangeTracker>>,
    ) -> Result<Self, SlateDBError> {
        let iters: Vec<Box<dyn KeyValueIterator>> = {
            // The write_batch iterator is provided only when operating within a Transaction. It represents the uncommitted
            // writes made during the transaction. We do not need to apply the max_seq filter to them, because they do
            // not have an real committed sequence number yet.
            let write_batch_iter = write_batch_iter
                .map(|iter| Box::new(iter) as Box<dyn KeyValueIterator>)
                .unwrap_or_else(|| Box::new(EmptyIterator::new()));

            // Apply the max_seq filter to all the iterators. Please note that we should apply this filter BEFORE
            // merging the iterators.
            //
            // For example, if have the following iterators:
            // - Iterator A with entries [(key1, seq=96), (key1, seq=110)]
            // - Iterator B with entries [(key1, seq=95)]
            //
            // If we filter the iterator after merging with max_seq=100, we'll lost the entry with seq=96 from the
            // iterator A. But the element with seq=96 is actually the correct answer for this scan.
            let mem_iters = Self::apply_max_seq_filter(mem_iters, max_seq);
            let l0_iters = Self::apply_max_seq_filter(l0_iters, max_seq);
            let sr_iters = Self::apply_max_seq_filter(sr_iters, max_seq);
            let (mem_iter, l0_iter, sr_iter) = tokio::join!(
                MergeIterator::new(mem_iters),
                MergeIterator::new(l0_iters),
                MergeIterator::new(sr_iters)
            );

            // Wrap all the iterators as Box<dyn KeyValueIterator>
            vec![
                write_batch_iter,
                Box::new(mem_iter?),
                Box::new(l0_iter?),
                Box::new(sr_iter?),
            ]
        };

        let iter = MergeIterator::new(iters).await?;
        Ok(DbIterator {
            range,
            iter,
            invalidated_error: None,
            last_key: None,
            range_tracker,
        })
    }

    fn apply_max_seq_filter<T>(
        iters: impl IntoIterator<Item = T>,
        max_seq: Option<u64>,
    ) -> Vec<FilterIterator<T>>
    where
        T: KeyValueIterator + 'a,
    {
        iters
            .into_iter()
            .map(|iter| FilterIterator::new_with_max_seq(iter, max_seq))
            .collect::<Vec<_>>()
    }

    /// Get the next record in the scan.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the iterator has been invalidated due to an underlying error.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
        if let Some(error) = self.invalidated_error.clone() {
            Err(error.into())
        } else {
            let result = self.iter.next().await;
            let result = self.maybe_invalidate(result);
            if let Ok(Some(ref kv)) = &result {
                self.last_key = Some(kv.key.clone());
                // Track the key in range tracker if present
                if let Some(tracker) = &self.range_tracker {
                    tracker.track_key(&kv.key);
                }
            }
            result.map_err(Into::into)
        }
    }

    fn maybe_invalidate<T: Clone>(
        &mut self,
        result: Result<T, SlateDBError>,
    ) -> Result<T, SlateDBError> {
        if let Err(error) = &result {
            self.invalidated_error = Some(error.clone());
        }
        result
    }

    /// Seek ahead to the next key. The next key must be larger than the
    /// last key returned by the iterator and less than the end bound specified
    /// in the `scan` arguments.
    ///
    /// After a successful seek, the iterator will return the next record
    /// with a key greater than or equal to `next_key`.
    ///
    /// # Errors
    ///
    /// Returns an invalid argument error in the following cases:
    ///
    /// - if `next_key` comes before the current iterator position
    /// - if `next_key` is beyond the upper bound specified in the original
    ///   [`crate::db::Db::scan`] parameters
    ///
    /// Returns [`Error`] if the iterator has been invalidated in order to reclaim resources.
    pub async fn seek<K: AsRef<[u8]>>(&mut self, next_key: K) -> Result<(), crate::Error> {
        let next_key = next_key.as_ref();
        if let Some(error) = self.invalidated_error.clone() {
            Err(error.into())
        } else if !self.range.contains(&next_key) {
            Err(SlateDBError::SeekKeyOutOfRange {
                key: next_key.to_vec(),
                range: self.range.clone(),
            }
            .into())
        } else if self
            .last_key
            .clone()
            .is_some_and(|last_key| next_key <= last_key)
        {
            Err(SlateDBError::SeekKeyLessThanLastReturnedKey.into())
        } else {
            let result = self.iter.seek(next_key).await;
            self.maybe_invalidate(result).map_err(Into::into)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::batch::{WriteBatch, WriteBatchIterator};
    use crate::bytes_range::BytesRange;
    use crate::db_iter::DbIterator;
    use crate::error::SlateDBError;
    use crate::iter::IterationOrder;
    use crate::mem_table::MemTableIterator;
    use crate::mem_table::WritableKVTable;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_invalidated_iterator() {
        let mem_iters: VecDeque<MemTableIterator> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            mem_iters,
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
        )
        .await
        .unwrap();

        iter.invalidated_error = Some(SlateDBError::ChecksumMismatch);

        let result = iter.next().await;
        let err = result.expect_err("Failed to return invalidated iterator");
        assert_invalidated_iterator_error(err);

        let result = iter.seek(Bytes::new()).await;
        let err = result.expect_err("Failed to return invalidated iterator");
        assert_invalidated_iterator_error(err);
    }

    fn assert_invalidated_iterator_error(err: crate::Error) {
        assert_eq!(err.to_string(), "Data error: checksum mismatch");
    }

    #[tokio::test]
    async fn test_sequence_number_filtering() {
        // Create two memtables with overlapping keys but different sequence numbers
        let mem1 = WritableKVTable::new();
        mem1.put(RowEntry::new_value(b"key1", b"value1", 96));
        mem1.put(RowEntry::new_value(b"key1", b"value2", 110));
        let mem_iter1 = mem1.table().range_ascending(BytesRange::from(..));

        let mem2 = WritableKVTable::new();
        mem2.put(RowEntry::new_value(b"key1", b"value3", 95));
        let mem_iter2 = mem2.table().range_ascending(BytesRange::from(..));

        // Create DbIterator with max_seq = 100
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![mem_iter1, mem_iter2],
            VecDeque::new(),
            VecDeque::new(),
            Some(100),
            None,
        )
        .await
        .unwrap();

        // The iterator should return the entry with seq=96 from the first memtable
        // and not the one with seq=95 from the second memtable
        let result = iter.next().await.unwrap();
        assert!(result.is_some());
        let kv = result.unwrap();
        assert_eq!(kv.key, Bytes::from("key1"));
        assert_eq!(kv.value, Bytes::from("value1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_cannot_rewind() {
        // Build a simple memtable with two keys
        let mem = WritableKVTable::new();
        mem.put(RowEntry::new_value(b"key1", b"value1", 1));
        mem.put(RowEntry::new_value(b"key2", b"value2", 2));

        // Create a DbIterator over the whole range
        let mem_iter = mem.table().range_ascending(BytesRange::from(..));
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![mem_iter],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
        )
        .await
        .unwrap();

        // Consume the first record
        let first = iter.next().await.unwrap().unwrap();
        assert_eq!(first.key, Bytes::from_static(b"key1"));

        // Seeking to the current key or a prior key should fail
        let err = iter.seek(b"key1").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid error: cannot seek to a key less than the last returned key"
        );

        let err = iter.seek(b"key0").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid error: cannot seek to a key less than the last returned key"
        );

        // Seeking forward succeeds and allows reading the next key
        iter.seek(b"key2").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, Bytes::from_static(b"key2"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dbiterator_with_writebatch() {
        // Create a WriteBatch with some data
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");

        // Create WriteBatchIterator
        let wb_iter = WriteBatchIterator::new(&batch, .., IterationOrder::Ascending);

        // Create DbIterator with WriteBatch
        let mem_iters: VecDeque<MemTableIterator> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            Some(wb_iter),
            mem_iters,
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
        )
        .await
        .unwrap();

        // Should get data from WriteBatch in sorted order
        let kv1 = iter.next().await.unwrap().unwrap();
        assert_eq!(kv1.key, Bytes::from_static(b"key1"));
        assert_eq!(kv1.value, Bytes::from_static(b"value1"));

        let kv2 = iter.next().await.unwrap().unwrap();
        assert_eq!(kv2.key, Bytes::from_static(b"key3"));
        assert_eq!(kv2.value, Bytes::from_static(b"value3"));

        // Should be done
        let kv3 = iter.next().await.unwrap();
        assert!(kv3.is_none());
    }
}
