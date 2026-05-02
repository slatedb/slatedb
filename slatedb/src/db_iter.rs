use crate::batch::WriteBatchIterator;
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::filter_iterator::FilterIterator;
use crate::iter::{EmptyIterator, IterationOrder, RowEntryIterator};
use crate::merge_iterator::MergeIterator;
use crate::merge_operator::{
    MergeOperatorIterator, MergeOperatorRequiredIterator, MergeOperatorType,
};
use crate::types::{KeyValue, RowEntry, ValueDeletable};

use async_trait::async_trait;
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
pub(crate) struct DbIteratorRangeTracker {
    inner: Mutex<DbIteratorRangeTrackerInner>,
}

#[derive(Debug)]
struct DbIteratorRangeTrackerInner {
    first_key: Option<Bytes>,
    last_key: Option<Bytes>,
    has_data: bool,
}

impl DbIteratorRangeTracker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(DbIteratorRangeTrackerInner {
                first_key: None,
                last_key: None,
                has_data: false,
            }),
        }
    }

    fn track_key(&self, key: &Bytes) {
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

    pub(crate) fn get_range(&self) -> Option<BytesRange> {
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

    pub(crate) fn has_data(&self) -> bool {
        self.inner.lock().has_data
    }
}

struct GetIterator {
    key: Bytes,
    iters: Vec<Box<dyn RowEntryIterator + 'static>>,
    idx: usize,
}

impl GetIterator {
    pub(crate) fn new(
        key: Bytes,
        write_batch_iter: Box<dyn RowEntryIterator + 'static>,
        mem_iters: impl IntoIterator<Item = Box<dyn RowEntryIterator + 'static>>,
        segment_iter: Box<dyn RowEntryIterator + 'static>,
    ) -> Self {
        let iters = vec![write_batch_iter]
            .into_iter()
            .chain(mem_iters)
            .chain(std::iter::once(segment_iter))
            .collect();

        Self { key, iters, idx: 0 }
    }
}

#[async_trait]
impl RowEntryIterator for GetIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        // GetIterator departs from the normal convention for RowEntryIterator
        // in that it lazily initializes the iterators only when necessary -
        // this is because it is used in a way that will early exit before all
        // iterators are used.
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while self.idx < self.iters.len() {
            // initialization is idempotent, so we can call it multiple times
            self.iters[self.idx].init().await?;
            let result = self.iters[self.idx].next().await?;
            if let Some(entry) = result {
                // Note: The Get iterator should not advance past tombstones, which is
                // why we filter them out here. When a tombstone is encountered, we return None
                // so the iterator stops without advancing to the next iterator in the chain.
                match &entry.value {
                    ValueDeletable::Tombstone => {
                        return Ok(None);
                    }
                    _ => {
                        return Ok(Some(entry));
                    }
                }
            }
            self.idx += 1;
        }

        Ok(None)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        // we expect the GetIterator to only cover a single key, so if we seek
        // to something other than that key we should just return an error
        if next_key != self.key {
            return Err(SlateDBError::SeekKeyOutOfRange {
                key: next_key.to_vec(),
                range: BytesRange::from(self.key.clone()..=self.key.clone()),
            });
        }

        Ok(())
    }
}

struct ScanIterator {
    delegate: Box<dyn RowEntryIterator + 'static>,
}

impl ScanIterator {
    pub(crate) fn new(
        write_batch_iter: Box<dyn RowEntryIterator + 'static>,
        mem_iters: impl IntoIterator<Item = Box<dyn RowEntryIterator + 'static>>,
        segment_iter: Box<dyn RowEntryIterator + 'static>,
        order: IterationOrder,
    ) -> Result<Self, SlateDBError> {
        // Three-arm top-level merge: write batch (transaction-only),
        // memtables, and the on-disk LSM chain. The chain itself
        // already merges each segment's L0 + sorted runs internally.
        let iters = vec![
            write_batch_iter,
            Box::new(MergeIterator::new_with_order(mem_iters, order)?),
            segment_iter,
        ];

        Ok(Self {
            delegate: Box::new(MergeIterator::new_with_order(iters, order)?),
        })
    }
}

#[async_trait]
impl RowEntryIterator for ScanIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.delegate.init().await
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.delegate.next().await
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.delegate.seek(next_key).await
    }
}

pub struct DbIterator {
    range: BytesRange,
    iter: Box<dyn RowEntryIterator + 'static>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
    range_tracker: Option<Arc<DbIteratorRangeTracker>>,
}

impl DbIterator {
    pub(crate) async fn new(
        range: BytesRange,
        write_batch_iter: Option<WriteBatchIterator>,
        mem_iters: impl IntoIterator<Item = Box<dyn RowEntryIterator + 'static>>,
        segment_iter: Box<dyn RowEntryIterator + 'static>,
        max_seq: Option<u64>,
        range_tracker: Option<Arc<DbIteratorRangeTracker>>,
        merge_operator: Option<MergeOperatorType>,
        order: IterationOrder,
    ) -> Result<Self, SlateDBError> {
        // The write_batch iterator is provided only when operating within a Transaction. It represents the uncommitted
        // writes made during the transaction. We do not need to apply the max_seq filter to them, because they do
        // not have an real committed sequence number yet.
        let write_batch_iter = write_batch_iter
            .map(|iter| Box::new(iter) as Box<dyn RowEntryIterator + 'static>)
            .unwrap_or_else(|| Box::new(EmptyIterator::new()));

        // Apply the max_seq filter before any deduping merge. The
        // per-segment merge inside `segment_iter` runs with dedup disabled,
        // so wrapping the chain as a whole is equivalent to wrapping
        // each leaf — versions > max_seq are dropped before the
        // top-level merge dedups.
        //
        // Example: a leaf carries [(key1, seq=96), (key1, seq=110)]
        // and another carries [(key1, seq=95)]. With max_seq=100,
        // filtering after a deduping merge would let seq=110 win and
        // then drop it, losing the correct seq=96 answer. Filter first.
        let mem_iters = apply_filters(mem_iters, max_seq);
        let segment_iter = Box::new(FilterIterator::new_with_max_seq(segment_iter, max_seq))
            as Box<dyn RowEntryIterator + 'static>;

        let mut iter = match range.as_point() {
            Some(key) => Box::new(GetIterator::new(
                key.clone(),
                write_batch_iter,
                mem_iters,
                segment_iter,
            )) as Box<dyn RowEntryIterator + 'static>,
            None => Box::new(ScanIterator::new(
                write_batch_iter,
                mem_iters,
                segment_iter,
                order,
            )?) as Box<dyn RowEntryIterator + 'static>,
        };

        if let Some(merge_operator) = merge_operator {
            iter = Box::new(MergeOperatorIterator::new(
                merge_operator,
                iter,
                true,
                // Its important not to set a snapshot seq num barrier for this merge iterator
                // The entries in the write batch iterator have seq num u64::MAX and any merges
                // there need to be merged with the entries from the other iterators.
                None,
            ));
        } else {
            // When no merge operator is configured, wrap with iterator that errors on merge operands
            iter = Box::new(MergeOperatorRequiredIterator::new(iter));
        }

        iter.init().await?;

        Ok(DbIterator {
            range,
            iter,
            invalidated_error: None,
            last_key: None,
            range_tracker,
        })
    }

    /// Get the next key-value pair.
    ///
    /// This method filters out tombstones and returns the user-facing [`KeyValue`] struct,
    /// which contains only the key and value.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the iterator has been invalidated due to an underlying error.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
        let entry_opt = self.next_entry().await?;
        match entry_opt {
            Some(entry) => {
                if entry.value.is_tombstone() {
                    return Err(crate::Error::from(
                        crate::error::SlateDBError::UnexpectedTombstone,
                    ));
                }
                Ok(Some(KeyValue::from(entry)))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(error) = self.invalidated_error.clone() {
            Err(error)
        } else {
            let result = loop {
                match self.iter.next().await {
                    Ok(Some(entry)) => match entry.value {
                        ValueDeletable::Tombstone => continue,
                        _ => break Ok(Some(entry)),
                    },
                    Ok(None) => break Ok(None),
                    Err(e) => break Err(e),
                }
            };
            let result = self.maybe_invalidate(result);
            if let Ok(Some(ref entry)) = result {
                self.last_key = Some(entry.key.clone());
                // Track the key in range tracker if present
                if let Some(tracker) = &self.range_tracker {
                    tracker.track_key(&entry.key);
                }
            }
            result
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

pub(crate) fn apply_filters<T>(
    iters: impl IntoIterator<Item = T>,
    max_seq: Option<u64>,
) -> Vec<Box<dyn RowEntryIterator>>
where
    T: RowEntryIterator + 'static,
{
    iters
        .into_iter()
        .map(|iter| FilterIterator::new_with_max_seq(iter, max_seq))
        .map(|iter| Box::new(iter) as Box<dyn RowEntryIterator + 'static>)
        .collect::<Vec<Box<dyn RowEntryIterator>>>()
}

#[cfg(test)]
mod tests {
    use crate::batch::{WriteBatch, WriteBatchIterator};
    use crate::bytes_range::BytesRange;
    use crate::db_iter::DbIterator;
    use crate::error::SlateDBError;
    use crate::iter::{EmptyIterator, IterationOrder, RowEntryIterator};
    use crate::test_utils::TestIterator;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_invalidated_iterator() {
        let mem_iters: VecDeque<Box<dyn RowEntryIterator + 'static>> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            mem_iters,
            Box::new(EmptyIterator::new()),
            None,
            None,
            None,
            IterationOrder::Ascending,
        )
        .await
        .unwrap();

        iter.invalidated_error = Some(SlateDBError::ChecksumMismatch { path: None });

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
        // Create two test iterators with overlapping keys but different sequence numbers
        let mem_iter1 = TestIterator::new()
            .with_entry(b"key1", b"value1", 96)
            .with_entry(b"key1", b"value2", 110);

        let mem_iter2 = TestIterator::new().with_entry(b"key1", b"value3", 95);

        // Create DbIterator with max_seq = 100
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![
                Box::new(mem_iter1) as Box<dyn RowEntryIterator + 'static>,
                Box::new(mem_iter2) as Box<dyn RowEntryIterator + 'static>,
            ],
            Box::new(EmptyIterator::new()),
            Some(100),
            None,
            None,
            IterationOrder::Ascending,
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
        // Build a simple test iterator with two keys
        let mem_iter = TestIterator::new()
            .with_entry(b"key1", b"value1", 1)
            .with_entry(b"key2", b"value2", 2);

        // Create a DbIterator over the whole range
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn RowEntryIterator + 'static>],
            Box::new(EmptyIterator::new()),
            None,
            None,
            None,
            IterationOrder::Ascending,
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
        let mem_iters: VecDeque<Box<dyn RowEntryIterator + 'static>> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            Some(wb_iter),
            mem_iters,
            Box::new(EmptyIterator::new()),
            None,
            None,
            None,
            IterationOrder::Ascending,
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
