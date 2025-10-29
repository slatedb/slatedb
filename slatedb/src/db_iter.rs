use crate::batch::WriteBatchIterator;
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::filter_iterator::FilterIterator;
use crate::iter::{EmptyIterator, KeyValueIterator};
use crate::map_iter::MapIterator;
use crate::merge_iterator::MergeIterator;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorType};
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

struct GetIterator<'a> {
    key: Bytes,
    iters: Vec<Box<dyn KeyValueIterator + 'a>>,
    idx: usize,
}

impl<'a> GetIterator<'a> {
    pub(crate) fn new(
        key: Bytes,
        write_batch_iter: Box<dyn KeyValueIterator + 'a>,
        mem_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        l0_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        sr_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
    ) -> Self {
        let iters = vec![write_batch_iter]
            .into_iter()
            .chain(mem_iters)
            .chain(l0_iters)
            .chain(sr_iters)
            .collect();

        Self { key, iters, idx: 0 }
    }
}

#[async_trait]
impl<'a> KeyValueIterator for GetIterator<'a> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        // GetIterator departs from the normal convention for KeyValueIterator
        // in that it lazily initializes the iterators only when necessary -
        // this is because it is used in a way that will early exit before all
        // iterators are used.
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        // Note the Get iterator should not advance past tombstones, which is
        // the default behavior of the KeyValueIterator::next method, because
        // the iterators are sequentially chained instead of merged.
        let next = self.next_entry().await?;
        if let Some(entry) = next {
            match entry.value {
                ValueDeletable::Value(v) => {
                    return Ok(Some(KeyValue {
                        key: entry.key,
                        value: v,
                    }));
                }
                ValueDeletable::Merge(value) => {
                    return Ok(Some(KeyValue {
                        key: entry.key,
                        value,
                    }))
                }
                ValueDeletable::Tombstone => return Ok(None),
            }
        }

        Ok(None)
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while self.idx < self.iters.len() {
            // initialization is idempotent, so we can call it multiple times
            self.iters[self.idx].init().await?;
            let result = self.iters[self.idx].next_entry().await?;
            if result.is_some() {
                return Ok(result);
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

struct ScanIterator<'a> {
    delegate: Box<dyn KeyValueIterator + 'a>,
}

impl<'a> ScanIterator<'a> {
    pub(crate) fn new(
        write_batch_iter: Box<dyn KeyValueIterator + 'a>,
        mem_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        l0_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        sr_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
    ) -> Result<Self, SlateDBError> {
        // wrap each in a merge iterator
        let iters = vec![
            write_batch_iter,
            Box::new(MergeIterator::new(mem_iters)?),
            Box::new(MergeIterator::new(l0_iters)?),
            Box::new(MergeIterator::new(sr_iters)?),
        ];

        Ok(Self {
            delegate: Box::new(MergeIterator::new(iters)?),
        })
    }
}

#[async_trait]
impl<'a> KeyValueIterator for ScanIterator<'a> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.delegate.init().await
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.delegate.next_entry().await
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.delegate.seek(next_key).await
    }
}

pub struct DbIterator<'a> {
    range: BytesRange,
    iter: Box<dyn KeyValueIterator + 'a>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
    range_tracker: Option<Arc<DbIteratorRangeTracker>>,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        range: BytesRange,
        write_batch_iter: Option<WriteBatchIterator>,
        mem_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        l0_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        sr_iters: impl IntoIterator<Item = Box<dyn KeyValueIterator + 'a>>,
        max_seq: Option<u64>,
        range_tracker: Option<Arc<DbIteratorRangeTracker>>,
        now: i64,
        merge_operator: Option<MergeOperatorType>,
    ) -> Result<Self, SlateDBError> {
        // The write_batch iterator is provided only when operating within a Transaction. It represents the uncommitted
        // writes made during the transaction. We do not need to apply the max_seq filter to them, because they do
        // not have an real committed sequence number yet.
        let write_batch_iter = write_batch_iter
            .map(|iter| Box::new(iter) as Box<dyn KeyValueIterator + 'a>)
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
        let mem_iters = apply_filters(mem_iters, max_seq, now);
        let l0_iters = apply_filters(l0_iters, max_seq, now);
        let sr_iters = apply_filters(sr_iters, max_seq, now);

        let mut iter = match range.as_point() {
            Some(key) => Box::new(GetIterator::new(
                key.clone(),
                write_batch_iter,
                mem_iters,
                l0_iters,
                sr_iters,
            )) as Box<dyn KeyValueIterator + 'a>,
            None => Box::new(ScanIterator::new(
                write_batch_iter,
                mem_iters,
                l0_iters,
                sr_iters,
            )?) as Box<dyn KeyValueIterator + 'a>,
        };

        if let Some(merge_operator) = merge_operator {
            iter = Box::new(MergeOperatorIterator::new(merge_operator, iter, true, now));
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

    /// Get the next record in the scan.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the iterator has been invalidated due to an underlying error.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
        self.next_key_value().await.map_err(Into::into)
    }

    pub(crate) async fn next_key_value(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        if let Some(error) = self.invalidated_error.clone() {
            Err(error)
        } else {
            let result = self.iter.next().await;
            let result = self.maybe_invalidate(result);
            if let Ok(Some(ref kv)) = result {
                self.last_key = Some(kv.key.clone());
                // Track the key in range tracker if present
                if let Some(tracker) = &self.range_tracker {
                    tracker.track_key(&kv.key);
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

pub(crate) fn apply_filters<'a, T>(
    iters: impl IntoIterator<Item = T>,
    max_seq: Option<u64>,
    now: i64,
) -> Vec<Box<dyn KeyValueIterator + 'a>>
where
    T: KeyValueIterator + 'a,
{
    iters
        .into_iter()
        .map(|iter| FilterIterator::new_with_max_seq(iter, max_seq))
        .map(|iter| MapIterator::new_with_ttl_now(iter, now))
        .map(|iter| Box::new(iter) as Box<dyn KeyValueIterator + 'a>)
        .collect::<Vec<Box<dyn KeyValueIterator + 'a>>>()
}

#[cfg(test)]
mod tests {
    use crate::batch::{WriteBatch, WriteBatchIterator};
    use crate::bytes_range::BytesRange;
    use crate::db_iter::DbIterator;
    use crate::error::SlateDBError;
    use crate::iter::{IterationOrder, KeyValueIterator};
    use crate::test_utils::TestIterator;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_invalidated_iterator() {
        let mem_iters: VecDeque<Box<dyn KeyValueIterator + 'static>> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            mem_iters,
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            0,
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
                Box::new(mem_iter1) as Box<dyn KeyValueIterator + 'static>,
                Box::new(mem_iter2) as Box<dyn KeyValueIterator + 'static>,
            ],
            VecDeque::new(),
            VecDeque::new(),
            Some(100),
            None,
            0,
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
        // Build a simple test iterator with two keys
        let mem_iter = TestIterator::new()
            .with_entry(b"key1", b"value1", 1)
            .with_entry(b"key2", b"value2", 2);

        // Create a DbIterator over the whole range
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            0,
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
        let wb_iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Create DbIterator with WriteBatch
        let mem_iters: VecDeque<Box<dyn KeyValueIterator + 'static>> = VecDeque::new();
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            Some(wb_iter),
            mem_iters,
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            0,
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

    #[tokio::test]
    async fn test_dbiterator_with_ttl_filtering() {
        // Create test iterators with entries that have different TTLs
        let mut entry1 = RowEntry::new_value(b"key1", b"value1", 1);
        entry1.create_ts = Some(0);
        entry1.expire_ts = Some(50);

        let mut entry2 = RowEntry::new_value(b"key2", b"value2", 2);
        entry2.create_ts = Some(0);
        entry2.expire_ts = Some(100);

        let mut entry3 = RowEntry::new_value(b"key3", b"value3", 3);
        entry3.create_ts = Some(0);
        entry3.expire_ts = None;

        let mem_iter = TestIterator::new()
            .with_row_entry(entry1)
            .with_row_entry(entry2)
            .with_row_entry(entry3);

        // Test at t=49 - all entries should be returned
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            49,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key1")
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key2")
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key3")
        );
        assert!(iter.next().await.unwrap().is_none());

        // Test at t=50 - key1 should be expired, key2 and key3 should be returned
        let mut entry1 = RowEntry::new_value(b"key1", b"value1", 1);
        entry1.create_ts = Some(0);
        entry1.expire_ts = Some(50);

        let mut entry2 = RowEntry::new_value(b"key2", b"value2", 2);
        entry2.create_ts = Some(0);
        entry2.expire_ts = Some(100);

        let mut entry3 = RowEntry::new_value(b"key3", b"value3", 3);
        entry3.create_ts = Some(0);
        entry3.expire_ts = None;

        let mem_iter = TestIterator::new()
            .with_row_entry(entry1)
            .with_row_entry(entry2)
            .with_row_entry(entry3);

        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            50,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key2")
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key3")
        );
        assert!(iter.next().await.unwrap().is_none());

        // Test at t=100 - key1 and key2 should be expired, only key3 should be returned
        let mut entry1 = RowEntry::new_value(b"key1", b"value1", 1);
        entry1.create_ts = Some(0);
        entry1.expire_ts = Some(50);

        let mut entry2 = RowEntry::new_value(b"key2", b"value2", 2);
        entry2.create_ts = Some(0);
        entry2.expire_ts = Some(100);

        let mut entry3 = RowEntry::new_value(b"key3", b"value3", 3);
        entry3.create_ts = Some(0);
        entry3.expire_ts = None;

        let mem_iter = TestIterator::new()
            .with_row_entry(entry1)
            .with_row_entry(entry2)
            .with_row_entry(entry3);

        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            100,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key3")
        );
        assert!(iter.next().await.unwrap().is_none());

        // Test at t=200 - only key3 (no TTL) should be returned
        let mut entry1 = RowEntry::new_value(b"key1", b"value1", 1);
        entry1.create_ts = Some(0);
        entry1.expire_ts = Some(50);

        let mut entry2 = RowEntry::new_value(b"key2", b"value2", 2);
        entry2.create_ts = Some(0);
        entry2.expire_ts = Some(100);

        let mut entry3 = RowEntry::new_value(b"key3", b"value3", 3);
        entry3.create_ts = Some(0);
        entry3.expire_ts = None;

        let mem_iter = TestIterator::new()
            .with_row_entry(entry1)
            .with_row_entry(entry2)
            .with_row_entry(entry3);

        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            200,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap().key,
            Bytes::from_static(b"key3")
        );
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dbiterator_expired_value_hides_older_valid_value() {
        // Test the case where a newer value is expired and an older value is not expired.
        // The newer expired value should become a tombstone and hide the older value,
        // so the iterator should return None for that key.

        // Newer entry (seq=100) that expires at t=50
        let mut newer_entry = RowEntry::new_value(b"key1", b"newer_value", 100);
        newer_entry.create_ts = Some(0);
        newer_entry.expire_ts = Some(50);

        // Older entry (seq=50) that doesn't expire
        let mut older_entry = RowEntry::new_value(b"key1", b"older_value", 50);
        older_entry.create_ts = Some(0);
        older_entry.expire_ts = None;

        let mem_iter = TestIterator::new()
            .with_row_entry(newer_entry)
            .with_row_entry(older_entry);

        // Test at t=100 - the newer entry is expired and should hide the older entry
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            None,
            vec![Box::new(mem_iter) as Box<dyn KeyValueIterator + 'static>],
            VecDeque::new(),
            VecDeque::new(),
            None,
            None,
            100, // now = 100, so newer_entry with expire_ts=50 is expired
            None,
        )
        .await
        .unwrap();

        // Should return None because the newer expired value becomes a tombstone
        // which hides the older non-expired value
        assert!(iter.next().await.unwrap().is_none());
    }
}
