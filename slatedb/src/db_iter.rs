use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::mem_table::MemTableIterator;
use crate::merge_iterator::MergeIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use crate::types::KeyValue;

use bytes::Bytes;
use std::ops::RangeBounds;

pub struct DbIterator<'a> {
    range: BytesRange,
    iter: MergeIterator<'a>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        range: BytesRange,
        mem_iters: impl IntoIterator<Item = MemTableIterator>,
        l0_iters: impl IntoIterator<Item = SstIterator<'a>>,
        sr_iters: impl IntoIterator<Item = SortedRunIterator<'a>>,
        max_seq: Option<u64>,
    ) -> Result<Self, SlateDBError> {
        let iters: [Box<dyn KeyValueIterator>; 3] = {
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
            [Box::new(mem_iter?), Box::new(l0_iter?), Box::new(sr_iter?)]
        };

        let iter = MergeIterator::new(iters).await?;
        Ok(DbIterator {
            range,
            iter,
            invalidated_error: None,
            last_key: None,
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
    /// Returns [`SlateDBError::InvalidatedIterator`] if the iterator has been invalidated
    ///  due to an underlying error
    pub async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        if let Some(error) = self.invalidated_error.clone() {
            Err(SlateDBError::InvalidatedIterator(Box::new(error)))
        } else {
            let result = self.iter.next().await;
            let result = self.maybe_invalidate(result);
            if let Ok(Some(ref kv)) = &result {
                self.last_key = Some(kv.key.clone());
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
    /// Returns [`SlateDBError::InvalidArgument`] in the following cases:
    ///
    /// - if `next_key` comes before the current iterator position
    /// - if `next_key` is beyond the upper bound specified in the original
    ///   [`crate::db::Db::scan`] parameters
    ///
    /// Returns [`SlateDBError::InvalidatedIterator`] if the iterator has been
    ///  invalidated in order to reclaim resources.
    pub async fn seek<K: AsRef<[u8]>>(&mut self, next_key: K) -> Result<(), SlateDBError> {
        let next_key = next_key.as_ref();
        if let Some(error) = self.invalidated_error.clone() {
            Err(SlateDBError::InvalidatedIterator(Box::new(error)))
        } else if !self.range.contains(&next_key) {
            Err(SlateDBError::InvalidArgument {
                msg: format!(
                    "Cannot seek to a key '{:?}' which is outside the iterator range {:?}",
                    next_key, self.range
                ),
            })
        } else if self
            .last_key
            .clone()
            .is_some_and(|last_key| next_key <= last_key)
        {
            Err(SlateDBError::InvalidArgument {
                msg: "Cannot seek to a key less than the last returned key".to_string(),
            })
        } else {
            let result = self.iter.seek(next_key).await;
            self.maybe_invalidate(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::db_iter::DbIterator;
    use crate::error::SlateDBError;
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
            mem_iters,
            VecDeque::new(),
            VecDeque::new(),
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

    fn assert_invalidated_iterator_error(err: SlateDBError) {
        let SlateDBError::InvalidatedIterator(from_err) = err else {
            panic!("Unexpected error")
        };
        assert!(matches!(*from_err, SlateDBError::ChecksumMismatch));
    }

    #[tokio::test]
    async fn test_sequence_number_filtering() {
        // Create two memtables with overlapping keys but different sequence numbers
        let mut mem1 = WritableKVTable::new();
        mem1.put(RowEntry::new_value(b"key1", b"value1", 96));
        mem1.put(RowEntry::new_value(b"key1", b"value2", 110));
        let mem_iter1 = mem1.table().range_ascending(BytesRange::from(..));

        let mut mem2 = WritableKVTable::new();
        mem2.put(RowEntry::new_value(b"key1", b"value3", 95));
        let mem_iter2 = mem2.table().range_ascending(BytesRange::from(..));

        // Create DbIterator with max_seq = 100
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            vec![mem_iter1, mem_iter2],
            VecDeque::new(),
            VecDeque::new(),
            Some(100),
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
        let mut mem = WritableKVTable::new();
        mem.put(RowEntry::new_value(b"key1", b"value1", 1));
        mem.put(RowEntry::new_value(b"key2", b"value2", 2));

        // Create a DbIterator over the whole range
        let mem_iter = mem.table().range_ascending(BytesRange::from(..));
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            vec![mem_iter],
            VecDeque::new(),
            VecDeque::new(),
            None,
        )
        .await
        .unwrap();

        // Consume the first record
        let first = iter.next().await.unwrap().unwrap();
        assert_eq!(first.key, Bytes::from_static(b"key1"));

        // Seeking to the current key or a prior key should fail
        assert!(matches!(
            iter.seek(b"key1").await,
            Err(SlateDBError::InvalidArgument { .. })
        ));
        assert!(matches!(
            iter.seek(b"key0").await,
            Err(SlateDBError::InvalidArgument { .. })
        ));

        // Seeking forward succeeds and allows reading the next key
        iter.seek(b"key2").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, Bytes::from_static(b"key2"));
        assert!(iter.next().await.unwrap().is_none());
    }
}
