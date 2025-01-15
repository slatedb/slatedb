use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{KeyValueIterator, SeekToKey};
use crate::mem_table::VecDequeKeyValueIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use crate::types::KeyValue;

use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::RangeBounds;

type ScanIterator<'a> = TwoMergeIterator<
    VecDequeKeyValueIterator,
    TwoMergeIterator<MergeIterator<SstIterator<'a>>, MergeIterator<SortedRunIterator<'a>>>,
>;

pub struct DbIterator {
    range: BytesRange,
    iter: ScanIterator<'static>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
}

impl DbIterator {
    pub(crate) async fn new(
        range: BytesRange,
        mem_iter: VecDequeKeyValueIterator,
        l0_iters: VecDeque<SstIterator<'static>>,
        sr_iters: VecDeque<SortedRunIterator<'static>>,
    ) -> Result<Self, SlateDBError> {
        let (l0_iter, sr_iter) =
            tokio::join!(MergeIterator::new(l0_iters), MergeIterator::new(sr_iters),);
        let sst_iter = TwoMergeIterator::new(l0_iter?, sr_iter?).await?;
        let iter = TwoMergeIterator::new(mem_iter, sst_iter).await?;
        Ok(DbIterator {
            range,
            iter,
            invalidated_error: None,
            last_key: None,
        })
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
            self.maybe_invalidate(result)
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
    pub async fn seek(&mut self, next_key: Bytes) -> Result<(), SlateDBError> {
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
            let result = self.iter.seek(&next_key).await;
            self.maybe_invalidate(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::db_iter::DbIterator;
    use crate::error::SlateDBError;
    use crate::mem_table::VecDequeKeyValueIterator;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_invalidated_iterator() {
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            VecDequeKeyValueIterator::new(VecDeque::new()),
            VecDeque::new(),
            VecDeque::new(),
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
}
