use crate::config::DbRecord;
use crate::db_state::{DbStateSnapshot, SsTableHandle};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::VecDequeKeyValueIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::range_util::BytesRange;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;

type ScanIterator<'a> = TwoMergeIterator<
    VecDequeKeyValueIterator,
    TwoMergeIterator<
        MergeIterator<SstIterator<'a, Arc<SsTableHandle>>>,
        MergeIterator<SortedRunIterator<'a, Arc<SsTableHandle>>>,
    >,
>;

pub struct DbIterator<'a> {
    #[allow(dead_code)]
    snapshot: Arc<DbStateSnapshot>,
    range: BytesRange,
    iter: ScanIterator<'a>,
    invalidated: bool,
    last_key: Option<Bytes>,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        snapshot: Arc<DbStateSnapshot>,
        range: BytesRange,
        mem_iter: VecDequeKeyValueIterator,
        l0_iters: VecDeque<SstIterator<'a, Arc<SsTableHandle>>>,
        sr_iters: VecDeque<SortedRunIterator<'a, Arc<SsTableHandle>>>,
    ) -> Result<Self, SlateDBError> {
        let l0_iter = MergeIterator::new(l0_iters).await?;
        let sr_iter = MergeIterator::new(sr_iters).await?;
        let sst_iter = TwoMergeIterator::new(l0_iter, sr_iter).await?;
        let iter = TwoMergeIterator::new(mem_iter, sst_iter).await?;
        Ok(DbIterator {
            snapshot,
            range,
            iter,
            invalidated: false,
            last_key: None,
        })
    }

    /// Get the next record in the scan.
    ///
    /// returns Ok(None) when the scan is complete
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  due to an underlying error
    pub async fn next(&mut self) -> Result<Option<DbRecord>, SlateDBError> {
        if self.invalidated {
            Err(SlateDBError::InvalidatedIterator)
        } else {
            let next_opt = self.iter.next().await?;
            if let Some(kv) = next_opt {
                // TODO: Should we just expose KeyValue instead of DbRecord?
                Ok(Some(kv.into()))
            } else {
                Ok(None)
            }
        }
    }

    /// Seek to a key ahead of the last key returned from the iterator or
    /// the lower range bound if no records have yet been returned.
    ///
    /// returns Ok(()) if the position is successfully advanced
    /// returns SlateDbError::InvalidArgument if `lower_bound` is `Unbounded`
    /// returns SlateDbError::InvalidArgument if the key is comes before the
    ///  current iterator position
    /// returns SlateDbError::InvalidArgument if `lower_bound` is beyond the
    ///  upper bound specified in the original `scan` parameters
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  in order to reclaim resources
    #[allow(dead_code)]
    pub async fn seek(&mut self, next_key: Bytes) -> Result<(), SlateDBError> {
        if self.invalidated {
            Err(SlateDBError::InvalidatedIterator)
        } else if !self.range.contains(&next_key) {
            Err(SlateDBError::InvalidArgument)
        } else if self
            .last_key
            .clone()
            .map_or(false, |last_key| next_key <= last_key)
        {
            Err(SlateDBError::InvalidArgument)
        } else {
            self.iter.seek(&next_key).await
        }
    }
}

pub(crate) trait SeekToKey {
    async fn seek(&mut self, next_key: &Bytes) -> Result<(), SlateDBError>;
}
