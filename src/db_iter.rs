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
        MergeIterator<SstIterator<'a, Box<SsTableHandle>>>,
        MergeIterator<SortedRunIterator<'a, Box<SsTableHandle>>>,
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
        l0_iters: VecDeque<SstIterator<'a, Box<SsTableHandle>>>,
        sr_iters: VecDeque<SortedRunIterator<'a, Box<SsTableHandle>>>,
    ) -> Result<Self, SlateDBError> {
        let (l0_iter, sr_iter) =
            tokio::join!(MergeIterator::new(l0_iters), MergeIterator::new(sr_iters),);
        let sst_iter = TwoMergeIterator::new(l0_iter?, sr_iter?).await?;
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
    /// # Errors
    ///
    /// Returns [`SlateDBError::InvalidatedIterator`] if the iterator has been invalidated
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
    //  invalidated in order to reclaim resources.
    #[allow(dead_code)]
    pub async fn seek(&mut self, next_key: Bytes) -> Result<(), SlateDBError> {
        if self.invalidated {
            Err(SlateDBError::InvalidatedIterator)
        } else if !self.range.contains(&next_key) {
            Err(SlateDBError::InvalidArgument {
                msg: "Next key must be contained in the original range".to_string(),
            })
        } else if self
            .last_key
            .clone()
            .map_or(false, |last_key| next_key <= last_key)
        {
            Err(SlateDBError::InvalidArgument {
                msg: "Cannot seek to a key less than the last returned key".to_string(),
            })
        } else {
            self.iter.seek(&next_key).await
        }
    }
}

pub(crate) trait SeekToKey {
    /// Seek to the next (inclusive) key
    async fn seek(&mut self, next_key: &Bytes) -> Result<(), SlateDBError>;
}
