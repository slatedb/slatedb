use std::sync::Arc;

use crate::db::DbInner;
use crate::{db_state, MergeOperator};
use crate::clock::SystemClock;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::KVTable;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorType};
use crate::retention_iterator::RetentionIterator;
use crate::seq_tracker::SequenceTracker;

impl DbInner {
    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
        write_cache: bool,
        min_snapshot_seq: Option<u64>,
        merge_operator: Option<MergeOperatorType>,
        logical_time: i64,
        clock: Arc<dyn SystemClock>
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let iter = imm_table.iter();
        let iter = if let Some(merge_operator) = merge_operator {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                iter,
                false,
                logical_time,
                min_snapshot_seq,
            )) as Box<dyn KeyValueIterator>
        } else {
            Box::new(iter)
        };
        let mut iter = RetentionIterator::new(
            iter,
            None,
            min_snapshot_seq,
            false,
            logical_time,
            clock
        ).await?;
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self
            .table_store
            .write_sst(id, encoded_sst, write_cache)
            .await?;

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());

        Ok(handle)
    }
}
