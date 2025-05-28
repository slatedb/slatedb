use std::sync::Arc;

use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::KVTable;

impl DbInner {
    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
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
        self.last_remote_persisted_seq
            .store_if_greater(self.last_seq.load());
        Ok(handle)
    }
}
