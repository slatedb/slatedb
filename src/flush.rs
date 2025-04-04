use log::warn;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use tracing::{error, info};

use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::utils::{bg_task_result_into_err, spawn_bg_task};

#[derive(Debug)]
pub(crate) enum WalFlushMsg {
    Shutdown,
    FlushImmutableWals {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
}

impl DbInner {
    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self.table_store.write_sst(id, encoded_sst).await?;

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());
        Ok(handle)
    }
}
