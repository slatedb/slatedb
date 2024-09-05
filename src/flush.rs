use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;

use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SSTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::types::ValueDeletable;

impl DbInner {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        self.state.write().freeze_wal();
        self.flush_imm_wals().await?;
        Ok(())
    }

    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
    ) -> Result<SSTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry().await? {
            match kv.value {
                ValueDeletable::Value(v) => {
                    sst_builder.add(&kv.key, Some(&v))?;
                }
                ValueDeletable::Tombstone => {
                    sst_builder.add(&kv.key, None)?;
                }
            }
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self.table_store.write_sst(id, encoded_sst).await?;
        Ok(handle)
    }

    async fn flush_imm_wal(&self, imm: Arc<ImmutableWal>) -> Result<SSTableHandle, SlateDBError> {
        let wal_id = db_state::SsTableId::Wal(imm.id());
        self.flush_imm_table(&wal_id, imm.table()).await
    }

    fn flush_imm_wal_to_memtable(&self, mem_table: &mut WritableKVTable, imm_table: Arc<KVTable>) {
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry_sync() {
            match kv.value {
                ValueDeletable::Value(v) => {
                    mem_table.put(&kv.key, &v);
                }
                ValueDeletable::Tombstone => {
                    mem_table.delete(&kv.key);
                }
            }
        }
    }

    async fn flush_imm_wals(&self) -> Result<(), SlateDBError> {
        while let Some(imm) = {
            let rguard = self.state.read();
            rguard.state().imm_wal.back().cloned()
        } {
            self.flush_imm_wal(imm.clone()).await?;
            let mut wguard = self.state.write();
            wguard.pop_imm_wal();
            // flush to the memtable before notifying so that data is available for reads
            self.flush_imm_wal_to_memtable(wguard.memtable(), imm.table());
            self.maybe_freeze_memtable(&mut wguard, imm.id());
            imm.table().notify_durable();
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let this = Arc::clone(self);
        Some(tokio_handle.spawn(async move {
            let mut ticker = tokio::time::interval(this.options.flush_interval);
            loop {
                select! {
                  // Tick to freeze and flush the memtable
                  _ = ticker.tick() => {
                    _ = this.flush().await;
                  }
                  // Stop the thread.
                  _ = rx.recv() => {
                        _ = this.flush().await;
                        return
                  }
                }
            }
        }))
    }
}
