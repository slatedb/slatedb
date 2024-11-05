use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;

use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::types::{RowAttributes, ValueDeletable};

pub(crate) enum WalFlushThreadMsg {
    Shutdown,
    FlushImmutableWals(Option<tokio::sync::oneshot::Sender<Result<(), SlateDBError>>>),
}

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
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self.table_store.write_sst(id, encoded_sst).await?;
        Ok(handle)
    }

    async fn flush_imm_wal(&self, imm: Arc<ImmutableWal>) -> Result<SsTableHandle, SlateDBError> {
        let wal_id = db_state::SsTableId::Wal(imm.id());
        self.flush_imm_table(&wal_id, imm.table()).await
    }

    fn flush_imm_wal_to_memtable(&self, mem_table: &mut WritableKVTable, imm_table: Arc<KVTable>) {
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry_sync() {
            match kv.value {
                ValueDeletable::Value(v) => {
                    mem_table.put(
                        kv.key,
                        v,
                        RowAttributes {
                            ts: None,
                            expire_ts: kv.expire_ts,
                        },
                    );
                }
                ValueDeletable::Tombstone => {
                    mem_table.delete(
                        kv.key,
                        RowAttributes {
                            ts: None,
                            expire_ts: kv.expire_ts,
                        },
                    );
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
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WalFlushThreadMsg>,
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
                  msg = rx.recv() => {
                        let msg = msg.expect("channel unexpectedly closed");
                        match msg {
                            WalFlushThreadMsg::Shutdown => {
                                // Stop the thread.
                                _ = this.flush().await;
                                return
                            },
                            WalFlushThreadMsg::FlushImmutableWals(rsp) => {
                                let result = this.flush().await;
                                if let Some(rsp) = rsp {
                                    _ = rsp.send(result)
                                }
                            },
                        }
                  }
                }
            }
        }))
    }
}
