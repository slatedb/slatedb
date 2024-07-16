use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::tablestore::SsTableId;
use std::sync::Arc;
use tokio::runtime::Handle;
use ulid::Ulid;

pub(crate) enum MemtableFlushThreadMsg {
    Shutdown,
    FlushImmutableMemtables,
}

impl DbInner {
    pub(crate) async fn flush_imm_memtables_to_l0(&self) -> Result<(), SlateDBError> {
        while let Some(imm_memtable) = {
            let rguard = self.state.read();
            rguard.state().imm_memtable.back().cloned()
        } {
            let id = SsTableId::Compacted(Ulid::new());
            let sst_handle = self.flush_imm_table(&id, imm_memtable.table()).await?;
            {
                let mut guard = self.state.write();
                guard.move_imm_memtable_to_l0(imm_memtable.clone(), sst_handle);
            }
            self.write_manifest().await?;
        }
        Ok(())
    }

    pub(crate) fn spawn_memtable_flush_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<MemtableFlushThreadMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let this = Arc::clone(self);
        Some(tokio_handle.spawn(async move {
            loop {
                let msg = rx.recv().await.expect("channel unexpectedly closed");
                match msg {
                    MemtableFlushThreadMsg::Shutdown => return,
                    MemtableFlushThreadMsg::FlushImmutableMemtables => {
                        match this.flush_imm_memtables_to_l0().await {
                            Ok(_) => {}
                            Err(err) => print!("error from memtable flush: {}", err),
                        }
                    }
                }
            }
        }))
    }
}
