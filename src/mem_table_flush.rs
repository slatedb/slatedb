use crate::db::DbInner;
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use std::sync::Arc;
use tokio::runtime::Handle;
use ulid::Ulid;

pub(crate) enum MemtableFlushThreadMsg {
    Shutdown,
    FlushImmutableMemtables,
}

impl DbInner {
    pub(crate) async fn load_manifest(&self) -> Result<(), SlateDBError> {
        let current_manifest = self
            .table_store
            .open_latest_manifest()
            .await?
            .expect("manifest must exist");
        let mut wguard_state = self.state.write();
        wguard_state.refresh_db_state(current_manifest);
        Ok(())
    }

    pub(crate) async fn write_manifest(&self) -> Result<(), SlateDBError> {
        let manifest = {
            let rguard_state = self.state.read();
            let mut wguard_manifest = self.manifest.write();
            let new_manifest = wguard_manifest.create_updated_manifest(&rguard_state.state().core);
            *wguard_manifest = new_manifest;
            wguard_manifest.clone()
        };
        self.table_store.write_manifest(&manifest).await
    }

    pub(crate) async fn write_manifest_safely(&self) -> Result<(), SlateDBError> {
        self.load_manifest().await?;
        self.write_manifest().await
    }

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
            self.write_manifest_safely().await?;
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
            let mut manifest_poll_interval =
                tokio::time::interval(this.options.manifest_poll_interval);
            loop {
                tokio::select! {
                    _ = manifest_poll_interval.tick() => {
                        if let Err(err) = this.load_manifest().await {
                            print!("error loading manifest: {}", err);
                        }
                    }
                    msg = rx.recv() => {
                        let msg = msg.expect("channel unexpectedly closed");
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
                }
            }
        }))
    }
}
