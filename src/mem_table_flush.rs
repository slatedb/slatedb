use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::tablestore::SsTableId;
use futures::executor::block_on;
use std::sync::Arc;
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

    pub(crate) fn spawn_memtable_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<MemtableFlushThreadMsg>,
    ) -> Option<std::thread::JoinHandle<()>> {
        let this = Arc::clone(self);
        Some(std::thread::spawn(move || loop {
            let msg = rx.recv();
            match msg {
                Ok(MemtableFlushThreadMsg::Shutdown) => return,
                Ok(MemtableFlushThreadMsg::FlushImmutableMemtables) => {
                    match block_on(this.flush_imm_memtables_to_l0()) {
                        Ok(_) => {}
                        Err(err) => print!("error from memtable flush: {}", err),
                    }
                }
                Err(err) => {
                    print!("error on memtable flush thread channel: {}", err)
                }
            }
        }))
    }
}
