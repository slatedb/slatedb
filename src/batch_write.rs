use core::panic;
use std::sync::Arc;

use log::warn;
use tokio::runtime::Handle;

use crate::{
    batch::{WriteBatch, WriteOp},
    db::DbInner,
    error::SlateDBError,
    mem_table::KVTable,
};

pub(crate) enum WriteBatchMsg {
    Shutdown,
    WriteBatch(WriteBatchRequest),
}

pub(crate) struct WriteBatchRequest {
    pub(crate) batch: WriteBatch,
    pub(crate) done: tokio::sync::oneshot::Sender<Result<Arc<KVTable>, SlateDBError>>,
}

impl DbInner {
    #[allow(clippy::panic)]
    async fn write_batch(&self, batch: WriteBatch) -> Result<Arc<KVTable>, SlateDBError> {
        self.maybe_apply_backpressure().await;

        let current_table = if self.wal_enabled() {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            for op in &batch.ops {
                match op {
                    WriteOp::Put(key, value) => {
                        current_wal.put(key, value);
                    }
                    WriteOp::Delete(key) => {
                        current_wal.delete(key);
                    }
                }
            }
            current_wal.table().clone()
        } else {
            if cfg!(not(feature = "wal_disable")) {
                panic!("wal_disabled feature must be enabled");
            }
            let mut guard = self.state.write();
            let current_memtable = guard.memtable();
            for op in &batch.ops {
                match op {
                    WriteOp::Put(key, value) => {
                        current_memtable.put(key, value);
                    }
                    WriteOp::Delete(key) => {
                        current_memtable.delete(key);
                    }
                }
            }
            let table = current_memtable.table().clone();
            let last_wal_id = guard.last_written_wal_id();
            self.maybe_freeze_memtable(&mut guard, last_wal_id);
            table
        };

        Ok(current_table)
    }

    pub(crate) async fn maybe_apply_backpressure(&self) {
        loop {
            let table = {
                let guard = self.state.read();
                let state = guard.state();
                if state.imm_memtable.len() <= self.options.max_unflushed_memtable {
                    return;
                }
                let Some(table) = state.imm_memtable.back() else {
                    return;
                };
                warn!(
                    "applying backpressure to write by waiting for imm table flush. imm tables({}), max({})",
                    state.imm_memtable.len(),
                    self.options.max_unflushed_memtable
                );
                table.clone()
            };
            table.await_flush_to_l0().await
        }
    }

    pub(crate) fn spawn_write_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriteBatchMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let this = Arc::clone(self);
        let mut is_stopped = false;
        Some(tokio_handle.spawn(async move {
            while !(is_stopped && rx.is_empty()) {
                match rx.recv().await.expect("unexpected channel close") {
                    WriteBatchMsg::WriteBatch(write_batch_request) => {
                        let WriteBatchRequest { batch, done } = write_batch_request;
                        let result = this.write_batch(batch).await;
                        _ = done.send(result);
                    }
                    WriteBatchMsg::Shutdown => {
                        is_stopped = true;
                    }
                }
            }
        }))
    }
}
