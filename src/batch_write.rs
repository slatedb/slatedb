//! # Batch Write
//!
//! This module adds batch write functionality to DbInner. Prior to this feature,
//! writes were performed directly in DbInner's `put_with_options` and
//! `delete_with_options` methods. For each operation, a lock was acquired on the
//! db_state to mutate the WAL or memtable. This worked fine for single writes,
//! but for batch writes, which take longer, it could create contention on the lock
//! because. This is dangerous in an async runtime because it can block the
//! threads, leading to starvation.
//!
//! This module spawns a separate task to handle batch writes. The task receives
//! a `WriteBatchMsg``, which contains a `WriteBatchRequest``. The `WriteBatchRequest`
//! contains a `WriteBatch` containing Put/Delete operations and a `oneshot::Sender`.
//! The `Sender` is used to send the table that the batch was written to back to the
//! caller so the caller can `.await` the result. The result is that callers safely
//! `.await` on their writes rather than holding a lock on the db_state.
//!
//! Centralizing the writes in a single event loop also provides a single location to
//! assign sequence numbers when we implement MVCC.
//!
//! [Pebble](https://github.com/cockroachdb/pebble) has a similar design and
//! [a good write-up](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#commit-pipeline)
//! describing its benefits.
//!
//! _Note: The `write_batch` loop still holds a lock on the db_state. There can still
//! be contention between `get`s, which holds a lock, and the write loop._

use core::panic;
use log::{info, warn};
use std::sync::Arc;
use tokio::runtime::Handle;

use crate::types::{RowEntry, ValueDeletable};
use crate::utils::spawn_bg_task;
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
        let now = self.mono_clock.now().await?;
        let seq = self.state.write().increment_seq();
        for op in batch.ops {
            let row_entry = match op {
                WriteOp::Put(key, value, opts) => RowEntry {
                    key,
                    value: ValueDeletable::Value(value),
                    create_ts: Some(now),
                    expire_ts: opts.expire_ts_from(self.options.default_ttl, now),
                    seq,
                },
                WriteOp::Delete(key) => RowEntry {
                    key,
                    value: ValueDeletable::Tombstone,
                    create_ts: Some(now),
                    expire_ts: None,
                    seq,
                },
            };
            if self.wal_enabled() {
                self.wal_buffer.append(&[row_entry.clone()]).await?;
            }
            // we do not need to lock the memtable in mid of the commit pipeline
            // the writes will not visible to the reader until the last_committed_seq
            // is updated.
            self.state.write().memtable().put(row_entry);
        }

        self.wal_buffer.maybe_trigger_flush().await?;
        // TODO: handle sync here, if sync is enabled, we can call `flush` here.

        let mut guard = self.state.write();
        let table = guard.memtable().table().clone();
        let last_wal_id = guard.last_written_wal_id();
        self.maybe_freeze_memtable(&mut guard, last_wal_id)?;

        Ok(table)
    }

    pub(crate) fn spawn_write_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriteBatchMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);
        let mut is_stopped = false;
        let fut = async move {
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
            Ok(())
        };

        let this = Arc::clone(self);
        Some(spawn_bg_task(
            tokio_handle,
            move |result| {
                let err = match result {
                    Ok(()) => {
                        info!("write task shutdown complete");
                        SlateDBError::BackgroundTaskShutdown
                    }
                    Err(err) => {
                        warn!("write task exited with {:?}", err);
                        err.clone()
                    }
                };
                // notify any waiters about the failure
                let mut state = this.state.write();
                state.record_fatal_error(err.clone());
            },
            fut,
        ))
    }
}
