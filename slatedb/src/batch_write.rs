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

use log::{info, warn};
use std::sync::Arc;
use tokio::runtime::Handle;

use crate::types::{RowEntry, ValueDeletable};
use crate::utils::{spawn_bg_task, WatchableOnceCellReader};
use crate::{
    batch::{WriteBatch, WriteOp},
    db::DbInner,
    error::SlateDBError,
};

pub(crate) enum WriteBatchMsg {
    Shutdown,
    WriteBatch(WriteBatchRequest),
}

pub(crate) struct WriteBatchRequest {
    pub(crate) batch: WriteBatch,
    pub(crate) done: tokio::sync::oneshot::Sender<
        Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError>,
    >,
}

impl DbInner {
    #[allow(clippy::panic)]
    async fn write_batch(
        &self,
        batch: WriteBatch,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let now = self.mono_clock.now().await?;
        let seq = self.oracle.last_seq.next();
        for op in batch.ops {
            let row_entry = match op {
                WriteOp::Put(key, value, opts) => RowEntry {
                    key,
                    value: ValueDeletable::Value(value),
                    create_ts: Some(now),
                    expire_ts: opts.expire_ts_from(self.settings.default_ttl, now),
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

            if self.wal_enabled {
                self.wal_buffer.append(&[row_entry.clone()]).await?;
            }
            // we do not need to lock the memtable in the middle of the commit pipeline.
            // the writes will not visible to the reader until the last_committed_seq
            // is updated.
            self.state.write().memtable().put(row_entry);
        }

        // update the last_applied_seq to wal buffer. if a chunk of WAL entries are applied to the memtable
        // and flushed to the remote storage, WAL buffer manager will recycle these WAL entries.
        self.wal_buffer.track_last_applied_seq(seq).await;

        // get the durable watcher. we'll await on current WAL table to be flushed if wal is enabled.
        // otherwise, we'll use the memtable's durable watcher.
        let durable_watcher = if self.wal_enabled {
            let current_wal = match self.wal_buffer.maybe_trigger_flush().await {
                Ok(current_wal) => current_wal,
                Err(err) => {
                    // maybe_trigger_flush returns an error when the WAL buffer is in a fatal error state
                    // (excluding BackgroundTaskShutdown). This indicates the flushing error is not
                    // recoverable, we should `record_fatal_error` to notify others.
                    if !matches!(err, SlateDBError::BackgroundTaskShutdown) {
                        self.state.write().record_fatal_error(err.clone());
                    }
                    return Err(err);
                }
            };
            // TODO: handle sync here, if sync is enabled, we can call `flush` here. let's put this
            // in another Pull Request.
            current_wal.durable_watcher()
        } else {
            self.state.write().memtable().table().durable_watcher()
        };

        // update the last_committed_seq, so the writes will be visible to the readers.
        self.oracle.last_committed_seq.store(seq);

        // maybe freeze the memtable.
        {
            let last_flushed_wal_id = self.wal_buffer.recent_flushed_wal_id();
            let mut guard = self.state.write();
            self.maybe_freeze_memtable(&mut guard, last_flushed_wal_id)?;
        }

        Ok(durable_watcher)
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
