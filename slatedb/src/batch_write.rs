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

use fail_parallel::fail_point;
use log::{info, warn};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::instrument;

use crate::config::WriteOptions;
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::{spawn_bg_task, WatchableOnceCellReader};
use crate::{
    batch::{WriteBatch, WriteOp},
    db::DbInner,
    error::SlateDBError,
};

pub(crate) enum WriteBatchMsg {
    Shutdown,
    WriteBatch(WriteBatchRequest, WriteOptions),
}

pub(crate) struct WriteBatchRequest {
    pub(crate) batch: WriteBatch,
    pub(crate) done: tokio::sync::oneshot::Sender<
        Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError>,
    >,
}

impl DbInner {
    #[allow(clippy::panic)]
    #[instrument(level = "trace", skip_all, fields(batch_size = batch.ops.len()))]
    async fn write_batch(
        &self,
        batch: WriteBatch,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let now = self.mono_clock.now().await?;
        let commit_seq = self.oracle.last_seq.next();
        let txn_id = batch.txn_id;

        let entries = self.extract_row_entries(batch, commit_seq, now);
        let conflict_keys = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<HashSet<_>>();

        // check if there's any conflict if this write batch is bound with a txn.
        if let Some(txn_id) = &txn_id {
            // TODO: this might better to be moved into the `commit()`` method of `DbTransaction`.
            self.txn_manager.track_write_keys(txn_id, &conflict_keys);
            if self.txn_manager.check_has_conflict(txn_id) {
                return Err(SlateDBError::TransactionConflict);
            }
        }

        if self.wal_enabled {
            // WAL entries must be appended to the wal buffer atomically. Otherwise,
            // the WAL buffer might flush the entries in the middle of the batch, which
            // would violate the guarantee that batches are written atomically. We do
            // this by appending the entire entry batch in a single call to the WAL buffer,
            // which holds a write lock during the append.
            self.wal_buffer.append(&entries).await?;
        }

        {
            let guard = self.state.read();
            let memtable = guard.memtable();
            entries.into_iter().for_each(|entry| memtable.put(entry));
        }

        // update the last_applied_seq to wal buffer. if a chunk of WAL entries are applied to the memtable
        // and flushed to the remote storage, WAL buffer manager will recycle these WAL entries.
        self.wal_buffer.track_last_applied_seq(commit_seq).await;

        // insert a fail point for easier to test the case where the last_committed_seq is not updated.
        // this is useful for testing the case where the reader is not able to see the writes.
        fail_point!(
            Arc::clone(&self.fp_registry),
            "write-batch-pre-commit",
            |_| { Err(SlateDBError::from(std::io::Error::other("oops"))) }
        );

        // get the durable watcher. we'll await on current WAL table to be flushed if wal is enabled.
        // otherwise, we'll use the memtable's durable watcher.
        let durable_watcher = if self.wal_enabled {
            let current_wal = self.wal_buffer.maybe_trigger_flush().await?;
            // TODO: handle sync here, if sync is enabled, we can call `flush` here. let's put this
            // in another Pull Request.
            current_wal.durable_watcher()
        } else {
            self.state.write().memtable().table().durable_watcher()
        };

        // track the recent committed txn for conflict check. if txn_id is not supplied,
        // we still consider this as an transaction commit.
        if let Some(txn_id) = &txn_id {
            self.txn_manager
                .track_recent_committed_txn(txn_id, commit_seq);
        } else {
            self.txn_manager
                .track_recent_committed_write_batch(&conflict_keys, commit_seq);
        }

        // update the last_committed_seq, so the writes will be visible to the readers.
        self.oracle.last_committed_seq.store(commit_seq);

        // maybe freeze the memtable.
        {
            let last_flushed_wal_id = self.wal_buffer.recent_flushed_wal_id();
            let mut guard = self.state.write();
            self.maybe_freeze_memtable(&mut guard, last_flushed_wal_id)?;
        }

        Ok(durable_watcher)
    }

    /// Converts a WriteBatch into a vector of RowEntry objects with seq and timestamp set.
    fn extract_row_entries(&self, batch: WriteBatch, seq: u64, now: i64) -> Vec<RowEntry> {
        batch
            .ops
            .into_iter()
            .map(|(_, op)| match op {
                WriteOp::Put(key, value, opts) => RowEntry {
                    key,
                    value: ValueDeletable::Value(value.clone()),
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
            })
            .collect::<Vec<_>>()
    }

    pub(crate) fn spawn_write_task(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriteBatchMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);
        let mut is_stopped = false;
        let mut is_first_write = true;
        let monitor_first_write =
            async move |mut watcher: WatchableOnceCellReader<Result<(), SlateDBError>>| {
                tokio::select! {
                    _ = watcher.await_value() => {}
                    _ = this.system_clock.sleep(Duration::from_secs(5)) => {
                        warn!("First write not durable after 5 seconds and WAL is disabled. \
                        SlateDB does not automatically flush memtables until `l0_sst_size_bytes` \
                        is reached. If writer is single threaded or has low throughput, the \
                        applications must call `flush` to ensure durability in a timely manner.");
                    }
                }
            };

        let this = Arc::clone(self);
        let fut = async move {
            while !(is_stopped && rx.is_empty()) {
                match rx.recv().await.expect("unexpected channel close") {
                    WriteBatchMsg::WriteBatch(write_batch_request, options) => {
                        let WriteBatchRequest { batch, done } = write_batch_request;
                        let result = this.write_batch(batch).await;
                        if is_first_write && !this.wal_enabled && options.await_durable {
                            is_first_write = false;
                            let monitor_first_write = monitor_first_write.clone();
                            let durable_watcher = result.clone()?;
                            tokio::spawn(async move {
                                monitor_first_write(durable_watcher).await;
                            });
                        }
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
                        warn!("write task exited [error={}]", err);
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
