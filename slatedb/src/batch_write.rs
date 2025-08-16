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

use async_trait::async_trait;
use fail_parallel::fail_point;
use log::warn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::instrument;

use crate::clock::SystemClock;
use crate::config::WriteOptions;
use crate::dispatcher::MessageHandler;
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::WatchableOnceCellReader;
use crate::{
    batch::{WriteBatch, WriteOp},
    db::DbInner,
    error::SlateDBError,
};

impl DbInner {
    #[allow(clippy::panic)]
    #[instrument(level = "trace", skip_all, fields(batch_size = batch.ops.len()))]
    async fn write_batch(
        &self,
        batch: WriteBatch,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let now = self.mono_clock.now().await?;
        let seq = self.oracle.last_seq.next();

        let entries = batch
            .ops
            .into_iter()
            .map(|op| match op {
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
            })
            .collect::<Vec<_>>();

        if self.wal_enabled {
            // WAL entries must be appended to the wal buffer atomically. Otherwise,
            // the WAL buffer might flush the entries in the middle of the batch, which
            // would violate the guarantee that batches are written atomically. We do
            // this by appending the entire entry batch in a single call to the WAL buffer,
            // which holds a write lock during the append.
            self.wal_buffer.append(&entries).await?;
        }

        {
            let mut guard = self.state.write();
            let memtable = guard.memtable();
            entries.into_iter().for_each(|entry| memtable.put(entry));
        }

        // update the last_applied_seq to wal buffer. if a chunk of WAL entries are applied to the memtable
        // and flushed to the remote storage, WAL buffer manager will recycle these WAL entries.
        self.wal_buffer.track_last_applied_seq(seq).await;

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
}

pub(crate) type WriteResponse =
    oneshot::Sender<Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError>>;

#[derive(Debug)]
pub(crate) enum BatchWriteMessage {
    Write(WriteBatch, WriteOptions, WriteResponse),
}

pub(crate) struct BatchWriteMessageHandler {
    inner: Arc<DbInner>,
    monitor_trigggered: AtomicBool,
}

impl BatchWriteMessageHandler {
    pub fn new(inner: Arc<DbInner>) -> Self {
        Self {
            inner,
            monitor_trigggered: AtomicBool::new(false),
        }
    }
    async fn monitor_first_write(
        &self,
        mut watcher: WatchableOnceCellReader<Result<(), SlateDBError>>,
        options: WriteOptions,
        system_clock: Arc<dyn SystemClock>,
    ) {
        let monitor_trigggered = self.monitor_trigggered.swap(true, Ordering::SeqCst);
        if !monitor_trigggered && !self.inner.wal_enabled && options.await_durable {
            tokio::spawn(async move {
                tokio::select! {
                    _ = watcher.await_value() => {}
                    _ = system_clock.sleep(Duration::from_secs(5)) => {
                        warn!("First write not durable after 5 seconds and WAL is disabled. \
                        SlateDB does not automatically flush memtables until `l0_sst_size_bytes` \
                        is reached. If writer is single threaded or has low throughput, the \
                        applications must call `flush` to ensure durability in a timely manner.");
                    }
                }
            });
        }
    }
}

#[async_trait]
impl MessageHandler<BatchWriteMessage> for BatchWriteMessageHandler {
    async fn handle(
        &mut self,
        message: BatchWriteMessage,
        error: Option<SlateDBError>,
    ) -> Result<(), SlateDBError> {
        match (message, error) {
            (BatchWriteMessage::Write(batch, options, response), None) => {
                let result = self.inner.write_batch(batch).await;
                if let Ok(ref watcher) = result {
                    self.monitor_first_write(
                        watcher.clone(),
                        options,
                        self.inner.system_clock.clone(),
                    )
                    .await;
                }
                let _ = response.send(result.clone());
                result.map(|_| ())
            }
            (BatchWriteMessage::Write(_, _, response), Some(e)) => {
                let _ = response.send(Err(e));
                Ok(())
            }
        }
    }
}
