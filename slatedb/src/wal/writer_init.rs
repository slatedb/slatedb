use fail_parallel::{fail_point_send, FailPointTx};
use crate::error::SlateDBError;
use crate::manifest::Manifest;
use crate::dispatcher::MessageHandlerExecutor;
use crate::tablestore::TableStore;
use crate::utils::WatchableOnceCellReader;
use crate::wal::{WalError, WriterInitResult, WriterManifest};
use crate::wal_buffer::WalBufferManager;
use crate::{wal, Settings};
use async_trait::async_trait;
use slatedb_common::metrics::MetricsRecorderHelper;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct WalWriterInit {
    closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
    recorder: MetricsRecorderHelper,
    table_store: Arc<TableStore>,
    max_wal_bytes_size: usize,
    max_flush_interval: Option<Duration>,
    empty_wal_id: u64,
    task_executor: Arc<MessageHandlerExecutor>,
    #[cfg_attr(not(test), allow(dead_code))]
    fp_tx: FailPointTx,
}

impl WalWriterInit {
    // TODO: rename maybe
    pub(crate) async fn load(
        closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
        recorder: MetricsRecorderHelper,
        table_store: Arc<TableStore>,
        settings: &Settings,
        manifest: &Manifest,
        task_executor: Arc<MessageHandlerExecutor>,
        fp_tx: FailPointTx,
    ) -> Result<Self, SlateDBError> {
        let empty_wal_id = table_store
            .next_wal_sst_id(manifest.core.replay_after_wal_id)
            .await?;
        fail_point_send!(fp_tx, "LoadEmptyWalId");
        Ok(Self {
            closed_result_reader,
            recorder,
            table_store,
            max_wal_bytes_size: settings.l0_sst_size_bytes,
            max_flush_interval: settings.flush_interval,
            empty_wal_id,
            task_executor,
            fp_tx,
        })
    }
}

#[async_trait]
impl wal::WriterInit for WalWriterInit {
    async fn fence_and_init(
        &self,
        writer_manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let mut empty_wal_id = self.empty_wal_id;
        let mut manifest = writer_manifest.manifest();
        // verify that the empty_wal_id we computed is still valid. Its possible that between
        // computing empty_wal_id and fencing the manifest, the fenced writer advanced the gc
        // boundary (replay_after_wal_id)
        if empty_wal_id <= manifest.core().replay_after_wal_id {
            // the wal gc boundary advanced because the old writer finished a flush - recompute
            // the next wal id
            empty_wal_id = self
                .table_store
                .next_wal_sst_id(manifest.core().replay_after_wal_id)
                .await?;
            writer_manifest.refresh().await?;
            manifest = writer_manifest.manifest();
            fail_point_send!(self.fp_tx, "ReloadEmptyWalId");
            // at this point we still hold the epoch, so it should not be possible for the barrier
            // to have advanced past the computed empty_wal_id
            assert!(empty_wal_id > manifest.core().replay_after_wal_id);
        }
        let manifest = manifest.clone();

        let mut _attempt = 0;
        loop {
            _attempt += 1;
            let wrote_fence = match self.table_store.write_wal_fence(empty_wal_id).await {
                Ok(()) => true,
                Err(SlateDBError::Fenced) => false,
                Err(err) => return Err(err.into()),
            };
            fail_point_send!(self.fp_tx, format!("{}:{}", "WriteWalFence", _attempt));

            if wrote_fence {
                // this writer is the only writer that could have written replay_after_wal_id,
                // so it should not be possible for it to have advanced past the fencing wal.
                // older writers would have failed with a stale epoch
                let replay_after_wal_id = manifest.core().replay_after_wal_id;
                assert!(empty_wal_id > replay_after_wal_id);
                let wal_writer = WalBufferManager::start_new(
                    self.closed_result_reader.clone(),
                    &self.recorder,
                    empty_wal_id,
                    self.table_store.clone(),
                    self.max_wal_bytes_size,
                    self.max_flush_interval,
                    self.task_executor.clone(),
                )
                .await?;
                let result = WriterInitResult {
                    replay_range: (replay_after_wal_id + 1..empty_wal_id + 1).into(),
                    wal_writer: Box::new(wal_writer),
                };
                return Ok(result);
            } else {
                // Refresh validates that we own the latest epoch still.
                writer_manifest.refresh().await?;
                fail_point_send!(self.fp_tx, format!("{}:{}", "RefreshManifest", _attempt));
                // The old writer managed to write a WAL before we could write the fencing wal.
                // Try the next wal ID
                empty_wal_id += 1;
            }
        }
    }
}
