use crate::db_status::DbStatusManager;
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::manifest::Manifest;
use crate::tablestore::TableStore;
use crate::wal_buffer::WalBufferManager;
use crate::Settings;
use fail_parallel::{fail_point_send, FailPointTx};
use slatedb_common::metrics::MetricsRecorderHelper;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct WalWriterInitResult {
    pub(crate) replay_range: Range<u64>,
    pub(crate) wal_buffer: WalBufferManager,
}

pub(crate) struct WalWriterInit {
    status_manager: DbStatusManager,
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
    pub(crate) async fn load(
        status_manager: DbStatusManager,
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
            status_manager,
            recorder,
            table_store,
            max_wal_bytes_size: settings.l0_sst_size_bytes,
            max_flush_interval: settings.flush_interval,
            empty_wal_id,
            task_executor,
            fp_tx,
        })
    }

    pub(crate) async fn fence_and_init(
        &self,
        manifest: &mut FenceableManifest,
    ) -> Result<WalWriterInitResult, SlateDBError> {
        let mut empty_wal_id = self.empty_wal_id;
        let mut manifest_dirty = manifest.prepare_dirty()?;
        // verify that the empty_wal_id we computed is still valid. Its possible that between
        // computing empty_wal_id and fencing the manifest, the fenced writer advanced the gc
        // boundary (replay_after_wal_id)
        if empty_wal_id <= manifest_dirty.value.core.replay_after_wal_id {
            // the wal gc boundary advanced because the old writer finished a flush - recompute
            // the next wal id
            empty_wal_id = self
                .table_store
                .next_wal_sst_id(manifest_dirty.value.core.replay_after_wal_id)
                .await?;
            manifest.refresh().await?;
            manifest_dirty = manifest.prepare_dirty()?;
            fail_point_send!(self.fp_tx, "ReloadEmptyWalId");
            // at this point we still hold the epoch, so it should not be possible for the barrier
            // to have advanced past the computed empty_wal_id
            assert!(empty_wal_id > manifest_dirty.value.core.replay_after_wal_id);
        }

        let mut _attempt = 0;
        loop {
            _attempt += 1;
            let wrote_fence = match self.table_store.write_wal_fence(empty_wal_id).await {
                Ok(()) => true,
                Err(SlateDBError::Fenced) => false,
                Err(err) => return Err(err),
            };
            fail_point_send!(self.fp_tx, format!("{}:{}", "WriteWalFence", _attempt));

            if wrote_fence {
                // this writer is the only writer that could have written replay_after_wal_id,
                // so it should not be possible for it to have advanced past the fencing wal.
                // older writers would have failed with a stale epoch
                let replay_after_wal_id = manifest_dirty.value.core.replay_after_wal_id;
                assert!(empty_wal_id > replay_after_wal_id);
                let mut wal_buffer = WalBufferManager::new(
                    self.status_manager.clone(),
                    &self.recorder,
                    empty_wal_id,
                    self.table_store.clone(),
                    self.max_wal_bytes_size,
                    self.max_flush_interval,
                );
                wal_buffer.init(self.task_executor.clone()).await?;
                return Ok(WalWriterInitResult {
                    replay_range: replay_after_wal_id + 1..empty_wal_id + 1,
                    wal_buffer,
                });
            } else {
                // Refresh validates that we own the latest epoch still.
                manifest.refresh().await?;
                manifest_dirty = manifest.prepare_dirty()?;
                fail_point_send!(self.fp_tx, format!("{}:{}", "RefreshManifest", _attempt));
                // The old writer managed to write a WAL before we could write the fencing wal.
                // Try the next wal ID
                empty_wal_id += 1;
            }
        }
    }
}
