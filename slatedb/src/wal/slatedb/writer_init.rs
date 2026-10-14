use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::manifest::Manifest;
use crate::utils::WatchableOnceCellReader;
use crate::wal::slatedb::iterator::{
    SlateDbWalIterator, SlateDbWalIteratorOptions, WalIteratorEndBound,
};
use crate::wal::slatedb::writer::SlateDbWalWriter;
use crate::wal::{WalError, WriterInitResult, WriterManifest};
use crate::{wal, Settings};
use async_trait::async_trait;
use fail_parallel::{fail_point_send, FailPointTx};
use slatedb_common::metrics::MetricsRecorderHelper;
use std::sync::Arc;
use std::time::Duration;

use super::sst_iterator::{WalSstIterator, WalSstIteratorOptions};
use super::store::{WalFileId, WalTableStore};

#[derive(Clone, Copy)]
pub(crate) struct SlateDbWalWriterInitOptions {
    max_wal_bytes_size: usize,
    max_wal_flushes_before_l0_flush: u64,
    max_flush_interval: Option<Duration>,
}

impl From<&Settings> for SlateDbWalWriterInitOptions {
    fn from(settings: &Settings) -> Self {
        Self {
            max_wal_bytes_size: settings.l0_sst_size_bytes,
            max_wal_flushes_before_l0_flush: settings.max_wal_flushes_before_l0_flush,
            max_flush_interval: settings.flush_interval,
        }
    }
}

pub(crate) struct SlateDbWalWriterInit {
    closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
    recorder: MetricsRecorderHelper,
    table_store: Arc<WalTableStore>,
    max_wal_bytes_size: usize,
    max_wal_flushes_before_l0_flush: u64,
    max_flush_interval: Option<Duration>,
    empty_wal_id: u64,
    task_executor: Arc<MessageHandlerExecutor>,
    #[cfg_attr(not(test), allow(dead_code))]
    fp_tx: FailPointTx,
}

impl SlateDbWalWriterInit {
    pub(crate) async fn load(
        closed_result_reader: WatchableOnceCellReader<Result<(), SlateDBError>>,
        recorder: MetricsRecorderHelper,
        table_store: Arc<WalTableStore>,
        options: SlateDbWalWriterInitOptions,
        manifest: &Manifest,
        task_executor: Arc<MessageHandlerExecutor>,
        fp_tx: FailPointTx,
    ) -> Result<Self, SlateDBError> {
        let empty_wal_id = table_store
            .next_wal_sst_id(manifest.core.replay_after_wal_id.into())
            .await?
            .value();
        fail_point_send!(fp_tx, "LoadEmptyWalId");
        Ok(Self {
            closed_result_reader,
            recorder,
            table_store,
            max_wal_bytes_size: options.max_wal_bytes_size,
            max_wal_flushes_before_l0_flush: options.max_wal_flushes_before_l0_flush,
            max_flush_interval: options.max_flush_interval,
            empty_wal_id,
            task_executor,
            fp_tx,
        })
    }
}

#[async_trait]
impl wal::WriterInit for SlateDbWalWriterInit {
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
                .next_wal_sst_id(manifest.core().replay_after_wal_id.into())
                .await?
                .value();
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
            let wrote_fence = match self.table_store.write_wal_fence(empty_wal_id.into()).await {
                Ok(()) => true,
                Err(SlateDBError::Fenced) => false,
                Err(err) => return Err(err.into()),
            };
            fail_point_send!(self.fp_tx, format!("{}:{}", "WriteWalFence", _attempt));

            if wrote_fence {
                // this writer is the only writer that could have written replay_after_wal_id,
                // so it should not be possible for it to have advanced past the fencing wal.
                // older writers would have failed with a stale epoch
                let mut replay_after_wal_id = manifest.core().replay_after_wal_id;
                assert!(empty_wal_id > replay_after_wal_id);

                // No earlier writer can append above our fence: the next WAL id of every
                // earlier writer is the fence id. So when the WALs below our fence hold no
                // rows, there is nothing to recover from them, and the boundary can move up
                // to just below our fence. The fence itself stays above the boundary, so the
                // WAL count the flush thresholds read does not shift on a fresh open. Later
                // opens then replay one fence instead of every fence written since the last
                // L0 flush.
                if self
                    .wal_above_boundary_holds_no_rows(replay_after_wal_id, empty_wal_id)
                    .await?
                {
                    writer_manifest
                        .advance_replay_after_wal_id(empty_wal_id - 1)
                        .await?;
                    replay_after_wal_id = empty_wal_id - 1;
                }

                let replay_iterator = SlateDbWalIterator::range(
                    replay_after_wal_id + 1,
                    WalIteratorEndBound::Exclusive(empty_wal_id + 1),
                    SlateDbWalIteratorOptions::default(),
                    self.table_store.clone(),
                )?;
                let wal_writer = SlateDbWalWriter::start_new(
                    self.closed_result_reader.clone(),
                    &self.recorder,
                    empty_wal_id,
                    self.table_store.clone(),
                    self.max_wal_bytes_size,
                    self.max_wal_flushes_before_l0_flush,
                    self.max_flush_interval,
                    self.task_executor.clone(),
                )
                .await?;
                let result = WriterInitResult {
                    replay_iterator: Box::new(replay_iterator),
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

impl SlateDbWalWriterInit {
    /// Returns true when every WAL file between `replay_after_wal_id` (exclusive) and
    /// `fence_wal_id` (exclusive) holds no rows. Replay of that range then recovers nothing,
    /// so the writer can move the replay boundary up to just below its own fence.
    async fn wal_above_boundary_holds_no_rows(
        &self,
        replay_after_wal_id: u64,
        fence_wal_id: u64,
    ) -> Result<bool, SlateDBError> {
        // The fence we just wrote is the only WAL above the boundary, so this open replayed
        // nothing and there is nothing to move past.
        if fence_wal_id <= replay_after_wal_id + 1 {
            return Ok(false);
        }
        for wal_id in (replay_after_wal_id + 1)..fence_wal_id {
            match self.wal_holds_rows(WalFileId::from(wal_id)).await {
                Ok(true) => return Ok(false),
                Ok(false) => {}
                // A WAL id that has no object holds no rows either. GC deletes only WALs
                // below the replay boundary, so this case is a hole in the id space.
                Err(err) if err.has_object_store_not_found() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    /// Returns true when the WAL file has at least one row.
    ///
    /// Fences hold no rows. Versions before 0.16 wrote a fence as an empty WAL SST, and
    /// later versions write a zero-byte object. Check the rows, and not the object size,
    /// so that both fence formats count as empty.
    async fn wal_holds_rows(&self, wal_id: WalFileId) -> Result<bool, SlateDBError> {
        let sst = match self.table_store.open_sst(wal_id).await {
            // A zero-byte object is a fence.
            Err(SlateDBError::EmptySSTable) => return Ok(false),
            Ok(sst) => sst,
            Err(err) => return Err(err),
        };
        let mut iter = WalSstIterator::new(
            sst,
            Arc::clone(&self.table_store),
            WalSstIteratorOptions::default(),
        )
        .await?;
        Ok(iter.next().await?.is_some())
    }
}
