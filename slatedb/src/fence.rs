use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, StoredManifest};
use crate::tablestore::TableStore;
use crate::Settings;
use fail_parallel::{fail_point, FailPointRegistry};
use slatedb_common::SystemClock;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) struct WriterFencer {
    table_store: Arc<TableStore>,
    manifest_update_timeout: Duration,
    system_clock: Arc<dyn SystemClock>,
    fp_ctl: Arc<FailPointCtl>,
}

pub(crate) struct WriterFenceResult {
    pub(crate) manifest: FenceableManifest,
    pub(crate) replay_range: Range<u64>,
}

struct FailPointCtl {
    fp_registry: Arc<FailPointRegistry>,
    event_tx: tokio::sync::mpsc::UnboundedSender<String>,
    event_toggle: Mutex<String>,
}

impl FailPointCtl {
    fn new(
        fp_registry: Arc<FailPointRegistry>,
        event_tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Self {
        Self {
            fp_registry,
            event_tx,
            event_toggle: Mutex::new("".to_string()),
        }
    }

    #[cfg(test)]
    fn enable_fp(&self, event: impl ToString) {
        *self.event_toggle.lock().unwrap() = event.to_string();
    }
}

impl WriterFencer {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        settings: &Settings,
        system_clock: Arc<dyn SystemClock>,
    ) -> Self {
        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
        Self::new_with_fp_ctl(
            table_store,
            settings,
            system_clock,
            Arc::new(FailPointCtl::new(
                Arc::new(FailPointRegistry::new()),
                event_tx,
            )),
        )
    }

    fn new_with_fp_ctl(
        table_store: Arc<TableStore>,
        settings: &Settings,
        system_clock: Arc<dyn SystemClock>,
        fp_ctl: Arc<FailPointCtl>,
    ) -> Self {
        Self {
            table_store,
            manifest_update_timeout: settings.manifest_update_timeout,
            system_clock,
            fp_ctl,
        }
    }

    #[cfg(test)]
    fn fp_notify(&self, event: impl ToString) {
        let event = event.to_string();
        let _ = self.fp_ctl.event_tx.send(event.clone());
        let event_toggle = String::clone(&*self.fp_ctl.event_toggle.lock().unwrap());
        fail_point!(
            Arc::clone(&self.fp_ctl.fp_registry),
            "fence_event",
            event == event_toggle,
            |_| {}
        );
    }

    #[cfg(not(test))]
    fn fp_notify(&self, _event: impl ToString) {}

    /// Fences all writers with an older epoch than the provided `stored_manifest` by (1) writing
    /// a new `FenceableManifest` with a bumped epoch, and (2) writing an empty WAL file that acts
    /// as a barrier. Any parallel old writers will fail with `SlateDBError::Fenced` when trying
    /// to "re-write" this file. Returns a `WriterFence` with the `FenceableManifest` and range
    /// that must be replayed to recover up to the current epoch
    pub(crate) async fn fence(
        self,
        stored_manifest: StoredManifest,
    ) -> Result<WriterFenceResult, SlateDBError> {
        let mut empty_wal_id = self
            .table_store
            .next_wal_sst_id(stored_manifest.manifest().core.replay_after_wal_id)
            .await?;
        self.fp_notify("LoadEmptyWalId");

        let mut manifest = FenceableManifest::init_writer(
            stored_manifest,
            self.manifest_update_timeout,
            self.system_clock.clone(),
        )
        .await?;
        self.fp_notify("FenceManifest");

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
            self.fp_notify("ReloadEmptyWalId");
            // at this point we still hold the epoch, so it should not be possible for the barrier
            // to have advanced past the computed empty_wal_id
            assert!(empty_wal_id > manifest_dirty.value.core.replay_after_wal_id);
        }

        loop {
            let wrote_fence = match self.table_store.write_wal_fence(empty_wal_id).await {
                Ok(()) => true,
                Err(SlateDBError::Fenced) => false,
                Err(err) => return Err(err),
            };
            self.fp_notify(format!("{}:{}", "WriteWalFence", empty_wal_id));

            // Refresh validates that we own the latest epoch still.
            manifest.refresh().await?;
            let dirty_manifest = manifest.prepare_dirty()?;
            let replay_after_wal_id = dirty_manifest.value.core.replay_after_wal_id;
            self.fp_notify(format!("{}:{}", "RefreshManifest", empty_wal_id));

            if wrote_fence {
                // this writer is the only writer that could have written replay_after_wal_id,
                // so it should not be possible for it to have advanced past the fencing wal.
                // older writers would have failed with a stale epoch
                assert!(empty_wal_id > replay_after_wal_id);
                return Ok(WriterFenceResult {
                    manifest,
                    replay_range: replay_after_wal_id + 1..empty_wal_id + 1,
                });
            } else {
                // The old writer managed to write a WAL before we could write the fencing wal.
                // Try the next wal ID
                empty_wal_id += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::ObjectStoreCacheOptions;
    use crate::db_state::{SsTableHandle, SsTableId};
    use crate::error::SlateDBError;
    use crate::fence::{FailPointCtl, WriterFencer};
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::ManifestCore;
    use crate::object_stores::ObjectStores;
    use crate::tablestore::TableStore;
    use crate::{RowEntry, Settings};
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::{DefaultSystemClock, SystemClock};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fence_retries_next_wal_id_when_behind_replay_boundary() {
        // Race:
        // - t0: W2 computes stale next_wal_id=1 while W1 is still active.
        // - t1: W1 writes WAL 1 and WAL 2, flushes them to L0, and advances
        //   the replay boundary to 2.
        // - t2: WAL GC deletes WAL 2, so a future create at stale WAL 2 can
        //   succeed, but it leaves WAL 3 because replay_after_wal_id is not
        //   eligible for deletion.
        // - t3: W2 claims the writer epoch from that manifest.
        // - t4: W2 builds the DbInner needed to run fence_writers.
        // - t5: W2 writes a fence at stale WAL 2; current code accepts it.
        // - t6: W1 can still create live WAL 4 unless W2 retried the fence
        //   above the replay boundary.
        // - t7: ValidateFence should make t6 fail with Fenced.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_fence_retries_next_wal_id_when_behind_replay_boundary";
        let settings = test_db_options();
        let fp_registry = Arc::new(FailPointRegistry::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat::default(),
            path,
            None,
        ));
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let core = ManifestCore::new();
        let stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), core, system_clock.clone())
                .await
                .unwrap();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let fp_ctl = Arc::new(FailPointCtl::new(fp_registry.clone(), event_tx));
        let fencer = WriterFencer::new_with_fp_ctl(
            table_store.clone(),
            &settings,
            system_clock.clone(),
            fp_ctl.clone(),
        );

        // - t0: W2 computes stale next_wal_id=1 while W1 is still active.
        fp_ctl.enable_fp("LoadEmptyWalId");
        fail_parallel::cfg(fp_registry.clone(), "fence_event", "pause").unwrap();
        let jh = tokio::task::spawn(async move { fencer.fence(stored_manifest).await });
        // wait for w2 to load empty wal id
        event_rx.recv().await.unwrap();

        // - t1: W1 writes WAL 1 and 2
        write_wal(1, table_store.as_ref()).await.unwrap();
        write_wal(2, table_store.as_ref()).await.unwrap();

        // - t2: WAL GC deletes WAL 1
        let mut stored_manifest =
            StoredManifest::load(manifest_store.clone(), system_clock.clone())
                .await
                .unwrap();
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.replay_after_wal_id = 2;
        stored_manifest.update(dirty).await.unwrap();
        delete_wal(1, table_store.as_ref()).await;

        // - t3-t5: W2 reloads last wal id and writes fence wal
        fail_parallel::cfg(fp_registry.clone(), "fence_event", "off").unwrap();
        let result = jh.await.unwrap().unwrap();
        assert_eq!(result.replay_range, 3u64..4u64);

        // - t6 - t7 - writing wal at 3 should fail
        let err = write_wal(3, table_store.as_ref()).await.unwrap_err();
        assert!(matches!(err, SlateDBError::Fenced));
    }

    async fn delete_wal(wal_id: u64, table_store: &TableStore) {
        table_store
            .delete_sst(&SsTableId::Wal(wal_id))
            .await
            .unwrap();
    }

    async fn write_wal(
        wal_id: u64,
        table_store: &TableStore,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut w1_wal = table_store.table_builder();
        w1_wal
            .add(RowEntry::new_value(b"w1", b"write", wal_id))
            .await
            .unwrap();
        let w1_wal = w1_wal.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(wal_id), &w1_wal, false)
            .await
    }

    fn test_db_options() -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(100)),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            max_unflushed_bytes: 134_217_728,
            l0_max_ssts: 8,
            l0_max_ssts_per_key: 8,
            l0_flush_parallelism: 1,
            min_filter_keys: 0,
            l0_sst_size_bytes: 128,
            max_wal_flushes_before_l0_flush: 4096,
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            garbage_collector_options: None,
            default_ttl: None,
            block_format: None,
            ..Settings::default()
        }
    }
}
