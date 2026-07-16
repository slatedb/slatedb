use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, StoredManifest};
use crate::tablestore::TableStore;
use crate::Settings;
use fail_parallel::{fail_point_send, FailPointTx};
use slatedb_common::SystemClock;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct WriterFencer {
    table_store: Arc<TableStore>,
    manifest_update_timeout: Duration,
    system_clock: Arc<dyn SystemClock>,
    #[cfg_attr(not(test), allow(dead_code))]
    fp_tx: FailPointTx,
}

pub(crate) struct WriterFenceResult {
    pub(crate) manifest: FenceableManifest,
    pub(crate) replay_range: Range<u64>,
}

impl WriterFencer {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        settings: &Settings,
        system_clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self::new_with_fp_handle(table_store, settings, system_clock, FailPointTx::dummy())
    }

    fn new_with_fp_handle(
        table_store: Arc<TableStore>,
        settings: &Settings,
        system_clock: Arc<dyn SystemClock>,
        fp_tx: FailPointTx,
    ) -> Self {
        Self {
            table_store,
            manifest_update_timeout: settings.manifest_update_timeout,
            system_clock,
            fp_tx,
        }
    }

    fn fail_point_send(&self, _name: impl ToString) {
        fail_point_send!(self.fp_tx, _name, |_| {});
    }

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
        self.fail_point_send("LoadEmptyWalId");

        let mut manifest = FenceableManifest::init_writer(
            stored_manifest,
            self.manifest_update_timeout,
            self.system_clock.clone(),
        )
        .await?;
        self.fail_point_send("FenceManifest");

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
            self.fail_point_send("ReloadEmptyWalId");
            // at this point we still hold the epoch, so it should not be possible for the barrier
            // to have advanced past the computed empty_wal_id
            assert!(empty_wal_id > manifest_dirty.value.core.replay_after_wal_id);
        }

        let mut attempt = 0;
        loop {
            attempt += 1;
            let wrote_fence = match self.table_store.write_wal_fence(empty_wal_id).await {
                Ok(()) => true,
                Err(SlateDBError::Fenced) => false,
                Err(err) => return Err(err),
            };
            self.fail_point_send(format!("{}:{}", "WriteWalFence", attempt));

            // Refresh validates that we own the latest epoch still.
            manifest.refresh().await?;
            let dirty_manifest = manifest.prepare_dirty()?;
            let replay_after_wal_id = dirty_manifest.value.core.replay_after_wal_id;
            self.fail_point_send(format!("{}:{}", "RefreshManifest", attempt));

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
    use crate::compactions_store::CompactionsStore;
    use crate::config::{
        FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
    };
    use crate::error::SlateDBError;
    use crate::fence::WriterFencer;
    use crate::format::sst::SsTableFormat;
    use crate::garbage_collector::GarbageCollector;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::ManifestCore;
    use crate::memtable_flusher::MANIFEST_REFRESH_COUNT;
    use crate::object_stores::ObjectStores;
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::{CloseReason, Db, ErrorKind, Settings};
    use bytes::Bytes;
    use fail_parallel::fail_point_channel;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use slatedb_common::metrics::{lookup_metric, DefaultMetricsRecorder, MetricsRecorderHelper};
    use slatedb_common::{DefaultSystemClock, SystemClock};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    struct WriterFencerTestHarness {
        object_store: Arc<dyn ObjectStore>,
        path: String,
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        fp_registry: Arc<FailPointRegistry>,
        event_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
        fencer: Option<WriterFencer>,
        stored_manifest: Option<StoredManifest>,
        data: HashMap<Bytes, Bytes>,
    }

    impl WriterFencerTestHarness {
        async fn new(path: &str) -> Self {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let settings = test_db_options();
            let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
            let manifest_store =
                Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
            let table_store = Arc::new(TableStore::new(
                ObjectStores::new(object_store.clone(), None),
                SsTableFormat::default(),
                path,
                None,
                TableStoreKind::Main,
            ));
            let stored_manifest = StoredManifest::create_new_db(
                manifest_store.clone(),
                ManifestCore::new(),
                system_clock.clone(),
            )
            .await
            .unwrap();
            let fp_registry = Arc::new(FailPointRegistry::new());
            let (fp_tx, event_rx) = fail_point_channel(fp_registry.clone());
            let fencer = WriterFencer::new_with_fp_handle(
                table_store.clone(),
                &settings,
                system_clock.clone(),
                fp_tx,
            );
            Self {
                object_store,
                path: path.to_string(),
                manifest_store,
                table_store,
                fp_registry,
                event_rx,
                fencer: Some(fencer),
                stored_manifest: Some(stored_manifest),
                data: HashMap::new(),
            }
        }

        async fn fenced_db(&self) -> Db {
            // initialize a db with manifest poll interval set to 1hr — long enough
            // that the db won't independently observe the fencer's epoch bump
            // through its background poller while the fencer is paused.
            let recorder = Arc::new(DefaultMetricsRecorder::new());
            let mut settings = test_db_options();
            settings.manifest_poll_interval = Duration::from_secs(3600);
            let db = Db::builder(self.path.clone(), self.object_store.clone())
                .with_settings(settings)
                .with_metrics_recorder(recorder.clone())
                .build()
                .await
                .unwrap();
            // wait for initial manifest poll using "manifest_refresh_count" stat
            // so the db has settled before the test starts fencing it.
            for _ in 0..600 {
                if lookup_metric(&recorder, MANIFEST_REFRESH_COUNT).unwrap_or(0) > 0 {
                    return db;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("manifest writer did not perform initial poll");
        }

        async fn db(&self) -> Db {
            let recorder = Arc::new(DefaultMetricsRecorder::new());
            Db::builder(self.path.clone(), self.object_store.clone())
                .with_settings(test_db_options())
                .with_metrics_recorder(recorder)
                .build()
                .await
                .unwrap()
        }

        async fn run_gc(&self, wal_id: u64) {
            let compactions_store = Arc::new(CompactionsStore::new(
                &Path::from(self.path.clone()),
                self.object_store.clone(),
            ));
            let gc_opts = GarbageCollectorOptions {
                manifest_options: None,
                wal_options: Some(GarbageCollectorDirectoryOptions {
                    min_age: Duration::ZERO,
                    interval: None,
                    dry_run: false,
                }),
                wal_fence_options: None,
                compacted_options: None,
                compactions_options: None,
                detach_options: None,
                metric_level: None,
                boundary_files_enabled: true,
                object_store_max_retries: None,
            };
            let gc = GarbageCollector::new(
                self.manifest_store.clone(),
                compactions_store,
                self.table_store.clone(),
                self.object_store.clone(),
                gc_opts,
                &MetricsRecorderHelper::noop(),
                Arc::new(DefaultSystemClock::new()),
                None,
            );
            gc.run_gc_once().await;
            // verify all regular (size > 0) wals up to wal_id are deleted (the wal
            // at replay_after_wal_id is retained as the boundary; old fence wals
            // stay because we don't enable wal_fence_options).
            let remaining: Vec<_> = self
                .table_store
                .list_wal_ssts(..wal_id)
                .await
                .unwrap()
                .into_iter()
                .filter(|w| w.metadata.size > 0)
                .collect();
            assert!(
                remaining.is_empty(),
                "expected no regular wals below {wal_id}, got {} wals",
                remaining.len()
            );
        }

        async fn put(&mut self, db: &Db, v: u32, expect_fenced: bool) {
            let k = Bytes::from(format!("k{}", v));
            let v = Bytes::from(format!("v{}", v));
            let result = db.put(k.as_ref(), v.as_ref()).await;
            if expect_fenced {
                assert_eq!(
                    result.unwrap_err().kind(),
                    ErrorKind::Closed(CloseReason::Fenced)
                );
            } else {
                self.data.insert(k, v);
                assert!(result.is_ok());
            }
        }

        async fn assert_fencing_wal(&self) {
            let wals = self.table_store.list_wal_ssts(..).await.unwrap();
            let last = wals.last().expect("wal list is empty");
            assert_eq!(last.metadata.size, 0, "last wal is not a fence wal");
        }

        async fn assert_wals_contiguous(&self) {
            let replay_after_wal_id = self
                .manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .replay_after_wal_id;
            let wal_ids: Vec<u64> = self
                .table_store
                .list_wal_ssts(replay_after_wal_id + 1..)
                .await
                .unwrap()
                .into_iter()
                .map(|w| w.id.unwrap_wal_id())
                .collect();
            for (i, id) in wal_ids.iter().enumerate() {
                let expected = replay_after_wal_id + 1 + i as u64;
                assert_eq!(
                    *id, expected,
                    "wal ids above replay_after_wal_id={replay_after_wal_id} are not contiguous: {wal_ids:?}"
                );
            }
        }

        async fn assert_data(&self) {
            let db = self.db().await;
            for (k, v) in &self.data {
                let actual = db.get(k.as_ref()).await.unwrap();
                assert_eq!(
                    actual.as_ref(),
                    Some(v),
                    "key {:?} expected {:?}, got {:?}",
                    k,
                    v,
                    actual
                );
            }
            db.close().await.unwrap();
        }
    }

    struct FencedWriterFlushTestCase {
        event: &'static str,
        write_fenced: bool,
        flush_fenced: bool,
    }

    impl FencedWriterFlushTestCase {
        fn new(event: &'static str, write_fenced: bool, flush_fenced: bool) -> Self {
            Self {
                event,
                write_fenced,
                flush_fenced,
            }
        }
    }

    #[tokio::test]
    async fn test_fence() {
        let mut h =
            WriterFencerTestHarness::new("/tmp/test_fence_handles_fenced_writer_flush").await;
        let db = h.db().await;
        h.put(&db, 1, false).await;
        let fencer = h.fencer.take().unwrap();

        let result = fencer.fence(h.stored_manifest.take().unwrap()).await;

        assert!(result.is_ok());
        h.put(&db, 2, true).await;
        h.assert_fencing_wal().await;
        h.assert_wals_contiguous().await;
        h.assert_data().await;
    }

    struct FencerFencedTestCase {
        pause_event: &'static str,
        new_db_writes: bool,
    }

    impl FencerFencedTestCase {
        const fn new(pause_event: &'static str, new_db_writes: bool) -> Self {
            Self {
                pause_event,
                new_db_writes,
            }
        }
    }

    #[rstest]
    #[case::fence_manifest(FencerFencedTestCase::new("FenceManifest", false))]
    #[case::fence_manifest_with_new_db_writes(FencerFencedTestCase::new("FenceManifest", true))]
    #[case::reload_empty_wal_id(FencerFencedTestCase::new("ReloadEmptyWalId", false))]
    #[case::reload_empty_wal_id_with_new_db_writes(FencerFencedTestCase::new(
        "ReloadEmptyWalId",
        true
    ))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fencer_fenced_after_claiming_epoch(#[case] case: FencerFencedTestCase) {
        // initialize a fenced db
        let mut h =
            WriterFencerTestHarness::new("/tmp/test_fencer_fenced_after_claiming_epoch").await;
        let db = h.fenced_db().await;
        h.put(&db, 0, false).await;

        // initialize a fencer. configure it to pause at LoadEmptyWalId (so the
        // fenced writer can race ahead) and at the case's event (so a new
        // writer can claim the epoch out from under the fencer).
        fail_parallel::cfg(h.fp_registry.clone(), "LoadEmptyWalId", "pause").unwrap();
        fail_parallel::cfg(h.fp_registry.clone(), case.pause_event, "pause").unwrap();

        let fencer = h.fencer.take().unwrap();
        let stored_manifest = h.stored_manifest.take().unwrap();
        let jh = tokio::task::spawn(async move { fencer.fence(stored_manifest).await });
        // wait for LoadEmptyWalId pause
        assert_eq!(h.event_rx.recv().await.unwrap(), "LoadEmptyWalId");

        // after LoadEmptyWalId pause, have the fenced db write some wals and flush and gc.
        // This advances replay_after_wal_id past the fencer's stale empty_wal_id, so the
        // recompute branch (and ReloadEmptyWalId fail_point_send) fires when the fencer resumes.
        h.put(&db, 1, false).await;
        h.put(&db, 2, false).await;
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        let replay_after_wal_id = h
            .manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest
            .core
            .replay_after_wal_id;
        h.run_gc(replay_after_wal_id).await;

        // resume the fencer. re-issuing "pause" wakes the current pause and
        // keeps the action set to "pause" so the next toggled event also
        // pauses.
        fail_parallel::cfg(h.fp_registry.clone(), "LoadEmptyWalId", "pause").unwrap();
        fail_parallel::cfg(h.fp_registry.clone(), case.pause_event, "pause").unwrap();

        // wait for the case's pause event. fp_notify sends an event for every
        // failpoint regardless of whether it pauses, so drain intermediate
        // events (e.g. FenceManifest fires before ReloadEmptyWalId).
        loop {
            let event = h.event_rx.recv().await.unwrap();
            if event == case.pause_event {
                break;
            }
        }

        // init the new db; this bumps the writer epoch, fencing our paused fencer
        let db2 = h.db().await;
        if case.new_db_writes {
            // optionally let the new db write wals, flush, and gc — exercises the
            // case where wals below the fencer's stale empty_wal_id get cleaned up
            // while it is paused.
            h.put(&db2, 3, false).await;
            h.put(&db2, 4, false).await;
            db2.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
            let replay_after_wal_id = h
                .manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .replay_after_wal_id;
            h.run_gc(replay_after_wal_id).await;
        }

        // resume the fencer
        fail_parallel::cfg(h.fp_registry.clone(), "LoadEmptyWalId", "off").unwrap();
        fail_parallel::cfg(h.fp_registry.clone(), case.pause_event, "off").unwrap();

        // validate that its fenced — the fencer's manifest.refresh sees the new db's
        // bumped epoch and returns Fenced.
        let result = jh.await.unwrap();
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result.map(|_| ())
        );
        db2.close().await.unwrap();

        // validate that the wal is contiguous and db has all data
        h.assert_wals_contiguous().await;
        h.assert_data().await;
    }

    #[rstest]
    #[case::load_empty_wal_id(FencedWriterFlushTestCase::new("LoadEmptyWalId", false, false))]
    #[case::fence_manifest(FencedWriterFlushTestCase::new("FenceManifest", false, true))]
    #[case::write_wal_fence(FencedWriterFlushTestCase::new("WriteWalFence:1", true, true))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fence_handles_fenced_writer_flush(#[case] case: FencedWriterFlushTestCase) {
        let mut h =
            WriterFencerTestHarness::new("/tmp/test_fence_handles_fenced_writer_flush").await;
        let db = h.fenced_db().await;

        // write a wal file
        h.put(&db, 0, false).await;

        // configure the fencer to pause
        fail_parallel::cfg(h.fp_registry.clone(), case.event, "pause").unwrap();

        // spawn WriterFencer on another task
        let fencer = h.fencer.take().unwrap();
        let stored_manifest = h.stored_manifest.take().unwrap();
        let jh = tokio::task::spawn(async move { fencer.fence(stored_manifest).await });
        // wait for fencer to load empty wal id and pause
        h.event_rx.recv().await.unwrap();

        // write 2 new wal files to fenced db (write with await durable twice)
        h.put(&db, 1, case.write_fenced).await;
        h.put(&db, 2, case.write_fenced).await;
        // force l0 flush — advances replay_after_wal_id past the fencer's
        // stale empty_wal_id.
        let result = db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await;
        let replay_after_wal_id = h
            .manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest
            .core
            .replay_after_wal_id;
        if case.flush_fenced {
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::Closed(CloseReason::Fenced)
            );
        } else {
            assert!(result.is_ok());
            assert!(replay_after_wal_id > 0);
            // force gc to run
            h.run_gc(replay_after_wal_id).await;
        }

        // unpause WriterFencer
        fail_parallel::cfg(h.fp_registry.clone(), case.event, "off").unwrap();
        // verify it returns successfully
        let result = jh.await.unwrap().unwrap();
        // The fencer's stale empty_wal_id was retried above the fenced writer's possibly
        // advanced replay_after_wal_id.
        assert_eq!(result.replay_range.start, replay_after_wal_id + 1);

        // verify that fenced db is fenced (new write fails)
        use crate::error::{CloseReason, ErrorKind};
        let err = db.put(b"k4", b"v4").await.unwrap_err();
        assert!(
            matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)),
            "expected Fenced, got {err}"
        );
        h.assert_fencing_wal().await;
        h.assert_wals_contiguous().await;
        h.assert_data().await;
    }

    fn test_db_options() -> Settings {
        Settings {
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            garbage_collector_options: None,
            ..Settings::default()
        }
    }
}
