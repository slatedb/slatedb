use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tokio::runtime::Handle;
use tracing::error;

use crate::config::{GarbageCollecterDirectoryOptions, GarbageCollectorOptions};
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::garbage_collector::GarbageCollectorMessage::*;
use crate::manifest_store::ManifestStore;
use crate::metrics::DbStats;
use crate::tablestore::TableStore;

const DEFAULT_MIN_AGE: std::time::Duration = std::time::Duration::from_secs(86400);

enum GarbageCollectorMessage {
    Shutdown,
}

pub(crate) struct GarbageCollector {
    main_tx: crossbeam_channel::Sender<GarbageCollectorMessage>,
    main_thread: Option<JoinHandle<()>>,
}

/// Garbage collector for the database. This will periodically check for old
/// manifests and SSTables and delete them. The collector will not delete any
/// SSTables or manifests that are still in use by the database.
impl GarbageCollector {
    /// Create a new garbage collector
    /// # Arguments
    /// * `manifest_store` - The manifest store to use
    /// * `table_store` - The table store to use
    /// * `options` - The options for the garbage collector
    /// * `tokio_handle` - The tokio runtime handle to use if no custom runtime is provided
    /// * `db_stats` - The database stats to use
    /// # Returns
    /// A new garbage collector
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: GarbageCollectorOptions,
        tokio_handle: Handle,
        db_stats: Arc<DbStats>,
    ) -> Self {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let tokio_handle = options.gc_runtime.clone().unwrap_or(tokio_handle);
        let main_thread = thread::spawn(move || {
            let orchestrator = GarbageCollectorOrchestrator {
                manifest_store,
                table_store,
                options,
                external_rx,
                db_stats,
            };
            tokio_handle.block_on(orchestrator.run());
        });
        Self {
            main_thread: Some(main_thread),
            main_tx: external_tx,
        }
    }

    /// Close the garbage collector
    pub(crate) async fn close(mut self) {
        if let Some(main_thread) = self.main_thread.take() {
            self.main_tx.send(Shutdown).expect("main tx disconnected");
            main_thread
                .join()
                .expect("failed to stop main compactor thread");
        }
    }
}

struct GarbageCollectorOrchestrator {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: GarbageCollectorOptions,
    external_rx: crossbeam_channel::Receiver<GarbageCollectorMessage>,
    db_stats: Arc<DbStats>,
}

impl GarbageCollectorOrchestrator {
    /// Collect garbage from the manifest store. This will delete any manifests
    /// that are older than the minimum age specified in the options.
    async fn collect_garbage_manifests(&self) -> Result<(), SlateDBError> {
        let utc_now = Utc::now();
        let min_age = self
            .options
            .manifest_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        let mut manifest_metadata_list = self.manifest_store.list_manifests(..).await?;

        // Remove the last element so we never delete the latest manifest
        manifest_metadata_list.pop();

        // TODO Should exclude snapshotted manifests when we implement snapshots

        // Delete manifests older than min_age
        for manifest_metadata in manifest_metadata_list {
            let min_age = chrono::Duration::from_std(min_age).expect("invalid duration");

            if utc_now.signed_duration_since(manifest_metadata.last_modified) > min_age {
                if let Err(e) = self
                    .manifest_store
                    .delete_manifest(manifest_metadata.id)
                    .await
                {
                    error!("Error deleting manifest: {}", e);
                } else {
                    self.db_stats.gc_manifest_count.inc();
                }
            }
        }

        Ok(())
    }

    /// Collect garbage from the WAL SSTs. This will delete any WAL SSTs that are
    /// older than the minimum age specified in the options and are also older than
    /// the last compacted WAL SST.
    async fn collect_garbage_wal_ssts(&self) -> Result<(), SlateDBError> {
        let utc_now = Utc::now();
        let last_compacted_wal_sst_id = self
            .manifest_store
            // Get the latest manifest so we can get the last compacted id
            .read_latest_manifest()
            .await?
            .ok_or_else(|| SlateDBError::ManifestMissing)?
            // read_latest_manifest returns (id, manifest) but we only care about the manifest
            .1
            .core
            .last_compacted_wal_sst_id;
        let min_age = self
            .options
            .wal_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        let sst_ids_to_delete = self
            .table_store
            .list_wal_ssts(..last_compacted_wal_sst_id)
            .await?
            .into_iter()
            .filter(|wal_sst| {
                let min_age = chrono::Duration::from_std(min_age).expect("invalid duration");
                utc_now.signed_duration_since(wal_sst.last_modified) > min_age
            })
            .map(|wal_sst| wal_sst.id)
            .collect::<Vec<_>>();

        for id in sst_ids_to_delete {
            if let Err(e) = self.table_store.delete_sst(&id).await {
                error!("Error deleting WAL SST: {}", e);
            } else {
                self.db_stats.gc_wal_count.inc();
            }
        }

        Ok(())
    }

    /// Collect garbage from the compacted SSTs. This will delete any compacted SSTs that are
    /// older than the minimum age specified in the options and are not active in the manifest.
    async fn collect_garbage_compacted_ssts(&self) -> Result<(), SlateDBError> {
        let utc_now = Utc::now();
        let manifest = self
            .manifest_store
            // Get the latest manifest so we can get the last compacted id
            .read_latest_manifest()
            .await?
            .ok_or_else(|| SlateDBError::ManifestMissing)?
            // read_latest_manifest returns (id, manifest) but we only care about the manifest
            .1;
        let active_l0_ssts = manifest
            .core
            .l0
            .iter()
            .map(|sst| sst.id)
            .collect::<HashSet<_>>();
        let active_compacted_ssts = manifest
            .core
            .compacted
            .iter()
            .flat_map(|sr| sr.ssts.iter().map(|sst| sst.id))
            .collect::<HashSet<SsTableId>>();
        let active_ssts = active_l0_ssts
            .union(&active_compacted_ssts)
            .collect::<HashSet<_>>();
        let min_age = self
            .options
            .compacted_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        let sst_ids_to_delete = self
            .table_store
            // List all SSTs in the table store
            .list_compacted_ssts(..)
            .await?
            .into_iter()
            // Filter out the ones that are too young to be collected
            .filter(|sst| {
                let min_age = chrono::Duration::from_std(min_age).expect("invalid duration");
                utc_now.signed_duration_since(sst.last_modified) > min_age
            })
            .map(|sst| sst.id)
            // Filter out the ones that are active in the manifest
            .filter(|id| !active_ssts.contains(id))
            .collect::<Vec<_>>();

        for id in sst_ids_to_delete {
            if let Err(e) = self.table_store.delete_sst(&id).await {
                error!("Error deleting SST: {}", e);
            } else {
                self.db_stats.gc_compacted_count.inc();
            }
        }

        Ok(())
    }

    /// Run the garbage collector
    pub async fn run(&self) {
        let manifest_ticker = Self::options_to_ticker(self.options.manifest_options.as_ref());
        let wal_ticker = Self::options_to_ticker(self.options.wal_options.as_ref());
        let compacted_ticker = Self::options_to_ticker(self.options.compacted_options.as_ref());

        loop {
            crossbeam_channel::select! {
                recv(manifest_ticker) -> _ => {
                    if let Err(e) = self.collect_garbage_manifests().await {
                        error!("Error collecting manifest garbage: {}", e);
                    }
                }
                recv(wal_ticker) -> _ => {
                    if let Err(e) = self.collect_garbage_wal_ssts().await {
                        error!("Error collecting WAL garbage: {}", e);
                    }
                }
                recv(compacted_ticker) -> _ => {
                    if let Err(e) = self.collect_garbage_compacted_ssts().await {
                        error!("Error collecting compacted garbage: {}", e);
                    }
                }
                recv(self.external_rx) -> _ => break, // Shutdown
            }
            self.db_stats.gc_count.inc();
        }
    }

    fn options_to_ticker(
        options: Option<&GarbageCollecterDirectoryOptions>,
    ) -> crossbeam_channel::Receiver<std::time::Instant> {
        options.map_or(crossbeam_channel::never(), |opts| {
            crossbeam_channel::tick(opts.poll_interval)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, time::SystemTime};

    use chrono::{DateTime, Utc};
    use log::info;
    use object_store::{local::LocalFileSystem, path::Path};
    use ulid::Ulid;

    use crate::{
        db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId},
        garbage_collector::GarbageCollector,
        manifest_store::{ManifestStore, StoredManifest},
        metrics::{Counter, DbStats},
        sst::SsTableFormat,
        tablestore::TableStore,
    };

    #[tokio::test]
    async fn test_collect_garbage_manifest() {
        let (manifest_store, table_store, local_object_store, db_stats) = build_objects();

        // Create a manifest
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::init_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_db_state(state.clone())
            .await
            .unwrap();

        // Set the first manifest file to be a day old
        let now_minus_24h = set_modified(
            local_object_store.clone(),
            &Path::from(format!("manifest/{:020}.{}", 1, "manifest")),
            86400,
        );

        // Verify that the manifests are there as expected
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);
        assert_eq!(manifests[0].last_modified, now_minus_24h);

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_manifest_count.clone());

        garbage_collector.close().await;

        // Verify that the first manifest was deleted
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    #[tokio::test]
    async fn test_collect_garbage_only_recent_manifests() {
        let (manifest_store, table_store, _, db_stats) = build_objects();

        // Create a manifest
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::init_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_db_state(state.clone())
            .await
            .unwrap();

        // Verify that the manifests are there as expected
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        // Use `gc_count` since the manifest counter won't increment
        wait_for_gc(db_stats.gc_count.clone());

        garbage_collector.close().await;

        // Verify that no manifests were deleted
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);
    }

    #[tokio::test]
    async fn test_collect_garbage_old_active_manifest() {
        let (manifest_store, table_store, local_object_store, db_stats) = build_objects();

        // Create a manifest
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::init_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_db_state(state.clone())
            .await
            .unwrap();

        // Set both manifests to be a day old
        let now_minus_24h_1 = set_modified(
            local_object_store.clone(),
            &Path::from(format!("manifest/{:020}.{}", 1, "manifest")),
            86400,
        );
        let now_minus_24h_2 = set_modified(
            local_object_store.clone(),
            &Path::from(format!("manifest/{:020}.{}", 2, "manifest")),
            86400,
        );

        // Verify that the manifests are there as expected
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);
        assert_eq!(manifests[0].last_modified, now_minus_24h_1);
        assert_eq!(manifests[1].last_modified, now_minus_24h_2);

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_manifest_count.clone());

        garbage_collector.close().await;

        // Verify that the first manifest was deleted, but the second is still safe
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    #[tokio::test]
    async fn test_collect_garbage_wal_ssts() {
        let (manifest_store, table_store, local_object_store, db_stats) = build_objects();

        // write a wal sst
        let id1 = SsTableId::Wal(1);
        let mut sst1 = table_store.table_builder();
        sst1.add(b"key", Some(b"value")).unwrap();
        let table1 = sst1.build().unwrap();
        table_store.write_sst(&id1, table1).await.unwrap();

        let id2 = SsTableId::Wal(2);
        let mut sst2 = table_store.table_builder();
        sst2.add(b"key", Some(b"value")).unwrap();
        let table2 = sst2.build().unwrap();
        table_store.write_sst(&id2, table2).await.unwrap();

        // Set the first WAL SST file to be a day old
        let now_minus_24h = set_modified(
            local_object_store.clone(),
            &Path::from(format!("wal/{:020}.{}", 1, "sst")),
            86400,
        );

        // Create a manifest
        let mut state = CoreDbState::new();
        state.last_compacted_wal_sst_id = id2.unwrap_wal_id();
        StoredManifest::init_new_db(manifest_store.clone(), state.clone())
            .await
            .unwrap();

        // Verify that the WAL SST is there as expected
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 2);
        assert_eq!(wal_ssts[0].id, id1);
        assert_eq!(wal_ssts[1].id, id2);
        assert_eq!(wal_ssts[0].last_modified, now_minus_24h);
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        let current_manifest = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(
            current_manifest.core.last_compacted_wal_sst_id,
            id2.unwrap_wal_id()
        );

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_wal_count.clone());

        garbage_collector.close().await;

        // Verify that the first WAL was deleted and the second is kept
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 1);
        assert_eq!(wal_ssts[0].id, id2);
    }

    #[tokio::test]
    async fn test_collect_garbage_wal_ssts_and_keep_expired_last_compacted() {
        let (manifest_store, table_store, local_object_store, db_stats) = build_objects();

        // write a wal sst
        let id1 = SsTableId::Wal(1);
        let mut sst1 = table_store.table_builder();
        sst1.add(b"key", Some(b"value")).unwrap();
        let table1 = sst1.build().unwrap();
        table_store.write_sst(&id1, table1).await.unwrap();

        let id2 = SsTableId::Wal(2);
        let mut sst2 = table_store.table_builder();
        sst2.add(b"key", Some(b"value")).unwrap();
        let table2 = sst2.build().unwrap();
        table_store.write_sst(&id2, table2).await.unwrap();

        // Set the both WAL SST file to be a day old
        let now_minus_24h_1 = set_modified(
            local_object_store.clone(),
            &Path::from(format!("wal/{:020}.{}", 1, "sst")),
            86400,
        );
        let now_minus_24h_2 = set_modified(
            local_object_store.clone(),
            &Path::from(format!("wal/{:020}.{}", 2, "sst")),
            86400,
        );

        // Create a manifest
        let mut state = CoreDbState::new();
        state.last_compacted_wal_sst_id = id2.unwrap_wal_id();
        StoredManifest::init_new_db(manifest_store.clone(), state.clone())
            .await
            .unwrap();

        // Verify that the WAL SST is there as expected
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 2);
        assert_eq!(wal_ssts[0].id, id1);
        assert_eq!(wal_ssts[1].id, id2);
        assert_eq!(wal_ssts[0].last_modified, now_minus_24h_1);
        assert_eq!(wal_ssts[1].last_modified, now_minus_24h_2);
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        let current_manifest = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(
            current_manifest.core.last_compacted_wal_sst_id,
            id2.unwrap_wal_id()
        );

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_wal_count.clone());

        garbage_collector.close().await;

        // Verify that the first WAL was deleted and the second is kept even though it's expired
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 1);
        assert_eq!(wal_ssts[0].id, id2);
    }

    /// This test creates eight compacted SSTs:
    /// - One L0 SST
    /// - One active L0 SST that's a day old
    /// - One inactive expired L0 SST
    /// - One inactive unexpired L0 SST
    /// - One active SST
    /// - One active SST that's a day old
    /// - One inactive expired SST
    /// - One inactive unexpired SST
    /// The test then runs the compactor to verify that only the inactive expired SSTs
    /// are deleted.
    #[tokio::test]
    async fn test_collect_garbage_compacted_ssts() {
        let (manifest_store, table_store, local_object_store, db_stats) = build_objects();
        let l0_sst_handle = create_sst(table_store.clone()).await;
        let active_expired_l0_sst_handle = create_sst(table_store.clone()).await;
        let inactive_expired_l0_sst_handle = create_sst(table_store.clone()).await;
        let inactive_unexpired_l0_sst_handle = create_sst(table_store.clone()).await;
        let active_sst_handle = create_sst(table_store.clone()).await;
        let active_expired_sst_handle = create_sst(table_store.clone()).await;
        let inactive_expired_sst_handle = create_sst(table_store.clone()).await;
        let inactive_unexpired_sst_handle = create_sst(table_store.clone()).await;

        // Set expiration for the old SSTs
        let now_minus_24h_expired_l0_sst = set_modified(
            local_object_store.clone(),
            &Path::from(format!(
                "compacted/{:020}.{}",
                active_expired_l0_sst_handle.id.unwrap_compacted_id(),
                "sst"
            )),
            86400,
        );
        let now_minus_24h_inactive_expired_l0_sst = set_modified(
            local_object_store.clone(),
            &Path::from(format!(
                "compacted/{:020}.{}",
                inactive_expired_l0_sst_handle.id.unwrap_compacted_id(),
                "sst"
            )),
            86400,
        );
        let now_minus_24h_active_expired_sst = set_modified(
            local_object_store.clone(),
            &Path::from(format!(
                "compacted/{:020}.{}",
                active_expired_sst_handle.id.unwrap_compacted_id(),
                "sst"
            )),
            86400,
        );
        let now_minus_24h_inactive_expired_sst_id = set_modified(
            local_object_store.clone(),
            &Path::from(format!(
                "compacted/{:020}.{}",
                inactive_expired_sst_handle.id.unwrap_compacted_id(),
                "sst"
            )),
            86400,
        );

        // Create a manifest
        let mut state = CoreDbState::new();
        state.l0.push_back(l0_sst_handle.clone());
        state.l0.push_back(active_expired_l0_sst_handle.clone());
        // Dont' push inactive_expired_l0_sst_handle
        state.compacted.push(SortedRun {
            id: 1,
            // Don't add inactive_expired_sst_handle
            ssts: vec![active_sst_handle.clone(), active_expired_sst_handle.clone()],
        });
        StoredManifest::init_new_db(manifest_store.clone(), state.clone())
            .await
            .unwrap();

        // Verify that the WAL SST is there as expected
        let compacted_ssts = table_store.list_compacted_ssts(..).await.unwrap();
        assert_eq!(compacted_ssts.len(), 8);
        assert_eq!(compacted_ssts[0].id, l0_sst_handle.id);
        assert_eq!(compacted_ssts[1].id, active_expired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[2].id, inactive_expired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[3].id, inactive_unexpired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[4].id, active_sst_handle.id);
        assert_eq!(compacted_ssts[5].id, active_expired_sst_handle.id);
        assert_eq!(compacted_ssts[6].id, inactive_expired_sst_handle.id);
        assert_eq!(compacted_ssts[7].id, inactive_unexpired_sst_handle.id);
        assert_eq!(
            compacted_ssts[1].last_modified,
            now_minus_24h_expired_l0_sst
        );
        assert_eq!(
            compacted_ssts[2].last_modified,
            now_minus_24h_inactive_expired_l0_sst
        );
        assert_eq!(
            compacted_ssts[5].last_modified,
            now_minus_24h_active_expired_sst
        );
        assert_eq!(
            compacted_ssts[6].last_modified,
            now_minus_24h_inactive_expired_sst_id
        );
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        let current_manifest = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(current_manifest.core.l0.len(), 2);
        assert_eq!(current_manifest.core.compacted.len(), 1);
        assert_eq!(current_manifest.core.compacted[0].ssts.len(), 2);

        // Start the garbage collector
        let garbage_collector = build_garbage_collector(
            manifest_store.clone(),
            table_store.clone(),
            db_stats.clone(),
        )
        .await;

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_compacted_count.clone());

        garbage_collector.close().await;

        // Verify that the first WAL was deleted and the second is kept
        let compacted_ssts = table_store.list_compacted_ssts(..).await.unwrap();
        assert_eq!(compacted_ssts.len(), 6);
        assert_eq!(compacted_ssts[0].id, l0_sst_handle.id);
        assert_eq!(compacted_ssts[1].id, active_expired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[2].id, inactive_unexpired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[3].id, active_sst_handle.id);
        assert_eq!(compacted_ssts[4].id, active_expired_sst_handle.id);
        assert_eq!(compacted_ssts[5].id, inactive_unexpired_sst_handle.id);
        let current_manifest = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(current_manifest.core.l0.len(), 2);
        assert_eq!(current_manifest.core.compacted.len(), 1);
        assert_eq!(current_manifest.core.compacted[0].ssts.len(), 2);
    }

    /// Builds the objects needed to construct the garbage collector.
    /// # Returns
    /// A tuple containing the manifest store, table store, local object store,
    /// and database stats
    fn build_objects() -> (
        Arc<ManifestStore>,
        Arc<TableStore>,
        Arc<LocalFileSystem>,
        Arc<DbStats>,
    ) {
        let tempdir = tempfile::tempdir().unwrap().into_path();
        let local_object_store = Arc::new(
            LocalFileSystem::new_with_prefix(tempdir)
                .unwrap()
                .with_automatic_cleanup(true),
        );
        let path = Path::from("/");
        let manifest_store = Arc::new(ManifestStore::new(&path, local_object_store.clone()));
        let sst_format = SsTableFormat::new(4096, 10, None);
        let table_store = Arc::new(TableStore::new(
            local_object_store.clone(),
            sst_format,
            path.clone(),
            None,
        ));
        let db_stats = Arc::new(DbStats::new());

        (manifest_store, table_store, local_object_store, db_stats)
    }

    /// Build a garbage collector for testing. The garbage collector is started
    /// as it's returned. The garbage collector is constructed separately from
    /// the other objects so that it can be started later (since the GC has no
    /// start method--it always starts on `new()`). This allows us to seed the
    /// object store with data before the GC starts.
    /// # Returns
    /// The started garbage collector
    async fn build_garbage_collector(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        db_stats: Arc<DbStats>,
    ) -> GarbageCollector {
        GarbageCollector::new(
            manifest_store.clone(),
            table_store.clone(),
            crate::config::GarbageCollectorOptions {
                manifest_options: Some(crate::config::GarbageCollecterDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    poll_interval: std::time::Duration::from_secs(0),
                }),
                wal_options: Some(crate::config::GarbageCollecterDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    poll_interval: std::time::Duration::from_secs(0),
                }),
                compacted_options: Some(crate::config::GarbageCollecterDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    poll_interval: std::time::Duration::from_secs(0),
                }),
                gc_runtime: None,
            },
            tokio::runtime::Handle::current(),
            db_stats.clone(),
        )
        .await
    }

    /// Create an SSTable and write it to the table store.
    /// # Arguments
    /// * `table_store` - The table store to write the SSTable to
    /// # Returns
    /// The handle to the SSTable that was created
    async fn create_sst(table_store: Arc<TableStore>) -> SsTableHandle {
        // Always sleep 1ms to make sure we get ULIDs that are sortable.
        // Without this, the ULIDs could have the same millisecond timestamp
        // and then ULID sorting is based on the random part.
        std::thread::sleep(std::time::Duration::from_millis(1));

        let sst_id = SsTableId::Compacted(Ulid::new());
        let mut sst = table_store.table_builder();
        sst.add(b"key", Some(b"value")).unwrap();
        let table = sst.build().unwrap();
        table_store.write_sst(&sst_id, table).await.unwrap()
    }

    /// Set the modified time of a file to be a certain number of seconds ago.
    /// # Arguments
    /// * `local_object_store` - The local object store that is writing files
    /// * `path` - The path to the file to modify (relative to the object store root)
    /// * `seconds_ago` - The number of seconds ago to set the modified time to
    /// # Returns
    /// The new modified time
    fn set_modified(
        local_object_store: Arc<LocalFileSystem>,
        path: &Path,
        seconds_ago: u64,
    ) -> DateTime<Utc> {
        let file = local_object_store.path_to_filesystem(path).unwrap();
        let file = File::open(file).unwrap();
        let now_minus_24h = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(seconds_ago))
            .unwrap();
        file.set_modified(now_minus_24h).unwrap();
        DateTime::<Utc>::from(now_minus_24h)
    }

    /// Wait for the garbage collector to run at least once.
    /// # Arguments
    /// * `counter` - The counter to wait for. Could be the manifest, WAL, or
    ///   compacted counter.
    fn wait_for_gc(counter: Counter) {
        let current = counter.get();
        while counter.get() == current {
            info!("Waiting for garbage collector to run");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
