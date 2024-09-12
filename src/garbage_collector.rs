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

            if Utc::now().signed_duration_since(manifest_metadata.last_modified) > min_age {
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
                Utc::now().signed_duration_since(wal_sst.last_modified) > min_age
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
                Utc::now().signed_duration_since(sst.last_modified) > min_age
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

    use crate::{
        db_state::CoreDbState,
        garbage_collector::GarbageCollector,
        manifest_store::{ManifestStore, StoredManifest},
        metrics::{Counter, DbStats},
        sst::SsTableFormat,
        tablestore::TableStore,
    };

    #[tokio::test]
    async fn test_collect_garbage_manifest() {
        let (garbage_collector, manifest_store, _, local_object_store, db_stats) =
            build_objects().await;

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

        // Wait for the garbage collector to run
        wait_for_gc(db_stats.gc_manifest_count.clone());

        garbage_collector.close().await;

        // Verify that the first manifest was deleted
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    /// Build a garbage collector for testing. The garbage collector is started
    /// as it's returned.
    /// # Returns
    /// The started garbage collector
    async fn build_objects() -> (
        GarbageCollector,
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
        let garbage_collector = GarbageCollector::new(
            manifest_store.clone(),
            table_store.clone(),
            crate::config::GarbageCollectorOptions {
                manifest_options: Some(crate::config::GarbageCollecterDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    poll_interval: std::time::Duration::from_secs(0),
                }),
                wal_options: None,
                compacted_options: None,
                gc_runtime: None,
            },
            tokio::runtime::Handle::current(),
            db_stats.clone(),
        )
        .await;

        (
            garbage_collector,
            manifest_store,
            table_store,
            local_object_store,
            db_stats,
        )
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
