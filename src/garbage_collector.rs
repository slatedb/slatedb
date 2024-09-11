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
