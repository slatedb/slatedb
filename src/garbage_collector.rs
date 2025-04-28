use crate::checkpoint::Checkpoint;
use crate::config::GcExecutionMode::Periodic;
use crate::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions, GcExecutionMode};
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::garbage_collector::stats::GcStats;
use crate::garbage_collector::GarbageCollectorMessage::*;
use crate::manifest::store::{DirtyManifest, ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::stats::StatRegistry;
use crate::tablestore::{SstFileMetadata, TableStore};
use crate::utils::spawn_bg_thread;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tracing::{debug, error, info};

const DEFAULT_MIN_AGE: Duration = Duration::from_secs(86400);

#[derive(Debug)]
enum GarbageCollectorMessage {
    Shutdown,
}

pub(crate) struct GarbageCollector {
    main_tx: Arc<crossbeam_channel::Sender<GarbageCollectorMessage>>,
    shutdown_rx: watch::Receiver<bool>,
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
    /// * `stat_registry` - The stat registry to add gc stats to
    /// # Returns
    /// A new garbage collector
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: GarbageCollectorOptions,
        tokio_handle: Handle,
        stat_registry: Arc<StatRegistry>,
        cleanup_fn: impl FnOnce(&Result<(), SlateDBError>) + Send + 'static,
    ) -> Self {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let tokio_handle = options.gc_runtime.clone().unwrap_or(tokio_handle);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let stats = Arc::new(GcStats::new(stat_registry));

        let gc_main = move || {
            let orchestrator = GarbageCollectorOrchestrator {
                manifest_store,
                table_store,
                options,
                external_rx,
                stats,
            };
            tokio_handle.block_on(orchestrator.run());

            // !important: make sure that this is always the last thing that this
            // thread does, otherwise we risk notifying waiters and leaving this
            // thread around after shutdown
            if shutdown_tx.send(true).is_err() {
                error!("Could not send shutdown signal to threads blocked on await_shutdown");
            }
            Ok(())
        };
        spawn_bg_thread("slatedb-gc", cleanup_fn, gc_main);
        Self {
            main_tx: Arc::new(external_tx),
            shutdown_rx,
        }
    }

    /// Waits for the main garbage collection thread to complete. This does
    /// not cause the thread to shut down, use [trigger_shutdown] or [close]
    /// instead to signal the thread to terminate
    pub(crate) async fn await_shutdown(mut self) {
        while !*self.shutdown_rx.borrow() {
            self.shutdown_rx
                .changed()
                .await
                .expect("Shutdown rx disconnected.");
        }
    }

    pub(crate) fn register_interrupt_handler(&self) {
        let main_tx = self.main_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install CTRL+C signal handler");
            debug!("Intercepted SIGINT ... shutting down garbage collector");
            // if we cant send a shutdown message it's probably because it's already closed
            let _ignored_error = main_tx.send(Shutdown);
        });
    }

    /// Triggers the main garbage collection thread to terminate
    fn trigger_shutdown(&self) {
        if self.main_tx.send(Shutdown).is_err() {
            error!("Could not send shutdown signal to threads blocked on await_shutdown");
        }
    }

    /// Close the garbage collector and await clean termination
    pub(crate) async fn close(self) {
        self.trigger_shutdown();
        self.await_shutdown().await;
    }
}

impl Drop for GarbageCollector {
    fn drop(&mut self) {
        debug!("Garbage collector dropped - external_tx will be disconnected.");
    }
}

struct GarbageCollectorOrchestrator {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: GarbageCollectorOptions,
    external_rx: crossbeam_channel::Receiver<GarbageCollectorMessage>,
    stats: Arc<GcStats>,
}

trait GcTask {
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError>;
    fn status(&mut self) -> &mut DirGcStatus;
    fn ticker(&self) -> crossbeam_channel::Receiver<Instant>;
    fn resource(&self) -> &str;
}

struct ManifestGcTask {
    manifest_store: Arc<ManifestStore>,
    stats: Arc<GcStats>,
    manifest_options: Option<GarbageCollectorDirectoryOptions>,
    status: DirGcStatus,
}

impl ManifestGcTask {
    fn new(
        manifest_store: Arc<ManifestStore>,
        stats: Arc<GcStats>,
        manifest_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        ManifestGcTask {
            manifest_store,
            stats,
            manifest_options,
            status: DirGcStatus::new(manifest_options.as_ref()),
        }
    }

    fn manifest_min_age(&self) -> chrono::Duration {
        let min_age = self
            .manifest_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
    }
}

impl GcTask for ManifestGcTask {
    /// Collect garbage from the manifest store. This will delete any manifests
    /// that are older than the minimum age specified in the options.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let min_age = self.manifest_min_age();
        let mut manifest_metadata_list = self.manifest_store.list_manifests(..).await?;

        // Remove the last element so we never delete the latest manifest
        let latest_manifest = if let Some(manifest_metadata) = manifest_metadata_list.pop() {
            self.manifest_store
                .read_manifest(manifest_metadata.id)
                .await?
        } else {
            return Err(SlateDBError::LatestManifestMissing);
        };

        // Do not delete manifests which are still referenced by active checkpoints
        let active_manifest_ids: HashSet<_> = latest_manifest
            .core
            .checkpoints
            .iter()
            .map(|checkpoint| checkpoint.manifest_id)
            .collect();

        // Delete manifests older than min_age
        for manifest_metadata in manifest_metadata_list {
            let is_active = active_manifest_ids.contains(&manifest_metadata.id);
            if !is_active
                && utc_now.signed_duration_since(manifest_metadata.last_modified) > min_age
            {
                if let Err(e) = self
                    .manifest_store
                    .delete_manifest(manifest_metadata.id)
                    .await
                {
                    error!("Error deleting manifest: {}", e);
                } else {
                    self.stats.gc_manifest_count.inc();
                }
            }
        }

        Ok(())
    }

    fn status(&mut self) -> &mut DirGcStatus {
        &mut self.status
    }

    fn ticker(&self) -> crossbeam_channel::Receiver<Instant> {
        self.status.ticker()
    }

    fn resource(&self) -> &str {
        "Manifest"
    }
}

struct WalGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    wal_options: Option<GarbageCollectorDirectoryOptions>,
    status: DirGcStatus,
}

impl WalGcTask {
    fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        wal_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        WalGcTask {
            manifest_store,
            table_store,
            stats,
            wal_options,
            status: DirGcStatus::new(wal_options.as_ref()),
        }
    }

    fn is_wal_sst_eligible_for_deletion(
        utc_now: &DateTime<Utc>,
        wal_sst: &SstFileMetadata,
        min_age: &chrono::Duration,
        active_manifests: &BTreeMap<u64, Manifest>,
    ) -> bool {
        if utc_now.signed_duration_since(wal_sst.last_modified) <= *min_age {
            return false;
        }

        let wal_sst_id = wal_sst.id.unwrap_wal_id();
        !active_manifests
            .values()
            .any(|manifest| manifest.has_wal_sst_reference(wal_sst_id))
    }

    fn wal_sst_min_age(&self) -> chrono::Duration {
        let min_age = self
            .wal_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
    }
}

impl GcTask for WalGcTask {
    /// Collect garbage from the WAL SSTs. This will delete any WAL SSTs that meet
    /// the following conditions:
    ///  - not referenced by an active checkpoint
    ///  - older than the minimum age specified in the options
    ///  - older than the last compacted WAL SST.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let active_manifests = self.manifest_store.read_active_manifests().await?;
        let latest_manifest = active_manifests
            .last_key_value()
            .ok_or(SlateDBError::LatestManifestMissing)?
            .1;

        let last_compacted_wal_sst_id = latest_manifest.core.last_compacted_wal_sst_id;
        let min_age = self.wal_sst_min_age();
        let sst_ids_to_delete = self
            .table_store
            .list_wal_ssts(..last_compacted_wal_sst_id)
            .await?
            .into_iter()
            .filter(|wal_sst| {
                Self::is_wal_sst_eligible_for_deletion(
                    &utc_now,
                    wal_sst,
                    &min_age,
                    &active_manifests,
                )
            })
            .map(|wal_sst| wal_sst.id)
            .collect::<Vec<_>>();

        for id in sst_ids_to_delete {
            if let Err(e) = self.table_store.delete_sst(&id).await {
                error!("Error deleting WAL SST: {}", e);
            } else {
                self.stats.gc_wal_count.inc();
            }
        }

        Ok(())
    }

    fn status(&mut self) -> &mut DirGcStatus {
        &mut self.status
    }

    fn ticker(&self) -> crossbeam_channel::Receiver<Instant> {
        self.status.ticker()
    }

    fn resource(&self) -> &str {
        "WAL"
    }
}

struct CompactedGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    compacted_options: Option<GarbageCollectorDirectoryOptions>,
    status: DirGcStatus,
}

impl CompactedGcTask {
    fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        compacted_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        CompactedGcTask {
            manifest_store,
            table_store,
            stats,
            compacted_options,
            status: DirGcStatus::new(compacted_options.as_ref()),
        }
    }

    fn compacted_sst_min_age(&self) -> chrono::Duration {
        let min_age = self
            .compacted_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
    }

    async fn list_active_l0_and_compacted_ssts(&self) -> Result<HashSet<SsTableId>, SlateDBError> {
        let active_manifests = self.manifest_store.read_active_manifests().await?;
        let mut active_ssts = HashSet::new();
        for manifest in active_manifests.values() {
            for sr in manifest.core.compacted.iter() {
                for sst in sr.ssts.iter() {
                    active_ssts.insert(sst.id);
                }
            }
            for sst in manifest.core.l0.iter() {
                active_ssts.insert(sst.id);
            }
        }
        Ok(active_ssts)
    }
}

impl GcTask for CompactedGcTask {
    /// Collect garbage from the compacted SSTs. This will delete any compacted SSTs that are
    /// older than the minimum age specified in the options and are not active in the manifest.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let active_ssts = self.list_active_l0_and_compacted_ssts().await?;
        let min_age = self.compacted_sst_min_age();
        let sst_ids_to_delete = self
            .table_store
            // List all SSTs in the table store
            .list_compacted_ssts(..)
            .await?
            .into_iter()
            // Filter out the ones that are too young to be collected
            .filter(|sst| utc_now.signed_duration_since(sst.last_modified) > min_age)
            .map(|sst| sst.id)
            // Filter out the ones that are active in the manifest
            .filter(|id| !active_ssts.contains(id))
            .collect::<Vec<_>>();

        for id in sst_ids_to_delete {
            if let Err(e) = self.table_store.delete_sst(&id).await {
                error!("Error deleting SST: {}", e);
            } else {
                self.stats.gc_compacted_count.inc();
            }
        }

        Ok(())
    }

    fn status(&mut self) -> &mut DirGcStatus {
        &mut self.status
    }

    fn ticker(&self) -> crossbeam_channel::Receiver<Instant> {
        self.status.ticker()
    }

    fn resource(&self) -> &str {
        "Compacted SSTs"
    }
}

impl GarbageCollectorOrchestrator {
    async fn load_stored_manifest(&self) -> Result<StoredManifest, SlateDBError> {
        StoredManifest::load(Arc::clone(&self.manifest_store)).await
    }

    fn filter_expired_checkpoints(
        manifest: &StoredManifest,
    ) -> Result<Option<DirtyManifest>, SlateDBError> {
        let utc_now = Utc::now();
        let mut dirty = manifest.prepare_dirty();
        let retained_checkpoints: Vec<Checkpoint> = dirty
            .core
            .checkpoints
            .iter()
            .filter(|checkpoint| match checkpoint.expire_time {
                Some(expire_time) => DateTime::<Utc>::from(expire_time) > utc_now,
                None => true,
            })
            .cloned()
            .collect();

        let maybe_dirty = if dirty.core.checkpoints.len() != retained_checkpoints.len() {
            dirty.core.checkpoints = retained_checkpoints;
            Some(dirty)
        } else {
            None
        };
        Ok(maybe_dirty)
    }

    async fn remove_expired_checkpoints(&self) -> Result<(), SlateDBError> {
        let mut stored_manifest = self.load_stored_manifest().await?;
        stored_manifest
            .maybe_apply_manifest_update(Self::filter_expired_checkpoints)
            .await
    }

    /// Run the garbage collector
    pub async fn run(&self) {
        let log_ticker = crossbeam_channel::tick(Duration::from_secs(60));

        let mut wal_gc_task = WalGcTask::new(
            self.manifest_store.clone(),
            self.table_store.clone(),
            self.stats.clone(),
            self.options.wal_options,
        );
        let mut compacted_gc_task = CompactedGcTask::new(
            self.manifest_store.clone(),
            self.table_store.clone(),
            self.stats.clone(),
            self.options.compacted_options,
        );
        let mut manifest_gc_task = ManifestGcTask::new(
            self.manifest_store.clone(),
            self.stats.clone(),
            self.options.manifest_options,
        );

        let manifest_ticker = manifest_gc_task.ticker();
        let wal_ticker = wal_gc_task.ticker();
        let compacted_ticker = compacted_gc_task.ticker();

        info!(
            "Starting Garbage Collector with [manifest: {}], [wal: {}], [compacted: {}]",
            manifest_gc_task.status(),
            wal_gc_task.status(),
            compacted_gc_task.status()
        );

        loop {
            crossbeam_channel::select! {
                recv(log_ticker) -> _ => {
                   debug!("GC has collected {} Manifests, {} WAL SSTs and {} Compacted SSTs.",
                        self.stats.gc_manifest_count.value.load(Ordering::SeqCst),
                        self.stats.gc_wal_count.value.load(Ordering::SeqCst),
                        self.stats.gc_compacted_count.value.load(Ordering::SeqCst)
                    );
                },
                recv(manifest_ticker) -> _ => { self.run_gc_task(&mut manifest_gc_task).await; },
                recv(wal_ticker) -> _ => { self.run_gc_task(&mut wal_gc_task).await; },
                recv(compacted_ticker) -> _ => { self.run_gc_task(&mut compacted_gc_task).await; },
                recv(self.external_rx) -> msg => {
                    match msg {
                        Ok(Shutdown) => {
                            info!("Garbage collector received shutdown signal... shutting down");
                            break
                        }
                        Err(e) => {
                            error!("Garbage collector received error message {}. Shutting down", e);
                            break;
                        }
                    }
                },
            }
            self.stats.gc_count.inc();
            if manifest_gc_task.status().is_done()
                && wal_gc_task.status().is_done()
                && compacted_gc_task.status().is_done()
            {
                info!("Garbage Collector is done - exiting main thread.");
                break;
            }
        }

        info!(
            "GC shutdown after collecting {} Manifests, {} WAL SSTs and {} Compacted SSTs.",
            self.stats.gc_manifest_count.value.load(Ordering::SeqCst),
            self.stats.gc_wal_count.value.load(Ordering::SeqCst),
            self.stats.gc_compacted_count.value.load(Ordering::SeqCst)
        );
    }

    async fn run_gc_task<T: GcTask>(&self, task: &mut T) {
        debug!(
            "Scheduled garbage collection attempt for {}.",
            task.resource()
        );
        if let Err(e) = self.remove_expired_checkpoints().await {
            error!("Error removing expired checkpoints: {}", e);
        } else if let Err(e) = task.collect(Utc::now()).await {
            error!("Error collecting compacted garbage: {}", e);
        }
        task.status().advance();
    }
}

#[derive(Eq, PartialEq)]
enum DirGcStatus {
    Indefinite(Duration),
    OneMore,
    Done,
}

impl DirGcStatus {
    fn new(options: Option<&GarbageCollectorDirectoryOptions>) -> Self {
        options.map_or(DirGcStatus::Done, |opts| match opts.execution_mode {
            GcExecutionMode::Once => DirGcStatus::OneMore,
            Periodic(duration) => DirGcStatus::Indefinite(duration),
        })
    }

    fn is_done(&self) -> bool {
        self == &DirGcStatus::Done
    }

    fn ticker(&self) -> crossbeam_channel::Receiver<Instant> {
        match self {
            DirGcStatus::Indefinite(duration) => crossbeam_channel::tick(*duration),
            DirGcStatus::OneMore => crossbeam_channel::at(Instant::now()),
            DirGcStatus::Done => crossbeam_channel::never(),
        }
    }

    fn advance(&mut self) {
        let next = match self {
            DirGcStatus::Indefinite(_) => return,
            DirGcStatus::OneMore => DirGcStatus::Done,
            DirGcStatus::Done => DirGcStatus::Done,
        };
        *self = next;
    }
}

impl fmt::Display for DirGcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DirGcStatus::Indefinite(duration) => {
                write!(f, "Run Every {:?}", duration)
            }
            DirGcStatus::OneMore => {
                write!(f, "Run Once")
            }
            DirGcStatus::Done => {
                write!(f, "Done")
            }
        }
    }
}

pub mod stats {
    use crate::stats::{Counter, StatRegistry};
    use std::sync::Arc;

    macro_rules! gc_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("gc", $suffix)
        };
    }

    pub const GC_MANIFEST_COUNT: &str = gc_stat_name!("manifest_count");
    pub const GC_WAL_COUNT: &str = gc_stat_name!("wal_count");
    pub const GC_COMPACTED_COUNT: &str = gc_stat_name!("compacted_count");
    pub const GC_COUNT: &str = gc_stat_name!("count");

    pub(super) struct GcStats {
        pub(super) gc_manifest_count: Arc<Counter>,
        pub(super) gc_wal_count: Arc<Counter>,
        pub(super) gc_compacted_count: Arc<Counter>,
        pub(super) gc_count: Arc<Counter>,
    }

    impl GcStats {
        pub(super) fn new(registry: Arc<StatRegistry>) -> Self {
            let stats = Self {
                gc_manifest_count: Arc::new(Counter::default()),
                gc_wal_count: Arc::new(Counter::default()),
                gc_compacted_count: Arc::new(Counter::default()),
                gc_count: Arc::new(Counter::default()),
            };
            registry.register(GC_MANIFEST_COUNT, stats.gc_manifest_count.clone());
            registry.register(GC_WAL_COUNT, stats.gc_wal_count.clone());
            registry.register(GC_COMPACTED_COUNT, stats.gc_compacted_count.clone());
            registry.register(GC_COUNT, stats.gc_count.clone());
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::{fs::File, sync::Arc, time::SystemTime};

    use chrono::{DateTime, Utc};
    use log::info;
    use object_store::{local::LocalFileSystem, path::Path};
    use ulid::Ulid;
    use uuid::Uuid;

    use crate::checkpoint::Checkpoint;
    use crate::config::GcExecutionMode::Once;
    use crate::error::SlateDBError;
    use crate::paths::PathResolver;
    use crate::stats::{ReadableStat, StatRegistry};
    use crate::types::RowEntry;
    use crate::{
        db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId},
        garbage_collector::GarbageCollector,
        manifest::store::{ManifestStore, StoredManifest},
        sst::SsTableFormat,
        tablestore::TableStore,
    };

    use crate::garbage_collector::stats::GC_COUNT;

    #[tokio::test]
    async fn test_collect_garbage_manifest() {
        let (manifest_store, table_store, local_object_store) = build_objects();

        // Create a manifest
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_manifest(stored_manifest.prepare_dirty())
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
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that the first manifest was deleted
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    #[tokio::test]
    async fn test_collect_garbage_only_recent_manifests() {
        let (manifest_store, table_store, _) = build_objects();

        // Create a manifest
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_manifest(stored_manifest.prepare_dirty())
            .await
            .unwrap();

        // Verify that the manifests are there as expected
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that no manifests were deleted
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);
    }

    fn new_checkpoint(manifest_id: u64, expire_time: Option<SystemTime>) -> Checkpoint {
        Checkpoint {
            id: Uuid::new_v4(),
            manifest_id,
            expire_time,
            create_time: SystemTime::now(),
        }
    }

    async fn checkpoint_current_manifest(
        stored_manifest: &mut StoredManifest,
        expire_time: Option<SystemTime>,
    ) -> Result<Uuid, SlateDBError> {
        let mut dirty = stored_manifest.prepare_dirty();
        let checkpoint = new_checkpoint(stored_manifest.id(), expire_time);
        let checkpoint_id = checkpoint.id;
        dirty.core.checkpoints.push(checkpoint);
        stored_manifest.update_manifest(dirty).await?;
        Ok(checkpoint_id)
    }

    async fn remove_checkpoint(
        checkpoint_id: Uuid,
        stored_manifest: &mut StoredManifest,
    ) -> Result<(), SlateDBError> {
        let mut dirty = stored_manifest.prepare_dirty();
        let updated_checkpoints = dirty
            .core
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.id != checkpoint_id)
            .cloned()
            .collect();
        dirty.core.checkpoints = updated_checkpoints;
        stored_manifest.update_manifest(dirty).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_expired_checkpoints() {
        let (manifest_store, table_store, local_object_store) = build_objects();

        // Manifest 1
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        // Manifest 2 (expired_checkpoint_id -> 1)
        let one_day_ago = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(86400))
            .unwrap();
        let _expired_checkpoint_id =
            checkpoint_current_manifest(&mut stored_manifest, Some(one_day_ago))
                .await
                .unwrap();
        // Manifest 3 (expired_checkpoint_id -> 1, unexpired_checkpoint_id -> 2)
        let one_day_ahead = SystemTime::now()
            .checked_add(std::time::Duration::from_secs(86400))
            .unwrap();
        let unexpired_checkpoint_id =
            checkpoint_current_manifest(&mut stored_manifest, Some(one_day_ahead))
                .await
                .unwrap();

        // Make all manifests eligible for deletion
        for i in 1..=3 {
            set_modified(
                local_object_store.clone(),
                &Path::from(format!("manifest/{:020}.{}", i, "manifest")),
                86400,
            );
        }

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // The GC should create a new manifest version 4 with the expired
        // checkpoint removed.
        let (latest_manifest_id, latest_manifest) =
            manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(4, latest_manifest_id);
        assert_eq!(1, latest_manifest.core.checkpoints.len());
        assert_eq!(
            unexpired_checkpoint_id,
            latest_manifest.core.checkpoints[0].id
        );
        assert_eq!(2, latest_manifest.core.checkpoints[0].manifest_id);

        // Only the latest manifest and the one referenced by the unexpired checkpoint
        // should be retained.
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 2);
        assert_eq!(manifests[1].id, 4);
    }

    #[tokio::test]
    async fn test_collector_should_not_clean_manifests_referenced_by_checkpoints() {
        let (manifest_store, table_store, local_object_store) = build_objects();

        // Manifest 1
        let state = CoreDbState::new();
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();
        // Manifest 2 (active_checkpoint_id -> 1)
        let active_checkpoint_id = checkpoint_current_manifest(&mut stored_manifest, None)
            .await
            .unwrap();
        // Manifest 3 (active_checkpoint_id -> 1, inactive_checkpoint_id -> 2)
        let inactive_checkpoint_id = checkpoint_current_manifest(&mut stored_manifest, None)
            .await
            .unwrap();
        // Manifest 4 (active_checkpoint_id -> 1)
        remove_checkpoint(inactive_checkpoint_id, &mut stored_manifest)
            .await
            .unwrap();

        // Set the older manifests to be a day old to make them eligible for deletion
        for i in 1..4 {
            set_modified(
                local_object_store.clone(),
                &Path::from(format!("manifest/{:020}.{}", i, "manifest")),
                86400,
            );
        }

        // Verify that the manifests are there as expected
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 4);

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that the latest manifest version is still 4 with the active checkpoint
        let (latest_manifest_id, latest_manifest) =
            manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(4, latest_manifest_id);
        assert_eq!(1, latest_manifest.core.checkpoints.len());
        assert_eq!(active_checkpoint_id, latest_manifest.core.checkpoints[0].id);
        assert_eq!(1, latest_manifest.core.checkpoints[0].manifest_id);

        // The active manifest and the manifest corresponding to the active
        // checkpoint should be retained. The rest should be deleted.
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 4);
    }

    #[tokio::test]
    async fn test_collect_garbage_old_active_manifest() {
        let (manifest_store, table_store, local_object_store) = build_objects();

        // Create a manifest
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
                .await
                .unwrap();

        // Add a second manifest
        stored_manifest
            .update_manifest(stored_manifest.prepare_dirty())
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
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that the first manifest was deleted, but the second is still safe
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    async fn write_sst(
        table_store: Arc<TableStore>,
        table_id: &SsTableId,
    ) -> Result<(), SlateDBError> {
        let mut sst = table_store.table_builder();
        sst.add(RowEntry::new_value(b"key", b"value", 0))?;
        let table1 = sst.build()?;
        table_store.write_sst(table_id, table1, false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_garbage_wal_ssts() {
        let (manifest_store, table_store, local_object_store) = build_objects();
        let path_resolver = PathResolver::new("/");

        // write a wal sst
        let id1 = SsTableId::Wal(1);
        write_sst(table_store.clone(), &id1).await.unwrap();

        let id2 = SsTableId::Wal(2);
        write_sst(table_store.clone(), &id2).await.unwrap();

        // Set the first WAL SST file to be a day old
        let now_minus_24h = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&SsTableId::Wal(1)),
            86400,
        );

        // Create a manifest
        let mut state = CoreDbState::new();
        state.last_compacted_wal_sst_id = id2.unwrap_wal_id();
        StoredManifest::create_new_db(manifest_store.clone(), state.clone())
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
        let current_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(
            current_manifest.core.last_compacted_wal_sst_id,
            id2.unwrap_wal_id()
        );

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that the first WAL was deleted and the second is kept
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 1);
        assert_eq!(wal_ssts[0].id, id2);
    }

    #[tokio::test]
    async fn test_do_not_remove_wals_referenced_by_active_checkpoints() {
        let (manifest_store, table_store, local_object_store) = build_objects();
        let path_resolver = PathResolver::new("/");

        let id1 = SsTableId::Wal(1);
        write_sst(table_store.clone(), &id1).await.unwrap();

        let id2 = SsTableId::Wal(2);
        write_sst(table_store.clone(), &id2).await.unwrap();

        let id3 = SsTableId::Wal(3);
        write_sst(table_store.clone(), &id3).await.unwrap();

        // Manifest 1 with table 1 eligible for deletion
        let mut state = CoreDbState::new();
        state.last_compacted_wal_sst_id = 1;
        state.next_wal_sst_id = 4;
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();
        assert_eq!(1, stored_manifest.id());

        // Manifest 2 with checkpoint referencing Manifest 1
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.last_compacted_wal_sst_id = 3;
        dirty.core.next_wal_sst_id = 4;
        dirty.core.checkpoints.push(new_checkpoint(1, None));
        stored_manifest.update_manifest(dirty).await.unwrap();
        assert_eq!(2, stored_manifest.id());

        // All tables are eligible for deletion
        for i in 1..=3 {
            set_modified(
                local_object_store.clone(),
                &path_resolver.table_path(&SsTableId::Wal(i)),
                86400,
            );
        }

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Only the first table is deleted. The second is eligible,
        // but the reference in the checkpoint is still active.
        let wal_ssts = table_store.list_wal_ssts(..).await.unwrap();
        assert_eq!(wal_ssts.len(), 2);
        assert_eq!(wal_ssts[0].id, id2);
        assert_eq!(wal_ssts[1].id, id3);
    }

    #[tokio::test]
    async fn test_collect_garbage_wal_ssts_and_keep_expired_last_compacted() {
        let (manifest_store, table_store, local_object_store) = build_objects();
        let path_resolver = PathResolver::new("/");

        // write a wal sst
        let id1 = SsTableId::Wal(1);
        let mut sst1 = table_store.table_builder();
        sst1.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();

        let table1 = sst1.build().unwrap();
        table_store.write_sst(&id1, table1, false).await.unwrap();

        let id2 = SsTableId::Wal(2);
        let mut sst2 = table_store.table_builder();
        sst2.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table2 = sst2.build().unwrap();
        table_store.write_sst(&id2, table2, false).await.unwrap();

        // Set the both WAL SST file to be a day old
        let now_minus_24h_1 = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&SsTableId::Wal(1)),
            86400,
        );
        let now_minus_24h_2 = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&SsTableId::Wal(2)),
            86400,
        );

        // Create a manifest
        let mut state = CoreDbState::new();
        state.last_compacted_wal_sst_id = id2.unwrap_wal_id();
        StoredManifest::create_new_db(manifest_store.clone(), state.clone())
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
        let current_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(
            current_manifest.core.last_compacted_wal_sst_id,
            id2.unwrap_wal_id()
        );

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

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
        let (manifest_store, table_store, local_object_store) = build_objects();
        let l0_sst_handle = create_sst(table_store.clone()).await;
        let active_expired_l0_sst_handle = create_sst(table_store.clone()).await;
        let inactive_expired_l0_sst_handle = create_sst(table_store.clone()).await;
        let inactive_unexpired_l0_sst_handle = create_sst(table_store.clone()).await;
        let active_sst_handle = create_sst(table_store.clone()).await;
        let active_expired_sst_handle = create_sst(table_store.clone()).await;
        let inactive_expired_sst_handle = create_sst(table_store.clone()).await;
        let inactive_unexpired_sst_handle = create_sst(table_store.clone()).await;
        let path_resolver = PathResolver::new("");

        // Set expiration for the old SSTs
        let now_minus_24h_expired_l0_sst = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&active_expired_l0_sst_handle.id),
            86400,
        );
        let now_minus_24h_inactive_expired_l0_sst = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&inactive_expired_l0_sst_handle.id),
            86400,
        );
        let now_minus_24h_active_expired_sst = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&active_expired_sst_handle.id),
            86400,
        );
        let now_minus_24h_inactive_expired_sst_id = set_modified(
            local_object_store.clone(),
            &path_resolver.table_path(&inactive_expired_sst_handle.id),
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
        StoredManifest::create_new_db(manifest_store.clone(), state.clone())
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
        let current_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(current_manifest.core.l0.len(), 2);
        assert_eq!(current_manifest.core.compacted.len(), 1);
        assert_eq!(current_manifest.core.compacted[0].ssts.len(), 2);

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Verify that the first WAL was deleted and the second is kept
        let compacted_ssts = table_store.list_compacted_ssts(..).await.unwrap();
        assert_eq!(compacted_ssts.len(), 6);
        assert_eq!(compacted_ssts[0].id, l0_sst_handle.id);
        assert_eq!(compacted_ssts[1].id, active_expired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[2].id, inactive_unexpired_l0_sst_handle.id);
        assert_eq!(compacted_ssts[3].id, active_sst_handle.id);
        assert_eq!(compacted_ssts[4].id, active_expired_sst_handle.id);
        assert_eq!(compacted_ssts[5].id, inactive_unexpired_sst_handle.id);
        let current_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(current_manifest.core.l0.len(), 2);
        assert_eq!(current_manifest.core.compacted.len(), 1);
        assert_eq!(current_manifest.core.compacted[0].ssts.len(), 2);
    }

    /// This test creates six compacted SSTs:
    /// - One L0 SST
    /// - One inactive expired L0 SST (w/ checkpoint)
    /// - One inactive expired L0 SST (w/o checkpoint)
    /// - One active SST
    /// - One inactive expired SST (w/ checkpoint)
    /// - One inactive expired SST (w/o checkpoint)
    /// The test then runs the compactor to verify that only the inactive expired SSTs
    /// are deleted.
    #[tokio::test]
    async fn test_collect_garbage_compacted_ssts_respects_checkpoint_references() {
        let (manifest_store, table_store, local_object_store) = build_objects();
        let active_l0_sst_handle = create_sst(table_store.clone()).await;
        let active_checkpoint_l0_sst_handle = create_sst(table_store.clone()).await;
        let inactive_l0_sst_handle = create_sst(table_store.clone()).await;
        let active_sst_handle = create_sst(table_store.clone()).await;
        let active_checkpoint_sst_handle = create_sst(table_store.clone()).await;
        let inactive_sst_handle = create_sst(table_store.clone()).await;
        let path_resolver = PathResolver::new("");

        // Set expiration for all SSTs to make them eligible for deletion
        let all_tables = vec![
            active_sst_handle.clone(),
            active_checkpoint_l0_sst_handle.clone(),
            inactive_l0_sst_handle.clone(),
            active_sst_handle.clone(),
            active_checkpoint_sst_handle.clone(),
            inactive_sst_handle.clone(),
        ];
        for table in &all_tables {
            set_modified(
                local_object_store.clone(),
                &path_resolver.table_path(&table.id),
                86400,
            );
        }

        // Create an initial manifest with active and active checkpoint tables
        let mut state = CoreDbState::new();
        state.l0.push_back(active_l0_sst_handle.clone());
        state.l0.push_back(active_checkpoint_l0_sst_handle.clone());
        state.compacted.push(SortedRun {
            id: 1,
            ssts: vec![active_sst_handle.clone()],
        });
        state.compacted.push(SortedRun {
            id: 2,
            ssts: vec![active_checkpoint_sst_handle.clone()],
        });
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();

        let checkpoint_id = checkpoint_current_manifest(&mut stored_manifest, None)
            .await
            .unwrap();

        // Now drop the active tables from the checkpoint
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.l0.truncate(1);
        dirty.core.compacted.truncate(1);
        stored_manifest.update_manifest(dirty).await.unwrap();

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;

        // Only the first table is deleted. The second is eligible,
        // but the reference in the checkpoint is still active.
        let compacted_ssts = table_store.list_compacted_ssts(..).await.unwrap();
        assert_eq!(compacted_ssts.len(), 4);
        assert_eq!(compacted_ssts[0].id, active_l0_sst_handle.id);
        assert_eq!(compacted_ssts[1].id, active_checkpoint_l0_sst_handle.id);
        assert_eq!(compacted_ssts[2].id, active_sst_handle.id);
        assert_eq!(compacted_ssts[3].id, active_checkpoint_sst_handle.id);

        // Drop the checkpoint and run the GC one more time
        remove_checkpoint(checkpoint_id, &mut stored_manifest)
            .await
            .unwrap();

        // Start the garbage collector
        run_gc_once(manifest_store.clone(), table_store.clone()).await;
        let compacted_ssts = table_store.list_compacted_ssts(..).await.unwrap();
        assert_eq!(compacted_ssts.len(), 2);
        assert_eq!(compacted_ssts[0].id, active_l0_sst_handle.id);
        assert_eq!(compacted_ssts[1].id, active_sst_handle.id);
    }

    /// Builds the objects needed to construct the garbage collector.
    /// # Returns
    /// A tuple containing the manifest store, table store, local object store,
    /// and database stats
    fn build_objects() -> (Arc<ManifestStore>, Arc<TableStore>, Arc<LocalFileSystem>) {
        let tempdir = tempfile::tempdir().unwrap().into_path();
        let local_object_store = Arc::new(
            LocalFileSystem::new_with_prefix(tempdir)
                .unwrap()
                .with_automatic_cleanup(true),
        );
        let path = Path::from("/");
        let manifest_store = Arc::new(ManifestStore::new(&path, local_object_store.clone()));
        let sst_format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            local_object_store.clone(),
            sst_format,
            path.clone(),
            None,
        ));

        (manifest_store, table_store, local_object_store)
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
        stats: Arc<StatRegistry>,
    ) -> GarbageCollector {
        GarbageCollector::new(
            manifest_store.clone(),
            table_store.clone(),
            crate::config::GarbageCollectorOptions {
                manifest_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    execution_mode: Once,
                }),
                wal_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    execution_mode: Once,
                }),
                compacted_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                    min_age: std::time::Duration::from_secs(3600),
                    execution_mode: Once,
                }),
                gc_runtime: None,
            },
            tokio::runtime::Handle::current(),
            stats.clone(),
            |_| {},
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
        sst.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table = sst.build().unwrap();
        table_store.write_sst(&sst_id, table, false).await.unwrap()
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
    fn wait_for_gc(gc_count_reader: Arc<dyn ReadableStat>) {
        let current = gc_count_reader.get();
        while gc_count_reader.get() == current {
            info!("Waiting for garbage collector to run");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    async fn assert_no_dangling_references(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
    ) {
        let manifests = manifest_store.read_active_manifests().await.unwrap();

        let wal_ssts = table_store
            .list_wal_ssts(..)
            .await
            .unwrap()
            .iter()
            .map(|sst| sst.id)
            .collect::<HashSet<SsTableId>>();
        let compacted_ssts = table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .iter()
            .map(|sst| sst.id)
            .collect::<HashSet<SsTableId>>();

        for manifest in manifests.values() {
            let wal_sst_start_inclusive = manifest.core.last_compacted_wal_sst_id + 1;
            let wal_sst_end_exclusive = manifest.core.next_wal_sst_id;
            for wal_sst_id in wal_sst_start_inclusive..wal_sst_end_exclusive {
                assert!(wal_ssts.contains(&SsTableId::Wal(wal_sst_id)));
            }

            for sst in &manifest.core.l0 {
                assert!(compacted_ssts.contains(&sst.id));
            }

            for sr in &manifest.core.compacted {
                for sst in &sr.ssts {
                    assert!(compacted_ssts.contains(&sst.id));
                }
            }
        }
    }

    async fn run_gc_once(manifest_store: Arc<ManifestStore>, table_store: Arc<TableStore>) {
        // Start the garbage collector
        let stats = Arc::new(StatRegistry::new());
        let garbage_collector =
            build_garbage_collector(manifest_store.clone(), table_store.clone(), stats.clone())
                .await;

        // Wait for the garbage collector to run
        wait_for_gc(stats.lookup(GC_COUNT).unwrap());

        garbage_collector.await_shutdown().await;

        // Verify reference integrity
        assert_no_dangling_references(manifest_store, table_store).await;
    }
}
