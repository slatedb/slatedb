//! This module contains SlateDB's garbage collector.
//!
//! The garbage collector is responsible for removing obsolete data from SlateDB storage:
//! - Write-ahead log (WAL) SSTs that are no longer referenced by active manifests or
//!   checkpoints
//! - Compacted SSTs that are no longer referenced by active manifests or checkpoints
//! - Old manifests that are not needed for recovery or checkpoints
//!
//! The garbage collector runs periodically in the background, with configurable intervals
//! and minimum age thresholds for each type of data. This ensures that recently created
//! data isn't immediately deleted, which would risk removing files that might still be
//! referenced by in-flight operations.

use crate::checkpoint::Checkpoint;
use crate::clock::SystemClock;
use crate::config::GarbageCollectorOptions;
use crate::error::SlateDBError;
use crate::garbage_collector::stats::GcStats;
use crate::manifest::store::{DirtyManifest, ManifestStore, StoredManifest};
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use chrono::{DateTime, Utc};
use compacted_gc::CompactedGcTask;
use manifest_gc::ManifestGcTask;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};
use wal_gc::WalGcTask;

mod compacted_gc;
mod manifest_gc;
pub mod stats;
mod wal_gc;

pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(86_400);
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(300);

trait GcTask {
    fn resource(&self) -> &str;
    fn interval(&self) -> Duration;
    fn ticker(&self) -> Interval;
    async fn collect(&self, now: DateTime<Utc>) -> Result<(), SlateDBError>;
}

/// SlateDB's garbage collector.
///
/// The `GarbageCollector` is responsible for cleaning up obsolete data in SlateDB:
///
/// - Write-ahead log (WAL) SSTs that are no longer referenced by active manifests or checkpoints
/// - Compacted SSTs that are no longer referenced by active manifests or checkpoints
/// - Old manifests that are not needed for recovery or checkpoints
///
/// The garbage collector can run in two modes:
///
/// - As an async task with [`run_async_task`](GarbageCollector::run_async_task)
/// - As a one-time operation with [`run_gc_once`](GarbageCollector::run_gc_once)
///
/// The garbage collector uses configurable intervals and minimum age thresholds for each
/// type of data to ensure that recently created data isn't immediately deleted, which
/// helps prevent removing files that might still be referenced by in-flight operations.
#[derive(Clone)]
pub struct GarbageCollector {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: GarbageCollectorOptions,
    stats: Arc<GcStats>,
    system_clock: Arc<dyn SystemClock>,
    cancellation_token: CancellationToken,
}

impl GarbageCollector {
    /// Creates a new `GarbageCollector` instance.
    ///
    /// # Arguments
    ///
    /// * `manifest_store` - The manifest store to use for garbage collection.
    /// * `table_store` - The table store to use for garbage collection.
    /// * `options` - Configuration options for the garbage collector.
    /// * `stat_registry` - Registry for tracking garbage collection metrics.
    /// * `system_clock` - Clock implementation for time-based decisions.
    /// * `cancellation_token` - Token used to signal cancellation of garbage collection tasks.
    ///
    /// # Returns
    ///
    /// A new `GarbageCollector` instance configured with the provided components.
    pub(crate) fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: GarbageCollectorOptions,
        stat_registry: Arc<StatRegistry>,
        system_clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let stats = Arc::new(GcStats::new(stat_registry));
        Self {
            manifest_store,
            table_store,
            options,
            stats,
            system_clock,
            cancellation_token,
        }
    }

    /// Starts the garbage collector. This method performs the actual garbage collection.
    /// The garbage collector runs until the cancellation token is cancelled.
    pub async fn run_async_task(&self) -> Result<(), SlateDBError> {
        let (mut wal_gc_task, mut compacted_gc_task, mut manifest_gc_task) = self.gc_tasks();
        let mut compacted_ticker = compacted_gc_task.ticker();
        let mut wal_ticker = wal_gc_task.ticker();
        let mut manifest_ticker = manifest_gc_task.ticker();
        let mut log_ticker = tokio::time::interval(Duration::from_secs(60));

        info!(
            "Starting Garbage Collector with [manifest: {:#?}], [wal: {:#?}], [compacted: {:#?}]",
            manifest_gc_task.interval(),
            wal_gc_task.interval(),
            compacted_gc_task.interval()
        );

        loop {
            tokio::select! {
                biased;
                // check the cancellation token first to avoid starting new GC tasks when the runtime is shutting down
                _ = self.cancellation_token.cancelled() => {
                    info!("Garbage collector received shutdown signal... shutting down");
                    break;
                },
                _ = manifest_ticker.tick() => { self.run_gc_task(&mut manifest_gc_task).await; },
                _ = wal_ticker.tick() => { self.run_gc_task(&mut wal_gc_task).await; },
                _ = compacted_ticker.tick() => { self.run_gc_task(&mut compacted_gc_task).await; },
                _ = log_ticker.tick() => {
                    debug!("GC has collected {} Manifests, {} WAL SSTs and {} Compacted SSTs.",
                         self.stats.gc_manifest_count.value.load(Ordering::SeqCst),
                         self.stats.gc_wal_count.value.load(Ordering::SeqCst),
                         self.stats.gc_compacted_count.value.load(Ordering::SeqCst)
                     );
                 }
            }
            self.stats.gc_count.inc();
        }

        info!(
            "GC shutdown after collecting {} Manifests, {} WAL SSTs and {} Compacted SSTs.",
            self.stats.gc_manifest_count.value.load(Ordering::SeqCst),
            self.stats.gc_wal_count.value.load(Ordering::SeqCst),
            self.stats.gc_compacted_count.value.load(Ordering::SeqCst)
        );

        Ok(())
    }

    /// Run the garbage collector once.
    ///
    /// This method runs all three garbage collection tasks:
    ///
    /// - WAL SST garbage collection
    /// - Compacted SST garbage collection
    /// - Manifest garbage collection
    pub async fn run_gc_once(&self) {
        let (mut wal_gc_task, mut compacted_gc_task, mut manifest_gc_task) = self.gc_tasks();

        self.run_gc_task(&mut manifest_gc_task).await;
        self.run_gc_task(&mut wal_gc_task).await;
        self.run_gc_task(&mut compacted_gc_task).await;

        self.stats.gc_count.inc();
    }

    fn gc_tasks(&self) -> (WalGcTask, CompactedGcTask, ManifestGcTask) {
        let wal_gc_task = WalGcTask::new(
            self.manifest_store.clone(),
            self.table_store.clone(),
            self.stats.clone(),
            self.options.wal_options,
        );
        let compacted_gc_task = CompactedGcTask::new(
            self.manifest_store.clone(),
            self.table_store.clone(),
            self.stats.clone(),
            self.options.compacted_options,
        );
        let manifest_gc_task = ManifestGcTask::new(
            self.manifest_store.clone(),
            self.stats.clone(),
            self.options.manifest_options,
        );
        (wal_gc_task, compacted_gc_task, manifest_gc_task)
    }

    #[instrument(level = "debug", skip_all, fields(resource = task.resource()))]
    async fn run_gc_task<T: GcTask + std::fmt::Debug>(&self, task: &mut T) {
        if let Err(e) = self.remove_expired_checkpoints().await {
            error!("Error removing expired checkpoints: {}", e);
        } else if let Err(e) = task.collect(self.system_clock.now().into()).await {
            error!("Error collecting compacted garbage: {}", e);
        }
    }

    async fn remove_expired_checkpoints(&self) -> Result<(), SlateDBError> {
        let mut stored_manifest = StoredManifest::load(Arc::clone(&self.manifest_store)).await?;

        stored_manifest
            .maybe_apply_manifest_update(|manifest| self.filter_expired_checkpoints(manifest))
            .await
    }

    fn filter_expired_checkpoints(
        &self,
        manifest: &StoredManifest,
    ) -> Result<Option<DirtyManifest>, SlateDBError> {
        let utc_now: DateTime<Utc> = self.system_clock.now().into();
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::{fs::File, sync::Arc, time::SystemTime};

    use chrono::{DateTime, Utc};
    use object_store::{local::LocalFileSystem, path::Path};
    use tokio::runtime::Handle;
    use uuid::Uuid;

    use crate::checkpoint::Checkpoint;
    use crate::clock::DefaultSystemClock;
    use crate::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions};
    use crate::error::SlateDBError;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::types::RowEntry;
    use crate::utils::spawn_bg_task;
    use crate::{
        db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId},
        manifest::store::{ManifestStore, StoredManifest},
        sst::SsTableFormat,
        tablestore::TableStore,
    };

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
            id: uuid::Uuid::new_v4(),
            manifest_id,
            expire_time,
            create_time: DefaultSystemClock::default().now(),
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
        let one_day_ago = DefaultSystemClock::default()
            .now()
            .checked_sub(std::time::Duration::from_secs(86400))
            .unwrap();
        let _expired_checkpoint_id =
            checkpoint_current_manifest(&mut stored_manifest, Some(one_day_ago))
                .await
                .unwrap();
        // Manifest 3 (expired_checkpoint_id -> 1, unexpired_checkpoint_id -> 2)
        let one_day_ahead = DefaultSystemClock::default()
            .now()
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
        state.replay_after_wal_id = id2.unwrap_wal_id();
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
            current_manifest.core.replay_after_wal_id,
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
        state.replay_after_wal_id = 1;
        state.next_wal_sst_id = 4;
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), state.clone())
                .await
                .unwrap();
        assert_eq!(1, stored_manifest.id());

        // Manifest 2 with checkpoint referencing Manifest 1
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.replay_after_wal_id = 3;
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
        state.replay_after_wal_id = id2.unwrap_wal_id();
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
            current_manifest.core.replay_after_wal_id,
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
            ObjectStores::new(local_object_store.clone(), None),
            sst_format,
            path.clone(),
            None,
        ));

        (manifest_store, table_store, local_object_store)
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
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        let sst_id = SsTableId::Compacted(ulid::Ulid::new());
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
        let now_minus_24h = DefaultSystemClock::default()
            .now()
            .checked_sub(std::time::Duration::from_secs(seconds_ago))
            .unwrap();
        file.set_modified(now_minus_24h).unwrap();
        DateTime::<Utc>::from(now_minus_24h)
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
            let wal_sst_start_inclusive = manifest.core.replay_after_wal_id + 1;
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

        let gc_opts = GarbageCollectorOptions {
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                min_age: std::time::Duration::from_secs(3600),
                interval: None,
            }),
            wal_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                min_age: std::time::Duration::from_secs(3600),
                interval: None,
            }),
            compacted_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                min_age: std::time::Duration::from_secs(3600),
                interval: None,
            }),
        };

        let gc = GarbageCollector::new(
            manifest_store.clone(),
            table_store.clone(),
            gc_opts,
            stats.clone(),
            Arc::new(DefaultSystemClock::default()),
            CancellationToken::new(),
        );

        gc.run_gc_once().await;

        // Verify reference integrity
        assert_no_dangling_references(manifest_store, table_store).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_gc_shutdown() {
        let (manifest_store, table_store, _) = build_objects();
        let stats = Arc::new(StatRegistry::new());

        let gc_opts = GarbageCollectorOptions {
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                min_age: Duration::from_secs(3600),
                interval: Some(Duration::from_secs(1)),
            }),
            wal_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                min_age: Duration::from_secs(3600),
                interval: Some(Duration::from_secs(1)),
            }),
            compacted_options: Some(crate::config::GarbageCollectorDirectoryOptions {
                min_age: Duration::from_secs(3600),
                interval: Some(Duration::from_secs(1)),
            }),
        };

        let cancellation_token = CancellationToken::new();

        let gc = GarbageCollector::new(
            manifest_store.clone(),
            table_store.clone(),
            gc_opts,
            stats.clone(),
            Arc::new(DefaultSystemClock::default()),
            cancellation_token.clone(),
        );

        let fut = async move { gc.run_async_task().await };
        let jh = spawn_bg_task(&Handle::current(), |result| assert!(result.is_ok()), fut);
        cancellation_token.cancel();
        let result = jh.await.unwrap();
        assert!(result.is_ok(), "result: {:#?}", result);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cancellation_token.is_cancelled());
    }
}
