use crate::{
    compactions_store::CompactionsStore, compactor_state::Compactions,
    compactor_state_protocols::CompactorStateReader, config::GarbageCollectorDirectoryOptions,
    db_state::SsTableId, error::SlateDBError, manifest::store::ManifestStore, manifest::Manifest,
    tablestore::TableStore,
};
use chrono::{DateTime, Utc};
use log::error;
use std::collections::HashSet;
use std::sync::Arc;

use super::{GcStats, GcTask, DEFAULT_MIN_AGE};

pub(crate) struct CompactedGcTask {
    manifest_store: Arc<ManifestStore>,
    compactions_store: Arc<CompactionsStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    compacted_options: Option<GarbageCollectorDirectoryOptions>,
}

impl std::fmt::Debug for CompactedGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactedGcTask")
            .field("compacted_options", &self.compacted_options)
            .finish()
    }
}

impl CompactedGcTask {
    pub fn new(
        manifest_store: Arc<ManifestStore>,
        compactions_store: Arc<CompactionsStore>,
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        compacted_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        CompactedGcTask {
            manifest_store,
            compactions_store,
            table_store,
            stats,
            compacted_options,
        }
    }

    fn compacted_sst_min_age(&self) -> chrono::Duration {
        let min_age = self
            .compacted_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
    }

    /// Lists all SSTs referenced by the latest manifest and its checkpoints.
    ///
    /// ## Arguments
    /// - `manifest_id`: The id of the latest manifest.
    /// - `manifest`: The latest manifest contents.
    ///
    /// ## Returns
    /// - A set of SST ids referenced by L0 and compacted runs across all referenced manifests.
    async fn list_active_l0_and_compacted_ssts(
        &self,
        manifest_id: u64,
        manifest: &Manifest,
    ) -> Result<HashSet<SsTableId>, SlateDBError> {
        let active_manifests = self
            .manifest_store
            .read_referenced_manifests(manifest_id, manifest)
            .await?;
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

    /// Computes the newest L0 timestamp from the latest manifest.
    ///
    /// This is used as a conservative upper bound for compacted SST deletion. The following
    /// branches are handled in order:
    ///
    /// 1. If there are active L0 SSTs, take the newest (max) L0 timestamp.
    /// 2. Else, if `l0_last_compacted` is set, use that timestamp as a fallback
    ///    barrier for recently compacted L0s.
    /// 3. Else, if the DB has never had L0s, return the Unix epoch to disable
    ///    deletion based on this signal.
    ///
    /// ## Arguments
    /// - `manifest`: The latest manifest contents.
    ///
    /// ## Returns
    /// - The newest L0 timestamp if any L0s exist, otherwise a conservative fallback
    ///   (last compacted L0 or Unix epoch).
    async fn newest_l0_dt(&self, manifest: &Manifest) -> Result<DateTime<Utc>, SlateDBError> {
        let l0_timestamps = if !manifest.core.l0.is_empty() {
            // Use active L0's if some exist
            manifest
                .core
                .l0
                .iter()
                .map(|sst| DateTime::<Utc>::from(sst.id.unwrap_compacted_id().datetime()))
                .collect::<Vec<_>>()
        } else if let Some(l0_last_compacted) = manifest.core.l0_last_compacted {
            // Else fall back to the last compacted L0, which can serve as a conservative barrier
            vec![DateTime::<Utc>::from(l0_last_compacted.datetime())]
        } else {
            // If there has never been an L0, don't allow garbage collection to delete anything
            vec![DateTime::<Utc>::UNIX_EPOCH]
        };
        let max_l0_ts = l0_timestamps
            .into_iter()
            .max()
            .expect("expected at least unix epoch");
        Ok(max_l0_ts)
    }

    /// Returns the minimum starting timestamp of:
    ///
    /// 1. All on-going compactions, and
    /// 2. the most recently completed compaction.
    ///
    /// This represents the boundary up to which the garbage collector can delete SSTs. Any SST
    /// with a timestamp less than this value is the result of a complete compaction and therefore
    /// eligible for garbage collection.
    ///
    /// The Unix epoch is returned if any of the following occur:
    ///
    /// 1. There is no compactions file
    /// 2. The compactions file exists but there are no compactions
    /// 3. There is an error reading the compactions file
    ///
    /// (1) should only occur on fresh Dbs.
    /// (2) should only occur if the Db has never run a compaction (including previous instances).
    /// (3) should only occur if there are object store faults.
    ///
    /// In all of these cases, we want to be conservative and avoid deleting any SSTs that
    /// might be in use by a running compaction, so we return the Unix epoch to effectively
    /// disable deletion based on compaction state.
    fn compaction_low_watermark_dt(compactions: &Option<(u64, Compactions)>) -> DateTime<Utc> {
        match compactions {
            Some((_, compactions)) => compactions
                .iter()
                .map(|c| DateTime::<Utc>::from(c.id().datetime()))
                .min()
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            None => DateTime::<Utc>::UNIX_EPOCH,
        }
    }
}

impl GcTask for CompactedGcTask {
    /// Collect garbage from the compacted SSTs. This will delete any compacted SSTs that are
    /// older than the minimum age specified in the options and are not active in the manifest.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        // Don't delete any SSTs that are more recent than the oldest actively running compaction job
        // since they might be an output SST from a compaction that hasn't yet been added to the
        // manifest (we write the sorted run SSTs, _then_ add them to the manifest and write the
        // manifest to object storage).
        //
        // WARN: This must happen **before** the active manifests are read. Otherwise, we could see
        // the manifest before a compaction job finishes (none of its output SSTs are in the
        // manifest) and the compaction low watermark _after_ the SSTs are added to the manifest.
        // This would allow the GC to delete the latest compaction job output SST since they would
        // not be active, and would be older than the low watermark.
        let state_reader = CompactorStateReader::new(&self.manifest_store, &self.compactions_store);
        let view = state_reader.read_view().await?;
        let compactions = view.compactions;
        let (manifest_id, manifest) = view.manifest;
        let compaction_low_watermark_dt = Self::compaction_low_watermark_dt(&compactions);
        let active_ssts = self
            .list_active_l0_and_compacted_ssts(manifest_id, &manifest)
            .await?;
        // Don't delete any SSTs that are newer than the configured minimum age.
        let configured_min_age_dt = utc_now - self.compacted_sst_min_age();
        // Don't delete SSTs that are newer than this SST since they're probably an L0 that hasn't yet
        // been added to the manifest (we write the L0, _then_ add it to the manifest and write the
        // manifest to object storage).
        let newest_l0_dt = self.newest_l0_dt(&manifest).await?;
        // Take the minimum of the configured min age, the compaction low watermark, and the most
        // recent SST in the manifest. This is the true upper-limit for SSTs that may be deleted.
        let cutoff_dt = configured_min_age_dt
            .min(compaction_low_watermark_dt)
            .min(newest_l0_dt);
        log::debug!(
            "calculated compacted SST GC cutoff [cutoff_dt={:?}, configured_min_age_dt={:?}, compaction_low_watermark_dt={:?}, most_recent_sst_dt={:?}]",
            cutoff_dt,
            configured_min_age_dt,
            compaction_low_watermark_dt,
            newest_l0_dt,
        );
        let sst_ids_to_delete = self
            .table_store
            // List all SSTs in the table store
            .list_compacted_ssts(..)
            .await?
            .into_iter()
            .map(|sst| sst.id)
            // Filter out SSTs that were more recently created than the cutoff_dt
            .filter(|id| DateTime::<Utc>::from(id.unwrap_compacted_id().datetime()) < cutoff_dt)
            // Filter out SSTs that are active in the manifest (including actively checkpointed SSTs)
            .filter(|id| !active_ssts.contains(id))
            .collect::<Vec<_>>();

        for id in sst_ids_to_delete {
            log::info!("deleting SST [id={:?}]", id);
            if let Err(e) = self.table_store.delete_sst(&id).await {
                error!("error deleting SST [id={:?}, error={}]", id, e);
            } else {
                self.stats.gc_compacted_count.inc();
            }
        }

        Ok(())
    }

    fn resource(&self) -> &str {
        "Compacted SSTs"
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::clock::DefaultSystemClock;
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::manifest::store::StoredManifest;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::stats::StatRegistry;
    use crate::test_utils::build_test_sst;
    use object_store::{memory::InMemory, path::Path};

    #[tokio::test]
    async fn test_compacted_gc_respects_min_age_cutoff() {
        // Object stores and table store
        let main_store = Arc::new(InMemory::new());
        let object_stores = ObjectStores::new(main_store.clone(), None);
        let format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            object_stores,
            format.clone(),
            Path::from("/root"),
            None,
        ));

        // Manifest store and initial manifest
        let manifest_store = Arc::new(ManifestStore::new(&Path::from("/root"), main_store.clone()));
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/root"),
            main_store.clone(),
        ));
        let mut stored_compactions = StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();

        // Set a compaction newer than id_within_min_age so that it doesn't affect the cutoff.
        let mut compactions_dirty = stored_compactions.prepare_dirty().unwrap();
        compactions_dirty.value.insert(Compaction::new(
            ulid::Ulid::from_parts(9_000, 0),
            CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
        ));
        stored_compactions.update(compactions_dirty).await.unwrap();

        // Three SSTs with distinct ULID timestamps
        let id_to_delete = SsTableId::Compacted(ulid::Ulid::from_parts(1_000, 0));
        let id_within_min_age = SsTableId::Compacted(ulid::Ulid::from_parts(7_000, 0));
        let id_active_recent = SsTableId::Compacted(ulid::Ulid::from_parts(8_000, 0));

        let sst_to_delete = build_test_sst(&format, 1);
        let sst_within_min_age = build_test_sst(&format, 1);
        let sst_active_recent = build_test_sst(&format, 1);

        table_store
            .write_sst(&id_to_delete, sst_to_delete, false)
            .await
            .unwrap();
        table_store
            .write_sst(&id_within_min_age, sst_within_min_age, false)
            .await
            .unwrap();
        let active_handle = table_store
            .write_sst(&id_active_recent, sst_active_recent, false)
            .await
            .unwrap();

        // Mark one SST as active in the manifest so that most_recent_sst_dt
        // is newer than the configured minimum-age cutoff.
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.l0.push_back(active_handle);
        stored_manifest.update(dirty).await.unwrap();

        let stat_registry = Arc::new(StatRegistry::new());

        // GC task with min_age = 5 seconds. Using utc_now at 10 seconds after the epoch
        // yields a configured_min_age_dt of 5 seconds.
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(5),
        });
        let stats = Arc::new(GcStats::new(stat_registry.clone()));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            compactions_store.clone(),
            table_store.clone(),
            stats,
            opts,
        );

        let utc_now = DateTime::<Utc>::from_timestamp_millis(10_000).unwrap();

        // Run GC and verify only the old, inactive SST is collected
        task.collect(utc_now).await.unwrap();
        let remaining: Vec<_> = table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect();

        assert_eq!(remaining, vec![id_within_min_age, id_active_recent]);
    }

    #[tokio::test]
    async fn test_compacted_gc_respects_manifest_most_recent_sst() {
        // Object stores and table store
        let main_store = Arc::new(InMemory::new());
        let object_stores = ObjectStores::new(main_store.clone(), None);
        let format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            object_stores,
            format.clone(),
            Path::from("/root"),
            None,
        ));

        // Manifest store and initial manifest
        let manifest_store = Arc::new(ManifestStore::new(&Path::from("/root"), main_store.clone()));
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/root"),
            main_store.clone(),
        ));
        let mut stored_compactions = StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();

        // Set a compaction newer than id_newer so that it doesn't affect the cutoff.
        let mut compactions_dirty = stored_compactions.prepare_dirty().unwrap();
        compactions_dirty.value.insert(Compaction::new(
            ulid::Ulid::from_parts(5_000, 0),
            CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
        ));
        stored_compactions.update(compactions_dirty).await.unwrap();

        // Three SSTs with distinct ULID timestamps
        let id_to_delete = SsTableId::Compacted(ulid::Ulid::from_parts(1_000, 0));
        let id_manifest = SsTableId::Compacted(ulid::Ulid::from_parts(3_000, 0));
        let id_newer = SsTableId::Compacted(ulid::Ulid::from_parts(4_000, 0));

        let sst_to_delete = build_test_sst(&format, 1);
        let sst_manifest = build_test_sst(&format, 1);
        let sst_newer = build_test_sst(&format, 1);

        table_store
            .write_sst(&id_to_delete, sst_to_delete, false)
            .await
            .unwrap();
        let manifest_handle = table_store
            .write_sst(&id_manifest, sst_manifest, false)
            .await
            .unwrap();
        table_store
            .write_sst(&id_newer, sst_newer, false)
            .await
            .unwrap();

        // Mark id_manifest as the only active SST in the manifest so that
        // most_recent_sst_dt is 3_000ms, which becomes the cutoff.
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.l0.push_back(manifest_handle);
        stored_manifest.update(dirty).await.unwrap();

        let stat_registry = Arc::new(StatRegistry::new());

        // min_age = 0, so configured_min_age_dt == utc_now (10 seconds after epoch).
        // The manifest's most recent SST (3 seconds) is the smallest cutoff, so only
        // SSTs older than that should be deleted.
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(0),
        });
        let stats = Arc::new(GcStats::new(stat_registry.clone()));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            compactions_store.clone(),
            table_store.clone(),
            stats,
            opts,
        );

        let utc_now = DateTime::<Utc>::from_timestamp_millis(10_000).unwrap();

        task.collect(utc_now).await.unwrap();
        let remaining: Vec<_> = table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect();

        // Only the manifest SST and newer SST should remain; the older,
        // inactive SST should be collected.
        assert_eq!(remaining, vec![id_manifest, id_newer]);
    }

    #[tokio::test]
    async fn test_compacted_gc_respects_compaction_barrier() {
        // Object stores and table store
        let main_store = Arc::new(InMemory::new());
        let object_stores = ObjectStores::new(main_store.clone(), None);
        let format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            object_stores,
            format.clone(),
            Path::from("/root"),
            None,
        ));

        // Manifest store with empty DB
        let manifest_store = Arc::new(ManifestStore::new(&Path::from("/root"), main_store.clone()));
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/root"),
            main_store.clone(),
        ));
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let compactor_epoch = stored_manifest.manifest().compactor_epoch;

        // Three SSTs with distinct ULID timestamps
        let id_to_delete = SsTableId::Compacted(ulid::Ulid::from_parts(1_000, 0)); // job 1
        let id_barrier = SsTableId::Compacted(ulid::Ulid::from_parts(2_000, 0)); // job 2
        let id_to_newer = SsTableId::Compacted(ulid::Ulid::from_parts(3_000, 0)); // job 2, too
        let sst_to_delete = build_test_sst(&format, 1);
        let sst_barrier = build_test_sst(&format, 1);
        let sst_to_newer = build_test_sst(&format, 1);
        table_store
            .write_sst(&id_to_delete, sst_to_delete, false)
            .await
            .unwrap();
        table_store
            .write_sst(&id_barrier, sst_barrier, false)
            .await
            .unwrap();
        let active_handle = table_store
            .write_sst(&id_to_newer, sst_to_newer, false)
            .await
            .unwrap();

        // Mark the newest SST active in the manifest so that the
        // most_recent_sst_dt boundary is 3_000ms and the compaction
        // low watermark (2_000ms) becomes the effective cutoff (see below).
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.l0.push_back(active_handle);
        stored_manifest.update(dirty).await.unwrap();

        // Persist a running compaction with a start time at 2_000ms to act as the GC barrier.
        let mut stored_compactions =
            StoredCompactions::create(compactions_store.clone(), compactor_epoch)
                .await
                .unwrap();
        let mut compactions_dirty = stored_compactions.prepare_dirty().unwrap();
        compactions_dirty.value.insert(Compaction::new(
            ulid::Ulid::from_parts(2_000, 0),
            CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
        ));
        stored_compactions.update(compactions_dirty).await.unwrap();

        // GC task with min_age = 0
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(0),
        });
        let stat_registry = Arc::new(StatRegistry::new());
        let stats = Arc::new(GcStats::new(stat_registry.clone()));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            compactions_store.clone(),
            table_store.clone(),
            stats,
            opts,
        );

        // Run GC at a fixed time and verify only the SST strictly
        // older than the compaction barrier is collected.
        let utc_now = DateTime::<Utc>::from_timestamp_millis(10_000).unwrap();
        task.collect(utc_now).await.unwrap();
        let remaining: Vec<_> = table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect();

        // Only the barrier and newer SSTs should remain
        assert_eq!(remaining, vec![id_barrier, id_to_newer]);
    }

    /// Reproduces the race where GC reads an empty compaction state and deletes the
    /// output of a compaction that starts afterward but hasn't yet updated the manifest.
    #[tokio::test]
    async fn test_compacted_gc_skips_running_compaction_output_without_watermark() {
        let main_store = Arc::new(InMemory::new());
        let object_stores = ObjectStores::new(main_store.clone(), None);
        let format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            object_stores,
            format.clone(),
            Path::from("/root"),
            None,
        ));

        // Manifest with an L0 newer than the compaction output.
        let manifest_store = Arc::new(ManifestStore::new(&Path::from("/root"), main_store.clone()));
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/root"),
            main_store.clone(),
        ));
        // Persist an empty compactions file so GC sees no active compactions.
        StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();

        // Newest L0 in the manifest has a later timestamp (9_000ms).
        let l0_id = SsTableId::Compacted(ulid::Ulid::from_parts(9_000, 0));
        let l0_handle = table_store
            .write_sst(&l0_id, build_test_sst(&format, 1), false)
            .await
            .unwrap();
        let mut dirty_manifest = stored_manifest.prepare_dirty().unwrap();
        dirty_manifest.value.core.l0.push_back(l0_handle);
        stored_manifest.update(dirty_manifest).await.unwrap();

        // Simulate a compaction that starts after GC reads compaction state, writes an
        // output SST (6_000ms), but hasn't updated the manifest yet.
        let compaction_output_id = SsTableId::Compacted(ulid::Ulid::from_parts(6_000, 0));
        table_store
            .write_sst(&compaction_output_id, build_test_sst(&format, 1), false)
            .await
            .unwrap();

        // With min_age=2s and newest_l0=9s, the cutoff becomes 8s; without a watermark
        // this incorrectly allows deleting the compaction output.
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(2),
        });
        let stats = Arc::new(GcStats::new(Arc::new(StatRegistry::new())));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            compactions_store.clone(),
            table_store.clone(),
            stats,
            opts,
        );

        let utc_now = DateTime::<Utc>::from_timestamp_millis(10_000).unwrap();
        task.collect(utc_now).await.unwrap();

        let remaining: Vec<_> = table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect();

        assert!(
            remaining.contains(&compaction_output_id),
            "expected GC to retain compacted SST output from a running compaction when the watermark is missing"
        );
    }
}
