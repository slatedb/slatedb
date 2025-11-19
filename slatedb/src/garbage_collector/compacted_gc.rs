use crate::manifest::Manifest;
use crate::{
    config::GarbageCollectorDirectoryOptions, db_state::SsTableId, error::SlateDBError,
    manifest::store::ManifestStore, stats::StatRegistry, tablestore::TableStore,
};
use chrono::{DateTime, Utc};
use log::error;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use super::{GcStats, GcTask, DEFAULT_MIN_AGE};
use crate::compactor::stats::COMPACTION_LOW_WATERMARK_TS;

pub(crate) struct CompactedGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    compacted_options: Option<GarbageCollectorDirectoryOptions>,
    stat_registry: Arc<StatRegistry>,
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
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        compacted_options: Option<GarbageCollectorDirectoryOptions>,
        stat_registry: Arc<StatRegistry>,
    ) -> Self {
        CompactedGcTask {
            manifest_store,
            table_store,
            stats,
            compacted_options,
            stat_registry,
        }
    }

    fn compacted_sst_min_age(&self) -> chrono::Duration {
        let min_age = self
            .compacted_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
    }

    async fn list_active_l0_and_compacted_ssts(
        &self,
        active_manifests: &BTreeMap<u64, Manifest>,
    ) -> Result<HashSet<SsTableId>, SlateDBError> {
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

    async fn newest_l0_dt(
        &self,
        active_manifests: &BTreeMap<u64, Manifest>,
    ) -> Result<DateTime<Utc>, SlateDBError> {
        let manifest = active_manifests
            .values()
            .last()
            .expect("expected at least one manifest");
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

    /// Returns a `DateTime<Utc>` barrier based on the compactor's oldest running compaction start.
    ///
    /// When compactions are active (compactor/running_compactions > 0), we read
    /// compactor/compaction_low_watermark_ts and convert it into a
    /// `DateTime<Utc>` value. GC should not delete any compacted SST whose ULID timestamp
    /// is greater than or equal to this barrier time.
    ///
    /// This is a process-local coordination mechanism that only works when the compactor
    /// and garbage collector run in the same process and share the same StatRegistry. It's
    /// a hack until we have proper compactor persistence (so GC can retrieve the compactor
    /// state from the object store). See #604 for details.
    fn compaction_low_watermark_dt(&self) -> DateTime<Utc> {
        let low_watermark_ts = self
            .stat_registry
            .lookup(COMPACTION_LOW_WATERMARK_TS)
            // Convert s.get() from u64 ro DateTime::<Utc>
            .map(|s| s.get())
            // If no compaction has ever run since the start of this process, then set
            // `compaction_low_watermark_ts` to 0 (UNIX_EPOCH) to prevent the GC from deleting
            // _anything_. Once a job kicks in, the low watermark will always be set, and will
            // only increase as new jobs start and old ones finish.
            .unwrap_or(0);
        DateTime::<Utc>::from_timestamp_millis(low_watermark_ts).expect("out of bounds timestamp")
    }
}

impl GcTask for CompactedGcTask {
    /// Collect garbage from the compacted SSTs. This will delete any compacted SSTs that are
    /// older than the minimum age specified in the options and are not active in the manifest.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let active_manifests = self.manifest_store.read_active_manifests().await?;
        // Don't delete any SSTs that are more recent than the oldest actively running compaction job
        // since they might be an output SST from a compaction that hasn't yet been added to the
        // manifest (we write the sorted run SSTs, _then_ add them to the manifest and write the
        // manifest to object storage).
        let compaction_low_watermark_dt = self.compaction_low_watermark_dt();
        let active_ssts = self
            .list_active_l0_and_compacted_ssts(&active_manifests)
            .await?;
        // Don't delete any SSTs that are newer than the configured minimum age.
        let configured_min_age_dt = utc_now - self.compacted_sst_min_age();
        // Don't delete SSTs that are newer than this SST since they're probably an L0 that hasn't yet
        // been added to the manifest (we write the L0, _then_ add it to the manifest and write the
        // manifest to object storage).
        let most_recent_sst_dt = self.newest_l0_dt(&active_manifests).await?;
        // Take the minimum of the configured min age, the compaction low watermark, and the most
        // recent SST in the manifest. This is the true upper-limit for SSTs that may be deleted.
        let cutoff_dt = configured_min_age_dt
            .min(compaction_low_watermark_dt)
            .min(most_recent_sst_dt);
        log::debug!(
            "calculated compacted SST GC cutoff [cutoff_dt={:?}, configured_min_age_dt={:?}, compaction_low_watermark_dt={:?}, most_recent_sst_dt={:?}]",
            cutoff_dt,
            configured_min_age_dt,
            compaction_low_watermark_dt,
            most_recent_sst_dt,
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
    use crate::compactor_stats::RUNNING_COMPACTIONS;
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::manifest::store::StoredManifest;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::stats::Gauge;
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
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from("/root"),
            main_store.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
                .await
                .unwrap();

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
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.l0.push_back(active_handle);
        stored_manifest.update_manifest(dirty).await.unwrap();

        // Register barrier metrics to be more recent than id_active_recent so it doesn't get in the way
        let stat_registry = Arc::new(StatRegistry::new());
        let running = Arc::new(Gauge::<i64>::default());
        running.set(1);
        stat_registry.register(RUNNING_COMPACTIONS, running);
        let barrier = Arc::new(Gauge::<u64>::default());
        barrier.set(10_000);
        stat_registry.register(COMPACTION_LOW_WATERMARK_TS, barrier);

        // GC task with min_age = 5 seconds. Using utc_now at 10 seconds after the epoch
        // yields a configured_min_age_dt of 5 seconds.
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(5),
        });
        let stats = Arc::new(GcStats::new(stat_registry.clone()));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            table_store.clone(),
            stats,
            opts,
            stat_registry.clone(),
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
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from("/root"),
            main_store.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
                .await
                .unwrap();

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
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.l0.push_back(manifest_handle);
        stored_manifest.update_manifest(dirty).await.unwrap();

        // Register barrier metric to be more recent than id_newer so it doesn't get in the way
        let stat_registry = Arc::new(StatRegistry::new());
        let running = Arc::new(Gauge::<i64>::default());
        running.set(1);
        stat_registry.register(RUNNING_COMPACTIONS, running);
        let barrier = Arc::new(Gauge::<u64>::default());
        barrier.set(10_000);
        stat_registry.register(COMPACTION_LOW_WATERMARK_TS, barrier);

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
            table_store.clone(),
            stats,
            opts,
            stat_registry.clone(),
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
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from("/root"),
            main_store.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let mut stored_manifest =
            StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
                .await
                .unwrap();

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
        let mut dirty = stored_manifest.prepare_dirty();
        dirty.core.l0.push_back(active_handle);
        stored_manifest.update_manifest(dirty).await.unwrap();

        // Register barrier metrics
        let stat_registry = Arc::new(StatRegistry::new());
        let running = Arc::new(Gauge::<i64>::default());
        running.set(1);
        stat_registry.register(RUNNING_COMPACTIONS, running);
        let barrier = Arc::new(Gauge::<u64>::default());
        barrier.set(2_000);
        stat_registry.register(COMPACTION_LOW_WATERMARK_TS, barrier);

        // GC task with min_age = 0
        let opts = Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::from_secs(0),
        });
        let stats = Arc::new(GcStats::new(stat_registry.clone()));
        let task = CompactedGcTask::new(
            manifest_store.clone(),
            table_store.clone(),
            stats,
            opts,
            stat_registry.clone(),
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
}
