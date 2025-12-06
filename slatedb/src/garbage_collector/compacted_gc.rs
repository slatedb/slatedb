use crate::{
    compactions_store::CompactionsStore, config::GarbageCollectorDirectoryOptions,
    db_state::SsTableId, error::SlateDBError, manifest::store::ManifestStore, manifest::Manifest,
    tablestore::TableStore,
};
use chrono::{DateTime, Utc};
use log::{error, warn};
use std::collections::BTreeMap;
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

enum PersistedCompactionLowWatermark {
    Missing,
    Empty,
    Active(DateTime<Utc>),
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

    /// Attempts to load the oldest active compaction start time from the persisted
    /// compactions state in object storage.
    async fn persisted_compaction_low_watermark_dt(
        &self,
    ) -> Result<PersistedCompactionLowWatermark, SlateDBError> {
        let Some((_, compactions)) = self.compactions_store.try_read_latest_compactions().await?
        else {
            return Ok(PersistedCompactionLowWatermark::Missing);
        };

        let watermark = compactions
            .iter()
            .map(|c| DateTime::<Utc>::from(c.id().datetime()))
            .min();

        Ok(match watermark {
            Some(dt) => PersistedCompactionLowWatermark::Active(dt),
            None => PersistedCompactionLowWatermark::Empty,
        })
    }

    /// Returns the oldest active compaction start time from persisted state. If the
    /// compactions file is missing or cannot be read, default to the Unix epoch to
    /// prevent unsafe deletions.
    async fn compaction_low_watermark_dt(&self) -> Option<DateTime<Utc>> {
        match self.persisted_compaction_low_watermark_dt().await {
            Ok(PersistedCompactionLowWatermark::Active(dt)) => Some(dt),
            Ok(PersistedCompactionLowWatermark::Empty) => None,
            Ok(PersistedCompactionLowWatermark::Missing) => {
                Some(DateTime::<Utc>::from_timestamp_millis(0).expect("out of bounds timestamp"))
            }
            Err(err) => {
                warn!(
                    "failed to read persisted compaction state for GC [error={}]",
                    err
                );
                Some(DateTime::<Utc>::from_timestamp_millis(0).expect("out of bounds timestamp"))
            }
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
        let compaction_low_watermark_dt = self.compaction_low_watermark_dt().await;
        let active_manifests = self.manifest_store.read_active_manifests().await?;
        let active_ssts = self
            .list_active_l0_and_compacted_ssts(&active_manifests)
            .await?;
        // Don't delete any SSTs that are newer than the configured minimum age.
        let configured_min_age_dt = utc_now - self.compacted_sst_min_age();
        // Don't delete SSTs that are newer than this SST since they're probably an L0 that hasn't yet
        // been added to the manifest (we write the L0, _then_ add it to the manifest and write the
        // manifest to object storage).
        let newest_l0_dt = self.newest_l0_dt(&active_manifests).await?;
        // Take the minimum of the configured min age, the compaction low watermark (if any), and the
        // most recent SST in the manifest. This is the true upper-limit for SSTs that may be deleted.
        let mut cutoff_dt = configured_min_age_dt.min(newest_l0_dt);
        if let Some(compaction_low_watermark_dt) = compaction_low_watermark_dt {
            cutoff_dt = cutoff_dt.min(compaction_low_watermark_dt);
        }
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
        StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
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
        StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
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
}
