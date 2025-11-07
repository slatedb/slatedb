use crate::{
    config::GarbageCollectorDirectoryOptions, db_state::SsTableId, error::SlateDBError,
    manifest::store::ManifestStore, stats::StatRegistry, tablestore::TableStore,
};
use chrono::{DateTime, Utc};
use log::error;
use std::collections::HashSet;
use std::sync::Arc;

use super::{GcStats, GcTask, DEFAULT_MIN_AGE};
use crate::compactor::stats::{COMPACTION_LOW_WATERMARK_TS, RUNNING_COMPACTIONS};

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
    fn compaction_low_watermark_dt(&self) -> Option<DateTime<Utc>> {
        let running = self
            .stat_registry
            .lookup(RUNNING_COMPACTIONS)
            .map(|s| s.get())
            .unwrap_or(0);
        if running <= 0 {
            return None;
        }
        let start_ts_ms = self
            .stat_registry
            .lookup(COMPACTION_LOW_WATERMARK_TS)
            .map(|s| s.get())
            .unwrap_or(0);
        if start_ts_ms <= 0 {
            return None;
        }
        DateTime::<Utc>::from_timestamp_millis(start_ts_ms as i64)
    }
}

impl GcTask for CompactedGcTask {
    /// Collect garbage from the compacted SSTs. This will delete any compacted SSTs that are
    /// older than the minimum age specified in the options and are not active in the manifest.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let active_ssts = self.list_active_l0_and_compacted_ssts().await?;
        let maybe_compaction_low_watermark_dt = self.compaction_low_watermark_dt();
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
            // Filter out the ones that are part of a running compaction (See #604 for details)
            .filter(|id| match (maybe_compaction_low_watermark_dt, id) {
                (Some(compaction_low_watermark_dt), SsTableId::Compacted(ulid)) => {
                    let ulid_dt = DateTime::<Utc>::from(ulid.datetime());
                    ulid_dt < compaction_low_watermark_dt
                }
                // Keep all compacted SSTs if there is no compaction low watermark
                (None, SsTableId::Compacted(_)) => true,
                _ => unreachable!("non-compacted SSTs should not be in list_compacted_ssts"),
            })
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
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::manifest::store::StoredManifest;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::stats::Gauge;
    use crate::test_utils::build_test_sst;
    use object_store::{memory::InMemory, path::Path};

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
        let _sm = StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new())
            .await
            .unwrap();

        // Two SSTs with distinct ULID timestamps
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
        table_store
            .write_sst(&id_to_newer, sst_to_newer, false)
            .await
            .unwrap();

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

        // Run GC and verify only old is collected
        task.collect(Utc::now()).await.unwrap();
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
