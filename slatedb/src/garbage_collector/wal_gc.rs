use crate::manifest::Manifest;
use crate::tablestore::SstFileMetadata;
use crate::{
    config::GarbageCollectorDirectoryOptions, manifest::store::ManifestStore,
    tablestore::TableStore, SlateDBError,
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Interval;
use tracing::error;

use super::{GcStats, GcTask, DEFAULT_INTERVAL, DEFAULT_MIN_AGE};

pub(crate) struct WalGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    wal_options: Option<GarbageCollectorDirectoryOptions>,
}

impl std::fmt::Debug for WalGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalGcTask")
            .field("wal_options", &self.wal_options)
            .finish()
    }
}

impl WalGcTask {
    pub fn new(
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

        let last_compacted_wal_sst_id = latest_manifest.core.replay_after_wal_id;
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

    fn period(&self) -> Duration {
        self.wal_options
            .and_then(|opts| opts.interval)
            .unwrap_or(DEFAULT_INTERVAL)
    }

    fn resource(&self) -> &str {
        "WAL"
    }
}
