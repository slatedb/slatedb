use crate::{
    config::GarbageCollectorDirectoryOptions, manifest::store::ManifestStore, SlateDBError,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Interval;
use tracing::error;

use super::{GcStats, GcTask, DEFAULT_INTERVAL, DEFAULT_MIN_AGE};

pub(crate) struct ManifestGcTask {
    manifest_store: Arc<ManifestStore>,
    stats: Arc<GcStats>,
    manifest_options: Option<GarbageCollectorDirectoryOptions>,
}

impl ManifestGcTask {
    pub fn new(
        manifest_store: Arc<ManifestStore>,
        stats: Arc<GcStats>,
        manifest_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        ManifestGcTask {
            manifest_store,
            stats,
            manifest_options,
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
    async fn collect(&self, now_in_millis: i64) -> Result<(), SlateDBError> {
        let utc_now = super::gc_task_time(now_in_millis);
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

    fn interval(&self) -> Duration {
        self.manifest_options
            .and_then(|opts| opts.interval)
            .unwrap_or(DEFAULT_INTERVAL)
    }

    fn ticker(&self) -> Interval {
        tokio::time::interval(self.interval())
    }

    fn resource(&self) -> &str {
        "Manifest"
    }
}
