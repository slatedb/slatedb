use crate::{
    config::GarbageCollectorDirectoryOptions, error::SlateDBError, manifest::store::ManifestStore,
};
use chrono::{DateTime, Utc};
use log::error;
use std::collections::HashSet;
use std::sync::Arc;

use super::{GcStats, GcTask};

pub(crate) struct ManifestGcTask {
    manifest_store: Arc<ManifestStore>,
    stats: Arc<GcStats>,
    manifest_options: GarbageCollectorDirectoryOptions,
}

impl std::fmt::Debug for ManifestGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestGcTask")
            .field("manifest_options", &self.manifest_options)
            .finish()
    }
}

impl ManifestGcTask {
    pub(super) fn new(
        manifest_store: Arc<ManifestStore>,
        stats: Arc<GcStats>,
        manifest_options: GarbageCollectorDirectoryOptions,
    ) -> Self {
        ManifestGcTask {
            manifest_store,
            stats,
            manifest_options,
        }
    }

    fn manifest_min_age(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.manifest_options.min_age).expect("invalid duration")
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
            return Err(SlateDBError::LatestTransactionalObjectVersionMissing);
        };

        // Do not delete manifests which are still referenced by active checkpoints
        let active_manifest_ids: HashSet<_> = latest_manifest
            .core
            .checkpoints
            .iter()
            .map(|checkpoint| checkpoint.manifest_id)
            .collect();

        // Advance the boundary to the latest manifest that is older than min_age
        if let Some(boundary) = manifest_metadata_list
            .iter()
            .filter(|manifest_metadata| {
                utc_now.signed_duration_since(manifest_metadata.last_modified) > min_age
            })
            .map(|manifest_metadata| manifest_metadata.id)
            .max()
        {
            self.manifest_store.advance_boundary(boundary).await?;
        }

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
                    error!(
                        "error deleting manifest [id={:?}, error={}]",
                        manifest_metadata.id, e
                    );
                } else {
                    self.stats.gc_manifest_count.increment(1);
                }
            }
        }

        Ok(())
    }

    fn resource(&self) -> &str {
        "Manifest"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        store::{ManifestStore, StoredManifest},
        ManifestCore,
    };
    use chrono::TimeDelta;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::MetricsRecorderHelper;
    use std::time::Duration;

    #[tokio::test]
    async fn test_collect_advances_boundary_for_old_manifest_files() {
        let object_store = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from("/root"),
            object_store.clone(),
        ));
        let mut stored_manifest = StoredManifest::create_new_db(
            manifest_store.clone(),
            ManifestCore::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        stored_manifest
            .update(stored_manifest.prepare_dirty().unwrap())
            .await
            .unwrap();
        stored_manifest
            .update(stored_manifest.prepare_dirty().unwrap())
            .await
            .unwrap();

        let recorder = MetricsRecorderHelper::noop();
        let task = ManifestGcTask::new(
            manifest_store.clone(),
            Arc::new(GcStats::new(&recorder)),
            GarbageCollectorDirectoryOptions {
                min_age: Duration::from_secs(1),
                interval: None,
            },
        );
        task.collect(Utc::now() + TimeDelta::hours(1))
            .await
            .unwrap();

        let raw_boundary = object_store
            .get(&Path::from("/root/gc/manifest.boundary"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("2", std::str::from_utf8(&raw_boundary).unwrap());

        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(
            vec![3],
            manifests
                .iter()
                .map(|manifest| manifest.id)
                .collect::<Vec<_>>()
        );
    }
}
