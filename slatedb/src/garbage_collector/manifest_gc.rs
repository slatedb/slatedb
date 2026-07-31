use crate::{
    config::GarbageCollectorDirectoryOptions, error::SlateDBError, manifest::store::ManifestStore,
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::error;
use std::collections::HashSet;
use std::sync::Arc;

use super::filter::retain_allowed_by_gc_filter;
use super::{GcFilter, GcStats, GcTask, GC_DELETE_CONCURRENCY};

#[derive(Clone)]
pub(crate) struct ManifestGcTask {
    manifest_store: Arc<ManifestStore>,
    stats: Arc<GcStats>,
    manifest_options: GarbageCollectorDirectoryOptions,
    gc_filter: Option<Arc<dyn GcFilter>>,
    boundary_files_enabled: bool,
}

impl std::fmt::Debug for ManifestGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestGcTask")
            .field("manifest_options", &self.manifest_options)
            .field("boundary_files_enabled", &self.boundary_files_enabled)
            .finish()
    }
}

impl ManifestGcTask {
    pub(super) fn new(
        manifest_store: Arc<ManifestStore>,
        stats: Arc<GcStats>,
        manifest_options: GarbageCollectorDirectoryOptions,
        gc_filter: Option<Arc<dyn GcFilter>>,
        boundary_files_enabled: bool,
    ) -> Self {
        ManifestGcTask {
            manifest_store,
            stats,
            manifest_options,
            gc_filter,
            boundary_files_enabled,
        }
    }

    fn manifest_min_age(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.manifest_options.min_age).expect("invalid duration")
    }

    /// Deletes the given manifests from the manifest store.
    ///
    /// In case of dryrun, the actual deletion doesn't happen.
    async fn maybe_delete_manifests(&self, manifest_ids: Vec<u64>) {
        if self.manifest_options.dry_run {
            if !manifest_ids.is_empty() {
                log::info!(
                    "dry run: skipping manifest deletion [count={}]",
                    manifest_ids.len()
                );
            }
            for id in manifest_ids {
                log::debug!("dry run: would delete manifest but skipped [id={:?}]", id);
            }
            return;
        }

        futures::stream::iter(manifest_ids)
            .for_each_concurrent(GC_DELETE_CONCURRENCY, |id| async move {
                if let Err(e) = self.manifest_store.delete_manifest_unchecked(id).await {
                    error!("error deleting manifest [id={:?}, error={}]", id, e);
                } else {
                    self.stats.gc_manifest_count.increment(1);
                }
            })
            .await;
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

        // Delete manifests older than min_age
        let manifests_to_delete = manifest_metadata_list
            .into_iter()
            .filter(|manifest_metadata| {
                let is_active = active_manifest_ids.contains(&manifest_metadata.id);
                !is_active
                    && utc_now.signed_duration_since(manifest_metadata.metadata.last_modified)
                        > min_age
            })
            .collect::<Vec<_>>();

        // Advance the boundary to the latest manifest selected by the GC model. The optional GC
        // filter only gates the final deletion pass.
        if self.boundary_files_enabled {
            if let Some(boundary) = manifests_to_delete
                .iter()
                .map(|manifest_metadata| manifest_metadata.id)
                .max()
            {
                self.manifest_store.advance_boundary(boundary).await?;
            }
        }
        let manifests_to_delete =
            retain_allowed_by_gc_filter(&self.gc_filter, manifests_to_delete).await;
        let manifest_ids_to_delete = manifests_to_delete
            .into_iter()
            .map(|manifest_metadata| manifest_metadata.id)
            .collect::<Vec<_>>();

        self.maybe_delete_manifests(manifest_ids_to_delete).await;

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
    use async_trait::async_trait;
    use chrono::TimeDelta;
    use object_store::{memory::InMemory, path::Path, ObjectStoreExt};
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::MetricsRecorderHelper;
    use slatedb_common::ObjectMetadata;
    use std::time::Duration;

    struct DenyAllGcFilter;

    #[async_trait]
    impl GcFilter for DenyAllGcFilter {
        async fn filter(&self, _candidates: HashSet<ObjectMetadata>) -> HashSet<ObjectMetadata> {
            HashSet::new()
        }
    }

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
                dry_run: false,
            },
            None,
            true,
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

    #[tokio::test]
    async fn test_collect_without_boundary_advancement_deletes_without_creating_boundary() {
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
                dry_run: false,
            },
            None,
            false,
        );
        task.collect(Utc::now() + TimeDelta::hours(1))
            .await
            .unwrap();

        assert!(matches!(
            object_store
                .get(&Path::from("/root/gc/manifest.boundary"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        let manifests = manifest_store.list_manifests(..).await.unwrap();
        assert_eq!(
            vec![3],
            manifests
                .iter()
                .map(|manifest| manifest.id)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_collect_advances_boundary_before_filtering_manifest_files() {
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
                dry_run: false,
            },
            Some(Arc::new(DenyAllGcFilter) as Arc<dyn GcFilter>),
            true,
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

        assert!(manifest_store.try_read_manifest(1).await.unwrap().is_some());
        assert!(manifest_store.try_read_manifest(2).await.unwrap().is_some());
    }
}
