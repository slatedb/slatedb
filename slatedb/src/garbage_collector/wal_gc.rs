use crate::manifest::Manifest;
use crate::{
    config::GarbageCollectorDirectoryOptions, db_state::SsTableId, error::SlateDBError,
    manifest::store::ManifestStore, tablestore::TableStore,
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::error;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::filter::retain_allowed_by_gc_filter;
use super::{GcFilter, GcStats, GcTask, GC_DELETE_CONCURRENCY};
use slatedb_common::object_metadata::IdentifiedObjectMetadata;

/// Selects which class of WAL object a [`WalGcTask`] collects.
///
/// Regular WAL SSTs and zero-byte WAL fence objects share the same WAL
/// directory and `SsTableId::Wal` identifier space, but they have separate
/// retention policies. This mode keeps a single task implementation while
/// allowing regular WAL GC and fence WAL GC to run on independent schedules.
#[derive(Debug, Clone, Copy)]
pub(super) enum WalGcMode {
    /// Collect non-empty WAL SSTs that are older than the compacted WAL
    /// boundary, old enough for retention, and unreferenced by active
    /// checkpoint manifests.
    Regular,

    /// Collect zero-byte WAL fence objects under the same safety checks as
    /// regular WAL GC.
    Fence,
}

#[derive(Clone)]
pub(crate) struct WalGcTask {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    wal_options: GarbageCollectorDirectoryOptions,
    mode: WalGcMode,
    gc_filter: Option<Arc<dyn GcFilter>>,
}

impl std::fmt::Debug for WalGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalGcTask")
            .field("wal_options", &self.wal_options)
            .field("mode", &self.mode)
            .finish()
    }
}

impl WalGcTask {
    pub(super) fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        wal_options: GarbageCollectorDirectoryOptions,
        mode: WalGcMode,
        gc_filter: Option<Arc<dyn GcFilter>>,
    ) -> Self {
        WalGcTask {
            manifest_store,
            table_store,
            stats,
            wal_options,
            mode,
            gc_filter,
        }
    }

    fn is_wal_sst_eligible_for_deletion(
        utc_now: &DateTime<Utc>,
        wal_sst: &IdentifiedObjectMetadata<SsTableId>,
        min_age: &chrono::Duration,
        active_manifests: &BTreeMap<u64, Manifest>,
    ) -> bool {
        if utc_now.signed_duration_since(wal_sst.metadata.last_modified) <= *min_age {
            return false;
        }

        let wal_sst_id = wal_sst.id.unwrap_wal_id();
        !active_manifests
            .values()
            .any(|manifest| manifest.has_wal_sst_reference(wal_sst_id))
    }

    fn wal_sst_min_age(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.wal_options.min_age).expect("invalid duration")
    }

    /// Deletes the given WAL SSTs from the table store.
    ///
    /// In case of dryrun, the actual deletion doesn't happen.
    async fn maybe_delete_wal_ssts(&self, sst_ids: Vec<SsTableId>) {
        if self.wal_options.dry_run {
            if !sst_ids.is_empty() {
                log::info!(
                    "dry run: skipping {} deletion [count={}]",
                    self.resource(),
                    sst_ids.len()
                );
                if matches!(self.mode, WalGcMode::Fence) {
                    log::info!(
                        "WAL fence GC is dry-run by default. This is a conservative setting. \
                        Set wal_fence_options.dry_run=false and use a conservative min_age to enable. \
                        Silence this log with wal_fence_options=None. See #352 for details."
                    );
                }
            }
            for id in sst_ids {
                log::debug!(
                    "dry run: would delete {} but skipped [id={:?}]",
                    self.resource(),
                    id
                );
            }
            return;
        }

        futures::stream::iter(sst_ids)
            .for_each_concurrent(GC_DELETE_CONCURRENCY, |id| async move {
                if let Err(e) = self.table_store.delete_sst(&id).await {
                    error!("error deleting WAL SST [id={:?}, error={}]", id, e);
                } else {
                    match self.mode {
                        WalGcMode::Regular => self.stats.gc_wal_count.increment(1),
                        WalGcMode::Fence => self.stats.gc_wal_fence_count.increment(1),
                    }
                }
            })
            .await;
    }
}

impl GcTask for WalGcTask {
    /// Collect garbage from the WAL SSTs. This will delete any WAL SSTs that meet
    /// the following conditions:
    ///  - not referenced by an active checkpoint
    ///  - older than the minimum age specified in the options
    ///  - older than the last compacted WAL SST.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let latest_manifest = self.manifest_store.read_latest_manifest().await?;
        let active_manifests = self
            .manifest_store
            .read_referenced_manifests(latest_manifest.id, &latest_manifest.manifest)
            .await?;
        let last_compacted_wal_sst_id = latest_manifest.manifest.core.replay_after_wal_id;
        let min_age = self.wal_sst_min_age();
        let ssts_to_delete = self
            .table_store
            .list_wal_ssts(..last_compacted_wal_sst_id)
            .await?
            .into_iter()
            .filter(|wal_sst| match self.mode {
                // In regular mode, only consider WAL SSTs with size > 0 for deletion.
                WalGcMode::Regular => wal_sst.metadata.size > 0,
                // In fence mode, only consider zero-byte WAL SSTs for deletion.
                WalGcMode::Fence => wal_sst.metadata.size == 0,
            })
            // Respect min_age and any WAL references held by active checkpoint manifests.
            .filter(|wal_sst| {
                Self::is_wal_sst_eligible_for_deletion(
                    &utc_now,
                    wal_sst,
                    &min_age,
                    &active_manifests,
                )
            })
            .collect::<Vec<_>>();
        let ssts_to_delete = retain_allowed_by_gc_filter(&self.gc_filter, ssts_to_delete).await;
        let sst_ids_to_delete = ssts_to_delete
            .into_iter()
            .map(|wal_sst| wal_sst.id)
            .collect::<Vec<_>>();

        self.maybe_delete_wal_ssts(sst_ids_to_delete).await;

        Ok(())
    }

    fn resource(&self) -> &str {
        match self.mode {
            WalGcMode::Regular => "WAL",
            WalGcMode::Fence => "WAL fence",
        }
    }
}
