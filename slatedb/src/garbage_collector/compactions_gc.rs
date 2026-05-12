//! Garbage collection for `.compactions` files.
//!
//! The compactions store is a versioned log of compactor state snapshots. GC can
//! safely delete older versions because only the latest record is required for:
//! - establishing the compaction low-watermark (to prevent deleting in-flight outputs),
//! - fencing semantics (epoch checks on the newest record).
//!
//! Policy:
//! - Always retain the most recent `.compactions` file.
//! - Only delete files older than the configured `min_age`.
//!
//! Safety:
//! - Deleting old versions does not affect recovery because the newest record
//!   contains the authoritative compactor epoch and retained compaction state.
//! - This task does not inspect compaction contents; it is purely time-based and
//!   version-aware (keeps the latest).
//!
//! Errors are logged and the task continues; stats are updated only on successful
//! deletes.

use crate::{
    compactions_store::CompactionsStore, config::GarbageCollectorDirectoryOptions,
    error::SlateDBError,
};
use chrono::{DateTime, Utc};
use log::error;
use std::sync::Arc;

use super::{GcStats, GcTask};

pub(crate) struct CompactionsGcTask {
    compactions_store: Arc<CompactionsStore>,
    stats: Arc<GcStats>,
    compactions_options: GarbageCollectorDirectoryOptions,
}

impl std::fmt::Debug for CompactionsGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionsGcTask")
            .field("compactions_options", &self.compactions_options)
            .finish()
    }
}

impl CompactionsGcTask {
    pub(super) fn new(
        compactions_store: Arc<CompactionsStore>,
        stats: Arc<GcStats>,
        compactions_options: GarbageCollectorDirectoryOptions,
    ) -> Self {
        Self {
            compactions_store,
            stats,
            compactions_options,
        }
    }

    fn compactions_min_age(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.compactions_options.min_age).expect("invalid duration")
    }
}

impl GcTask for CompactionsGcTask {
    /// Collect garbage from the compactions store. This will delete any compactions files
    /// that are older than the minimum age specified in the options, excluding the latest
    /// compactions file.
    async fn collect(&self, utc_now: DateTime<Utc>) -> Result<(), SlateDBError> {
        let min_age = self.compactions_min_age();
        let mut compactions_metadata_list = self.compactions_store.list_compactions(..).await?;

        // Remove the last element so we never delete the latest compactions file
        compactions_metadata_list.pop();

        // Advance the boundary to the latest compactions file that is older than min_age
        if let Some(boundary) = compactions_metadata_list
            .iter()
            .filter(|compactions_metadata| {
                utc_now.signed_duration_since(compactions_metadata.last_modified) > min_age
            })
            .map(|compactions_metadata| compactions_metadata.id)
            .max()
        {
            self.compactions_store.advance_boundary(boundary).await?;
        }

        // Delete compactions files older than min_age
        for compactions_metadata in compactions_metadata_list {
            if utc_now.signed_duration_since(compactions_metadata.last_modified) > min_age {
                if let Err(e) = self
                    .compactions_store
                    .delete_compactions(compactions_metadata.id)
                    .await
                {
                    error!(
                        "error deleting compactions [id={:?}, error={}]",
                        compactions_metadata.id, e
                    );
                } else {
                    self.stats.gc_compactions_count.increment(1);
                }
            }
        }

        Ok(())
    }

    fn resource(&self) -> &str {
        "Compactions"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use chrono::TimeDelta;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb_common::metrics::MetricsRecorderHelper;
    use std::time::Duration;

    #[tokio::test]
    async fn test_collect_advances_boundary_for_old_compactions_files() {
        let object_store = Arc::new(InMemory::new());
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/root"),
            object_store.clone(),
        ));
        let mut stored_compactions = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();
        stored_compactions
            .update(stored_compactions.prepare_dirty().unwrap())
            .await
            .unwrap();
        stored_compactions
            .update(stored_compactions.prepare_dirty().unwrap())
            .await
            .unwrap();

        let recorder = MetricsRecorderHelper::noop();
        let task = CompactionsGcTask::new(
            compactions_store.clone(),
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
            .get(&Path::from("/root/gc/compactions.boundary"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("2", std::str::from_utf8(&raw_boundary).unwrap());

        let compactions = compactions_store.list_compactions(..).await.unwrap();
        assert_eq!(
            vec![3],
            compactions
                .iter()
                .map(|compactions| compactions.id)
                .collect::<Vec<_>>()
        );
    }
}
