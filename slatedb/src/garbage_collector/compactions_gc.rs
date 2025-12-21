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

use super::{GcStats, GcTask, DEFAULT_MIN_AGE};

pub(crate) struct CompactionsGcTask {
    compactions_store: Arc<CompactionsStore>,
    stats: Arc<GcStats>,
    compactions_options: Option<GarbageCollectorDirectoryOptions>,
}

impl std::fmt::Debug for CompactionsGcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionsGcTask")
            .field("compactions_options", &self.compactions_options)
            .finish()
    }
}

impl CompactionsGcTask {
    pub fn new(
        compactions_store: Arc<CompactionsStore>,
        stats: Arc<GcStats>,
        compactions_options: Option<GarbageCollectorDirectoryOptions>,
    ) -> Self {
        Self {
            compactions_store,
            stats,
            compactions_options,
        }
    }

    fn compactions_min_age(&self) -> chrono::Duration {
        let min_age = self
            .compactions_options
            .map_or(DEFAULT_MIN_AGE, |opts| opts.min_age);
        chrono::Duration::from_std(min_age).expect("invalid duration")
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
                    self.stats.gc_compactions_count.inc();
                }
            }
        }

        Ok(())
    }

    fn resource(&self) -> &str {
        "Compactions"
    }
}
