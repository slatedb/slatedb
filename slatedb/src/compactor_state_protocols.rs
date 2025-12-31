//! Protocols for reading and writing compactor state safely.
//!
//! This module isolates the ordering rules for compactor state persistence:
//! - **Reads**: fetch compactions before manifests so GC sees a consistent view of
//!   in-flight/finished compactions alongside the active manifests.
//! - **Writes**: persist manifest updates before compactions so new SSTs are visible
//!   before trimming input references. Checkpoints are written first to keep inputs
//!   GC-safe during the update.
//!
//! Keeping these rules in one place makes it harder to regress GC safety or
//! compactor fencing logic elsewhere in the codebase.
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};

use crate::clock::SystemClock;
use crate::compactions_store::{CompactionsStore, FenceableCompactions, StoredCompactions};
use crate::compactor_state::{CompactionStatus, Compactions, CompactorState};
use crate::config::{CheckpointOptions, CompactorOptions};
use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::utils::IdGenerator;
use crate::DbRand;

/// A read-only view of compactor state suitable for consumers like GC.
///
/// This view intentionally avoids `DirtyObject` because reads should not create or mutate
/// remote state (e.g., a missing `.compactions` file on a fresh DB).
pub(crate) struct CompactorStateView {
    /// The latest compactions state if present. The u64 is the compactions file version.
    pub(crate) compactions: Option<(u64, Compactions)>,
    /// The latest manifest. The u64 is the manifest file version.
    pub(crate) manifest: (u64, Manifest),
}

/// Reader that enforces compactions-first ordering when fetching state.
pub(crate) struct CompactorStateReader {
    /// Shared manifest store to read the latest manifest.
    manifest_store: Arc<ManifestStore>,
    /// Shared compactions store to fetch the latest compaction state first.
    compactions_store: Arc<CompactionsStore>,
}

impl CompactorStateReader {
    /// Creates a reader that returns a consistent view of compactions and active manifests.
    ///
    /// ## Arguments
    /// - `manifest_store`: Manifest store handle to read from.
    /// - `compactions_store`: Compactions store handle to read from.
    ///
    /// ## Returns
    /// - New reader instance that always fetches compactions before manifests.
    pub(crate) fn new(
        manifest_store: &Arc<ManifestStore>,
        compactions_store: &Arc<CompactionsStore>,
    ) -> Self {
        Self {
            manifest_store: manifest_store.clone(),
            compactions_store: compactions_store.clone(),
        }
    }

    /// Reads compactions then the latest manifest to keep consumers from observing an inconsistent view.
    pub(crate) async fn read_view(&self) -> Result<CompactorStateView, SlateDBError> {
        // Always read latest compactions before reading latest manifest.
        let compactions = self.compactions_store.try_read_latest_compactions().await?;
        let manifest = self.manifest_store.read_latest_manifest().await?;
        Ok(CompactorStateView {
            compactions,
            manifest,
        })
    }
}

/// Writer that fences and persists manifest-before-compactions with checkpointing.
pub(crate) struct CompactorStateWriter {
    /// Current in-memory compactor state (dirty manifest + compactions).
    pub(crate) state: CompactorState,
    /// Fenceable manifest handle used for refresh/update with fencing.
    manifest: FenceableManifest,
    /// Fenceable compactions handle used for refresh/update with fencing.
    compactions: FenceableCompactions,
    /// RNG for checkpoint ids.
    rand: Arc<DbRand>,
}

impl CompactorStateWriter {
    /// Initializes a fenced compactor state writer with manifest-first ordering.
    ///
    /// ## Arguments
    /// - `manifest_store`: Manifest store backing the writer.
    /// - `compactions_store`: Compactions store backing the writer.
    /// - `system_clock`: Clock for fencing/timeouts.
    /// - `options`: Compactor options containing timeouts.
    /// - `rand`: RNG for checkpoint ids.
    ///
    /// ## Returns
    /// - A new writer seeded with dirty manifest/compactions and finished compactions trimmed.
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        compactions_store: Arc<CompactionsStore>,
        system_clock: Arc<dyn SystemClock>,
        options: &CompactorOptions,
        rand: Arc<DbRand>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest =
            StoredManifest::load(manifest_store.clone(), system_clock.clone()).await?;
        let (manifest, compactions) = Self::fence(
            stored_manifest,
            compactions_store,
            system_clock.clone(),
            options,
        )
        .await?;
        let dirty_manifest = manifest.prepare_dirty()?;
        let mut dirty_compactions = compactions.prepare_dirty()?;
        // Mark any persisted compactions as finished so we don't resume them after restart.
        // Keep only the most recent finished compaction for GC safety (#1044).
        dirty_compactions
            .value
            .iter_mut()
            .for_each(|c| c.set_status(CompactionStatus::Finished));
        dirty_compactions.value.retain_active_and_last_finished();
        let state = CompactorState::new(dirty_manifest, dirty_compactions);
        Ok(Self {
            state,
            manifest,
            compactions,
            rand,
        })
    }

    async fn fence(
        stored_manifest: StoredManifest,
        compactions_store: Arc<CompactionsStore>,
        system_clock: Arc<dyn SystemClock>,
        options: &CompactorOptions,
    ) -> Result<(FenceableManifest, FenceableCompactions), SlateDBError> {
        let fenceable_manifest = FenceableManifest::init_compactor(
            stored_manifest,
            options.manifest_update_timeout,
            system_clock.clone(),
        )
        .await?;
        let stored_compactions =
            match StoredCompactions::try_load(compactions_store.clone()).await? {
                Some(compactions) => compactions,
                None => {
                    info!("creating new compactions file [compactor_epoch=0]");
                    StoredCompactions::create(compactions_store.clone(), 0).await?
                }
            };
        let fenceable_compactions = FenceableCompactions::init_with_epoch(
            stored_compactions,
            options.manifest_update_timeout,
            system_clock.clone(),
            fenceable_manifest.local_epoch(),
        )
        .await?;
        Ok((fenceable_manifest, fenceable_compactions))
    }

    /// Refreshes the manifest and updates the local compactor state with any remote
    /// changes.
    ///
    /// ## Returns
    /// - `Ok(())` after state is refreshed, or `SlateDBError` on failure.
    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        Ok(())
    }

    /// Persists the updated manifest after a compaction finishes.
    ///
    /// A checkpoint with a 15-minute lifetime is written first to prevent GC from
    /// deleting SSTs that are about to be removed. This is to keep them around for a
    /// while in case any in-flight operations (such as iterator scans) are still using
    /// them.
    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        // write the checkpoint first so that it points to the manifest with the ssts
        // being removed
        let checkpoint_id = self.rand.rng().gen_uuid();
        self.manifest
            .write_checkpoint(
                checkpoint_id,
                &CheckpointOptions {
                    // TODO(rohan): for now, just write a checkpoint with 15-minute expiry
                    //              so that it's extremely unlikely for the gc to delete ssts
                    //              out from underneath the writer. In a follow up, we'll write
                    //              a checkpoint with no expiry and with metadata indicating its
                    //              a compactor checkpoint. Then, the gc will delete the checkpoint
                    //              based on a configurable timeout
                    lifetime: Some(Duration::from_secs(900)),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        let dirty = self.state.manifest().clone();
        self.manifest.update(dirty).await
    }

    /// Writes the manifest, retrying on version conflicts by reloading and retrying.
    ///
    /// ## Returns
    /// - `Ok(())` when the manifest is successfully persisted.
    /// - `SlateDBError` if non-retryable errors occur.
    pub(crate) async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest().await?;
            match self.write_manifest().await {
                Ok(_) => return Ok(()),
                Err(SlateDBError::TransactionalObjectVersionExists) => {
                    debug!("conflicting manifest version. updating and retrying write again.");
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Persists the current compactions state to the compactions store and refreshes the
    /// local dirty object with the latest version.
    ///
    /// ## Returns
    /// - `Ok(())` when compactions are successfully written.
    /// - `SlateDBError` if an unrecoverable error occurs.
    pub(crate) async fn write_compactions_safely(&mut self) -> Result<(), SlateDBError> {
        let mut desired_value = self.state.compactions_dirty().value.clone();
        desired_value.retain_active_and_last_finished();
        loop {
            let mut dirty_compactions = self.compactions.prepare_dirty()?;
            dirty_compactions.value = desired_value.clone();
            match self.compactions.update(dirty_compactions).await {
                Ok(()) => {
                    let refreshed = self.compactions.prepare_dirty()?;
                    self.state.set_compactions(refreshed);
                    return Ok(());
                }
                Err(SlateDBError::TransactionalObjectVersionExists) => {
                    // Retry with a refreshed view. Refresh will surface fencing if the epoch advanced.
                    // If another process modified the compactions file legally (such as an external
                    // compaction request triggered from the CLI), this will pick up those changes.
                    self.compactions.refresh().await?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Writes manifest first, then compactions, retrying manifest conflicts.
    ///
    /// ## Returns
    /// - `Ok(())` when both manifest and compactions are persisted in order.
    /// - `SlateDBError` on unrecoverable failures.
    pub(crate) async fn write_state_safely(&mut self) -> Result<(), SlateDBError> {
        // Writes are always manifest-first
        self.write_manifest_safely().await?;
        self.write_compactions_safely().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::AdminBuilder;
    use crate::clock::{DefaultSystemClock, SystemClock};
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use crate::db_state::CoreDbState;
    use crate::error::SlateDBError;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    const ROOT: &str = "/compactor-state";

    #[tokio::test]
    async fn test_new_fences_manifest_and_compactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        let mut first_writer = CompactorStateWriter::new(
            manifest_store.clone(),
            compactions_store.clone(),
            system_clock.clone(),
            &options,
            Arc::clone(&rand),
        )
        .await
        .unwrap();

        let _second_writer = CompactorStateWriter::new(
            manifest_store,
            compactions_store,
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        let manifest_result = first_writer.manifest.refresh().await;
        assert!(matches!(manifest_result, Err(SlateDBError::Fenced)));

        let compactions_result = first_writer.compactions.refresh().await;
        assert!(matches!(compactions_result, Err(SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_fence_syncs_compactor_epochs() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let mut stored_manifest =
            StoredManifest::load(manifest_store.clone(), system_clock.clone())
                .await
                .unwrap();
        let mut dirty_manifest = stored_manifest.prepare_dirty().unwrap();
        dirty_manifest.value.compactor_epoch = 8;
        stored_manifest.update(dirty_manifest).await.unwrap();

        StoredCompactions::create(compactions_store.clone(), 7)
            .await
            .unwrap();

        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        CompactorStateWriter::new(
            manifest_store.clone(),
            compactions_store.clone(),
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        let (_, compactions) = compactions_store.read_latest_compactions().await.unwrap();
        assert_eq!(manifest.compactor_epoch, 9);
        assert_eq!(manifest.compactor_epoch, compactions.compactor_epoch);
    }

    #[tokio::test]
    async fn test_write_compactions_safely_retries_on_version_conflict() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        let mut writer = CompactorStateWriter::new(
            manifest_store,
            compactions_store.clone(),
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        // Record the version after fencing.
        let (start_id, _) = compactions_store.read_latest_compactions().await.unwrap();

        // Simulate an external writer racing and creating the next version.
        let mut external = StoredCompactions::load(compactions_store.clone())
            .await
            .unwrap();
        external
            .update(external.prepare_dirty().unwrap())
            .await
            .unwrap();

        let (conflicting_id, _) = compactions_store.read_latest_compactions().await.unwrap();
        assert_eq!(conflicting_id, start_id + 1);

        // This should retry on conflict and succeed with a new version.
        writer.write_compactions_safely().await.unwrap();

        let (final_id, _) = compactions_store.read_latest_compactions().await.unwrap();
        assert_eq!(final_id, start_id + 2);
    }

    #[tokio::test]
    async fn write_manifest_safely_retries_on_version_conflict() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from(ROOT),
            Arc::clone(&object_store),
        ));
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            CoreDbState::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        let mut writer = CompactorStateWriter::new(
            manifest_store.clone(),
            compactions_store.clone(),
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        // Record the version after fencing.
        let (start_id, _) = manifest_store.read_latest_manifest().await.unwrap();

        // Simulate an external writer creating a checkpoint of the manifest and updating it.
        let admin = AdminBuilder::new(ROOT, object_store.clone()).build();
        admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .expect("create checkpoint failed");

        let (conflicting_id, _) = manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(conflicting_id, start_id + 1);

        // This should retry on conflict and succeed with a new version.
        writer.write_manifest_safely().await.unwrap();

        let (final_id, _) = manifest_store.read_latest_manifest().await.unwrap();
        // write_manifest_safely now bumps the manifest twice per successful call because write_manifest
        // writes a checkpoint first:
        // - write_manifest() calls self.manifest.write_checkpoint(...) to create the checkpoint, then
        // - write_manifest() calls self.manifest.update(...) to update the manifest
        // So we do +1 for the external update and +2 for the successful write_manifest_safely call.
        assert_eq!(final_id, start_id + 3);
    }
}
