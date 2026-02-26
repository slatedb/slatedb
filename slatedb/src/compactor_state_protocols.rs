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

use crate::compactions_store::{CompactionsStore, FenceableCompactions, StoredCompactions};
use crate::compactor::CompactionsCore;
use crate::compactor_state::{CompactionStatus, Compactions, CompactorState};
use crate::config::{CheckpointOptions, CompactorOptions};
use crate::db_state::ManifestCore;
use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::utils::IdGenerator;
use crate::DbRand;
use slatedb_common::clock::SystemClock;

/// A read-only view of compactor state suitable for consumers like GC.
///
/// This view intentionally avoids `DirtyObject` because reads should not create or mutate
/// remote state (e.g., a missing `.compactions` file on a fresh DB).
pub struct CompactorStateView {
    /// The latest compactions state if present. The u64 is the compactions file version.
    pub(crate) compactions: Option<(u64, Compactions)>,
    /// The latest manifest. The u64 is the manifest file version.
    pub(crate) manifest: (u64, Manifest),
}

impl CompactorStateView {
    /// Returns a read-only view of the .compactions file if present.
    pub fn compactions(&self) -> Option<&CompactionsCore> {
        self.compactions.as_ref().map(|(_, c)| &c.core)
    }

    /// Returns a read-only view of the .manifest file.
    pub fn manifest(&self) -> &ManifestCore {
        &self.manifest.1.core
    }
}

/// Converts a full [`CompactorState`] into a read-only view.
impl From<&CompactorState> for CompactorStateView {
    fn from(state: &CompactorState) -> Self {
        CompactorStateView {
            compactions: Some((
                state.compactions().id.into(),
                state.compactions().value.clone(),
            )),
            manifest: (state.manifest().id.into(), state.manifest().value.clone()),
        }
    }
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
        let (manifest, mut compactions) = Self::fence(
            stored_manifest,
            compactions_store,
            system_clock.clone(),
            options,
        )
        .await?;
        let dirty_manifest = manifest.prepare_dirty()?;
        let mut dirty_compactions = compactions.prepare_dirty()?;
        // Move running compactions back to submitted so we can resume them after restart.
        // Submitted compactions are left intact for future scheduling.
        // Keep only the most recent finished compaction for GC safety (#1044).
        dirty_compactions.value.iter_mut().for_each(|c| {
            if matches!(c.status(), CompactionStatus::Running) {
                c.set_status(CompactionStatus::Submitted);
            }
        });
        dirty_compactions.value.retain_active_and_last_finished();
        // Persist recovery state before any refresh() can overwrite it.
        compactions.update(dirty_compactions.clone()).await?;
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

    /// Refreshes the compactions view and updates the local compactor state with any remote
    /// changes.
    ///
    /// ## Returns
    /// - `Ok(())` after compactions are refreshed, or `SlateDBError` on failure.
    pub(crate) async fn load_compactions(&mut self) -> Result<(), SlateDBError> {
        self.compactions.refresh().await?;
        self.state
            .merge_remote_compactions(self.compactions.prepare_dirty()?);
        Ok(())
    }

    /// Refreshes compactions first, then manifests, to preserve a consistent ordering.
    ///
    /// ## Returns
    /// - `Ok(())` after state is refreshed, or `SlateDBError` on failure.
    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.load_compactions().await?;
        self.load_manifest().await?;
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
        let mut desired_value = self.state.compactions().value.clone();
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
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use crate::compactor_state::{
        Compaction, CompactionSpec, CompactionStatus, Compactions, CompactorState,
    };
    use crate::db_state::{ManifestCore, SsTableHandle, SsTableId, SsTableInfo};
    use crate::error::SlateDBError;
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use slatedb_txn_obj::test_utils::new_dirty_object;
    use std::sync::Arc;
    use ulid::Ulid;

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
            ManifestCore::new(),
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

    #[test]
    fn test_compactor_state_to_view() {
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.compactor_epoch = 11;
        manifest.core.l0_last_compacted = Some(Ulid::from_parts(5, 0));

        let mut compactions = Compactions::new(manifest.compactor_epoch);
        let compaction_id = Ulid::from_parts(10, 0);
        let spec = CompactionSpec::new(vec![], 7);
        let compaction =
            Compaction::new(compaction_id, spec.clone()).with_status(CompactionStatus::Running);
        compactions.insert(compaction);

        let manifest_id = 3u64;
        let compactions_id = 8u64;
        let state = CompactorState::new(
            new_dirty_object(manifest_id, manifest.clone()),
            new_dirty_object(compactions_id, compactions.clone()),
        );

        let view = CompactorStateView::from(&state);

        assert_eq!(view.manifest.0, manifest_id);
        assert_eq!(view.manifest.1, manifest);

        let (view_compactions_id, view_compactions) =
            view.compactions.expect("missing compactions");
        assert_eq!(view_compactions_id, compactions_id);
        assert_eq!(
            view_compactions.compactor_epoch,
            compactions.compactor_epoch
        );

        let view_compaction = view_compactions
            .get(&compaction_id)
            .expect("missing compaction");
        assert_eq!(view_compaction.status(), CompactionStatus::Running);
        assert_eq!(view_compaction.spec(), &spec);
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
            ManifestCore::new(),
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
    async fn test_new_resets_running_to_submitted_and_preserves_submitted() {
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
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Seed the compactions store with Submitted/Running/Finished entries.
        let mut stored_compactions = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();
        let submitted_id = Ulid::from_parts(1, 0);
        let failed_old_id = Ulid::from_parts(2, 0);
        let completed_old_id = Ulid::from_parts(3, 0);
        let running_id = Ulid::from_parts(4, 0);
        let mut dirty = stored_compactions.prepare_dirty().unwrap();
        dirty.value.insert(Compaction::new(
            submitted_id,
            CompactionSpec::new(vec![], 0),
        ));
        dirty.value.insert(
            Compaction::new(failed_old_id, CompactionSpec::new(vec![], 0))
                .with_status(CompactionStatus::Failed),
        );
        dirty.value.insert(
            Compaction::new(completed_old_id, CompactionSpec::new(vec![], 0))
                .with_status(CompactionStatus::Completed),
        );
        dirty.value.insert(
            Compaction::new(running_id, CompactionSpec::new(vec![], 0))
                .with_status(CompactionStatus::Running),
        );
        stored_compactions.update(dirty).await.unwrap();

        // Initialize a new writer (restart) which should flip Running -> Submitted and trim.
        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        let writer = CompactorStateWriter::new(
            manifest_store,
            compactions_store,
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        // Submitted should remain; Running should become Submitted; older finished should be trimmed.
        let compactions = &writer.state.compactions().value;
        assert_eq!(
            compactions
                .get(&submitted_id)
                .expect("missing submitted compaction")
                .status(),
            CompactionStatus::Submitted
        );
        assert_eq!(
            compactions
                .get(&running_id)
                .expect("missing running compaction")
                .status(),
            CompactionStatus::Submitted
        );
        assert!(!compactions.contains(&failed_old_id));
        assert!(compactions.contains(&completed_old_id));
    }

    #[tokio::test]
    async fn test_new_resets_running_to_submitted_and_preserves_output_ssts() {
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
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let output_ssts = vec![
            SsTableHandle::new_compacted(
                SsTableId::Compacted(Ulid::from_parts(10, 0)),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::copy_from_slice(b"a")),
                    ..Default::default()
                },
                None,
            ),
            SsTableHandle::new_compacted(
                SsTableId::Compacted(Ulid::from_parts(11, 0)),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::copy_from_slice(b"m")),
                    ..Default::default()
                },
                None,
            ),
        ];

        let mut stored_compactions = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();
        let running_id = Ulid::from_parts(4, 0);
        let mut dirty = stored_compactions.prepare_dirty().unwrap();
        dirty.value.insert(
            Compaction::new(running_id, CompactionSpec::new(vec![], 0))
                .with_status(CompactionStatus::Running)
                .with_output_ssts(output_ssts.clone()),
        );
        stored_compactions.update(dirty).await.unwrap();

        let options = CompactorOptions::default();
        let rand = Arc::new(DbRand::new(7));

        let writer = CompactorStateWriter::new(
            manifest_store,
            compactions_store,
            system_clock,
            &options,
            rand,
        )
        .await
        .unwrap();

        let compaction = writer
            .state
            .compactions()
            .value
            .get(&running_id)
            .expect("missing running compaction");
        assert_eq!(compaction.status(), CompactionStatus::Submitted);
        assert_eq!(compaction.output_ssts(), &output_ssts);
    }

    #[tokio::test]
    async fn test_load_compactions_merges_external_submitted() {
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
            ManifestCore::new(),
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

        let submitted_id = Ulid::from_parts(10, 0);
        let mut external = StoredCompactions::load(compactions_store).await.unwrap();
        let mut dirty = external.prepare_dirty().unwrap();
        dirty.value.insert(Compaction::new(
            submitted_id,
            CompactionSpec::new(vec![], 0),
        ));
        external.update(dirty).await.unwrap();

        writer.load_compactions().await.unwrap();

        let compactions = &writer.state.compactions().value;
        assert_eq!(
            compactions
                .get(&submitted_id)
                .expect("missing submitted compaction")
                .status(),
            CompactionStatus::Submitted
        );
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
            ManifestCore::new(),
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
            ManifestCore::new(),
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
