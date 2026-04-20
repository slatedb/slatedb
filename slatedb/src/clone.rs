use crate::bytes_range::BytesRange;
use crate::checkpoint::Checkpoint;
use crate::config::CheckpointOptions;

use crate::db::builder::CloneSourceSpec;
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::error::SlateDBError::CheckpointMissing;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::{Manifest, ManifestCore};
use crate::object_stores::ObjectStoreType::{Main, Wal};
use crate::object_stores::ObjectStores;
use crate::paths::PathResolver;
use crate::rand::DbRand;
use crate::utils::IdGenerator;
use bytes::Bytes;
use fail_parallel::{fail_point, FailPointRegistry};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb_common::clock::SystemClock;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) async fn create_clone<P: Into<Path>, R: RangeBounds<Bytes> + Clone>(
    clone_sources: Vec<CloneSourceSpec<R>>,
    clone_path: P,
    object_stores: ObjectStores,
    fp_registry: Arc<FailPointRegistry>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    projection_range: Option<R>,
) -> Result<(), SlateDBError> {
    let clone_path = clone_path.into();

    validate_clone_source_specs(clone_sources.clone(), clone_path.clone())?;

    let mut clone_manifest = create_clone_manifest(
        clone_path.clone(),
        clone_sources.clone(),
        object_stores.store_of(Main).clone(),
        system_clock.clone(),
        rand,
        fp_registry.clone(),
        projection_range,
    )
    .await?;

    if !clone_manifest.db_state().initialized {
        // Copy WAL SSTs from all sources - WAL is only supported for single source
        // this invariant is enforced in create_clone_manifest()
        if clone_sources.len() == 1 {
            for source in &clone_sources {
                let parent_path = source.path.clone();
                copy_wal_ssts(
                    object_stores.store_of(Wal).clone(),
                    clone_manifest.db_state(),
                    &parent_path,
                    &clone_path,
                    fp_registry.clone(),
                )
                .await?;
            }
        }

        let mut dirty = clone_manifest.prepare_dirty()?;
        dirty.value.core.initialized = true;
        clone_manifest.update(dirty).await?;
    }

    Ok(())
}

async fn create_clone_manifest<R: RangeBounds<Bytes> + Clone>(
    clone_path: Path,
    source_specs: Vec<CloneSourceSpec<R>>,
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    #[allow(unused)] fp_registry: Arc<FailPointRegistry>,
    projection_range: Option<R>,
) -> Result<StoredManifest, SlateDBError> {
    let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));

    let clone_manifest =
        match StoredManifest::try_load(clone_manifest_store.clone(), system_clock.clone()).await? {
            Some(initialized_clone_manifest)
                if initialized_clone_manifest.db_state().initialized =>
            {
                for source_spec in &source_specs {
                    validate_attached_to_external_db(
                        source_spec.path.to_string(),
                        source_spec.checkpoint,
                        &initialized_clone_manifest,
                    )?;
                    validate_external_dbs_contain_final_checkpoint(
                        Arc::new(ManifestStore::new(&source_spec.path, object_store.clone())),
                        source_spec.path.to_string(),
                        &initialized_clone_manifest,
                        object_store.clone(),
                    )
                    .await?;
                }
                return Ok(initialized_clone_manifest);
            }
            Some(uninitialized_clone_manifest) => {
                for source_spec in &source_specs {
                    validate_attached_to_external_db(
                        source_spec.path.to_string(),
                        source_spec.checkpoint,
                        &uninitialized_clone_manifest,
                    )?;
                }
                uninitialized_clone_manifest
            }
            None => {
                let sources = build_sources(
                    &source_specs,
                    &object_store,
                    &system_clock,
                    &rand,
                    &projection_range,
                )
                .await?;

                let manifest: Manifest = match &sources[..] {
                    [single_source] => Manifest::cloned(
                        &single_source.manifest,
                        single_source.path.to_string(),
                        single_source.checkpoint.id,
                        rand,
                    ),
                    [..] => {
                        validate_no_wal(&sources)?;
                        Manifest::cloned_from_union(
                            sources.iter().map(|s| s.manifest.clone()).collect(),
                        )
                    }
                };

                StoredManifest::store_uninitialized_clone(
                    clone_manifest_store,
                    manifest,
                    system_clock.clone(),
                )
                .await?
            }
        };

    fail_point!(fp_registry, "create-clone-manifest-io-error", |_| Err(
        SlateDBError::from(std::io::Error::other("oops"))
    ));

    // Ensure all external databases contain the final checkpoint.
    for external_db in &clone_manifest.manifest().external_dbs {
        let Some(final_checkpoint_id) = external_db.final_checkpoint_id else {
            // If the final checkpoint id is not set, we can skip this check
            continue;
        };
        let external_db_manifest_store = source_specs
            .iter()
            .find(|p| p.path.to_string() == external_db.path)
            .map(|p| Arc::new(ManifestStore::new(&p.path, object_store.clone())))
            .unwrap_or_else(|| {
                Arc::new(ManifestStore::new(
                    &external_db.path.clone().into(),
                    object_store.clone(),
                ))
            });

        let mut external_db_manifest =
            load_initialized_manifest(external_db_manifest_store, system_clock.clone()).await?;

        if external_db_manifest
            .db_state()
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            external_db_manifest
                .write_checkpoint(
                    final_checkpoint_id,
                    &CheckpointOptions {
                        lifetime: None,
                        source: Some(external_db.source_checkpoint_id),
                        name: None,
                    },
                )
                .await?;
        }
    }

    Ok(clone_manifest)
}

fn to_byte_range<T: RangeBounds<Bytes> + Clone>(bounds: &T) -> BytesRange {
    BytesRange::from(bounds.clone())
}

#[derive(Clone)]
struct CloneSource {
    pub path: Path,
    pub manifest: Manifest,
    pub checkpoint: Checkpoint,
}

async fn build_sources<R: RangeBounds<Bytes> + Clone>(
    source_specs: &Vec<CloneSourceSpec<R>>,
    object_store: &Arc<dyn ObjectStore>,
    system_clock: &Arc<dyn SystemClock>,
    rand: &Arc<DbRand>,
    projection_range: &Option<R>,
) -> Result<Vec<CloneSource>, SlateDBError> {
    let mut result: Vec<CloneSource> = vec![];
    for source in source_specs {
        let manifest_store = Arc::new(ManifestStore::new(&source.path, object_store.clone()));
        let mut latest_manifest =
            load_initialized_manifest(manifest_store.clone(), system_clock.clone()).await?;
        let checkpoint =
            get_or_create_parent_checkpoint(&mut latest_manifest, source.checkpoint, rand.clone())
                .await?;
        let mut manifest_at_checkpoint =
            manifest_store.read_manifest(checkpoint.manifest_id).await?;

        let range: Option<BytesRange> = match (source.projection_range.clone(), projection_range) {
            (Some(l), Some(r)) => to_byte_range(&l).intersect(&to_byte_range(r)),
            (Some(l), None) => Some(to_byte_range(&l)),
            (None, Some(r)) => Some(to_byte_range(r)),
            (None, None) => None,
        };

        manifest_at_checkpoint = match range {
            Some(range) => Manifest::projected(&manifest_at_checkpoint, range),
            None => manifest_at_checkpoint,
        };

        result.push(CloneSource {
            path: source.path.clone(),
            manifest: manifest_at_checkpoint,
            checkpoint,
        });
    }
    Ok(result)
}

// Get a checkpoint and the corresponding manifest that will be used as the source
// for the clone's initial state.
//
// If `parent_checkpoint_id` is `None`, then create an ephemeral checkpoint from
// the latest state.  Making it ephemeral ensures that it will
// get cleaned up if the clone operation fails.
async fn get_or_create_parent_checkpoint(
    manifest: &mut StoredManifest,
    maybe_checkpoint_id: Option<Uuid>,
    rand: Arc<DbRand>,
) -> Result<Checkpoint, SlateDBError> {
    let checkpoint = match maybe_checkpoint_id {
        Some(checkpoint_id) => match manifest.db_state().find_checkpoint(checkpoint_id) {
            Some(found_checkpoint) => found_checkpoint.clone(),
            None => return Err(CheckpointMissing(checkpoint_id)),
        },
        None => {
            let checkpoint_id = rand.rng().gen_uuid();
            manifest
                .write_checkpoint(
                    checkpoint_id,
                    &CheckpointOptions {
                        lifetime: Some(Duration::from_secs(300)),
                        source: None,
                        name: None,
                    },
                )
                .await?
        }
    };
    Ok(checkpoint)
}

fn validate_clone_source_specs<R: RangeBounds<Bytes> + Clone>(
    specs: Vec<CloneSourceSpec<R>>,
    clone_path: Path,
) -> Result<(), SlateDBError> {
    if specs.is_empty() {
        return Err(SlateDBError::InvalidUnionSetEmpty());
    }

    let mut seen_paths = std::collections::HashSet::new();
    for source in &specs {
        if clone_path == source.path {
            return Err(SlateDBError::IdenticalClonePaths(clone_path.clone()));
        }
        if !seen_paths.insert(source.path.to_string()) {
            return Err(SlateDBError::DuplicatedCloneSourcePath(source.path.clone()));
        }
    }
    Ok(())
}

fn validate_no_wal(sources: &[CloneSource]) -> Result<(), SlateDBError> {
    let mut parents_with_wal = vec![];
    for source in sources {
        let m = source.manifest.clone();
        let has_wal = m.core.next_wal_sst_id - 1 > m.core.replay_after_wal_id;
        if has_wal {
            parents_with_wal.push(source.path.clone());
        }
    }
    if !parents_with_wal.is_empty() {
        return Err(SlateDBError::InvalidUnionSourceWithWal {
            paths: parents_with_wal,
        });
    }
    Ok(())
}

// Validate that the manifest is attached to an external database at specific checkpoint.
fn validate_attached_to_external_db(
    path: String,
    checkpoint_id: Option<Uuid>,
    clone_manifest: &StoredManifest,
) -> Result<(), SlateDBError> {
    let external_dbs = &clone_manifest.manifest().external_dbs;
    if external_dbs.is_empty() {
        return Err(SlateDBError::CloneExternalDbMissing);
    }
    if !external_dbs.iter().any(|external_db| {
        path == external_db.path
            && checkpoint_id
                .map(|id| id == external_db.source_checkpoint_id)
                .unwrap_or(true)
    }) {
        return Err(SlateDBError::CloneIncorrectExternalDbCheckpoint {
            path,
            checkpoint_id,
        });
    };
    Ok(())
}

async fn validate_external_dbs_contain_final_checkpoint(
    parent_manifest_store: Arc<ManifestStore>,
    parent_path: String,
    clone_manifest: &StoredManifest,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), SlateDBError> {
    // Validate external dbs all contain the final checkpoint
    for external_db in &clone_manifest.manifest().external_dbs {
        let Some(final_checkpoint_id) = external_db.final_checkpoint_id else {
            // If the final checkpoint id is not set, we can skip this check
            continue;
        };
        let external_manifest_store = if external_db.path == parent_path {
            parent_manifest_store.clone()
        } else {
            Arc::new(ManifestStore::new(
                &external_db.path.clone().into(),
                object_store.clone(),
            ))
        };
        let external_manifest = external_manifest_store
            .read_latest_manifest()
            .await?
            .manifest;
        if external_manifest
            .core
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            return Err(SlateDBError::CloneIncorrectFinalCheckpoint {
                path: external_db.path.clone(),
                checkpoint_id: final_checkpoint_id,
            });
        }
    }

    Ok(())
}

async fn load_initialized_manifest(
    manifest_store: Arc<ManifestStore>,
    system_clock: Arc<dyn SystemClock>,
) -> Result<StoredManifest, SlateDBError> {
    let Some(manifest) =
        StoredManifest::try_load(manifest_store.clone(), system_clock.clone()).await?
    else {
        return Err(SlateDBError::LatestTransactionalObjectVersionMissing);
    };

    if !manifest.db_state().initialized {
        return Err(SlateDBError::InvalidDBState);
    }

    Ok(manifest)
}

async fn copy_wal_ssts(
    object_store: Arc<dyn ObjectStore>,
    parent_checkpoint_state: &ManifestCore,
    parent_path: &Path,
    clone_path: &Path,
    #[allow(unused)] fp_registry: Arc<FailPointRegistry>,
) -> Result<(), SlateDBError> {
    let parent_path_resolver = PathResolver::new(parent_path.clone());
    let clone_path_resolver = PathResolver::new(clone_path.clone());

    let mut wal_id = parent_checkpoint_state.replay_after_wal_id + 1;
    while wal_id < parent_checkpoint_state.next_wal_sst_id {
        fail_point!(fp_registry.clone(), "copy-wal-ssts-io-error", |_| Err(
            SlateDBError::from(std::io::Error::other("oops"))
        ));

        let id = SsTableId::Wal(wal_id);
        let parent_path = parent_path_resolver.table_path(&id);
        let clone_path = clone_path_resolver.table_path(&id);
        object_store
            .as_ref()
            .copy(&parent_path, &clone_path)
            .await?;
        wal_id += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::{
        CheckpointOptions, CheckpointScope, FlushOptions, FlushType, PutOptions, Settings,
        WriteOptions,
    };
    use crate::db::builder::CloneSourceSpec;
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::error::SlateDBError;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::manifest::ManifestCore;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::proptest_util::{rng, sample};
    use crate::rand::DbRand;
    use crate::test_utils;
    use crate::utils::IdGenerator;
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::Error as ObjectStoreError;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::SystemClock;
    use std::ops::RangeFull;
    use std::sync::Arc;
    use uuid::Uuid;

    // helper method for tests that creates CloneSourceSpec
    async fn create_clone<P: Into<Path>>(
        clone_path: P,
        parent_path: P,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Arc<dyn ObjectStore>,
        parent_checkpoint: Option<Uuid>,
        fp_registry: Arc<FailPointRegistry>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<(), SlateDBError> {
        let source: CloneSourceSpec = match parent_checkpoint {
            Some(cp) => CloneSourceSpec::with_checkpoint(parent_path, cp),
            None => CloneSourceSpec::new(parent_path),
        };
        crate::clone::create_clone(
            vec![source],
            clone_path,
            ObjectStores::new(object_store, Some(wal_object_store)),
            fp_registry,
            system_clock,
            rand,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn should_clone_latest_state_if_no_checkpoint_provided() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), object_store.clone())
            .await
            .unwrap();
        let mut db_iter = clone_db.scan::<Vec<u8>, RangeFull>(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_enabled() {
        should_clone_from_checkpoint(Settings::default()).await
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_disabled() {
        should_clone_from_checkpoint(Settings {
            wal_enabled: false,
            ..Settings::default()
        })
        .await
    }

    async fn should_clone_from_checkpoint(db_opts: Settings) {
        let mut rng = rng::new_test_rng(None);
        let checkpoint_table = sample::table(&mut rng, 5000, 10);
        let post_checkpoint_table = sample::table(&mut rng, 1000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_settings(db_opts.clone())
            .build()
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &checkpoint_table, false)
            .await
            .unwrap();
        let checkpoint = parent_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        // Add some more data so that we can be sure that the clone was created
        // from the checkpoint and not the latest state.
        test_utils::seed_database(&parent_db, &post_checkpoint_table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::builder(clone_path, object_store.clone())
            .with_settings(db_opts)
            .build()
            .await
            .unwrap();
        let mut db_iter = clone_db.scan::<Vec<u8>, RangeFull>(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&checkpoint_table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_is_invalid() {
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Create the parent with empty state
        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // Create an uninitialized manifest with an invalid checkpoint id
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let non_existent_source_checkpoint_id = uuid::Uuid::new_v4();
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &Manifest::initial(ManifestCore::new()),
                parent_path.to_string(),
                non_existent_source_checkpoint_id,
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Cloning should reset the checkpoint to a newly generated id
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, SlateDBError::CheckpointMissing(id) if id == non_existent_source_checkpoint_id)
        );
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_differs_from_provided() {
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Create the parent with empty state
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_sm = StoredManifest::create_new_db(
            parent_manifest_store,
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();
        let uuid_1 = rand.rng().gen_uuid();
        let checkpoint_1 = parent_sm
            .write_checkpoint(uuid_1, &CheckpointOptions::default())
            .await
            .unwrap();
        let uuid_2 = rand.rng().gen_uuid();
        let checkpoint_2 = parent_sm
            .write_checkpoint(uuid_2, &CheckpointOptions::default())
            .await
            .unwrap();

        // Create an uninitialized manifest referring to the first checkpoint
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &Manifest::initial(ManifestCore::new()),
                parent_path.to_string(),
                checkpoint_1.id,
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Cloning with the second checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint_2.id),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. }
        ));
    }

    #[tokio::test]
    async fn should_fail_retry_if_parent_path_is_different() {
        let object_store = Arc::new(InMemory::new());
        let original_parent_path = Path::from("/tmp/test_parent");
        let updated_parent_path = Path::from("/tmp/test_parent/new");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Setup an uninitialized manifest pointing to a different parent
        let parent_manifest = Manifest::initial(ManifestCore::new());
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &parent_manifest,
                original_parent_path.to_string(),
                uuid::Uuid::new_v4(),
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Initialize the parent at the updated path
        let parent_db = Db::open(updated_parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // The clone should fail because of inconsistent parent information
        let err = create_clone(
            clone_path.clone(),
            updated_parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. }
        ));
    }

    #[tokio::test]
    async fn clone_retry_should_be_idempotent_after_success() -> Result<(), SlateDBError> {
        let object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::open(parent_path, object_store.clone()).await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();

        let clone_manifest_store =
            ManifestStore::new(&Path::from(clone_path), object_store.clone());
        let manifest_id = clone_manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .id;

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await?;

        assert_eq!(
            manifest_id,
            clone_manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .id
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_retry_clone_after_io_error_copying_wals() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::builder(parent_path.clone(), object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let mut rng = rng::new_test_rng(None);
        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();

        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        // Block L0 uploads so the data remains in the WAL after close.
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        parent_db.close().await.unwrap();
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "off",
        )
        .unwrap();

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "copy-wal-ssts-io-error",
            "1*off->return",
        )
        .unwrap();

        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::IoError(_)));

        fail_parallel::cfg(Arc::clone(&fp_registry), "copy-wal-ssts-io-error", "off").unwrap();
        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_source_checkpoint_is_missing() -> Result<(), crate::Error> {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::open(parent_path.clone(), object_store.clone()).await?;
        let mut rng = rng::new_test_rng(None);
        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false).await?;
        let checkpoint = parent_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await?;
        parent_db.close().await?;

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "create-clone-manifest-io-error",
            "return",
        )
        .unwrap();

        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::IoError(_)));

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "create-clone-manifest-io-error",
            "off",
        )
        .unwrap();

        // Delete the checkpoint from the parent database
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_manifest =
            StoredManifest::load(parent_manifest_store, system_clock.clone()).await?;
        parent_manifest.delete_checkpoint(checkpoint.id).await?;

        // Attempting to clone with a missing checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::CheckpointMissing(id) if id == checkpoint.id));

        Ok(())
    }

    #[tokio::test]
    async fn clone_should_succeed_when_wal_object_store_is_provided() {
        let object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        let write_options = WriteOptions {
            await_durable: false,
        };
        let put_options = PutOptions::default();
        let l0_and_wal_data = [
            (b"l0-key-1".as_slice(), b"l0-value-1".as_slice()),
            (b"l0-key-2".as_slice(), b"l0-value-2".as_slice()),
        ];
        let wal_only_data = [
            (b"wal-only-key-1".as_slice(), b"wal-only-value-1".as_slice()),
            (b"wal-only-key-2".as_slice(), b"wal-only-value-2".as_slice()),
        ];
        for &(key, value) in &l0_and_wal_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        for &(key, value) in &wal_only_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        let manifest = parent_db.manifest();
        assert!(
            !manifest.manifest.core.l0.is_empty(),
            "expected cloned state to include L0 data"
        );
        assert!(
            manifest.manifest.core.replay_after_wal_id + 1 < manifest.manifest.core.next_wal_sst_id,
            "expected cloned state to retain WAL-only SSTs"
        );
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            wal_object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::builder(clone_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        for &(key, value) in &l0_and_wal_data {
            assert_eq!(
                clone_db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
        for &(key, value) in &wal_only_data {
            assert_eq!(
                clone_db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn clone_should_fail_when_wal_store_is_not_provided() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let write_options = WriteOptions {
            await_durable: false,
        };
        let put_options = PutOptions::default();
        let l0_and_wal_data = [
            (b"l0-key-1".as_slice(), b"l0-value-1".as_slice()),
            (b"l0-key-2".as_slice(), b"l0-value-2".as_slice()),
        ];
        let wal_only_data = [
            (b"wal-only-key-1".as_slice(), b"wal-only-value-1".as_slice()),
            (b"wal-only-key-2".as_slice(), b"wal-only-value-2".as_slice()),
        ];
        for &(key, value) in &l0_and_wal_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        for &(key, value) in &wal_only_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        let manifest = parent_db.manifest();
        assert!(
            !manifest.manifest.core.l0.is_empty(),
            "expected cloned state to include L0 data"
        );
        assert!(
            manifest.manifest.core.replay_after_wal_id + 1 < manifest.manifest.core.next_wal_sst_id,
            "expected cloned state to retain WAL-only SSTs"
        );
        let expected_missing_wal_path = PathResolver::new(Path::from(parent_path))
            .table_path(&SsTableId::Wal(
                manifest.manifest.core.replay_after_wal_id + 1,
            ))
            .to_string();
        // Block L0 uploads so the WAL-only data stays in the WAL.
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        parent_db.close().await.unwrap();
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        // Pass main store as WAL store — WAL SSTs won't be found there
        let err = create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SlateDBError::ObjectStoreError(ref source)
                if matches!(
                    source.as_ref(),
                    ObjectStoreError::NotFound { path, .. } if path == &expected_missing_wal_path
                )
        ));
    }
}
