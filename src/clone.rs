use crate::checkpoint::Checkpoint;
use crate::config::CheckpointOptions;
use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use crate::error::SlateDBError::CheckpointMissing;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::paths::PathResolver;
use fail_parallel::{fail_point, FailPointRegistry};
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) async fn create_clone<P: Into<Path>>(
    clone_path: P,
    parent_path: P,
    object_store: Arc<dyn ObjectStore>,
    parent_checkpoint: Option<Uuid>,
    fp_registry: Arc<FailPointRegistry>,
) -> Result<(), SlateDBError> {
    let clone_path = clone_path.into();
    let parent_path = parent_path.into();

    if clone_path == parent_path {
        return Err(SlateDBError::InvalidArgument {
            msg: format!(
                "Parent path '{}' must be different from the clone's path '{}'",
                parent_path, clone_path
            ),
        });
    }

    let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
    let parent_manifest_store = Arc::new(ManifestStore::new(&parent_path, object_store.clone()));

    let mut clone_manifest = create_clone_manifest(
        clone_manifest_store,
        parent_manifest_store,
        parent_path.to_string(),
        parent_checkpoint,
        object_store.clone(),
        fp_registry.clone(),
    )
    .await?;

    if !clone_manifest.db_state().initialized {
        copy_wal_ssts(
            object_store,
            clone_manifest.db_state(),
            &parent_path,
            &clone_path,
            fp_registry,
        )
        .await?;

        let mut dirty = clone_manifest.prepare_dirty();
        dirty.core.initialized = true;
        clone_manifest.update_manifest(dirty).await?;
    }

    Ok(())
}

async fn create_clone_manifest(
    clone_manifest_store: Arc<ManifestStore>,
    parent_manifest_store: Arc<ManifestStore>,
    parent_path: String,
    parent_checkpoint_id: Option<Uuid>,
    object_store: Arc<dyn ObjectStore>,
    #[allow(unused)] fp_registry: Arc<FailPointRegistry>,
) -> Result<StoredManifest, SlateDBError> {
    let clone_manifest = match StoredManifest::try_load(clone_manifest_store.clone()).await? {
        Some(initialized_clone_manifest) if initialized_clone_manifest.db_state().initialized => {
            validate_attached_to_external_db(
                parent_path.clone(),
                parent_checkpoint_id,
                &initialized_clone_manifest,
            )?;
            validate_external_dbs_contain_final_checkpoint(
                parent_manifest_store,
                parent_path.clone(),
                &initialized_clone_manifest,
                object_store.clone(),
            )
            .await?;
            return Ok(initialized_clone_manifest);
        }
        Some(uninitialized_clone_manifest) => {
            validate_attached_to_external_db(
                parent_path.clone(),
                parent_checkpoint_id,
                &uninitialized_clone_manifest,
            )?;
            uninitialized_clone_manifest
        }
        None => {
            let mut parent_manifest =
                load_initialized_manifest(parent_manifest_store.clone()).await?;
            let parent_checkpoint =
                get_or_create_parent_checkpoint(&mut parent_manifest, parent_checkpoint_id).await?;
            let parent_manifest_at_checkpoint = parent_manifest_store
                .read_manifest(parent_checkpoint.manifest_id)
                .await?;

            StoredManifest::create_uninitialized_clone(
                clone_manifest_store,
                &parent_manifest_at_checkpoint,
                parent_path.clone(),
                parent_checkpoint.id,
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
        let external_db_manifest_store = if external_db.path == parent_path {
            parent_manifest_store.clone()
        } else {
            Arc::new(ManifestStore::new(
                &external_db.path.clone().into(),
                object_store.clone(),
            ))
        };
        let mut external_db_manifest =
            load_initialized_manifest(external_db_manifest_store).await?;

        if external_db_manifest
            .db_state()
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            external_db_manifest
                .write_checkpoint(
                    Some(final_checkpoint_id),
                    &CheckpointOptions {
                        lifetime: None,
                        source: Some(external_db.source_checkpoint_id),
                    },
                )
                .await?;
        }
    }

    Ok(clone_manifest)
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
) -> Result<Checkpoint, SlateDBError> {
    let checkpoint = match maybe_checkpoint_id {
        Some(checkpoint_id) => match manifest.db_state().find_checkpoint(checkpoint_id) {
            Some(found_checkpoint) => found_checkpoint.clone(),
            None => return Err(CheckpointMissing(checkpoint_id)),
        },
        None => {
            manifest
                .write_checkpoint(
                    None,
                    &CheckpointOptions {
                        lifetime: Some(Duration::from_secs(300)),
                        source: None,
                    },
                )
                .await?
        }
    };
    Ok(checkpoint)
}

// Validate that the manifest is attached to an external database at specific checkpoint.
fn validate_attached_to_external_db(
    path: String,
    checkpoint_id: Option<Uuid>,
    clone_manifest: &StoredManifest,
) -> Result<(), SlateDBError> {
    let external_dbs = &clone_manifest.manifest().external_dbs;
    if external_dbs.is_empty() {
        return Err(SlateDBError::DatabaseAlreadyExists {
            msg: "Database exists, but is not attached to any external database".to_string(),
        });
    }
    if !external_dbs.iter().any(|external_db| {
        path == external_db.path
            && checkpoint_id
                .map(|id| id == external_db.source_checkpoint_id)
                .unwrap_or(true)
    }) {
        return Err(SlateDBError::DatabaseAlreadyExists {
            msg: format!(
                "Database exists, but is not attached to external database at [{}] with checkpoint [{}]",
                path,
                checkpoint_id.map(|id| id.to_string()).unwrap_or("<any>".to_string()),
            ),
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
        let external_manifest = external_manifest_store.read_latest_manifest().await?.1;
        if external_manifest
            .core
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            return Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!(
                    "Cloned database already exists and is initialized, but the final checkpoint [{}] \
                        referred to in the manifest no longer exists in the external database at \
                        path [{}]",
                    final_checkpoint_id, external_db.path,
                ),
            });
        }
    }

    Ok(())
}

async fn load_initialized_manifest(
    manifest_store: Arc<ManifestStore>,
) -> Result<StoredManifest, SlateDBError> {
    let Some(manifest) = StoredManifest::try_load(manifest_store.clone()).await? else {
        return Err(SlateDBError::LatestManifestMissing);
    };

    if !manifest.db_state().initialized {
        return Err(SlateDBError::InvalidDBState);
    }

    Ok(manifest)
}

async fn copy_wal_ssts(
    object_store: Arc<dyn ObjectStore>,
    parent_checkpoint_state: &CoreDbState,
    parent_path: &Path,
    clone_path: &Path,
    #[allow(unused)] fp_registry: Arc<FailPointRegistry>,
) -> Result<(), SlateDBError> {
    let parent_path_resolver = PathResolver::new(parent_path.clone());
    let clone_path_resolver = PathResolver::new(clone_path.clone());

    let mut wal_id = parent_checkpoint_state.last_compacted_wal_sst_id + 1;
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
    use crate::clone::create_clone;
    use crate::config::{CheckpointOptions, CheckpointScope, DbOptions};
    use crate::db::Db;
    use crate::db_state::CoreDbState;
    use crate::error::SlateDBError;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::proptest_util::{rng, sample};
    use crate::test_utils;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::ops::RangeFull;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn should_clone_latest_state_if_no_checkpoint_provided() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store))
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
            Arc::clone(&object_store),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        let mut db_iter = clone_db.scan::<Vec<u8>, RangeFull>(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_enabled() {
        should_clone_from_checkpoint(DbOptions::default()).await
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_disabled() {
        should_clone_from_checkpoint(DbOptions {
            wal_enabled: false,
            ..DbOptions::default()
        })
        .await
    }

    async fn should_clone_from_checkpoint(db_opts: DbOptions) {
        let mut rng = rng::new_test_rng(None);
        let checkpoint_table = sample::table(&mut rng, 5000, 10);
        let post_checkpoint_table = sample::table(&mut rng, 1000, 10);

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open_with_opts(parent_path.clone(), db_opts, Arc::clone(&object_store))
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &checkpoint_table, false)
            .await
            .unwrap();
        let checkpoint = parent_db
            .create_checkpoint(
                CheckpointScope::All { force_flush: true },
                &CheckpointOptions::default(),
            )
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
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            Some(checkpoint.id),
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        let mut db_iter = clone_db.scan::<Vec<u8>, RangeFull>(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&checkpoint_table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_is_invalid() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        // Create the parent with empty state
        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // Create an uninitialized manifest with an invalid checkpoint id
        let clone_manifest_store =
            Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));
        let non_existent_source_checkpoint_id = Uuid::new_v4();
        StoredManifest::create_uninitialized_clone(
            clone_manifest_store,
            &Manifest::initial(CoreDbState::new()),
            parent_path.to_string(),
            non_existent_source_checkpoint_id,
        )
        .await
        .unwrap();

        // Cloning should reset the checkpoint to a newly generated id
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, SlateDBError::CheckpointMissing(id) if id == non_existent_source_checkpoint_id)
        );
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_differs_from_provided() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        // Create the parent with empty state
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_sm =
            StoredManifest::create_new_db(parent_manifest_store, CoreDbState::new())
                .await
                .unwrap();
        let checkpoint_1 = parent_sm
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint_2 = parent_sm
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        // Create an uninitialized manifest referring to the first checkpoint
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        StoredManifest::create_uninitialized_clone(
            clone_manifest_store,
            &Manifest::initial(CoreDbState::new()),
            parent_path.to_string(),
            checkpoint_1.id,
        )
        .await
        .unwrap();

        // Cloning with the second checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            Some(checkpoint_2.id),
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, SlateDBError::DatabaseAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn should_fail_retry_if_parent_path_is_different() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let original_parent_path = Path::from("/tmp/test_parent");
        let updated_parent_path = Path::from("/tmp/test_parent/new");
        let clone_path = Path::from("/tmp/test_clone");

        // Setup an uninitialized manifest pointing to a different parent
        let parent_manifest = Manifest::initial(CoreDbState::new());
        let clone_manifest_store =
            Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));
        StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            &parent_manifest,
            original_parent_path.to_string(),
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        // Initialize the parent at the updated path
        let parent_db = Db::open(updated_parent_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // The clone should fail because of inconsistent parent information
        let err = create_clone(
            clone_path.clone(),
            updated_parent_path.clone(),
            Arc::clone(&object_store),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            SlateDBError::DatabaseAlreadyExists { msg: _ }
        ));
    }

    #[tokio::test]
    async fn clone_retry_should_be_idempotent_after_success() -> Result<(), SlateDBError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::open(parent_path, Arc::clone(&object_store))
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            Arc::clone(&object_store),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap();

        let clone_manifest_store =
            ManifestStore::new(&Path::from(clone_path), Arc::clone(&object_store));
        let (manifest_id, _) = clone_manifest_store.read_latest_manifest().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            Arc::clone(&object_store),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await?;

        assert_eq!(
            manifest_id,
            clone_manifest_store.read_latest_manifest().await.unwrap().0
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_retry_clone_after_io_error_copying_wals() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store))
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
        parent_db.close().await.unwrap();

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "copy-wal-ssts-io-error",
            "1*off->return",
        )
        .unwrap();

        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            None,
            Arc::clone(&fp_registry),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::IoError(_)));

        fail_parallel::cfg(Arc::clone(&fp_registry), "copy-wal-ssts-io-error", "off").unwrap();
        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            None,
            Arc::clone(&fp_registry),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_source_checkpoint_is_missing() -> Result<(), SlateDBError> {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store)).await?;
        let mut rng = rng::new_test_rng(None);
        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false).await?;
        let checkpoint = parent_db
            .create_checkpoint(
                CheckpointScope::All { force_flush: true },
                &CheckpointOptions::default(),
            )
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
            Arc::clone(&object_store),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
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
        let mut parent_manifest = StoredManifest::load(parent_manifest_store).await?;
        parent_manifest.delete_checkpoint(checkpoint.id).await?;

        // Attempting to clone with a missing checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::CheckpointMissing(id) if id == checkpoint.id));

        Ok(())
    }
}
