use crate::checkpoint::Checkpoint;
use crate::config::CheckpointOptions;
use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use crate::error::SlateDBError::CheckpointMissing;
use crate::manifest::{Manifest, ParentDb};
use crate::manifest_store::{ManifestStore, StoredManifest};
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

    let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));

    let parent_manifest_store =
        Arc::new(ManifestStore::new(&parent_path, Arc::clone(&object_store)));

    let mut parent_manifest = load_initialized_manifest(Arc::clone(&parent_manifest_store)).await?;

    let mut clone_manifest = create_clone_manifest(
        Arc::clone(&clone_manifest_store),
        Arc::clone(&parent_manifest_store),
        &mut parent_manifest,
        &parent_path,
        &parent_checkpoint,
    )
    .await?;

    if !clone_manifest.db_state().initialized {
        copy_wal_ssts(
            object_store.clone(),
            clone_manifest.db_state(),
            &parent_path,
            &clone_path,
            fp_registry,
        )
        .await?;

        let mut initialized_db_state = clone_manifest.db_state().clone();
        initialized_db_state.initialized = true;
        clone_manifest.update_db_state(initialized_db_state).await?;
    }

    Ok(())
}

async fn create_clone_manifest(
    clone_manifest_store: Arc<ManifestStore>,
    parent_manifest_store: Arc<ManifestStore>,
    parent_manifest: &mut StoredManifest,
    parent_path: &Path,
    parent_checkpoint_id: &Option<Uuid>,
) -> Result<StoredManifest, SlateDBError> {
    let existing_clone_manifest =
        match StoredManifest::try_load(Arc::clone(&clone_manifest_store)).await? {
            // If the checkpoint is valid, just return the manifest as is.
            // Otherwise, retry checkpoint initialization below.
            Some(clone_manifest)
                if check_valid_checkpoint(
                    &clone_manifest,
                    parent_path,
                    parent_manifest,
                    parent_checkpoint_id,
                )? =>
            {
                return Ok(clone_manifest)
            }
            manifest => manifest,
        };

    let (parent_checkpoint, parent_checkpoint_manifest) = get_or_create_parent_checkpoint(
        Arc::clone(&parent_manifest_store),
        parent_manifest,
        parent_checkpoint_id,
    )
    .await?;

    let final_checkpoint_id = Uuid::new_v4();
    let parent_db = ParentDb {
        path: parent_path.to_string(),
        checkpoint_id: final_checkpoint_id,
    };

    let clone_manifest = create_uninitialized_clone_manifest(
        Arc::clone(&clone_manifest_store),
        existing_clone_manifest,
        parent_db,
        &parent_checkpoint_manifest,
    )
    .await?;

    parent_manifest
        .write_checkpoint(
            Some(final_checkpoint_id),
            &CheckpointOptions {
                lifetime: None,
                source: Some(parent_checkpoint.id),
            },
        )
        .await?;

    Ok(clone_manifest)
}

async fn create_uninitialized_clone_manifest(
    clone_manifest_store: Arc<ManifestStore>,
    existing_clone_manifest: Option<StoredManifest>,
    parent_db: ParentDb,
    parent_manifest: &Manifest,
) -> Result<StoredManifest, SlateDBError> {
    match existing_clone_manifest {
        Some(mut clone_manifest) => {
            clone_manifest
                .rewrite_parent_db(parent_db, parent_manifest)
                .await?;
            Ok(clone_manifest)
        }
        None => {
            StoredManifest::create_uninitialized_clone(
                Arc::clone(&clone_manifest_store),
                parent_db,
                parent_manifest,
            )
            .await
        }
    }
}

// Get a checkpoint and the corresponding manifest that will used as the source
// for the clone's initial state.
//
// If `parent_checkpoint_id` is `None`, then create an ephemeral checkpoint from
// the latest state.  Making it ephemeral ensures that it will
// get cleaned up if the clone operation fails.
async fn get_or_create_parent_checkpoint(
    parent_manifest_store: Arc<ManifestStore>,
    parent_manifest: &mut StoredManifest,
    parent_checkpoint_id: &Option<Uuid>,
) -> Result<(Checkpoint, Manifest), SlateDBError> {
    let checkpoint = if let Some(id) = parent_checkpoint_id {
        if let Some(checkpoint) = parent_manifest.db_state().find_checkpoint(id) {
            checkpoint.clone()
        } else {
            return Err(CheckpointMissing(*id));
        }
    } else {
        parent_manifest
            .write_checkpoint(
                None,
                &CheckpointOptions {
                    lifetime: Some(Duration::from_secs(300)),
                    source: None,
                },
            )
            .await?
    };

    let checkpoint_manifest = parent_manifest_store
        .read_manifest(checkpoint.manifest_id)
        .await?;
    Ok((checkpoint, checkpoint_manifest))
}

// For pre-existing manifests, we need to verify that the referenced checkpoint
// is valid and consistent with the arguments passed to `create_clone`. This
// function returns true if the checkpoint in the clone manifest is still valid
// and false if we should retry checkpoint creation. For other errors, such as
// an inconsistent `DbParent` path, return an error.
fn check_valid_checkpoint(
    clone_manifest: &StoredManifest,
    parent_path: &Path,
    parent_manifest: &StoredManifest,
    parent_checkpoint_id: &Option<Uuid>,
) -> Result<bool, SlateDBError> {
    let Some(parent_db) = &clone_manifest.manifest().parent else {
        return Err(SlateDBError::DatabaseAlreadyExists {
            msg: "Database exists, but is not attached to a parent database".to_string(),
        });
    };

    if Path::from(parent_db.path.clone()) != *parent_path {
        return Err(SlateDBError::DatabaseAlreadyExists {
            msg: format!(
                "Database exists, but is attached to a different parent with path '{}' \
                than the expected path '{}'",
                parent_db.path, parent_path
            ),
        });
    }

    let Some(actual_checkpoint) = parent_manifest
        .db_state()
        .find_checkpoint(&parent_db.checkpoint_id)
    else {
        // If the clone database has not yet been initialized, then we
        // can reset the checkpoint. Otherwise, we fail the operation.
        return if !clone_manifest.db_state().initialized {
            Ok(false)
        } else {
            Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!(
                    "Clone database already exists and is initialized, but the checkpoint {} \
                        referred to in the manifest no longer exists in the parent at \
                        path '{}'",
                    parent_db.checkpoint_id, parent_path,
                ),
            })
        };
    };

    if let Some(expected_checkpoint_id) = parent_checkpoint_id {
        let Some(expected_checkpoint) = parent_manifest
            .db_state()
            .find_checkpoint(expected_checkpoint_id)
        else {
            return Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!(
                    "The clone database exists, but cannot confirm that it is derived \
                        from the checkpoint {} since this checkpoint no longer exists",
                    expected_checkpoint_id
                ),
            });
        };

        // The current checkpoint points to a different manifest than `parent_checkpoint_id`.
        // If the database never finished initialization, we can rewrite the checkpoint.
        return if expected_checkpoint.manifest_id == actual_checkpoint.manifest_id {
            Ok(true)
        } else {
            Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!(
                    "The clone database already exists, but refers to a different \
                        checkpoint {} in the parent than the expected one ({})",
                    actual_checkpoint.id, expected_checkpoint_id
                ),
            })
        };
    }

    Ok(true)
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
        fail_point!(Arc::clone(&fp_registry), "copy-wal-ssts-io-error", |_| Err(
            SlateDBError::from(std::io::Error::new(std::io::ErrorKind::Other, "oops"))
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
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::error::SlateDBError;
    use crate::manifest::{Manifest, ParentDb};
    use crate::manifest_store::{ManifestStore, StoredManifest};
    use crate::proptest_util::{rng, sample};
    use crate::test_utils;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore};
    use std::ops::RangeFull;
    use std::sync::Arc;
    use futures::StreamExt;
    use uuid::Uuid;
    use crate::paths::PathResolver;

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
        test_utils::assert_ordered_scan_in_range(&table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_clone_from_checkpoint() {
        let mut rng = rng::new_test_rng(None);
        let checkpoint_table = sample::table(&mut rng, 5000, 10);
        let post_checkpoint_table = sample::table(&mut rng, 1000, 10);

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store))
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
        test_utils::assert_ordered_scan_in_range(&checkpoint_table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_rewrite_checkpoint_if_uninitialized_checkpoint_is_invalid() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        // Create the parent with empty state
        let parent_db = Db::open(parent_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // Create an uninitialized manifest with an invalid checkpoint id
        let parent_db = ParentDb {
            path: parent_path.to_string(),
            checkpoint_id: Uuid::new_v4(),
        };
        let clone_manifest_store =
            Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));
        let mut sm = StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_db,
            &Manifest::initial(CoreDbState::new()),
        )
        .await
        .unwrap();

        // Cloning should reset the checkpoint to a newly generated id
        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            None,
            Arc::new(FailPointRegistry::new()),
        )
        .await
        .unwrap();

        // Assert that the clone's checkpoint is valid in the parent manifest
        let _ = sm.refresh().await.unwrap();
        assert_valid_db_parent(Arc::clone(&object_store), &parent_path, sm.manifest()).await;
    }

    async fn assert_valid_db_parent(
        object_store: Arc<dyn ObjectStore>,
        parent_path: &Path,
        clone_manifest: &Manifest,
    ) {
        let parent_db = clone_manifest.parent.as_ref().unwrap();
        assert_eq!(parent_path.to_string(), parent_db.path);

        let manifest_store = Arc::new(ManifestStore::new(parent_path, Arc::clone(&object_store)));
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        assert!(manifest
            .core
            .find_checkpoint(&parent_db.checkpoint_id)
            .is_some());
    }

    #[tokio::test]
    async fn should_fail_retry_if_parent_path_is_different() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let original_parent_path = Path::from("/tmp/test_parent");
        let updated_parent_path = Path::from("/tmp/test_parent/new");
        let clone_path = Path::from("/tmp/test_clone");

        // Setup an uninitialized manifest pointing to a different parent
        let parent_manifest = Manifest::initial(CoreDbState::new());
        let parent_db = ParentDb {
            path: original_parent_path.to_string(),
            checkpoint_id: Uuid::new_v4(),
        };
        let clone_manifest_store =
            Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));
        StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_db,
            &parent_manifest,
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
    async fn clone_retry_should_be_idempotent_after_success() {
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
        .await
        .unwrap();
        assert_eq!(
            manifest_id,
            clone_manifest_store.read_latest_manifest().await.unwrap().0
        )
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
    async fn should_fail_and_write_nothing_on_clone_from_different_checkpoint() {
        async fn load_wals(object_store: &Arc<dyn ObjectStore>, path: &Path) -> Vec<ObjectMeta>{
            let path_resolver = PathResolver::new(path.clone());
            let mut wal_entries = vec![];
            let mut list_stream = object_store.list(Some(path));
            while let Some(meta) = list_stream.next().await.transpose().unwrap() {
                if let Some(table_id) = path_resolver.parse_table_id(&meta.location).unwrap() {
                    if matches!(table_id, SsTableId::Wal(_)) {
                        wal_entries.push(meta);
                    }
                }
            }
            wal_entries
        }

        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let parent_db_opts = DbOptions {
            compactor_options: None,
            ..DbOptions::default()
        };
        let parent_db =
            Db::open_with_opts(parent_path.clone(), parent_db_opts, object_store.clone()).await.unwrap();
        parent_db.put("first", "foo").await.unwrap();
        let first_checkpoint = parent_db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        parent_db.put("second", "bar").await.unwrap();
        let second_checkpoint = parent_db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        fail_parallel::cfg(
            fp_registry.clone(),
            "copy-wal-ssts-io-error",
            "1*off->return",
        )
            .unwrap();
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            Some(second_checkpoint.id),
            fp_registry.clone(),
        )
            .await
            .unwrap_err();
        assert!(matches!(err, SlateDBError::IoError(_)));
        let clone_manifest_store = ManifestStore::new(&clone_path, object_store.clone());
        let clone_manifest = clone_manifest_store.read_latest_manifest().await.unwrap();
        let wal_entries = load_wals(&object_store, &clone_path).await;
        assert_eq!(wal_entries.len(), 1);

        // Create clone that moves to earlier checkpoint
        fail_parallel::cfg(fp_registry.clone(), "copy-wal-ssts-io-error", "off").unwrap();
        let result = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            Some(first_checkpoint.id),
            fp_registry,
        )
            .await;
        assert!(matches!(result, Err(SlateDBError::DatabaseAlreadyExists {..})));
        let clone_manifest_end = clone_manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(clone_manifest, clone_manifest_end);
        let wal_entries_end = load_wals(&object_store, &clone_path).await;
        assert_eq!(wal_entries, wal_entries_end);
    }
}
