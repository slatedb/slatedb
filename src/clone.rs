use crate::config::CheckpointOptions;
use crate::db::Db;
use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use crate::error::SlateDBError::CheckpointMissing;
use crate::manifest;
use crate::manifest_store::{ManifestStore, StoredManifest};
use object_store::path::Path;
use object_store::ObjectStore;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

impl Db {
    /// Clone a database. If no db already exists at the specified path, then this will create
    /// a new db under the path that is a clone of the db at parent_path.
    ///
    /// A clone is a shallow copy of the parent database - it starts with a manifest that
    /// references the same SSTs, but doesn't actually copy those SSTs, except for the WAL.
    /// New writes will be written to the newly created db and will not be reflected in the
    /// parent database.
    ///
    /// The clone can optionally be created from an existing checkpoint. If
    /// `parent_checkpoint` is present, then the referenced manifest is used
    /// as the base for the clone db's manifest. Otherwise, this method creates a new checkpoint
    /// for the current version of the parent db.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::admin;
    /// use slatedb::db::Db;
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::error::Error;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error>> {
    ///    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///    let db = Db::open("parent_path", Arc::clone(&object_store)).await?;
    ///    db.put(b"key", b"value").await?;
    ///    db.close().await?;
    ///
    ///     Db::create_clone(
    ///       "clone_path",
    ///       "parent_path",
    ///       object_store,
    ///       None,
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    pub async fn create_clone<P: Into<Path>>(
        clone_path: P,
        parent_path: P,
        object_store: Arc<dyn ObjectStore>,
        parent_checkpoint: Option<Uuid>,
    ) -> Result<(), Box<dyn Error>> {
        let clone_path = clone_path.into();
        let parent_path = parent_path.into();

        if clone_path == parent_path {
            return Err(Box::new(SlateDBError::InvalidArgument {
                msg: format!(
                    "Parent path '{}' must be different from the clone's path '{}'",
                    parent_path, clone_path
                ),
            }));
        }

        let clone_manifest_store =
            Arc::new(ManifestStore::new(&clone_path, Arc::clone(&object_store)));

        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, Arc::clone(&object_store)));

        let mut parent_manifest =
            Self::load_initialized_manifest(Arc::clone(&parent_manifest_store)).await?;

        let mut clone_manifest = Self::load_clone_manifest(
            Arc::clone(&clone_manifest_store),
            Arc::clone(&parent_manifest_store),
            &mut parent_manifest,
            &parent_path,
            &parent_checkpoint,
        )
        .await?;

        if !clone_manifest.db_state().initialized {
            Self::copy_wal_ssts(
                object_store.clone(),
                clone_manifest.db_state(),
                &parent_path,
                &clone_path,
            )
            .await?;

            let mut initialized_db_state = clone_manifest.db_state().clone();
            initialized_db_state.initialized = true;
            clone_manifest.update_db_state(initialized_db_state).await?;
        }

        Ok(())
    }

    async fn load_clone_manifest(
        clone_manifest_store: Arc<ManifestStore>,
        parent_manifest_store: Arc<ManifestStore>,
        parent_manifest: &mut StoredManifest,
        parent_path: &Path,
        parent_checkpoint_id: &Option<Uuid>,
    ) -> Result<StoredManifest, SlateDBError> {
        if let Some(clone_manifest) = StoredManifest::try_load(clone_manifest_store.clone()).await?
        {
            Self::validate_existing_manifest(
                &clone_manifest,
                parent_path,
                parent_manifest,
                parent_checkpoint_id,
            )?;
            return Ok(clone_manifest);
        }

        let source_checkpoint = if let Some(id) = parent_checkpoint_id {
            if let Some(checkpoint) = parent_manifest.db_state().find_checkpoint(id) {
                checkpoint.clone()
            } else {
                return Err(CheckpointMissing(*id));
            }
        } else {
            // If no checkpoint is provided, then create an ephemeral checkpoint from
            // the latest state. This will be used as the source checkpoint when we
            // write the final checkpoint. Making it ephemeral ensures that it will
            // get cleaned up if the clone operation fails.
            parent_manifest
                .write_new_checkpoint(&CheckpointOptions {
                    lifetime: Some(Duration::from_secs(300)),
                    source: *parent_checkpoint_id,
                })
                .await?
        };

        let final_checkpoint_id = Uuid::new_v4();
        let parent_db = manifest::DbLink {
            path: parent_path.to_string(),
            checkpoint_id: final_checkpoint_id,
        };

        let parent_checkpoint_manifest = if parent_manifest.id() == source_checkpoint.manifest_id {
            parent_manifest.manifest().clone()
        } else {
            parent_manifest_store
                .read_manifest(source_checkpoint.manifest_id)
                .await?
        };

        let clone_manifest = StoredManifest::load_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_db,
            &parent_checkpoint_manifest,
        )
        .await?;

        parent_manifest
            .write_checkpoint(
                final_checkpoint_id,
                &CheckpointOptions {
                    lifetime: None,
                    source: Some(source_checkpoint.id),
                },
            )
            .await?;

        Ok(clone_manifest)
    }

    fn validate_existing_manifest(
        clone_manifest: &StoredManifest,
        parent_path: &Path,
        parent_manifest: &StoredManifest,
        parent_checkpoint_id: &Option<Uuid>,
    ) -> Result<(), SlateDBError> {
        // The database is already initialized, so we just need to verify that
        // the parent information is consistent with the arguments passed to clone.
        let Some(parent_db) = &clone_manifest.manifest().parent else {
            return Err(SlateDBError::DatabaseAlreadyExists {
                msg: "Database exists, but is not attached to a parent database".to_string(),
            });
        };

        if parent_db.path != parent_path.to_string() {
            return Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!(
                    "Database exists, but is attached to a different parent with path {}",
                    parent_db.path
                ),
            });
        }

        let Some(actual_checkpoint) = parent_manifest
            .db_state()
            .find_checkpoint(&parent_db.checkpoint_id)
        else {
            return Err(SlateDBError::DatabaseAlreadyExists {
                msg: format!("Database exists, but the checkpoint with id {} is not present in the parent database at path {}",
                             parent_db.checkpoint_id, parent_db.path),
            });
        };

        if let Some(clone_checkpoint_id) = parent_checkpoint_id {
            let Some(expected_checkpoint) = parent_manifest
                .db_state()
                .find_checkpoint(clone_checkpoint_id)
            else {
                return Err(SlateDBError::DatabaseAlreadyExists {
                    msg: "Database ".to_string(),
                });
            };

            if expected_checkpoint.manifest_id != actual_checkpoint.manifest_id {
                return Err(SlateDBError::DatabaseAlreadyExists {
                    msg: "Database is already initialized, but refers to a different checkpoint"
                        .to_string(),
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
    ) -> Result<(), SlateDBError> {
        let mut wal_id = parent_checkpoint_state.last_compacted_wal_sst_id + 1;
        while wal_id < parent_checkpoint_state.next_wal_sst_id {
            let id = SsTableId::Wal(wal_id);
            let parent_path = id.path(parent_path);
            let clone_path = id.path(clone_path);
            object_store
                .as_ref()
                .copy_if_not_exists(&parent_path, &clone_path)
                .await?;
            wal_id += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{CheckpointOptions, CheckpointScope};
    use crate::db::Db;
    use crate::proptest_util::{rng, sample};
    use crate::test_utils;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_clone_latest_state() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::open(parent_path, Arc::clone(&object_store))
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db.close().await.unwrap();

        Db::create_clone(clone_path, parent_path, Arc::clone(&object_store), None)
            .await
            .unwrap();

        let clone_db = Db::open(clone_path, Arc::clone(&object_store))
            .await
            .unwrap();
        let mut db_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ordered_scan_in_range(&table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_clone_from_checkpoint() {
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

        Db::create_clone(
            clone_path.clone(),
            parent_path.clone(),
            Arc::clone(&object_store),
            Some(checkpoint.id),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path, Arc::clone(&object_store))
            .await
            .unwrap();
        let mut db_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ordered_scan_in_range(&checkpoint_table, .., &mut db_iter).await;
        clone_db.close().await.unwrap();
    }
}
