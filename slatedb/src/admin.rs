use crate::checkpoint::{Checkpoint, CheckpointCreateResult};
use crate::compactions_store::CompactionsStore;
use crate::compactor::{Compaction, CompactionSpec, Compactor, CompactorStateView};
use crate::compactor_state::VersionedCompactions;
use crate::compactor_state_protocols::CompactorStateReader;
use crate::config::{CheckpointOptions, GarbageCollectorOptions};
use crate::db::builder::GarbageCollectorBuilder;
use crate::db_state::VersionedManifest;
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::garbage_collector::GC_TASK_NAME;
use crate::manifest::store::{ManifestStore, StoredManifest};
use slatedb_common::clock::SystemClock;

use crate::clone;
use crate::db_status::ClosedResultWriter;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::rand::DbRand;
use crate::seq_tracker::FindOption;
use crate::utils::IdGenerator;
use crate::utils::WatchableOnceCell;
use chrono::{DateTime, Utc};
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use std::env;
use std::env::VarError;
use std::error::Error;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;
use uuid::Uuid;

pub use crate::db::builder::AdminBuilder;
use slatedb_txn_obj::TransactionalObject;

/// An Admin struct for SlateDB administration operations.
///
/// This struct provides methods for administrative functions such as
/// reading manifests, creating checkpoints, cloning databases, and
/// running garbage collection.
pub struct Admin {
    /// The path to the database.
    pub(crate) path: Path,
    /// The object stores to use for the main database and WAL.
    pub(crate) object_stores: ObjectStores,
    /// The system clock to use for operations.
    pub(crate) system_clock: Arc<dyn SystemClock>,
    /// The random number generator to use for randomness.
    pub(crate) rand: Arc<DbRand>,
    #[cfg(feature = "compaction_filters")]
    pub(crate) compaction_filter_supplier:
        Option<Arc<dyn crate::compaction_filter::CompactionFilterSupplier>>,
}

impl Admin {
    /// Read-only access to a specific or the latest manifest file.
    ///
    /// ## Arguments
    /// - `maybe_id`: Optional ID of the manifest file to read. If `None`, reads the latest.
    ///
    /// ## Returns
    /// - `Ok(Some(VersionedManifest))`: The manifest if found.
    /// - `Ok(None)`: If the manifest file does not exist.
    pub async fn read_manifest(
        &self,
        maybe_id: Option<u64>,
    ) -> Result<Option<VersionedManifest>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let manifest = if let Some(id) = maybe_id {
            manifest_store
                .try_read_manifest(id)
                .await?
                .map(|manifest| VersionedManifest {
                    id,
                    manifest: manifest.core,
                })
        } else {
            manifest_store
                .try_read_latest_manifest()
                .await?
                .map(|(id, manifest)| VersionedManifest {
                    id,
                    manifest: manifest.core,
                })
        };

        Ok(manifest)
    }

    /// List manifests within a range.
    ///
    /// ## Returns
    /// - `Ok(Vec<VersionedManifest>)`: The manifests in ascending ID order.
    pub async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<Vec<VersionedManifest>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let manifest_metadata = manifest_store.list_manifests(range).await?;
        let mut manifests = Vec::with_capacity(manifest_metadata.len());
        for metadata in manifest_metadata {
            let manifest = manifest_store.read_manifest(metadata.id).await?;
            manifests.push(VersionedManifest {
                id: metadata.id,
                manifest: manifest.core,
            });
        }
        Ok(manifests)
    }

    /// Read-only access to a specific or the latest compactions file.
    ///
    /// ## Arguments
    /// - `maybe_id`: Optional ID of the compactions file to read. If None, reads from the latest.
    ///
    /// ## Returns
    /// - `Ok(Some(VersionedCompactions))`: The compactions if found.
    /// - `Ok(None)`: If the compactions file does not exist.
    pub async fn read_compactions(
        &self,
        maybe_id: Option<u64>,
    ) -> Result<Option<VersionedCompactions>, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let compactions = if let Some(id) = maybe_id {
            compactions_store
                .try_read_compactions(id)
                .await?
                .map(|compactions| VersionedCompactions {
                    id,
                    compactions: compactions.core,
                })
        } else {
            compactions_store
                .try_read_latest_compactions()
                .await?
                .map(|(id, compactions)| VersionedCompactions {
                    id,
                    compactions: compactions.core,
                })
        };

        Ok(compactions)
    }

    /// Read-only access to a compaction by id from a specific or latest compactions file.
    ///
    /// ## Arguments
    /// - `compaction_id`: The ULID of the compaction to read.
    /// - `maybe_id`: Optional ID of the compactions file to read from. If None, reads from the latest.
    ///
    /// ## Returns
    /// - `Ok(Some(Compaction))`: The compaction if found.
    /// - `Ok(None)`: If the compactions file or compaction ID does not exist.
    pub async fn read_compaction(
        &self,
        compaction_id: Ulid,
        maybe_id: Option<u64>,
    ) -> Result<Option<Compaction>, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let compactions = if let Some(compactions_id) = maybe_id {
            compactions_store
                .try_read_compactions(compactions_id)
                .await?
        } else {
            compactions_store
                .try_read_latest_compactions()
                .await?
                .map(|(_id, compactions)| compactions)
        };
        let Some(compactions) = compactions else {
            return Ok(None);
        };
        let Some(compaction) = compactions.get(&compaction_id) else {
            return Ok(None);
        };

        Ok(Some(compaction.clone()))
    }

    /// Returns a read-only view of the current compactor state.
    pub async fn read_compactor_state_view(&self) -> Result<CompactorStateView, Box<dyn Error>> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let compactions_store = Arc::new(self.compactions_store());
        let reader = CompactorStateReader::new(&manifest_store, &compactions_store);
        Ok(reader.read_view().await?)
    }

    /// Generate a compaction from a spec and submit it.
    ///
    /// ## Returns
    /// - `Ok(Compaction)`: The submitted compaction.
    /// - `Err`: If there was an error during submission or reading the submitted compaction.
    pub async fn submit_compaction(
        &self,
        spec: CompactionSpec,
    ) -> Result<Compaction, Box<dyn Error>> {
        let compactions_store = Arc::new(self.compactions_store());
        let rand = Arc::new(DbRand::new(self.rand.rng().next_u64()));
        let compaction_id =
            Compactor::submit(spec, compactions_store, rand, self.system_clock.clone()).await?;
        let Some(compaction) = self.read_compaction(compaction_id, None).await? else {
            return Err(Box::new(SlateDBError::InvalidDBState));
        };

        Ok(compaction)
    }

    /// List compactions files within a range.
    ///
    /// ## Returns
    /// - `Ok(Vec<VersionedCompactions>)`: The compactions files in ascending ID order.
    pub async fn list_compactions<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<Vec<VersionedCompactions>, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let compactions_metadata = compactions_store.list_compactions(range).await?;
        let mut compactions = Vec::with_capacity(compactions_metadata.len());
        for metadata in compactions_metadata {
            let stored_compactions = compactions_store.read_compactions(metadata.id).await?;
            compactions.push(VersionedCompactions {
                id: metadata.id,
                compactions: stored_compactions.core,
            });
        }
        Ok(compactions)
    }

    /// List checkpoints, optionally filtering by name. When name is provided, only checkpoints
    /// with this exact name will be returned.
    ///
    /// # Arguments
    ///
    /// * `name_filter`: Name that will be used to filter checkpoints.
    pub async fn list_checkpoints(
        &self,
        name_filter: Option<&str>,
    ) -> Result<Vec<Checkpoint>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let (_, manifest) = manifest_store.read_latest_manifest().await?;

        let checkpoints = match name_filter {
            Some("") => manifest
                .core
                .checkpoints
                .into_iter()
                .filter(|cp| cp.name.as_deref() == Some("") || cp.name.is_none())
                .collect(),
            Some(name) => manifest
                .core
                .checkpoints
                .into_iter()
                .filter(|cp| cp.name.as_deref() == Some(name))
                .collect(),
            None => manifest.core.checkpoints,
        };

        Ok(checkpoints)
    }

    /// Run the garbage collector once in the foreground.
    ///
    /// This function runs the garbage collector letting Tokio decide when to run the task.
    ///
    /// # Arguments
    ///
    /// * `gc_opts`: The garbage collector options.
    ///
    pub async fn run_gc_once(
        &self,
        gc_opts: GarbageCollectorOptions,
    ) -> Result<(), Box<dyn Error>> {
        let gc = GarbageCollectorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_wal_object_store(self.object_stores.store_of(ObjectStoreType::Wal).clone())
        .with_options(gc_opts)
        .with_seed(self.rand.rng().next_u64())
        .build();
        gc.run_gc_once().await;
        Ok(())
    }

    /// Run the garbage collector in the background.
    ///
    /// This function runs the garbage collector in a Tokio background task.
    ///
    /// # Arguments
    ///
    /// * `gc_opts`: The garbage collector options.
    ///
    pub async fn run_gc(&self, gc_opts: GarbageCollectorOptions) -> Result<(), crate::Error> {
        let gc = GarbageCollectorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_wal_object_store(self.object_stores.store_of(ObjectStoreType::Wal).clone())
        .with_options(gc_opts)
        .with_seed(self.rand.rng().next_u64())
        .build();

        let (_, rx) = async_channel::unbounded();
        let closed_result: Arc<dyn ClosedResultWriter> = Arc::new(WatchableOnceCell::new());
        let task_executor = MessageHandlerExecutor::new(closed_result, self.system_clock.clone());

        task_executor
            .add_handler(
                GC_TASK_NAME.to_string(),
                Box::new(gc),
                rx,
                &Handle::current(),
            )
            .map_err(Into::<crate::Error>::into)?;

        task_executor
            .join_task(GC_TASK_NAME)
            .await
            .map_err(Into::<crate::Error>::into)
    }

    /// Run the compactor in the foreground until the provided cancellation token is cancelled.
    ///
    /// This method blocks until `cancellation_token` is cancelled, at which point it requests a
    /// graceful shutdown and waits for the compactor to stop.
    ///
    /// To use compaction filters with the standalone compactor, configure the `AdminBuilder`
    /// with [`AdminBuilder::with_compaction_filter_supplier`] before building.
    pub async fn run_compactor(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<(), crate::Error> {
        #[allow(unused_mut)]
        let mut builder = crate::CompactorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_seed(self.rand.rng().next_u64());

        #[cfg(feature = "compaction_filters")]
        if let Some(supplier) = &self.compaction_filter_supplier {
            builder = builder.with_compaction_filter_supplier(supplier.clone());
        }

        let compactor = builder.build();

        let mut run_task = tokio::spawn({
            let compactor = compactor.clone();
            async move { compactor.run().await }
        });

        tokio::select! {
            result = &mut run_task => {
                return match result {
                    Ok(inner) => inner,
                    Err(join_err) => Err(crate::Error::internal("compactor task failed".to_string()).with_source(Box::new(join_err))),
                };
            }
            _ = cancellation_token.cancelled() => {
                // fall through to shutdown logic
            }
        }

        compactor.stop().await
    }

    /// Creates a checkpoint of the db stored in the object store at the specified path using the
    /// provided options. The checkpoint will reference the current active manifest of the db. This
    /// method does not flush writer memtables or WALs before creating the checkpoint. You will be
    /// responsible for refreshing checkpoints periodically.
    ///
    /// If you have a [`crate::Db`] instance open, you can use the [`crate::Db::create_checkpoint`]
    /// method instead. That method will flush the memtables and WALs before creating the checkpoint.
    ///
    /// If you're using a [`crate::DbReader`], you might wish to have the reader manage the checkpoint
    /// for you by calling [`crate::DbReader::open`] with no `checkpoint_id` set. The reader will
    /// create a checkpoint for you and periodically refresh it.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::admin::{Admin, AdminBuilder};
    /// use slatedb::config::CheckpointOptions;
    /// use slatedb::Db;
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
    ///    let admin = AdminBuilder::new("parent_path", object_store).build();
    ///    let _ = admin.create_detached_checkpoint(
    ///      &CheckpointOptions::default(),
    ///    ).await?;
    ///
    ///    Ok(())
    /// }
    /// ```
    pub async fn create_detached_checkpoint(
        &self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;

        let manifest_has_wal = stored_manifest.db_state().wal_object_store_uri.is_some();
        if self.object_stores.has_wal_object_store() != manifest_has_wal {
            return Err(SlateDBError::WalStoreReconfigurationError.into());
        }

        let checkpoint_id = self.rand.rng().gen_uuid();
        let checkpoint = stored_manifest
            .write_checkpoint(checkpoint_id, options)
            .await?;
        Ok(CheckpointCreateResult {
            id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        })
    }

    /// Refresh the lifetime of an existing checkpoint. Takes the id of an existing checkpoint
    /// and a lifetime, and sets the lifetime of the checkpoint to the specified lifetime. If
    /// there is no checkpoint with the specified id, then this fn fails with
    /// SlateDBError::InvalidDbState
    pub async fn refresh_checkpoint(
        &self,
        id: Uuid,
        lifetime: Option<Duration>,
    ) -> Result<(), crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;
        stored_manifest
            .maybe_apply_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty()?;
                let expire_time = lifetime.map(|l| self.system_clock.now() + l);
                let Some(_) = dirty.value.core.checkpoints.iter_mut().find_map(|c| {
                    if c.id == id {
                        c.expire_time = expire_time;
                        return Some(());
                    }
                    None
                }) else {
                    return Err(SlateDBError::InvalidDBState);
                };
                Ok(Some(dirty))
            })
            .await
            .map_err(Into::into)
    }

    /// Deletes the checkpoint with the specified id.
    pub async fn delete_checkpoint(&self, id: Uuid) -> Result<(), crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;
        stored_manifest
            .maybe_apply_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty()?;
                let checkpoints: Vec<Checkpoint> = dirty
                    .value
                    .core
                    .checkpoints
                    .iter()
                    .filter(|c| c.id != id)
                    .cloned()
                    .collect();
                dirty.value.core.checkpoints = checkpoints;
                Ok(Some(dirty))
            })
            .await
            .map_err(Into::into)
    }

    /// Returns the timestamp or sequence from the latest manifest's sequence tracker.
    /// When `round_up` is true, uses the next higher value; otherwise the previous one.
    pub async fn get_timestamp_for_sequence(
        &self,
        seq: u64,
        round_up: bool,
    ) -> Result<Option<DateTime<Utc>>, crate::Error> {
        let manifest_store = self.manifest_store();

        let id_manifest = manifest_store.try_read_latest_manifest().await?;
        let Some((_id, manifest)) = id_manifest else {
            return Ok(None);
        };

        let opt = if round_up {
            FindOption::RoundUp
        } else {
            FindOption::RoundDown
        };
        Ok(manifest.core.sequence_tracker.find_ts(seq, opt))
    }

    /// Returns the sequence for a given timestamp from the latest manifest's sequence tracker.
    /// When `round_up` is true, uses the next higher value; otherwise the previous one.
    pub async fn get_sequence_for_timestamp(
        &self,
        ts: DateTime<Utc>,
        round_up: bool,
    ) -> Result<Option<u64>, crate::Error> {
        let manifest_store = self.manifest_store();

        let id_manifest = manifest_store.try_read_latest_manifest().await?;
        let Some((_id, manifest)) = id_manifest else {
            return Ok(None);
        };

        let opt = if round_up {
            FindOption::RoundUp
        } else {
            FindOption::RoundDown
        };
        Ok(manifest.core.sequence_tracker.find_seq(ts, opt))
    }

    fn manifest_store(&self) -> ManifestStore {
        ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
    }

    fn compactions_store(&self) -> CompactionsStore {
        CompactionsStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
    }

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
    /// use slatedb::admin::{Admin, AdminBuilder};
    /// use slatedb::Db;
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
    ///    let admin = AdminBuilder::new("clone_path", object_store).build();
    ///    admin.create_clone(
    ///      "parent_path",
    ///      None,
    ///    ).await?;
    ///
    ///    Ok(())
    /// }
    /// ```
    pub async fn create_clone<P: Into<Path>>(
        &self,
        parent_path: P,
        parent_checkpoint: Option<Uuid>,
    ) -> Result<(), Box<dyn Error>> {
        clone::create_clone(
            self.path.clone(),
            parent_path.into(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
            self.object_stores.store_of(ObjectStoreType::Wal).clone(),
            parent_checkpoint,
            Arc::new(FailPointRegistry::new()),
            self.system_clock.clone(),
            self.rand.clone(),
        )
        .await?;
        Ok(())
    }

    /// Creates a new builder for an admin client at the given path.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `object_store`: the object store to use for the database
    ///
    /// ## Returns
    /// - `AdminBuilder`: the builder to initialize the admin client
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::admin::Admin;
    /// use slatedb::object_store::memory::InMemory;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let object_store = Arc::new(InMemory::new());
    ///     let admin = Admin::builder("/tmp/test_db", object_store).build();
    /// }
    /// ```
    pub fn builder<P: Into<Path>>(path: P, object_store: Arc<dyn ObjectStore>) -> AdminBuilder<P> {
        AdminBuilder::new(path, object_store)
    }
}

fn get_env_variable(name: &str) -> Result<String, SlateDBError> {
    env::var(name).map_err(|e| match e {
        VarError::NotPresent => SlateDBError::UndefinedEnvironmentVariable {
            key: name.to_string(),
        },
        VarError::NotUnicode(not_unicode_value) => SlateDBError::InvalidEnvironmentVariable {
            key: name.to_string(),
            value: format!("{:?}", not_unicode_value),
        },
    })
}

/// Loads an object store from configured environment variables.
/// The provider is specified using the CLOUD_PROVIDER variable.
/// For specific provider configurations, see the corresponding
/// method documentation:
///
/// | Provider | Value | Documentation |
/// |----------|-------|---------------|
/// | Local | `local` | [load_local] |
/// | Memory | `memory` | [load_memory] |
/// | AWS | `aws` | [load_aws] |
/// | Azure | `azure` | [load_azure] |
/// | OpenDAL | `opendal` | [load_opendal] |
pub fn load_object_store_from_env(
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    dotenvy::from_filename(env_file.unwrap_or(String::from(".env"))).ok();
    let cloud_provider = get_env_variable("CLOUD_PROVIDER")?;
    match cloud_provider.to_lowercase().as_str() {
        "local" => load_local(),
        "memory" => load_memory(),
        #[cfg(feature = "aws")]
        "aws" => load_aws(),
        #[cfg(feature = "azure")]
        "azure" => load_azure(),
        #[cfg(feature = "opendal")]
        "opendal" => load_opendal(),
        invalid_value => Err(SlateDBError::InvalidEnvironmentVariable {
            key: "CLOUD_PROVIDER".to_string(),
            value: invalid_value.to_string(),
        }
        .into()),
    }
}

/// Loads a local object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | LOCAL_PATH | The path to the local directory where all data will be stored | Yes |
pub fn load_local() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let local_path = get_env_variable("LOCAL_PATH")?;
    let lfs = object_store::local::LocalFileSystem::new_with_prefix(local_path)?;
    Ok(Arc::new(lfs) as Arc<dyn ObjectStore>)
}

/// Loads an in-memory object store instance.
pub fn load_memory() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    Ok(Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>)
}

/// Loads an AWS S3 Object store instance. The environment variables consumed are
/// the same as those supported by [`AmazonS3Builder::from_env`]. Refer to the
/// builder documentation for the full list and meaning of supported variables:
/// <https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.with_config>
#[cfg(feature = "aws")]
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use object_store::aws::S3ConditionalPut;

    let builder = object_store::aws::AmazonS3Builder::from_env()
        .with_conditional_put(S3ConditionalPut::ETagMatch);

    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an Azure Object store instance. The environment variables consumed are
/// the same as those supported by [`MicrosoftAzureBuilder::from_env`]. Refer to
/// the builder documentation for the full list and meaning of supported variables:
/// <https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html#method.with_config>
#[cfg(feature = "azure")]
pub fn load_azure() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let builder = object_store::azure::MicrosoftAzureBuilder::from_env();
    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an OpenDAL Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | OPENDAL_SCHEME | The OpenDAL scheme to use | Yes |
/// | OPENDAL_* | The OpenDAL configuration | Yes |
/// full list of schemes: https://docs.rs/opendal/latest/opendal/enum.Scheme.html
/// for example, to use s3-compatible storage, you can set:
/// ```bash
/// OPENDAL_SCHEME=s3
/// OPENDAL_ENDPOINT=http://localhost:9000
/// OPENDAL_ACCESS_KEY_ID=minioadmin
/// OPENDAL_SECRET_ACCESS_KEY=minioadmin
/// OPENDAL_BUCKET=test
/// OPENDAL_REGION=us-east-1
/// OPENDAL_ROOT=/tmp
/// ```
/// full list of config: https://docs.rs/opendal/latest/opendal/services/s3/config/struct.S3Config.html
/// for example, to use oss, you can set:
/// ```bash
/// OPENDAL_SCHEME=oss
/// OPENDAL_ENDPOINT=http://oss-cn-shanghai.aliyuncs.com
/// OPENDAL_ACCESS_KEY_ID=your-access-key-id
/// OPENDAL_ACCESS_KEY_SECRET=your-access-key-secret
/// OPENDAL_BUCKET=your-bucket-name
/// OPENDAL_ROOT=/your/root/path
/// ```
/// full list of config: https://docs.rs/opendal/latest/opendal/services/oss/config/struct.OssConfig.html
#[cfg(feature = "opendal")]
pub fn load_opendal() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use opendal::{Operator, Scheme};
    use std::collections::HashMap;
    use std::str::FromStr;

    let scheme =
        Scheme::from_str(&env::var("OPENDAL_SCHEME").expect("OPENDAL_SCHEME must be set"))?;
    let iter = env::vars()
        .filter_map(|(k, v)| k.strip_prefix("OPENDAL_").map(|k| (k.to_lowercase(), v)))
        .collect::<HashMap<String, String>>();

    let op = Operator::via_iter(scheme, iter)?;
    Ok(Arc::new(object_store_opendal::OpendalStore::new(op)) as Arc<dyn ObjectStore>)
}

#[cfg(test)]
mod tests {
    use crate::admin::{load_object_store_from_env, AdminBuilder};
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use crate::compactor_state::{Compaction, CompactionSpec, CompactionStatus, SourceId};
    use crate::db_state::ManifestCore;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use std::sync::Arc;
    use ulid::Ulid;

    #[test]
    fn test_load_object_store_from_env() {
        figment::Jail::expect_with(|jail| {
            // creating an object store without CLOUD_PROVIDER env variable
            let r = load_object_store_from_env(None);
            assert!(r.is_err());
            assert_eq!(
                r.unwrap_err().to_string(),
                "undefined environment variable CLOUD_PROVIDER"
            );

            jail.create_file("invalid.env", "CLOUD_PROVIDER=invalid")
                .expect("failed to create temp env file");
            let r = load_object_store_from_env(Some("invalid.env".to_string()));
            assert!(r.is_err());
            assert_eq!(
                r.unwrap_err().to_string(),
                "invalid environment variable CLOUD_PROVIDER value `invalid`"
            );
            // unset since the environment variable loaded in from invalid.env
            // takes precedence over the memory.env file.
            std::env::remove_var("CLOUD_PROVIDER");

            jail.create_file("memory.env", "CLOUD_PROVIDER=memory")
                .expect("failed to create temp env file");
            let r = load_object_store_from_env(Some("memory.env".to_string()));
            let store = r.expect("expected memory object store");
            assert_eq!(store.to_string(), "InMemory");

            Ok(())
        });
    }

    #[tokio::test]
    async fn test_admin_read_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_read_manifest");
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut stored = StoredManifest::create_new_db(
            manifest_store,
            ManifestCore::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.core.next_wal_sst_id = 17;
        dirty.value.core.last_l0_seq = 9;
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();

        let latest = admin
            .read_manifest(None)
            .await
            .unwrap()
            .expect("expected manifest");
        assert_eq!(latest.id, 2);
        assert_eq!(latest.manifest.next_wal_sst_id, 17);
        assert_eq!(latest.manifest.last_l0_seq, 9);

        let first = admin
            .read_manifest(Some(1))
            .await
            .unwrap()
            .expect("expected manifest");
        assert_eq!(first.id, 1);
        assert_eq!(first.manifest.next_wal_sst_id, 1);
        assert_eq!(first.manifest.last_l0_seq, 0);
    }

    #[tokio::test]
    async fn test_admin_list_manifests() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_list_manifests");
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut stored = StoredManifest::create_new_db(
            manifest_store,
            ManifestCore::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.core.next_wal_sst_id = 5;
        dirty.value.core.last_l0_seq = 10;
        stored.update(dirty).await.unwrap();

        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.core.next_wal_sst_id = 8;
        dirty.value.core.last_l0_seq = 20;
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();

        let all = admin.list_manifests(..).await.unwrap();
        assert_eq!(
            all.iter().map(|manifest| manifest.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            all.iter()
                .map(|manifest| manifest.manifest.last_l0_seq)
                .collect::<Vec<_>>(),
            vec![0, 10, 20]
        );

        let bounded = admin.list_manifests(2..3).await.unwrap();
        assert_eq!(
            bounded
                .iter()
                .map(|manifest| manifest.id)
                .collect::<Vec<_>>(),
            vec![2]
        );

        let left_bounded = admin.list_manifests(2..).await.unwrap();
        assert_eq!(
            left_bounded
                .iter()
                .map(|manifest| manifest.id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        let right_bounded = admin.list_manifests(..3).await.unwrap();
        assert_eq!(
            right_bounded
                .iter()
                .map(|manifest| manifest.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn test_admin_read_compactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_read_compactions");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 7)
            .await
            .unwrap();

        let compaction_id = Ulid::new();
        let compaction = Compaction::new(
            compaction_id,
            CompactionSpec::new(vec![SourceId::SortedRun(3)], 7),
        );
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();

        let latest = admin
            .read_compactions(None)
            .await
            .unwrap()
            .expect("expected compactions");
        assert_eq!(latest.id, 2);
        assert_eq!(
            latest
                .compactions
                .recent_compactions()
                .map(|compaction| compaction.id())
                .collect::<Vec<_>>(),
            vec![compaction_id]
        );

        let first = admin
            .read_compactions(Some(1))
            .await
            .unwrap()
            .expect("expected compactions");
        assert_eq!(first.id, 1);
        assert_eq!(first.compactions.recent_compactions().count(), 0);
    }

    #[tokio::test]
    async fn test_admin_list_compactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_list_compactions");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();

        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(3)], 7),
        ));
        stored.update(dirty).await.unwrap();

        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(5)], 9),
        ));
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let listed = admin.list_compactions(..).await.unwrap();
        let ids: Vec<u64> = listed.iter().map(|compactions| compactions.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(
            listed
                .iter()
                .map(|compactions| compactions.compactions.recent_compactions().count())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        let bounded = admin.list_compactions(2..3).await.unwrap();
        assert_eq!(
            bounded
                .iter()
                .map(|compactions| compactions.id)
                .collect::<Vec<_>>(),
            vec![2]
        );

        let left_bounded = admin.list_compactions(2..).await.unwrap();
        assert_eq!(
            left_bounded
                .iter()
                .map(|compactions| compactions.id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        let right_bounded = admin.list_compactions(..3).await.unwrap();
        assert_eq!(
            right_bounded
                .iter()
                .map(|compactions| compactions.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn test_admin_read_compaction() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_read_compaction");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();

        let compaction_id = Ulid::new();
        let compaction = Compaction::new(
            compaction_id,
            CompactionSpec::new(vec![SourceId::SortedRun(3)], 7),
        );
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let compaction = admin
            .read_compaction(compaction_id, None)
            .await
            .unwrap()
            .expect("expected compaction");
        assert_eq!(compaction.id(), compaction_id);
    }

    #[tokio::test]
    async fn test_admin_submit_compaction() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_submit_compaction");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(3)], 3);
        let compaction = admin.submit_compaction(spec).await.unwrap();

        assert_eq!(compaction.spec().destination(), 3);
        assert_eq!(compaction.spec().sources(), &vec![SourceId::SortedRun(3)]);
        assert_eq!(compaction.status(), CompactionStatus::Submitted);
    }

    #[cfg(feature = "compaction_filters")]
    #[test]
    fn test_admin_builder_with_compaction_filter_supplier() {
        use crate::compaction_filter::{
            CompactionFilter, CompactionFilterDecision, CompactionFilterError,
            CompactionFilterSupplier, CompactionJobContext,
        };
        use crate::types::RowEntry;

        struct NoopFilter;

        #[async_trait::async_trait]
        impl CompactionFilter for NoopFilter {
            async fn filter(
                &mut self,
                _entry: &RowEntry,
            ) -> Result<CompactionFilterDecision, CompactionFilterError> {
                Ok(CompactionFilterDecision::Keep)
            }
            async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
                Ok(())
            }
        }

        struct NoopFilterSupplier;

        #[async_trait::async_trait]
        impl CompactionFilterSupplier for NoopFilterSupplier {
            async fn create_compaction_filter(
                &self,
                _context: &CompactionJobContext,
            ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
                Ok(Box::new(NoopFilter))
            }
        }

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let admin = AdminBuilder::new("/tmp/test_filter_supplier", object_store)
            .with_compaction_filter_supplier(Arc::new(NoopFilterSupplier))
            .build();

        assert!(admin.compaction_filter_supplier.is_some());
    }
}
