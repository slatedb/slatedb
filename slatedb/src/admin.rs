use crate::checkpoint::{Checkpoint, CheckpointCreateResult};
use crate::clock::SystemClock;
use crate::config::{CheckpointOptions, GarbageCollectorOptions};
use crate::db::builder::GarbageCollectorBuilder;
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::garbage_collector::GC_TASK_NAME;
use crate::manifest::store::{ManifestStore, StoredManifest};

use crate::clone;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::rand::DbRand;
use crate::seq_tracker::FindOption;
use crate::utils::{IdGenerator, WatchableOnceCell};
use chrono::{DateTime, Utc};
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::error::Error;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use uuid::Uuid;

pub use crate::db::builder::AdminBuilder;

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
}

impl Admin {
    /// Read-only access to the latest manifest file
    pub async fn read_manifest(
        &self,
        maybe_id: Option<u64>,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
            self.system_clock.clone(),
        );
        let id_manifest = if let Some(id) = maybe_id {
            manifest_store
                .try_read_manifest(id)
                .await?
                .map(|manifest| (id, manifest))
        } else {
            manifest_store.try_read_latest_manifest().await?
        };

        match id_manifest {
            None => Ok(None),
            Some(result) => Ok(Some(serde_json::to_string(&result)?)),
        }
    }

    /// List manifests within a range
    pub async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<String, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
            self.system_clock.clone(),
        );
        let manifests = manifest_store.list_manifests(range).await?;
        Ok(serde_json::to_string(&manifests)?)
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
            self.system_clock.clone(),
        );
        let (_, manifest) = manifest_store.read_latest_manifest().await?;

        let checkpoints = if let Some(_x) = name_filter {
            manifest
                .core
                .checkpoints
                .into_iter()
                .filter(|cp| cp.name.as_deref() == name_filter)
                .collect()
        } else {
            manifest.core.checkpoints
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
        .build();

        let (_, rx) = mpsc::unbounded_channel();
        let closed_result = WatchableOnceCell::new();
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
            self.system_clock.clone(),
        ));
        manifest_store
            .validate_no_wal_object_store_configured()
            .await?;
        let mut stored_manifest = StoredManifest::load(manifest_store).await?;
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
            self.system_clock.clone(),
        ));
        let mut stored_manifest = StoredManifest::load(manifest_store).await?;
        stored_manifest
            .maybe_apply_manifest_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty();
                let expire_time = lifetime.map(|l| self.system_clock.now() + l);
                let Some(_) = dirty.core.checkpoints.iter_mut().find_map(|c| {
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
            self.system_clock.clone(),
        ));
        let mut stored_manifest = StoredManifest::load(manifest_store).await?;
        stored_manifest
            .maybe_apply_manifest_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty();
                let checkpoints: Vec<Checkpoint> = dirty
                    .core
                    .checkpoints
                    .iter()
                    .filter(|c| c.id != id)
                    .cloned()
                    .collect();
                dirty.core.checkpoints = checkpoints;
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
            self.system_clock.clone(),
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

/// Loads an object store from configured environment variables.
/// The provider is specified using the CLOUD_PROVIDER variable.
/// For specific provider configurations, see the corresponding
/// method documentation:
///
/// | Provider | Value | Documentation |
/// |----------|-------|---------------|
/// | Local | `local` | [load_local] |
/// | AWS | `aws` | [load_aws] |
/// | Azure | `azure` | [load_azure] |
/// | OpenDAL | `opendal` | [load_opendal] |
pub fn load_object_store_from_env(
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    dotenvy::from_filename(env_file.unwrap_or(String::from(".env"))).ok();

    let provider = &*env::var("CLOUD_PROVIDER")
        .expect("CLOUD_PROVIDER must be set")
        .to_lowercase();

    match provider {
        "local" => load_local(),
        #[cfg(feature = "aws")]
        "aws" => load_aws(),
        #[cfg(feature = "azure")]
        "azure" => load_azure(),
        #[cfg(feature = "opendal")]
        "opendal" => load_opendal(),
        _ => Err(format!("Unknown CLOUD_PROVIDER: '{}'", provider).into()),
    }
}

/// Loads a local object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | LOCAL_PATH | The path to the local directory where all data will be stored | Yes |
pub fn load_local() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let local_path = env::var("LOCAL_PATH").expect("LOCAL_PATH must be set");
    let lfs = object_store::local::LocalFileSystem::new_with_prefix(local_path)?;
    Ok(Arc::new(lfs) as Arc<dyn ObjectStore>)
}

/// Loads an AWS S3 Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | AWS_ACCESS_KEY_ID | The access key for a role with permissions to access the store | No |
/// | AWS_SECRET_ACCESS_KEY | The access key secret for the above ID | No |
/// | AWS_SESSION_TOKEN | The session token for the above ID | No |
/// | AWS_BUCKET | The bucket to use within S3 | Yes |
/// | AWS_REGION | The AWS region to use | Yes |
/// | AWS_ENDPOINT | The endpoint to use for S3 (disables https) | No |
#[cfg(feature = "aws")]
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use object_store::aws::S3ConditionalPut;

    // Mandatory environment variables
    let bucket = env::var("AWS_BUCKET").expect("AWS_BUCKET must be set");
    let region = env::var("AWS_REGION").expect("AWS_REGION must be set");

    // Optional environment variables (credentials / session token)
    let key = env::var("AWS_ACCESS_KEY_ID").ok();
    let secret = env::var("AWS_SECRET_ACCESS_KEY").ok();
    let session_token = env::var("AWS_SESSION_TOKEN").ok();
    let endpoint = env::var("AWS_ENDPOINT").ok();

    // Start building the S3 object store builder with required params.
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_bucket_name(bucket)
        .with_region(region);

    // If explicit credentials are supplied, configure them; otherwise rely on the AWS SDK
    // default credential provider chain (which covers IMDS / IRSA).
    if let (Some(access_key), Some(secret_key)) = (key, secret) {
        builder = builder
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);

        if let Some(token) = session_token {
            builder = builder.with_token(token);
        }
    }

    let builder = if let Some(endpoint) = endpoint {
        builder.with_allow_http(true).with_endpoint(endpoint)
    } else {
        builder
    };

    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an Azure Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | AZURE_ACCOUNT | The azure storage account name | Yes |
/// | AZURE_KEY | The azure storage account key| Yes |
/// | AZURE_CONTAINER | The storage container name| Yes |
#[cfg(feature = "azure")]
pub fn load_azure() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let account = env::var("AZURE_ACCOUNT").expect("AZURE_ACCOUNT must be set");
    let key = env::var("AZURE_KEY").expect("AZURE_KEY must be set");
    let container = env::var("AZURE_CONTAINER").expect("AZURE_CONTAINER must be set");
    let builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_access_key(key)
        .with_container_name(container);
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
