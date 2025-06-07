use crate::checkpoint::{Checkpoint, CheckpointCreateResult};
use crate::clock::SystemClock;
use crate::config::{CheckpointOptions, GarbageCollectorOptions};
use crate::error::SlateDBError;
use crate::garbage_collector::stats::GcStats;
use crate::garbage_collector::GarbageCollector;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::sst::SsTableFormat;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;

use crate::clone;
use crate::object_stores::ObjectStores;
use fail_parallel::FailPointRegistry;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::error::Error;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// read-only access to the latest manifest file
pub async fn read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    maybe_id: Option<u64>,
) -> Result<Option<String>, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
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

pub async fn list_manifests<R: RangeBounds<u64>>(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    range: R,
) -> Result<String, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    let manifests = manifest_store.list_manifests(range).await?;
    Ok(serde_json::to_string(&manifests)?)
}

pub async fn list_checkpoints(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<Checkpoint>, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    let (_, manifest) = manifest_store.read_latest_manifest().await?;
    Ok(manifest.core.checkpoints)
}

/// Deletes all objects with the specified prefix. This includes all
/// "subdirectories" objects, since object stores are not hierarchical.
pub async fn delete_objects_with_prefix(
    object_store: Arc<dyn ObjectStore>,
    maybe_prefix: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let stream = object_store
        .list(maybe_prefix)
        .map_ok(|m| m.location)
        .boxed();
    object_store
        .delete_stream(stream)
        .try_collect::<Vec<Path>>()
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

/// Loads an object store from configured environment variables.
/// The provider is specified using the CLOUD_PROVIDER variable.
/// For specific provider configurations, see the corresponding
/// method documentation:
///
/// | Provider | Value | Documentation |
/// |----------|-------|---------------|
/// | AWS | `aws` | [load_aws] |
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
        _ => Err(format!("Unknown CLOUD_PROVIDER: '{}'", provider).into()),
    }
}

/// Run the garbage collector once in the foreground.
///
/// This function runs the garbage collector letting Tokio decide when to run the task.
///
/// # Arguments
///
/// * `path`: The path to the database.
/// * `object_store`: The object store to use.
/// * `gc_opts`: The garbage collector options.
///
pub async fn run_gc_once(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    gc_opts: GarbageCollectorOptions,
) -> Result<(), Box<dyn Error>> {
    let manifest_store = Arc::new(ManifestStore::new(path, object_store.clone()));
    manifest_store
        .validate_no_wal_object_store_configured()
        .await?;
    let sst_format = SsTableFormat::default(); // read only SSTs, can use default
    let table_store = Arc::new(TableStore::new(
        ObjectStores::new(object_store.clone(), None),
        sst_format.clone(),
        path.clone(),
        None, // no need for cache in GC
    ));

    let stats = Arc::new(StatRegistry::new());
    GarbageCollector::run_gc_once(manifest_store, table_store, stats, gc_opts).await;
    Ok(())
}

/// Run the garbage collector in the background.
///
/// This function runs the garbage collector in a Tokio background task.
///
/// # Arguments
///
/// * `path`: The path to the database.
/// * `object_store`: The object store to use.
/// * `gc_opts`: The garbage collector options.
/// * `cancellation_token`: The cancellation token to stop the garbage collector.
pub async fn run_gc_in_background(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    gc_opts: GarbageCollectorOptions,
    cancellation_token: CancellationToken,
    system_clock: Arc<dyn SystemClock>,
) -> Result<(), Box<dyn Error>> {
    let manifest_store = Arc::new(ManifestStore::new(path, object_store.clone()));
    manifest_store
        .validate_no_wal_object_store_configured()
        .await?;
    let sst_format = SsTableFormat::default(); // read only SSTs, can use default
    let table_store = Arc::new(TableStore::new(
        ObjectStores::new(object_store.clone(), None),
        sst_format.clone(),
        path.clone(),
        None, // no need for cache in GC
    ));

    let stats = Arc::new(GcStats::new(Arc::new(StatRegistry::new())));

    let tracker = TaskTracker::new();
    let ct = cancellation_token.clone();

    tracker.spawn(GarbageCollector::start_async_task(
        manifest_store,
        table_store,
        stats,
        ct,
        gc_opts,
        system_clock,
    ));
    tracker.close();

    tracker.wait().await;
    Ok(())
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
/// | AWS_ACCESS_KEY_ID | The access key for a role with permissions to access the store | Yes |
/// | AWS_SECRET_ACCESS_KEY | The access key secret for the above ID | Yes |
/// | AWS_SESSION_TOKEN | The session token for the above ID | No |
/// | AWS_BUCKET | The bucket to use within S3 | Yes |
/// | AWS_REGION | The AWS region to use | Yes |
/// | AWS_ENDPOINT | The endpoint to use for S3 (disables https) | No |
/// | AWS_DYNAMODB_TABLE | The DynamoDB table to use for locking | No |
#[cfg(feature = "aws")]
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use object_store::aws::{DynamoCommit, S3ConditionalPut};

    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret =
        env::var("AWS_SECRET_ACCESS_KEY").expect("Expected AWS_SECRET_ACCESS_KEY must be set");
    let session_token = env::var("AWS_SESSION_TOKEN").ok();
    let bucket = env::var("AWS_BUCKET").expect("AWS_BUCKET must be set");
    let region = env::var("AWS_REGION").expect("AWS_REGION must be set");
    let endpoint = env::var("AWS_ENDPOINT").ok();
    let dynamodb_table = env::var("AWS_DYNAMODB_TABLE").ok();
    let builder = object_store::aws::AmazonS3Builder::new()
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .with_bucket_name(bucket)
        .with_region(region);

    let builder = if let Some(token) = session_token {
        builder.with_token(token)
    } else {
        builder
    };

    let builder = if let Some(dynamodb_table) = dynamodb_table {
        builder.with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new(dynamodb_table)))
    } else {
        builder.with_conditional_put(S3ConditionalPut::ETagMatch)
    };

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

/// Creates a checkpoint of the db stored in the object store at the specified path using the
/// provided options. The checkpoint will reference the current active manifest of the db.
///
/// # Examples
///
/// ```
/// use slatedb::admin;
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
///    let _ = admin::create_checkpoint(
///      "parent_path",
///      object_store,
///      &CheckpointOptions::default(),
///    ).await?;
///
///    Ok(())
/// }
/// ```
pub async fn create_checkpoint<P: Into<Path>>(
    path: P,
    object_store: Arc<dyn ObjectStore>,
    options: &CheckpointOptions,
) -> Result<CheckpointCreateResult, SlateDBError> {
    let manifest_store = Arc::new(ManifestStore::new(&path.into(), object_store));
    manifest_store
        .validate_no_wal_object_store_configured()
        .await?;
    let mut stored_manifest = StoredManifest::load(manifest_store).await?;
    let checkpoint = stored_manifest.write_checkpoint(None, options).await?;
    Ok(CheckpointCreateResult {
        id: checkpoint.id,
        manifest_id: checkpoint.manifest_id,
    })
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
/// use slatedb::admin;
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
///    admin::create_clone(
///      "clone_path",
///      "parent_path",
///      object_store,
///      None,
///    ).await?;
///
///    Ok(())
/// }
/// ```
pub async fn create_clone<P: Into<Path>>(
    clone_path: P,
    parent_path: P,
    object_store: Arc<dyn ObjectStore>,
    parent_checkpoint: Option<Uuid>,
) -> Result<(), Box<dyn Error>> {
    clone::create_clone(
        clone_path,
        parent_path,
        object_store,
        parent_checkpoint,
        Arc::new(FailPointRegistry::new()),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{memory::InMemory, path::Path};

    #[tokio::test]
    async fn test_delete_objects_with_prefix_empty() {
        let store = Arc::new(InMemory::new());
        let result = delete_objects_with_prefix(store, Some(&Path::from("test/prefix"))).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_objects_with_prefix_single_object() {
        let store = Arc::new(InMemory::new());

        // Put an object
        store
            .put(&Path::from("test/prefix/object1"), vec![1, 2, 3].into())
            .await
            .unwrap();

        // Delete objects with prefix
        let result =
            delete_objects_with_prefix(store.clone(), Some(&Path::from("test/prefix"))).await;
        assert!(result.is_ok());

        // Verify object is deleted
        let list_result = store
            .list(Some(&Path::from("test/prefix")))
            .collect::<Vec<_>>()
            .await;
        assert!(list_result.is_empty());
    }

    #[tokio::test]
    async fn test_delete_objects_with_prefix_multiple_objects() {
        let store = Arc::new(InMemory::new());

        // Put multiple objects with same prefix
        store
            .put(&Path::from("test/prefix/object1"), vec![1, 2, 3].into())
            .await
            .unwrap();
        store
            .put(&Path::from("test/prefix/object2"), vec![4, 5, 6].into())
            .await
            .unwrap();
        store
            .put(&Path::from("test/other/object3"), vec![7, 8, 9].into())
            .await
            .unwrap();

        // Delete objects with prefix
        let result =
            delete_objects_with_prefix(store.clone(), Some(&Path::from("test/prefix"))).await;
        assert!(result.is_ok());

        // Verify only objects with prefix are deleted
        let list_result = store.list(None).collect::<Vec<_>>().await;
        assert_eq!(list_result.len(), 1);
        assert_eq!(
            list_result[0].as_ref().unwrap().location,
            Path::from("test/other/object3")
        );
    }

    #[tokio::test]
    async fn test_delete_objects_with_empty_prefix() {
        let store = Arc::new(InMemory::new());

        // Put multiple objects at different paths
        store
            .put(&Path::from("test/prefix/object1"), vec![1, 2, 3].into())
            .await
            .unwrap();
        store
            .put(&Path::from("other/path/object2"), vec![4, 5, 6].into())
            .await
            .unwrap();
        store
            .put(&Path::from("root_object"), vec![7, 8, 9].into())
            .await
            .unwrap();

        // Test with empty string prefix
        let result = delete_objects_with_prefix(store.clone(), Some(&Path::from(""))).await;
        assert!(result.is_ok());

        // Verify all objects are deleted
        let list_result = store.list(None).collect::<Vec<_>>().await;
        assert!(
            list_result.is_empty(),
            "Expected all objects to be deleted with empty prefix"
        );
    }

    #[tokio::test]
    async fn test_delete_objects_with_root_prefix() {
        let store = Arc::new(InMemory::new());

        // Put multiple objects at different paths
        store
            .put(&Path::from("test/prefix/object1"), vec![1, 2, 3].into())
            .await
            .unwrap();
        store
            .put(&Path::from("other/path/object2"), vec![4, 5, 6].into())
            .await
            .unwrap();
        store
            .put(&Path::from("root_object"), vec![7, 8, 9].into())
            .await
            .unwrap();

        // Test with "/" prefix
        let result = delete_objects_with_prefix(store.clone(), Some(&Path::from("/"))).await;
        assert!(result.is_ok());

        // Verify all objects are deleted
        let list_result = store.list(None).collect::<Vec<_>>().await;
        assert!(
            list_result.is_empty(),
            "Expected all objects to be deleted with '/' prefix"
        );
    }

    #[tokio::test]
    async fn test_delete_objects_with_none_prefix() {
        let store = Arc::new(InMemory::new());

        // Put multiple objects at different paths
        store
            .put(&Path::from("test/prefix/object1"), vec![1, 2, 3].into())
            .await
            .unwrap();
        store
            .put(&Path::from("other/path/object2"), vec![4, 5, 6].into())
            .await
            .unwrap();
        store
            .put(&Path::from("root_object"), vec![7, 8, 9].into())
            .await
            .unwrap();

        // Test with None prefix
        let result = delete_objects_with_prefix(store.clone(), None).await;
        assert!(result.is_ok());

        // Verify all objects are deleted
        let list_result = store.list(None).collect::<Vec<_>>().await;
        assert!(
            list_result.is_empty(),
            "Expected all objects to be deleted with None prefix"
        );
    }
}
