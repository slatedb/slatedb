use crate::manifest_store::ManifestStore;
#[cfg(feature = "aws")]
use log::warn;
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::error::Error;
use std::ops::RangeBounds;
use std::sync::Arc;

/// read-only access to the latest manifest file
pub async fn read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    maybe_id: Option<u64>,
) -> Result<Option<String>, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    let id_manifest = match maybe_id {
        None => manifest_store.read_latest_manifest().await?,
        Some(id) => manifest_store.read_manifest(id).await?,
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
        warn!(
            "Running without configuring a DynamoDB Table. This is OK when running an admin, \
        but any operations that attempt a CAS will fail."
        );
        builder
    };

    let builder = if let Some(endpoint) = endpoint {
        builder.with_allow_http(true).with_endpoint(endpoint)
    } else {
        builder
    };

    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}
