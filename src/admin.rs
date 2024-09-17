use crate::manifest_store::ManifestStore;
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::error::Error;
use std::sync::Arc;

/// read-only access to the latest manifest file
pub async fn read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<String>, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    match manifest_store.read_latest_manifest().await? {
        None => Ok(None),
        Some(id_manifest) => Ok(Some(serde_json::to_string(&id_manifest)?)),
    }
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
        "aws" => load_aws(),
        _ => Err(format!("Unknown OS_PROVIDER: '{}'", provider).into()),
    }
}

/// Loads an AWS S3 Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | AWS_ACCESS_KEY_ID | The access key for a role with permissions to access the store | Yes |
/// | AWS_SECRET_ACCESS_KEY | The access key secret for the above ID | Yes |
/// | AWS_BUCKET | The bucket to use within S3 | Yes |
/// | AWS_REGION | The AWS region to use | Yes |
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    #[cfg(not(feature = "aws"))]
    panic!("feature 'aws' must be enabled to use OS_PROVIDER=aws");

    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret =
        env::var("AWS_SECRET_ACCESS_KEY").expect("Expected AWS_SECRET_ACCESS_KEY must be set");
    let bucket = env::var("AWS_BUCKET").expect("AWS_BUCKET must be set");
    let region = env::var("AWS_REGION").expect("AWS_REGION must be set");

    Ok(Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_bucket_name(bucket)
            .with_region(region)
            .build()?,
    ) as Arc<dyn ObjectStore>)
}
