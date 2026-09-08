use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore as RawObjectStore, ObjectStoreScheme};
use parking_lot::Mutex;
use url::Url;

use crate::error::{Error, SlateDbError};
use crate::ObjectStore;

/// Backing provider targeted by an [`ObjectStoreBuilder`].
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum ObjectStoreType {
    /// Amazon S3, or an S3-compatible store.
    S3,
    /// Azure Blob Storage.
    Azure,
    /// Google Cloud Storage.
    Gcs,
    /// A local filesystem rooted at the path given to `with_url` or the
    /// `local_path` config entry.
    Local,
    /// An in-memory store, useful for tests.
    #[default]
    InMemory,
}

/// Stand-in for the `object_store` crate's provider builders, which has none
/// of its own for [`LocalFileSystem`].
#[derive(Default)]
struct LocalFileSystemBuilder {
    prefix: Option<PathBuf>,
}

impl LocalFileSystemBuilder {
    fn with_url(mut self, url: impl Into<String>) -> Self {
        self.prefix = Some(PathBuf::from(url.into()));
        self
    }

    fn with_config(mut self, key: &str, value: impl Into<String>) -> Result<Self, SlateDbError> {
        if key != "local_path" {
            return Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "Local",
                key: key.to_string(),
            });
        }
        self.prefix = Some(PathBuf::from(value.into()));
        Ok(self)
    }

    fn build(self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        let prefix = self
            .prefix
            .ok_or(SlateDbError::MissingObjectStoreConfigKey {
                provider: "Local",
                key: "local_path",
            })?;
        let store = LocalFileSystem::new_with_prefix(prefix).map_err(|source| {
            SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            }
        })?;
        Ok(Arc::new(store))
    }
}

/// Stand-in for the `object_store` crate's provider builders, which has none
/// of its own for [`InMemory`]. Accepts no configuration.
#[derive(Default)]
struct InMemoryBuilder;

impl InMemoryBuilder {
    fn build(self) -> Arc<dyn RawObjectStore> {
        Arc::new(InMemory::new())
    }
}

/// The provider-specific builder backing an [`ObjectStoreBuilder`].
enum ObjectStoreBuilderInner {
    Aws(AmazonS3Builder),
    Azure(MicrosoftAzureBuilder),
    Gcs(GoogleCloudStorageBuilder),
    Local(LocalFileSystemBuilder),
    InMemory(InMemoryBuilder),
}

impl ObjectStoreBuilderInner {
    fn new(store_type: ObjectStoreType) -> Self {
        match store_type {
            ObjectStoreType::S3 => Self::Aws(AmazonS3Builder::new()),
            ObjectStoreType::Azure => Self::Azure(MicrosoftAzureBuilder::new()),
            ObjectStoreType::Gcs => Self::Gcs(GoogleCloudStorageBuilder::new()),
            ObjectStoreType::Local => Self::Local(LocalFileSystemBuilder::default()),
            ObjectStoreType::InMemory => Self::InMemory(InMemoryBuilder),
        }
    }

    fn from_env(store_type: ObjectStoreType) -> Self {
        match store_type {
            ObjectStoreType::S3 => Self::Aws(AmazonS3Builder::from_env()),
            ObjectStoreType::Azure => Self::Azure(MicrosoftAzureBuilder::from_env()),
            ObjectStoreType::Gcs => Self::Gcs(GoogleCloudStorageBuilder::from_env()),
            // Local and InMemory have no environment-derived configuration.
            ObjectStoreType::Local => Self::Local(LocalFileSystemBuilder::default()),
            ObjectStoreType::InMemory => Self::InMemory(InMemoryBuilder),
        }
    }

    /// Builds directly from `url`'s scheme, the same way
    /// [`object_store::parse_url`] (and `ObjectStore::resolve`) do, instead
    /// of starting from an explicit [`ObjectStoreType`].
    fn from_url(url: String) -> Result<Self, SlateDbError> {
        let parsed = Url::parse(&url).map_err(|source| SlateDbError::InvalidObjectStoreUrl {
            url: url.clone(),
            source,
        })?;

        // `ObjectStoreScheme::parse` only recognizes `file:` URLs with no
        // host (`file:///path`), rejecting host-qualified ones
        // (`file://host/path`) outright. Handle `file:` ourselves so a host
        // component doesn't prevent treating the URL as `Local`. `Url::to_file_path`
        // converts to a platform-native path (and rejects a host-less
        // `file://` URL on Windows), which would make this behave
        // differently across platforms. `parsed.path()` is a plain string,
        // so it stays portable and matches how `ObjectStoreScheme::parse`
        // itself extracts a `Local` path.
        if parsed.scheme() == "file" {
            return Ok(Self::Local(LocalFileSystemBuilder {
                prefix: Some(PathBuf::from(parsed.path())),
            }));
        }

        let (scheme, _path) = ObjectStoreScheme::parse(&parsed).map_err(|source| {
            SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            }
        })?;

        Ok(match scheme {
            ObjectStoreScheme::Memory => Self::InMemory(InMemoryBuilder),
            ObjectStoreScheme::AmazonS3 => Self::Aws(AmazonS3Builder::new().with_url(url)),
            ObjectStoreScheme::GoogleCloudStorage => {
                Self::Gcs(GoogleCloudStorageBuilder::new().with_url(url))
            }
            ObjectStoreScheme::MicrosoftAzure => {
                Self::Azure(MicrosoftAzureBuilder::new().with_url(url))
            }
            _ => return Err(SlateDbError::UnsupportedObjectStoreScheme { url }),
        })
    }

    fn with_url(self, url: String) -> Self {
        match self {
            Self::Aws(b) => Self::Aws(b.with_url(url)),
            Self::Azure(b) => Self::Azure(b.with_url(url)),
            Self::Gcs(b) => Self::Gcs(b.with_url(url)),
            Self::Local(b) => Self::Local(b.with_url(url)),
            Self::InMemory(b) => Self::InMemory(b),
        }
    }

    fn with_config(self, key: String, value: String) -> Result<Self, SlateDbError> {
        match self {
            Self::Aws(b) => {
                let config_key = AmazonS3ConfigKey::from_str(&key).map_err(|_| {
                    SlateDbError::InvalidObjectStoreConfigKey {
                        provider: "S3",
                        key: key.clone(),
                    }
                })?;
                Ok(Self::Aws(b.with_config(config_key, value)))
            }
            Self::Azure(b) => {
                let config_key = AzureConfigKey::from_str(&key).map_err(|_| {
                    SlateDbError::InvalidObjectStoreConfigKey {
                        provider: "Azure",
                        key: key.clone(),
                    }
                })?;
                Ok(Self::Azure(b.with_config(config_key, value)))
            }
            Self::Gcs(b) => {
                let config_key = GoogleConfigKey::from_str(&key).map_err(|_| {
                    SlateDbError::InvalidObjectStoreConfigKey {
                        provider: "GCS",
                        key: key.clone(),
                    }
                })?;
                Ok(Self::Gcs(b.with_config(config_key, value)))
            }
            Self::Local(b) => Ok(Self::Local(b.with_config(&key, value)?)),
            // InMemory ignores all config entries.
            Self::InMemory(b) => Ok(Self::InMemory(b)),
        }
    }

    fn build(self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        match self {
            Self::Aws(b) => {
                let store = b
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map_err(|source| SlateDbError::ObjectStoreCreationError {
                        source: Box::new(source),
                    })?;
                Ok(Arc::new(store))
            }
            Self::Azure(b) => {
                let store = b
                    .build()
                    .map_err(|source| SlateDbError::ObjectStoreCreationError {
                        source: Box::new(source),
                    })?;
                Ok(Arc::new(store))
            }
            Self::Gcs(b) => {
                let store = b
                    .build()
                    .map_err(|source| SlateDbError::ObjectStoreCreationError {
                        source: Box::new(source),
                    })?;
                Ok(Arc::new(store))
            }
            Self::Local(b) => b.build(),
            Self::InMemory(b) => Ok(b.build()),
        }
    }
}

/// Builds an [`ObjectStore`] through the same builder shape the `object_store`
/// crate itself uses, for callers that need finer control than
/// `ObjectStore::resolve` or `ObjectStore::from_env`.
///
/// Mirrors `from_env`, `with_url`, and `with_config` from the crate's
/// provider builders (`AmazonS3Builder`, `MicrosoftAzureBuilder`,
/// `GoogleCloudStorageBuilder`). More esoteric setters (for example
/// `with_service_account_path`) are left out; reach them indirectly through
/// `with_config` instead. Config keys match the corresponding `object_store`
/// crate config keys (for example `access_key_id`, `secret_access_key`,
/// `bucket`, `region` for S3; `account_name`, `access_key`, `container_name`
/// for Azure; `service_account`, `bucket` for GCS). `Local` accepts only a
/// `local_path` entry naming the root directory. `InMemory` ignores all
/// config entries.
///
/// Builders are single-use: calling [`ObjectStoreBuilder::build`] consumes
/// the builder.
#[derive(uniffi::Object)]
pub struct ObjectStoreBuilder {
    inner: Mutex<Option<ObjectStoreBuilderInner>>,
}

impl ObjectStoreBuilder {
    fn update(
        &self,
        update: impl FnOnce(ObjectStoreBuilderInner) -> Result<ObjectStoreBuilderInner, SlateDbError>,
    ) -> Result<(), SlateDbError> {
        let mut guard = self.inner.lock();
        let inner = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(inner)?);
        Ok(())
    }

    fn take(&self) -> Result<ObjectStoreBuilderInner, SlateDbError> {
        let mut guard = self.inner.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl ObjectStoreBuilder {
    /// Creates an empty builder for `store_type`.
    #[uniffi::constructor]
    pub fn new(store_type: ObjectStoreType) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Some(ObjectStoreBuilderInner::new(store_type))),
        })
    }

    /// Creates a builder for `store_type`, seeded from environment variables
    /// the same way the `object_store` crate's own `from_env` builders are.
    #[uniffi::constructor]
    pub fn from_env(store_type: ObjectStoreType) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Some(ObjectStoreBuilderInner::from_env(store_type))),
        })
    }

    /// Creates a builder by inferring the provider from `url`'s scheme (for
    /// example `s3://`, `gs://`, `az://`, `file://`, `memory://`), the same
    /// way `ObjectStore::resolve` does, then applies `url` via `with_url`.
    ///
    /// Returns an error if `url` cannot be parsed or its scheme does not map
    /// to one of the supported providers.
    #[uniffi::constructor]
    pub fn from_url(url: String) -> Result<Arc<Self>, Error> {
        let inner = ObjectStoreBuilderInner::from_url(url)?;
        Ok(Arc::new(Self {
            inner: Mutex::new(Some(inner)),
        }))
    }

    /// Applies provider-specific configuration parsed out of `url`.
    pub fn with_url(&self, url: String) -> Result<(), Error> {
        self.update(|inner| Ok(inner.with_url(url)))
            .map_err(Into::into)
    }

    /// Sets a single provider-specific configuration entry.
    pub fn with_config(&self, key: String, value: String) -> Result<(), Error> {
        self.update(|inner| inner.with_config(key, value))
            .map_err(Into::into)
    }

    /// Builds the configured object store, consuming this builder.
    pub fn build(&self) -> Result<Arc<ObjectStore>, Error> {
        let inner = self.take()?;
        let store = inner.build()?;
        Ok(Arc::new(ObjectStore { inner: store }))
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;
    use std::path::Path;

    use super::*;

    #[test]
    fn from_url_detects_s3() {
        let inner = ObjectStoreBuilderInner::from_url("s3://my-bucket/path".to_string()).unwrap();
        assert!(matches!(inner, ObjectStoreBuilderInner::Aws(_)));
    }

    #[test]
    fn from_url_detects_gcs() {
        let inner = ObjectStoreBuilderInner::from_url("gs://my-bucket/path".to_string()).unwrap();
        assert!(matches!(inner, ObjectStoreBuilderInner::Gcs(_)));
    }

    #[test]
    fn from_url_detects_azure() {
        let inner = ObjectStoreBuilderInner::from_url("az://container/path".to_string()).unwrap();
        assert!(matches!(inner, ObjectStoreBuilderInner::Azure(_)));
    }

    #[test]
    fn from_url_detects_memory() {
        let inner = ObjectStoreBuilderInner::from_url("memory:///".to_string()).unwrap();
        assert!(matches!(inner, ObjectStoreBuilderInner::InMemory(_)));
    }

    #[test]
    fn from_url_detects_local_and_derives_prefix() {
        let inner =
            ObjectStoreBuilderInner::from_url("file:///tmp/slatedb-test".to_string()).unwrap();
        match inner {
            ObjectStoreBuilderInner::Local(builder) => {
                assert_eq!(
                    builder.prefix.as_deref(),
                    Some(Path::new("/tmp/slatedb-test"))
                );
            }
            _ => panic!("expected a Local builder"),
        }
    }

    #[test]
    fn from_url_keeps_local_prefix_for_host_qualified_file_urls() {
        let inner =
            ObjectStoreBuilderInner::from_url("file://example.com/tmp/slatedb-test".to_string())
                .unwrap();
        match inner {
            ObjectStoreBuilderInner::Local(builder) => {
                assert_eq!(
                    builder.prefix.as_deref(),
                    Some(Path::new("/tmp/slatedb-test"))
                );
            }
            _ => panic!("expected a Local builder"),
        }
    }

    #[test]
    fn from_url_rejects_unsupported_scheme() {
        let result = ObjectStoreBuilderInner::from_url("http://example.com/path".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::UnsupportedObjectStoreScheme { .. })
        ));
    }

    #[test]
    fn from_url_rejects_invalid_url() {
        let result = ObjectStoreBuilderInner::from_url("not a url".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::InvalidObjectStoreUrl { .. })
        ));
    }

    #[test]
    fn build_consumes_the_builder() {
        let builder = ObjectStoreBuilder::new(ObjectStoreType::InMemory);
        assert!(builder.build().is_ok());
        assert!(builder.build().is_err());
    }

    #[test]
    fn with_config_after_build_fails() {
        let builder = ObjectStoreBuilder::new(ObjectStoreType::InMemory);
        assert!(builder.build().is_ok());
        assert!(builder
            .with_config("local_path".to_string(), "/tmp".to_string())
            .is_err());
    }

    #[test]
    fn with_url_after_build_fails() {
        let builder = ObjectStoreBuilder::new(ObjectStoreType::InMemory);
        assert!(builder.build().is_ok());
        assert!(builder.with_url("memory:///".to_string()).is_err());
    }

    #[test]
    fn s3_with_config_rejects_invalid_key() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::S3)
            .with_config("not_a_real_key".to_string(), "value".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::InvalidObjectStoreConfigKey { provider: "S3", .. })
        ));
    }

    #[test]
    fn azure_with_config_rejects_invalid_key() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::Azure)
            .with_config("not_a_real_key".to_string(), "value".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "Azure",
                ..
            })
        ));
    }

    #[test]
    fn gcs_with_config_rejects_invalid_key() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::Gcs)
            .with_config("not_a_real_key".to_string(), "value".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "GCS",
                ..
            })
        ));
    }

    #[test]
    fn local_with_config_rejects_keys_other_than_local_path() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::Local)
            .with_config("bucket".to_string(), "value".to_string());
        assert!(matches!(
            result,
            Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "Local",
                ..
            })
        ));
    }

    #[test]
    fn local_build_without_local_path_fails() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::Local).build();
        assert!(matches!(
            result,
            Err(SlateDbError::MissingObjectStoreConfigKey {
                provider: "Local",
                key: "local_path",
            })
        ));
    }

    #[test]
    fn in_memory_ignores_config() {
        let inner = ObjectStoreBuilderInner::new(ObjectStoreType::InMemory)
            .with_config("anything".to_string(), "value".to_string())
            .unwrap();
        assert!(inner.build().is_ok());
    }

    #[test]
    fn s3_build_without_bucket_fails() {
        let result = ObjectStoreBuilderInner::new(ObjectStoreType::S3).build();
        assert!(matches!(
            result,
            Err(SlateDbError::ObjectStoreCreationError { .. })
        ));
    }

    #[test]
    fn s3_with_valid_config_builds() {
        let builder = ObjectStoreBuilder::new(ObjectStoreType::S3);
        builder
            .with_config("bucket".to_string(), "test-bucket".to_string())
            .unwrap();
        builder
            .with_config("access_key_id".to_string(), "test-key".to_string())
            .unwrap();
        builder
            .with_config("secret_access_key".to_string(), "test-secret".to_string())
            .unwrap();
        assert!(builder.build().is_ok());
    }

    #[test]
    fn s3_from_env_reads_bucket_and_credentials() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("AWS_BUCKET", "test-bucket");
            jail.set_env("AWS_ACCESS_KEY_ID", "test-key");
            jail.set_env("AWS_SECRET_ACCESS_KEY", "test-secret");

            let builder = ObjectStoreBuilder::from_env(ObjectStoreType::S3);
            assert!(builder.build().is_ok());

            Ok(())
        });
    }

    #[tokio::test]
    async fn in_memory_store_round_trips_put_get() {
        let store = ObjectStoreBuilder::new(ObjectStoreType::InMemory)
            .build()
            .unwrap();
        let path = object_store::path::Path::from("key");
        store
            .inner
            .put(&path, b"hello".to_vec().into())
            .await
            .unwrap();
        let result = store.inner.get(&path).await.unwrap();
        assert_eq!(result.bytes().await.unwrap().as_ref(), b"hello");
    }

    #[tokio::test]
    async fn local_store_round_trips_put_get() {
        let dir =
            std::env::temp_dir().join(format!("slatedb-uniffi-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();

        let builder = ObjectStoreBuilder::new(ObjectStoreType::Local);
        builder
            .with_config("local_path".to_string(), dir.to_string_lossy().into_owned())
            .unwrap();
        let store = builder.build().unwrap();

        let path = object_store::path::Path::from("key");
        store
            .inner
            .put(&path, b"hello".to_vec().into())
            .await
            .unwrap();
        let result = store.inner.get(&path).await.unwrap();
        assert_eq!(result.bytes().await.unwrap().as_ref(), b"hello");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
