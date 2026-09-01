use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore as RawObjectStore;

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
    /// A local filesystem rooted at the `local_path` config entry.
    Local,
    /// An in-memory store, useful for tests.
    #[default]
    InMemory,
}

/// Builds an [`ObjectStore`] from a provider-specific key/value configuration
/// map, for callers that need finer control than `ObjectStore::resolve` or
/// `ObjectStore::from_env`.
///
/// Config keys match the corresponding `object_store` crate config keys (for
/// example `access_key_id`, `secret_access_key`, `bucket`, `region` for S3;
/// `account_name`, `access_key`, `container_name` for Azure; `service_account`,
/// `bucket` for GCS). `Local` requires a `local_path` entry naming the root
/// directory. `InMemory` ignores all config entries.
#[derive(uniffi::Object)]
pub struct ObjectStoreBuilder {
    store_type: ObjectStoreType,
    config: HashMap<String, String>,
}

#[uniffi::export]
impl ObjectStoreBuilder {
    /// Creates a builder for `store_type` using the given configuration entries.
    #[uniffi::constructor]
    pub fn new(store_type: ObjectStoreType, config: HashMap<String, String>) -> Arc<Self> {
        Arc::new(Self { store_type, config })
    }

    /// Builds the configured object store.
    pub fn build(&self) -> Result<Arc<ObjectStore>, Error> {
        let inner = match self.store_type {
            ObjectStoreType::S3 => self.build_s3()?,
            ObjectStoreType::Azure => self.build_azure()?,
            ObjectStoreType::Gcs => self.build_gcs()?,
            ObjectStoreType::Local => self.build_local()?,
            ObjectStoreType::InMemory => Arc::new(InMemory::new()) as Arc<dyn RawObjectStore>,
        };
        Ok(Arc::new(ObjectStore { inner }))
    }
}

impl ObjectStoreBuilder {
    fn build_s3(&self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        let mut builder = AmazonS3Builder::new();
        for (key, value) in &self.config {
            let config_key = AmazonS3ConfigKey::from_str(key).map_err(|_| {
                SlateDbError::InvalidObjectStoreConfigKey {
                    provider: "S3",
                    key: key.clone(),
                }
            })?;
            builder = builder.with_config(config_key, value);
        }
        builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);

        let store = builder
            .build()
            .map_err(|source| SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            })?;
        Ok(Arc::new(store))
    }

    fn build_azure(&self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        let mut builder = MicrosoftAzureBuilder::new();
        for (key, value) in &self.config {
            let config_key = AzureConfigKey::from_str(key).map_err(|_| {
                SlateDbError::InvalidObjectStoreConfigKey {
                    provider: "Azure",
                    key: key.clone(),
                }
            })?;
            builder = builder.with_config(config_key, value);
        }

        let store = builder
            .build()
            .map_err(|source| SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            })?;
        Ok(Arc::new(store))
    }

    fn build_gcs(&self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        let mut builder = GoogleCloudStorageBuilder::new();
        for (key, value) in &self.config {
            let config_key = GoogleConfigKey::from_str(key).map_err(|_| {
                SlateDbError::InvalidObjectStoreConfigKey {
                    provider: "GCS",
                    key: key.clone(),
                }
            })?;
            builder = builder.with_config(config_key, value);
        }

        let store = builder
            .build()
            .map_err(|source| SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            })?;
        Ok(Arc::new(store))
    }

    fn build_local(&self) -> Result<Arc<dyn RawObjectStore>, SlateDbError> {
        let local_path =
            self.config
                .get("local_path")
                .ok_or(SlateDbError::MissingObjectStoreConfigKey {
                    provider: "Local",
                    key: "local_path",
                })?;

        let store = LocalFileSystem::new_with_prefix(local_path).map_err(|source| {
            SlateDbError::ObjectStoreCreationError {
                source: Box::new(source),
            }
        })?;
        Ok(Arc::new(store))
    }
}