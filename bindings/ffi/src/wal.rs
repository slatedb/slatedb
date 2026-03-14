use std::ops::Bound;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::FfiError;
use crate::object_store::FfiObjectStore;
use crate::types::FfiRowEntry;

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiWalFileMetadata {
    pub last_modified_seconds: i64,
    pub last_modified_nanos: u32,
    pub size_bytes: u64,
    pub location: String,
}

#[derive(uniffi::Object)]
pub struct FfiWalFile {
    inner: slatedb::WalFile,
}

impl FfiWalFile {
    fn new(inner: slatedb::WalFile) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl FfiWalFile {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn next_id(&self) -> u64 {
        self.inner.next_id()
    }

    pub fn next_file(&self) -> Arc<FfiWalFile> {
        Arc::new(FfiWalFile::new(self.inner.next_file()))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), FfiError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiWalFile {
    pub async fn metadata(&self) -> Result<FfiWalFileMetadata, FfiError> {
        let metadata = self.inner.metadata().await?;
        Ok(FfiWalFileMetadata {
            last_modified_seconds: metadata.last_modified_dt.timestamp(),
            last_modified_nanos: metadata.last_modified_dt.timestamp_subsec_nanos(),
            size_bytes: metadata.size_bytes,
            location: metadata.location.to_string(),
        })
    }

    pub async fn iterator(&self) -> Result<Arc<FfiWalFileIterator>, FfiError> {
        let iter = self.inner.iterator().await?;
        Ok(Arc::new(FfiWalFileIterator::new(iter)))
    }
}

#[derive(uniffi::Object)]
pub struct FfiWalFileIterator {
    inner: Mutex<slatedb::WalFileIterator>,
}

impl FfiWalFileIterator {
    fn new(inner: slatedb::WalFileIterator) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export]
impl FfiWalFileIterator {
    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), FfiError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiWalFileIterator {
    pub async fn next(&self) -> Result<Option<FfiRowEntry>, FfiError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(FfiRowEntry::from_core))
    }
}

#[derive(uniffi::Object)]
pub struct FfiWalReader {
    inner: slatedb::WalReader,
}

#[uniffi::export]
impl FfiWalReader {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<FfiObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: slatedb::WalReader::new(path, object_store.inner.clone()),
        })
    }

    pub fn get(&self, id: u64) -> Arc<FfiWalFile> {
        Arc::new(FfiWalFile::new(self.inner.get(id)))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), FfiError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiWalReader {
    pub async fn list(
        &self,
        start_id: Option<u64>,
        end_id: Option<u64>,
    ) -> Result<Vec<Arc<FfiWalFile>>, FfiError> {
        let start = start_id.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let end = end_id.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let files = self.inner.list((start, end)).await?;
        Ok(files
            .into_iter()
            .map(|file| Arc::new(FfiWalFile::new(file)))
            .collect())
    }
}
