use std::ops::Bound;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::DbError;
use crate::object_store::ObjectStore;
use crate::types::RowEntry;

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WalFileMetadata {
    pub last_modified_seconds: i64,
    pub last_modified_nanos: u32,
    pub size_bytes: u64,
    pub location: String,
}

#[derive(uniffi::Object)]
pub struct WalFile {
    inner: slatedb::WalFile,
}

impl WalFile {
    fn new(inner: slatedb::WalFile) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl WalFile {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn next_id(&self) -> u64 {
        self.inner.next_id()
    }

    pub fn next_file(&self) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.next_file()))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), DbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFile {
    pub async fn metadata(&self) -> Result<WalFileMetadata, DbError> {
        let metadata = self.inner.metadata().await?;
        Ok(WalFileMetadata {
            last_modified_seconds: metadata.last_modified_dt.timestamp(),
            last_modified_nanos: metadata.last_modified_dt.timestamp_subsec_nanos(),
            size_bytes: metadata.size_bytes,
            location: metadata.location.to_string(),
        })
    }

    pub async fn iterator(&self) -> Result<Arc<WalFileIterator>, DbError> {
        let iter = self.inner.iterator().await?;
        Ok(Arc::new(WalFileIterator::new(iter)))
    }
}

#[derive(uniffi::Object)]
pub struct WalFileIterator {
    inner: Mutex<slatedb::WalFileIterator>,
}

impl WalFileIterator {
    fn new(inner: slatedb::WalFileIterator) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export]
impl WalFileIterator {
    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), DbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFileIterator {
    pub async fn next(&self) -> Result<Option<RowEntry>, DbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(RowEntry::from_core))
    }
}

#[derive(uniffi::Object)]
pub struct WalReader {
    inner: slatedb::WalReader,
}

#[uniffi::export]
impl WalReader {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: slatedb::WalReader::new(path, object_store.inner.clone()),
        })
    }

    pub fn get(&self, id: u64) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.get(id)))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), DbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalReader {
    pub async fn list(
        &self,
        start_id: Option<u64>,
        end_id: Option<u64>,
    ) -> Result<Vec<Arc<WalFile>>, DbError> {
        let start = start_id.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let end = end_id.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let files = self.inner.list((start, end)).await?;
        Ok(files
            .into_iter()
            .map(|file| Arc::new(WalFile::new(file)))
            .collect())
    }
}
