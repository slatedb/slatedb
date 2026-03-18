use std::ops::Bound;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Error;
use crate::object_store::ObjectStore;
use crate::types::RowEntry;

/// Metadata describing a WAL file in object storage.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WalFileMetadata {
    /// Last-modified timestamp seconds component.
    pub last_modified_seconds: i64,
    /// Last-modified timestamp nanoseconds component.
    pub last_modified_nanos: u32,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Object-store location of the file.
    pub location: String,
}

/// Handle for a single WAL file.
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
    /// Returns the WAL file ID.
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// Returns the WAL ID immediately after this file.
    pub fn next_id(&self) -> u64 {
        self.inner.next_id()
    }

    /// Returns a handle for the next WAL file ID without checking existence.
    pub fn next_file(&self) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.next_file()))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    /// No-op close method for API symmetry.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFile {
    /// Reads object-store metadata for this WAL file.
    pub async fn metadata(&self) -> Result<WalFileMetadata, Error> {
        let metadata = self.inner.metadata().await?;
        Ok(WalFileMetadata {
            last_modified_seconds: metadata.last_modified_dt.timestamp(),
            last_modified_nanos: metadata.last_modified_dt.timestamp_subsec_nanos(),
            size_bytes: metadata.size_bytes,
            location: metadata.location.to_string(),
        })
    }

    /// Opens an iterator over raw row entries in this WAL file.
    pub async fn iterator(&self) -> Result<Arc<WalFileIterator>, Error> {
        let iter = self.inner.iterator().await?;
        Ok(Arc::new(WalFileIterator::new(iter)))
    }
}

/// Iterator over raw row entries stored in a WAL file.
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
    /// No-op close method for API symmetry.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFileIterator {
    /// Returns the next raw row entry from the WAL file.
    pub async fn next(&self) -> Result<Option<RowEntry>, Error> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(RowEntry::from))
    }
}

/// Reader for WAL files stored under a database path.
#[derive(uniffi::Object)]
pub struct WalReader {
    inner: slatedb::WalReader,
}

#[uniffi::export]
impl WalReader {
    /// Creates a WAL reader for `path` in `object_store`.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: slatedb::WalReader::new(path, object_store.inner.clone()),
        })
    }

    /// Returns a handle for the WAL file with the given ID.
    pub fn get(&self, id: u64) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.get(id)))
    }

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    /// No-op close method for API symmetry.
    #[uniffi::method(name = "shutdown")]
    pub fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalReader {
    /// Lists WAL files in ascending ID order.
    ///
    /// `start_id` is inclusive and `end_id` is exclusive when provided.
    pub async fn list(
        &self,
        start_id: Option<u64>,
        end_id: Option<u64>,
    ) -> Result<Vec<Arc<WalFile>>, Error> {
        let start = start_id.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let end = end_id.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let files = self.inner.list((start, end)).await?;
        Ok(files
            .into_iter()
            .map(|file| Arc::new(WalFile::new(file)))
            .collect())
    }
}
