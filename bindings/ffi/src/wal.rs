//! WAL inspection APIs exposed by UniFFI.

use std::ops::Bound;
use std::sync::Arc;

use slatedb::{
    RowEntry as CoreRowEntry, ValueDeletable, WalFile as CoreWalFile,
    WalFileIterator as CoreWalFileIterator, WalReader as CoreWalReader,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::error::SlatedbError;
use crate::object_store::ObjectStore;

/// The kind of entry stored in a WAL row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RowEntryKind {
    Value,
    Tombstone,
    Merge,
}

/// A row entry returned by WAL iteration.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowEntry {
    pub kind: RowEntryKind,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub seq: u64,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

/// Metadata for a single WAL file.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WalFileMetadata {
    pub last_modified_seconds: i64,
    pub last_modified_nanos: u32,
    pub size_bytes: u64,
    pub location: String,
}

/// A WAL file handle.
#[derive(uniffi::Object)]
pub struct WalFile {
    inner: CoreWalFile,
}

/// An iterator over rows in a WAL file.
#[derive(uniffi::Object)]
pub struct WalFileIterator {
    inner: AsyncMutex<CoreWalFileIterator>,
}

/// A WAL reader scoped to a single database path and object store.
#[derive(uniffi::Object)]
pub struct WalReader {
    inner: CoreWalReader,
}

impl RowEntry {
    fn from_core(entry: CoreRowEntry) -> Self {
        let (kind, value) = match entry.value {
            ValueDeletable::Value(value) => (RowEntryKind::Value, Some(value.to_vec())),
            ValueDeletable::Tombstone => (RowEntryKind::Tombstone, None),
            ValueDeletable::Merge(value) => (RowEntryKind::Merge, Some(value.to_vec())),
        };

        Self {
            kind,
            key: entry.key.to_vec(),
            value,
            seq: entry.seq,
            create_ts: entry.create_ts,
            expire_ts: entry.expire_ts,
        }
    }
}

impl WalFile {
    fn new(inner: CoreWalFile) -> Self {
        Self { inner }
    }
}

impl WalFileIterator {
    fn new(inner: CoreWalFileIterator) -> Self {
        Self {
            inner: AsyncMutex::new(inner),
        }
    }
}

#[uniffi::export]
impl WalReader {
    /// Create a WAL reader for the provided database path and object store.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: CoreWalReader::new(path, object_store.inner.clone()),
        })
    }

    /// Return a handle for a specific WAL file ID.
    pub fn get(&self, id: u64) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.get(id)))
    }

    /// Close the WAL reader.
    pub fn close(&self) -> Result<(), SlatedbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalReader {
    /// List WAL files in ascending ID order.
    pub async fn list(
        &self,
        start_id: Option<u64>,
        end_id: Option<u64>,
    ) -> Result<Vec<Arc<WalFile>>, SlatedbError> {
        let start = start_id.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let end = end_id.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let files = self.inner.list((start, end)).await?;
        Ok(files
            .into_iter()
            .map(|file| Arc::new(WalFile::new(file)))
            .collect())
    }
}

#[uniffi::export]
impl WalFile {
    /// Return this file's WAL ID.
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// Return the next WAL ID after this file.
    pub fn next_id(&self) -> u64 {
        self.inner.next_id()
    }

    /// Return a handle for the next WAL file after this one.
    pub fn next_file(&self) -> Arc<WalFile> {
        Arc::new(WalFile::new(self.inner.next_file()))
    }

    /// Close the WAL file handle.
    pub fn close(&self) -> Result<(), SlatedbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFile {
    /// Fetch metadata for this WAL file.
    pub async fn metadata(&self) -> Result<WalFileMetadata, SlatedbError> {
        let metadata = self.inner.metadata().await?;
        Ok(WalFileMetadata {
            last_modified_seconds: metadata.last_modified_dt.timestamp(),
            last_modified_nanos: metadata.last_modified_dt.timestamp_subsec_nanos(),
            size_bytes: metadata.size_bytes,
            location: metadata.location.to_string(),
        })
    }

    /// Create an iterator over rows in this WAL file.
    pub async fn iterator(&self) -> Result<Arc<WalFileIterator>, SlatedbError> {
        let iter = self.inner.iterator().await?;
        Ok(Arc::new(WalFileIterator::new(iter)))
    }
}

#[uniffi::export]
impl WalFileIterator {
    /// Close the WAL iterator.
    pub fn close(&self) -> Result<(), SlatedbError> {
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WalFileIterator {
    /// Return the next WAL entry, or `None` when the iterator is exhausted.
    pub async fn next(&self) -> Result<Option<RowEntry>, SlatedbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(RowEntry::from_core))
    }
}
