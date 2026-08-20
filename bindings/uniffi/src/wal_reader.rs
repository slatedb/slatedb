use std::sync::Arc;

use slatedb::wal::WalReader as _;
use tokio::sync::Mutex;

use crate::error::Error;
use crate::object_store::ObjectStore;
use crate::types::RowEntry;

/// Options controlling how the native SlateDB WAL reader fetches WAL SSTs.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SlateDbWalReaderOptions {
    /// Number of WAL SSTs to preload.
    #[uniffi(default = 4)]
    pub sst_batch_size: u64,
    /// Number of concurrent fetch tasks per WAL SST.
    #[uniffi(default = 2)]
    pub max_fetch_tasks: u64,
    /// Number of bytes to read ahead from each WAL SST.
    #[uniffi(default = 1048576)]
    pub read_ahead_bytes: u64,
}

impl Default for SlateDbWalReaderOptions {
    fn default() -> Self {
        Self {
            sst_batch_size: 4,
            max_fetch_tasks: 2,
            read_ahead_bytes: 1024 * 1024,
        }
    }
}

impl TryFrom<SlateDbWalReaderOptions> for slatedb::wal::SlateDbWalReaderOptions {
    type Error = Error;

    fn try_from(options: SlateDbWalReaderOptions) -> Result<Self, Self::Error> {
        Ok(Self {
            sst_batch_size: positive_usize(options.sst_batch_size, "sst_batch_size")?,
            max_fetch_tasks: positive_usize(options.max_fetch_tasks, "max_fetch_tasks")?,
            read_ahead_bytes: positive_usize(options.read_ahead_bytes, "read_ahead_bytes")?,
        })
    }
}

fn positive_usize(value: u64, field: &'static str) -> Result<usize, Error> {
    if value == 0 {
        return Err(Error::Invalid {
            message: format!("{field} must be greater than zero"),
        });
    }
    usize::try_from(value).map_err(|_| Error::Invalid {
        message: format!("{field} is too large for this platform"),
    })
}

/// Rows from one fully consumed WAL file.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WalRows {
    /// Rows stored in the WAL file. Empty fence WALs produce an empty vector.
    pub rows: Vec<RowEntry>,
    /// Last WAL file ID fully consumed by this batch.
    pub last_consumed_wal_file_id: u64,
}

impl From<slatedb::wal::WalRows> for WalRows {
    fn from(rows: slatedb::wal::WalRows) -> Self {
        Self {
            rows: rows.rows.into_iter().map(Into::into).collect(),
            last_consumed_wal_file_id: rows.last_consumed_wal_file_id,
        }
    }
}

/// Live iterator over SlateDB WAL files starting at a required WAL file ID.
#[derive(uniffi::Object)]
pub struct SlateDbWalIterator {
    inner: Mutex<Box<dyn slatedb::wal::WalIterator>>,
}

impl SlateDbWalIterator {
    fn new(inner: Box<dyn slatedb::wal::WalIterator>) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl SlateDbWalIterator {
    /// Returns rows from the next fully consumed WAL file. When it reaches the
    /// current tail, this call waits for the next WAL file rather than ending.
    pub async fn next(&self) -> Result<Option<WalRows>, Error> {
        let mut iterator = self.inner.lock().await;
        Ok(iterator.next().await?.map(Into::into))
    }
}

/// CDC reader backed by SlateDB's native live WAL reader.
#[derive(uniffi::Object)]
pub struct SlateDbWalReader {
    inner: slatedb::wal::SlateDbWalReader,
}

impl SlateDbWalReader {
    fn build(
        path: String,
        object_store: Arc<ObjectStore>,
        wal_object_store: Option<Arc<ObjectStore>>,
        options: SlateDbWalReaderOptions,
    ) -> Result<Arc<Self>, Error> {
        let options = options.try_into()?;
        let path = slatedb::object_store::path::Path::from(path);
        let mut builder = slatedb::wal::SlateDbWalReaderBuilder::new()
            .with_object_store(Arc::clone(&object_store.inner))
            .with_path(path)
            .with_options(options);
        if let Some(wal_object_store) = wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(&wal_object_store.inner));
        }
        let inner = builder.build()?;
        Ok(Arc::new(Self { inner }))
    }
}

#[uniffi::export]
impl SlateDbWalReader {
    /// Opens a reader when the manifest and WAL use the same object store.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Result<Arc<Self>, Error> {
        Self::build(path, object_store, None, SlateDbWalReaderOptions::default())
    }

    /// Opens a reader with explicit fetch options.
    #[uniffi::constructor]
    pub fn with_options(
        path: String,
        object_store: Arc<ObjectStore>,
        options: SlateDbWalReaderOptions,
    ) -> Result<Arc<Self>, Error> {
        Self::build(path, object_store, None, options)
    }

    /// Opens a reader for a database with a dedicated WAL object store.
    #[uniffi::constructor]
    pub fn with_wal_object_store(
        path: String,
        object_store: Arc<ObjectStore>,
        wal_object_store: Arc<ObjectStore>,
    ) -> Result<Arc<Self>, Error> {
        Self::build(
            path,
            object_store,
            Some(wal_object_store),
            SlateDbWalReaderOptions::default(),
        )
    }

    /// Opens a reader for a dedicated WAL object store with explicit options.
    #[uniffi::constructor]
    pub fn with_wal_object_store_and_options(
        path: String,
        object_store: Arc<ObjectStore>,
        wal_object_store: Arc<ObjectStore>,
        options: SlateDbWalReaderOptions,
    ) -> Result<Arc<Self>, Error> {
        Self::build(path, object_store, Some(wal_object_store), options)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl SlateDbWalReader {
    /// Returns a snapshot of the current WAL tail after replay_after_wal_id, or
    /// the supplied ID when no later WAL file exists.
    pub async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, Error> {
        Ok(self.inner.last_wal_file_id(replay_after_wal_id).await?)
    }

    /// Opens a live iterator starting at start_wal_file_id. The iterator waits
    /// and polls internally when it reaches the current WAL tail.
    pub async fn iterator(&self, start_wal_file_id: u64) -> Result<Arc<SlateDbWalIterator>, Error> {
        let iterator = self.inner.iterator((start_wal_file_id..).into()).await?;
        Ok(Arc::new(SlateDbWalIterator::new(iterator)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_reader_options() {
        let options = SlateDbWalReaderOptions {
            sst_batch_size: 0,
            ..SlateDbWalReaderOptions::default()
        };
        assert!(matches!(
            slatedb::wal::SlateDbWalReaderOptions::try_from(options),
            Err(Error::Invalid { .. })
        ));
    }
}
