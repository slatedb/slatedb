use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use uuid::Uuid;
use crate::config::ReadOptions;
use crate::error::SlateDBError;

/// Read-only
struct DbReader {

}

struct DbReaderOptions {
    /// How frequently to poll for new manifest files. Refreshing the manifest file allows readers
    /// to detect newly compacted data.
    pub manifest_poll_interval: Duration,

    /// For readers that refresh their checkpoint, this specifies the lifetime to use for the
    /// created checkpoint. The checkpoint’s expire time will be set to the current time plus
    /// this value. If not specified, then the checkpoint will be created with no expiry, and
    /// must be manually removed. This lifetime must always be greater than
    /// manifest_poll_interval x 2
    pub checkpoint_lifetime: Option<Duration>,

    /// The max size of a single in-memory table used to buffer WAL entries
    /// Defaults to 64MB
    pub max_memtable_bytes: u64
}

pub trait Reader {
    /// Get a value from the database with default read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get(b"key").await?, Some(Bytes::from_static(b"value")));
    ///     Ok(())
    /// }
    /// ```
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, SlateDBError>;

    /// Get a value from the database with custom read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, config::ReadOptions, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get_with_options(b"key", &ReadOptions::default()).await?, Some(Bytes::from_static(b"value")));
    ///     Ok(())
    /// }
    /// ```
    async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError>;
}


impl DbReader {
    /// Creates a database reader that can read the contents of a database (but cannot write any
    /// data). The caller can provide an optional checkpoint. If the checkpoint is provided, the
    /// reader will read using the specified checkpoint and will not periodically refresh the
    /// checkpoint. Otherwise, the reader creates a new checkpoint pointing to the current manifest
    /// and refreshes it periodically as specified in the options. It also removes the previous
    /// checkpoint once any ongoing reads have completed.
    pub async fn open<P: Into<Path>>(
        _path: P,
        _object_store: Arc<dyn ObjectStore>,
        _checkpoint: Option<Uuid>,
        _options: DbReaderOptions,
    ) -> Result<Self, SlateDBError> {
        unimplemented!()
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        unimplemented!()
    }
}

impl Reader for DbReader {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        todo!()
    }

    async fn get_with_options(&self, key: &[u8], options: &ReadOptions) -> Result<Option<Bytes>, SlateDBError> {
        todo!()
    }
}