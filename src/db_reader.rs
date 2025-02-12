use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::Checkpoint;
use crate::config::{CheckpointOptions, ReadOptions};
use crate::db_cache::DbCache;
use crate::error::SlateDBError;
use crate::manifest_store::{ManifestStore, StoredManifest};
use crate::sst::SsTableFormat;
use crate::tablestore::TableStore;

/// Read-only
struct DbReader {
    pub(crate) manifest: StoredManifest,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) options: DbReaderOptions,
}

#[derive(Clone, Deserialize, Serialize)]
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
    pub max_memtable_bytes: u64,

    #[serde(skip)]
    pub block_cache: Option<Arc<dyn DbCache>>,
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
    pub async fn open(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        checkpoint: Option<Uuid>,
        options: DbReaderOptions,
    ) -> Result<Self, SlateDBError> {
        let table_store = Arc::new(TableStore::new(
            Arc::clone(&object_store),
            SsTableFormat::default(),
            path.clone(),
            options.block_cache.clone(),
        ));
        let manifest_store = Arc::new(ManifestStore::new(
            &path,
            Arc::clone(&object_store),
        ));
        let mut manifest = StoredManifest::load(
            Arc::clone(&manifest_store),
        ).await?;

        let checkpoint = if let Some(checkpoint_id) = checkpoint  {
            manifest.db_state().find_checkpoint(&checkpoint_id)
                .ok_or(SlateDBError::CheckpointMissing(checkpoint_id))?
                .clone()
        } else {
            // Create a new checkpoint from the latest state
            let options = CheckpointOptions {
                lifetime: options.checkpoint_lifetime.clone(),
                ..CheckpointOptions::default()
            };
            manifest.write_checkpoint(None, &options).await?
        };

        Ok(Self {
            manifest,
            table_store,
            options,
        })
    }

    fn establish_checkpoint(checkpoint: &Checkpoint) {
        // Read checkpoint WALs into a memtable
    }

    fn spawn_manifest_poller(&mut self) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let fut = async move {
            ()
        };
        None
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        unimplemented!()
    }

}

impl Reader for DbReader {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        unimplemented!()
    }

    async fn get_with_options(&self, key: &[u8], options: &ReadOptions) -> Result<Option<Bytes>, SlateDBError> {
        unimplemented!()
    }
}
