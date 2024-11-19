use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use uuid::Uuid;
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

    /// Read a key an return the read value, if any.
    pub async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        unimplemented!()
    }
}