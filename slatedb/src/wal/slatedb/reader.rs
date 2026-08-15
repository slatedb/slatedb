use std::sync::Arc;

use async_trait::async_trait;
use object_store::{path::Path, ObjectStore};
use tokio::sync::watch;

use crate::block_cache_policy::BlockCachePolicy;
use crate::db_status::DbStatusManager;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::iter::IterationOrder;
use crate::manifest::store::ManifestStore;
use crate::object_stores::ObjectStores;
use crate::sst_iter::SstIteratorOptions;
use crate::tablestore::{TableStore, TableStoreKind};
use crate::wal::slatedb::iterator::{SlateDbWalIterator, SlateDbWalIteratorOptions};
use crate::wal::{WalError, WalFileRange, WalIterator, WalReader};
use crate::{DbStatus, VersionedManifest};

#[async_trait]
trait ManifestReader: Send + Sync + 'static {
    async fn manifest(&self) -> Result<VersionedManifest, SlateDBError>;
}

#[async_trait]
impl ManifestReader for watch::Receiver<DbStatus> {
    async fn manifest(&self) -> Result<VersionedManifest, SlateDBError> {
        Ok(self.borrow().current_manifest.clone())
    }
}

#[async_trait]
impl ManifestReader for ManifestStore {
    async fn manifest(&self) -> Result<VersionedManifest, SlateDBError> {
        self.read_latest_manifest().await
    }
}

#[derive(Clone, Debug)]
pub struct SlateDbWalReaderOptions {
    /// The number of SSTs to preload while replaying
    pub sst_batch_size: usize,

    /// The number of fetch tasks to spawn per sst. Defaults to 2 so there is always a fetch
    /// pending while the current data is being consumed.
    pub max_fetch_tasks: usize,

    /// The number of bytes to read ahead in each sst. The value is rounded up to the nearest
    /// block size when fetching from object storage. The default is 1MB
    pub read_ahead_bytes: usize,
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

impl From<SlateDbWalReaderOptions> for SlateDbWalIteratorOptions {
    fn from(options: SlateDbWalReaderOptions) -> Self {
        let format = SsTableFormat::default();
        let blocks_to_fetch = options.read_ahead_bytes.div_ceil(format.block_size);
        Self {
            sst_batch_size: options.sst_batch_size,
            sst_iter_options: SstIteratorOptions {
                max_fetch_tasks: options.max_fetch_tasks,
                blocks_to_fetch,
                cache_blocks: false,
                cache_metadata: false,
                eager_spawn: true,
                order: IterationOrder::Ascending,
                prefix: None,
                filter_context: None,
            },
        }
    }
}

pub struct SlateDbWalReader {
    table_store: Arc<TableStore>,
    manifest_reader: Arc<dyn ManifestReader>,
    options: SlateDbWalReaderOptions,
}

impl SlateDbWalReader {
    pub fn new_for_db(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            path.clone(),
            None,
            TableStoreKind::Reader,
            BlockCachePolicy::default(),
        ));
        let manifest_reader: Arc<dyn ManifestReader> =
            Arc::new(ManifestStore::new(&path, object_store));
        Self {
            table_store,
            manifest_reader,
            options,
        }
    }

    pub(crate) fn new_with_status_manager(
        table_store: Arc<TableStore>,
        db_status: &DbStatusManager,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        let manifest_reader: Arc<dyn ManifestReader> = Arc::new(db_status.subscribe());
        Self {
            table_store,
            manifest_reader,
            options,
        }
    }
}

#[async_trait]
impl WalReader for SlateDbWalReader {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        let wal_id_range = wal_file_id_range.try_into().map_err(|()| {
            WalError::InternalError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "native WAL reader requires an included start and excluded end",
            )))
        })?;
        let iterator = SlateDbWalIterator::range(
            wal_id_range,
            self.options.clone().into(),
            Arc::clone(&self.table_store),
        )?;
        Ok(Box::new(iterator))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        let last = self
            .table_store
            .last_seen_wal_id(replay_after_wal_id)
            .await?;
        let manifest = self.manifest_reader.manifest().await?;
        if last < manifest.core().replay_after_wal_id {
            return Err(WalError::WalTruncated(last));
        }
        Ok(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FlushOptions, FlushType};
    use crate::db_state::SsTableId;
    use crate::manifest::store::StoredManifest;
    use crate::manifest::ManifestCore;
    use crate::types::ValueDeletable;
    use crate::Db;
    use object_store::memory::InMemory;
    use slatedb_common::clock::DefaultSystemClock;

    #[tokio::test]
    async fn test_native_wal_reader_trait() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/test_native_wal_reader_trait");
        let db = Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader =
            SlateDbWalReader::new_for_db(object_store, path, SlateDbWalReaderOptions::default());
        let last_wal_id = wal_reader.last_wal_file_id(0).await.unwrap();
        let mut iterator = wal_reader
            .iterator((1..last_wal_id + 1).into())
            .await
            .unwrap();

        let mut rows = Vec::new();
        while let Some(wal_rows) = iterator.next().await.unwrap() {
            rows.extend(wal_rows.rows);
        }
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.as_ref(), b"key");
        assert!(matches!(
            &rows[0].value,
            ValueDeletable::Value(value) if value.as_ref() == b"value"
        ));
    }

    #[tokio::test]
    async fn last_wal_file_id_errors_when_last_id_precedes_manifest_gc_cutoff() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/last_wal_file_id_before_gc_cutoff");
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            path.clone(),
            None,
            TableStoreKind::Reader,
            BlockCachePolicy::default(),
        ));
        table_store
            .table_writer(SsTableId::Wal(1))
            .close()
            .await
            .unwrap();

        let mut core = ManifestCore::new();
        core.next_wal_sst_id = 3;
        core.replay_after_wal_id = 2;
        StoredManifest::create_new_db(
            Arc::new(ManifestStore::new(&path, Arc::clone(&object_store))),
            core,
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let wal_reader =
            SlateDbWalReader::new_for_db(object_store, path, SlateDbWalReaderOptions::default());
        assert!(matches!(
            wal_reader.last_wal_file_id(0).await,
            Err(WalError::WalTruncated(1))
        ));
    }
}
