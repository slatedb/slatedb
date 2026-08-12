use std::sync::Arc;

use async_trait::async_trait;

use crate::iter::IterationOrder;
use crate::sst_iter::SstIteratorOptions;
use crate::tablestore::TableStore;
use crate::wal::{WalError, WalFileRange, WalIterator, WalReader};
use crate::wal_replay::{WalIterator as WalReplayIterator, WalIteratorOptions};

pub(crate) struct SlateDbWalReader {
    table_store: Arc<TableStore>,
}

impl SlateDbWalReader {
    pub(crate) fn new(table_store: Arc<TableStore>) -> Self {
        Self { table_store }
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
        let iterator = WalReplayIterator::range(
            wal_id_range,
            WalIteratorOptions {
                sst_batch_size: 4,
                sst_iter_options: SstIteratorOptions {
                    max_fetch_tasks: 1,
                    blocks_to_fetch: 256,
                    cache_blocks: true,
                    cache_metadata: false,
                    eager_spawn: true,
                    order: IterationOrder::Ascending,
                    prefix: None,
                    filter_context: None,
                },
            },
            Arc::clone(&self.table_store),
        )?;
        Ok(Box::new(iterator))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        Ok(self
            .table_store
            .last_seen_wal_id(replay_after_wal_id)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::config::{FlushOptions, FlushType};
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::tablestore::TableStoreKind;
    use crate::types::ValueDeletable;
    use crate::Db;
    use object_store::memory::InMemory;
    use object_store::{path::Path, ObjectStore};

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

        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            path,
            None,
            TableStoreKind::Reader,
            BlockCachePolicy::default(),
        ));
        let wal_reader = SlateDbWalReader::new(table_store);
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
}
