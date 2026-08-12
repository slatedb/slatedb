use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::db_status::DbStatus;
use crate::iter::IterationOrder;
use crate::sst_iter::SstIteratorOptions;
use crate::tablestore::TableStore;
use crate::wal::{WalError, WalFileRange, WalIterator, WalReader};
use crate::wal_replay::{WalIterator as WalReplayIterator, WalIteratorOptions};

pub(crate) struct SlateDbWalReader {
    table_store: Arc<TableStore>,
    status: watch::Receiver<DbStatus>,
}

impl SlateDbWalReader {
    pub(crate) fn new(table_store: Arc<TableStore>, status: watch::Receiver<DbStatus>) -> Self {
        Self {
            table_store,
            status,
        }
    }
}

#[async_trait]
impl WalReader for SlateDbWalReader {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        let valid_start = matches!(wal_file_id_range.0, Bound::Included(_));
        let valid_end = matches!(wal_file_id_range.1, Bound::Excluded(_) | Bound::Unbounded);
        if !valid_start || !valid_end {
            return Err(WalError::InternalError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "native WAL reader requires an included start and excluded or unbounded end",
            ))));
        }
        let iterator = WalReplayIterator::range(
            wal_file_id_range,
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
                poll_interval: Duration::from_secs(1),
            },
            Arc::clone(&self.table_store),
            self.status.clone(),
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
        let wal_reader = SlateDbWalReader::new(table_store, db.subscribe());

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

        let mut unbounded_iterator = wal_reader.iterator((last_wal_id..).into()).await.unwrap();
        let wal_rows = unbounded_iterator
            .next()
            .await
            .unwrap()
            .expect("unbounded iterator returned None");
        assert_eq!(wal_rows.rows.len(), 1);
        assert_eq!(wal_rows.rows[0].key.as_ref(), b"key");
    }
}
