use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::error;
use object_store::{path::Path, ObjectStore};

use crate::block_cache_policy::BlockCachePolicy;
use crate::db_status::DbStatusManager;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::iter::IterationOrder;
use crate::manifest::store::ManifestStore;
use crate::object_stores::ObjectStores;
use crate::sst_iter::SstIteratorOptions;
use crate::tablestore::{TableStore, TableStoreKind};
use crate::wal::slatedb::iterator::{
    ManifestReader, SlateDbWalIterator, SlateDbWalIteratorOptions, WalIteratorEndBound,
};
use crate::wal::{WalError, WalFileRange, WalIterator, WalReader};

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
    /// Creates a reader for a database whose manifest and WAL use the same object store.
    pub fn new_for_db(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        Self::new_for_db_with_stores(object_store, None, path, options)
    }

    /// Creates a reader for a database with a dedicated WAL object store.
    ///
    /// The manifest is read from `object_store`, while WAL files are read from
    /// `wal_object_store`.
    pub fn new_for_db_with_wal_object_store(
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Arc<dyn ObjectStore>,
        path: Path,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        Self::new_for_db_with_stores(object_store, Some(wal_object_store), path, options)
    }

    fn new_for_db_with_stores(
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
        path: Path,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(Arc::clone(&object_store), wal_object_store),
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
        let from_wal_id = match wal_file_id_range.0 {
            Bound::Included(wal_id) => wal_id,
            Bound::Excluded(wal_id) => wal_id.checked_add(1).ok_or_else(|| {
                error!(
                    "WAL iterator start bound overflowed. [range={:?}]",
                    wal_file_id_range
                );
                SlateDBError::InvalidDBState
            })?,
            Bound::Unbounded => {
                error!(
                    "WAL iterator range must have a bounded start. [range={:?}]",
                    wal_file_id_range
                );
                return Err(SlateDBError::InvalidDBState.into());
            }
        };
        let end_bound = match wal_file_id_range.1 {
            Bound::Included(wal_id) => {
                WalIteratorEndBound::Exclusive(wal_id.checked_add(1).ok_or_else(|| {
                    error!(
                        "WAL iterator end bound overflowed. [range={:?}]",
                        wal_file_id_range
                    );
                    SlateDBError::InvalidDBState
                })?)
            }
            Bound::Excluded(wal_id) => WalIteratorEndBound::Exclusive(wal_id),
            Bound::Unbounded => WalIteratorEndBound::Unbounded {
                manifest_reader: Arc::clone(&self.manifest_reader),
                poll_interval: Duration::from_secs(1),
            },
        };
        let iterator = SlateDbWalIterator::range(
            from_wal_id,
            end_bound,
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
    use std::ops::Bound;
    use std::time::Duration;

    use super::*;
    use crate::config::{FlushOptions, FlushType};
    use crate::db_state::SsTableId;
    use crate::manifest::store::StoredManifest;
    use crate::manifest::ManifestCore;
    use crate::paths::PathResolver;
    use crate::test_utils::StringConcatMergeOperator;
    use crate::types::ValueDeletable;
    use crate::wal::WalRows;
    use crate::Db;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use slatedb_common::clock::DefaultSystemClock;

    fn end_after(wal_id: u64) -> u64 {
        wal_id.checked_add(1).expect("test WAL ID overflow")
    }

    async fn collect_batches(
        wal_reader: &SlateDbWalReader,
        start_wal_id: u64,
        end_wal_id_exclusive: u64,
    ) -> Result<Vec<WalRows>, WalError> {
        let mut iterator = wal_reader
            .iterator((start_wal_id..end_wal_id_exclusive).into())
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = iterator.next().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    #[tokio::test]
    async fn bounded_polling_discovers_new_wals_and_advances_the_cursor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/bounded_polling_discovers_new_wals");
        let db = Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        db.put(b"first", b"value-1").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = SlateDbWalReader::new_for_db(
            Arc::clone(&object_store),
            path,
            SlateDbWalReaderOptions::default(),
        );
        let first_tail = wal_reader.last_wal_file_id(0).await.unwrap();
        let first_batches = collect_batches(&wal_reader, 1, end_after(first_tail))
            .await
            .unwrap();
        let first_cursors: Vec<_> = first_batches
            .iter()
            .map(|batch| batch.last_consumed_wal_file_id)
            .collect();
        assert_eq!(first_cursors, (1..=first_tail).collect::<Vec<_>>());
        let first_rows: Vec<_> = first_batches
            .iter()
            .flat_map(|batch| batch.rows.iter())
            .collect();
        assert_eq!(first_rows.len(), 1);
        assert_eq!(first_rows[0].key.as_ref(), b"first");

        let mut cursor = first_batches.last().unwrap().last_consumed_wal_file_id;
        assert_eq!(cursor, first_tail);
        assert_eq!(wal_reader.last_wal_file_id(cursor).await.unwrap(), cursor);

        db.put(b"second", b"value-2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let second_tail = wal_reader.last_wal_file_id(cursor).await.unwrap();
        assert!(second_tail > cursor);
        let second_batches = collect_batches(
            &wal_reader,
            cursor.checked_add(1).unwrap(),
            end_after(second_tail),
        )
        .await
        .unwrap();
        let second_rows: Vec<_> = second_batches
            .iter()
            .flat_map(|batch| batch.rows.iter())
            .collect();
        assert_eq!(second_rows.len(), 1);
        assert_eq!(second_rows[0].key.as_ref(), b"second");
        cursor = second_batches.last().unwrap().last_consumed_wal_file_id;
        assert_eq!(cursor, second_tail);
        assert_eq!(wal_reader.last_wal_file_id(cursor).await.unwrap(), cursor);
    }

    #[tokio::test]
    async fn bounded_iteration_preserves_value_tombstone_merge_and_sequence_order() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/bounded_iteration_preserves_row_kinds");
        let db = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        db.put(b"a", b"1").await.unwrap();
        db.put(b"b", b"2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
        db.delete(b"a").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
        db.merge(b"m", b"x").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader =
            SlateDbWalReader::new_for_db(object_store, path, SlateDbWalReaderOptions::default());
        let tail = wal_reader.last_wal_file_id(0).await.unwrap();
        let batches = collect_batches(&wal_reader, 1, end_after(tail))
            .await
            .unwrap();
        assert_eq!(batches.last().unwrap().last_consumed_wal_file_id, tail);
        let rows: Vec<_> = batches.into_iter().flat_map(|batch| batch.rows).collect();
        assert_eq!(rows.len(), 4);
        assert!(rows.windows(2).all(|pair| pair[0].seq < pair[1].seq));
        assert_eq!(rows[0].key.as_ref(), b"a");
        assert!(matches!(
            &rows[0].value,
            ValueDeletable::Value(value) if value.as_ref() == b"1"
        ));
        assert_eq!(rows[1].key.as_ref(), b"b");
        assert!(matches!(
            &rows[1].value,
            ValueDeletable::Value(value) if value.as_ref() == b"2"
        ));
        assert_eq!(rows[2].key.as_ref(), b"a");
        assert!(matches!(rows[2].value, ValueDeletable::Tombstone));
        assert_eq!(rows[3].key.as_ref(), b"m");
        assert!(matches!(
            &rows[3].value,
            ValueDeletable::Merge(value) if value.as_ref() == b"x"
        ));
    }

    #[tokio::test]
    async fn empty_fence_wal_advances_the_cursor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/empty_fence_wal_advances_the_cursor");
        let _db = Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();

        let wal_reader =
            SlateDbWalReader::new_for_db(object_store, path, SlateDbWalReaderOptions::default());
        let tail = wal_reader.last_wal_file_id(0).await.unwrap();
        let batches = collect_batches(&wal_reader, 1, end_after(tail))
            .await
            .unwrap();

        assert_eq!(tail, 1);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].rows.is_empty());
        assert_eq!(batches[0].last_consumed_wal_file_id, 1);
    }

    #[tokio::test]
    async fn should_accept_an_unbounded_end_range() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/reader_accepts_an_unbounded_end_range");
        let _db = Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        let wal_reader =
            SlateDbWalReader::new_for_db(object_store, path, SlateDbWalReaderOptions::default());
        let mut iterator = wal_reader.iterator((1..).into()).await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), iterator.next())
            .await
            .expect("unbounded iterator did not observe the fence WAL")
            .unwrap()
            .expect("unbounded iterator returned None");
        assert!(first.rows.is_empty());
        assert_eq!(first.last_consumed_wal_file_id, 1);
    }

    #[tokio::test]
    async fn should_reject_an_unbounded_start_range() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_reader = SlateDbWalReader::new_for_db(
            object_store,
            Path::from("/reader_rejects_an_unbounded_start_range"),
            SlateDbWalReaderOptions::default(),
        );

        let result = wal_reader
            .iterator(WalFileRange(Bound::Unbounded, Bound::Unbounded))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_normalize_excluded_start_and_included_end_bounds() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_reader = SlateDbWalReader::new_for_db(
            object_store,
            Path::from("/reader_normalizes_range_bounds"),
            SlateDbWalReaderOptions::default(),
        );
        wal_reader.table_store.write_wal_fence(1).await.unwrap();
        wal_reader.table_store.write_wal_fence(2).await.unwrap();

        let mut iterator = wal_reader
            .iterator(WalFileRange(Bound::Excluded(1), Bound::Included(2)))
            .await
            .unwrap();
        let batch = iterator.next().await.unwrap().unwrap();
        assert_eq!(batch.last_consumed_wal_file_id, 2);
        assert!(batch.rows.is_empty());
        assert!(iterator.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reads_manifest_and_wals_from_separate_object_stores() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/reader_with_dedicated_wal_store");
        let db = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_wal_object_store(Arc::clone(&wal_object_store))
            .build()
            .await
            .unwrap();
        db.put(b"dedicated", b"wal-store").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = SlateDbWalReader::new_for_db_with_wal_object_store(
            object_store,
            wal_object_store,
            path,
            SlateDbWalReaderOptions::default(),
        );
        let tail = wal_reader.last_wal_file_id(0).await.unwrap();
        let rows: Vec<_> = collect_batches(&wal_reader, 1, end_after(tail))
            .await
            .unwrap()
            .into_iter()
            .flat_map(|batch| batch.rows)
            .collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.as_ref(), b"dedicated");
        assert!(matches!(
            &rows[0].value,
            ValueDeletable::Value(value) if value.as_ref() == b"wal-store"
        ));
    }

    #[tokio::test]
    async fn missing_wal_in_a_bounded_range_returns_truncation() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/missing_wal_in_a_bounded_range");
        let db = Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = SlateDbWalReader::new_for_db(
            Arc::clone(&object_store),
            path.clone(),
            SlateDbWalReaderOptions::default(),
        );
        let tail = wal_reader.last_wal_file_id(0).await.unwrap();
        let wal_path = PathResolver::from_root(path).sst_path(&SsTableId::Wal(tail));
        object_store.delete(&wal_path).await.unwrap();

        let mut iterator = wal_reader
            .iterator((tail..end_after(tail)).into())
            .await
            .unwrap();
        assert!(matches!(
            iterator.next().await,
            Err(WalError::WalTruncated(wal_id)) if wal_id == tail
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
