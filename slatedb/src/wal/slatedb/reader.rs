use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::error;
use object_store::{path::Path, ObjectStore};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};

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

/// Builder for a [`SlateDbWalReader`].
///
/// Callers must configure both the database path with [`Self::with_path`] and
/// the primary object store with [`Self::with_object_store`]. By default, the
/// primary object store is used for both the manifest and WAL files. Use
/// [`Self::with_wal_object_store`] when WAL files are stored separately.
///
/// Reader options and the system clock use their defaults when they are not
/// explicitly configured.
pub struct SlateDbWalReaderBuilder {
    path: Option<Path>,
    table_store: Option<Arc<TableStore>>,
    object_store: Option<Arc<dyn ObjectStore>>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    manifest_reader: Option<Arc<dyn ManifestReader>>,
    system_clock: Arc<dyn SystemClock>,
    options: SlateDbWalReaderOptions,
}

impl Default for SlateDbWalReaderBuilder {
    fn default() -> Self {
        Self {
            path: None,
            table_store: None,
            object_store: None,
            wal_object_store: None,
            manifest_reader: None,
            system_clock: Arc::new(DefaultSystemClock::new()),
            options: SlateDbWalReaderOptions::default(),
        }
    }
}

impl SlateDbWalReaderBuilder {
    /// Creates a builder with default reader options and system clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the root path of the database to read.
    pub fn with_path(mut self, path: Path) -> Self {
        self.path = Some(path);
        self
    }

    /// Sets an existing table store for internal construction.
    pub(crate) fn with_table_store(mut self, table_store: Arc<TableStore>) -> Self {
        self.table_store = Some(table_store);
        self
    }

    /// Sets the primary object store used to read the manifest and, by
    /// default, WAL files.
    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Sets a dedicated object store from which WAL files are read.
    ///
    /// The primary object store configured by [`Self::with_object_store`]
    /// remains the source for the database manifest.
    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    /// Sets the clock used to wait between polls by live WAL iterators.
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Sets the options controlling how WAL files are read.
    pub fn with_options(mut self, options: SlateDbWalReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets an existing manifest reader for internal construction.
    pub(crate) fn with_manifest_reader(mut self, manifest_reader: Arc<dyn ManifestReader>) -> Self {
        self.manifest_reader = Some(manifest_reader);
        self
    }

    /// Builds a WAL reader from the configured state.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error when the database path or
    /// primary object store has not been configured. Internal callers may
    /// instead provide both a table store and manifest reader.
    pub fn build(self) -> Result<SlateDbWalReader, crate::Error> {
        let manifest_reader = match self.manifest_reader {
            Some(manifest_reader) => manifest_reader,
            None => {
                let Some(object_store) = self.object_store.clone() else {
                    return Err(crate::Error::invalid(
                        "must specify object store".to_string(),
                    ));
                };
                let Some(path) = self.path.clone() else {
                    return Err(crate::Error::invalid("must specify db path".to_string()));
                };
                Arc::new(ManifestStore::new(&path, object_store))
            }
        };
        let table_store = match self.table_store {
            Some(table_store) => table_store,
            None => {
                let Some(object_store) = self.object_store.clone() else {
                    return Err(crate::Error::invalid(
                        "must specify object store".to_string(),
                    ));
                };
                let Some(path) = self.path.clone() else {
                    return Err(crate::Error::invalid("must specify db path".to_string()));
                };
                Arc::new(TableStore::new(
                    ObjectStores::new(object_store, self.wal_object_store.clone()),
                    SsTableFormat::default(),
                    path.clone(),
                    None,
                    TableStoreKind::Reader,
                    BlockCachePolicy::default(),
                ))
            }
        };
        Ok(SlateDbWalReader {
            table_store,
            manifest_reader,
            system_clock: self.system_clock,
            options: self.options,
        })
    }
}

pub struct SlateDbWalReader {
    table_store: Arc<TableStore>,
    manifest_reader: Arc<dyn ManifestReader>,
    system_clock: Arc<dyn SystemClock>,
    options: SlateDbWalReaderOptions,
}

impl SlateDbWalReader {
    pub(crate) fn new_with_status_manager(
        table_store: Arc<TableStore>,
        db_status: &DbStatusManager,
        system_clock: Arc<dyn SystemClock>,
        options: SlateDbWalReaderOptions,
    ) -> Self {
        let manifest_reader: Arc<dyn ManifestReader> = Arc::new(db_status.subscribe());
        SlateDbWalReaderBuilder::new()
            .with_table_store(table_store)
            .with_manifest_reader(manifest_reader)
            .with_system_clock(system_clock)
            .with_options(options)
            .build()
            .expect("table store and manifest reader initialize a WAL reader")
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
                system_clock: Arc::clone(&self.system_clock),
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
    fn end_after(wal_id: u64) -> u64 {
        wal_id.checked_add(1).expect("test WAL ID overflow")
    }

    fn assert_invalid_build(builder: SlateDbWalReaderBuilder, expected_message: &str) {
        let error = match builder.build() {
            Ok(_) => panic!("expected WAL reader builder to reject incomplete state"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), crate::ErrorKind::Invalid);
        assert!(
            error.to_string().contains(expected_message),
            "expected error containing {expected_message:?}, got {error}"
        );
    }

    #[test]
    fn builder_rejects_missing_object_store() {
        assert_invalid_build(
            SlateDbWalReaderBuilder::new().with_path(Path::from("/missing-object-store")),
            "must specify object store",
        );
    }

    #[test]
    fn builder_rejects_missing_db_path() {
        assert_invalid_build(
            SlateDbWalReaderBuilder::new().with_object_store(Arc::new(InMemory::new())),
            "must specify db path",
        );
    }

    #[test]
    fn builder_rejects_table_store_without_manifest_reader() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/table-store-without-manifest-reader"),
            None,
            TableStoreKind::Reader,
            BlockCachePolicy::default(),
        ));

        assert_invalid_build(
            SlateDbWalReaderBuilder::new().with_table_store(table_store),
            "must specify object store",
        );
    }

    #[test]
    fn builder_rejects_manifest_reader_without_table_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/manifest-reader-without-table-store");
        let manifest_reader: Arc<dyn ManifestReader> =
            Arc::new(ManifestStore::new(&path, object_store));

        assert_invalid_build(
            SlateDbWalReaderBuilder::new().with_manifest_reader(manifest_reader),
            "must specify object store",
        );
    }

    #[test]
    fn builder_accepts_table_store_and_manifest_reader() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/table-store-and-manifest-reader");
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

        assert!(SlateDbWalReaderBuilder::new()
            .with_table_store(table_store)
            .with_manifest_reader(manifest_reader)
            .build()
            .is_ok());
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(Arc::clone(&object_store))
            .with_path(path)
            .build()
            .unwrap();
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(path)
            .build()
            .unwrap();
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(path)
            .build()
            .unwrap();
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
        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(path)
            .build()
            .unwrap();
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
        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(Path::from("/reader_rejects_an_unbounded_start_range"))
            .build()
            .unwrap();

        let result = wal_reader
            .iterator(WalFileRange(Bound::Unbounded, Bound::Unbounded))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_normalize_excluded_start_and_included_end_bounds() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(Path::from("/reader_normalizes_range_bounds"))
            .build()
            .unwrap();
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_wal_object_store(wal_object_store)
            .with_path(path)
            .build()
            .unwrap();
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(Arc::clone(&object_store))
            .with_path(path.clone())
            .build()
            .unwrap();
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

        let wal_reader = SlateDbWalReaderBuilder::new()
            .with_object_store(object_store)
            .with_path(path)
            .build()
            .unwrap();
        assert!(matches!(
            wal_reader.last_wal_file_id(0).await,
            Err(WalError::WalTruncated(1))
        ));
    }
}
