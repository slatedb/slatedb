use std::ops::RangeBounds;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::ObjectStore;

use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::iter::KeyValueIterator;
use crate::object_stores::ObjectStores;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// Represents a single WAL file stored in object storage. Contains metadata about the
/// WAL file as well as methods to read its contents.
pub struct WalFile {
    /// The unique identifier for this WAL file. Corresponds to the SST filename without
    /// the extension. For example, file `000123.sst` would have id `123`.
    pub id: u64,

    /// The time this WAL file was written to object storage.
    pub create_time: DateTime<Utc>,

    /// The size of this WAL file in bytes.
    pub size_bytes: u64,

    table_store: Arc<TableStore>,
    stats: stats::WalReaderStats,
}

impl WalFile {
    /// Reads and returns all `RowEntry`s in this WAL file. Raises an error if the
    /// WAL file could not be read.
    pub async fn rows(&self) -> Result<Vec<RowEntry>, crate::Error> {
        self.stats.rows_calls.inc();
        let result: Result<Vec<RowEntry>, SlateDBError> = async {
            let sst = self.table_store.open_sst(&SsTableId::Wal(self.id)).await?;
            let mut iter = match SstIterator::new_owned_initialized(
                ..,
                sst,
                Arc::clone(&self.table_store),
                SstIteratorOptions::default(),
            )
            .await?
            {
                Some(iter) => iter,
                None => return Ok(Vec::new()),
            };

            let mut rows = Vec::new();
            while let Some(entry) = iter.next_entry().await? {
                rows.push(entry);
            }
            Ok(rows)
        }
        .await;

        result.map_err(Into::into)
    }
}

/// Reads WAL files in object storage for a specific database.
pub struct WalReader {
    table_store: Arc<TableStore>,
    stat_registry: Arc<StatRegistry>,
    stats: stats::WalReaderStats,
}

impl WalReader {
    /// Creates a new WAL reader for the database at the given path.
    ///
    /// If the database was configured with a separate WAL object store, pass that
    /// object store here.
    pub fn new<P: Into<Path>>(path: P, object_store: Arc<dyn ObjectStore>) -> Self {
        let sst_format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            sst_format,
            path.into(),
            None,
        ));
        let stat_registry = Arc::new(StatRegistry::new());
        let stats = stats::WalReaderStats::new(stat_registry.as_ref());
        Self {
            table_store,
            stat_registry,
            stats,
        }
    }

    /// Lists WAL files in ascending order by their ID within the specified range.
    /// If `range` is unbounded, all WAL files are returned.
    pub async fn list<R: RangeBounds<u64>>(&self, range: R) -> Result<Vec<WalFile>, crate::Error> {
        self.stats.list_calls.inc();
        let result = self.table_store.list_wal_ssts(range).await;
        Ok(result?
            .into_iter()
            .map(|wal_file| WalFile {
                id: wal_file.id.unwrap_wal_id(),
                create_time: wal_file.last_modified,
                size_bytes: wal_file.size,
                table_store: Arc::clone(&self.table_store),
                stats: self.stats.clone(),
            })
            .collect())
    }

    /// Get the metrics registry for the WAL reader.
    pub fn metrics(&self) -> Arc<StatRegistry> {
        Arc::clone(&self.stat_registry)
    }
}

pub(crate) mod stats {
    use crate::stats::{Counter, StatRegistry};
    use std::sync::Arc;

    macro_rules! wal_reader_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("wal_reader", $suffix)
        };
    }

    pub(crate) const LIST_CALLS: &str = wal_reader_stat_name!("list_calls");
    pub(crate) const ROWS_CALLS: &str = wal_reader_stat_name!("rows_calls");

    #[derive(Clone)]
    pub(crate) struct WalReaderStats {
        pub(crate) list_calls: Arc<Counter>,
        pub(crate) rows_calls: Arc<Counter>,
    }

    impl WalReaderStats {
        pub(crate) fn new(registry: &StatRegistry) -> Self {
            let stats = Self {
                list_calls: Arc::new(Counter::default()),
                rows_calls: Arc::new(Counter::default()),
            };
            registry.register(LIST_CALLS, stats.list_calls.clone());
            registry.register(ROWS_CALLS, stats.rows_calls.clone());
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
    use crate::Db;
    use object_store::memory::InMemory;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_path(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}_{}", prefix, nanos)
    }

    #[tokio::test]
    async fn test_list_and_rows() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = unique_path("/test_wal_reader");
        let db = Db::open(path.clone(), main_store.clone()).await.unwrap();
        db.put_with_options(
            b"k1",
            b"v1",
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put_with_options(
            b"k2",
            b"v2",
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = WalReader::new(path, main_store.clone());
        let wal_files = wal_reader.list(..).await.unwrap();
        assert!(!wal_files.is_empty());
        let mut rows = Vec::new();
        for wal_file in wal_files {
            rows.extend(wal_file.rows().await.unwrap());
        }
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key.as_ref(), b"k1");
        assert_eq!(rows[1].key.as_ref(), b"k2");
    }

    #[tokio::test]
    async fn test_reads_from_wal_object_store() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = unique_path("/test_wal_store");
        let db = Db::builder(path.clone(), main_store.clone())
            .with_wal_object_store(wal_store.clone())
            .build()
            .await
            .unwrap();
        db.put_with_options(
            b"k1",
            b"v1",
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = WalReader::new(path, wal_store.clone());
        let wal_files = wal_reader.list(..).await.unwrap();
        assert!(!wal_files.is_empty());
        let mut rows = Vec::new();
        for wal_file in wal_files {
            rows.extend(wal_file.rows().await.unwrap());
        }
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.as_ref(), b"k1");
    }
}
