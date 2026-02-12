//! This module provides a read-only API for inspecting WAL SST files that were
//! flushed by SlateDB writers.
//!
//! The API has the following main types:
//!
//! - [`WalReader`]: opens a WAL namespace and lists WAL files.
//! - [`WalFile`]: one WAL file (`id`) plus accessors for metadata and contents.
//! - [`WalFileMetadata`]: metadata for one WAL file (`last_modified_dt`, `size_bytes`).
//! - [`WalFileIterator`]: entry-level iterator over a WAL file.
//!
//! WAL files returned by [`WalReader::list`] are ordered by WAL ID in ascending
//! order. Iterating each file with [`WalFile::iterator`] yields [`RowEntry`]
//! values in the order they are stored in that WAL SST.
//!
//! `WalFileIterator` intentionally exposes `next_entry` (and seek/init), not
//! `next`. This keeps the API at entry level and preserves tombstones and merge
//! rows exactly as written to the WAL.
//!
//! # Listing costs and polling strategy
//!
//! The `list()` API can become expensive when WAL retention is high or GC is not
//! keeping up. If the GC is not running, listings can grow without bound. Even
//! with GC, CDC often needs higher retention. Retaining WAL files for just
//! 1 hour can yield tens of thousands of files, which is expensive to list in
//! both cost and time.
//!
//! If you plan to poll frequently, use `list()` once to establish a baseline,
//! then poll with `WalReader::get(latest_id + 1)` to avoid repeated large
//! listings.
//!
//! # Example
//!
//! ```
//! use slatedb::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
//! use slatedb::object_store::memory::InMemory;
//! use slatedb::{Db, Error, WalReader};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let path = "/wal_reader_example";
//!     let db = Db::open(path, object_store.clone()).await?;
//!
//!     db.put_with_options(
//!         b"k1",
//!         b"v1",
//!         &PutOptions::default(),
//!         &WriteOptions::default(),
//!     )
//!     .await?;
//!     db.flush_with_options(FlushOptions {
//!         flush_type: FlushType::Wal,
//!     })
//!     .await?;
//!
//!     let reader = WalReader::new(path, object_store);
//!     for wal_file in reader.list(..).await? {
//!         if let Some(mut iter) = wal_file.iterator().await? {
//!             while let Some(entry) = iter.next_entry().await? {
//!                 let _ = entry;
//!             }
//!         }
//!     }
//!     Ok(())
//! }
//! ```

use std::ops::RangeBounds;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::ObjectStore;

use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::iter::{EmptyIterator, KeyValueIterator};
use crate::object_stores::ObjectStores;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// Iterator over entries in a WAL file.
pub struct WalFileIterator {
    iter: Box<dyn KeyValueIterator + 'static>,
}

impl WalFileIterator {
    fn new(iter: Box<dyn KeyValueIterator + 'static>) -> Self {
        Self { iter }
    }

    /// Initializes the iterator.
    pub async fn init(&mut self) -> Result<(), crate::Error> {
        self.iter.init().await.map_err(Into::into)
    }

    /// Returns the next entry in the WAL file.
    pub async fn next_entry(&mut self) -> Result<Option<RowEntry>, crate::Error> {
        self.iter.next_entry().await.map_err(Into::into)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFileMetadata {
    /// The time this WAL file was last written to object storage.
    pub last_modified_dt: DateTime<Utc>,

    /// The size of this WAL file in bytes.
    pub size_bytes: u64,

    /// The path of this WAL file in object storage.
    pub location: Path,
}

/// Represents a single WAL file stored in object storage and provides methods
/// to inspect and read its contents.
pub struct WalFile {
    /// The unique identifier for this WAL file. Corresponds to the SST filename without
    /// the extension. For example, file `000123.sst` would have id `123`.
    pub id: u64,

    table_store: Arc<TableStore>,
}

impl WalFile {
    /// Returns metadata for this WAL file.
    ///
    /// Returns `Ok(None)` if the WAL file no longer exists.
    pub async fn metadata(&self) -> Result<Option<WalFileMetadata>, crate::Error> {
        let metadata = match self.table_store.metadata(&SsTableId::Wal(self.id)).await {
            Ok(metadata) => metadata,
            Err(err) if is_object_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        Ok(Some(WalFileMetadata {
            last_modified_dt: metadata.last_modified,
            size_bytes: metadata.size,
            location: metadata.location,
        }))
    }

    /// Returns an iterator over `RowEntry`s in this WAL file. Raises an error if the
    /// WAL file could not be read.
    ///
    /// Returns `Ok(None)` if the WAL file no longer exists.
    pub async fn iterator(&self) -> Result<Option<WalFileIterator>, crate::Error> {
        let sst = match self.table_store.open_sst(&SsTableId::Wal(self.id)).await {
            Ok(sst) => sst,
            Err(err) if is_object_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let iter = match SstIterator::new_owned_initialized(
            ..,
            sst,
            Arc::clone(&self.table_store),
            SstIteratorOptions::default(),
        )
        .await
        {
            Ok(Some(iter)) => Box::new(iter) as Box<dyn KeyValueIterator + 'static>,
            Ok(None) => Box::new(EmptyIterator::new()) as Box<dyn KeyValueIterator + 'static>,
            Err(err) if is_object_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        Ok(Some(WalFileIterator::new(iter)))
    }

    /// Returns the WAL ID immediately following this file's ID.
    pub fn next_id(&self) -> u64 {
        self.id + 1
    }

    /// Returns a [`WalFile`] handle for the next WAL file after this one.
    ///
    /// This does not check whether the next WAL file actually exists in object storage.
    pub fn next_file(&self) -> Self {
        Self {
            id: self.next_id(),
            table_store: Arc::clone(&self.table_store),
        }
    }
}

fn is_object_not_found(err: &SlateDBError) -> bool {
    matches!(
        err,
        SlateDBError::ObjectStoreError(source)
            if matches!(source.as_ref(), object_store::Error::NotFound { .. })
    )
}

/// Reads WAL files in object storage for a specific database.
pub struct WalReader {
    table_store: Arc<TableStore>,
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
        Self { table_store }
    }

    /// Lists WAL files in ascending order by their ID within the specified range.
    /// If `range` is unbounded, all WAL files are returned.
    pub async fn list<R: RangeBounds<u64>>(&self, range: R) -> Result<Vec<WalFile>, crate::Error> {
        let result = self.table_store.list_wal_ssts(range).await;
        Ok(result?
            .into_iter()
            .map(|wal_file| WalFile {
                id: wal_file.id.unwrap_wal_id(),
                table_store: Arc::clone(&self.table_store),
            })
            .collect())
    }

    /// Creates a [`WalFile`] handle for a WAL ID.
    pub fn get(&self, id: u64) -> WalFile {
        WalFile {
            id,
            table_store: Arc::clone(&self.table_store),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
    use crate::test_utils::StringConcatMergeOperator;
    use crate::types::ValueDeletable;
    use crate::Db;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_list_and_iterator() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader";
        let db = Db::open(path, main_store.clone()).await.unwrap();
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
            let mut iter = wal_file
                .iterator()
                .await
                .unwrap()
                .expect("WAL file listed by WalReader should be readable");
            while let Some(entry) = iter.next_entry().await.unwrap() {
                rows.push(entry);
            }
        }
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key.as_ref(), b"k1");
        assert!(matches!(
            &rows[0].value,
            ValueDeletable::Value(value) if value.as_ref() == b"v1"
        ));
        assert_eq!(rows[1].key.as_ref(), b"k2");
        assert!(matches!(
            &rows[1].value,
            ValueDeletable::Value(value) if value.as_ref() == b"v2"
        ));
    }

    #[tokio::test]
    async fn test_reads_from_wal_object_store() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_store";
        let db = Db::builder(path, main_store.clone())
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
            let mut iter = wal_file
                .iterator()
                .await
                .unwrap()
                .expect("WAL file listed by WalReader should be readable");
            while let Some(entry) = iter.next_entry().await.unwrap() {
                rows.push(entry);
            }
        }
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.as_ref(), b"k1");
        assert!(matches!(
            &rows[0].value,
            ValueDeletable::Value(value) if value.as_ref() == b"v1"
        ));
    }

    #[tokio::test]
    async fn test_wal_file_metadata_matches_object_store_metadata() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_metadata";
        let db = Db::open(path, main_store.clone()).await.unwrap();
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

        let wal_reader = WalReader::new(path, main_store.clone());
        let wal_files = wal_reader.list(..).await.unwrap();
        assert!(!wal_files.is_empty());

        for wal_file in wal_files {
            let wal_metadata = wal_file
                .metadata()
                .await
                .unwrap()
                .expect("WAL file listed by WalReader should have metadata");
            let object_metadata = main_store.head(&wal_metadata.location).await.unwrap();
            assert_eq!(wal_metadata.last_modified_dt, object_metadata.last_modified);
            assert_eq!(wal_metadata.size_bytes, object_metadata.size);
            assert_eq!(wal_metadata.location, object_metadata.location);
        }
    }

    #[tokio::test]
    async fn test_get_returns_wal_file_when_exists() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_get_exists";
        let db = Db::open(path, main_store.clone()).await.unwrap();
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

        let wal_reader = WalReader::new(path, main_store);
        let wal_file = wal_reader
            .list(..)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("expected at least one WAL file");
        let fetched = wal_reader.get(wal_file.id);
        assert_eq!(fetched.id, wal_file.id);
    }

    #[tokio::test]
    async fn test_get_returns_wal_file_for_id() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_get_missing";
        let wal_reader = WalReader::new(path, main_store);
        let wal_file = wal_reader.get(42);
        assert_eq!(wal_file.id, 42);
    }

    #[test]
    fn test_wal_file_next_id_and_next_file() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_next_id";
        let wal_reader = WalReader::new(path, main_store);
        let wal_file = wal_reader.get(41);

        assert_eq!(wal_file.next_id(), 42);

        let next = wal_file.next_file();
        assert_eq!(next.id, 42);
        assert!(Arc::ptr_eq(&wal_file.table_store, &next.table_store));
    }

    #[tokio::test]
    async fn test_metadata_returns_none_when_file_deleted() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_missing_metadata";
        let db = Db::open(path, main_store.clone()).await.unwrap();
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

        let wal_reader = WalReader::new(path, main_store.clone());
        let wal_file = wal_reader
            .list(..)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("expected at least one WAL file");
        let wal_metadata = wal_file
            .metadata()
            .await
            .unwrap()
            .expect("expected WAL metadata before delete");

        main_store.delete(&wal_metadata.location).await.unwrap();
        assert!(wal_file.metadata().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iterator_returns_none_when_file_deleted() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_missing_iterator";
        let db = Db::open(path, main_store.clone()).await.unwrap();
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

        let wal_reader = WalReader::new(path, main_store.clone());
        let wal_file = wal_reader
            .list(..)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("expected at least one WAL file");
        let wal_metadata = wal_file
            .metadata()
            .await
            .unwrap()
            .expect("expected WAL metadata before delete");

        main_store.delete(&wal_metadata.location).await.unwrap();
        assert!(wal_file.iterator().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iterator_returns_tombstones() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_tombstones";
        let db = Db::open(path, main_store.clone()).await.unwrap();

        db.delete(b"k_tombstone").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = WalReader::new(path, main_store);
        let wal_files = wal_reader.list(..).await.unwrap();
        assert!(!wal_files.is_empty());

        let mut rows = Vec::new();
        for wal_file in wal_files {
            let mut iter = wal_file
                .iterator()
                .await
                .unwrap()
                .expect("WAL file listed by WalReader should be readable");
            while let Some(entry) = iter.next_entry().await.unwrap() {
                rows.push(entry);
            }
        }

        let tombstone_entry = rows
            .iter()
            .find(|entry| entry.key.as_ref() == b"k_tombstone")
            .expect("expected deleted key in WAL iterator output");
        assert!(matches!(tombstone_entry.value, ValueDeletable::Tombstone));
    }

    #[tokio::test]
    async fn test_iterator_returns_merge_operands() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_wal_reader_merges";
        let db = Db::builder(path, main_store.clone())
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        db.merge(b"k_merge", b"merge_operand").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let wal_reader = WalReader::new(path, main_store);
        let wal_files = wal_reader.list(..).await.unwrap();
        assert!(!wal_files.is_empty());

        let mut rows = Vec::new();
        for wal_file in wal_files {
            let mut iter = wal_file
                .iterator()
                .await
                .unwrap()
                .expect("WAL file listed by WalReader should be readable");
            while let Some(entry) = iter.next_entry().await.unwrap() {
                rows.push(entry);
            }
        }

        let merge_entry = rows
            .iter()
            .find(|entry| entry.key.as_ref() == b"k_merge")
            .expect("expected merge key in WAL iterator output");
        assert!(matches!(
            &merge_entry.value,
            ValueDeletable::Merge(value) if value.as_ref() == b"merge_operand"
        ));
    }
}
