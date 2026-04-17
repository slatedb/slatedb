//! Read-only, metadata-only API for inspecting compacted SST files.
//!
//! The main types are:
//!
//! - [`SstReader`]: opens compacted SSTs by ULID or from an existing handle.
//! - [`SstFile`]: one compacted SST with accessors for metadata, stats, and index.
//!
//! # Example
//!
//! ```
//! use slatedb::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
//! use slatedb::object_store::memory::InMemory;
//! use slatedb::{Db, DbMetadataOps, SstReader};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), slatedb::Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let path = "/sst_reader_example";
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
//!         flush_type: FlushType::MemTable,
//!     })
//!     .await?;
//!
//!     let manifest = db.manifest();
//!     let reader = SstReader::new(path, object_store, None, None);
//!
//!     // Inspect L0 SSTs
//!     for view in manifest.l0() {
//!         let sst_file = reader.open_with_handle(view.sst.clone())?;
//!         if let Some(stats) = sst_file.stats().await? {
//!             let _ = stats.num_puts;
//!         }
//!     }
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use ulid::Ulid;

use crate::db_cache::DbCache;
use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo};
use crate::format::sst::{BlockTransformer, SsTableFormat};
use crate::object_stores::ObjectStores;
use crate::sst_stats::SstStats;
use crate::tablestore::{SstFileMetadata, TableStore};

/// Opens compacted SST files for read-only inspection.
///
/// `SstReader` wraps the read-only functionality needed to open individual
/// SSTs and inspect their metadata, stats, and index data. SST paths are
/// resolved as `{root}/compacted/{ulid}.sst`.
pub struct SstReader {
    table_store: Arc<TableStore>,
}

impl SstReader {
    /// Creates a new SST reader for the database at the given path.
    ///
    /// The `object_store` should point at the same store used by the `Db`.
    /// An optional `DbCache` can be provided for in-memory caching of index
    /// and stats blocks. If the database was opened with a `BlockTransformer`
    /// (e.g. for encryption), the same transformer must be provided here.
    pub fn new<P: Into<Path>>(
        root_path: P,
        object_store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn DbCache>>,
        block_transformer: Option<Arc<dyn BlockTransformer>>,
    ) -> Self {
        let sst_format = SsTableFormat {
            block_transformer,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            sst_format,
            root_path.into(),
            cache,
        ));
        Self { table_store }
    }

    /// Opens a compacted SST by its ULID.
    ///
    /// This reads the SST footer and metadata from object storage.
    ///
    /// ## Errors
    ///
    /// Returns an error if the SST file does not exist (e.g. it was GC'd)
    /// or if there is an issue reading from object storage.
    pub async fn open(&self, id: Ulid) -> Result<SstFile, crate::Error> {
        let handle = self.table_store.open_sst(&SsTableId::Compacted(id)).await?;
        Ok(SstFile {
            id,
            handle,
            table_store: Arc::clone(&self.table_store),
        })
    }

    /// Creates an `SstFile` from an existing `SsTableHandle` (no I/O needed).
    ///
    /// ## Errors
    ///
    /// Returns an error if the handle is not a compacted SST (e.g. a WAL SST).
    pub fn open_with_handle(&self, handle: SsTableHandle) -> Result<SstFile, crate::Error> {
        let id = match handle.id {
            SsTableId::Compacted(ulid) => ulid,
            SsTableId::Wal(_) => {
                return Err(crate::Error::invalid(
                    "SstReader only supports compacted SSTs, not WAL SSTs".to_string(),
                ));
            }
        };
        Ok(SstFile {
            id,
            handle,
            table_store: Arc::clone(&self.table_store),
        })
    }
}

/// A single compacted SST file with accessors for metadata, stats, and index.
pub struct SstFile {
    id: Ulid,
    handle: SsTableHandle,
    table_store: Arc<TableStore>,
}

impl SstFile {
    /// Returns the SST's ULID identifier.
    pub fn id(&self) -> Ulid {
        self.id
    }

    /// Returns the SST file metadata from `SsTableInfo`.
    ///
    /// This is available without any I/O since the info is cached in the handle.
    pub fn info(&self) -> &SsTableInfo {
        &self.handle.info
    }

    /// Returns object store metadata for this SST file.
    ///
    /// This performs one HEAD request to the object store.
    ///
    /// ## Errors
    ///
    /// Returns an error if the SST file does not exist or if there is an
    /// issue reading from object storage.
    pub async fn metadata(&self) -> Result<SstFileMetadata, crate::Error> {
        self.table_store
            .metadata(&self.handle.id)
            .await
            .map_err(Into::into)
    }

    /// Reads the stats block from object storage.
    ///
    /// Returns `None` for old SSTs that were written before the stats block
    /// was added (i.e. `stats_offset == 0 && stats_len == 0`).
    ///
    /// ## Errors
    ///
    /// Returns an error if there is an issue reading from object storage.
    pub async fn stats(&self) -> Result<Option<SstStats>, crate::Error> {
        self.table_store
            .read_stats(&self.handle, true)
            .await
            .map_err(Into::into)
    }

    /// Returns `(block_offset, first_key)` pairs from the SST index block.
    ///
    /// The returned vector is parallel to the data blocks in the SST. Each
    /// entry contains the on-disk byte offset of the block and the first key
    /// stored in that block.
    ///
    /// ## Errors
    ///
    /// Returns an error if there is an issue reading from object storage.
    pub async fn index(&self) -> Result<Vec<(u64, Bytes)>, crate::Error> {
        let index = self.table_store.read_index(&self.handle, true).await?;
        let borrowed = index.borrow();
        let block_meta = borrowed.block_meta();
        let result: Vec<(u64, Bytes)> = (0..block_meta.len())
            .map(|i| {
                let meta = block_meta.get(i);
                (
                    meta.offset(),
                    Bytes::copy_from_slice(meta.first_key().bytes()),
                )
            })
            .collect();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FlushOptions, FlushType, PutOptions, SstBlockSize, WriteOptions};
    use crate::test_utils::StringConcatMergeOperator;
    use crate::{Db, DbMetadataOps};
    use object_store::memory::InMemory;

    /// Helper: create a DB with 10 puts, 3 deletes, and 2 merges, flush to
    /// L0, and return the object store + path + manifest for inspection.
    async fn setup_db_with_l0() -> (
        Arc<dyn ObjectStore>,
        &'static str,
        crate::manifest::VersionedManifest,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_sst_reader";
        let db = Db::builder(path, object_store.clone())
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .with_sst_block_size(SstBlockSize::Other(64))
            .build()
            .await
            .unwrap();

        // 10 puts
        for i in 0..10u8 {
            db.put_with_options(
                &[b'k', i],
                &[b'v', i],
                &PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
        }

        // 3 deletes
        for i in 10..13u8 {
            db.delete(&[b'k', i]).await.unwrap();
        }

        // 2 merges
        for i in 13..15u8 {
            db.merge(&[b'k', i], &[b'm', i]).await.unwrap();
        }

        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let manifest = db.manifest();
        db.close().await.unwrap();
        (object_store, path, manifest)
    }

    #[tokio::test]
    async fn test_open_and_info() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        assert!(
            !manifest.manifest.core.l0.is_empty(),
            "expected at least one L0 SST"
        );
        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader
            .open(view.sst.id.unwrap_compacted_id())
            .await
            .unwrap();

        let info = sst_file.info();
        assert!(info.first_entry.is_some());
        assert!(info.index_offset > 0);
    }

    #[tokio::test]
    async fn test_open_with_handle() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();

        assert_eq!(sst_file.id(), view.sst.id.unwrap_compacted_id());
        assert_eq!(sst_file.info(), &view.sst.info);
    }

    #[tokio::test]
    async fn test_stats() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();
        let stats = sst_file.stats().await.unwrap();

        let stats = stats.expect("expected stats block to be present");
        assert_eq!(stats.num_puts, 10);
        assert_eq!(stats.num_deletes, 3);
        assert_eq!(stats.num_merges, 2);
        assert_eq!(stats.num_rows(), 15);
        assert!(stats.raw_key_size > 0);
        assert!(stats.raw_val_size > 0);
    }

    #[tokio::test]
    async fn test_index() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();
        let index = sst_file.index().await.unwrap();

        assert!(!index.is_empty());
        // First index key should be <= the SST's first entry (it may be a
        // shortened separator key rather than the exact first key).
        if let Some(first_entry) = sst_file.info().first_entry.as_ref() {
            assert!(index[0].1.as_ref() <= first_entry.as_ref());
        }
        // Offsets should be monotonically increasing
        for window in index.windows(2) {
            assert!(window[0].0 < window[1].0);
        }
    }

    #[tokio::test]
    async fn test_block_stats_parallel_to_index() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();

        let stats = sst_file
            .stats()
            .await
            .unwrap()
            .expect("expected stats block");
        let index = sst_file.index().await.unwrap();

        assert_eq!(
            stats.block_stats.len(),
            index.len(),
            "block_stats should be parallel to the index"
        );

        // Sum of per-block puts should equal aggregate puts
        let sum_puts: u64 = stats.block_stats.iter().map(|b| b.num_puts as u64).sum();
        assert_eq!(sum_puts, stats.num_puts);
    }

    #[tokio::test]
    async fn test_metadata() {
        let (store, path, manifest) = setup_db_with_l0().await;
        let reader = SstReader::new(path, store, None, None);

        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();
        let metadata = sst_file.metadata().await.unwrap();

        assert!(metadata.size > 0);
        assert!(matches!(metadata.id, SsTableId::Compacted(_)));
    }

    #[tokio::test]
    async fn test_open_with_wal_handle_returns_error() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reader = SstReader::new("/test", store, None, None);

        let wal_handle = SsTableHandle::new(
            SsTableId::Wal(42),
            0,
            crate::db_state::SsTableInfo::default(),
        );
        let result = reader.open_with_handle(wal_handle);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_missing_sst() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reader = SstReader::new("/nonexistent", store, None, None);

        let result = reader.open(Ulid::new()).await;
        assert!(result.is_err());
    }

    struct XorTransformer {
        key: u8,
    }

    #[async_trait::async_trait]
    impl BlockTransformer for XorTransformer {
        async fn encode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            let transformed: Vec<u8> = data.iter().map(|b| b ^ self.key).collect();
            Ok(Bytes::from(transformed))
        }

        async fn decode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            self.encode(data).await
        }
    }

    #[tokio::test]
    async fn test_stats_with_block_transformer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/test_sst_reader_transformer";
        let transformer: Arc<dyn BlockTransformer> = Arc::new(XorTransformer { key: 0x42 });

        // Write data with a block transformer
        let db = Db::builder(path, object_store.clone())
            .with_block_transformer(transformer.clone())
            .build()
            .await
            .unwrap();

        for i in 0..5u8 {
            db.put_with_options(
                &[b'k', i],
                &[b'v', i],
                &PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let manifest = db.manifest();
        db.close().await.unwrap();

        // Reading with the correct transformer should succeed
        let reader = SstReader::new(path, object_store.clone(), None, Some(transformer));
        let view = &manifest.manifest.core.l0[0];
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();
        let stats = sst_file.stats().await.unwrap().expect("expected stats");
        assert_eq!(stats.num_puts, 5);

        // Reading without the transformer should fail
        let reader_no_transformer = SstReader::new(path, object_store, None, None);
        let sst_file = reader_no_transformer
            .open_with_handle(view.sst.clone())
            .unwrap();
        assert!(sst_file.stats().await.is_err());
    }
}
