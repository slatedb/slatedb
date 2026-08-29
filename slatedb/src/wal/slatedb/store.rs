#![allow(dead_code)] // This store is intentionally implemented before its call sites are migrated.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::future::join_all;
use log::debug;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::Serialize;

use crate::db_state::{SsTableId, SsTableInfo, SstType};
use crate::error::SlateDBError;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::format::sst::{EncodedSsTable, SsTableFormat};
use crate::object_store_tag::{ObjectStoreCallTag, TableStoreKind};
use crate::paths::PathResolver;
use crate::sst_io::{read_obj, read_with_validation_retry, ReadOnlyObject};
use crate::wal::slatedb::sst_builder::EncodedWalSsTableBuilder;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct WalFileId(u64);

impl WalFileId {
    pub(crate) fn value(self) -> u64 {
        self.0
    }
}

impl From<u64> for WalFileId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct WalFileHandle {
    pub(crate) id: WalFileId,
    pub(crate) format_version: u16,
    pub(crate) info: SsTableInfo,
}

impl WalFileHandle {
    fn new(id: u64, format_version: u16, info: SsTableInfo) -> Self {
        Self {
            id: id.into(),
            format_version,
            info,
        }
    }
}

/// Cacheless storage adapter for SlateDB's object-store-backed WAL files.
///
/// WALs continue to use the shared SST format and builders. This type owns only
/// WAL object-store operations and deliberately has no `DbCache` integration.
pub(crate) struct WalTableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    path_resolver: PathResolver,
    fp_registry: Arc<FailPointRegistry>,
    kind: TableStoreKind,
}

impl WalTableStore {
    pub(crate) fn new<P: Into<Path>>(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: P,
        kind: TableStoreKind,
    ) -> Self {
        Self::new_with_fp_registry(
            object_store,
            sst_format,
            PathResolver::from_root(root_path),
            Arc::new(FailPointRegistry::new()),
            kind,
        )
    }

    pub(crate) fn new_with_fp_registry(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        path_resolver: PathResolver,
        fp_registry: Arc<FailPointRegistry>,
        kind: TableStoreKind,
    ) -> Self {
        Self {
            object_store,
            sst_format,
            path_resolver,
            fp_registry,
            kind,
        }
    }

    pub(crate) fn table_builder(&self) -> EncodedWalSsTableBuilder {
        self.sst_format.wal_table_builder()
    }

    pub(crate) fn estimate_encoded_size(&self, num_entries: usize, size_entries: usize) -> usize {
        self.sst_format
            .estimate_encoded_size_wal(num_entries, size_entries)
    }

    /// Writes a WAL SST with create-if-absent semantics required for fencing.
    pub(crate) async fn write_sst(
        &self,
        wal_id: u64,
        encoded_sst: &EncodedSsTable,
    ) -> Result<WalFileHandle, SlateDBError> {
        fail_point!(self.fp_registry.clone(), "write-wal-sst-io-error", |_| {
            Err(slatedb_io_error())
        });

        self.write_create(wal_id, encoded_sst.remaining_as_bytes())
            .await?;
        Ok(WalFileHandle::new(
            wal_id,
            encoded_sst.format_version,
            encoded_sst.info.clone(),
        ))
    }

    /// Writes a zero-byte WAL object as a fencing marker.
    pub(crate) async fn write_wal_fence(&self, wal_id: u64) -> Result<(), SlateDBError> {
        fail_point!(self.fp_registry.clone(), "write-wal-sst-io-error", |_| {
            Err(slatedb_io_error())
        });
        self.write_create(wal_id, Bytes::new()).await
    }

    async fn write_create(&self, wal_id: u64, data: Bytes) -> Result<(), SlateDBError> {
        let path = self.path(wal_id);
        let opts = PutOptions {
            mode: PutMode::Create,
            extensions: ObjectStoreCallTag::new(self.kind, SstType::Wal).into(),
            ..PutOptions::default()
        };
        self.object_store
            .put_opts(&path, data.into(), opts)
            .await
            .map_err(|error| match error {
                object_store::Error::AlreadyExists { .. } => {
                    debug!("path already exists [path={}]", path);
                    SlateDBError::Fenced
                }
                error => SlateDBError::from(error),
            })?;
        Ok(())
    }

    pub(crate) async fn open_sst(&self, wal_id: u64) -> Result<WalFileHandle, SlateDBError> {
        let (info, version) = read_obj!(
            Arc::clone(&self.object_store),
            self.path(wal_id),
            ObjectStoreCallTag::new(self.kind, SstType::Wal),
            |obj| self.sst_format.read_info_and_version(&obj)
        )
        .await?;
        Ok(WalFileHandle::new(wal_id, version, info))
    }

    pub(crate) async fn read_index(
        &self,
        handle: &WalFileHandle,
    ) -> Result<Arc<SsTableIndexOwned>, SlateDBError> {
        let index = read_obj!(
            Arc::clone(&self.object_store),
            self.path(handle.id.value()),
            ObjectStoreCallTag::new(self.kind, SstType::Wal),
            |obj| self.sst_format.read_index(&handle.info, &obj)
        )
        .await?;
        Ok(Arc::new(index))
    }

    pub(crate) fn block_range_for_target_bytes(
        &self,
        handle: &WalFileHandle,
        index: &SsTableIndexOwned,
        first_block: usize,
        target_bytes: usize,
    ) -> Range<usize> {
        assert!(target_bytes > 0);

        let index = index.borrow();
        let block_meta = index.block_meta();
        let num_blocks = block_meta.len();
        assert!(first_block < num_blocks);
        let target_bytes = u64::try_from(target_bytes).unwrap_or(u64::MAX);

        let mut blocks = first_block..first_block + 1;
        loop {
            let byte_range = self
                .sst_format
                .block_range(blocks.clone(), &handle.info, &index);
            if byte_range.end.saturating_sub(byte_range.start) >= target_bytes
                || blocks.end == num_blocks
            {
                return blocks;
            }
            blocks.end += 1;
        }
    }

    pub(crate) fn block_range_size(
        &self,
        handle: &WalFileHandle,
        index: &SsTableIndexOwned,
        blocks: Range<usize>,
    ) -> usize {
        if blocks.is_empty() {
            return 0;
        }
        let byte_range = self
            .sst_format
            .block_range(blocks, &handle.info, &index.borrow());
        usize::try_from(byte_range.end.saturating_sub(byte_range.start)).unwrap_or(usize::MAX)
    }

    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &WalFileHandle,
        index: Arc<SsTableIndexOwned>,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Arc<Block>>, SlateDBError> {
        let object_store = Arc::clone(&self.object_store);
        let path = self.path(handle.id.value());
        let index = &index;
        let blocks =
            read_with_validation_retry(ObjectStoreCallTag::new(self.kind, SstType::Wal), |tag| {
                let obj = ReadOnlyObject {
                    object_store: Arc::clone(&object_store),
                    path: path.clone(),
                    tag,
                };
                let blocks = blocks.clone();
                async move {
                    self.sst_format
                        .read_blocks(&handle.info, index, blocks, &obj)
                        .await
                        .map_err(|error| error.with_path(&obj.path))
                }
            })
            .await?;
        Ok(blocks.into_iter().map(Arc::new).collect())
    }

    /// Find the highest WAL SST id present in the object store at or above
    /// `start_after + 1`, returning `start_after` if none exist.
    ///
    /// `start_after` should be a known lower bound (e.g. `replay_after_wal_id`
    /// from the manifest, or the highest already-replayed WAL id). Passing 0
    /// scans the entire WAL id space.
    ///
    /// Two phases:
    ///   1. Parallel exponential probe at offsets `2^0, 2^1, ..., 2^k` from
    ///      `start_after`. One RTT per round of 8 exponents. Brackets the
    ///      frontier between two adjacent powers of two.
    ///   2. Sequential binary search inside the bracketed range to find the
    ///      exact frontier.
    ///
    /// Relies on the fencing protocol's contiguity invariant: "id exists" is
    /// monotone-decreasing in id, so binary search is sound. Total HEAD count
    /// is `O(log N)` for a gap of size N, vs `O(N)` for a windowed scan.
    pub(crate) async fn last_seen_wal_id(&self, start_after: u64) -> Result<u64, SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "probe-wal-ssts", |_| {
            Err(SlateDBError::from(std::io::Error::other("oops")))
        });

        const ROUND_SIZE: u32 = 8;
        const MAX_EXP: u32 = 48;

        let mut lo_offset = None;
        let mut hi_offset = None;
        let mut next_exp = 0;

        while hi_offset.is_none() {
            if next_exp >= MAX_EXP {
                return Err(SlateDBError::InvalidDBState);
            }
            let end_exp = (next_exp + ROUND_SIZE).min(MAX_EXP);
            let exps: Vec<u32> = (next_exp..end_exp).collect();
            let probes = exps.iter().map(|&exp| {
                let offset = 1u64 << exp;
                let path = self.path(start_after + offset);
                let object_store = Arc::clone(&self.object_store);
                async move { wal_object_exists(&object_store, &path).await }
            });
            let results = join_all(probes).await;

            for (exp, result) in exps.iter().zip(results) {
                let offset = 1u64 << exp;
                if result? {
                    lo_offset = Some(offset);
                } else {
                    hi_offset = Some(offset);
                    break;
                }
            }
            next_exp = end_exp;
        }

        let hi = hi_offset.expect("loop exits only after finding an upper bound");
        let Some(lo) = lo_offset else {
            return Ok(start_after);
        };

        let mut left = lo + 1;
        let mut right = hi;
        while left < right {
            let mid = left + (right - left) / 2;
            if wal_object_exists(&self.object_store, &self.path(start_after + mid)).await? {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(start_after + left - 1)
    }

    pub(crate) async fn next_wal_sst_id(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<u64, SlateDBError> {
        Ok(self.last_seen_wal_id(wal_id_last_compacted).await? + 1)
    }

    fn path(&self, wal_id: u64) -> Path {
        self.path_resolver.sst_path(&SsTableId::Wal(wal_id))
    }
}

async fn wal_object_exists(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<bool, SlateDBError> {
    match object_store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(SlateDBError::from(error)),
    }
}

fn slatedb_io_error() -> SlateDBError {
    SlateDBError::from(std::io::Error::other("oops"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_iterator::DataBlockIterator;
    use crate::iter::IterationOrder;
    use crate::types::RowEntry;
    use object_store::memory::InMemory;

    fn test_store() -> WalTableStore {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        WalTableStore::new(
            object_store,
            SsTableFormat::default(),
            Path::from("test-db"),
            TableStoreKind::Main,
        )
    }

    #[tokio::test]
    async fn writes_and_reads_wal_sst_without_a_cache() {
        let store = test_store();
        let rows = [
            RowEntry::new_value(b"first", b"value-1", 10),
            RowEntry::new_value(b"second", b"value-2", 11),
        ];
        assert!(store.estimate_encoded_size(2, 26) > 0);

        let mut builder = store.table_builder();
        for row in rows.iter().cloned() {
            builder.add(row).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let written = store.write_sst(1, &encoded).await.unwrap();
        let opened = store.open_sst(1).await.unwrap();

        assert_eq!(written, opened);
        assert_eq!(opened.id.value(), 1);

        let index = store.read_index(&opened).await.unwrap();
        let block_count = index.borrow().block_meta().len();
        assert_eq!(block_count, 1);
        assert_eq!(
            index.borrow().block_meta().get(0).first_key().bytes(),
            &10u64.to_be_bytes()
        );

        let block_range = store.block_range_for_target_bytes(&opened, &index, 0, usize::MAX);
        assert_eq!(block_range, 0..block_count);
        assert!(store.block_range_size(&opened, &index, block_range.clone()) > 0);

        let blocks = store
            .read_blocks_using_index(&opened, Arc::clone(&index), block_range)
            .await
            .unwrap();
        let mut actual = Vec::new();
        for block in blocks {
            let mut iter =
                DataBlockIterator::new(block, opened.format_version, IterationOrder::Ascending)
                    .unwrap();
            while let Some(row) = iter.next().await.unwrap() {
                actual.push(row);
            }
        }
        assert_eq!(actual, rows);
    }

    #[tokio::test]
    async fn uses_create_semantics_for_wals_and_fences() {
        let store = test_store();

        store.write_wal_fence(1).await.unwrap();
        assert_eq!(store.last_seen_wal_id(0).await.unwrap(), 1);
        assert_eq!(store.next_wal_sst_id(0).await.unwrap(), 2);
        assert!(matches!(
            store.write_wal_fence(1).await,
            Err(SlateDBError::Fenced)
        ));
        assert!(matches!(
            store.open_sst(1).await,
            Err(SlateDBError::EmptySSTable)
        ));

        let mut builder = store.table_builder();
        builder
            .add(RowEntry::new_value(b"key", b"value", 12))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        assert!(matches!(
            store.write_sst(1, &encoded).await,
            Err(SlateDBError::Fenced)
        ));
    }
}
