use std::collections::VecDeque;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use chrono::Utc;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{future::join_all, StreamExt};
use log::warn;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::db_cache::CachedEntry;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use crate::types::RowEntry;
use crate::{blob::ReadOnlyBlob, block::Block, db_cache::DbCache};

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
    transactional_wal_store: Arc<dyn TransactionalObjectStore>,
    /// In-memory cache for blocks
    block_cache: Option<Arc<dyn DbCache>>,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<usize, SlateDBError> {
        let object_metadata = self.object_store.head(&self.path).await?;
        Ok(object_metadata.size)
    }

    async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError> {
        let bytes = self.object_store.get_range(&self.path, range).await?;
        Ok(bytes)
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        let file = self.object_store.get(&self.path).await?;
        let bytes = file.bytes().await?;
        Ok(bytes)
    }
}

/// Represents the metadata of an SST file in the compacted directory.
pub(crate) struct SstFileMetadata {
    pub(crate) id: SsTableId,
    #[allow(dead_code)]
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: usize,
}

impl TableStore {
    pub fn new<P: Into<Path>>(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: P,
        block_cache: Option<Arc<dyn DbCache>>,
    ) -> Self {
        Self::new_with_fp_registry(
            object_store,
            sst_format,
            root_path,
            Arc::new(FailPointRegistry::new()),
            block_cache,
        )
    }

    pub fn new_with_fp_registry<P: Into<Path>>(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: P,
        fp_registry: Arc<FailPointRegistry>,
        block_cache: Option<Arc<dyn DbCache>>,
    ) -> Self {
        Self {
            object_store: object_store.clone(),
            sst_format,
            root_path: root_path.into(),
            wal_path: "wal",
            compacted_path: "compacted",
            fp_registry,
            transactional_wal_store: Arc::new(DelegatingTransactionalObjectStore::new(
                Path::from("/"),
                object_store.clone(),
            )),
            block_cache,
        }
    }

    pub(crate) async fn list_wal_ssts<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<SstFileMetadata>, SlateDBError> {
        let mut wal_list: Vec<SstFileMetadata> = Vec::new();
        let wal_path = &Path::from(format!("{}/{}/", &self.root_path, self.wal_path));
        let mut files_stream = self.object_store.list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match Self::parse_id(&self.root_path, &file.location) {
                Ok(Some(SsTableId::Wal(id))) => {
                    if id_range.contains(&id) {
                        wal_list.push(SstFileMetadata {
                            id: SsTableId::Wal(id),
                            location: file.location,
                            last_modified: file.last_modified,
                            size: file.size,
                        });
                    }
                }
                _ => continue,
            }
        }
        wal_list.sort_by_key(|m| m.id.unwrap_wal_id());
        Ok(wal_list)
    }

    pub(crate) async fn next_wal_sst_id(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<u64, SlateDBError> {
        Ok(self
            .list_wal_ssts(wal_id_last_compacted..)
            .await?
            .into_iter()
            .map(|wal_sst| wal_sst.id.unwrap_wal_id())
            .max()
            .unwrap_or(wal_id_last_compacted)
            + 1)
    }

    pub(crate) fn table_writer(&self, id: SsTableId) -> EncodedSsTableWriter {
        let path = self.path(&id);
        EncodedSsTableWriter {
            id,
            builder: self.sst_format.table_builder(),
            writer: BufWriter::new(self.object_store.clone(), path),
            table_store: self,
            blocks_written: 0,
        }
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        self.sst_format.table_builder()
    }

    pub(crate) async fn write_sst(
        &self,
        id: &SsTableId,
        encoded_sst: EncodedSsTable,
    ) -> Result<SsTableHandle, SlateDBError> {
        fail_point!(
            self.fp_registry.clone(),
            "write-wal-sst-io-error",
            matches!(id, SsTableId::Wal(_)),
            |_| Result::Err(slatedb_io_error())
        );
        fail_point!(
            self.fp_registry.clone(),
            "write-compacted-sst-io-error",
            matches!(id, SsTableId::Compacted(_)),
            |_| Result::Err(slatedb_io_error())
        );

        let total_size = encoded_sst
            .unconsumed_blocks
            .iter()
            .map(|chunk| chunk.len())
            .sum();
        let mut data = Vec::<u8>::with_capacity(total_size);
        for chunk in encoded_sst.unconsumed_blocks {
            data.put_slice(chunk.as_ref())
        }

        let path = self.path(id);
        self.transactional_wal_store
            .put_if_not_exists(&path, Bytes::from(data))
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path: _, source: _ } => match id {
                    SsTableId::Wal(_) => SlateDBError::Fenced,
                    SsTableId::Compacted(_) => SlateDBError::from(e),
                },
                _ => SlateDBError::from(e),
            })?;

        self.cache_filter(*id, encoded_sst.info.filter_offset, encoded_sst.filter)
            .await;
        Ok(SsTableHandle {
            id: *id,
            info: encoded_sst.info,
        })
    }

    async fn cache_filter(&self, sst: SsTableId, id: u64, filter: Option<Arc<BloomFilter>>) {
        let Some(cache) = &self.block_cache else {
            return;
        };
        if let Some(filter) = filter {
            cache
                .insert((sst, id).into(), CachedEntry::with_bloom_filter(filter))
                .await;
        }
    }

    /// Delete an SSTable from the object store.
    pub(crate) async fn delete_sst(&self, id: &SsTableId) -> Result<(), SlateDBError> {
        let path = self.path(id);
        self.object_store
            .delete(&path)
            .await
            .map_err(SlateDBError::from)
    }

    /// List all SSTables in the compacted directory.
    /// The SSTables are returned in ascending order of their IDs. Ulids within
    /// the same millisecond are sorted based on their random suffix.
    /// # Arguments
    /// * `id_range` - The range of IDs to list
    /// # Returns
    /// A list of SSTables in the compacted directory
    pub(crate) async fn list_compacted_ssts<R: RangeBounds<Ulid>>(
        &self,
        id_range: R,
    ) -> Result<Vec<SstFileMetadata>, SlateDBError> {
        let mut sst_list: Vec<SstFileMetadata> = Vec::new();
        let sst_path = &Path::from(format!("{}/{}/", &self.root_path, self.compacted_path));
        let mut files_stream = self.object_store.list(Some(sst_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match Self::parse_id(&self.root_path, &file.location) {
                Ok(Some(SsTableId::Compacted(id))) => {
                    if id_range.contains(&id) {
                        sst_list.push(SstFileMetadata {
                            id: SsTableId::Compacted(id),
                            location: file.location,
                            last_modified: file.last_modified,
                            size: file.size,
                        });
                    }
                }
                Err(e) => {
                    warn!("Error while parsing file id: {}", e);
                }
                _ => {
                    warn!(
                        "Unexpected file found in compacted directory: {:?}",
                        file.location
                    );
                }
            }
        }

        sst_list.sort_by_key(|m| m.id.unwrap_compacted_id());
        Ok(sst_list)
    }

    pub(crate) async fn open_sst(&self, id: &SsTableId) -> Result<SsTableHandle, SlateDBError> {
        let path = self.path(id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let info = self.sst_format.read_info(&obj).await?;
        Ok(SsTableHandle { id: *id, info })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SsTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        if let Some(cache) = &self.block_cache {
            if let Some(filter) = cache
                .get((handle.id, handle.info.filter_offset).into())
                .await
                .and_then(|entry| entry.bloom_filter())
            {
                return Ok(Some(filter));
            }
        }
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let filter = self.sst_format.read_filter(&handle.info, &obj).await?;
        if let Some(cache) = &self.block_cache {
            if let Some(filter) = filter.as_ref() {
                cache
                    .insert(
                        (handle.id, handle.info.filter_offset).into(),
                        CachedEntry::with_bloom_filter(filter.clone()),
                    )
                    .await;
            }
        }
        Ok(filter)
    }

    pub(crate) async fn read_index(
        &self,
        handle: &SsTableHandle,
    ) -> Result<Arc<SsTableIndexOwned>, SlateDBError> {
        if let Some(cache) = &self.block_cache {
            if let Some(index) = cache
                .get((handle.id, handle.info.index_offset).into())
                .await
                .and_then(|entry| entry.sst_index())
            {
                return Ok(index);
            }
        }
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let index = Arc::new(self.sst_format.read_index(&handle.info, &obj).await?);
        if let Some(cache) = &self.block_cache {
            cache
                .insert(
                    (handle.id, handle.info.index_offset).into(),
                    CachedEntry::with_sst_index(index.clone()),
                )
                .await;
        }
        Ok(index)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_blocks(
        &self,
        handle: &SsTableHandle,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let index = self.sst_format.read_index(&handle.info, &obj).await?;
        self.sst_format
            .read_blocks(&handle.info, &index, blocks, &obj)
            .await
    }

    /// Reads specified blocks from an SSTable using the provided index.
    ///
    /// This function attempts to read blocks from the cache if available
    /// and falls back to reading from storage for uncached blocks
    /// using an async fetch for each contiguous range that blocks are not cached.
    /// It can optionally cache newly read blocks.
    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &SsTableHandle,
        index: Arc<SsTableIndexOwned>,
        blocks: Range<usize>,
        cache_blocks: bool,
    ) -> Result<VecDeque<Arc<Block>>, SlateDBError> {
        // Create a ReadOnlyObject for accessing the SSTable file
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        // Initialize the result vector and a vector to track uncached ranges
        let mut blocks_read = VecDeque::with_capacity(blocks.end - blocks.start);
        let mut uncached_ranges = Vec::new();

        // If block cache is available, try to retrieve cached blocks
        if let Some(cache) = &self.block_cache {
            let index_borrow = index.borrow();
            // Attempt to get all requested blocks from cache concurrently
            let cached_blocks = join_all(blocks.clone().map(|block_num| async move {
                let block_meta = index_borrow.block_meta().get(block_num);
                let offset = block_meta.offset();
                cache
                    .get((handle.id, offset).into())
                    .await
                    .and_then(|entry| entry.block())
            }))
            .await;

            let mut last_uncached_start = None;

            // Process cached block results
            for (index, block_result) in cached_blocks.into_iter().enumerate() {
                match block_result {
                    Some(cached_block) => {
                        // If a cached block is found, add it to blocks_read
                        if let Some(start) = last_uncached_start.take() {
                            uncached_ranges.push((blocks.start + start)..(blocks.start + index));
                        }
                        blocks_read.push_back(cached_block);
                    }
                    None => {
                        // If a block is not in cache, mark the start of an uncached range
                        last_uncached_start.get_or_insert(index);
                    }
                }
            }
            // Add the last uncached range if it exists
            if let Some(start) = last_uncached_start {
                uncached_ranges.push((blocks.start + start)..blocks.end);
            }
        } else {
            // If no cache is available, treat all blocks as uncached
            uncached_ranges.push(blocks.clone());
        }
        // Read uncached blocks concurrently
        let uncached_blocks = join_all(uncached_ranges.iter().map(|range| {
            let obj_ref = &obj;
            let index_ref = &index;
            async move {
                self.sst_format
                    .read_blocks(&handle.info, index_ref, range.clone(), obj_ref)
                    .await
            }
        }))
        .await;

        // Merge uncached blocks with blocks_read and prepare blocks for caching
        let mut blocks_to_cache = vec![];
        for (range, range_blocks) in uncached_ranges.into_iter().zip(uncached_blocks) {
            let index_borrow = index.borrow();
            for (block_num, block_read) in range.zip(range_blocks?) {
                let block = Arc::new(block_read);
                if cache_blocks {
                    let block_meta = index_borrow.block_meta().get(block_num);
                    let offset = block_meta.offset();
                    blocks_to_cache.push((handle.id, offset, block.clone()));
                }
                blocks_read.insert(block_num - blocks.start, block);
            }
        }

        // Cache the newly read blocks if caching is enabled
        if let Some(cache) = &self.block_cache {
            if !blocks_to_cache.is_empty() {
                join_all(blocks_to_cache.into_iter().map(|(id, offset, block)| {
                    cache.insert((id, offset).into(), CachedEntry::with_block(block))
                }))
                .await;
            }
        }

        Ok(blocks_read)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_block(
        &self,
        handle: &SsTableHandle,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let index = self.sst_format.read_index(&handle.info, &obj).await?;
        self.sst_format
            .read_block(&handle.info, &index, block, &obj)
            .await
    }

    fn path(&self, id: &SsTableId) -> Path {
        match id {
            SsTableId::Wal(id) => Path::from(format!(
                "{}/{}/{:020}.sst",
                &self.root_path, self.wal_path, id
            )),
            SsTableId::Compacted(ulid) => Path::from(format!(
                "{}/{}/{}.sst",
                &self.root_path,
                self.compacted_path,
                ulid.to_string()
            )),
        }
    }

    /// Parses the SsTableId from a given path
    fn parse_id(root_path: &Path, path: &Path) -> Result<Option<SsTableId>, SlateDBError> {
        if let Some(mut suffix_iter) = path.prefix_match(root_path) {
            match suffix_iter.next() {
                Some(a) if a.as_ref() == "wal" => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(|s| s.parse::<u64>()))
                    .transpose()
                    .map(|r| r.map(SsTableId::Wal))
                    .map_err(|_| SlateDBError::InvalidDBState),
                Some(a) if a.as_ref() == "compacted" => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(Ulid::from_string))
                    .transpose()
                    .map(|r| r.map(SsTableId::Compacted))
                    .map_err(|_| SlateDBError::InvalidDBState),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct EncodedSsTableWriter<'a> {
    id: SsTableId,
    builder: EncodedSsTableBuilder<'a>,
    writer: BufWriter,
    table_store: &'a TableStore,
    blocks_written: usize,
}

impl<'a> EncodedSsTableWriter<'a> {
    pub async fn add(&mut self, entry: RowEntry) -> Result<(), SlateDBError> {
        self.builder.add(entry)?;
        self.drain_blocks().await
    }

    pub async fn close(mut self) -> Result<SsTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build()?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.as_ref()).await?;
        }
        self.writer.shutdown().await?;
        self.table_store
            .cache_filter(self.id, encoded_sst.info.filter_offset, encoded_sst.filter)
            .await;
        Ok(SsTableHandle {
            id: self.id,
            info: encoded_sst.info,
        })
    }

    async fn drain_blocks(&mut self) -> Result<(), SlateDBError> {
        while let Some(block) = self.builder.next_block() {
            self.writer.write_all(block.as_ref()).await?;
            self.blocks_written += 1;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn blocks_written(&self) -> usize {
        self.blocks_written
    }
}

#[allow(dead_code)]
fn slatedb_io_error() -> SlateDBError {
    SlateDBError::from(std::io::Error::new(std::io::ErrorKind::Other, "oops"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::sync::Arc;

    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use ulid::Ulid;

    use crate::error;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstIterator;
    #[cfg(feature = "moka")]
    use crate::tablestore::DbCache;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::{
        block::Block, block_iterator::BlockIterator, db_state::SsTableId, iter::KeyValueIterator,
    };

    const ROOT: &str = "/root";

    #[test]
    fn test_parse_id() {
        let root = Path::from(ROOT);
        let path = Path::from("/root/wal/00000000000000000003.sst");
        let id = TableStore::parse_id(&root, &path).unwrap();
        assert_eq!(id, Some(SsTableId::Wal(3)));

        let path = Path::from("/root/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let id = TableStore::parse_id(&root, &path).unwrap();
        assert_eq!(
            id,
            Some(SsTableId::Compacted(
                Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").unwrap()
            ))
        );

        let path = Path::from("/root/invalid/00000000000000000001.sst");
        let id = TableStore::parse_id(&root, &path).unwrap();
        assert_eq!(id, None);
    }

    #[tokio::test]
    async fn test_sst_writer_should_write_sst() {
        // given:
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));
        let id = SsTableId::Compacted(Ulid::new());

        // when:
        let mut writer = ts.table_writer(id);
        writer
            .add(RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_tombstone(&[b'c'; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0))
            .await
            .unwrap();
        let sst = writer.close().await.unwrap();

        // then:
        let mut iter = SstIterator::new(&sst, ts.clone(), 1, 1, true)
            .await
            .unwrap();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0),
                RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0),
                RowEntry::new_tombstone(&[b'c'; 16], 0),
                RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_wal_write_should_fail_when_fenced() {
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));
        let wal_id = SsTableId::Wal(1);

        // write a wal sst
        let mut sst1 = ts.table_builder();
        sst1.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table = sst1.build().unwrap();
        ts.write_sst(&wal_id, table).await.unwrap();

        let mut sst2 = ts.table_builder();
        sst2.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table2 = sst2.build().unwrap();

        // write another wal sst with the same id.
        let result = ts.write_sst(&wal_id, table2).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    #[cfg(feature = "moka")]
    async fn test_tablestore_sst_and_partial_cache_hits() {
        use crate::db_cache::moka::MokaCache;

        // Setup
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };

        let block_cache = Arc::new(MokaCache::new());
        let ts = Arc::new(TableStore::new(
            os.clone(),
            format,
            Path::from("/root"),
            Some(block_cache.clone()),
        ));

        // Create and write SST
        let id = SsTableId::Compacted(Ulid::new());
        let mut writer = ts.table_writer(id);
        let mut expected_data = Vec::with_capacity(20);
        for i in 0..20 {
            let key = [i as u8; 16];
            let value = [(i + 1) as u8; 16];
            expected_data.push((
                Vec::from(key.as_slice()),
                ValueDeletable::Value(Bytes::copy_from_slice(&value)),
            ));
            writer
                .add(RowEntry::new_value(key.as_ref(), value.as_ref(), 0))
                .await
                .unwrap();
        }
        let handle = writer.close().await.unwrap();

        // Read the index
        let index = ts.read_index(&handle).await.unwrap();

        // Test 1: SST hit
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();

        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are now in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            assert!(
                block_cache
                    .get((handle.id, offset).into())
                    .await
                    .is_some_and(|entry| entry.block().is_some()),
                "Block with offset {} should be in cache",
                offset
            );
        }

        // Partially clear the cache (remove blocks 5..10 and 15..20)
        for i in 5..10 {
            let offset = index.borrow().block_meta().get(i).offset();
            block_cache.remove((handle.id, offset).into()).await;
        }
        for i in 15..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            block_cache.remove((handle.id, offset).into()).await;
        }

        // Test 2: Partial cache hit, everything should be returned since missing blocks are returned from sst
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are again in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            assert!(
                block_cache
                    .get((handle.id, offset).into())
                    .await
                    .is_some_and(|entry| entry.block().is_some()),
                "Block with offset {} should be in cache after partial hit",
                offset
            );
        }

        // Replace SST file with an empty file
        let path = ts.path(&id);
        os.put(&path, Bytes::new().into()).await.unwrap();

        // Test 3: All blocks should be in cache after SST file is emptied
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are still in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            assert!(
                block_cache
                    .get((handle.id, offset).into())
                    .await
                    .is_some_and(|entry| entry.block().is_some()),
                "Block with offset {} should be in cache after SST emptying",
                offset
            );
        }

        // Test 4: Verify that reading specific ranges still works after SST file is emptied
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 5..10, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data[5..10]).await;

        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 15..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data[15..20]).await;
    }

    #[allow(dead_code)]
    async fn assert_blocks(blocks: &VecDeque<Arc<Block>>, expected: &[(Vec<u8>, ValueDeletable)]) {
        let mut block_iter = blocks.iter();
        let mut expected_iter = expected.iter();

        while let (Some(block), Some(expected_item)) = (block_iter.next(), expected_iter.next()) {
            let mut iter = BlockIterator::from_first_key(block.clone());
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, expected_item.0);
            assert_eq!(ValueDeletable::Value(kv.value), expected_item.1);
        }

        assert!(block_iter.next().is_none());
        assert!(expected_iter.next().is_none());
    }

    #[tokio::test]
    async fn test_list_compacted_ssts() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));

        // Create id1, id2, and i3 as three random UUIDs that have been sorted ascending.
        // Need to do this because the Ulids are sometimes generated in the same millisecond
        // and the random suffix is used to break the tie, which might be out of order.
        let mut ulids = (0..3).map(|_| Ulid::new()).collect::<Vec<Ulid>>();
        ulids.sort();
        let (id1, id2, id3) = (
            SsTableId::Compacted(ulids[0]),
            SsTableId::Compacted(ulids[1]),
            SsTableId::Compacted(ulids[2]),
        );

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        os.put(&path1, Bytes::new().into()).await.unwrap();
        os.put(&path2, Bytes::new().into()).await.unwrap();
        os.put(&path3, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 3);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
        assert_eq!(ssts[2].id, id3);

        let ssts = ts
            .list_compacted_ssts(id2.unwrap_compacted_id()..id3.unwrap_compacted_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        let ssts = ts
            .list_compacted_ssts(id2.unwrap_compacted_id()..)
            .await
            .unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id2);
        assert_eq!(ssts[1].id, id3);

        let ssts = ts
            .list_compacted_ssts(..id3.unwrap_compacted_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
    }

    #[tokio::test]
    async fn test_list_wal_ssts() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));

        let id1 = SsTableId::Wal(1);
        let id2 = SsTableId::Wal(2);
        let id3 = SsTableId::Wal(3);

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        os.put(&path1, Bytes::new().into()).await.unwrap();
        os.put(&path2, Bytes::new().into()).await.unwrap();
        os.put(&path3, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 3);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
        assert_eq!(ssts[2].id, id3);

        let ssts = ts
            .list_wal_ssts(id2.unwrap_wal_id()..id3.unwrap_wal_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        let ssts = ts.list_wal_ssts(id2.unwrap_wal_id()..).await.unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id2);
        assert_eq!(ssts[1].id, id3);

        let ssts = ts.list_wal_ssts(..id3.unwrap_wal_id()).await.unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
    }

    #[tokio::test]
    async fn test_delete_sst() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));

        let id1 = SsTableId::Compacted(Ulid::new());
        let id2 = SsTableId::Compacted(Ulid::new());
        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        os.put(&path1, Bytes::new().into()).await.unwrap();
        os.put(&path2, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 2);

        ts.delete_sst(&id1).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);
    }
}
