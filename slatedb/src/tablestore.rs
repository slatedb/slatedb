use std::collections::VecDeque;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{future::join_all, StreamExt};
use log::{debug, warn};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::db_cache::{CachedEntry, DbCache};
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::paths::PathResolver;
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use crate::types::RowEntry;
use crate::{blob::ReadOnlyBlob, block::Block};

pub struct TableStore {
    object_stores: ObjectStores,
    sst_format: SsTableFormat,
    path_resolver: PathResolver,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
    /// In-memory cache for blocks
    block_cache: Option<Arc<dyn DbCache>>,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<u64, SlateDBError> {
        let object_metadata = self.object_store.head(&self.path).await?;
        Ok(object_metadata.size)
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError> {
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
    pub(crate) size: u64,
}

impl TableStore {
    pub fn new<P: Into<Path>>(
        object_stores: ObjectStores,
        sst_format: SsTableFormat,
        root_path: P,
        block_cache: Option<Arc<dyn DbCache>>,
    ) -> Self {
        Self::new_with_fp_registry(
            object_stores,
            sst_format,
            PathResolver::new(root_path),
            Arc::new(FailPointRegistry::new()),
            block_cache,
        )
    }

    pub fn new_with_fp_registry(
        object_stores: ObjectStores,
        sst_format: SsTableFormat,
        path_resolver: PathResolver,
        fp_registry: Arc<FailPointRegistry>,
        block_cache: Option<Arc<dyn DbCache>>,
    ) -> Self {
        Self {
            object_stores,
            sst_format,
            path_resolver,
            fp_registry,
            block_cache,
        }
    }

    /// Get the number of blocks for a size specified in bytes.
    /// The returned value will be rounded down to the nearest block.
    pub(crate) fn bytes_to_blocks(&self, bytes: usize) -> usize {
        bytes.div_ceil(self.sst_format.block_size)
    }

    pub(crate) async fn last_seen_wal_id(&self) -> Result<u64, SlateDBError> {
        let wal_ssts = self.list_wal_ssts(..).await?;
        let last_wal_id = wal_ssts.last().map(|md| md.id.unwrap_wal_id());
        Ok(last_wal_id.unwrap_or(0))
    }

    pub(crate) async fn list_wal_ssts<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<SstFileMetadata>, SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "list-wal-ssts", |_| {
            Err(SlateDBError::from(std::io::Error::other("oops")))
        });

        let mut wal_list: Vec<SstFileMetadata> = Vec::new();
        let wal_path = &self.path_resolver.wal_path();
        let mut files_stream = self
            .object_stores
            .store_of(ObjectStoreType::Wal)
            .list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.path_resolver.parse_table_id(&file.location) {
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
        let object_store = self.object_stores.store_for(&id);
        let path = self.path(&id);
        EncodedSsTableWriter {
            id,
            builder: self.sst_format.table_builder(),
            writer: BufWriter::new(object_store, path),
            table_store: self,
            #[cfg(test)]
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
        write_cache: bool,
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

        let object_store = self.object_stores.store_for(id);
        let data = encoded_sst.remaining_as_bytes();
        let path = self.path(id);
        object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(data),
                PutOptions::from(PutMode::Create),
            )
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path: _, source: _ } => match id {
                    SsTableId::Wal(_) => {
                        debug!("Path {path} already exists");
                        SlateDBError::Fenced
                    }
                    SsTableId::Compacted(_) => SlateDBError::from(e),
                },
                _ => SlateDBError::from(e),
            })?;

        if let Some(cache) = self.block_cache.as_ref() {
            if write_cache {
                for block in encoded_sst.unconsumed_blocks {
                    let offset = block.offset;
                    let block = Arc::new(block.block);
                    cache
                        .insert((*id, offset).into(), CachedEntry::with_block(block))
                        .await;
                }
                cache
                    .insert(
                        (*id, encoded_sst.info.index_offset).into(),
                        CachedEntry::with_sst_index(Arc::new(encoded_sst.index)),
                    )
                    .await;
            }
        }
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
        let object_store = self.object_stores.store_for(id);
        let path = self.path(id);
        object_store.delete(&path).await.map_err(SlateDBError::from)
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
        let compacted_path = self.path_resolver.compacted_path();
        let mut files_stream = self
            .object_stores
            .store_of(ObjectStoreType::Main)
            .list(Some(&compacted_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.path_resolver.parse_table_id(&file.location) {
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
        let object_store = self.object_stores.store_for(id);
        let path = self.path(id);
        let obj = ReadOnlyObject { object_store, path };
        let info = self.sst_format.read_info(&obj).await?;
        Ok(SsTableHandle { id: *id, info })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SsTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        if let Some(cache) = &self.block_cache {
            if let Some(filter) = cache
                .get_filter((handle.id, handle.info.filter_offset).into())
                .await
                .unwrap_or(None)
                .and_then(|e| e.bloom_filter())
            {
                return Ok(Some(filter));
            }
        }
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject { object_store, path };
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
                .get_index((handle.id, handle.info.index_offset).into())
                .await
                .unwrap_or(None)
                .and_then(|e| e.sst_index())
            {
                return Ok(index);
            }
        }
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject { object_store, path };
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
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject { object_store, path };
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
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject { object_store, path };
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
                    .get_block((handle.id, offset).into())
                    .await
                    .unwrap_or(None)
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
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject { object_store, path };
        let index = self.sst_format.read_index(&handle.info, &obj).await?;
        self.sst_format
            .read_block(&handle.info, &index, block, &obj)
            .await
    }

    fn path(&self, id: &SsTableId) -> Path {
        self.path_resolver.table_path(id)
    }

    pub(crate) fn estimate_encoded_size(&self, entry_num: usize, entries_size: usize) -> usize {
        self.sst_format
            .estimate_encoded_size(entry_num, entries_size)
    }
}

pub(crate) struct EncodedSsTableWriter<'a> {
    id: SsTableId,
    builder: EncodedSsTableBuilder<'a>,
    writer: BufWriter,
    table_store: &'a TableStore,
    #[cfg(test)]
    blocks_written: usize,
}

impl EncodedSsTableWriter<'_> {
    pub async fn add(&mut self, entry: RowEntry) -> Result<(), SlateDBError> {
        self.builder.add(entry)?;
        self.drain_blocks().await
    }

    pub async fn close(mut self) -> Result<SsTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build()?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.encoded_bytes.as_ref()).await?;
        }
        self.writer.write_all(encoded_sst.footer.as_ref()).await?;
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
            self.writer.write_all(block.encoded_bytes.as_ref()).await?;
            #[cfg(test)]
            {
                self.blocks_written += 1;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn blocks_written(&self) -> usize {
        self.blocks_written
    }
}

#[allow(dead_code)]
fn slatedb_io_error() -> SlateDBError {
    SlateDBError::from(std::io::Error::other("oops"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::future;
    use futures::StreamExt;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use proptest::prelude::any;
    use proptest::proptest;
    use rstest::rstest;
    use std::collections::VecDeque;
    use std::sync::Arc;

    use crate::clock::DefaultSystemClock;
    use crate::db_cache::test_utils::TestCache;
    use crate::db_cache::{DbCache, DbCacheWrapper};
    use crate::error;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    #[cfg(feature = "moka")]
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, build_test_sst};
    use crate::types::{RowEntry, ValueDeletable};
    use crate::{
        block::Block, block_iterator::BlockIterator, db_state::SsTableId, iter::KeyValueIterator,
    };

    const ROOT: &str = "/root";

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn count_ssts_in(store: &Arc<dyn ObjectStore>) -> usize {
        store
            .list(None)
            .filter(|r| {
                future::ready(
                    r.as_ref()
                        .unwrap()
                        .location
                        .extension()
                        .unwrap()
                        .to_lowercase()
                        == "sst",
                )
            })
            .count()
            .await
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_sst_writer_should_write_compacted_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        // given:
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
        ));
        let id = SsTableId::Compacted(crate::utils::ulid());

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

        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        // then:
        let mut iter = SstIterator::new_owned(.., sst, ts.clone(), sst_iter_options)
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

        assert_eq!(count_ssts_in(&main_store).await, 1);
        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&wal_store).await, 0);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_sst_writer_should_write_wal_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        // given:
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
        ));
        let id = SsTableId::Wal(123);

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

        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        // then:
        let mut iter = SstIterator::new_owned(.., sst, ts.clone(), sst_iter_options)
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

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 1);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    #[tokio::test]
    async fn test_wal_write_should_fail_when_fenced() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from(ROOT),
            None,
        ));
        let wal_id = SsTableId::Wal(1);

        // write a wal sst
        let mut sst1 = ts.table_builder();
        sst1.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table = sst1.build().unwrap();
        ts.write_sst(&wal_id, table, false).await.unwrap();

        let mut sst2 = ts.table_builder();
        sst2.add(RowEntry::new_value(b"key", b"value", 0)).unwrap();
        let table2 = sst2.build().unwrap();

        // write another wal sst with the same id.
        let result = ts.write_sst(&wal_id, table2, false).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    #[cfg(feature = "moka")]
    async fn test_tablestore_sst_and_partial_cache_hits() {
        use crate::db_cache::moka::MokaCache;

        // Setup
        let os = Arc::new(InMemory::new());
        let clock = Arc::new(DefaultSystemClock::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };

        let stat_registry = StatRegistry::new();
        let block_cache = Arc::new(MokaCache::new());
        let wrapper = Arc::new(DbCacheWrapper::new(
            block_cache.clone(),
            &stat_registry,
            clock,
        ));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from("/root"),
            Some(wrapper),
        ));

        // Create and write SST
        let id = SsTableId::Compacted(crate::utils::ulid());
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
                    .get_block((handle.id, offset).into())
                    .await
                    .unwrap_or(None)
                    .is_some(),
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
                    .get_block((handle.id, offset).into())
                    .await
                    .unwrap_or(None)
                    .is_some(),
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
                    .get_block((handle.id, offset).into())
                    .await
                    .unwrap_or(None)
                    .is_some(),
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

    #[tokio::test]
    async fn test_write_sst_should_write_cache() {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let cache = Arc::new(TestCache::new());
        let clock = Arc::new(DefaultSystemClock::new());
        let wrapper = Arc::new(DbCacheWrapper::new(cache.clone(), &stat_registry, clock));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from("/root"),
            Some(wrapper),
        ));
        let id = SsTableId::Compacted(crate::utils::ulid());
        let sst = build_test_sst(&ts.sst_format, 3);
        let sst_bytes = sst.remaining_as_bytes();
        let sst_info = sst.info.clone();

        ts.write_sst(&id, sst, true).await.unwrap();

        let index = ts.sst_format.read_index_raw(&sst_info, &sst_bytes).unwrap();
        let block_metas = index.borrow().block_meta();
        for i in 0..block_metas.len() {
            let block_meta = block_metas.get(i);
            let block = ts
                .sst_format
                .read_block_raw(&sst_info, &index, i, &sst_bytes)
                .unwrap();
            let cached_block = cache
                .get_block((id, block_meta.offset()).into())
                .await
                .unwrap();
            assert!(cached_block.is_some());
            assert!(block == *cached_block.unwrap().block().unwrap());
        }
    }

    #[tokio::test]
    async fn test_write_sst_should_not_write_cache() {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let cache = Arc::new(TestCache::new());
        let clock = Arc::new(DefaultSystemClock::new());
        let wrapper = Arc::new(DbCacheWrapper::new(cache.clone(), &stat_registry, clock));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from("/root"),
            Some(wrapper),
        ));
        let id = SsTableId::Compacted(crate::utils::ulid());
        let sst = build_test_sst(&ts.sst_format, 3);
        let sst_bytes = sst.remaining_as_bytes();
        let sst_info = sst.info.clone();

        ts.write_sst(&id, sst, false).await.unwrap();

        let index = ts.sst_format.read_index_raw(&sst_info, &sst_bytes).unwrap();
        let block_metas = index.borrow().block_meta();
        for i in 0..block_metas.len() {
            let block_meta = block_metas.get(i);
            let cached_block = cache
                .get_block((id, block_meta.offset()).into())
                .await
                .unwrap();
            assert!(cached_block.is_none());
        }
    }

    #[allow(dead_code)]
    async fn assert_blocks(blocks: &VecDeque<Arc<Block>>, expected: &[(Vec<u8>, ValueDeletable)]) {
        let mut block_iter = blocks.iter();
        let mut expected_iter = expected.iter();

        while let (Some(block), Some(expected_item)) = (block_iter.next(), expected_iter.next()) {
            let mut iter = BlockIterator::new_ascending(block.clone());
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, expected_item.0);
            assert_eq!(ValueDeletable::Value(kv.value), expected_item.1);
        }

        assert!(block_iter.next().is_none());
        assert!(expected_iter.next().is_none());
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_list_compacted_ssts(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        use ulid::Ulid;

        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store),
            format,
            Path::from(ROOT),
            None,
        ));

        // Create id1, id2, and i3 as three random UUIDs that have been sorted ascending.
        // Need to do this because the Ulids are sometimes generated in the same millisecond
        // and the random suffix is used to break the tie, which might be out of order.
        let mut ulids = (0..3).map(|_| crate::utils::ulid()).collect::<Vec<Ulid>>();
        ulids.sort();
        let (id1, id2, id3) = (
            SsTableId::Compacted(ulids[0]),
            SsTableId::Compacted(ulids[1]),
            SsTableId::Compacted(ulids[2]),
        );

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        main_store.put(&path1, Bytes::new().into()).await.unwrap();
        main_store.put(&path2, Bytes::new().into()).await.unwrap();
        main_store.put(&path3, Bytes::new().into()).await.unwrap();

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

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_list_wal_ssts(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
        ));

        let id1 = SsTableId::Wal(1);
        let id2 = SsTableId::Wal(2);
        let id3 = SsTableId::Wal(3);

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path1, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path2, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path3, Bytes::new().into())
            .await
            .unwrap();

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

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 3);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 3);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_delete_compacted_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
        ));

        let id1 = SsTableId::Compacted(crate::utils::ulid());
        let id2 = SsTableId::Compacted(crate::utils::ulid());
        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        main_store.put(&path1, Bytes::new().into()).await.unwrap();
        main_store.put(&path2, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 2);

        ts.delete_sst(&id1).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 1);
            assert_eq!(count_ssts_in(&wal_store).await, 0);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_delete_wal_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
        ));

        let id1 = SsTableId::Wal(123);
        let id2 = SsTableId::Wal(321);
        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path1, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path2, Bytes::new().into())
            .await
            .unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 2);

        ts.delete_sst(&id1).await.unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 1);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    proptest! {
        #[test]
        fn convert_bytes_to_blocks_precise_when_aligned_with_block_size(
            block_size in any::<usize>(),
            num_blocks in any::<usize>(),
        ) {
            let os = Arc::new(InMemory::new());
            let format = SsTableFormat { block_size, ..SsTableFormat::default() };
            let ts = Arc::new(TableStore::new(ObjectStores::new(os.clone(), None),
                format, Path::from(ROOT), None));
            if let Some(bytes) = block_size.checked_mul(num_blocks) {
                assert_eq!(num_blocks, ts.bytes_to_blocks(bytes));
            }
        }
    }
}
