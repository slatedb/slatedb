use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{future::join_all, StreamExt};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::io::AsyncWriteExt;

use crate::db_state::{SSTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use crate::{
    blob::ReadOnlyBlob,
    block::Block,
    inmemory_cache::{BlockCache, CachedBlock},
};

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
    // TODO: we cache the filters here for now, so the db doesn't need to reload them
    //       for each read. This means that all the filters need to fit in memory.
    //       Once we've put in a proper cache, we should instead cache the filter block in
    //       the cache and get rid of this.
    //       https://github.com/slatedb/slatedb/issues/89
    filter_cache: RwLock<HashMap<SsTableId, Option<Arc<BloomFilter>>>>,
    transactional_wal_store: Arc<dyn TransactionalObjectStore>,
    /// In-memory cache for blocks
    block_cache: Option<Arc<dyn BlockCache>>,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<usize, SlateDBError> {
        let object_metadata = self
            .object_store
            .head(&self.path)
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(object_metadata.size)
    }

    async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError> {
        self.object_store
            .get_range(&self.path, range)
            .await
            .map_err(SlateDBError::ObjectStoreError)
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        let file = self.object_store.get(&self.path).await?;
        file.bytes().await.map_err(SlateDBError::ObjectStoreError)
    }
}

impl TableStore {
    #[allow(dead_code)]
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
        block_cache: Option<Arc<dyn BlockCache>>,
    ) -> Self {
        Self::new_with_fp_registry(
            object_store,
            sst_format,
            root_path,
            Arc::new(FailPointRegistry::new()),
            block_cache,
        )
    }

    pub fn new_with_fp_registry(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
        fp_registry: Arc<FailPointRegistry>,
        block_cache: Option<Arc<dyn BlockCache>>,
    ) -> Self {
        Self {
            object_store: object_store.clone(),
            sst_format,
            root_path: root_path.clone(),
            wal_path: "wal",
            compacted_path: "compacted",
            fp_registry,
            filter_cache: RwLock::new(HashMap::new()),
            transactional_wal_store: Arc::new(DelegatingTransactionalObjectStore::new(
                Path::from("/"),
                object_store.clone(),
            )),
            block_cache,
        }
    }

    pub(crate) async fn get_wal_sst_list(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<Vec<u64>, SlateDBError> {
        let mut wal_list: Vec<u64> = Vec::new();
        let wal_path = &Path::from(format!("{}/{}/", &self.root_path, self.wal_path));
        let mut files_stream = self.object_store.list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.parse_id(&file.location, "sst") {
                Ok(wal_id) => {
                    if wal_id > wal_id_last_compacted {
                        wal_list.push(wal_id);
                    }
                }
                Err(_) => continue,
            }
        }

        wal_list.sort();
        Ok(wal_list)
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
    ) -> Result<SSTableHandle, SlateDBError> {
        fail_point!(
            self.fp_registry.clone(),
            "write-wal-sst-io-error",
            matches!(id, SsTableId::Wal(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
        );
        fail_point!(
            self.fp_registry.clone(),
            "write-compacted-sst-io-error",
            matches!(id, SsTableId::Compacted(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
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
                    SsTableId::Compacted(_) => SlateDBError::ObjectStoreError(e),
                },
                _ => SlateDBError::ObjectStoreError(e),
            })?;

        self.cache_filter(*id, encoded_sst.filter);
        Ok(SSTableHandle {
            id: *id,
            info: encoded_sst.info,
        })
    }

    fn cache_filter(&self, sst: SsTableId, filter: Option<Arc<BloomFilter>>) {
        {
            let mut wguard = self.filter_cache.write();
            wguard.insert(sst, filter);
        }
    }

    // todo: clean up the warning suppression when we start using open_sst outside tests
    #[allow(dead_code)]
    pub(crate) async fn open_sst(&self, id: &SsTableId) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let info = self.sst_format.read_info(&obj).await?;
        Ok(SSTableHandle { id: *id, info })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SSTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        {
            let rguard = self.filter_cache.read();
            if let Some(filter) = rguard.get(&handle.id) {
                return Ok(filter.clone());
            }
        }
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let filter = self.sst_format.read_filter(&handle.info, &obj).await?;
        let mut wguard = self.filter_cache.write();
        wguard.insert(handle.id, filter.clone());
        Ok(filter)
    }

    pub(crate) async fn read_index(
        &self,
        handle: &SSTableHandle,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format.read_index(&handle.info, &obj).await
    }

    #[allow(dead_code)]
    pub(crate) async fn read_blocks(
        &self,
        handle: &SSTableHandle,
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
    /// TODO: we probably won't need this once we're caching the index
    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &SSTableHandle,
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
            // Attempt to get all requested blocks from cache concurrently
            let cached_blocks =
                join_all(blocks.clone().map(|block_num| async move {
                    cache.get((handle.id, block_num)).await.block()
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
            for (block_num, block_read) in range.zip(range_blocks?) {
                let block = Arc::new(block_read);
                if cache_blocks {
                    blocks_to_cache.push((handle.id, block_num, block.clone()));
                }
                blocks_read.insert(block_num - blocks.start, block);
            }
        }

        // Cache the newly read blocks if caching is enabled
        if let Some(cache) = &self.block_cache {
            if !blocks_to_cache.is_empty() {
                join_all(blocks_to_cache.into_iter().map(|(id, block_num, block)| {
                    cache.insert((id, block_num), CachedBlock::Block(block))
                }))
                .await;
            }
        }

        Ok(blocks_read)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_block(
        &self,
        handle: &SSTableHandle,
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

    fn parse_id(&self, path: &Path, expected_extension: &str) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == expected_extension => path
                .filename()
                .expect("invalid wal file")
                .split('.')
                .next()
                .ok_or_else(|| SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState),
            _ => Err(SlateDBError::InvalidDBState),
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
    pub async fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SlateDBError> {
        self.builder.add(key, value)?;
        self.drain_blocks().await
    }

    pub async fn close(mut self) -> Result<SSTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build()?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.as_ref()).await?;
        }
        self.writer.shutdown().await?;
        self.table_store.cache_filter(self.id, encoded_sst.filter);
        Ok(SSTableHandle {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::collections::VecDeque;

    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use ulid::Ulid;

    use crate::sst_iter::SstIterator;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;
    use crate::{
        block::Block, block_iterator::BlockIterator, db_state::SsTableId, iter::KeyValueIterator,
    };
    use crate::{error, tablestore::BlockCache};
    use crate::{
        inmemory_cache::{BlockCacheOptions, MokaCache},
        sst::SsTableFormat,
    };

    const ROOT: &str = "/root";

    #[tokio::test]
    async fn test_sst_writer_should_write_sst() {
        // given:
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat::new(32, 1, None);
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));
        let id = SsTableId::Compacted(Ulid::new());

        // when:
        let mut writer = ts.table_writer(id);
        writer.add(&[b'a'; 16], Some(&[1u8; 16])).await.unwrap();
        writer.add(&[b'b'; 16], Some(&[2u8; 16])).await.unwrap();
        writer.add(&[b'c'; 16], None).await.unwrap();
        writer.add(&[b'd'; 16], Some(&[4u8; 16])).await.unwrap();
        let sst = writer.close().await.unwrap();

        // then:
        let mut iter = SstIterator::new(&sst, ts.clone(), 1, 1, true)
            .await
            .unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    vec![b'a'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 16])),
                ),
                (
                    vec![b'b'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 16])),
                ),
                (vec![b'c'; 16], ValueDeletable::Tombstone),
                (
                    vec![b'd'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[4u8; 16])),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_wal_write_should_fail_when_fenced() {
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat::new(32, 1, None);
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT), None));
        let wal_id = SsTableId::Wal(1);

        // write a wal sst
        let mut sst1 = ts.table_builder();
        sst1.add(b"key", Some(b"value")).unwrap();
        let table = sst1.build().unwrap();
        ts.write_sst(&wal_id, table).await.unwrap();

        let mut sst2 = ts.table_builder();
        sst2.add(b"key", Some(b"value")).unwrap();
        let table2 = sst2.build().unwrap();

        // write another walsst with the same id.
        let result = ts.write_sst(&wal_id, table2).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_tablestore_sst_and_partial_cache_hits() {
        // Setup
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat::new(32, 1, None);
        let block_cache = Arc::new(MokaCache::new(BlockCacheOptions::default()));
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
            writer.add(&key, Some(&value)).await.unwrap();
        }
        let handle = writer.close().await.unwrap();

        // Read the index
        let index = Arc::new(ts.read_index(&handle).await.unwrap());

        // Test 1: SST hit
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();

        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are now in cache
        for i in 0..20 {
            assert!(
                block_cache.get((handle.id, i)).await.block().is_some(),
                "Block {} should be in cache",
                i
            );
        }

        // Partially clear the cache (remove blocks 5..10 and 15..20)
        for i in 5..10 {
            block_cache.remove((handle.id, i)).await;
        }
        for i in 15..20 {
            block_cache.remove((handle.id, i)).await;
        }

        // Test 2: Partial cache hit, everything should be returned since missing blocks are returned from sst
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are again in cache
        for i in 0..20 {
            assert!(
                block_cache.get((handle.id, i)).await.block().is_some(),
                "Block {} should be in cache after partial hit",
                i
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
            assert!(
                block_cache.get((handle.id, i)).await.block().is_some(),
                "Block {} should be in cache after SST emptying",
                i
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
}
