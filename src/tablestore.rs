use crate::block::Block;
use crate::db_state::{SSTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use crate::{blob::ReadOnlyBlob, db::BlockCache};
use bytes::{BufMut, Bytes};
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{future::join_all, StreamExt};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

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
    block_cache: Option<Arc<BlockCache>>,
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
        block_cache: Option<Arc<BlockCache>>,
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
        block_cache: Option<Arc<BlockCache>>,
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

        self.cache_filter(id.clone(), encoded_sst.filter);
        Ok(SSTableHandle {
            id: id.clone(),
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
        Ok(SSTableHandle {
            id: id.clone(),
            info,
        })
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
        wguard.insert(handle.id.clone(), filter.clone());
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

    // TODO: we probably won't need this once we're caching the index
    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &SSTableHandle,
        index: Arc<SsTableIndexOwned>,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Arc<Block>>, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let mut blocks_read = VecDeque::with_capacity(blocks.end - blocks.start);
        let mut uncached_ranges = Vec::new();

        if let Some(cache) = &self.block_cache {
            let cached_blocks = join_all(
                blocks
                    .clone()
                    .map(|block_num| async move { cache.get(&(handle.id, block_num)).await }),
            )
            .await;

            let mut last_uncached_start = None;

            for (index, block_result) in cached_blocks.into_iter().enumerate() {
                match block_result {
                    Some(cached_block) => {
                        if let Some(start) = last_uncached_start.take() {
                            uncached_ranges.push((blocks.start + start)..(blocks.start + index));
                        }
                        blocks_read.push_back(cached_block);
                    }
                    None => {
                        last_uncached_start.get_or_insert(index);
                    }
                }
            }

            if let Some(start) = last_uncached_start {
                uncached_ranges.push((blocks.start + start)..blocks.end);
            }
        } else {
            uncached_ranges.push(blocks.clone());
        }

        // Read uncached blocks
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

        // Merge uncached blocks with blocks_read
        for (range, range_blocks) in uncached_ranges.into_iter().zip(uncached_blocks) {
            for (block_num, block_read) in range.zip(range_blocks?) {
                let block = Arc::new(block_read);
                if let Some(cache) = &self.block_cache {
                    cache.insert((handle.id, block_num), block.clone()).await;
                }
                blocks_read.insert(block_num - blocks.start, block);
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
        self.table_store
            .cache_filter(self.id.clone(), encoded_sst.filter);
        Ok(SSTableHandle {
            id: self.id.clone(),
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
    use crate::error;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstIterator;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;
    use crate::{db::BlockCache, db_state::SsTableId};
    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::sync::Arc;
    use ulid::Ulid;

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
        let mut iter = SstIterator::new(&sst, ts.clone(), 1, 1).await.unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 16])),
                ),
                (
                    &[b'b'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 16])),
                ),
                (&[b'c'; 16], ValueDeletable::Tombstone),
                (
                    &[b'd'; 16],
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
        let format = SsTableFormat::new(20, 1, None);
        let block_cache = Arc::new(BlockCache::new(1000));
        let ts = Arc::new(TableStore::new(
            os.clone(),
            format,
            Path::from("/root"),
            Some(block_cache.clone()),
        ));

        // Create and write SST
        let id = SsTableId::Compacted(Ulid::new());
        let mut writer = ts.table_writer(id.clone());
        for i in 0..20 {
            writer.add(&[i], Some(&[i])).await.unwrap();
        }
        let handle = writer.close().await.unwrap();

        // Read index
        let index = Arc::new(ts.read_index(&handle).await.unwrap());

        // Test 1: SST hit
        let sst_blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20)
            .await
            .unwrap();
        assert_eq!(sst_blocks.len(), 20);

        // Partially clear the cache (remove blocks 5..10 and 15..20)
        for i in 5..10 {
            block_cache.remove(&(handle.id.clone(), i)).await;
        }
        for i in 15..20 {
            block_cache.remove(&(handle.id.clone(), i)).await;
        }

        // Test 2: Partial cache hit, everything should be returned since missing blocks are returned from sst
        let result = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20)
            .await;
        assert_eq!(result.unwrap().len(), 20);

        // Test 3: Verify deleted blocks were added back to cache
        for i in 5..10 {
            assert!(block_cache.get(&(handle.id.clone(), i)).await.is_some());
        }
        for i in 15..20 {
            assert!(block_cache.get(&(handle.id.clone(), i)).await.is_some());
        }

        // Delete SST to see cache hit occurs for all blocks
        let path = ts.path(&id);
        os.delete(&path).await.unwrap();

        // Test 4: All blocks should be in cache after SST deletion
        let cache_only_result = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20)
            .await;
        assert_eq!(cache_only_result.unwrap().len(), 20);

        // Test 5: Verify that reading specific ranges still works after SST deletion
        let partial_result_1 = ts
            .read_blocks_using_index(&handle, index.clone(), 5..10)
            .await;
        assert_eq!(partial_result_1.unwrap().len(), 5);

        let partial_result_2 = ts
            .read_blocks_using_index(&handle, index.clone(), 15..20)
            .await;
        assert_eq!(partial_result_2.unwrap().len(), 5);
    }
}
