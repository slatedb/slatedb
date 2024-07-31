use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::filter::{BloomFilter, BloomFilterBuilder};
use crate::flatbuffer_types::{
    BlockMeta, BlockMetaArgs, SsTableInfo, SsTableInfoArgs, SsTableInfoOwned,
};
use crate::{block::BlockBuilder, error::SlateDBError};
use bytes::{Buf, BufMut, Bytes};
use flatbuffers::DefaultAllocator;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct SsTableFormat {
    block_size: usize,
    min_filter_keys: u32,
}

impl SsTableFormat {
    pub fn new(block_size: usize, min_filter_keys: u32) -> Self {
        Self {
            block_size,
            min_filter_keys,
        }
    }

    pub(crate) async fn read_info(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableInfoOwned, SlateDBError> {
        let len = obj.len().await?;
        if len <= 4 {
            return Err(SlateDBError::EmptySSTable);
        }
        // Get the size of the metadata
        let sst_metadata_offset_range = (len - 4)..len;
        let sst_metadata_offset =
            obj.read_range(sst_metadata_offset_range).await?.get_u32() as usize;
        // Get the metadata. Last 4 bytes are the offset of SsTableInfo
        let sst_metadata_range = sst_metadata_offset..len - 4;
        let sst_metadata_bytes = obj.read_range(sst_metadata_range).await?;
        SsTableInfoOwned::decode(sst_metadata_bytes)
    }

    pub(crate) async fn read_filter(
        &self,
        info: &SsTableInfoOwned,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        let mut filter = None;
        let handle = info.borrow();
        if handle.filter_len() > 0 {
            let filter_end = handle.filter_offset() + handle.filter_len();
            let filter_offset_range = handle.filter_offset() as usize..filter_end as usize;
            let filter_bytes = obj.read_range(filter_offset_range).await?;
            filter = Some(Arc::new(BloomFilter::decode(&filter_bytes)));
        }
        Ok(filter)
    }

    fn block_range(&self, blocks: Range<usize>, handle: &SsTableInfo) -> Range<usize> {
        let mut end_offset = handle.filter_offset() as usize;
        if blocks.end < handle.block_meta().len() {
            let next_block_meta = handle.block_meta().get(blocks.end);
            end_offset = next_block_meta.offset() as usize;
        }
        let start_offset = handle.block_meta().get(blocks.start).offset() as usize;
        start_offset..end_offset
    }

    pub(crate) async fn read_blocks(
        &self,
        info: &SsTableInfoOwned,
        blocks: Range<usize>,
        obj: &impl ReadOnlyBlob,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let handle = &info.borrow();
        assert!(blocks.start <= blocks.end);
        assert!(blocks.end <= handle.block_meta().len());
        if blocks.start == blocks.end {
            return Ok(VecDeque::new());
        }
        let range = self.block_range(blocks.clone(), handle);
        let start_offset = range.start;
        let bytes: Bytes = obj.read_range(range).await?;
        let mut decoded_blocks = VecDeque::new();
        for block in blocks {
            let block_meta = handle.block_meta().get(block);
            let block_bytes_start = block_meta.offset() as usize - start_offset;
            let block_bytes = if block == handle.block_meta().len() - 1 {
                bytes.slice(block_bytes_start..)
            } else {
                let next_block_meta = handle.block_meta().get(block + 1);
                let block_bytes_end = next_block_meta.offset() as usize - start_offset;
                bytes.slice(block_bytes_start..block_bytes_end)
            };
            decoded_blocks.push_back(self.decode_block(block_bytes)?);
        }
        Ok(decoded_blocks)
    }

    fn decode_block(&self, bytes: Bytes) -> Result<Block, SlateDBError> {
        let checksum_sz = std::mem::size_of::<u32>();
        let block_bytes = bytes.slice(..bytes.len() - checksum_sz);
        let mut checksum_bytes = bytes.slice(bytes.len() - checksum_sz..);
        let checksum = crc32fast::hash(&block_bytes);
        let stored_checksum = checksum_bytes.get_u32();
        if checksum != stored_checksum {
            return Err(SlateDBError::ChecksumMismatch);
        }
        Ok(Block::decode(block_bytes))
    }

    pub(crate) async fn read_block(
        &self,
        info: &SsTableInfoOwned,
        block: usize,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Block, SlateDBError> {
        let mut blocks = self.read_blocks(info, block..block + 1, obj).await?;
        Ok(blocks.pop_front().expect("expected a block to be returned"))
    }

    #[allow(dead_code)]
    pub(crate) fn read_block_raw(
        &self,
        info: &SsTableInfoOwned,
        block: usize,
        sst_bytes: &Bytes,
    ) -> Result<Block, SlateDBError> {
        let handle = &info.borrow();
        let bytes: Bytes = sst_bytes.slice(self.block_range(block..block + 1, handle));
        self.decode_block(bytes)
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        EncodedSsTableBuilder::new(self.block_size, self.min_filter_keys)
    }
}

impl SsTableInfoOwned {
    fn encode(info: &SsTableInfoOwned, buf: &mut Vec<u8>) {
        buf.extend_from_slice(info.data());
        buf.put_u32(crc32fast::hash(info.data()));
    }

    pub(crate) fn decode(raw_block_meta: Bytes) -> Result<SsTableInfoOwned, SlateDBError> {
        if raw_block_meta.len() <= 4 {
            return Err(SlateDBError::EmptyBlockMeta);
        }
        let data = raw_block_meta.slice(..raw_block_meta.len() - 4).clone();
        let checksum = raw_block_meta.slice(raw_block_meta.len() - 4..).get_u32();
        if checksum != crc32fast::hash(&data) {
            return Err(SlateDBError::ChecksumMismatch);
        }

        let info = SsTableInfoOwned::new(data)?;
        Ok(info)
    }
}

pub(crate) struct EncodedSsTable {
    pub(crate) info: SsTableInfoOwned,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    pub(crate) unconsumed_blocks: VecDeque<Bytes>,
}

/// Builds an SSTable from key-value pairs.
pub(crate) struct EncodedSsTableBuilder<'a> {
    builder: BlockBuilder,
    sst_info_builder: flatbuffers::FlatBufferBuilder<'a, DefaultAllocator>,
    first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    sst_first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'a>>>,
    current_len: usize,
    blocks: VecDeque<Bytes>,
    block_size: usize,
    min_filter_keys: u32,
    num_keys: u32,
    filter_builder: BloomFilterBuilder,
}

impl<'a> EncodedSsTableBuilder<'a> {
    /// Create a builder based on target block size.
    fn new(block_size: usize, min_filter_keys: u32) -> Self {
        Self {
            current_len: 0,
            blocks: VecDeque::new(),
            block_meta: Vec::new(),
            first_key: None,
            sst_first_key: None,
            block_size,
            builder: BlockBuilder::new(block_size),
            min_filter_keys,
            num_keys: 0,
            filter_builder: BloomFilterBuilder::new(10),
            sst_info_builder: flatbuffers::FlatBufferBuilder::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SlateDBError> {
        self.num_keys += 1;

        if !self.builder.add(key, value) {
            // Create a new block builder and append block data
            if let Some(block) = self.finish_block()? {
                self.current_len += block.len();
                self.blocks.push_back(Bytes::from(block));
            }

            // New block must always accept the first KV pair
            assert!(self.builder.add(key, value));
            self.first_key = Some(self.sst_info_builder.create_vector(key));
        } else if self.sst_first_key.is_none() {
            self.sst_first_key = Some(self.sst_info_builder.create_vector(key));
            self.first_key = Some(self.sst_info_builder.create_vector(key));
        }

        self.filter_builder.add_key(key);

        Ok(())
    }

    pub fn next_block(&mut self) -> Option<Bytes> {
        self.blocks.pop_front()
    }

    #[allow(dead_code)]
    pub fn estimated_size(&self) -> usize {
        self.current_len
    }

    fn finish_block(&mut self) -> Result<Option<Vec<u8>>, SlateDBError> {
        if self.builder.is_empty() {
            return Ok(None);
        }

        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build()?.encode();

        let block_meta = BlockMeta::create(
            &mut self.sst_info_builder,
            &BlockMetaArgs {
                offset: self.current_len as u64,
                first_key: self.first_key,
            },
        );

        self.block_meta.push(block_meta);
        let checksum = crc32fast::hash(&encoded_block);
        let total_block_size = encoded_block.len() + std::mem::size_of::<u32>();
        let mut block = Vec::with_capacity(total_block_size);
        block.put(encoded_block);
        block.put_u32(checksum);
        Ok(Some(block))
    }

    pub fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        let mut buf = self.finish_block()?.unwrap_or(Vec::new());
        let mut maybe_filter = None;
        let mut filter_len = 0;
        let filter_offset = self.current_len + buf.len();
        if self.num_keys >= self.min_filter_keys {
            let filter = Arc::new(self.filter_builder.build());
            let encoded_filter = filter.encode();
            filter_len = encoded_filter.len();
            buf.put(encoded_filter);
            maybe_filter = Some(filter);
        }

        let meta_offset = self.current_len + buf.len();
        let vector = self.sst_info_builder.create_vector(&self.block_meta);
        let info_wip_offset = SsTableInfo::create(
            &mut self.sst_info_builder,
            &SsTableInfoArgs {
                first_key: self.sst_first_key,
                block_meta: Some(vector),
                filter_offset: filter_offset as u64,
                filter_len: filter_len as u64,
            },
        );

        self.sst_info_builder.finish(info_wip_offset, None);
        let info =
            SsTableInfoOwned::new(Bytes::from(self.sst_info_builder.finished_data().to_vec()))?;

        SsTableInfoOwned::encode(&info, &mut buf);

        // write the metadata offset at the end of the file. FlatBuffer internal representation is not intended to be used directly.
        buf.put_u32(meta_offset as u32);
        self.blocks.push_back(Bytes::from(buf));
        Ok(EncodedSsTable {
            info,
            filter: maybe_filter,
            unconsumed_blocks: self.blocks,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::block_iterator::BlockIterator;
    use crate::db_state::SsTableId;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;
    use bytes::BytesMut;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    use super::*;

    fn next_block_to_iter(builder: &mut EncodedSsTableBuilder) -> BlockIterator<Block> {
        let block = builder.next_block();
        assert!(block.is_some());
        let block = block.unwrap();
        let block = Block::decode(block.slice(..block.len() - 4));
        BlockIterator::from_first_key(block)
    }

    #[tokio::test]
    async fn test_builder_should_make_blocks_available() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(32, 0);
        let table_store = TableStore::new(object_store, format, root_path);
        let mut builder = table_store.table_builder();
        builder.add(&[b'a'; 8], Some(&[b'1'; 8])).unwrap();
        builder.add(&[b'b'; 8], Some(&[b'2'; 8])).unwrap();
        builder.add(&[b'c'; 8], Some(&[b'3'; 8])).unwrap();

        // when:
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            &[(
                &[b'a'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'1'; 8])),
            )],
        )
        .await;
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            &[(
                &[b'b'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'2'; 8])),
            )],
        )
        .await;
        assert!(builder.next_block().is_none());
        builder.add(&[b'd'; 8], Some(&[b'4'; 8])).unwrap();
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            &[(
                &[b'c'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'3'; 8])),
            )],
        )
        .await;
        assert!(builder.next_block().is_none());
    }

    #[tokio::test]
    async fn test_builder_should_return_unconsumed_blocks() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(32, 0);
        let table_store = TableStore::new(object_store, format.clone(), root_path);
        let mut builder = table_store.table_builder();
        builder.add(&[b'a'; 8], Some(&[b'1'; 8])).unwrap();
        builder.add(&[b'b'; 8], Some(&[b'2'; 8])).unwrap();
        builder.add(&[b'c'; 8], Some(&[b'3'; 8])).unwrap();
        let first_block = builder.next_block();

        let mut encoded = builder.build().unwrap();

        let mut raw_sst = Vec::<u8>::new();
        raw_sst.put_slice(first_block.unwrap().as_ref());
        assert_eq!(encoded.unconsumed_blocks.len(), 2);
        while let Some(block) = encoded.unconsumed_blocks.pop_front() {
            raw_sst.put_slice(block.as_ref());
        }
        let raw_sst = Bytes::copy_from_slice(raw_sst.as_slice());
        let block = format.read_block_raw(&encoded.info, 0, &raw_sst).unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                &[b'a'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'1'; 8])),
            )],
        )
        .await;
        let block = format.read_block_raw(&encoded.info, 1, &raw_sst).unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                &[b'b'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'2'; 8])),
            )],
        )
        .await;
        let block = format.read_block_raw(&encoded.info, 2, &raw_sst).unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                &[b'c'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'3'; 8])),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_sstable() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 0);
        let table_store = TableStore::new(object_store, format, root_path);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();

        // write sst and validate that the handle returned has the correct content.
        let sst_handle = table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        assert_eq!(encoded_info, sst_handle.info);
        let sst_info = sst_handle.info.borrow();
        assert_eq!(1, sst_info.block_meta().len());
        assert_eq!(
            b"key1",
            sst_info.first_key().unwrap().bytes(),
            "first key in sst info should be correct"
        );
        assert_eq!(
            b"key1",
            sst_info.block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let sst_info_from_store = sst_handle_from_store.info.borrow();
        assert_eq!(1, sst_info_from_store.block_meta().len());
        assert_eq!(
            b"key1",
            sst_info_from_store.first_key().unwrap().bytes(),
            "first key in sst info should be correct after reading from store"
        );
        assert_eq!(
            b"key1",
            sst_info_from_store.block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct after reading from store"
        );
    }

    #[tokio::test]
    async fn test_sstable_no_filter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = TableStore::new(object_store, format, root_path);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle.info);
        let handle = sst_handle.info.borrow();
        assert_eq!(handle.filter_len(), 0);
    }

    #[tokio::test]
    async fn test_read_blocks() {
        // given:
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(32, 1);
        let table_store = TableStore::new(object_store, format.clone(), root_path);
        let mut builder = table_store.table_builder();
        builder.add(&[b'a'; 2], Some(&[1u8; 2])).unwrap();
        builder.add(&[b'b'; 2], Some(&[2u8; 2])).unwrap();
        builder.add(&[b'c'; 20], Some(&[3u8; 20])).unwrap();
        builder.add(&[b'd'; 20], Some(&[4u8; 20])).unwrap();
        let encoded = builder.build().unwrap();
        let info = encoded.info.clone();
        let mut bytes = BytesMut::new();
        encoded
            .unconsumed_blocks
            .iter()
            .for_each(|b| bytes.put(b.clone()));
        let blob = BytesBlob {
            bytes: bytes.freeze(),
        };

        // when:
        let mut blocks = format.read_blocks(&info, 0..2, &blob).await.unwrap();

        // then:
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 2])),
                ),
                (
                    &[b'b'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 2])),
                ),
            ],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                &[b'c'; 20],
                ValueDeletable::Value(Bytes::copy_from_slice(&[3u8; 20])),
            )],
        )
        .await;
        assert!(blocks.is_empty())
    }

    #[tokio::test]
    async fn test_read_all_blocks() {
        // given:
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(32, 1);
        let table_store = TableStore::new(object_store, format.clone(), root_path);
        let mut builder = table_store.table_builder();
        builder.add(&[b'a'; 2], Some(&[1u8; 2])).unwrap();
        builder.add(&[b'b'; 2], Some(&[2u8; 2])).unwrap();
        builder.add(&[b'c'; 20], Some(&[3u8; 20])).unwrap();
        builder.add(&[b'd'; 20], Some(&[4u8; 20])).unwrap();
        let encoded = builder.build().unwrap();
        let info = encoded.info.clone();
        let mut bytes = BytesMut::new();
        encoded
            .unconsumed_blocks
            .iter()
            .for_each(|b| bytes.put(b.clone()));
        let blob = BytesBlob {
            bytes: bytes.freeze(),
        };

        // when:
        let mut blocks = format.read_blocks(&info, 0..3, &blob).await.unwrap();

        // then:
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 2])),
                ),
                (
                    &[b'b'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 2])),
                ),
            ],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                &[b'c'; 20],
                ValueDeletable::Value(Bytes::copy_from_slice(&[3u8; 20])),
            )],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                &[b'd'; 20],
                ValueDeletable::Value(Bytes::copy_from_slice(&[4u8; 20])),
            )],
        )
        .await;
        assert!(blocks.is_empty())
    }

    struct BytesBlob {
        bytes: Bytes,
    }

    impl ReadOnlyBlob for BytesBlob {
        async fn len(&self) -> Result<usize, SlateDBError> {
            Ok(self.bytes.len())
        }

        async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError> {
            Ok(self.bytes.slice(range))
        }

        async fn read(&self) -> Result<Bytes, SlateDBError> {
            Ok(self.bytes.clone())
        }
    }
}
