use std::collections::VecDeque;
#[cfg(feature = "snappy")]
use std::io::Read;
#[cfg(feature = "lz4")]
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use flatbuffers::DefaultAllocator;

use crate::block::Block;
use crate::db_state::{SsTableInfo, SsTableInfoCodec};
use crate::filter::{BloomFilter, BloomFilterBuilder};
use crate::flatbuffer_types::{
    BlockMeta, BlockMetaArgs, FlatBufferSsTableInfoCodec, SsTableIndex, SsTableIndexArgs,
    SsTableIndexOwned,
};
use crate::{blob::ReadOnlyBlob, config::CompressionCodec};
use crate::{block::BlockBuilder, error::SlateDBError};

#[derive(Clone)]
pub(crate) struct SsTableFormat {
    pub(crate) block_size: usize,
    pub(crate) min_filter_keys: u32,
    pub(crate) sst_codec: Box<dyn SsTableInfoCodec>,
    pub(crate) filter_bits_per_key: u32,
    pub(crate) compression_codec: Option<CompressionCodec>,
}

impl Default for SsTableFormat {
    fn default() -> Self {
        Self {
            block_size: 4096,
            min_filter_keys: 0,
            sst_codec: Box::new(FlatBufferSsTableInfoCodec {}),
            filter_bits_per_key: 10,
            compression_codec: None,
        }
    }
}

impl SsTableFormat {
    pub(crate) async fn read_info(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableInfo, SlateDBError> {
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
        SsTableInfo::decode(sst_metadata_bytes, &*self.sst_codec)
    }

    pub(crate) async fn read_filter(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        let mut filter = None;
        if info.filter_len > 0 {
            let filter_end = info.filter_offset + info.filter_len;
            let filter_offset_range = info.filter_offset as usize..filter_end as usize;
            let filter_bytes = obj.read_range(filter_offset_range).await?;
            let compression_codec = info.compression_codec;
            filter = Some(Arc::new(
                self.decode_filter(filter_bytes, compression_codec)?,
            ));
        }
        Ok(filter)
    }

    pub(crate) fn decode_filter(
        &self,
        filter_bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<BloomFilter, SlateDBError> {
        let filter_bytes = match compression_codec {
            Some(c) => Self::decompress(filter_bytes, c)?,
            None => filter_bytes,
        };
        Ok(BloomFilter::decode(&filter_bytes))
    }

    pub(crate) async fn read_index(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_off = info.index_offset as usize;
        let index_end = index_off + info.index_len as usize;
        let index_bytes = obj.read_range(index_off..index_end).await?;
        let compression_codec = info.compression_codec;
        self.decode_index(index_bytes, compression_codec)
    }

    #[allow(dead_code)]
    pub(crate) fn read_index_raw(
        &self,
        info: &SsTableInfo,
        sst_bytes: &Bytes,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_off = info.index_offset as usize;
        let index_end = index_off + info.index_len as usize;
        let index_bytes: Bytes = sst_bytes.slice(index_off..index_end);
        let compression_codec = info.compression_codec;
        self.decode_index(index_bytes, compression_codec)
    }

    fn decode_index(
        &self,
        index_bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_bytes = match compression_codec {
            Some(c) => Self::decompress(index_bytes, c)?,
            None => index_bytes,
        };
        Ok(SsTableIndexOwned::new(index_bytes)?)
    }

    /// Decompresses the compressed data using the specified compression codec.
    fn decompress(
        #[allow(unused_variables)] compressed_data: Bytes,
        compression_option: CompressionCodec,
    ) -> Result<Bytes, SlateDBError> {
        match compression_option {
            #[cfg(feature = "snappy")]
            CompressionCodec::Snappy => Ok(Bytes::from(
                snap::raw::Decoder::new()
                    .decompress_vec(&compressed_data)
                    .map_err(|_| SlateDBError::BlockDecompressionError)?,
            )),
            #[cfg(feature = "zlib")]
            CompressionCodec::Zlib => {
                let mut decoder = flate2::read::ZlibDecoder::new(&compressed_data[..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|_| SlateDBError::BlockDecompressionError)?;
                Ok(Bytes::from(decompressed))
            }
            #[cfg(feature = "lz4")]
            CompressionCodec::Lz4 => {
                let decompressed = lz4_flex::block::decompress_size_prepended(&compressed_data)
                    .map_err(|_| SlateDBError::BlockDecompressionError)?;
                Ok(Bytes::from(decompressed))
            }
            #[cfg(feature = "zstd")]
            CompressionCodec::Zstd => {
                let decompressed = zstd::stream::decode_all(&compressed_data[..])
                    .map_err(|_| SlateDBError::BlockDecompressionError)?;
                Ok(Bytes::from(decompressed))
            }
        }
    }

    fn block_range(
        &self,
        blocks: Range<usize>,
        info: &SsTableInfo,
        index: &SsTableIndex,
    ) -> Range<usize> {
        let mut end_offset = info.filter_offset as usize;
        if blocks.end < index.block_meta().len() {
            let next_block_meta = index.block_meta().get(blocks.end);
            end_offset = next_block_meta.offset() as usize;
        }
        let start_offset = index.block_meta().get(blocks.start).offset() as usize;
        start_offset..end_offset
    }

    pub(crate) async fn read_blocks(
        &self,
        info: &SsTableInfo,
        index_owned: &SsTableIndexOwned,
        blocks: Range<usize>,
        obj: &impl ReadOnlyBlob,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let index = index_owned.borrow();
        assert!(blocks.start <= blocks.end);
        assert!(blocks.end <= index.block_meta().len());
        if blocks.start == blocks.end {
            return Ok(VecDeque::new());
        }
        let range = self.block_range(blocks.clone(), info, &index);
        let start_offset = range.start;
        let bytes: Bytes = obj.read_range(range).await?;
        let mut decoded_blocks = VecDeque::new();
        let compression_codec = info.compression_codec;
        for block in blocks {
            let block_meta = index.block_meta().get(block);
            let block_bytes_start = block_meta.offset() as usize - start_offset;
            let block_bytes = if block == index.block_meta().len() - 1 {
                bytes.slice(block_bytes_start..)
            } else {
                let next_block_meta = index.block_meta().get(block + 1);
                let block_bytes_end = next_block_meta.offset() as usize - start_offset;
                bytes.slice(block_bytes_start..block_bytes_end)
            };
            decoded_blocks.push_back(self.decode_block(block_bytes, compression_codec)?);
        }
        Ok(decoded_blocks)
    }

    fn decode_block(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<Block, SlateDBError> {
        let checksum_sz = std::mem::size_of::<u32>();
        let block_bytes = bytes.slice(..bytes.len() - checksum_sz);
        let mut checksum_bytes = bytes.slice(bytes.len() - checksum_sz..);
        let checksum = crc32fast::hash(&block_bytes);
        let stored_checksum = checksum_bytes.get_u32();
        if checksum != stored_checksum {
            return Err(SlateDBError::ChecksumMismatch);
        }
        let decoded_block = Block::decode(block_bytes);
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(decoded_block.data, c)?,
            None => decoded_block.data,
        };
        Ok(Block {
            data: decompressed_bytes,
            offsets: decoded_block.offsets,
        })
    }

    pub(crate) async fn read_block(
        &self,
        info: &SsTableInfo,
        index: &SsTableIndexOwned,
        block: usize,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Block, SlateDBError> {
        let mut blocks = self.read_blocks(info, index, block..block + 1, obj).await?;
        Ok(blocks.pop_front().expect("expected a block to be returned"))
    }

    #[allow(dead_code)]
    pub(crate) fn read_block_raw(
        &self,
        info: &SsTableInfo,
        index_owned: &SsTableIndexOwned,
        block: usize,
        sst_bytes: &Bytes,
    ) -> Result<Block, SlateDBError> {
        let index = index_owned.borrow();
        let bytes: Bytes = sst_bytes.slice(self.block_range(block..block + 1, info, &index));
        let compression_codec = info.compression_codec;
        self.decode_block(bytes, compression_codec)
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        EncodedSsTableBuilder::new(
            self.block_size,
            self.min_filter_keys,
            self.sst_codec.clone(),
            self.filter_bits_per_key,
            self.compression_codec,
        )
    }
}

impl SsTableInfo {
    pub(crate) fn encode(info: &SsTableInfo, buf: &mut Vec<u8>, sst_codec: &dyn SsTableInfoCodec) {
        let data = &sst_codec.encode(info);
        buf.extend_from_slice(data);
        buf.put_u32(crc32fast::hash(data));
    }

    pub(crate) fn decode(
        raw_info: Bytes,
        sst_codec: &dyn SsTableInfoCodec,
    ) -> Result<SsTableInfo, SlateDBError> {
        if raw_info.len() <= 4 {
            return Err(SlateDBError::EmptyBlockMeta);
        }
        let data = raw_info.slice(..raw_info.len() - 4).clone();
        let checksum = raw_info.slice(raw_info.len() - 4..).get_u32();
        if checksum != crc32fast::hash(&data) {
            return Err(SlateDBError::ChecksumMismatch);
        }

        let info = sst_codec.decode(&data)?;
        Ok(info)
    }
}

pub(crate) struct EncodedSsTable {
    pub(crate) info: SsTableInfo,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    pub(crate) unconsumed_blocks: VecDeque<Bytes>,
}

/// Builds an SSTable from key-value pairs.
pub(crate) struct EncodedSsTableBuilder<'a> {
    builder: BlockBuilder,
    index_builder: flatbuffers::FlatBufferBuilder<'a, DefaultAllocator>,
    first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    sst_first_key: Option<Bytes>,
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'a>>>,
    current_len: usize,
    blocks: VecDeque<Bytes>,
    block_size: usize,
    min_filter_keys: u32,
    num_keys: u32,
    filter_builder: BloomFilterBuilder,
    sst_codec: Box<dyn SsTableInfoCodec>,
    compression_codec: Option<CompressionCodec>,
}

impl<'a> EncodedSsTableBuilder<'a> {
    /// Create a builder based on target block size.
    fn new(
        block_size: usize,
        min_filter_keys: u32,
        sst_codec: Box<dyn SsTableInfoCodec>,
        filter_bits_per_key: u32,
        compression_codec: Option<CompressionCodec>,
    ) -> Self {
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
            filter_builder: BloomFilterBuilder::new(filter_bits_per_key),
            index_builder: flatbuffers::FlatBufferBuilder::new(),
            sst_codec,
            compression_codec,
        }
    }

    /// Compresses the data using the specified compression codec.
    fn compress(
        #[allow(unused_variables)] data: Bytes,
        c: CompressionCodec,
    ) -> Result<Bytes, SlateDBError> {
        match c {
            #[cfg(feature = "snappy")]
            CompressionCodec::Snappy => {
                let compressed = snap::raw::Encoder::new()
                    .compress_vec(&data)
                    .map_err(|_| SlateDBError::BlockCompressionError)?;
                Ok(Bytes::from(compressed))
            }
            #[cfg(feature = "zlib")]
            CompressionCodec::Zlib => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(&data)
                    .map_err(|_| SlateDBError::BlockCompressionError)?;
                Ok(Bytes::from(
                    encoder
                        .finish()
                        .map_err(|_| SlateDBError::BlockCompressionError)?,
                ))
            }
            #[cfg(feature = "lz4")]
            CompressionCodec::Lz4 => {
                let compressed = lz4_flex::block::compress_prepend_size(&data);
                Ok(Bytes::from(compressed))
            }
            #[cfg(feature = "zstd")]
            CompressionCodec::Zstd => {
                let compressed = zstd::bulk::compress(&data, 3)
                    .map_err(|_| SlateDBError::BlockCompressionError)?;
                Ok(Bytes::from(compressed))
            }
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
            self.first_key = Some(self.index_builder.create_vector(key));
        } else if self.sst_first_key.is_none() {
            self.sst_first_key = Some(Bytes::copy_from_slice(key));
            self.first_key = Some(self.index_builder.create_vector(key));
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
        let compressed_block = match self.compression_codec {
            Some(c) => Self::compress(encoded_block, c)?,
            None => encoded_block,
        };

        let block_meta = BlockMeta::create(
            &mut self.index_builder,
            &BlockMetaArgs {
                offset: self.current_len as u64,
                first_key: self.first_key,
            },
        );

        self.block_meta.push(block_meta);
        let checksum = crc32fast::hash(&compressed_block);
        let total_block_size = compressed_block.len() + std::mem::size_of::<u32>();
        let mut block = Vec::with_capacity(total_block_size);
        block.put(compressed_block);
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
            let compressed_filter = match self.compression_codec {
                None => encoded_filter,
                Some(c) => Self::compress(encoded_filter, c)?,
            };
            filter_len = compressed_filter.len();
            buf.put(compressed_filter);
            maybe_filter = Some(filter);
        }

        // write the index block
        let vector = self.index_builder.create_vector(&self.block_meta);
        let index_wip = SsTableIndex::create(
            &mut self.index_builder,
            &SsTableIndexArgs {
                block_meta: Some(vector),
            },
        );
        self.index_builder.finish(index_wip, None);
        let index_block = Bytes::from(self.index_builder.finished_data().to_vec());
        let index_block = match self.compression_codec {
            None => index_block,
            Some(c) => Self::compress(index_block, c)?,
        };
        let index_offset = self.current_len + buf.len();
        let index_len = index_block.len();
        buf.put(index_block);

        let meta_offset = self.current_len + buf.len();
        let info = SsTableInfo {
            first_key: self.sst_first_key,
            index_offset: index_offset as u64,
            index_len: index_len as u64,
            filter_offset: filter_offset as u64,
            filter_len: filter_len as u64,
            compression_codec: self.compression_codec,
        };
        SsTableInfo::encode(&info, &mut buf, &*self.sst_codec);

        // write the metadata offset at the end of the file. FlatBuffer internal
        // representation is not intended to be used directly.
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
    use std::sync::Arc;

    use bytes::BytesMut;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::*;
    use crate::block_iterator::BlockIterator;
    use crate::db_state::SsTableId;
    use crate::filter::filter_hash;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;

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
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(object_store, format, root_path, None);
        let mut builder = table_store.table_builder();
        builder.add(&[b'a'; 8], Some(&[b'1'; 8])).unwrap();
        builder.add(&[b'b'; 8], Some(&[b'2'; 8])).unwrap();
        builder.add(&[b'c'; 8], Some(&[b'3'; 8])).unwrap();

        // when:
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            &[(
                [b'a'; 8].into(),
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'1'; 8])),
            )],
        )
        .await;
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            &[(
                vec![b'b'; 8],
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
                vec![b'c'; 8],
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
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
        let index = format.read_index_raw(&encoded.info, &raw_sst).unwrap();
        let block = format
            .read_block_raw(&encoded.info, &index, 0, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                vec![b'a'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'1'; 8])),
            )],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 1, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                vec![b'b'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'2'; 8])),
            )],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 2, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_iterator(
            &mut iter,
            &[(
                vec![b'c'; 8],
                ValueDeletable::Value(Bytes::copy_from_slice(&[b'3'; 8])),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_sstable() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::default();
        let table_store = TableStore::new(object_store, format, root_path, None);
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
        let sst_info = sst_handle.info;
        assert_eq!(
            b"key1",
            sst_info.first_key.unwrap().as_ref(),
            "first key in sst info should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let index = table_store
            .read_index(&sst_handle_from_store)
            .await
            .unwrap();
        let sst_info_from_store = sst_handle_from_store.info;
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_info_from_store.first_key.unwrap().as_ref(),
            "first key in sst info should be correct after reading from store"
        );
        assert_eq!(
            b"key1",
            index.borrow().block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct after reading from store"
        );
    }

    #[tokio::test]
    async fn test_sstable_no_filter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(object_store, format, root_path, None);
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
        assert_eq!(sst_handle.info.filter_len, 0);
    }

    #[tokio::test]
    async fn test_sstable_builds_filter_with_correct_bits_per_key() {
        async fn test_inner(filter_bits_per_key: u32) {
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat {
                filter_bits_per_key,
                ..SsTableFormat::default()
            };
            let table_store = TableStore::new(object_store, format, root_path, None);
            let mut builder = table_store.table_builder();
            for k in 0..8 {
                builder
                    .add(format!("{}", k).as_bytes(), Some(b"value"))
                    .unwrap();
            }
            let encoded = builder.build().unwrap();
            let filter = encoded.filter.unwrap();
            let bytes = filter.encode();
            // filters are encoded as a 16-bit number of probes followed by the filter
            assert_eq!(bytes.len() as u32, 2 + filter_bits_per_key);
        }

        test_inner(10).await;
        test_inner(20).await;
    }

    #[tokio::test]
    async fn test_sstable_with_compression() {
        #[allow(unused)]
        async fn test_compression_inner(compression: CompressionCodec) {
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat {
                compression_codec: Some(compression),
                ..SsTableFormat::default()
            };
            let table_store = TableStore::new(object_store, format, root_path, None);
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
            let index = table_store.read_index(&sst_handle).await.unwrap();
            let filter = table_store.read_filter(&sst_handle).await.unwrap().unwrap();

            assert!(filter.might_contain(filter_hash(b"key1")));
            assert!(filter.might_contain(filter_hash(b"key2")));
            assert_eq!(encoded_info, sst_handle.info);
            assert_eq!(1, index.borrow().block_meta().len());
            assert_eq!(
                b"key1",
                sst_handle.info.first_key.unwrap().as_ref(),
                "first key in sst info should be correct"
            );
        }

        #[allow(unused)]
        async fn test_compression_using_sst_info(
            compression: CompressionCodec,
            dummy_codec: CompressionCodec,
        ) {
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat {
                compression_codec: Some(compression),
                ..SsTableFormat::default()
            };
            let table_store =
                TableStore::new(object_store.clone(), format, root_path.clone(), None);
            let mut builder = table_store.table_builder();
            builder.add(b"key1", Some(b"value1")).unwrap();
            builder.add(b"key2", Some(b"value2")).unwrap();
            let encoded = builder.build().unwrap();
            let encoded_info = encoded.info.clone();
            table_store
                .write_sst(&SsTableId::Wal(0), encoded)
                .await
                .unwrap();

            // Decompression is independent of TableFormat. It uses the CompressionFormat from SSTable Info to decompress sst.
            let format = SsTableFormat {
                compression_codec: Some(dummy_codec),
                ..SsTableFormat::default()
            };
            let table_store = TableStore::new(object_store, format, root_path, None);
            let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
            let index = table_store.read_index(&sst_handle).await.unwrap();
            let filter = table_store.read_filter(&sst_handle).await.unwrap().unwrap();

            assert!(filter.might_contain(filter_hash(b"key1")));
            assert!(filter.might_contain(filter_hash(b"key2")));
            assert_eq!(encoded_info, sst_handle.info);
            assert_eq!(1, index.borrow().block_meta().len());
            assert_eq!(
                b"key1",
                sst_handle.info.first_key.unwrap().as_ref(),
                "first key in sst info should be correct"
            );
        }

        #[cfg(feature = "snappy")]
        {
            test_compression_inner(CompressionCodec::Snappy).await;
            test_compression_using_sst_info(CompressionCodec::Snappy, CompressionCodec::Zlib).await;
        }
        #[cfg(feature = "zlib")]
        {
            test_compression_inner(CompressionCodec::Zlib).await;
            test_compression_using_sst_info(CompressionCodec::Zlib, CompressionCodec::Lz4).await;
        }
        #[cfg(feature = "lz4")]
        {
            test_compression_inner(CompressionCodec::Lz4).await;
            test_compression_using_sst_info(CompressionCodec::Lz4, CompressionCodec::Zstd).await;
        }
        #[cfg(feature = "zstd")]
        {
            test_compression_inner(CompressionCodec::Zstd).await;
            test_compression_using_sst_info(CompressionCodec::Zstd, CompressionCodec::Snappy).await;
        }
    }

    #[tokio::test]
    async fn test_read_blocks() {
        // given:
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
        let bytes = bytes.freeze();
        let index = format.read_index_raw(&encoded.info, &bytes).unwrap();
        let blob = BytesBlob { bytes };

        // when:
        let mut blocks = format
            .read_blocks(&info, &index, 0..2, &blob)
            .await
            .unwrap();

        // then:
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[
                (
                    vec![b'a'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 2])),
                ),
                (
                    vec![b'b'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 2])),
                ),
            ],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                vec![b'c'; 20],
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
        let format = SsTableFormat {
            min_filter_keys: 1,
            block_size: 32,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
        let bytes = bytes.freeze();
        let index = format.read_index_raw(&encoded.info, &bytes).unwrap();
        let blob = BytesBlob { bytes };

        // when:
        let mut blocks = format
            .read_blocks(&info, &index, 0..3, &blob)
            .await
            .unwrap();

        // then:
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[
                (
                    vec![b'a'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 2])),
                ),
                (
                    vec![b'b'; 2],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 2])),
                ),
            ],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                vec![b'c'; 20],
                ValueDeletable::Value(Bytes::copy_from_slice(&[3u8; 20])),
            )],
        )
        .await;
        let mut iter = BlockIterator::from_first_key(blocks.pop_front().unwrap());
        assert_iterator(
            &mut iter,
            &[(
                vec![b'd'; 20],
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
