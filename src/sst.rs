use std::collections::VecDeque;
#[cfg(feature = "snappy")]
use std::io::Read;
#[cfg(feature = "lz4")]
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use flatbuffers::DefaultAllocator;

use crate::block::{Block, BLOCK_SIZE_BYTES, SIZEOF_U16, SIZEOF_U32, SIZEOF_U64};
use crate::filter::{BloomFilter, BloomFilterBuilder, DEFAULT_BITS_PER_KEY};

use crate::flatbuffer_types::{
    BlockMeta, BlockMetaArgs, SsTableIndex, SsTableIndexArgs, SsTableIndexOwned, SsTableInfo,
    SsTableInfoArgs, SsTableInfoOwned,
};
use crate::{blob::ReadOnlyBlob, config::CompressionCodec};
use crate::{block::BlockBuilder, error::SlateDBError};

#[derive(Clone)]
pub(crate) struct SsTableFormat {
    block_size: usize,
    min_filter_keys: u32,
    compression_codec: Option<CompressionCodec>,
}

impl SsTableFormat {
    pub fn new(
        block_size: usize,
        min_filter_keys: u32,
        compression_codec: Option<CompressionCodec>,
    ) -> Self {
        Self {
            block_size,
            min_filter_keys,
            compression_codec,
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
            let compression_codec = handle.compression_format();
            filter = Some(Arc::new(
                self.decode_filter(filter_bytes, compression_codec.into())?,
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
        info_owned: &SsTableInfoOwned,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let info = info_owned.borrow();
        let index_off = info.index_offset() as usize;
        let index_end = index_off + info.index_len() as usize;
        let index_bytes = obj.read_range(index_off..index_end).await?;
        let compression_codec = info.compression_format();
        self.decode_index(index_bytes, compression_codec.into())
    }

    #[allow(dead_code)]
    pub(crate) fn read_index_raw(
        &self,
        info_owned: &SsTableInfoOwned,
        sst_bytes: &Bytes,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let info = info_owned.borrow();
        let index_off = info.index_offset() as usize;
        let index_end = index_off + info.index_len() as usize;
        let index_bytes: Bytes = sst_bytes.slice(index_off..index_end);
        let compression_codec = info.compression_format();
        self.decode_index(index_bytes, compression_codec.into())
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
        handle: &SsTableInfo,
        index: &SsTableIndex,
    ) -> Range<usize> {
        let mut end_offset = handle.filter_offset() as usize;
        if blocks.end < index.block_meta().len() {
            let next_block_meta = index.block_meta().get(blocks.end);
            end_offset = next_block_meta.offset() as usize;
        }
        let start_offset = index.block_meta().get(blocks.start).offset() as usize;
        start_offset..end_offset
    }

    pub(crate) async fn read_blocks(
        &self,
        info: &SsTableInfoOwned,
        index_owned: &SsTableIndexOwned,
        blocks: Range<usize>,
        obj: &impl ReadOnlyBlob,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let handle = &info.borrow();
        let index = index_owned.borrow();
        assert!(blocks.start <= blocks.end);
        assert!(blocks.end <= index.block_meta().len());
        if blocks.start == blocks.end {
            return Ok(VecDeque::new());
        }
        let range = self.block_range(blocks.clone(), handle, &index);
        let start_offset = range.start;
        let bytes: Bytes = obj.read_range(range).await?;
        let mut decoded_blocks = VecDeque::new();
        let compression_codec = handle.compression_format();
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
            decoded_blocks.push_back(self.decode_block(block_bytes, compression_codec.into())?);
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
        info: &SsTableInfoOwned,
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
        info: &SsTableInfoOwned,
        index_owned: &SsTableIndexOwned,
        block: usize,
        sst_bytes: &Bytes,
    ) -> Result<Block, SlateDBError> {
        let handle = &info.borrow();
        let index = index_owned.borrow();
        let bytes: Bytes = sst_bytes.slice(self.block_range(block..block + 1, handle, &index));
        let compression_codec = handle.compression_format();
        self.decode_block(bytes, compression_codec.into())
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        EncodedSsTableBuilder::new(
            self.block_size,
            self.min_filter_keys,
            self.compression_codec,
        )
    }

    /// Estimate the SsTable size given a memtable, including key, value length
    /// headers, offset ptrs, bloom filter sizes, and block overheads.
    /// TODO: Consider BlockBuilder.estimated_size() for some of these calcs.
    /// TODO: Add 2 checksums, one per block and one on the SST.
    /// +--------------------------------------------------------------------------------+
    /// |                                Data Blocks                                     |
    /// +--------------------------------------------------------------------------------+
    /// +--------------------------------------------------------------------------------+
    /// |                                                                                |
    /// | +--------------+----------+---------+-----------+------------+-----------+----+|
    /// | |  Number of   | K-V      |   Key   |   Key     |   Value    | Value     |    ||
    /// | |  KV Pairs    | Offsets  |  Length |   Data    |   Length   | Data      | .. ||
    /// | |    (2B)      | (2B each)|   (2B)  | (variable)|    (4B)    | (variable)|    ||
    /// | +--------------+----------+---------+-----------+------------+-----------+----+|
    /// |                                               followed by:   | Checksum |      |
    /// |                                                              |   (4B)   |      |
    /// +--------------------------------------------------------------------------------+
    /// |                             More Data Blocks                                   |
    /// +--------------------------------------------------------------------------------+
    /// |                                                                                |
    /// |                        ... (Additional Data Blocks) ...                        |
    /// |                                                                                |
    /// +--------------------------------------------------------------------------------+
    /// |                               Index Block                                      |
    /// +--------------------------------------------------------------------------------+
    /// +--------------------------------------------------------------------------------+
    /// |                              Bloom filter                                      |
    /// |                                                                                |
    /// |                (variable size, bits_per_key * num keys)                        |
    /// +--------------------------------------------------------------------------------+
    /// |                     Index: per data block metadata                             |
    /// |                                                                                |
    /// |    (Index Data: first key(variable) + Offset per Data Blocks) (variable size)  |
    /// +--------------------------------------------------------------------------------+
    /// |                              SSTable Info                                      |
    /// |                                                                                |
    /// | First Key (variable) | Index Offset (8B) | Index Length (8B) |                 |
    /// | Filter Offset (8B) | Filter Length (8B) | Compression Format (1B)              |
    /// |                                                                                |
    /// +--------------------------------------------------------------------------------+
    /// |                           Metadata Offset (4 bytes)                            |
    /// |                           (points to SSTable Info)                             |
    /// +--------------------------------------------------------------------------------+
    /// |                              SST Checksum (4 bytes)                            |
    /// +--------------------------------------------------------------------------------+
    pub(crate) fn estimate_sst_size(
        &self,
        num_entries: usize,
        total_entry_size_bytes: usize,
    ) -> usize {
        if num_entries == 0 {
            return 0;
        }

        // Data Blocks.

        // u16 for key size, u32 for value size
        let entry_key_and_val_size_header_offset = SIZEOF_U16 + SIZEOF_U32;
        let entry_offset_overhead = SIZEOF_U16;
        let per_entry_overhead = entry_key_and_val_size_header_offset + entry_offset_overhead;

        let number_of_blocks = usize::div_ceil(total_entry_size_bytes, BLOCK_SIZE_BYTES);
        let block_kv_pair_count = SIZEOF_U16; // Number of key-value pairs in the block.
        let block_checksum = SIZEOF_U16; // checksum at the end of the block.
        let per_block_overhead = block_kv_pair_count + block_checksum;
        let total_block_overhead = number_of_blocks * per_block_overhead;
        let total_data_blocks_bytes =
            total_entry_size_bytes + per_entry_overhead * num_entries + total_block_overhead;

        // Index Block calculation.
        // 1. Bloom filter
        let bloom_filter_header = SIZEOF_U16;
        let bloom_filter_bytes_for_filter = if self.min_filter_keys as usize <= num_entries {
            BloomFilterBuilder::filter_size_bytes(num_entries, DEFAULT_BITS_PER_KEY as usize)
        } else {
            0
        };
        let bloom_filter_total_size = bloom_filter_header + bloom_filter_bytes_for_filter;
        // 2. Per-block meta.
        let guess_at_average_first_key_size_bytes = 12;
        let per_block_metadata_size = number_of_blocks
            * (guess_at_average_first_key_size_bytes + SIZEOF_U16/* offsets in block */);
        // 3. SST Info section (see SsTableInfoArgs).
        let sst_info_size = guess_at_average_first_key_size_bytes + 4 * SIZEOF_U64 + 1;
        let sst_checksum = SIZEOF_U64;

        let index_block_size = bloom_filter_total_size
            + per_block_metadata_size
            + sst_info_size
            + sst_checksum
            + block_checksum;

        total_data_blocks_bytes + index_block_size
    }
}

impl SsTableInfoOwned {
    fn encode(info: &SsTableInfoOwned, buf: &mut Vec<u8>) {
        buf.extend_from_slice(info.data());
        buf.put_u32(crc32fast::hash(info.data()));
    }

    pub(crate) fn decode(raw_info: Bytes) -> Result<SsTableInfoOwned, SlateDBError> {
        if raw_info.len() <= 4 {
            return Err(SlateDBError::EmptyBlockMeta);
        }
        let data = raw_info.slice(..raw_info.len() - 4).clone();
        let checksum = raw_info.slice(raw_info.len() - 4..).get_u32();
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
    compression_codec: Option<CompressionCodec>,
}

impl<'a> EncodedSsTableBuilder<'a> {
    /// Create a builder based on target block size.
    fn new(
        block_size: usize,
        min_filter_keys: u32,
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
            filter_builder: BloomFilterBuilder::new(DEFAULT_BITS_PER_KEY),
            index_builder: flatbuffers::FlatBufferBuilder::new(),
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
        // write the index block
        // 1. maybe serialize the bloom filter
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

        // 2. write the index data, containing metadata per data block.
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

        // 3. write the SST info.
        let mut sst_info_builder = flatbuffers::FlatBufferBuilder::new();
        let first_key = self
            .sst_first_key
            .map(|k| sst_info_builder.create_vector(k.as_ref()));
        let meta_offset = self.current_len + buf.len();
        let info_wip_offset = SsTableInfo::create(
            &mut sst_info_builder,
            &SsTableInfoArgs {
                first_key,
                index_offset: index_offset as u64,
                index_len: index_len as u64,
                filter_offset: filter_offset as u64,
                filter_len: filter_len as u64,
                compression_format: self.compression_codec.into(),
            },
        );

        sst_info_builder.finish(info_wip_offset, None);
        let info = SsTableInfoOwned::new(Bytes::from(sst_info_builder.finished_data().to_vec()))?;

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
    use std::sync::Arc;

    use bytes::BytesMut;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::*;
    use crate::block::BLOCK_SIZE_BYTES;
    use crate::block_iterator::BlockIterator;
    use crate::db_state::SsTableId;
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
        let format = SsTableFormat::new(32, 0, None);
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
        let format = SsTableFormat::new(32, 0, None);
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
        let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
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
        let sst_info = sst_handle.info.borrow();
        assert_eq!(
            b"key1",
            sst_info.first_key().unwrap().bytes(),
            "first key in sst info should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let index = table_store
            .read_index(&sst_handle_from_store)
            .await
            .unwrap();
        let sst_info_from_store = sst_handle_from_store.info.borrow();
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_info_from_store.first_key().unwrap().bytes(),
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
        let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 3, None);
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
        let handle = sst_handle.info.borrow();
        assert_eq!(handle.filter_len(), 0);
    }

    #[tokio::test]
    async fn test_sstable_with_compression() {
        #[allow(unused)]
        async fn test_compression_inner(compression: CompressionCodec) {
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, Some(compression));
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

            assert!(filter.has_key(b"key1"));
            assert!(filter.has_key(b"key2"));
            assert_eq!(encoded_info, sst_handle.info);
            let sst_info = sst_handle.info.borrow();
            assert_eq!(1, index.borrow().block_meta().len());
            assert_eq!(
                b"key1",
                sst_info.first_key().unwrap().bytes(),
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
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, Some(compression));
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
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, Some(dummy_codec));
            let table_store = TableStore::new(object_store, format, root_path, None);
            let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
            let index = table_store.read_index(&sst_handle).await.unwrap();
            let filter = table_store.read_filter(&sst_handle).await.unwrap().unwrap();

            assert!(filter.has_key(b"key1"));
            assert!(filter.has_key(b"key2"));
            assert_eq!(encoded_info, sst_handle.info);
            let sst_info = sst_handle.info.borrow();
            assert_eq!(1, index.borrow().block_meta().len());
            assert_eq!(
                b"key1",
                sst_info.first_key().unwrap().bytes(),
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
        let format = SsTableFormat::new(32, 1, None);
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
        let format = SsTableFormat::new(32, 1, None);
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

    #[cfg(test)]
    mod estimate_sst_size_tests {
        use super::*;
        use crate::iter::KeyValueIterator;
        use crate::mem_table::WritableKVTable;
        use crate::tablestore::TableStore;
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use std::sync::Arc;

        fn create_table_store() -> Arc<TableStore> {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
            Arc::new(TableStore::new(object_store, format, Path::from(""), None))
        }

        async fn serialize_memtable_to_sst(memtable: &WritableKVTable) -> usize {
            let table_store = create_table_store();
            let mut builder = table_store.table_builder();

            let iter = &mut memtable.table().iter();
            while let Ok(Some(next_val)) = iter.next().await {
                builder.add(&next_val.key, Some(&next_val.value)).unwrap();
            }

            let encoded = &mut builder.build().unwrap();
            let mut total_size = 0;
            while let Some(block) = encoded.unconsumed_blocks.pop_front() {
                total_size += block.len();
            }
            total_size
        }

        #[tokio::test]
        async fn test_empty_sst() {
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
            let memtable = WritableKVTable::new();
            let actual_size = serialize_memtable_to_sst(&memtable).await;
            let estimated_size = format.estimate_sst_size(0, 0);

            assert_eq!(actual_size, estimated_size);
        }

        #[tokio::test]
        async fn test_single_small_entry() {
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
            let mut memtable = WritableKVTable::new();
            memtable.put(b"0123456789ab", b"value");

            let actual_size = serialize_memtable_to_sst(&memtable).await;
            let estimated_size = format.estimate_sst_size(1, 8);

            assert!(
                (actual_size as i64 - estimated_size as i64).abs() <= 50,
                "Actual size: {}, Estimated size: {}",
                actual_size,
                estimated_size
            );
        }

        #[tokio::test]
        async fn test_multiple_entries() {
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
            let mut memtable = WritableKVTable::new();
            let mut total_size = 0;

            for i in 0..100 {
                let key = format!("key{:03}", i);
                let value = format!("value{:03}", i);
                memtable.put(key.as_bytes(), value.as_bytes());
                total_size += key.len() + value.len();
            }

            let actual_size = serialize_memtable_to_sst(&memtable).await;
            let estimated_size = format.estimate_sst_size(100, total_size);

            assert!(
                (actual_size as i64 - estimated_size as i64).abs() <= 500,
                "Actual size: {}, Estimated size: {}",
                actual_size,
                estimated_size
            );
        }

        #[tokio::test]
        async fn test_large_entries() {
            let format = SsTableFormat::new(BLOCK_SIZE_BYTES, 0, None);
            let mut memtable = WritableKVTable::new();
            let mut total_size = 0;

            for i in 0..10000 {
                let key = format!("key{:05}", i);
                let value = "a".repeat(95);
                memtable.put(key.as_bytes(), value.as_bytes());
                total_size += key.len() + value.len();
            }

            let actual_size = serialize_memtable_to_sst(&memtable).await;
            let estimated_size = format.estimate_sst_size(10000, total_size);

            assert!(
                (actual_size as i64 - estimated_size as i64).abs() <= 50000,
                "Actual size: {}, Estimated size: {}",
                actual_size,
                estimated_size
            );
        }
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
