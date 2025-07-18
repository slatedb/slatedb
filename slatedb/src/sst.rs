use std::collections::VecDeque;
#[cfg(feature = "zlib")]
use std::io::{Read, Write};
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
use crate::row_codec;
use crate::types::RowEntry;
use crate::utils::compute_index_key;
use crate::{blob::ReadOnlyBlob, config::CompressionCodec};
use crate::{block::BlockBuilder, error::SlateDBError};

pub(crate) const SST_FORMAT_VERSION: u16 = 1;

// 8 bytes for the metadata offset + 2 bytes for the version
const NUM_FOOTER_BYTES: usize = 10;
const NUM_FOOTER_BYTES_LONG: u64 = NUM_FOOTER_BYTES as u64;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const SIZEOF_U64: usize = std::mem::size_of::<u64>();
pub(crate) const CHECKSUM_SIZE: usize = SIZEOF_U32;
pub(crate) const OFFSET_SIZE: usize = SIZEOF_U16;
pub(crate) const METADATA_OFFSET_SIZE: usize = SIZEOF_U64;
pub(crate) const VERSION_SIZE: usize = SIZEOF_U16;

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
        let obj_len = obj.len().await?;
        if obj_len <= NUM_FOOTER_BYTES_LONG {
            return Err(SlateDBError::EmptySSTable);
        }
        // Get the size of the metadata
        let header = obj
            .read_range((obj_len - NUM_FOOTER_BYTES_LONG)..obj_len)
            .await?;
        assert!(header.len() == NUM_FOOTER_BYTES);

        // Last 2 bytes of the header represent the version
        let version = header.slice(8..NUM_FOOTER_BYTES).get_u16();
        // TODO: Support older and newer versions
        if version != SST_FORMAT_VERSION {
            return Err(SlateDBError::InvalidVersion {
                expected_version: SST_FORMAT_VERSION,
                actual_version: version,
            });
        }

        // First 8 bytes of the header represent the metadata offset
        let sst_metadata_offset = header.slice(0..8).get_u64();
        let sst_metadata_bytes = obj
            .read_range(sst_metadata_offset..obj_len - NUM_FOOTER_BYTES_LONG)
            .await?;
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
            let filter_offset_range = info.filter_offset..filter_end;
            let filter_bytes = obj.read_range(filter_offset_range).await?;
            let compression_codec = info.compression_codec;
            filter = Some(Arc::new(
                self.decode_filter(filter_bytes, compression_codec)?,
            ));
        }
        Ok(filter)
    }

    #[allow(dead_code)]
    pub(crate) fn read_filter_raw(
        &self,
        info: &SsTableInfo,
        sst_bytes: &Bytes,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        if info.filter_len == 0 {
            return Ok(None);
        }
        let filter_end = info.filter_offset + info.filter_len;
        let filter_offset_range = info.filter_offset as usize..filter_end as usize;
        let filter_bytes = sst_bytes.slice(filter_offset_range);
        let compression_codec = info.compression_codec;
        Ok(Some(Arc::new(
            self.decode_filter(filter_bytes, compression_codec)?,
        )))
    }

    pub(crate) fn decode_filter(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<BloomFilter, SlateDBError> {
        let filter_bytes = self.validate_checksum(bytes)?;
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(filter_bytes, c)?,
            None => filter_bytes,
        };
        Ok(BloomFilter::decode(&decompressed_bytes))
    }

    pub(crate) async fn read_index(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_off = info.index_offset;
        let index_end = index_off + info.index_len;
        let index_bytes = obj.read_range(index_off..index_end).await?;
        let compression_codec = info.compression_codec;
        self.decode_index(index_bytes, compression_codec)
    }

    #[cfg(test)]
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
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_bytes = self.validate_checksum(bytes)?;
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(index_bytes, c)?,
            None => index_bytes,
        };
        Ok(SsTableIndexOwned::new(decompressed_bytes)?)
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
    ) -> Range<u64> {
        let mut end_offset = info.filter_offset;
        if blocks.end < index.block_meta().len() {
            let next_block_meta = index.block_meta().get(blocks.end);
            end_offset = next_block_meta.offset();
        }
        let start_offset = index.block_meta().get(blocks.start).offset();
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
        let start_range = range.start;
        let bytes: Bytes = obj.read_range(range).await?;
        let mut decoded_blocks = VecDeque::new();
        let compression_codec = info.compression_codec;
        for block in blocks {
            let block_meta = index.block_meta().get(block);
            let block_bytes_start = usize::try_from(block_meta.offset() - start_range).expect(
                "attempted to read byte data with size \
                larger than 32 bits on a 32-bit system",
            );
            let block_bytes = if block == index.block_meta().len() - 1 {
                bytes.slice(block_bytes_start..)
            } else {
                let next_block_meta = index.block_meta().get(block + 1);
                let block_bytes_end = usize::try_from(next_block_meta.offset() - start_range)
                    .expect(
                        "attempted to read byte data with size \
                        larger than 32 bits on a 32-bit system",
                    );
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
        let block_bytes = self.validate_checksum(bytes)?;
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(block_bytes, c)?,
            None => block_bytes,
        };
        Ok(Block::decode(decompressed_bytes))
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

    #[cfg(test)]
    pub(crate) fn read_block_raw(
        &self,
        info: &SsTableInfo,
        index_owned: &SsTableIndexOwned,
        block: usize,
        sst_bytes: &Bytes,
    ) -> Result<Block, SlateDBError> {
        let index = index_owned.borrow();
        let range = self.block_range(block..block + 1, info, &index);
        let range = range.start as usize..range.end as usize;
        let bytes: Bytes = sst_bytes.slice(range);
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

    /// validate checksum and return the actual data bytes
    fn validate_checksum(&self, bytes: Bytes) -> Result<Bytes, SlateDBError> {
        let checksum_sz = std::mem::size_of::<u32>();
        let data_bytes = bytes.slice(..bytes.len() - checksum_sz);
        let mut checksum_bytes = bytes.slice(bytes.len() - checksum_sz..);
        let checksum = crc32fast::hash(&data_bytes);
        let stored_checksum = checksum_bytes.get_u32();
        if checksum != stored_checksum {
            return Err(SlateDBError::ChecksumMismatch);
        }
        Ok(data_bytes)
    }

    /// The function estimates the size of the SST (Sorted String Table) without considering compression effects.
    pub(crate) fn estimate_encoded_size(
        &self,
        entry_num: usize,
        estimated_entries_size: usize,
    ) -> usize {
        if entry_num == 0 {
            return 0;
        }

        let entries_size_encoded =
            row_codec::SstRowCodecV0::estimate_encoded_size(entry_num, estimated_entries_size);

        // estimate sum of Block
        let number_of_blocks = usize::div_ceil(entries_size_encoded, self.block_size);
        let mut ans =
            Block::estimate_encoded_size(entry_num, entries_size_encoded, number_of_blocks);

        // estimate sum of BloomFilter
        if entry_num >= self.min_filter_keys as usize {
            ans += BloomFilter::estimate_encoded_size(entry_num as u32, self.filter_bits_per_key);
        }

        // estimate sum of Index
        let guess_at_average_first_key_size_bytes = 12;
        ans += (number_of_blocks * guess_at_average_first_key_size_bytes + OFFSET_SIZE)
            + CHECKSUM_SIZE;

        // estimate sum of Metadata
        ans += guess_at_average_first_key_size_bytes + 4 * SIZEOF_U64 + SIZEOF_U16;

        ans += METADATA_OFFSET_SIZE + VERSION_SIZE;

        ans
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

pub(crate) struct EncodedSsTableBlock {
    pub(crate) offset: u64,
    pub(crate) block: Block,
    pub(crate) encoded_bytes: Bytes,
}

pub(crate) struct EncodedSsTable {
    pub(crate) info: SsTableInfo,
    pub(crate) index: SsTableIndexOwned,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    pub(crate) unconsumed_blocks: VecDeque<EncodedSsTableBlock>,
    pub(crate) footer: Bytes,
}

impl EncodedSsTable {
    pub(crate) fn put_remaining<T: BufMut>(&self, buf: &mut T) {
        for chunk in self.unconsumed_blocks.iter() {
            buf.put_slice(chunk.encoded_bytes.as_ref())
        }
        buf.put_slice(self.footer.as_ref());
    }

    pub(crate) fn remaining_as_bytes(&self) -> Bytes {
        let total_size = self
            .unconsumed_blocks
            .iter()
            .map(|chunk| chunk.encoded_bytes.len())
            .sum::<usize>()
            + self.footer.len();
        let mut data = Vec::<u8>::with_capacity(total_size);
        self.put_remaining(&mut data);
        Bytes::from(data)
    }
}

/// Builds an SSTable from key-value pairs.
pub(crate) struct EncodedSsTableBuilder<'a> {
    builder: BlockBuilder,
    index_builder: flatbuffers::FlatBufferBuilder<'a, DefaultAllocator>,
    first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    sst_first_key: Option<Bytes>,
    current_block_max_key: Option<Bytes>,
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'a>>>,
    current_len: u64,
    blocks: VecDeque<EncodedSsTableBlock>,
    block_size: usize,
    min_filter_keys: u32,
    num_keys: u32,
    filter_builder: BloomFilterBuilder,
    sst_codec: Box<dyn SsTableInfoCodec>,
    compression_codec: Option<CompressionCodec>,
}

impl EncodedSsTableBuilder<'_> {
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
            current_block_max_key: None,
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

    pub fn add(&mut self, entry: RowEntry) -> Result<(), SlateDBError> {
        self.num_keys += 1;
        let key = entry.key.clone();

        let index_key = compute_index_key(self.current_block_max_key.take(), &key);
        self.current_block_max_key = Some(key.clone());

        if !self.builder.add(entry.clone()) {
            // Create a new block builder and append block data
            self.finish_block()?;

            // New block must always accept the first KV pair
            assert!(self.builder.add(entry));

            self.first_key = Some(self.index_builder.create_vector(&index_key));
        } else if self.sst_first_key.is_none() {
            self.sst_first_key = Some(Bytes::copy_from_slice(&key));

            self.first_key = Some(self.index_builder.create_vector(&index_key));
        }

        self.filter_builder.add_key(&key);

        Ok(())
    }

    #[cfg(test)]
    pub fn add_value(
        &mut self,
        key: &[u8],
        val: &[u8],
        attrs: crate::types::RowAttributes,
    ) -> Result<(), SlateDBError> {
        let entry = RowEntry::new(
            key.to_vec().into(),
            crate::types::ValueDeletable::Value(Bytes::copy_from_slice(val)),
            0,
            attrs.ts,
            attrs.expire_ts,
        );
        self.add(entry)
    }

    pub fn next_block(&mut self) -> Option<EncodedSsTableBlock> {
        self.blocks.pop_front()
    }

    #[cfg(test)]
    pub(crate) fn num_blocks(&self) -> usize {
        // use block_meta since blocks can be consumed as sst is being built
        self.block_meta.len()
    }

    fn finish_block(&mut self) -> Result<(), SlateDBError> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let new_builder = BlockBuilder::new(self.block_size);
        let builder = std::mem::replace(&mut self.builder, new_builder);
        let block = self.prepare_block(builder, self.current_len)?;
        let block_meta = BlockMeta::create(
            &mut self.index_builder,
            &BlockMetaArgs {
                offset: block.offset,
                first_key: self.first_key,
            },
        );
        self.block_meta.push(block_meta);
        self.current_len += block.encoded_bytes.len() as u64;
        self.blocks.push_back(block);
        self.first_key = None;
        Ok(())
    }

    fn prepare_block(
        &mut self,
        builder: BlockBuilder,
        offset: u64,
    ) -> Result<EncodedSsTableBlock, SlateDBError> {
        let block = builder.build()?;
        let encoded_block = block.encode();
        let compressed_block = match self.compression_codec {
            Some(c) => Self::compress(encoded_block, c)?,
            None => encoded_block,
        };
        let checksum = crc32fast::hash(&compressed_block);
        let total_block_size = compressed_block.len() + std::mem::size_of::<u32>();
        let mut encoded_bytes = Vec::with_capacity(total_block_size);
        encoded_bytes.put(compressed_block);
        encoded_bytes.put_u32(checksum);
        let sst_block = EncodedSsTableBlock {
            offset,
            block,
            encoded_bytes: Bytes::from(encoded_bytes),
        };
        Ok(sst_block)
    }

    /// Builds the SST from the current state.
    ///
    /// # Format
    ///
    /// +---------------------------------------------------+
    /// |                Data Blocks                        |
    /// |    (raw bytes produced by finish_block)           |
    /// +---------------------------------------------------+
    /// |                Filter Block*                      |
    /// |  +---------------------------------------------+  |
    /// |  | Filter Data (compressed encoded filter)     |  |
    /// |  +---------------------------------------------+  |
    /// |  | 4-byte Checksum (CRC32 of filter data)      |  |
    /// |  +---------------------------------------------+  |
    /// +---------------------------------------------------+
    /// |                Index Block                        |
    /// |  +---------------------------------------------+  |
    /// |  | Index Data (compressed index block)         |  |
    /// |  +---------------------------------------------+  |
    /// |  | 4-byte Checksum (CRC32 of index data)       |  |
    /// |  +---------------------------------------------+  |
    /// +---------------------------------------------------+
    /// |                Metadata Block                     |
    /// |    (SsTableInfo encoded with FlatBuffers)         |
    /// +---------------------------------------------------+
    /// |             8-byte Metadata Offset                |
    /// +---------------------------------------------------+
    /// |                 2-byte Version                    |
    /// +---------------------------------------------------+
    /// * Only present if num_keys >= min_filter_keys.
    ///
    pub fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        self.finish_block()?;
        let mut buf = Vec::new();
        let mut maybe_filter = None;
        let mut filter_len = 0;
        let filter_offset = self.current_len + buf.len() as u64;
        if self.num_keys >= self.min_filter_keys {
            let filter = Arc::new(self.filter_builder.build());
            let encoded_filter = filter.encode();
            let compressed_filter = match self.compression_codec {
                None => encoded_filter,
                Some(c) => Self::compress(encoded_filter, c)?,
            };
            let checksum = crc32fast::hash(&compressed_filter);
            filter_len = compressed_filter.len() + std::mem::size_of::<u32>();
            buf.put(compressed_filter);
            buf.put_u32(checksum);
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
        let index = SsTableIndexOwned::new(index_block.clone())?;
        let index_block = match self.compression_codec {
            None => index_block,
            Some(c) => Self::compress(index_block, c)?,
        };
        let checksum = crc32fast::hash(&index_block);
        let index_offset = self.current_len + buf.len() as u64;
        let index_len = index_block.len() + std::mem::size_of::<u32>();
        buf.put(index_block);
        buf.put_u32(checksum);

        let meta_offset = self.current_len + buf.len() as u64;
        let info = SsTableInfo {
            first_key: self.sst_first_key,
            index_offset,
            index_len: index_len as u64,
            filter_offset,
            filter_len: filter_len as u64,
            compression_codec: self.compression_codec,
        };
        SsTableInfo::encode(&info, &mut buf, &*self.sst_codec);

        // write the metadata offset at the end of the file. FlatBuffer internal
        // representation is not intended to be used directly.
        buf.put_u64(meta_offset);
        // write the version at the end of the file.
        buf.put_u16(SST_FORMAT_VERSION);
        Ok(EncodedSsTable {
            info,
            index,
            filter: maybe_filter,
            unconsumed_blocks: self.blocks,
            footer: Bytes::from(buf),
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use std::sync::Arc;
    use std::vec;

    use bytes::BytesMut;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::*;
    use crate::block_iterator::BlockIterator;
    use crate::bytes_range::BytesRange;
    use crate::db_cache::DbCacheWrapper;
    use crate::db_state::SsTableId;
    use crate::filter::filter_hash;
    use crate::iter::IterationOrder::Ascending;
    use crate::object_stores::ObjectStores;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, build_test_sst, gen_attrs, gen_empty_attrs};

    #[test]
    fn test_estimate_encoded_size() {
        let mut format = SsTableFormat::default();

        // Test with zero entries
        assert_eq!(format.estimate_encoded_size(0, 0), 0);

        // Test with one entry
        let encoded_entry_size = 100;
        let size = format.estimate_encoded_size(1, encoded_entry_size);
        assert!(size > 0);

        // Test with multiple entries with not trigger bloom filter
        format.min_filter_keys = 1000;
        let num_entries = 100;
        let total_size = encoded_entry_size * num_entries;
        let size = format.estimate_encoded_size(num_entries, total_size);
        assert!(size > total_size); // Should be larger due to overhead

        // Test with entries that should trigger bloom filter
        let num_entries = format.min_filter_keys as usize * 10;
        let total_size = encoded_entry_size * num_entries;
        let size_with_filter = format.estimate_encoded_size(num_entries, total_size);
        format.min_filter_keys = format.min_filter_keys * 10 + 1;
        let size_without_filter =
            format.estimate_encoded_size(num_entries, encoded_entry_size * num_entries);
        assert!(size_with_filter > size_without_filter); // Should be larger due to bloom filter
    }

    fn next_block_to_iter(builder: &mut EncodedSsTableBuilder) -> BlockIterator<Block> {
        let block = builder.next_block();
        assert!(block.is_some());
        let block = block.unwrap().block;
        BlockIterator::new(block, Ascending)
    }

    #[tokio::test]
    async fn test_builder_should_make_blocks_available() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 8], &[b'1'; 8], gen_attrs(1))
            .unwrap();
        builder
            .add_value(&[b'b'; 8], &[b'2'; 8], gen_attrs(2))
            .unwrap();
        builder
            .add_value(&[b'c'; 8], &[b'3'; 8], gen_attrs(3))
            .unwrap();

        // when:
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'a'; 8], &[b'1'; 8], 0).with_create_ts(1)],
        )
        .await;
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'b'; 8], &[b'2'; 8], 0).with_create_ts(2)],
        )
        .await;
        assert!(builder.next_block().is_none());
        builder
            .add_value(&[b'd'; 8], &[b'4'; 8], gen_attrs(1))
            .unwrap();
        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'c'; 8], &[b'3'; 8], 0).with_create_ts(3)],
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
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format.clone(),
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 8], &[b'1'; 8], gen_attrs(1))
            .unwrap();
        builder
            .add_value(&[b'b'; 8], &[b'2'; 8], gen_attrs(2))
            .unwrap();
        builder
            .add_value(&[b'c'; 8], &[b'3'; 8], gen_attrs(3))
            .unwrap();
        let first_block = builder.next_block();

        let encoded = builder.build().unwrap();

        let mut raw_sst = Vec::<u8>::new();
        raw_sst.put_slice(first_block.unwrap().encoded_bytes.as_ref());
        assert_eq!(encoded.unconsumed_blocks.len(), 2);
        encoded.put_remaining(&mut raw_sst);
        let raw_sst = Bytes::copy_from_slice(raw_sst.as_slice());
        let index = format.read_index_raw(&encoded.info, &raw_sst).unwrap();
        let block = format
            .read_block_raw(&encoded.info, &index, 0, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'a'; 8], &[b'1'; 8], 0).with_create_ts(1)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 1, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'b'; 8], &[b'2'; 8], 0).with_create_ts(2)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 2, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'c'; 8], &[b'3'; 8], 0).with_create_ts(3)],
        )
        .await;
    }

    #[test]
    fn test_builder_should_return_blocks_with_correct_data_and_offsets() {
        let format = SsTableFormat::default();

        let sst = build_test_sst(&format, 3);

        let bytes = sst.remaining_as_bytes();
        let index = format.read_index_raw(&sst.info, &bytes).unwrap();
        let block_metas = index.borrow().block_meta();
        assert_eq!(block_metas.len(), sst.unconsumed_blocks.len());
        for i in 0..block_metas.len() {
            let encoded_block = sst.unconsumed_blocks.get(i).unwrap();
            assert_eq!(block_metas.get(i).offset(), encoded_block.offset);
            let read_block = format.read_block_raw(&sst.info, &index, i, &bytes).unwrap();
            assert!(encoded_block.block == read_block);
        }
    }

    #[rstest]
    #[case::default_sst(SsTableFormat::default(), 0, true)]
    #[case::sst_with_no_filter(SsTableFormat { min_filter_keys: 9, ..SsTableFormat::default() }, 0, false)]
    #[case::sst_builds_filter_with_correct_bits_per_key(SsTableFormat { filter_bits_per_key: 10, ..SsTableFormat::default() }, 0, true)]
    #[case::sst_builds_filter_with_correct_bits_per_key(SsTableFormat { filter_bits_per_key: 20, ..SsTableFormat::default() }, 0, true)]
    #[tokio::test]
    async fn test_sstable(
        #[case] format: SsTableFormat,
        #[case] wal_id: u64,
        #[case] should_have_filter: bool,
    ) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format.clone(),
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        for k in 1..=8 {
            builder
                .add_value(
                    format!("key{}", k).as_bytes(),
                    format!("value{}", k).as_bytes(),
                    gen_attrs(k),
                )
                .unwrap();
        }
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();

        if let Some(filter) = encoded.filter.clone() {
            let bytes = filter.encode();
            // filters are encoded as a 16-bit number of probes followed by the filter
            assert_eq!(bytes.len() as u32, 2 + format.filter_bits_per_key);
        }

        // write sst and validate that the handle returned has the correct content.
        let sst_handle = table_store
            .write_sst(&SsTableId::Wal(wal_id), encoded, false)
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
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(wal_id)).await.unwrap();
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
            b"",
            index.borrow().block_meta().get(0).first_key().bytes(),
            "index key in block meta should be correct after reading from store"
        );

        // Validate filter presence
        if should_have_filter {
            assert!(sst_info.filter_len > 0);
        } else {
            assert_eq!(sst_info.filter_len, 0);
        }
    }

    #[rstest]
    #[case::none(None)]
    #[cfg_attr(feature = "snappy", case::snappy(Some(CompressionCodec::Snappy)))]
    #[cfg_attr(feature = "zlib", case::zlib(Some(CompressionCodec::Zlib)))]
    #[cfg_attr(feature = "lz4", case::lz4(Some(CompressionCodec::Lz4)))]
    #[cfg_attr(feature = "zstd", case::zstd(Some(CompressionCodec::Zstd)))]
    #[tokio::test]
    async fn test_sstable_with_compression(#[case] compression: Option<CompressionCodec>) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let format = SsTableFormat {
            compression_codec: compression,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
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

    #[rstest]
    #[case::none(None, None)]
    #[cfg_attr(
        all(feature = "snappy", feature = "zlib"),
        case::snappy_zlib(Some(CompressionCodec::Snappy), Some(CompressionCodec::Zlib))
    )]
    #[cfg_attr(
        all(feature = "zlib", feature = "lz4"),
        case::zlib_lz4(Some(CompressionCodec::Zlib), Some(CompressionCodec::Lz4))
    )]
    #[cfg_attr(
        all(feature = "lz4", feature = "zstd"),
        case::lz4_zstd(Some(CompressionCodec::Lz4), Some(CompressionCodec::Zstd))
    )]
    #[cfg_attr(
        all(feature = "zstd", feature = "snappy"),
        case::zstd_snappy(Some(CompressionCodec::Zstd), Some(CompressionCodec::Snappy))
    )]
    #[tokio::test]
    async fn test_sstable_with_compression_using_sst_info(
        #[case] compression: Option<CompressionCodec>,
        #[case] dummy_codec: Option<CompressionCodec>,
    ) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            compression_codec: compression,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            format,
            root_path.clone(),
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();

        // decompression is independent of TableFormat. It uses the CompressionFormat from SSTable Info to decompress sst.
        let format = SsTableFormat {
            compression_codec: dummy_codec,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
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

    #[rstest]
    #[case::partial_blocks(0..2, vec![
        vec![
            RowEntry::new_value(&[b'a'; 2], &[1u8; 2], 0),
            RowEntry::new_value(&[b'b'; 2], &[2u8; 2], 0),
        ],
        vec![
            RowEntry::new_value(&[b'c'; 20], &[3u8; 20], 0).with_create_ts(3),
        ],
    ])]
    #[case::all_blocks(0..3, vec![
        vec![
            RowEntry::new_value(&[b'a'; 2], &[1u8; 2], 0),
            RowEntry::new_value(&[b'b'; 2], &[2u8; 2], 0),
        ],
        vec![
            RowEntry::new_value(&[b'c'; 20], &[3u8; 20], 0).with_create_ts(3),
        ],
        vec![
            RowEntry::new_value(&[b'd'; 20], &[4u8; 20], 0).with_create_ts(4),
        ],
    ])]
    #[tokio::test]
    async fn test_read_blocks(
        #[case] block_range: Range<usize>,
        #[case] expected_blocks: Vec<Vec<RowEntry>>,
    ) {
        // given:
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 48,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format.clone(),
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 2], &[1u8; 2], gen_empty_attrs())
            .unwrap();
        builder
            .add_value(&[b'b'; 2], &[2u8; 2], gen_empty_attrs())
            .unwrap();
        builder
            .add_value(&[b'c'; 20], &[3u8; 20], gen_attrs(3))
            .unwrap();
        builder
            .add_value(&[b'd'; 20], &[4u8; 20], gen_attrs(4))
            .unwrap();
        let encoded = builder.build().unwrap();
        let info = encoded.info.clone();
        let bytes = encoded.remaining_as_bytes();
        let index = format.read_index_raw(&encoded.info, &bytes).unwrap();
        let blob = BytesBlob { bytes };

        // when:
        let mut blocks = format
            .read_blocks(&info, &index, block_range, &blob)
            .await
            .unwrap();

        // then:
        for expected_entries in expected_blocks {
            let mut iter = BlockIterator::new(blocks.pop_front().unwrap(), Ascending);
            assert_iterator(&mut iter, expected_entries).await;
        }
        assert!(blocks.is_empty())
    }

    #[tokio::test]
    async fn test_sstable_index_size() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };

        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();

        // write sst and validate that the handle returned has the correct content.
        let sst_handle = table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
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

        assert_eq!(88, index.size());
    }

    #[tokio::test]
    async fn test_checksum_mismatch() {
        let format = SsTableFormat::default();
        // create corrupted data by modifying bytes but keeping same checksum
        let mut corrupted_bytes = BytesMut::new();
        let bytes = &b"something"[..];
        corrupted_bytes.put(bytes);
        corrupted_bytes.put_u32(crc32fast::hash(bytes)); // original checksum
        corrupted_bytes[0] ^= 1; // corrupt one byte

        assert!(matches!(
            format.validate_checksum(corrupted_bytes.into()),
            Err(SlateDBError::ChecksumMismatch)
        ));
    }

    #[tokio::test]
    async fn test_version_checking() {
        // Create a valid SST
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };

        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format.clone(),
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        );
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let bytes = encoded.remaining_as_bytes();

        // Test valid version decodes properly through read_info
        let valid_blob = BytesBlob {
            bytes: bytes.clone(),
        };
        let result = format.read_info(&valid_blob).await;
        match result {
            Ok(_) => {}
            Err(e) => {
                panic!("Expected Ok result, but got error: {:?}", e);
            }
        }

        let mut invalid_bytes = BytesMut::from(bytes.clone());
        // Corrupt the version
        invalid_bytes[bytes.len() - 1] ^= 1;
        let invalid_blob = BytesBlob {
            bytes: invalid_bytes.freeze(),
        };
        assert!(matches!(
            format.read_info(&invalid_blob).await,
            Err(SlateDBError::InvalidVersion { .. })
        ));
    }

    #[rstest]
    #[case::owned(true)]
    #[case::borrowed(false)]
    #[tokio::test]
    async fn test_sst_handle_with_visible_ranges(
        #[case] is_owned: bool,
    ) -> Result<(), SlateDBError> {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 1024,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
        ));
        let mut builder = table_store.table_builder();
        for key in 'a'..='z' {
            let key_bytes = [key as u8];
            builder.add_value(&key_bytes, b"value", gen_empty_attrs())?;
        }
        let encoded = builder.build()?;

        let sst_id = SsTableId::Wal(0);
        let sst_handle = table_store
            .write_sst(&sst_id, encoded, false)
            .await?
            .with_visible_range(BytesRange::from_ref("c"..="f"));

        let expected_entries = vec![
            RowEntry::new_value(b"c", b"value", 0),
            RowEntry::new_value(b"d", b"value", 0),
            RowEntry::new_value(b"e", b"value", 0),
            RowEntry::new_value(b"f", b"value", 0),
        ];

        if is_owned {
            // scan the entire sst and validate that the visible range is respected.
            let mut iter = SstIterator::new_owned(
                ..,
                sst_handle.clone(),
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await?
            .expect("Expected Some(iter) but got None");

            assert_iterator(&mut iter, expected_entries).await;

            // scan range outside of visible range and validate that it returns empty iterator.
            let iter = SstIterator::new_owned(
                Bytes::from_static(b"m")..Bytes::from_static(b"p"),
                sst_handle,
                table_store,
                SstIteratorOptions::default(),
            )
            .await?;

            assert!(iter.is_none());
        } else {
            // scan the entire sst and validate that the visible range is respected.
            let mut iter = SstIterator::new_borrowed(
                ..,
                &sst_handle,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await?
            .expect("Expected Some(iter) but got None");

            assert_iterator(&mut iter, expected_entries).await;

            // scan range outside of visible range and validate that it returns empty iterator.
            let iter = SstIterator::new_borrowed(
                Bytes::from_static(b"m")..Bytes::from_static(b"p"),
                &sst_handle,
                table_store,
                SstIteratorOptions::default(),
            )
            .await?;

            assert!(iter.is_none());
        }

        Ok(())
    }

    struct BytesBlob {
        bytes: Bytes,
    }

    impl ReadOnlyBlob for BytesBlob {
        async fn len(&self) -> Result<u64, SlateDBError> {
            Ok(self.bytes.len() as u64)
        }

        async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError> {
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }

        async fn read(&self) -> Result<Bytes, SlateDBError> {
            Ok(self.bytes.clone())
        }
    }
}
