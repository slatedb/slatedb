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
use crate::types::RowEntry;
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
        let index_off = info.index_offset as usize;
        let index_end = index_off + info.index_len as usize;
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
        let block_bytes = self.validate_checksum(bytes)?;
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

    #[cfg(test)]
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

        if !self.builder.add(entry.clone()) {
            // Create a new block builder and append block data
            if let Some(block) = self.finish_block()? {
                self.current_len += block.len();
                self.blocks.push_back(Bytes::from(block));
            }

            // New block must always accept the first KV pair
            assert!(self.builder.add(entry));
            self.first_key = Some(self.index_builder.create_vector(&key));
        } else if self.sst_first_key.is_none() {
            self.sst_first_key = Some(Bytes::copy_from_slice(&key));
            self.first_key = Some(self.index_builder.create_vector(&key));
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

    pub fn next_block(&mut self) -> Option<Bytes> {
        self.blocks.pop_front()
    }

    fn finish_block(&mut self) -> Result<Option<Vec<u8>>, SlateDBError> {
        if self.builder.is_empty() {
            return Ok(None);
        }

        let new_builder = BlockBuilder::new(self.block_size);
        let builder = std::mem::replace(&mut self.builder, new_builder);
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
        let index_block = match self.compression_codec {
            None => index_block,
            Some(c) => Self::compress(index_block, c)?,
        };
        let checksum = crc32fast::hash(&index_block);
        let index_offset = self.current_len + buf.len();
        let index_len = index_block.len() + std::mem::size_of::<u32>();
        buf.put(index_block);
        buf.put_u32(checksum);

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
    use rstest::rstest;

    use std::sync::Arc;
    use std::vec;

    use bytes::BytesMut;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::*;
    use crate::block_iterator::BlockIterator;
    use crate::db_state::SsTableId;
    use crate::filter::filter_hash;
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, gen_attrs, gen_empty_attrs};

    fn next_block_to_iter(builder: &mut EncodedSsTableBuilder) -> BlockIterator<Block> {
        let block = builder.next_block();
        assert!(block.is_some());
        let block = block.unwrap();
        let block = Block::decode(block.slice(..block.len() - 4));
        BlockIterator::new(block)
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
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
        let mut iter = BlockIterator::new(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'a'; 8], &[b'1'; 8], 0).with_create_ts(1)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 1, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::new(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'b'; 8], &[b'2'; 8], 0).with_create_ts(2)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 2, &raw_sst)
            .unwrap();
        let mut iter = BlockIterator::new(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'c'; 8], &[b'3'; 8], 0).with_create_ts(3)],
        )
        .await;
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
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
            .write_sst(&SsTableId::Wal(wal_id), encoded)
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
            b"key1",
            index.borrow().block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct after reading from store"
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
        let table_store = TableStore::new(object_store, format, root_path, None);
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
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
        let table_store = TableStore::new(object_store.clone(), format, root_path.clone(), None);
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();

        // decompression is independent of TableFormat. It uses the CompressionFormat from SSTable Info to decompress sst.
        let format = SsTableFormat {
            compression_codec: dummy_codec,
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
        let table_store = TableStore::new(object_store, format.clone(), root_path, None);
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
            .read_blocks(&info, &index, block_range, &blob)
            .await
            .unwrap();

        // then:
        for expected_entries in expected_blocks {
            let mut iter = BlockIterator::new(blocks.pop_front().unwrap());
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

        let table_store = TableStore::new(object_store, format, root_path, None);
        let mut builder = table_store.table_builder();
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
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
