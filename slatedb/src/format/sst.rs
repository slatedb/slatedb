use crate::blob::ReadOnlyBlob;
use crate::config::CompressionCodec;
use crate::db_state::{SsTableInfo, SsTableInfoCodec, SstType};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::{
    BlockMeta, FlatBufferSsTableInfoCodec, SsTableIndex, SsTableIndexArgs, SsTableIndexOwned,
};
use crate::format::block::{Block, BlockBuilderV1};
use crate::format::block_v2::BlockBuilderV2;
use crate::format::row;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use flatbuffers::DefaultAllocator;
use std::collections::VecDeque;
#[cfg(feature = "zlib")]
use std::io::Read;
#[cfg(feature = "zlib")]
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

// 8 bytes for the metadata offset + 2 bytes for the version
const NUM_FOOTER_BYTES: usize = 10;
const NUM_FOOTER_BYTES_LONG: u64 = NUM_FOOTER_BYTES as u64;
const SEQNUM_SIZE: usize = size_of::<u64>();
pub(crate) const SST_FORMAT_VERSION: u16 = 1;
pub(crate) const SST_FORMAT_VERSION_V2: u16 = 2;
pub(crate) const SST_FORMAT_VERSION_LATEST: u16 = SST_FORMAT_VERSION_V2;

fn is_supported_version(version: u16) -> bool {
    matches!(version, SST_FORMAT_VERSION | SST_FORMAT_VERSION_V2)
}

#[allow(private_interfaces)]
pub(crate) enum BlockBuilder {
    V1(BlockBuilderV1),
    V2(BlockBuilderV2),
}

impl BlockBuilder {
    pub(crate) fn new_v1(block_size: usize) -> Self {
        Self::V1(BlockBuilderV1::new(block_size))
    }

    pub(crate) fn new_v2(block_size: usize) -> Self {
        Self::V2(BlockBuilderV2::new(block_size))
    }

    pub(crate) fn new_latest(block_size: usize) -> Self {
        Self::new_v2(block_size)
    }

    #[cfg(test)]
    pub(crate) fn new_v2_with_restart_interval(block_size: usize, restart_interval: usize) -> Self {
        Self::V2(BlockBuilderV2::new_with_restart_interval(
            block_size,
            restart_interval,
        ))
    }

    pub(crate) fn would_fit(&self, entry: &crate::types::RowEntry) -> bool {
        match self {
            Self::V1(builder) => builder.would_fit(entry),
            Self::V2(builder) => builder.would_fit(entry),
        }
    }

    pub(crate) fn add(
        &mut self,
        entry: crate::types::RowEntry,
    ) -> Result<bool, crate::error::SlateDBError> {
        match self {
            Self::V1(builder) => builder.add(entry),
            Self::V2(builder) => builder.add(entry),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::V1(builder) => builder.is_empty(),
            Self::V2(builder) => builder.is_empty(),
        }
    }

    pub(crate) fn build(self) -> Result<Block, SlateDBError> {
        match self {
            Self::V1(builder) => builder.build(),
            Self::V2(builder) => builder.build(),
        }
    }

    #[cfg(test)]
    pub(crate) fn add_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        attrs: crate::types::RowAttributes,
    ) -> bool {
        match self {
            Self::V1(builder) => builder.add_value(key, value, attrs),
            Self::V2(builder) => builder.add_value(key, value, attrs),
        }
    }
}
pub(crate) const SIZEOF_U16: usize = size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = size_of::<u32>();
pub(crate) const SIZEOF_U64: usize = size_of::<u64>();
pub(crate) const CHECKSUM_SIZE: usize = SIZEOF_U32;
pub(crate) const OFFSET_SIZE: usize = SIZEOF_U16;
pub(crate) const METADATA_OFFSET_SIZE: usize = SIZEOF_U64;
pub(crate) const VERSION_SIZE: usize = SIZEOF_U16;

/// Trait for transforming block data before storage and after retrieval.
///
/// This can be used to implement encryption, custom encoding, or other
/// transformations on block data. The transformer is applied after compression
/// on write and before decompression on read.
///
/// The trait is async to allow implementations to use `spawn_blocking` for
/// CPU-intensive operations (like encryption) or to call external services
/// (like a KMS).
///
/// # Example
///
/// ```
/// use async_trait::async_trait;
/// use bytes::Bytes;
/// use slatedb::{BlockTransformer, Error};
///
/// struct XorTransformer {
///     key: u8,
/// }
///
/// #[async_trait]
/// impl BlockTransformer for XorTransformer {
///     async fn encode(&self, data: Bytes) -> Result<Bytes, Error> {
///         let transformed: Vec<u8> = data.iter().map(|b| b ^ self.key).collect();
///         Ok(Bytes::from(transformed))
///     }
///
///     async fn decode(&self, data: Bytes) -> Result<Bytes, Error> {
///         self.encode(data).await
///     }
/// }
/// ```
#[async_trait]
pub trait BlockTransformer: Send + Sync {
    /// Encode (transform) block data before storage.
    async fn encode(&self, data: Bytes) -> Result<Bytes, crate::error::Error>;

    /// Decode (inverse transform) block data after retrieval.
    async fn decode(&self, data: Bytes) -> Result<Bytes, crate::error::Error>;
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

/// Encoded SST data blocks
pub(crate) struct EncodedSsTableBlock {
    /// offset of the block within the SST
    pub(crate) offset: u64,
    /// uncompressed and untransformed block
    pub(crate) block: Block,
    /// compressed and transformed block
    pub(crate) encoded_bytes: Bytes,
}

impl EncodedSsTableBlock {
    pub(crate) fn len(&self) -> usize {
        self.encoded_bytes.len()
    }
}

/// Builder for encoding a single SST block with compression and transformation.
pub(crate) struct EncodedSsTableBlockBuilder {
    /// builder for data blocks
    block_builder: BlockBuilder,
    /// offset of the block within the SST
    offset: u64,
    /// codec for compressing the data block
    compression_codec: Option<CompressionCodec>,
    /// transformer for transforming the data block (e.g. encryption)
    block_transformer: Option<Arc<dyn BlockTransformer>>,
}

impl EncodedSsTableBlockBuilder {
    pub(crate) fn new(block_builder: BlockBuilder, offset: u64) -> Self {
        Self {
            block_builder,
            offset,
            compression_codec: None,
            block_transformer: None,
        }
    }

    /// Sets the compression codec for compressing the data block
    pub(crate) fn with_compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.compression_codec = Some(codec);
        self
    }

    /// Sets the block transformer for transforming the data block
    pub(crate) fn with_block_transformer(mut self, transformer: Arc<dyn BlockTransformer>) -> Self {
        self.block_transformer = Some(transformer);
        self
    }

    pub(crate) async fn build(self) -> Result<EncodedSsTableBlock, SlateDBError> {
        let block = self.block_builder.build()?;
        let encoded_block = block.encode();
        let mut compressed_and_transformed_block = Vec::new();
        compress_and_transform(
            &mut compressed_and_transformed_block,
            encoded_block,
            self.compression_codec,
            self.block_transformer.as_ref(),
        )
        .await?;
        Ok(EncodedSsTableBlock {
            offset: self.offset,
            block,
            encoded_bytes: Bytes::from(compressed_and_transformed_block),
        })
    }
}

/// The encoded footer of an SSTable, containing filter, index, info, and metadata.
pub(crate) struct EncodedSsTableFooter {
    pub(crate) info: SsTableInfo,
    pub(crate) index: SsTableIndexOwned,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    pub(crate) encoded_bytes: Bytes,
}

/// Builder for encoding the SSTable footer (filter, index, info, and metadata).
pub(crate) struct EncodedSsTableFooterBuilder<'a, 'b> {
    /// size of all data blocks in the SST
    blocks_size: u64,
    /// first entry in the SST, key for compacted data, sequence number for WAL
    first_entry: Option<Bytes>,
    /// codec for compressing data blocks, index blocks, and filter blocks
    compression_codec: Option<CompressionCodec>,
    /// transformer for transforming data blocks, index blocks, and filter blocks
    block_transformer: Option<Arc<dyn BlockTransformer>>,
    /// codec for the SST info
    sst_info_codec: &'a dyn SsTableInfoCodec,
    /// builder for the index block
    index_builder: flatbuffers::FlatBufferBuilder<'b, flatbuffers::DefaultAllocator>,
    /// metadata block
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'b>>>,
    /// filter block
    filter: Option<(Arc<BloomFilter>, Bytes)>,
    /// SST format version
    sst_format_version: u16,
    /// type of SST (Compacted or Wal)
    sst_type: SstType,
}

impl<'a, 'b> EncodedSsTableFooterBuilder<'a, 'b> {
    pub(crate) fn new(
        blocks_len: u64,
        sst_first_entry: Option<Bytes>,
        sst_codec: &'a dyn SsTableInfoCodec,
        index_builder: flatbuffers::FlatBufferBuilder<'b, DefaultAllocator>,
        block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'b>>>,
        sst_format_version: u16,
        sst_type: SstType,
    ) -> Self {
        Self {
            blocks_size: blocks_len,
            first_entry: sst_first_entry,
            compression_codec: None,
            block_transformer: None,
            sst_info_codec: sst_codec,
            index_builder,
            block_meta,
            filter: None,
            sst_format_version,
            sst_type,
        }
    }

    /// Sets an optional compression codec to the footer.
    pub(crate) fn with_compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.compression_codec = Some(codec);
        self
    }

    /// Sets an optional block transformer to the footer.
    pub(crate) fn with_block_transformer(mut self, transformer: Arc<dyn BlockTransformer>) -> Self {
        self.block_transformer = Some(transformer);
        self
    }

    /// Adds a bloom filter to the footer.
    pub(crate) fn with_filter(mut self, filter: Arc<BloomFilter>, encoded_filter: Bytes) -> Self {
        self.filter = Some((filter, encoded_filter));
        self
    }

    /// Builds the footer with the index and optional filter.
    pub(crate) async fn build(mut self) -> Result<EncodedSsTableFooter, SlateDBError> {
        let mut buf = Vec::new();

        // Write filter block if present
        let filter_offset = self.blocks_size + buf.len() as u64;
        let (filter_len, maybe_filter) = match self.filter.take() {
            Some((filter, encoded_filter)) => {
                let len = compress_and_transform(
                    &mut buf,
                    encoded_filter,
                    self.compression_codec,
                    self.block_transformer.as_ref(),
                )
                .await?;
                (len as u64, Some(filter))
            }
            None => (0u64, None),
        };

        let vector = self.index_builder.create_vector(&self.block_meta);
        let index_wip = SsTableIndex::create(
            &mut self.index_builder,
            &SsTableIndexArgs {
                block_meta: Some(vector),
            },
        );
        self.index_builder.finish(index_wip, None);
        let index_data = Bytes::from(self.index_builder.finished_data().to_vec());
        let index = SsTableIndexOwned::new(index_data.clone())?;
        let index_offset = self.blocks_size + buf.len() as u64;
        let index_len = compress_and_transform(
            &mut buf,
            index_data,
            self.compression_codec,
            self.block_transformer.as_ref(),
        )
        .await? as u64;

        let meta_offset = self.blocks_size + buf.len() as u64;
        let info = SsTableInfo {
            first_entry: self.first_entry,
            index_offset,
            index_len,
            filter_offset,
            filter_len,
            compression_codec: self.compression_codec,
            sst_type: self.sst_type,
        };
        SsTableInfo::encode(&info, &mut buf, self.sst_info_codec);

        buf.put_u64(meta_offset);
        buf.put_u16(self.sst_format_version);

        Ok(EncodedSsTableFooter {
            info,
            index,
            filter: maybe_filter,
            encoded_bytes: Bytes::from(buf),
        })
    }
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

/// Compresses and transforms the data using the specified
/// compression codec and the block transformer.
pub(crate) async fn compress_and_transform(
    buf: &mut Vec<u8>,
    data: Bytes,
    compression_codec: Option<CompressionCodec>,
    block_transformer: Option<&Arc<dyn BlockTransformer>>,
) -> Result<usize, SlateDBError> {
    let compressed = match compression_codec {
        None => data,
        Some(c) => compress(data, c)?,
    };
    let transformed = transform(compressed, block_transformer).await?;
    let checksum = crc32fast::hash(&transformed);
    let len = transformed.len() + CHECKSUM_SIZE;
    buf.put(transformed);
    buf.put_u32(checksum);
    Ok(len)
}

/// Compresses the data using the specified compression codec.
pub(crate) fn compress(
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
            let compressed =
                zstd::bulk::compress(&data, 3).map_err(|_| SlateDBError::BlockCompressionError)?;
            Ok(Bytes::from(compressed))
        }
    }
}

/// Transforms the data using the specified block transformer.
pub(crate) async fn transform(
    data: Bytes,
    block_transformer: Option<&Arc<dyn BlockTransformer>>,
) -> Result<Bytes, SlateDBError> {
    let transformed = match block_transformer {
        Some(t) => t
            .encode(data)
            .await
            .map_err(|_| SlateDBError::BlockTransformError)?,
        None => data,
    };
    Ok(transformed)
}

pub(crate) type OffsetAndVersion = (u64, u16);

#[derive(Clone)]
pub(crate) struct SsTableFormat {
    pub(crate) block_size: usize,
    pub(crate) min_filter_keys: u32,
    pub(crate) sst_codec: Box<dyn SsTableInfoCodec>,
    pub(crate) filter_bits_per_key: u32,
    pub(crate) compression_codec: Option<CompressionCodec>,
    pub(crate) block_transformer: Option<Arc<dyn BlockTransformer>>,
    pub(crate) block_format: Option<crate::sst_builder::BlockFormat>,
}

impl Default for SsTableFormat {
    fn default() -> Self {
        Self {
            block_size: 4096,
            min_filter_keys: 0,
            sst_codec: Box::new(FlatBufferSsTableInfoCodec {}),
            filter_bits_per_key: 10,
            compression_codec: None,
            block_transformer: None,
            block_format: None,
        }
    }
}

impl SsTableFormat {
    async fn read_metadata_offset_and_version(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<OffsetAndVersion, SlateDBError> {
        let obj_len = obj.len().await?;
        if obj_len <= NUM_FOOTER_BYTES_LONG {
            return Err(SlateDBError::EmptySSTable);
        }
        let header = obj
            .read_range((obj_len - NUM_FOOTER_BYTES_LONG)..obj_len)
            .await?;
        assert_eq!(header.len(), NUM_FOOTER_BYTES);

        let version = header.slice(8..NUM_FOOTER_BYTES).get_u16();
        let sst_metadata_offset = header.slice(0..8).get_u64();
        Ok((sst_metadata_offset, version))
    }

    fn validate_version(&self, version: u16) -> Result<(), SlateDBError> {
        if !is_supported_version(version) {
            return Err(SlateDBError::InvalidVersion {
                format_name: "SST",
                supported_versions: vec![SST_FORMAT_VERSION, SST_FORMAT_VERSION_V2],
                actual_version: version,
            });
        }
        Ok(())
    }

    pub(crate) async fn read_version(&self, obj: &impl ReadOnlyBlob) -> Result<u16, SlateDBError> {
        let (_, version) = self.read_metadata_offset_and_version(obj).await?;
        self.validate_version(version)?;
        Ok(version)
    }

    pub(crate) async fn read_info(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableInfo, SlateDBError> {
        let (sst_metadata_offset, version) = self.read_metadata_offset_and_version(obj).await?;
        self.validate_version(version)?;
        let obj_len = obj.len().await?;
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
                self.decode_filter(filter_bytes, compression_codec).await?,
            ));
        }
        Ok(filter)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_filter_raw(
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
            self.decode_filter(filter_bytes, compression_codec).await?,
        )))
    }

    pub(crate) async fn decode_filter(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<BloomFilter, SlateDBError> {
        let filter_bytes = self.validate_checksum(bytes)?;

        let untransformed_bytes = match &self.block_transformer {
            Some(t) => t
                .decode(filter_bytes)
                .await
                .map_err(|_| SlateDBError::BlockTransformError)?,
            None => filter_bytes,
        };
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(untransformed_bytes, c)?,
            None => untransformed_bytes,
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
        self.decode_index(index_bytes, compression_codec).await
    }

    #[cfg(test)]
    pub(crate) async fn read_index_raw(
        &self,
        info: &SsTableInfo,
        sst_bytes: &Bytes,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_off = info.index_offset as usize;
        let index_end = index_off + info.index_len as usize;
        let index_bytes: Bytes = sst_bytes.slice(index_off..index_end);
        let compression_codec = info.compression_codec;
        self.decode_index(index_bytes, compression_codec).await
    }

    async fn decode_index(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let index_bytes = self.validate_checksum(bytes)?;

        let untransformed_bytes = match &self.block_transformer {
            Some(t) => t
                .decode(index_bytes)
                .await
                .map_err(|_| SlateDBError::BlockTransformError)?,
            None => index_bytes,
        };
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(untransformed_bytes, c)?,
            None => untransformed_bytes,
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
            decoded_blocks.push_back(self.decode_block(block_bytes, compression_codec).await?);
        }
        Ok(decoded_blocks)
    }

    async fn decode_block(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<Block, SlateDBError> {
        let block_bytes = self.validate_checksum(bytes)?;
        let untransformed_bytes = match &self.block_transformer {
            Some(t) => t
                .decode(block_bytes)
                .await
                .map_err(|_| SlateDBError::BlockTransformError)?,
            None => block_bytes,
        };
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(untransformed_bytes, c)?,
            None => untransformed_bytes,
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
    pub(crate) async fn read_block_raw(
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
        self.decode_block(bytes, compression_codec).await
    }

    /// validate checksum and return the actual data bytes
    pub(crate) fn validate_checksum(&self, bytes: Bytes) -> Result<Bytes, SlateDBError> {
        let data_bytes = bytes.slice(..bytes.len() - CHECKSUM_SIZE);
        let mut checksum_bytes = bytes.slice(bytes.len() - CHECKSUM_SIZE..);
        let checksum = crc32fast::hash(&data_bytes);
        let stored_checksum = checksum_bytes.get_u32();
        if checksum != stored_checksum {
            return Err(SlateDBError::ChecksumMismatch);
        }
        Ok(data_bytes)
    }

    /// The function estimates the size of the SST (Sorted String Table) without considering compression effects.
    pub(crate) fn estimate_encoded_size_compacted(
        &self,
        entry_num: usize,
        estimated_entries_size: usize,
    ) -> usize {
        if entry_num == 0 {
            return 0;
        }
        let guess_at_average_first_key_size_bytes = 12usize;
        let mut ans = self.estimate_encoded_size_data_index_metadata(
            entry_num,
            estimated_entries_size,
            guess_at_average_first_key_size_bytes,
        );
        ans += self.estimate_encoded_size_filter(entry_num);
        ans
    }

    pub(crate) fn estimate_encoded_size_wal(
        &self,
        entry_num: usize,
        estimated_entries_size: usize,
    ) -> usize {
        if entry_num == 0 {
            return 0;
        }
        self.estimate_encoded_size_data_index_metadata(
            entry_num,
            estimated_entries_size,
            SEQNUM_SIZE,
        )
    }

    fn estimate_encoded_size_data_index_metadata(
        &self,
        entry_num: usize,
        estimated_entries_size: usize,
        average_first_key_size: usize,
    ) -> usize {
        let entries_size_encoded =
            row::SstRowCodecV0::estimate_encoded_size(entry_num, estimated_entries_size);

        // estimate sum of Block
        let number_of_blocks = usize::div_ceil(entries_size_encoded, self.block_size);
        let mut ans =
            Block::estimate_encoded_size(entry_num, entries_size_encoded, number_of_blocks);

        // estimate sum of Index
        let guess_at_average_first_key_size_bytes = average_first_key_size;
        ans += (number_of_blocks * guess_at_average_first_key_size_bytes + OFFSET_SIZE)
            + CHECKSUM_SIZE;

        // estimate sum of Metadata
        ans += guess_at_average_first_key_size_bytes + 4 * SIZEOF_U64 + SIZEOF_U16;

        ans += METADATA_OFFSET_SIZE + VERSION_SIZE;

        ans
    }

    fn estimate_encoded_size_filter(&self, entry_num: usize) -> usize {
        if entry_num >= self.min_filter_keys as usize {
            BloomFilter::estimate_encoded_size(entry_num as u32, self.filter_bits_per_key)
        } else {
            0usize
        }
    }
}
