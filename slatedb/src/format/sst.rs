use crate::blob::ReadOnlyBlob;
use crate::config::CompressionCodec;
use crate::db_state::{SsTableInfo, SsTableInfoCodec, SstIndexType, SstType};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::{
    BlockMeta, BlockMetaArgs, FlatBufferSsTableInfoCodec, PartitionIndex, PartitionIndexArgs,
    PartitionIndexOwned, PartitionMeta, PartitionMetaArgs, SsTableIndex, SsTableIndexArgs,
    SsTableIndexOwned, SsTableIndexV2, SsTableIndexV2Args, SsTableIndexV2Owned,
};
use crate::format::block::{Block, BlockBuilderV1};
use crate::format::block_v2::BlockBuilderV2;
use crate::format::row;
use crate::sst_stats::{BlockStats, SstStats};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use std::collections::VecDeque;
#[cfg(feature = "zlib")]
use std::io::Read;
#[cfg(feature = "zlib")]
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

// 8 bytes for the metadata offset + 2 bytes for the version
const NUM_FOOTER_BYTES: usize = 10;
/// Target size in bytes for each partition index block. When the estimated
/// serialized size of block-meta entries exceeds this threshold, a new
/// partition is started.
const PARTITION_INDEX_TARGET_SIZE: usize = 4096;
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
        ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> bool {
        match self {
            Self::V1(builder) => builder.add_value(key, value, ts, expire_ts),
            Self::V2(builder) => builder.add_value(key, value, ts, expire_ts),
        }
    }
}

pub(crate) struct BlockBuilderWithStats {
    builder: BlockBuilder,
    stats: BlockStats,
}

impl BlockBuilderWithStats {
    pub(crate) fn new(builder: BlockBuilder) -> Self {
        Self {
            builder,
            stats: BlockStats::default(),
        }
    }

    pub(crate) fn would_fit(&self, entry: &crate::types::RowEntry) -> bool {
        self.builder.would_fit(entry)
    }

    pub(crate) fn add(
        &mut self,
        entry: crate::types::RowEntry,
    ) -> Result<bool, crate::error::SlateDBError> {
        match &entry.value {
            crate::types::ValueDeletable::Value(_) => self.stats.num_puts += 1,
            crate::types::ValueDeletable::Merge(_) => self.stats.num_merges += 1,
            crate::types::ValueDeletable::Tombstone => self.stats.num_deletes += 1,
        }
        self.builder.add(entry)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    pub(crate) fn into_parts(self) -> (BlockBuilder, BlockStats) {
        (self.builder, self.stats)
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

/// The index data produced when building an SSTable footer, used to populate
/// the in-memory cache after the SST is written to the object store.
pub(crate) enum SstIndexData {
    /// Flat index (used for WAL SSTs).
    Flat(SsTableIndexOwned),
    /// Partitioned index (used for compacted SSTs): the top-level V2 directory
    /// plus the individually-decoded partition blocks at their on-disk offsets.
    Partitioned {
        v2: Arc<SsTableIndexV2Owned>,
        /// (partition_offset_in_sst, decoded_partition)
        partitions: Vec<(u64, Arc<PartitionIndexOwned>)>,
    },
}

impl SstIndexData {
    /// Reconstruct a flat `SsTableIndexOwned` from this index data.
    /// For `Flat`, returns the index directly.
    /// For `Partitioned`, stitches all partition blocks into a flat index.
    #[cfg(test)]
    pub(crate) fn into_flat_index(self) -> Result<SsTableIndexOwned, SlateDBError> {
        match self {
            SstIndexData::Flat(index) => Ok(index),
            SstIndexData::Partitioned { partitions, .. } => {
                let mut raw: Vec<(u64, Bytes)> = Vec::new();
                for (_, partition) in &partitions {
                    let p = partition.borrow();
                    for j in 0..p.blocks().len() {
                        let bm = p.blocks().get(j);
                        raw.push((bm.offset(), Bytes::copy_from_slice(bm.first_key().bytes())));
                    }
                }
                let flat = EncodedSsTableFooterBuilder::encode_flat_index(&raw);
                Ok(SsTableIndexOwned::new(flat)?)
            }
        }
    }
}

/// The encoded footer of an SSTable, containing filter, index, stats, info, and metadata.
pub(crate) struct EncodedSsTableFooter {
    pub(crate) info: SsTableInfo,
    pub(crate) index: SstIndexData,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    #[allow(dead_code)]
    pub(crate) stats: Option<SstStats>,
    pub(crate) encoded_bytes: Bytes,
}

/// Builder for encoding the SSTable footer (filter, index, info, and metadata).
pub(crate) struct EncodedSsTableFooterBuilder<'a> {
    /// size of all data blocks in the SST
    blocks_size: u64,
    /// first entry in the SST, key for compacted data, sequence number for WAL
    first_entry: Option<Bytes>,
    /// last entry (key) in the SST for compacted data, None for WAL SSTs
    last_entry: Option<Bytes>,
    /// codec for compressing data blocks, index blocks, and filter blocks
    compression_codec: Option<CompressionCodec>,
    /// transformer for transforming data blocks, index blocks, and filter blocks
    block_transformer: Option<Arc<dyn BlockTransformer>>,
    /// codec for the SST info
    sst_info_codec: &'a dyn SsTableInfoCodec,
    /// raw block metadata: (block_offset, first_key_bytes)
    raw_block_meta: Vec<(u64, Bytes)>,
    /// filter block
    filter: Option<(Arc<BloomFilter>, Bytes)>,
    /// stats block
    stats: Option<SstStats>,
    /// SST format version
    sst_format_version: u16,
    /// type of SST (Compacted or Wal)
    sst_type: SstType,
}

impl<'a> EncodedSsTableFooterBuilder<'a> {
    pub(crate) fn new(
        blocks_len: u64,
        sst_first_entry: Option<Bytes>,
        sst_last_entry: Option<Bytes>,
        sst_codec: &'a dyn SsTableInfoCodec,
        raw_block_meta: Vec<(u64, Bytes)>,
        sst_format_version: u16,
        sst_type: SstType,
    ) -> Self {
        Self {
            blocks_size: blocks_len,
            first_entry: sst_first_entry,
            last_entry: sst_last_entry,
            compression_codec: None,
            block_transformer: None,
            sst_info_codec: sst_codec,
            raw_block_meta,
            filter: None,
            stats: None,
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

    /// Adds stats to the footer.
    pub(crate) fn with_stats(mut self, stats: SstStats) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Adds a bloom filter to the footer.
    pub(crate) fn with_filter(mut self, filter: Arc<BloomFilter>, encoded_filter: Bytes) -> Self {
        self.filter = Some((filter, encoded_filter));
        self
    }

    /// Builds the footer with the index, optional filter and optional stats.
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

        // Build index: partitioned for compacted SSTs, flat for WAL SSTs.
        let (index_offset, index_len, index, index_type) = match self.sst_type {
            SstType::Compacted => self.build_partitioned_index(&mut buf).await?,
            SstType::Wal => self.build_flat_index(&mut buf).await?,
        };

        // Write stats block if present
        let maybe_stats = self.stats.take();
        let (stats_offset, stats_len) = match &maybe_stats {
            Some(stats) => {
                let offset = self.blocks_size + buf.len() as u64;
                let len = compress_and_transform(
                    &mut buf,
                    stats.encode(),
                    self.compression_codec,
                    self.block_transformer.as_ref(),
                )
                .await? as u64;
                (offset, len)
            }
            None => (0u64, 0u64),
        };

        let meta_offset = self.blocks_size + buf.len() as u64;
        let info = SsTableInfo {
            first_entry: self.first_entry,
            last_entry: self.last_entry,
            index_offset,
            index_len,
            filter_offset,
            filter_len,
            compression_codec: self.compression_codec,
            sst_type: self.sst_type,
            stats_offset,
            stats_len,
            index_type,
        };
        SsTableInfo::encode(&info, &mut buf, self.sst_info_codec);

        buf.put_u64(meta_offset);
        buf.put_u16(self.sst_format_version);

        Ok(EncodedSsTableFooter {
            info,
            index,
            filter: maybe_filter,
            stats: maybe_stats,
            encoded_bytes: Bytes::from(buf),
        })
    }

    /// Builds a flat `SsTableIndex` from `raw_block_meta`, writes it to `buf`,
    /// and returns `(index_offset, index_len, SstIndexData::Flat, SstIndexType::Flat)`.
    async fn build_flat_index(
        &self,
        buf: &mut Vec<u8>,
    ) -> Result<(u64, u64, SstIndexData, SstIndexType), SlateDBError> {
        let index_data = Self::encode_flat_index(&self.raw_block_meta);
        let index = SsTableIndexOwned::new(index_data.clone())?;
        let index_offset = self.blocks_size + buf.len() as u64;
        let index_len = compress_and_transform(
            buf,
            index_data,
            self.compression_codec,
            self.block_transformer.as_ref(),
        )
        .await? as u64;
        Ok((
            index_offset,
            index_len,
            SstIndexData::Flat(index),
            SstIndexType::Flat,
        ))
    }

    /// Builds a partitioned index from `raw_block_meta`:
    /// - Writes each `PartitionIndex` block to `buf`
    /// - Writes the top-level `SsTableIndexV2` directory to `buf`
    /// - Returns `(index_offset, index_len, SstIndexData::Partitioned, SstIndexType::Partitioned)`
    ///
    /// The returned `SstIndexData::Partitioned` holds the decoded V2 directory and each
    /// decoded partition block so the caller can cache them individually without stitching.
    async fn build_partitioned_index(
        &self,
        buf: &mut Vec<u8>,
    ) -> Result<(u64, u64, SstIndexData, SstIndexType), SlateDBError> {
        if self.raw_block_meta.is_empty() {
            // Empty SST: fall back to a flat empty index
            return self.build_flat_index(buf).await;
        }

        // Group raw_block_meta into partitions of ~PARTITION_INDEX_TARGET_SIZE bytes.
        let partition_slices = Self::partition_block_meta(&self.raw_block_meta);

        // Write each PartitionIndex block and record its location.
        let mut partition_metas: Vec<(u64, u64, Bytes)> = Vec::new();
        let mut cached_partitions: Vec<(u64, Arc<PartitionIndexOwned>)> = Vec::new();
        for partition_slice in &partition_slices {
            let first_key = partition_slice[0].1.clone();
            let partition_data = Self::encode_partition_index(partition_slice);
            let partition_offset = self.blocks_size + buf.len() as u64;
            // Stash the decoded partition before compression (for caching).
            let partition_owned = Arc::new(PartitionIndexOwned::new(partition_data.clone())?);
            let partition_len = compress_and_transform(
                buf,
                partition_data,
                self.compression_codec,
                self.block_transformer.as_ref(),
            )
            .await? as u64;
            partition_metas.push((partition_offset, partition_len, first_key));
            cached_partitions.push((partition_offset, partition_owned));
        }

        // Build and write the top-level SsTableIndexV2 directory.
        let v2_data = Self::encode_sst_index_v2(&partition_metas);
        let index_offset = self.blocks_size + buf.len() as u64;
        let v2_owned = Arc::new(SsTableIndexV2Owned::new(v2_data.clone())?);
        let index_len = compress_and_transform(
            buf,
            v2_data,
            self.compression_codec,
            self.block_transformer.as_ref(),
        )
        .await? as u64;

        Ok((
            index_offset,
            index_len,
            SstIndexData::Partitioned {
                v2: v2_owned,
                partitions: cached_partitions,
            },
            SstIndexType::Partitioned,
        ))
    }

    /// Groups `raw_block_meta` into slices, each with an estimated serialized
    /// size ≤ `PARTITION_INDEX_TARGET_SIZE`.
    fn partition_block_meta(raw: &[(u64, Bytes)]) -> Vec<&[(u64, Bytes)]> {
        // Conservative per-entry overhead: 8 (offset) + 4 (vec header) + 4 (alignment) + key
        const OVERHEAD_PER_ENTRY: usize = 16;

        let mut partitions: Vec<&[(u64, Bytes)]> = Vec::new();
        let mut start = 0;
        let mut current_size = 0;

        for (i, (_, key)) in raw.iter().enumerate() {
            current_size += OVERHEAD_PER_ENTRY + key.len();
            let is_last = i + 1 == raw.len();
            if current_size >= PARTITION_INDEX_TARGET_SIZE || is_last {
                partitions.push(&raw[start..=i]);
                start = i + 1;
                current_size = 0;
            }
        }
        partitions
    }

    /// Encodes a slice of `(offset, first_key)` pairs as a flat `SsTableIndex` FlatBuffer.
    fn encode_flat_index(raw: &[(u64, Bytes)]) -> Bytes {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let entries: Vec<_> = raw
            .iter()
            .map(|(offset, key)| {
                let key_vec = builder.create_vector(key);
                BlockMeta::create(
                    &mut builder,
                    &BlockMetaArgs {
                        offset: *offset,
                        first_key: Some(key_vec),
                    },
                )
            })
            .collect();
        let vec = builder.create_vector(&entries);
        let index = SsTableIndex::create(
            &mut builder,
            &SsTableIndexArgs {
                block_meta: Some(vec),
            },
        );
        builder.finish(index, None);
        Bytes::from(builder.finished_data().to_vec())
    }

    /// Encodes a partition's `(offset, first_key)` pairs as a `PartitionIndex` FlatBuffer.
    fn encode_partition_index(raw: &[(u64, Bytes)]) -> Bytes {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let entries: Vec<_> = raw
            .iter()
            .map(|(offset, key)| {
                let key_vec = builder.create_vector(key);
                BlockMeta::create(
                    &mut builder,
                    &BlockMetaArgs {
                        offset: *offset,
                        first_key: Some(key_vec),
                    },
                )
            })
            .collect();
        let vec = builder.create_vector(&entries);
        let partition =
            PartitionIndex::create(&mut builder, &PartitionIndexArgs { blocks: Some(vec) });
        builder.finish(partition, None);
        Bytes::from(builder.finished_data().to_vec())
    }

    /// Encodes a list of `(partition_offset, partition_len, first_key)` as a
    /// top-level `SsTableIndexV2` FlatBuffer.
    fn encode_sst_index_v2(partition_metas: &[(u64, u64, Bytes)]) -> Bytes {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let entries: Vec<_> = partition_metas
            .iter()
            .map(|(offset, length, first_key)| {
                let key_vec = builder.create_vector(first_key);
                PartitionMeta::create(
                    &mut builder,
                    &PartitionMetaArgs {
                        offset: *offset,
                        length: *length,
                        first_key: Some(key_vec),
                    },
                )
            })
            .collect();
        let vec = builder.create_vector(&entries);
        let index_v2 = SsTableIndexV2::create(
            &mut builder,
            &SsTableIndexV2Args {
                partitions: Some(vec),
            },
        );
        builder.finish(index_v2, None);
        Bytes::from(builder.finished_data().to_vec())
    }
}

pub(crate) struct EncodedSsTable {
    pub(crate) format_version: u16,
    pub(crate) info: SsTableInfo,
    pub(crate) index: SstIndexData,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    #[allow(dead_code)]
    pub(crate) stats: Option<SstStats>,
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

pub(crate) type LengthOffsetAndVersion = (u64, u64, u16);

pub(crate) type TableInfoAndVersion = (SsTableInfo, u16);

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
    async fn read_length_and_metadata_offset_and_version(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<LengthOffsetAndVersion, SlateDBError> {
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
        Ok((obj_len, sst_metadata_offset, version))
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

    pub(crate) async fn read_info_and_version(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<TableInfoAndVersion, SlateDBError> {
        let (obj_len, sst_metadata_offset, version) = self
            .read_length_and_metadata_offset_and_version(obj)
            .await?;
        self.validate_version(version)?;
        let sst_metadata_bytes = obj
            .read_range(sst_metadata_offset..obj_len - NUM_FOOTER_BYTES_LONG)
            .await?;
        SsTableInfo::decode(sst_metadata_bytes, &*self.sst_codec).map(|info| (info, version))
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
        match info.index_type {
            SstIndexType::Flat => {
                let index_off = info.index_offset;
                let index_end = index_off + info.index_len;
                let index_bytes = obj.read_range(index_off..index_end).await?;
                self.decode_index(index_bytes, info.compression_codec).await
            }
            SstIndexType::Partitioned => self.read_partitioned_index(info, obj).await,
        }
    }

    /// Reads and decodes the top-level `SsTableIndexV2` directory from the object store.
    pub(crate) async fn read_v2_directory(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableIndexV2Owned, SlateDBError> {
        let index_off = info.index_offset;
        let index_end = index_off + info.index_len;
        let v2_bytes = obj.read_range(index_off..index_end).await?;
        let v2_bytes = self
            .validate_and_decompress(v2_bytes, info.compression_codec)
            .await?;
        Ok(SsTableIndexV2Owned::new(v2_bytes)?)
    }

    /// Reads all partition blocks from the object store in a single range read,
    /// returning each as a decoded `(partition_offset, PartitionIndexOwned)` pair.
    pub(crate) async fn read_partitions_from_store(
        &self,
        info: &SsTableInfo,
        v2: &SsTableIndexV2Owned,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Vec<(u64, PartitionIndexOwned)>, SlateDBError> {
        let v2_borrow = v2.borrow();
        let Some(partitions) = v2_borrow.partitions() else {
            return Ok(vec![]);
        };
        if partitions.is_empty() {
            return Ok(vec![]);
        }

        // Single contiguous read covering all partition blocks.
        let first = partitions.get(0);
        let last = partitions.get(partitions.len() - 1);
        let read_start = first.offset();
        let read_end = last.offset() + last.length();
        let all_bytes = obj.read_range(read_start..read_end).await?;

        let mut result = Vec::with_capacity(partitions.len());
        for i in 0..partitions.len() {
            let pm = partitions.get(i);
            let slice_start = (pm.offset() - read_start) as usize;
            let slice_end = slice_start + pm.length() as usize;
            let partition_bytes = all_bytes.slice(slice_start..slice_end);
            let partition_bytes = self
                .validate_and_decompress(partition_bytes, info.compression_codec)
                .await?;
            result.push((pm.offset(), PartitionIndexOwned::new(partition_bytes)?));
        }
        Ok(result)
    }

    /// Reads and decodes a single partition block from the object store by its byte range.
    pub(crate) async fn read_single_partition(
        &self,
        info: &SsTableInfo,
        offset: u64,
        length: u64,
        obj: &impl ReadOnlyBlob,
    ) -> Result<PartitionIndexOwned, SlateDBError> {
        let partition_bytes = obj.read_range(offset..offset + length).await?;
        let partition_bytes = self
            .validate_and_decompress(partition_bytes, info.compression_codec)
            .await?;
        Ok(PartitionIndexOwned::new(partition_bytes)?)
    }

    /// Stitches a slice of decoded partition blocks into a flat `SsTableIndexOwned`.
    pub(crate) fn stitch_partitions_to_flat(
        partitions: &[(u64, PartitionIndexOwned)],
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let mut raw: Vec<(u64, Bytes)> = Vec::new();
        for (_, partition) in partitions {
            let p = partition.borrow();
            for j in 0..p.blocks().len() {
                let bm = p.blocks().get(j);
                raw.push((bm.offset(), Bytes::copy_from_slice(bm.first_key().bytes())));
            }
        }
        let flat = EncodedSsTableFooterBuilder::encode_flat_index(&raw);
        Ok(SsTableIndexOwned::new(flat)?)
    }

    /// Reads a partitioned index by fetching the V2 directory and all partition
    /// blocks in a single range read, then stitching into a flat `SsTableIndexOwned`.
    async fn read_partitioned_index(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let v2 = self.read_v2_directory(info, obj).await?;
        let partitions = self.read_partitions_from_store(info, &v2, obj).await?;
        if partitions.is_empty() {
            return Ok(SsTableIndexOwned::new(
                EncodedSsTableFooterBuilder::encode_flat_index(&[]),
            )?);
        }
        Self::stitch_partitions_to_flat(&partitions)
    }

    /// Validates checksum, untransforms, and decompresses raw bytes from the object store.
    async fn validate_and_decompress(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<Bytes, SlateDBError> {
        let bytes = self.validate_checksum(bytes)?;
        let bytes = match &self.block_transformer {
            Some(t) => t
                .decode(bytes)
                .await
                .map_err(|_| SlateDBError::BlockTransformError)?,
            None => bytes,
        };
        Ok(match compression_codec {
            Some(c) => Self::decompress(bytes, c)?,
            None => bytes,
        })
    }

    #[cfg(test)]
    pub(crate) async fn read_index_raw(
        &self,
        info: &SsTableInfo,
        sst_bytes: &Bytes,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        match info.index_type {
            SstIndexType::Flat => {
                let index_off = info.index_offset as usize;
                let index_end = index_off + info.index_len as usize;
                let index_bytes: Bytes = sst_bytes.slice(index_off..index_end);
                self.decode_index(index_bytes, info.compression_codec).await
            }
            SstIndexType::Partitioned => {
                // Decode V2 directory from sst_bytes, then stitch all partitions.
                let index_off = info.index_offset as usize;
                let index_end = index_off + info.index_len as usize;
                let v2_bytes = sst_bytes.slice(index_off..index_end);
                let v2_bytes = self
                    .validate_and_decompress(v2_bytes, info.compression_codec)
                    .await?;
                let v2_owned = SsTableIndexV2Owned::new(v2_bytes)?;
                let v2 = v2_owned.borrow();
                let Some(partitions) = v2.partitions() else {
                    return Ok(SsTableIndexOwned::new(
                        EncodedSsTableFooterBuilder::encode_flat_index(&[]),
                    )?);
                };
                let mut raw_block_meta: Vec<(u64, Bytes)> = Vec::new();
                for i in 0..partitions.len() {
                    let pm = partitions.get(i);
                    let p_off = pm.offset() as usize;
                    let p_end = p_off + pm.length() as usize;
                    let p_bytes = sst_bytes.slice(p_off..p_end);
                    let p_bytes = self
                        .validate_and_decompress(p_bytes, info.compression_codec)
                        .await?;
                    let partition_owned = PartitionIndexOwned::new(p_bytes)?;
                    let partition = partition_owned.borrow();
                    for j in 0..partition.blocks().len() {
                        let bm = partition.blocks().get(j);
                        let key = Bytes::copy_from_slice(bm.first_key().bytes());
                        raw_block_meta.push((bm.offset(), key));
                    }
                }
                let flat_data = EncodedSsTableFooterBuilder::encode_flat_index(&raw_block_meta);
                Ok(SsTableIndexOwned::new(flat_data)?)
            }
        }
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

    pub(crate) async fn read_stats(
        &self,
        info: &SsTableInfo,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Option<SstStats>, SlateDBError> {
        if info.stats_len == 0 {
            return Ok(None);
        }
        let stats_end = info.stats_offset + info.stats_len;
        let stats_bytes = obj.read_range(info.stats_offset..stats_end).await?;
        let compression_codec = info.compression_codec;
        Ok(Some(
            self.decode_stats(stats_bytes, compression_codec).await?,
        ))
    }

    #[allow(dead_code)]
    async fn decode_stats(
        &self,
        bytes: Bytes,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<SstStats, SlateDBError> {
        let stats_bytes = self.validate_checksum(bytes)?;
        let untransformed_bytes = match &self.block_transformer {
            Some(t) => t
                .decode(stats_bytes)
                .await
                .map_err(|_| SlateDBError::BlockTransformError)?,
            None => stats_bytes,
        };
        let decompressed_bytes = match compression_codec {
            Some(c) => Self::decompress(untransformed_bytes, c)?,
            None => untransformed_bytes,
        };
        SstStats::decode(decompressed_bytes)
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
        // estimate sum of Stats
        ans += 5 * SIZEOF_U64 + CHECKSUM_SIZE;
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
