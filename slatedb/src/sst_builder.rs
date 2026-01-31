//! SSTable (Sorted String Table) encoding and building.
//!
//! This module provides the core data structures and builders for creating and reading
//! SSTables, the persistent on-disk format used by SlateDB for storing sorted key-value
//! data.
//!
//! # SSTable Format
//!
//! An SSTable consists of the following components, written sequentially:
//!
//! ```text
//! +------------------+
//! |   Data Blocks    |  <- Key-value entries, optionally compressed
//! +------------------+
//! |   Filter Block   |  <- Bloom filter for efficient key lookups (optional)
//! +------------------+
//! |   Index Block    |  <- Block metadata (offsets, first keys)
//! +------------------+
//! |   SST Info       |  <- Table metadata (key range, compression, etc.)
//! +------------------+
//! |  Metadata offset |  <- Metadata offset (8 bytes)
//! +------------------+
//! |     Version      |  <- Format version (2 bytes)
//! +------------------+
//! ```
//!
//! Each block is followed by a CRC32 checksum for data integrity verification.
//!
//! # Key Components
//!
//! - [`EncodedSsTableBuilder`]: Builder for constructing SSTables from entries
//!
//! The builder reuses shared components from the [`crate::format::sst`] module:
//! - [`EncodedSsTableBlockBuilder`]: For encoding data blocks
//! - [`EncodedSsTableFooterBuilder`]: For encoding the footer
//! - [`BlockTransformer`]: Trait for custom block transformations (e.g., encryption)
//!
//! # Compression
//!
//! SSTables support optional compression via the [`CompressionCodec`] enum:
//! - Snappy (feature: `snappy`)
//! - LZ4 (feature: `lz4`)
//! - Zstd (feature: `zstd`)
//! - Zlib (feature: `zlib`)
//!
//! Compression is applied per-block before the checksum is computed.
//!
//! # Block Transformation
//!
//! The [`BlockTransformer`] trait allows custom transformations on block data,
//! such as encryption. Transformations are applied after compression on write
//! and before decompression on read.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::DefaultAllocator;

use crate::config::CompressionCodec;
use crate::db_state::SsTableInfoCodec;
use crate::error::SlateDBError;
use crate::filter::BloomFilterBuilder;
use crate::flatbuffer_types::{BlockMeta, BlockMetaArgs};
use crate::format::block::BlockBuilder;
use crate::format::sst::{
    EncodedSsTableBlock, EncodedSsTableBlockBuilder, EncodedSsTableFooterBuilder,
};
use crate::types::RowEntry;
use crate::utils::compute_index_key;

// Re-export for backwards compatibility
pub(crate) use crate::format::sst::{BlockTransformer, EncodedSsTable, SsTableFormat};

impl SsTableFormat {
    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder<'_> {
        let mut builder = EncodedSsTableBuilder::new(
            self.block_size,
            self.min_filter_keys,
            self.sst_codec.clone(),
            self.filter_bits_per_key,
        );
        if let Some(codec) = self.compression_codec {
            builder = builder.with_compression_codec(codec);
        }
        if let Some(ref transformer) = self.block_transformer {
            builder = builder.with_block_transformer(transformer.clone());
        }
        builder
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
    block_transformer: Option<Arc<dyn BlockTransformer>>,
}

impl EncodedSsTableBuilder<'_> {
    /// Create a builder based on target block size.
    fn new(
        block_size: usize,
        min_filter_keys: u32,
        sst_codec: Box<dyn SsTableInfoCodec>,
        filter_bits_per_key: u32,
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
            compression_codec: None,
            block_transformer: None,
        }
    }

    /// Sets the compression codec for compressing the blocks
    fn with_compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.compression_codec = Some(codec);
        self
    }

    /// Sets the block transformer for transforming the blocks
    fn with_block_transformer(mut self, transformer: Arc<dyn BlockTransformer>) -> Self {
        self.block_transformer = Some(transformer);
        self
    }

    /// Adds an entry to the SSTable and returns the size of the block that was finished if any.
    /// The block size is calculated after applying any compression if enabled.
    /// The block size is None if the builder has not finished compacting a block yet.
    pub(crate) async fn add(&mut self, entry: RowEntry) -> Result<Option<usize>, SlateDBError> {
        self.num_keys += 1;

        let index_key = compute_index_key(self.current_block_max_key.take(), &entry.key);
        let is_sst_first_key = self.sst_first_key.is_none();

        let mut block_size = None;
        if !self.builder.would_fit(&entry) {
            block_size = self.finish_block().await?;
            self.first_key = Some(self.index_builder.create_vector(&index_key));
        } else if is_sst_first_key {
            self.first_key = Some(self.index_builder.create_vector(&index_key));
        }

        self.filter_builder.add_key(&entry.key);
        if is_sst_first_key {
            self.sst_first_key = Some(entry.key.clone());
        }
        self.current_block_max_key = Some(entry.key.clone());

        assert!(self.builder.add(entry));

        Ok(block_size)
    }

    #[cfg(test)]
    pub(crate) async fn add_value(
        &mut self,
        key: &[u8],
        val: &[u8],
        attrs: crate::types::RowAttributes,
    ) -> Result<Option<usize>, SlateDBError> {
        let entry = RowEntry::new(
            key.to_vec().into(),
            crate::types::ValueDeletable::Value(Bytes::copy_from_slice(val)),
            0,
            attrs.ts,
            attrs.expire_ts,
        );
        self.add(entry).await
    }

    pub(crate) fn next_block(&mut self) -> Option<EncodedSsTableBlock> {
        self.blocks.pop_front()
    }

    #[cfg(test)]
    pub(crate) fn num_blocks(&self) -> usize {
        // use block_meta since blocks can be consumed as sst is being built
        self.block_meta.len()
    }

    async fn finish_block(&mut self) -> Result<Option<usize>, SlateDBError> {
        if self.is_drained() {
            return Ok(None);
        }

        let new_builder = BlockBuilder::new(self.block_size);
        let builder = std::mem::replace(&mut self.builder, new_builder);
        let mut block_builder = EncodedSsTableBlockBuilder::new(builder, self.current_len);
        if let Some(codec) = self.compression_codec {
            block_builder = block_builder.with_compression_codec(codec);
        }
        if let Some(transformer) = self.block_transformer.clone() {
            block_builder = block_builder.with_block_transformer(transformer);
        }
        let block = block_builder.build().await?;
        let block_meta = BlockMeta::create(
            &mut self.index_builder,
            &BlockMetaArgs {
                offset: block.offset,
                first_key: self.first_key,
            },
        );
        self.block_meta.push(block_meta);

        let block_size = block.len();
        self.current_len += block_size as u64;
        self.blocks.push_back(block);
        self.first_key = None;

        Ok(Some(block_size))
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
    pub(crate) async fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        self.finish_block().await?;

        // Build footer (includes index building)
        let mut footer_builder = EncodedSsTableFooterBuilder::new(
            self.current_len,
            self.sst_first_key,
            &*self.sst_codec,
            self.index_builder,
            self.block_meta,
        );
        if let Some(codec) = self.compression_codec {
            footer_builder = footer_builder.with_compression_codec(codec);
        }
        if let Some(transformer) = self.block_transformer.clone() {
            footer_builder = footer_builder.with_block_transformer(transformer);
        }

        // Add filter if enough keys
        if self.num_keys >= self.min_filter_keys {
            let filter = Arc::new(self.filter_builder.build());
            let encoded_filter = filter.encode();
            footer_builder = footer_builder.with_filter(filter, encoded_filter);
        }

        let footer = footer_builder.build().await?;

        Ok(EncodedSsTable {
            info: footer.info,
            index: footer.index,
            filter: footer.filter,
            unconsumed_blocks: self.blocks,
            footer: footer.encoded_bytes,
        })
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.builder.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::ops::Range;

    use async_trait::async_trait;
    use bytes::{BufMut, BytesMut};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::vec;

    use super::*;
    use crate::blob::ReadOnlyBlob;
    use crate::block_iterator::BlockIterator;
    use crate::bytes_range::BytesRange;
    use crate::db_state::SsTableId;
    use crate::filter::filter_hash;
    use crate::format::block::Block;
    use crate::iter::IterationOrder::Ascending;
    use crate::object_stores::ObjectStores;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, build_test_sst, gen_attrs, gen_empty_attrs};

    #[test]
    fn test_estimate_encoded_size() {
        let mut format = SsTableFormat::default();

        // Test with zero entries
        assert_eq!(format.estimate_encoded_size_compacted(0, 0), 0);

        // Test with one entry
        let encoded_entry_size = 100;
        let size = format.estimate_encoded_size_compacted(1, encoded_entry_size);
        assert!(size > 0);

        // Test with multiple entries with not trigger bloom filter
        format.min_filter_keys = 1000;
        let num_entries = 100;
        let total_size = encoded_entry_size * num_entries;
        let size = format.estimate_encoded_size_compacted(num_entries, total_size);
        assert!(size > total_size); // Should be larger due to overhead

        // Test with entries that should trigger bloom filter
        let num_entries = format.min_filter_keys as usize * 10;
        let total_size = encoded_entry_size * num_entries;
        let size_with_filter = format.estimate_encoded_size_compacted(num_entries, total_size);
        format.min_filter_keys = format.min_filter_keys * 10 + 1;
        let size_without_filter =
            format.estimate_encoded_size_compacted(num_entries, encoded_entry_size * num_entries);
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 8], &[b'1'; 8], gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(&[b'b'; 8], &[b'2'; 8], gen_attrs(2))
            .await
            .unwrap();
        builder
            .add_value(&[b'c'; 8], &[b'3'; 8], gen_attrs(3))
            .await
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
            .await
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 8], &[b'1'; 8], gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(&[b'b'; 8], &[b'2'; 8], gen_attrs(2))
            .await
            .unwrap();
        builder
            .add_value(&[b'c'; 8], &[b'3'; 8], gen_attrs(3))
            .await
            .unwrap();
        let first_block = builder.next_block();

        let encoded = builder.build().await.unwrap();

        let mut raw_sst = Vec::<u8>::new();
        raw_sst.put_slice(first_block.unwrap().encoded_bytes.as_ref());
        assert_eq!(encoded.unconsumed_blocks.len(), 2);
        encoded.put_remaining(&mut raw_sst);
        let raw_sst = Bytes::copy_from_slice(raw_sst.as_slice());
        let index = format
            .read_index_raw(&encoded.info, &raw_sst)
            .await
            .unwrap();
        let block = format
            .read_block_raw(&encoded.info, &index, 0, &raw_sst)
            .await
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'a'; 8], &[b'1'; 8], 0).with_create_ts(1)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 1, &raw_sst)
            .await
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'b'; 8], &[b'2'; 8], 0).with_create_ts(2)],
        )
        .await;
        let block = format
            .read_block_raw(&encoded.info, &index, 2, &raw_sst)
            .await
            .unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'c'; 8], &[b'3'; 8], 0).with_create_ts(3)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_builder_should_return_blocks_with_correct_data_and_offsets() {
        let format = SsTableFormat::default();

        let sst = build_test_sst(&format, 3).await;

        let bytes = sst.remaining_as_bytes();
        let index = format.read_index_raw(&sst.info, &bytes).await.unwrap();
        let block_metas = index.borrow().block_meta();
        assert_eq!(block_metas.len(), sst.unconsumed_blocks.len());
        for i in 0..block_metas.len() {
            let encoded_block = sst.unconsumed_blocks.get(i).unwrap();
            assert_eq!(block_metas.get(i).offset(), encoded_block.offset);
            let read_block = format
                .read_block_raw(&sst.info, &index, i, &bytes)
                .await
                .unwrap();
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
            None,
        );
        let mut builder = table_store.table_builder();
        for k in 1..=8 {
            builder
                .add_value(
                    format!("key{}", k).as_bytes(),
                    format!("value{}", k).as_bytes(),
                    gen_attrs(k),
                )
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
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
            sst_info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(wal_id)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let index = table_store
            .read_index(&sst_handle_from_store, true)
            .await
            .unwrap();
        let sst_info_from_store = sst_handle_from_store.info;
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_info_from_store.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct after reading from store"
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        let filter = table_store
            .read_filter(&sst_handle, true)
            .await
            .unwrap()
            .unwrap();

        assert!(filter.might_contain(filter_hash(b"key1")));
        assert!(filter.might_contain(filter_hash(b"key2")));
        assert_eq!(encoded_info, sst_handle.info);
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_handle.info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct"
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
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
            None,
        );
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        let filter = table_store
            .read_filter(&sst_handle, true)
            .await
            .unwrap()
            .unwrap();

        assert!(filter.might_contain(filter_hash(b"key1")));
        assert!(filter.might_contain(filter_hash(b"key2")));
        assert_eq!(encoded_info, sst_handle.info);
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_handle.info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct"
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(&[b'a'; 2], &[1u8; 2], gen_empty_attrs())
            .await
            .unwrap();
        builder
            .add_value(&[b'b'; 2], &[2u8; 2], gen_empty_attrs())
            .await
            .unwrap();
        builder
            .add_value(&[b'c'; 20], &[3u8; 20], gen_attrs(3))
            .await
            .unwrap();
        builder
            .add_value(&[b'd'; 20], &[4u8; 20], gen_attrs(4))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let info = encoded.info.clone();
        let bytes = encoded.remaining_as_bytes();
        let index = format.read_index_raw(&encoded.info, &bytes).await.unwrap();
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
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
            sst_info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let index = table_store
            .read_index(&sst_handle_from_store, true)
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
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
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
            None,
        ));
        let mut builder = table_store.table_builder();
        for key in 'a'..='z' {
            let key_bytes = [key as u8];
            builder
                .add_value(&key_bytes, b"value", gen_empty_attrs())
                .await?;
        }
        let encoded = builder.build().await?;

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
            let mut iter = SstIterator::new_owned_initialized(
                ..,
                sst_handle.clone(),
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await?
            .expect("Expected Some(iter) but got None");

            assert_iterator(&mut iter, expected_entries).await;

            // scan range outside of visible range and validate that it returns empty iterator.
            let iter = SstIterator::new_owned_initialized(
                Bytes::from_static(b"m")..Bytes::from_static(b"p"),
                sst_handle,
                table_store,
                SstIteratorOptions::default(),
            )
            .await?;

            assert!(iter.is_none());
        } else {
            // scan the entire sst and validate that the visible range is respected.
            let mut iter = SstIterator::new_borrowed_initialized(
                ..,
                &sst_handle,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await?
            .expect("Expected Some(iter) but got None");

            assert_iterator(&mut iter, expected_entries).await;

            // scan range outside of visible range and validate that it returns empty iterator.
            let iter = SstIterator::new_borrowed_initialized(
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

    struct XorTransformer {
        key: u8,
    }

    #[async_trait]
    impl BlockTransformer for XorTransformer {
        async fn encode(&self, data: Bytes) -> Result<Bytes, crate::error::Error> {
            let transformed: Vec<u8> = data.iter().map(|b| b ^ self.key).collect();
            Ok(Bytes::from(transformed))
        }

        async fn decode(&self, data: Bytes) -> Result<Bytes, crate::error::Error> {
            self.encode(data).await
        }
    }

    #[tokio::test]
    async fn test_sstable_with_block_transformer() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let transformer = Arc::new(XorTransformer { key: 0x42 });

        let format = SsTableFormat {
            block_transformer: Some(transformer.clone()),
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();

        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        let filter = table_store
            .read_filter(&sst_handle, true)
            .await
            .unwrap()
            .unwrap();

        assert!(filter.might_contain(filter_hash(b"key1")));
        assert!(filter.might_contain(filter_hash(b"key2")));
        assert_eq!(encoded_info, sst_handle.info);
        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_handle.info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct"
        );
    }

    #[tokio::test]
    async fn test_block_transformer_with_compression() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let transformer = Arc::new(XorTransformer { key: 0xAB });

        #[cfg(feature = "snappy")]
        let compression = Some(crate::config::CompressionCodec::Snappy);
        #[cfg(not(feature = "snappy"))]
        let compression = None;

        let format = SsTableFormat {
            block_transformer: Some(transformer),
            compression_codec: compression,
            ..SsTableFormat::default()
        };
        let table_store = TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        );
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();

        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();

        assert_eq!(1, index.borrow().block_meta().len());
        assert_eq!(
            b"key1",
            sst_handle.info.first_entry.unwrap().as_ref(),
            "first entry in sst info should be correct with compression + transformer"
        );
    }
}
