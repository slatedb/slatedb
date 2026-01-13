use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::DefaultAllocator;

use crate::block::BlockBuilder;
use crate::config::CompressionCodec;
use crate::db_state::SsTableInfoCodec;
use crate::error::SlateDBError;
use crate::flatbuffer_types::{BlockMeta, BlockMetaArgs};
use crate::sst::{
    BlockTransformer, EncodedSsTable, EncodedSsTableBlock, EncodedSsTableBlockBuilder,
    EncodedSsTableFooterBuilder,
};
use crate::types::RowEntry;

/// Builds a WAL SSTable from entries.
///
/// This builder differs from the regular EncodedSsTableBuilder in that:
/// - No bloom filter is created
/// - The index tracks sequence number ranges per block instead of first keys
#[allow(unused)]
pub(crate) struct EncodedSsTableBuilder<'a> {
    builder: BlockBuilder,
    index_builder: flatbuffers::FlatBufferBuilder<'a, DefaultAllocator>,
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'a>>>,
    current_len: u64,
    blocks: VecDeque<EncodedSsTableBlock>,
    block_size: usize,
    sst_codec: Box<dyn SsTableInfoCodec>,
    compression_codec: Option<CompressionCodec>,
    block_transformer: Option<Arc<dyn BlockTransformer>>,
    sst_first_key: Option<Bytes>,
    first_seq: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
}

impl EncodedSsTableBuilder<'_> {
    /// Create a builder based on target block size.
    #[allow(unused)]
    pub(crate) fn new(
        block_size: usize,
        sst_codec: Box<dyn SsTableInfoCodec>,
        compression_codec: Option<CompressionCodec>,
        block_transformer: Option<Arc<dyn BlockTransformer>>,
    ) -> Self {
        Self {
            current_len: 0,
            blocks: VecDeque::new(),
            block_meta: Vec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
            index_builder: flatbuffers::FlatBufferBuilder::new(),
            sst_codec,
            compression_codec,
            block_transformer,
            sst_first_key: None,
            first_seq: None,
        }
    }

    /// Adds an entry to the WAL SSTable and returns the size of the block that was finished if any.
    #[allow(unused)]
    pub(crate) async fn add(&mut self, entry: RowEntry) -> Result<Option<usize>, SlateDBError> {
        // Track first key for SST info
        let is_sst_first_key = self.sst_first_key.is_none();
        // ToDo(cadonna) Is this really needed for the WAL?
        if is_sst_first_key {
            self.sst_first_key = Some(entry.key.clone());
        }

        let first_seq_bytes = entry.seq.to_be_bytes();

        let mut block_size = None;
        if !self.builder.would_fit(&entry) {
            block_size = self.finish_block().await?;
            self.first_seq = Some(self.index_builder.create_vector(&first_seq_bytes));
        } else if is_sst_first_key {
            self.first_seq = Some(self.index_builder.create_vector(&first_seq_bytes));
        }

        assert!(self.builder.add(entry));

        Ok(block_size)
    }

    #[cfg(test)]
    pub(crate) async fn add_value(
        &mut self,
        key: &[u8],
        val: &[u8],
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Result<Option<usize>, SlateDBError> {
        let entry = RowEntry::new(
            key.to_vec().into(),
            crate::types::ValueDeletable::Value(Bytes::copy_from_slice(val)),
            seq,
            create_ts,
            expire_ts,
        );
        self.add(entry).await
    }

    #[allow(unused)]
    pub(crate) fn next_block(&mut self) -> Option<EncodedSsTableBlock> {
        self.blocks.pop_front()
    }

    #[allow(unused)]
    async fn finish_block(&mut self) -> Result<Option<usize>, SlateDBError> {
        if self.is_drained() {
            return Ok(None);
        }

        let new_builder = BlockBuilder::new(self.block_size);
        let builder = std::mem::replace(&mut self.builder, new_builder);
        let block = EncodedSsTableBlockBuilder::new(
            builder,
            self.current_len,
            self.compression_codec,
            self.block_transformer.as_ref(),
        )
        .build()
        .await?;
        let block_meta = BlockMeta::create(
            &mut self.index_builder,
            &BlockMetaArgs {
                offset: block.offset,
                first_key: self.first_seq,
            },
        );
        self.block_meta.push(block_meta);

        let block_size = block.len();
        self.current_len += block_size as u64;
        self.blocks.push_back(block);
        self.first_seq = None;

        Ok(Some(block_size))
    }

    /// Builds the WAL SST from the current state.
    ///
    /// # Format
    ///
    /// +---------------------------------------------------+
    /// |                Data Blocks                        |
    /// |    (raw bytes produced by finish_block)           |
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
    ///
    /// Note: Unlike regular SSTs, WAL SSTs have no bloom filter.
    /// The index first_key field contains the min sequence number (as bytes) for each block.
    #[allow(unused)]
    pub(crate) async fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        self.finish_block().await?;

        // Build footer (includes index building, no filter for WAL SSTs)
        let footer = EncodedSsTableFooterBuilder::new(
            self.current_len,
            self.sst_first_key,
            self.compression_codec,
            self.block_transformer.as_ref(),
            &*self.sst_codec,
            self.index_builder,
            self.block_meta,
        )
        .build()
        .await?;

        Ok(EncodedSsTable {
            info: footer.info,
            index: footer.index,
            filter: None,
            unconsumed_blocks: self.blocks,
            footer: footer.data,
        })
    }

    #[allow(unused)]
    pub(crate) fn is_drained(&self) -> bool {
        self.builder.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;
    use crate::block_iterator::BlockIterator;
    use crate::flatbuffer_types::FlatBufferSsTableInfoCodec;
    use crate::iter::IterationOrder::Ascending;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;

    fn next_block_to_iter(builder: &mut EncodedSsTableBuilder) -> BlockIterator<Block> {
        let block = builder.next_block();
        assert!(block.is_some());
        let block = block.unwrap().block;
        BlockIterator::new(block, Ascending)
    }

    #[tokio::test]
    async fn should_make_blocks_available_and_report_size() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(32, Box::new(FlatBufferSsTableInfoCodec {}), None, None);

        // When
        let result1 = builder
            .add_value(&[b'a'; 8], &[b'1'; 8], 1, Some(1), None)
            .await
            .unwrap();
        let result2 = builder
            .add_value(&[b'b'; 8], &[b'2'; 8], 2, Some(2), None)
            .await
            .unwrap();
        let result3 = builder
            .add_value(&[b'c'; 8], &[b'3'; 8], 3, Some(3), None)
            .await
            .unwrap();

        // Then
        assert!(result1.is_none(), "First entry should not finish a block");
        assert!(
            result2.is_some(),
            "Second entry should finish the first block"
        );
        assert!(
            result2.unwrap() > 0,
            "Block size of first block should be positive"
        );
        assert!(
            result3.is_some(),
            "Third entry should finish the second block"
        );

        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'a'; 8], &[b'1'; 8], 1).with_create_ts(1)],
        )
        .await;

        let mut iter = next_block_to_iter(&mut builder);
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_value(&[b'b'; 8], &[b'2'; 8], 2).with_create_ts(2)],
        )
        .await;

        assert!(builder.next_block().is_none());
    }

    #[tokio::test]
    async fn should_build_without_bloom_filter() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"value1", 1, None, None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, None, None)
            .await
            .unwrap();

        // When
        let encoded = builder.build().await.unwrap();

        // Then
        assert!(encoded.filter.is_none());
        assert_eq!(encoded.info.filter_len, 0);
    }

    #[tokio::test]
    async fn should_store_entries_in_insertion_order() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(4096, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"zebra", b"val1", 5, Some(100), None)
            .await
            .unwrap();
        builder
            .add_value(b"apple", b"val2", 2, Some(200), None)
            .await
            .unwrap();
        builder
            .add_value(b"mango", b"val3", 10, Some(300), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(encoded.info.first_key.as_ref().unwrap().as_ref(), b"zebra");
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"zebra", b"val1", 5).with_create_ts(100),
            RowEntry::new_value(b"apple", b"val2", 2).with_create_ts(200),
            RowEntry::new_value(b"mango", b"val3", 10).with_create_ts(300),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_tombstones() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        let tombstone = RowEntry::new(
            Bytes::from_static(b"key1"),
            ValueDeletable::Tombstone,
            1,
            Some(100),
            None,
        );
        builder.add(tombstone).await.unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_tombstone(b"key1", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_merge() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        let merge = RowEntry::new(
            Bytes::from_static(b"key1"),
            ValueDeletable::Merge(Bytes::from_static(b"merge_value")),
            1,
            Some(100),
            None,
        );
        builder.add(merge).await.unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_merge(b"key1", b"merge_value", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_store_first_seq_in_index() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(32, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(&[b'a'; 8], &[b'1'; 8], 10, None, None)
            .await
            .unwrap();
        builder
            .add_value(&[b'b'; 8], &[b'2'; 8], 11, None, None)
            .await
            .unwrap();
        builder
            .add_value(&[b'c'; 8], &[b'3'; 8], 12, None, None)
            .await
            .unwrap();

        // When
        let encoded = builder.build().await.unwrap();

        // Then
        let index = encoded.index.borrow();
        let block_metas = index.block_meta();
        assert_eq!(3, block_metas.len(), "Index should have three entries");
        assert_eq!(block_metas.get(0).first_key().bytes(), &10u64.to_be_bytes());
        assert_eq!(block_metas.get(1).first_key().bytes(), &11u64.to_be_bytes());
        assert_eq!(block_metas.get(2).first_key().bytes(), &12u64.to_be_bytes());
    }

    #[tokio::test]
    async fn should_build_empty_sst_when_no_entries() {
        // Given
        let builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        assert!(builder.is_drained());

        // When
        let encoded = builder.build().await.unwrap();

        // Then
        assert!(encoded.unconsumed_blocks.is_empty());
        assert!(encoded.info.first_key.is_none());
        assert!(encoded.filter.is_none());
    }

    #[tokio::test]
    async fn should_handle_empty_value() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"", 1, Some(1), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(2), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"", 1).with_create_ts(1),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(2),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_large_value() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(64, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        let large_value = vec![b'x'; 256];
        builder
            .add_value(b"key1", &large_value, 1, Some(1), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![RowEntry::new_value(b"key1", &large_value, 1).with_create_ts(1)];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_single_entry_sst() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"only_key", b"only_value", 42, Some(100), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(
            encoded.info.first_key.as_ref().unwrap().as_ref(),
            b"only_key"
        );
        assert_eq!(encoded.unconsumed_blocks.len(), 1);
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected =
            vec![RowEntry::new_value(b"only_key", b"only_value", 42).with_create_ts(100)];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_max_seq_number() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"value1", u64::MAX, Some(1), None)
            .await
            .unwrap();

        // When
        let encoded = builder.build().await.unwrap();

        // Then
        let index = encoded.index.borrow();
        let block_metas = index.block_meta();
        assert_eq!(
            block_metas.get(0).first_key().bytes(),
            &u64::MAX.to_be_bytes()
        );
    }

    #[tokio::test]
    async fn should_handle_duplicate_keys_different_seqs() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"value1", 1, Some(1), None)
            .await
            .unwrap();
        builder
            .add_value(b"key1", b"value2", 2, Some(2), None)
            .await
            .unwrap();
        builder
            .add_value(b"key1", b"value3", 3, Some(3), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(1),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(2),
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(3),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_handle_entries_with_expire_ts() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"value1", 1, Some(100), Some(200))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(100), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1)
                .with_create_ts(100)
                .with_expire_ts(200),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(100),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_verify_block_checksums() {
        // Given
        let mut builder =
            EncodedSsTableBuilder::new(1024, Box::new(FlatBufferSsTableInfoCodec {}), None, None);
        builder
            .add_value(b"key1", b"value1", 1, None, None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let encoded_bytes = &block.encoded_bytes;

        // Then
        let checksum_offset = encoded_bytes.len() - size_of::<u32>();
        let stored_checksum =
            u32::from_be_bytes(encoded_bytes[checksum_offset..].try_into().unwrap());
        let computed_checksum = crc32fast::hash(&encoded_bytes[..checksum_offset]);
        assert_eq!(stored_checksum, computed_checksum);
    }

    #[cfg(feature = "snappy")]
    #[tokio::test]
    async fn should_compress_blocks_with_snappy() {
        // Given
        let mut builder = EncodedSsTableBuilder::new(
            1024,
            Box::new(FlatBufferSsTableInfoCodec {}),
            Some(CompressionCodec::Snappy),
            None,
        );
        builder
            .add_value(b"key1", b"value1", 1, Some(100), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(
            encoded.info.compression_codec,
            Some(CompressionCodec::Snappy)
        );
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn should_compress_blocks_with_lz4() {
        // Given
        let mut builder = EncodedSsTableBuilder::new(
            1024,
            Box::new(FlatBufferSsTableInfoCodec {}),
            Some(CompressionCodec::Lz4),
            None,
        );
        builder
            .add_value(b"key1", b"value1", 1, Some(100), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(encoded.info.compression_codec, Some(CompressionCodec::Lz4));
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn should_compress_blocks_with_zstd() {
        // Given
        let mut builder = EncodedSsTableBuilder::new(
            1024,
            Box::new(FlatBufferSsTableInfoCodec {}),
            Some(CompressionCodec::Zstd),
            None,
        );
        builder
            .add_value(b"key1", b"value1", 1, Some(100), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(encoded.info.compression_codec, Some(CompressionCodec::Zstd));
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    #[cfg(feature = "zlib")]
    #[tokio::test]
    async fn should_compress_blocks_with_zlib() {
        // Given
        let mut builder = EncodedSsTableBuilder::new(
            1024,
            Box::new(FlatBufferSsTableInfoCodec {}),
            Some(CompressionCodec::Zlib),
            None,
        );
        builder
            .add_value(b"key1", b"value1", 1, Some(100), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", 2, Some(200), None)
            .await
            .unwrap();

        // When
        let mut encoded = builder.build().await.unwrap();

        // Then
        assert_eq!(encoded.info.compression_codec, Some(CompressionCodec::Zlib));
        let block = encoded.unconsumed_blocks.pop_front().unwrap();
        let mut iter = BlockIterator::new(block.block, Ascending);
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(100),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
        ];
        assert_iterator(&mut iter, expected).await;
    }

    mod block_transformer_tests {
        use super::*;
        use async_trait::async_trait;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Mock block transformer that tracks encode/decode calls.
        struct MockBlockTransformer {
            encode_call_count: AtomicUsize,
            decode_call_count: AtomicUsize,
        }

        impl MockBlockTransformer {
            fn new() -> Arc<Self> {
                Arc::new(Self {
                    encode_call_count: AtomicUsize::new(0),
                    decode_call_count: AtomicUsize::new(0),
                })
            }

            fn encode_call_count(&self) -> usize {
                self.encode_call_count.load(Ordering::SeqCst)
            }

            #[allow(dead_code)]
            fn decode_call_count(&self) -> usize {
                self.decode_call_count.load(Ordering::SeqCst)
            }
        }

        #[async_trait]
        impl BlockTransformer for MockBlockTransformer {
            async fn encode(&self, data: Bytes) -> Result<Bytes, crate::error::Error> {
                self.encode_call_count.fetch_add(1, Ordering::SeqCst);
                Ok(data)
            }

            async fn decode(&self, data: Bytes) -> Result<Bytes, crate::error::Error> {
                self.decode_call_count.fetch_add(1, Ordering::SeqCst);
                Ok(data)
            }
        }

        #[tokio::test]
        async fn should_call_encode_for_data_block() {
            // Given
            let transformer = MockBlockTransformer::new();
            let mut builder = EncodedSsTableBuilder::new(
                1024,
                Box::new(FlatBufferSsTableInfoCodec {}),
                None,
                Some(Arc::clone(&transformer) as Arc<dyn BlockTransformer>),
            );
            builder
                .add_value(b"key1", b"value1", 1, Some(100), None)
                .await
                .unwrap();

            // When
            let _ = builder.build().await.unwrap();

            // Then - encode should be called for 1 data block + 1 index block = 2 calls
            assert_eq!(transformer.encode_call_count(), 2);
        }

        #[tokio::test]
        async fn should_call_encode_for_each_data_block() {
            // Given - use small block size to force multiple blocks
            let transformer = MockBlockTransformer::new();
            let mut builder = EncodedSsTableBuilder::new(
                32,
                Box::new(FlatBufferSsTableInfoCodec {}),
                None,
                Some(Arc::clone(&transformer) as Arc<dyn BlockTransformer>),
            );
            builder
                .add_value(&[b'a'; 8], &[b'1'; 8], 1, Some(1), None)
                .await
                .unwrap();
            builder
                .add_value(&[b'b'; 8], &[b'2'; 8], 2, Some(2), None)
                .await
                .unwrap();
            builder
                .add_value(&[b'c'; 8], &[b'3'; 8], 3, Some(3), None)
                .await
                .unwrap();

            // When
            let encoded = builder.build().await.unwrap();

            // Then - encode should be called for each data block + index block
            let data_block_count = encoded.unconsumed_blocks.len();
            assert!(data_block_count >= 2, "Should have multiple data blocks");
            // Each data block + 1 index block
            assert_eq!(transformer.encode_call_count(), data_block_count + 1);
        }

        #[tokio::test]
        async fn should_preserve_block_data_with_transformer() {
            // Given
            let transformer = MockBlockTransformer::new();
            let mut builder = EncodedSsTableBuilder::new(
                1024,
                Box::new(FlatBufferSsTableInfoCodec {}),
                None,
                Some(transformer as Arc<dyn BlockTransformer>),
            );
            builder
                .add_value(b"key1", b"value1", 1, Some(100), None)
                .await
                .unwrap();
            builder
                .add_value(b"key2", b"value2", 2, Some(200), None)
                .await
                .unwrap();

            // When
            let mut encoded = builder.build().await.unwrap();

            // Then - block data should still be readable
            let block = encoded.unconsumed_blocks.pop_front().unwrap();
            let mut iter = BlockIterator::new(block.block, Ascending);
            let expected = vec![
                RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(100),
                RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(200),
            ];
            assert_iterator(&mut iter, expected).await;
        }

        #[cfg(feature = "lz4")]
        #[tokio::test]
        async fn should_call_encode_with_compression() {
            // Given
            let transformer = MockBlockTransformer::new();
            let mut builder = EncodedSsTableBuilder::new(
                1024,
                Box::new(FlatBufferSsTableInfoCodec {}),
                Some(CompressionCodec::Lz4),
                Some(Arc::clone(&transformer) as Arc<dyn BlockTransformer>),
            );
            builder
                .add_value(b"key1", b"value1", 1, Some(100), None)
                .await
                .unwrap();

            // When
            let _ = builder.build().await.unwrap();

            // Then - encode should still be called (1 data block + 1 index block)
            assert_eq!(transformer.encode_call_count(), 2);
        }
    }
}
