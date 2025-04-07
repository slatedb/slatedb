use crate::error::SlateDBError;
use crate::row_codec::{SstRowCodecV0, SstRowEntry};
use crate::types::RowEntry;
use crate::utils::clamp_allocated_size_bytes;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

#[derive(Eq, PartialEq)]
pub(crate) struct Block {
    pub(crate) data: Bytes,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    #[rustfmt::skip]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size());
        buf.put_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.freeze()
    }

    #[rustfmt::skip]
    pub fn decode(bytes: Bytes) -> Self {
        // Get number of elements in the block
        let data = bytes.as_ref();
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len()
            - SIZEOF_U16                                            // Entry u16 length
            - entry_offsets_len * SIZEOF_U16; // Offset byte array length
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16]; // Entry u16
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let bytes = bytes.slice(0..data_end);
        Self {
            data: bytes,
            offsets,
        }
    }

    pub(crate) fn clamp_allocated_size(&self) -> Self {
        Self {
            data: clamp_allocated_size_bytes(&self.data),
            offsets: self.offsets.clone(),
        }
    }

    /// Returns the size of the block in bytes.
    #[rustfmt::skip]
    pub(crate) fn size(&self) -> usize {
        self.data.len()                   // data byte length
        + self.offsets.len() * SIZEOF_U16 // offsets as u16's
        + SIZEOF_U16 // number of offsets in the block
    }

    /// estimate the size of Blocks encoded in SST
    pub(crate) fn estimate_encoded_size(
        entry_num: usize,
        entries_size_encoded: usize,
        number_of_blocks: usize,
    ) -> usize {
        let mut ans = entries_size_encoded;
        let offset_len = std::mem::size_of::<u16>();
        let checksum_len = std::mem::size_of::<u32>();
        ans += offset_len * entry_num;
        ans += checksum_len * number_of_blocks;
        ans
    }
}

pub struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
    first_key: Bytes,
}

// Details can be found: https://users.rust-lang.org/t/how-to-find-common-prefix-of-two-byte-slices-effectively/25815/4
fn compute_prefix(lhs: &[u8], rhs: &[u8]) -> usize {
    compute_prefix_chunks::<128>(lhs, rhs)
}

fn compute_prefix_chunks<const N: usize>(lhs: &[u8], rhs: &[u8]) -> usize {
    let off = std::iter::zip(lhs.chunks_exact(N), rhs.chunks_exact(N))
        .take_while(|(a, b)| a == b)
        .count()
        * N;
    off + std::iter::zip(&lhs[off..], &rhs[off..])
        .take_while(|(a, b)| a == b)
        .count()
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: Bytes::new(),
        }
    }

    #[rustfmt::skip]
    #[inline]
    fn estimated_size(&self) -> usize {
        SIZEOF_U16           // number of key-value pairs in the block
        + self.offsets.len() * SIZEOF_U16 // offsets
        + self.data.len()    // key-value pairs
    }

    #[must_use]
    pub fn add(&mut self, entry: RowEntry) -> bool {
        assert!(!entry.key.is_empty(), "key must not be empty");
        let key_prefix_len = compute_prefix(&self.first_key, &entry.key);
        let key_suffix = entry.key.slice(key_prefix_len..);

        let sst_row_entry = &SstRowEntry::new(
            key_prefix_len,
            key_suffix,
            entry.seq,
            entry.value,
            entry.create_ts,
            entry.expire_ts,
        );

        // If adding the key-value pair would exceed the block size limit, don't add it.
        // (Unless the block is empty, in which case, allow the block to exceed the limit.)
        if self.estimated_size() + sst_row_entry.size() > self.block_size && !self.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        let codec = SstRowCodecV0::new();
        let key = entry.key.clone();
        codec.encode(&mut self.data, sst_row_entry);

        if self.first_key.is_empty() {
            self.first_key = Bytes::copy_from_slice(&key);
        }

        true
    }

    #[cfg(test)]
    pub fn add_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        attrs: crate::types::RowAttributes,
    ) -> bool {
        let entry = RowEntry::new(
            key.to_vec().into(),
            crate::types::ValueDeletable::Value(Bytes::copy_from_slice(value)),
            0,
            attrs.ts,
            attrs.expire_ts,
        );
        self.add(entry)
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn add_tombstone(&mut self, key: &[u8], attrs: crate::types::RowAttributes) -> bool {
        let entry = RowEntry::new(
            key.to_vec().into(),
            crate::types::ValueDeletable::Tombstone,
            0,
            attrs.ts,
            attrs.expire_ts,
        );
        self.add(entry)
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn build(self) -> Result<Block, SlateDBError> {
        if self.is_empty() {
            return Err(SlateDBError::EmptyBlock);
        }
        Ok(Block {
            data: Bytes::from(self.data),
            offsets: self.offsets,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::block_iterator::BlockIterator;
    use crate::test_utils::assert_iterator;
    use crate::{
        test_utils::{assert_debug_snapshot, decode_codec_entries},
        types::ValueDeletable,
    };

    #[derive(Debug)]
    struct BlockTestCase {
        name: &'static str,
        entries: Vec<RowEntry>, // Use RowEntry instead of (key, value)
    }

    fn build_block(test_case: &BlockTestCase) -> Block {
        let mut builder = BlockBuilder::new(4096);

        for entry in &test_case.entries {
            assert!(builder.add(entry.clone()));
        }

        builder.build().expect("Failed to build block")
    }

    #[rstest]
    #[case(BlockTestCase {
        name: "test_block",
        entries: vec![
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1")),
                0,
                Some(0),
                Some(0),
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1")),
                0,
                Some(0),
                Some(0),
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key2"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value2")),
                0,
                Some(0),
                Some(0),
            ),
        ],
    })]
    #[case(BlockTestCase {
        name: "block_with_tombstone",
        entries: vec![
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1")),
                0,
                Some(0),
                Some(0),
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key2"),
                ValueDeletable::Tombstone,
                0,
                Some(0),
                None,
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key3"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value3")),
                0,
                Some(0),
                Some(0),
            ),
        ],
    })]
    #[case(BlockTestCase {
        name: "block_with_merge",
        entries: vec![
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1")),
                0,
                Some(0),
                Some(0),
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Merge(Bytes::copy_from_slice(b"value1")),
                0,
                Some(0),
                Some(0),
            ),
            RowEntry::new(
                Bytes::copy_from_slice(b"key2"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value2")),
                0,
                Some(0),
                Some(0),
            ),
        ],
    })]
    fn test_block(#[case] test_case: BlockTestCase) {
        let block = build_block(&test_case);
        let encoded = block.encode();
        let decoded = Block::decode(encoded.clone());
        let block_data = &block.data;
        let block_offsets = &block.offsets;
        // Decode the block data using offsets and validate each decoded entry
        let decoded_entries = decode_codec_entries(block_data.clone(), block_offsets)
            .expect("Failed to decode codec entries");
        assert_eq!(decoded_entries, test_case.entries);

        assert_eq!(block_data, &decoded.data);
        assert_eq!(block_offsets, &decoded.offsets);
        assert_debug_snapshot!(test_case.name, (block.size(), block.data, block.offsets));
    }

    #[test]
    fn test_prefix_computing() {
        assert_eq!(compute_prefix(b"1", b"11"), 1);
        assert_eq!(compute_prefix(b"222", b"111"), 0);
        assert_eq!(compute_prefix(b"1234567", b"123456789"), 7);
    }

    fn row_entries(n: u64) -> Vec<RowEntry> {
        (0..n)
            .map(|i| {
                RowEntry::new(
                    Bytes::copy_from_slice(format!("key{}", i).as_bytes()),
                    ValueDeletable::Value(Bytes::copy_from_slice(format!("value{}", i).as_bytes())),
                    i,
                    Some(100 + i as i64),
                    Some(200 + i as i64),
                )
            })
            .collect()
    }

    struct ClampAllocTestCase {
        entries: Vec<RowEntry>,
        extra_bytes: usize,
    }

    #[rstest]
    #[case(ClampAllocTestCase {
        entries: row_entries(3),
        extra_bytes: 100
    })]
    #[case(ClampAllocTestCase {
        entries: row_entries(3),
        extra_bytes: 0
    })]
    #[tokio::test]
    async fn test_should_clamp_allocated_size(#[case] case: ClampAllocTestCase) {
        let mut builder = BlockBuilder::new(4096);
        for e in case.entries.iter() {
            assert!(builder.add(e.clone()));
        }
        let block = builder.build().unwrap();
        let encoded = block.encode();
        let mut extended_data = BytesMut::with_capacity(encoded.len() + case.extra_bytes);
        extended_data.put(encoded.as_ref());
        extended_data.put_bytes(0u8, case.extra_bytes);
        let extended_data = extended_data.freeze();
        let block_extended = Block::decode(extended_data.slice(..encoded.len()));

        let block_clamped = block_extended.clamp_allocated_size();

        assert_eq!(block.data, block_clamped.data);
        assert_eq!(block.offsets, block_clamped.offsets);
        assert_ne!(block.data.as_ptr(), block_clamped.data.as_ptr());
        let mut iter = BlockIterator::new_ascending(block_clamped);
        assert_iterator(&mut iter, case.entries).await;
    }

    #[test]
    fn test_estimate_encoded_size() {
        // Test with zero entries and blocks
        assert_eq!(Block::estimate_encoded_size(0, 0, 0), 0);

        // Test with one entry and one block
        let entry_size = 100;
        let expected_size = entry_size + 2 + 4; // entry_size + offset + checksum
        assert_eq!(
            Block::estimate_encoded_size(1, entry_size, 1),
            expected_size
        );

        // Test with multiple entries and one block
        let num_entries = 5;
        let total_entry_size = entry_size * num_entries;
        let expected_size = total_entry_size + (2 * num_entries) + 4; // entries + offsets + checksum
        assert_eq!(
            Block::estimate_encoded_size(num_entries, total_entry_size, 1),
            expected_size
        );

        // Test with multiple entries and multiple blocks
        let num_blocks = 3;
        let expected_size = total_entry_size + (2 * num_entries) + (4 * num_blocks);
        assert_eq!(
            Block::estimate_encoded_size(num_entries, total_entry_size, num_blocks),
            expected_size
        );

        // Test with large numbers（assume 20GB and every block 4kb with 200 entries）
        let large_entry_size = 20 * 1024 * 1024 * 1024; // 20GB
        let num_entries = 200;
        let block_size = 4 * 1024; // 4KB
        let num_blocks = usize::div_ceil(large_entry_size, block_size);
        let expected_size = large_entry_size + (2 * num_entries) + (4 * num_blocks);
        assert_eq!(
            Block::estimate_encoded_size(num_entries, large_entry_size, num_blocks),
            expected_size
        );
    }
}
