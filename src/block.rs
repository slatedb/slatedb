use crate::error::SlateDBError;
use crate::row_codec::{SstRowCodecV0, SstRowEntry};
use crate::types::RowEntry;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

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

    /// Returns the size of the block in bytes.
    #[rustfmt::skip]
    pub(crate) fn size(&self) -> usize {
        self.data.len()                   // data byte length
        + self.offsets.len() * SIZEOF_U16 // offsets as u16's
        + SIZEOF_U16 // number of offsets in the block
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
    use crate::test_utils::{assert_debug_snapshot, gen_attrs, gen_empty_attrs};

    #[derive(Debug)]
    struct BlockTestCase {
        name: &'static str,
        entries: Vec<(&'static [u8], Option<&'static [u8]>)>, // (key, value) with None for tombstone
        expected_size: Option<usize>,                         // Expected size of the block
        use_timestamped_attrs: bool, // Flag to use attributes with a timestamp
        timestamp: Option<i64>,      // timestamp value
    }

    fn build_block(test_case: &BlockTestCase) -> Block {
        let mut builder = BlockBuilder::new(4096);

        for (key, value) in &test_case.entries {
            let attrs = if test_case.use_timestamped_attrs {
                gen_attrs(test_case.timestamp.unwrap_or(1)) // Use timestamp value or default to 1
            } else {
                gen_empty_attrs()
            };

            match value {
                Some(v) => assert!(builder.add_value(key, v, attrs)),
                None => assert!(builder.add_tombstone(key, gen_empty_attrs())),
            }
        }
        builder.build().expect("Failed to build block")
    }

    #[rstest]
    #[case(BlockTestCase {
        name: "test_block",
        entries: vec![
            (b"key1", Some(b"value1")),
            (b"key1", Some(b"value1")),
            (b"key2", Some(b"value2")),
        ],
        expected_size: None,
        use_timestamped_attrs: false,
        timestamp: None,
    })]
    fn test_block(#[case] test_case: BlockTestCase) {
        let block = build_block(&test_case);
        let encoded = block.encode();
        let decoded = Block::decode(encoded);
        assert_eq!(block.data, decoded.data);
        assert_eq!(block.offsets, decoded.offsets);
        assert_debug_snapshot!(test_case.name, (block.data, block.offsets));
    }

    #[rstest]
    #[case(BlockTestCase {
        name: "block_with_tombstone",
        entries: vec![
            (b"key1", Some(b"value1")),
            (b"key2", None),
            (b"key3", Some(b"value3")),
        ],
        expected_size: None,
        use_timestamped_attrs: false,
        timestamp: None,
    })]
    fn test_block_with_tombstone(#[case] test_case: BlockTestCase) {
        let block = build_block(&test_case);
        let encoded = block.encode();
        let _decoded = Block::decode(encoded);
        assert_debug_snapshot!(test_case.name, (block.data, block.offsets));
    }

    #[rstest]
    #[case(BlockTestCase {
        name: "block_size_with_attrs",
        entries: vec![
            (b"key1", Some(b"value1")),
            (b"key2", Some(b"value2")),
        ],
        expected_size: Some(73),
        use_timestamped_attrs: true,
        timestamp: Some(1),
    })]
    fn test_block_size(#[case] test_case: BlockTestCase) {
        let block = build_block(&test_case);
        if let Some(expected_size) = test_case.expected_size {
            assert_eq!(expected_size, block.size());
        }
        assert_debug_snapshot!(test_case.name, (block.size(), block.data, block.offsets));
    }

    #[rstest]
    #[case(BlockTestCase {
        name: "block_size_with_empty_attrs",
        entries: vec![
            (b"key1", Some(b"value1")),
            (b"key2", Some(b"value2")),
        ],
        expected_size: Some(57),
        use_timestamped_attrs: false,
        timestamp: None,
    })]
    fn test_block_size_with_empty_attrs(#[case] test_case: BlockTestCase) {
        let block = build_block(&test_case);
        if let Some(expected_size) = test_case.expected_size {
            assert_eq!(expected_size, block.size());
        }
        assert_debug_snapshot!(test_case.name, (block.size(), block.data, block.offsets));
    }

    #[test]
    fn test_prefix_computing() {
        assert_eq!(compute_prefix(b"1", b"11"), 1);
        assert_eq!(compute_prefix(b"222", b"111"), 0);
        assert_eq!(compute_prefix(b"1234567", b"123456789"), 7);
    }
}
