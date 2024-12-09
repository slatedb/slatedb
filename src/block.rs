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
    use super::*;
    use crate::test_utils::{gen_attrs, gen_empty_attrs};

    #[test]
    fn test_block() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add_value(b"key1", b"value1", gen_empty_attrs()));
        assert!(builder.add_value(b"key1", b"value1", gen_empty_attrs()));
        assert!(builder.add_value(b"key2", b"value2", gen_empty_attrs()));
        let block = builder.build().unwrap();
        let encoded = block.encode();
        let decoded = Block::decode(encoded);
        assert_eq!(block.data, decoded.data);
        assert_eq!(block.offsets, decoded.offsets);
    }

    #[test]
    fn test_block_with_tombstone() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add_value(b"key1", b"value1", gen_empty_attrs()));
        assert!(builder.add_tombstone(b"key2", gen_empty_attrs()));
        assert!(builder.add_value(b"key3", b"value3", gen_empty_attrs()));
        let block = builder.build().unwrap();
        let encoded = block.encode();
        let _decoded = Block::decode(encoded);
    }

    #[test]
    fn test_block_size() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add_value(b"key1", b"value1", gen_attrs(1)));
        assert!(builder.add_value(b"key2", b"value2", gen_attrs(1)));
        let block = builder.build().unwrap();
        assert_eq!(73, block.size());

        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add_value(b"key1", b"value1", gen_empty_attrs()));
        assert!(builder.add_value(b"key2", b"value2", gen_empty_attrs()));
        let block = builder.build().unwrap();
        assert_eq!(57, block.size());
    }

    #[test]
    fn test_prefix_computing() {
        assert_eq!(compute_prefix(b"1", b"11"), 1);
        assert_eq!(compute_prefix(b"222", b"111"), 0);
        assert_eq!(compute_prefix(b"1234567", b"123456789"), 7);
    }
}
