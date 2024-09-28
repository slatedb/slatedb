use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::SlateDBError;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

/// "None" values are encoded by using the maximum u32 value as the value length.
pub(crate) const TOMBSTONE: u32 = u32::MAX;

pub(crate) struct Block {
    pub(crate) data: Bytes,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    #[rustfmt::skip]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(
            self.data.len()                   // data byte length
            + self.offsets.len() * SIZEOF_U16 // offsets as u16's
            + SIZEOF_U16, // number of offsets in the block
        );
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
}

pub struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    #[rustfmt::skip]
    fn estimated_size(&self) -> usize {
        SIZEOF_U16           // number of key-value pairs in the block
        + self.offsets.len() // offsets
        + self.data.len()    // key-value pairs
    }

    #[must_use]
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        // If adding the key-value pair would exceed the block size limit, don't add it.
        // (Unless the block is empty, in which case, allow the block to exceed the limit.)
        if self.estimated_size()
                + key.len()
                + value.map(|v| v.len()).unwrap_or_default() // None takes no space (besides u32)
                + SIZEOF_U16 * 2 // key size and offset size
                + SIZEOF_U32 // value size
                > self.block_size
            && !self.is_empty()
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        if let Some(value) = value {
            self.data.put_u32(value.len() as u32);
            self.data.put(value);
        } else {
            self.data.put_u32(TOMBSTONE);
        }

        true
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

    #[test]
    fn test_block() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(b"key1", Some(b"value1")));
        assert!(builder.add(b"key2", Some(b"value2")));
        let block = builder.build().unwrap();
        let encoded = block.encode();
        let decoded = Block::decode(encoded);
        assert_eq!(block.data, decoded.data);
        assert_eq!(block.offsets, decoded.offsets);
    }

    #[test]
    fn test_block_with_tombstone() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.add(b"key1", Some(b"value1")));
        assert!(builder.add(b"key2", None));
        assert!(builder.add(b"key3", Some(b"value3")));
        let block = builder.build().unwrap();
        let encoded = block.encode();
        let _decoded = Block::decode(encoded);
    }
}
