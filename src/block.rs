use bytes::{BufMut, Bytes, Buf, BytesMut};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

pub struct Block {
  pub(crate) data: Bytes,
  pub(crate) offsets: Vec<u16>,
}

impl Block {
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(&self.data);
    for offset in &self.offsets {
      buf.put_u16(*offset);
    }
    buf.put_u16(self.offsets.len() as u16);
    buf.into()
  }

  pub fn decode(data: &[u8]) -> Self {
    // Get number of elements in the block
    let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
    let data_end = data.len()
      - SIZEOF_U16 // Entry u16 length
      - entry_offsets_len * SIZEOF_U16; // Offset byte array length
    let offsets_raw = &data[data_end..data.len() - SIZEOF_U16]; // Entry u16
    let offsets = offsets_raw
      .chunks(SIZEOF_U16)
      .map(|mut x| x.get_u16())
      .collect();
    let data = Bytes::copy_from_slice(data[0..data_end].as_ref());
    Self { data, offsets }
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

  fn estimated_size(&self) -> usize {
    SIZEOF_U16 // number of key-value pairs in the block
    + self.offsets.len() // offsets
    + self.data.len() // key-value pairs
  }

  #[must_use]
  pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
    assert!(!key.is_empty(), "key must not be empty");
    // If adding the key-value pair would exceed the block size limit, don't add it.
    // (Unless the block is empty, in which case, allow the block to exceed the limit.)
    if self.estimated_size()
      + key.len()
      + value.len()
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
    self.data.put_u32(value.len() as u32);
    self.data.put(value);
    true
  }

  pub fn is_empty(&self) -> bool {
    self.offsets.is_empty()
  }

  pub fn build(self) -> Block {
    if self.is_empty() {
      panic!("Block should not be empty");
    }
    Block {
      data: Bytes::from(self.data),
      offsets: self.offsets,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockMeta {
  pub(crate) offset: usize,
  pub(crate) first_key: Bytes,
  pub(crate) last_key: Bytes,
}

impl BlockMeta {
  // Encode a vector of block metadatas into a buffer.
  pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
    let mut estimated_size = std::mem::size_of::<u32>(); // u32 = number of block metadatas
    for meta in block_meta {
      estimated_size += std::mem::size_of::<u32>();      // u32 = size of offset
      estimated_size += std::mem::size_of::<u16>();      // u16 = size of key length
      estimated_size += meta.first_key.len();
      estimated_size += std::mem::size_of::<u16>();      // u16 = size of key length
      estimated_size += meta.last_key.len();
    }
    estimated_size += std::mem::size_of::<u32>();        // u32 = checksum
    // Reserve the space to improve performance
    buf.reserve(estimated_size);
    let original_len = buf.len();
    buf.put_u32(block_meta.len() as u32);
    for meta in block_meta {
      buf.put_u32(meta.offset as u32);
      buf.put_u16(meta.first_key.len() as u16);
      buf.put(meta.first_key.as_ref());
      buf.put_u16(meta.last_key.len() as u16);
      buf.put(meta.last_key.as_ref());
    }
    buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
    assert_eq!(estimated_size, buf.len() - original_len);
  }

  /// Decode a vector of block metadatas from a buffer.
  pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
    let mut block_meta = Vec::new();
    let num = buf.get_u32() as usize;
    let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
    for _ in 0..num {
      let offset = buf.get_u32() as usize;
      let first_key_len = buf.get_u16() as usize;
      let first_key = buf.copy_to_bytes(first_key_len);
      let last_key_len = buf.get_u16() as usize;
      let last_key = buf.copy_to_bytes(last_key_len);
      block_meta.push(BlockMeta {
        offset,
        first_key,
        last_key,
      });
    }
    if buf.get_u32() != checksum {
      // TODO use anyhow to handle errors
      panic!("meta checksum mismatched");
    }
    block_meta
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_block() {
    let mut builder = BlockBuilder::new(4096);
    assert!(builder.add(b"key1", b"value1"));
    assert!(builder.add(b"key2", b"value2"));
    let block = builder.build();
    let encoded = block.encode();
    let decoded = Block::decode(&encoded);
    assert_eq!(block.data, decoded.data);
    assert_eq!(block.offsets, decoded.offsets);
  }

  #[test]
  fn test_block_meta() {
    let mut block_meta = Vec::new();
    block_meta.push(BlockMeta {
      offset: 0,
      first_key: Bytes::from("key1"),
      last_key: Bytes::from("key2"),
    });
    block_meta.push(BlockMeta {
      offset: 99,
      first_key: Bytes::from("key3"),
      last_key: Bytes::from("key4"),
    });
    let mut buf = Vec::new();
    BlockMeta::encode_block_meta(&block_meta, &mut buf);
    let decoded = BlockMeta::decode_block_meta(&buf);
    assert_eq!(block_meta, decoded);
  }
}