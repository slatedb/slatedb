use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use object_store::{ObjectStore};

use crate::block::{BlockBuilder, BlockMeta};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SsTableInfo {
  pub(crate) id: usize,
  pub(crate) first_key: Bytes,
  // todo: we probably dont want to keep this here, and instead store this in block cache
  //       and load it from there (ditto for bloom filter when that's implemented)
  pub(crate) block_meta: Vec<BlockMeta>,
  pub(crate) block_meta_offset: usize,
}

impl SsTableInfo {
  pub(crate) fn decode(id: usize, bytes: &Bytes) -> SsTableInfo {
    // TODO Read the last 4 bytes to get the meta offset. Then read the metadata.
    // TODO Optimization: Try and guess the block metadata size and the last 4 bytes in one fetch.
    //      (A missed guess would require a secnod fetch to get the rest of the metadata block.)
    // TODO: return an error if the buffer doesn't have enough data
    let len = bytes.len();
    let block_meta_offset = (&bytes[len - 4..]).get_u32() as usize;

    // Read the block meta
    let raw_meta = &bytes[block_meta_offset..len - 4].to_vec();
    let block_meta = BlockMeta::decode_block_meta(&raw_meta[..]);

    SsTableInfo {
      id,
      first_key: block_meta.first().unwrap().first_key.clone(),
      block_meta,
      block_meta_offset,
    }
  }
}

pub struct EncodedSsTable {
  pub(crate) info: SsTableInfo,
  pub(crate) raw: Bytes,
}

/// Builds an SSTable from key-value pairs.
pub struct EncodedSsTableBuilder {
  builder: BlockBuilder,
  first_key: Vec<u8>,
  block_meta: Vec<BlockMeta>,
  data: Vec<u8>,
  block_size: usize,
}

impl EncodedSsTableBuilder {
  /// Create a builder based on target block size.
  pub fn new(block_size: usize) -> Self {
    Self {
      data: Vec::new(),
      block_meta: Vec::new(),
      first_key: Vec::new(),
      block_size,
      builder: BlockBuilder::new(block_size),
    }
  }

  pub fn add(&mut self, key: &[u8], value: &[u8]) {
    if self.first_key.is_empty() {
      self.first_key = key.to_vec();
    }

    if !self.builder.add(key, value) {
      // Create a new block builder and append block data
      self.finish_block();

      // New block must always accept the first KV pair
      assert!(self.builder.add(key, value));
      self.first_key = key.to_vec();
    }
  }

  pub fn estimated_size(&self) -> usize {
    self.data.len()
  }

  fn finish_block(&mut self) {
    let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
    let encoded_block = builder.build().encode();
    self.block_meta.push(BlockMeta {
      offset: self.data.len(),
      first_key: std::mem::take(&mut self.first_key).into(),
    });
    let checksum = crc32fast::hash(&encoded_block);
    self.data.extend(encoded_block);
    self.data.put_u32(checksum);
  }

  pub fn build(
    mut self,
    id: usize,
  ) -> EncodedSsTable {
    self.finish_block();
    let mut buf = self.data;
    let meta_offset = buf.len();
    BlockMeta::encode_block_meta(&self.block_meta, &mut buf);
    buf.put_u32(meta_offset as u32);
    let mut first_key = Bytes::new();
    if let Some(first_block_meta) = self.block_meta.first() {
      first_key = first_block_meta.first_key.clone();
    }
    EncodedSsTable {
      info: SsTableInfo {
        id,
        first_key,
        block_meta: self.block_meta,
        block_meta_offset: meta_offset,
      },
      raw: Bytes::from(buf),
    }
  }
}


#[cfg(test)]
mod tests {
  use object_store::memory::InMemory;
  use tokio::runtime::Runtime;
  use crate::tablestore::TableStore;

  use super::*;

  #[test]
  fn test_sstable() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
      let table_store = TableStore::new(object_store);
      let mut builder = EncodedSsTableBuilder::new(4096);
      builder.add(b"key1", b"value1");
      builder.add(b"key2", b"value2");
      let encoded = builder.build(0);
      table_store.write_sst(&encoded).await;
      let sst_info = table_store.open_sst(0).await;
      assert_eq!(encoded.info, sst_info);
    });
  }
}
