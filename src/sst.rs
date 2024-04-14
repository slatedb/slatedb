use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use object_store::{path::Path, ObjectStore};

use crate::block::{BlockBuilder, BlockMeta};

#[derive(Clone, Debug)]
pub struct SsTable {
  pub(crate) id: usize,
  pub(crate) path: Path,
  #[allow(dead_code)]
  object_store: Arc<dyn ObjectStore>,
  pub(crate) first_key: Bytes,
  block_meta: Vec<BlockMeta>,
}

// Have to manually implement this since object_store doesn't support comparison.
impl PartialEq for SsTable {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
      && self.path == other.path
      && self.first_key == other.first_key
      && self.block_meta == other.block_meta
  }
}

impl Eq for SsTable {}

impl SsTable {
  pub async fn open(
    id: usize,
    path: Path,
    object_store: Arc<dyn ObjectStore>,
  ) -> Self {
    // Read the entire file into memory for now.
    // TODO Read the last 4 bytes to get the meta offset. Then read the metadata.
    // TODO Optimization: Try and guess the block metadata size and the last 4 bytes in one fetch.
    //      (A missed guess would require a secnod fetch to get the rest of the metadata block.)
    let file = object_store.get(&path).await.unwrap();
    let bytes = file.bytes().await.unwrap();

    // Read the last 4 bytes to get the offset of the block meta
    let len = bytes.len();
    let block_meta_offset = (&bytes[len - 4..]).get_u32() as usize;

    // Read the block meta
    let raw_meta = &bytes[block_meta_offset..len - 4].to_vec();
    let block_meta = BlockMeta::decode_block_meta(&raw_meta[..]);

    Self {
      id,
      path,
      object_store,
      first_key: block_meta.first().unwrap().first_key.clone(),
      block_meta,
    }
  }
}

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
  builder: BlockBuilder,
  first_key: Vec<u8>,
  block_meta: Vec<BlockMeta>,
  data: Vec<u8>,
  block_size: usize,
}

impl SsTableBuilder {
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

  pub async fn build(
    mut self,
    id: usize,
    path: Path,
    object_store: Arc<dyn ObjectStore>,
  ) -> SsTable {
    self.finish_block();
    let mut buf = self.data;
    let meta_offset = buf.len();
    BlockMeta::encode_block_meta(&self.block_meta, &mut buf);
    buf.put_u32(meta_offset as u32);
    object_store.put(&path, Bytes::from(buf)).await.unwrap();
    SsTable {
      id,
      path,
      object_store,
      first_key: self.block_meta.first().unwrap().first_key.clone(),
      block_meta: self.block_meta,
    }
  }
}


#[cfg(test)]
mod tests {
  use object_store::memory::InMemory;
  use tokio::runtime::Runtime;

  use super::*;

  #[test]
  fn test_sstable() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let object_store = Arc::new(InMemory::new());
      let mut builder = SsTableBuilder::new(4096);
      builder.add(b"key1", b"value1");
      builder.add(b"key2", b"value2");
      let encoded = builder.build(0, Path::from("test"), object_store.clone()).await;
      let decoded = SsTable::open(0, Path::from("test"), object_store.clone()).await;
      assert_eq!(encoded, decoded);
    });
  }
}