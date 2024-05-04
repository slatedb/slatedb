use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::{
    block::{BlockBuilder, BlockMeta},
    error::SlateDBError,
};
use bytes::{Buf, BufMut, Bytes};

pub(crate) struct SsTableFormat {
    block_size: usize,
}

impl SsTableFormat {
    pub fn new(block_size: usize) -> Self {
        Self { block_size }
    }

    pub(crate) async fn read_info(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableInfo, SlateDBError> {
        let len = obj.len().await?;
        if len <= 4 {
            return Err(SlateDBError::EmptySSTable);
        }
        // Get the size of the metadata
        let sst_metadata_offset_range = (len - 4)..len;
        let sst_metadata_offset =
            obj.read_range(sst_metadata_offset_range).await?.get_u32() as usize;
        // Get the metadata
        let sst_metadata_range = sst_metadata_offset..len;
        let mut sst_metadata_bytes = obj.read_range(sst_metadata_range).await?;
        SsTableInfo::decode(&mut sst_metadata_bytes)
    }

    pub(crate) async fn read_block(
        &self,
        info: &SsTableInfo,
        block: usize,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Block, SlateDBError> {
        // todo: range read
        let bytes = obj.read().await?;
        let block_meta = &info.block_meta[block];
        let mut end = info.block_meta_offset;
        if block < info.block_meta.len() - 1 {
            let next_block_meta = &info.block_meta[block + 1];
            end = next_block_meta.offset;
        }
        // account for checksum
        end -= 4;
        let raw_block = bytes.slice(block_meta.offset..end);
        Ok(Block::decode(raw_block.as_ref()))
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        EncodedSsTableBuilder::new(self.block_size)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SsTableInfo {
    pub(crate) first_key: Bytes,
    // todo: we probably dont want to keep this here, and instead store this in block cache
    //       and load it from there
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
}

impl SsTableInfo {
    fn encode(info: &SsTableInfo, buf: &mut Vec<u8>) {
        BlockMeta::encode_block_meta(&info.block_meta, buf);
        buf.put_u32(info.block_meta_offset as u32);
    }

    pub(crate) fn decode(raw_block_meta: &mut Bytes) -> Result<SsTableInfo, SlateDBError> {
        if raw_block_meta.len() <= 4 {
            return Err(SlateDBError::EmptyBlockMeta);
        }
        // Read the block meta
        let block_meta = BlockMeta::decode_block_meta(raw_block_meta)?;
        let block_meta_offset = raw_block_meta.get_u32() as usize;
        Ok(SsTableInfo {
            first_key: block_meta
                .first()
                .ok_or(SlateDBError::EmptyBlockMeta)?
                .first_key
                .clone(),
            block_meta,
            block_meta_offset,
        })
    }
}

pub(crate) struct EncodedSsTable {
    pub(crate) info: SsTableInfo,
    pub(crate) raw: Bytes,
}

/// Builds an SSTable from key-value pairs.
pub(crate) struct EncodedSsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    block_meta: Vec<BlockMeta>,
    data: Vec<u8>,
    block_size: usize,
    num_keys: u32,
}

impl EncodedSsTableBuilder {
    /// Create a builder based on target block size.
    fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            block_meta: Vec::new(),
            first_key: Vec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
            num_keys: 0,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), SlateDBError> {
        self.num_keys += 1;

        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

        if !self.builder.add(key, value) {
            // Create a new block builder and append block data
            self.finish_block()?;

            // New block must always accept the first KV pair
            assert!(self.builder.add(key, value));
            self.first_key = key.to_vec();
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn finish_block(&mut self) -> Result<(), SlateDBError> {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build()?.encode();
        self.block_meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into(),
        });
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
        Ok(())
    }

    pub fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        self.finish_block()?;
        let mut buf = self.data;
        let mut first_key = Bytes::new();
        if let Some(first_block_meta) = self.block_meta.first() {
            first_key = first_block_meta.first_key.clone();
        }
        let meta_offset = buf.len();
        let info = SsTableInfo {
            first_key,
            block_meta: self.block_meta,
            block_meta_offset: meta_offset,
        };
        // make sure to encode the info at the end so that the block meta offset is last
        SsTableInfo::encode(&info, &mut buf);
        Ok(EncodedSsTable {
            info,
            raw: Bytes::from(buf),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::tablestore::TableStore;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_sstable() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096);
        let table_store = TableStore::new(object_store, format);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", b"value1").unwrap();
        builder.add(b"key2", b"value2").unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store.write_sst(0, encoded).await.unwrap();
        let sst_handle = table_store.open_sst(0).await.unwrap();
        assert_eq!(encoded_info, sst_handle.info);
    }
}
