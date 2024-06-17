use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::filter::{BloomFilter, BloomFilterBuilder};
use crate::flatbuffer_types::{
    BlockMeta, BlockMetaArgs, SsTableInfo, SsTableInfoArgs, SsTableInfoOwned,
};
use crate::{block::BlockBuilder, error::SlateDBError};
use bytes::{Buf, BufMut, Bytes};
use flatbuffers::DefaultAllocator;
use std::sync::Arc;

pub(crate) struct SsTableFormat {
    block_size: usize,
    min_filter_keys: u32,
}

impl SsTableFormat {
    pub fn new(block_size: usize, min_filter_keys: u32) -> Self {
        Self {
            block_size,
            min_filter_keys,
        }
    }

    pub(crate) async fn read_info(
        &self,
        obj: &impl ReadOnlyBlob,
    ) -> Result<SsTableInfoOwned, SlateDBError> {
        let len = obj.len().await?;
        if len <= 4 {
            return Err(SlateDBError::EmptySSTable);
        }
        // Get the size of the metadata
        let sst_metadata_offset_range = (len - 4)..len;
        let sst_metadata_offset =
            obj.read_range(sst_metadata_offset_range).await?.get_u32() as usize;
        // Get the metadata. Last 4 bytes are the offset of SsTableInfo
        let sst_metadata_range = sst_metadata_offset..len - 4;
        let sst_metadata_bytes = obj.read_range(sst_metadata_range).await?;
        SsTableInfoOwned::decode(sst_metadata_bytes)
    }

    pub(crate) async fn read_filter(
        &self,
        info: &SsTableInfoOwned,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        let mut filter = None;
        let handle = info.borrow();
        if handle.filter_len() > 0 {
            let filter_end = handle.filter_offset() + handle.filter_len();
            let filter_offset_range = handle.filter_offset() as usize..filter_end as usize;
            let filter_bytes = obj.read_range(filter_offset_range).await?;
            filter = Some(Arc::new(BloomFilter::decode(&filter_bytes)));
        }
        Ok(filter)
    }

    pub(crate) async fn read_block(
        &self,
        info: &SsTableInfoOwned,
        block: usize,
        obj: &impl ReadOnlyBlob,
    ) -> Result<Block, SlateDBError> {
        let handle = &info.borrow();
        let block_meta = handle.block_meta().get(block);
        let mut end = handle.filter_offset();
        if block < handle.block_meta().len() - 1 {
            let next_block_meta = handle.block_meta().get(block + 1);
            end = next_block_meta.offset();
        }
        // account for checksum
        end -= 4;
        let bytes: Bytes = obj
            .read_range(block_meta.offset() as usize..end as usize)
            .await?;
        Ok(Block::decode(&bytes))
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        EncodedSsTableBuilder::new(self.block_size, self.min_filter_keys)
    }
}

impl SsTableInfoOwned {
    fn encode(info: &SsTableInfoOwned, buf: &mut Vec<u8>) {
        buf.extend_from_slice(info.data());
        buf.put_u32(crc32fast::hash(info.data()));
    }

    pub(crate) fn decode(raw_block_meta: Bytes) -> Result<SsTableInfoOwned, SlateDBError> {
        if raw_block_meta.len() <= 4 {
            return Err(SlateDBError::EmptyBlockMeta);
        }
        let data = raw_block_meta.slice(..raw_block_meta.len() - 4).clone();
        let checksum = raw_block_meta.slice(raw_block_meta.len() - 4..).get_u32();
        if checksum != crc32fast::hash(&data) {
            return Err(SlateDBError::ChecksumMismatch);
        }

        let info = SsTableInfoOwned::new(data)?;
        Ok(info)
    }
}

pub(crate) struct EncodedSsTable {
    pub(crate) info: SsTableInfoOwned,
    pub(crate) filter: Option<Arc<BloomFilter>>,
    pub(crate) raw: Bytes,
}

/// Builds an SSTable from key-value pairs.
pub(crate) struct EncodedSsTableBuilder<'a> {
    builder: BlockBuilder,
    sst_info_builder: flatbuffers::FlatBufferBuilder<'a, DefaultAllocator>,
    first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    sst_first_key: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    block_meta: Vec<flatbuffers::WIPOffset<BlockMeta<'a>>>,
    data: Vec<u8>,
    block_size: usize,
    min_filter_keys: u32,
    num_keys: u32,
    filter_builder: BloomFilterBuilder,
}

impl<'a> EncodedSsTableBuilder<'a> {
    /// Create a builder based on target block size.
    fn new(block_size: usize, min_filter_keys: u32) -> Self {
        Self {
            data: Vec::new(),
            block_meta: Vec::new(),
            first_key: None,
            sst_first_key: None,
            block_size,
            builder: BlockBuilder::new(block_size),
            min_filter_keys,
            num_keys: 0,
            filter_builder: BloomFilterBuilder::new(10),
            sst_info_builder: flatbuffers::FlatBufferBuilder::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SlateDBError> {
        self.num_keys += 1;

        if !self.builder.add(key, value) {
            // Create a new block builder and append block data
            self.finish_block()?;

            // New block must always accept the first KV pair
            assert!(self.builder.add(key, value));
            self.first_key = Some(self.sst_info_builder.create_vector(key));
        } else if self.sst_first_key.is_none() {
            self.sst_first_key = Some(self.sst_info_builder.create_vector(key));
            self.first_key = Some(self.sst_info_builder.create_vector(key));
        }

        self.filter_builder.add_key(key);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn finish_block(&mut self) -> Result<(), SlateDBError> {
        if self.builder.is_empty() {
            return Ok(());
        }

        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build()?.encode();

        let block_meta = BlockMeta::create(
            &mut self.sst_info_builder,
            &BlockMetaArgs {
                offset: self.data.len() as u64,
                first_key: self.first_key,
            },
        );

        self.block_meta.push(block_meta);
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
        Ok(())
    }

    pub fn build(mut self) -> Result<EncodedSsTable, SlateDBError> {
        self.finish_block()?;
        let mut buf = self.data;
        let mut maybe_filter = None;
        let mut filter_len = 0;
        let filter_offset = buf.len();
        if self.num_keys >= self.min_filter_keys {
            let filter = Arc::new(self.filter_builder.build());
            let encoded_filter = filter.encode();
            filter_len = encoded_filter.len();
            buf.put(encoded_filter);
            maybe_filter = Some(filter);
        }

        let meta_offset = buf.len();
        let vector = self.sst_info_builder.create_vector(&self.block_meta);
        let info_wip_offset = SsTableInfo::create(
            &mut self.sst_info_builder,
            &SsTableInfoArgs {
                first_key: self.sst_first_key,
                block_meta: Some(vector),
                filter_offset: filter_offset as u64,
                filter_len: filter_len as u64,
            },
        );

        self.sst_info_builder.finish(info_wip_offset, None);
        let info =
            SsTableInfoOwned::new(Bytes::from(self.sst_info_builder.finished_data().to_vec()))?;

        SsTableInfoOwned::encode(&info, &mut buf);

        // write the metadata offset at the end of the file. FlatBuffer internal representation is not intended to be used directly.
        buf.put_u32(meta_offset as u32);
        Ok(EncodedSsTable {
            info,
            filter: maybe_filter,
            raw: Bytes::from(buf),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::tablestore::{SsTableId, TableStore};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_sstable() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 0);
        let table_store = TableStore::new(object_store, format, root_path);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();

        // write sst and validate that the handle returned has the correct content.
        let sst_handle = table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        assert_eq!(encoded_info, sst_handle.info);
        let sst_info = sst_handle.info.borrow();
        assert_eq!(1, sst_info.block_meta().len());
        assert_eq!(
            b"key1",
            sst_info.first_key().unwrap().bytes(),
            "first key in sst info should be correct"
        );
        assert_eq!(
            b"key1",
            sst_info.block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct"
        );

        // construct sst info from the raw bytes and validate that it matches the original info.
        let sst_handle_from_store = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle_from_store.info);
        let sst_info_from_store = sst_handle_from_store.info.borrow();
        assert_eq!(1, sst_info_from_store.block_meta().len());
        assert_eq!(
            b"key1",
            sst_info_from_store.first_key().unwrap().bytes(),
            "first key in sst info should be correct after reading from store"
        );
        assert_eq!(
            b"key1",
            sst_info_from_store.block_meta().get(0).first_key().bytes(),
            "first key in block meta should be correct after reading from store"
        );
    }

    #[tokio::test]
    async fn test_sstable_no_filter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = TableStore::new(object_store, format, root_path);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        let encoded = builder.build().unwrap();
        let encoded_info = encoded.info.clone();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(encoded_info, sst_handle.info);
        let handle = sst_handle.info.borrow();
        assert_eq!(handle.filter_len(), 0);
    }
}
