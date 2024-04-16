use std::sync::Arc;
use object_store::ObjectStore;
use object_store::path::Path;
use crate::block::{Block};
use crate::sst::{EncodedSsTable, SsTableInfo};

pub(crate) struct TableStore {
    object_store: Arc<dyn ObjectStore>,
}

impl TableStore {
    pub fn new(object_store: &Arc<dyn ObjectStore>) -> TableStore {
        TableStore{
            object_store: object_store.clone()
        }
    }

    pub async fn write_sst(&self, encoded_sst: &EncodedSsTable) {
        let result = self.object_store.put(&self.path(encoded_sst.info.id), encoded_sst.raw.clone()).await;
        if result.is_err() {
            panic!("put to store failed")
        }
    }

    // todo: wrap info in some handle object that cleans up stuff like open file handles when
    //       handle is cleaned up
    pub async fn open_sst(&self, id: usize) -> SsTableInfo {
        // Read the entire file into memory for now.
        let path = self.path(id);
        let file = self.object_store.get(&path).await.unwrap();
        let bytes = file.bytes().await.unwrap();
        SsTableInfo::decode(id, &bytes)
    }

    pub async fn read_block(&self, info: &SsTableInfo, block: usize) -> Block {
        let path = self.path(info.id);
        // todo: range read
        let file = self.object_store.get(&path).await.unwrap();
        let bytes = file.bytes().await.unwrap();
        let block_meta = &info.block_meta[block];
        let mut end = info.block_meta_offset;
        if block < info.block_meta.len() - 1 {
            let next_block_meta = &info.block_meta[block + 1];
            end = next_block_meta.offset;
        }
        // account for checksum
        end -= 4;
        let raw_block = bytes.slice(block_meta.offset..end);
        Block::decode(raw_block.as_ref())
    }

    fn path(&self, id: usize) -> Path {
        Path::from(format!("sst-{}", id))
    }
}