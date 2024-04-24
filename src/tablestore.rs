use crate::block::Block;
use crate::error::SlateDBError;
use crate::sst::{EncodedSsTable, SsTableInfo};
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
}

impl TableStore {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> TableStore {
        TableStore {
            object_store: object_store.clone(),
        }
    }

    pub(crate) async fn write_sst(&self, encoded_sst: &EncodedSsTable) -> Result<(), SlateDBError> {
        self.object_store
            .put(&self.path(encoded_sst.info.id), encoded_sst.raw.clone())
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(())
    }

    // todo: wrap info in some handle object that cleans up stuff like open file handles when
    //       handle is cleaned up
    // todo: clean up the warning suppression when we start using open_sst outside tests
    #[allow(dead_code)]
    pub(crate) async fn open_sst(&self, id: usize) -> Result<SsTableInfo, SlateDBError> {
        // Read the entire file into memory for now.
        let path = self.path(id);
        let file = self
            .object_store
            .get(&path)
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        let bytes = file.bytes().await.map_err(SlateDBError::ObjectStoreError)?;
        SsTableInfo::decode(id, &bytes)
    }

    pub(crate) async fn read_block(
        &self,
        info: &SsTableInfo,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        let path = self.path(info.id);
        // todo: range read
        let file = self.object_store.get(&path).await?;
        let bytes = file.bytes().await.map_err(SlateDBError::ObjectStoreError)?;
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

    fn path(&self, id: usize) -> Path {
        Path::from(format!("sst-{}", id))
    }
}
