use crate::{
    block::Block,
    block_iterator::BlockIterator,
    iter::KeyValueIterator,
    tablestore::{SSTableHandle, TableStore},
    types::KeyValueDeletable,
};
use object_store::path::Path;

struct SstIterator {
    table: SSTableHandle,
    current_iter: Option<BlockIterator<Block>>,
    next_block_idx: usize,
    table_store: TableStore,
    root_path: Path,
    sub_path: String,
}

impl SstIterator {
    #[allow(dead_code)] // will be used in #8
    fn new(
        table: SSTableHandle,
        table_store: TableStore,
        root_path: Path,
        sub_path: String,
    ) -> Self {
        Self {
            table,
            current_iter: None,
            next_block_idx: 0,
            table_store,
            root_path,
            sub_path,
        }
    }
}

impl KeyValueIterator for SstIterator {
    async fn next_entry(
        &mut self,
    ) -> Result<Option<KeyValueDeletable>, crate::error::SlateDBError> {
        loop {
            let current_iter = if let Some(current_iter) = self.current_iter.as_mut() {
                current_iter
            } else {
                if self.next_block_idx >= self.table.info.borrow().block_meta().len() {
                    // No more blocks in the SST.
                    return Ok(None);
                }

                let block = self
                    .table_store
                    .read_block(
                        &self.root_path,
                        &self.sub_path,
                        &self.table,
                        self.next_block_idx,
                    )
                    .await?;
                self.next_block_idx += 1;
                self.current_iter
                    .insert(BlockIterator::from_first_key(block))
            };

            let kv = current_iter.next_entry().await?;

            match kv {
                Some(kv) => return Ok(Some(kv)),
                None => {
                    self.current_iter = None;
                    // We have exhausted the current block, but not necessarily the entire SST,
                    // so we fall back to the top to check if we have more blocks to read.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SsTableFormat;
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_one_block_sst_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = TableStore::new(object_store, format);
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        builder.add(b"key3", Some(b"value3")).unwrap();
        builder.add(b"key4", Some(b"value4")).unwrap();
        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&root_path, &String::from("wal"), 0, encoded)
            .await
            .unwrap();
        let sst_handle = table_store
            .open_sst(&root_path, &String::from("wal"), 0)
            .await
            .unwrap();
        assert_eq!(sst_handle.info.borrow().block_meta().len(), 1);

        let mut iter = SstIterator::new(sst_handle, table_store, root_path, String::from("wal"));
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key4".as_slice());
        assert_eq!(kv.value, b"value4".as_slice());
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_many_block_sst_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = TableStore::new(object_store, format);
        let mut builder = table_store.table_builder();

        for i in 0..1000 {
            builder
                .add(
                    format!("key{}", i).as_bytes(),
                    Some(format!("value{}", i).as_bytes()),
                )
                .unwrap();
        }

        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&root_path, &String::from("wal"), 0, encoded)
            .await
            .unwrap();
        let sst_handle = table_store
            .open_sst(&root_path, &String::from("wal"), 0)
            .await
            .unwrap();
        assert_eq!(sst_handle.info.borrow().block_meta().len(), 6);

        let mut iter = SstIterator::new(sst_handle, table_store, root_path, String::from("wal"));
        for i in 0..1000 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, format!("key{}", i));
            assert_eq!(kv.value, format!("value{}", i));
        }

        let next = iter.next().await.unwrap();
        assert!(next.is_none());
    }
}
