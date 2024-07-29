use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::sst_iter::SstIterator;
use crate::tablestore::{SSTableHandle, TableStore};
use crate::types::KeyValueDeletable;
use std::slice::Iter;
use std::sync::Arc;

pub(crate) struct SortedRunIterator<'a> {
    current_iter: Option<SstIterator<'a>>,
    sorted_run_iter: Iter<'a, SSTableHandle>,
    table_store: Arc<TableStore>,
    blocks_to_fetch: usize,
    blocks_to_buffer: usize,
}

impl<'a> SortedRunIterator<'a> {
    pub(crate) fn new_spawn(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
    ) -> Self {
        let mut sorted_run_iter = sorted_run.ssts.iter();
        let current_iter = sorted_run_iter.next().map(|h| {
            SstIterator::new_spawn(
                h,
                table_store.clone(),
                max_fetch_tasks,
                blocks_to_fetch,
                spawn,
            )
        });
        Self {
            current_iter,
            sorted_run_iter,
            table_store,
            blocks_to_fetch: max_fetch_tasks,
            blocks_to_buffer: blocks_to_fetch,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Self {
        Self::new_spawn(
            sorted_run,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
        )
    }
}

impl<'a> KeyValueIterator for SortedRunIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
        loop {
            if let Some(iter) = &mut self.current_iter {
                if let Some(kv) = iter.next_entry().await? {
                    return Ok(Some(kv));
                }
                self.current_iter = self.sorted_run_iter.next().map(|h| {
                    SstIterator::new(
                        h,
                        self.table_store.clone(),
                        self.blocks_to_fetch,
                        self.blocks_to_buffer,
                    )
                });
            } else {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SsTableFormat;
    use crate::tablestore::SsTableId;
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_one_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        builder.add(b"key3", Some(b"value3")).unwrap();
        let encoded = builder.build().unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let handle = table_store.write_sst(&id, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle],
        };

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1);

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_many_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        let encoded = builder.build().unwrap();
        let id1 = SsTableId::Compacted(Ulid::new());
        let handle1 = table_store.write_sst(&id1, encoded).await.unwrap();
        let mut builder = table_store.table_builder();
        builder.add(b"key3", Some(b"value3")).unwrap();
        let encoded = builder.build().unwrap();
        let id2 = SsTableId::Compacted(Ulid::new());
        let handle2 = table_store.write_sst(&id2, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle1, handle2],
        };

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1);

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }
}
