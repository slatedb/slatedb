use crate::db_state::{SSTableHandle, SortedRun};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
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
    pub(crate) async fn new_from_key(
        sorted_run: &'a SortedRun,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Result<Self, SlateDBError> {
        assert!(!sorted_run.ssts.is_empty());
        Self::new_opts(
            sorted_run,
            Some(key),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
        )
        .await
    }

    pub(crate) async fn new_spawn(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            sorted_run,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            true,
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn new(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            sorted_run,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
        )
        .await
    }

    pub(crate) async fn new_opts(
        sorted_run: &'a SortedRun,
        from_key: Option<&'a [u8]>,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
    ) -> Result<Self, SlateDBError> {
        let mut sorted_run_iter = from_key
            .map(|from_key| Self::find_iter_from_key(from_key, sorted_run))
            .unwrap_or(sorted_run.ssts.iter());
        let current_iter = match sorted_run_iter.next() {
            None => None,
            Some(h) => Some(
                SstIterator::new_opts(
                    h,
                    from_key,
                    table_store.clone(),
                    max_fetch_tasks,
                    blocks_to_fetch,
                    spawn,
                )
                .await?,
            ),
        };
        Ok(Self {
            current_iter,
            sorted_run_iter,
            table_store,
            blocks_to_fetch: max_fetch_tasks,
            blocks_to_buffer: blocks_to_fetch,
        })
    }

    pub(crate) fn find_iter_from_key(
        key: &[u8],
        sorted_run: &'a SortedRun,
    ) -> Iter<'a, SSTableHandle> {
        match sorted_run.find_sst_with_range_covering_key_idx(key) {
            None => sorted_run.ssts.iter(),
            Some(idx) => sorted_run.ssts[idx..].iter(),
        }
    }
}

impl<'a> KeyValueIterator for SortedRunIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
        loop {
            if let Some(iter) = &mut self.current_iter {
                if let Some(kv) = iter.next_entry().await? {
                    return Ok(Some(kv));
                }
                self.current_iter = match self.sorted_run_iter.next() {
                    None => None,
                    Some(h) => Some(
                        SstIterator::new(
                            h,
                            self.table_store.clone(),
                            self.blocks_to_fetch,
                            self.blocks_to_buffer,
                        )
                        .await?,
                    ),
                };
            } else {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::test_utils::{assert_kv, OrderedBytesGenerator};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_one_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3, None);
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

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1)
            .await
            .unwrap();

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
        let format = SsTableFormat::new(4096, 3, None);
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

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1)
            .await
            .unwrap();

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
    async fn test_sr_iter_from_key() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3, None);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut test_case_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut test_case_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        for i in 0..30 {
            let mut expected_key_gen = test_case_key_gen.clone();
            let mut expected_val_gen = test_case_val_gen.clone();
            let from_key = test_case_key_gen.next();
            _ = test_case_val_gen.next();
            let mut iter =
                SortedRunIterator::new_from_key(&sr, from_key.as_ref(), table_store.clone(), 1, 1)
                    .await
                    .unwrap();
            for _ in 0..30 - i {
                assert_kv(
                    &iter.next().await.unwrap().unwrap(),
                    expected_key_gen.next().as_ref(),
                    expected_val_gen.next().as_ref(),
                );
            }
            assert!(iter.next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_lower_than_range() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3, None);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut expected_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_from_key(&sr, &[b'a', 10], table_store.clone(), 1, 1)
            .await
            .unwrap();

        for _ in 0..30 {
            assert_kv(
                &iter.next().await.unwrap().unwrap(),
                expected_key_gen.next().as_ref(),
                expected_val_gen.next().as_ref(),
            );
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_higher_than_range() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3, None);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_from_key(&sr, &[b'z', 30], table_store.clone(), 1, 1)
            .await
            .unwrap();

        assert!(iter.next().await.unwrap().is_none());
    }

    async fn build_sr_with_ssts(
        table_store: Arc<TableStore>,
        n: usize,
        keys_per_sst: usize,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> SortedRun {
        let mut ssts = Vec::<SSTableHandle>::new();
        for _ in 0..n {
            let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
            for _ in 0..keys_per_sst {
                writer
                    .add(key_gen.next().as_ref(), Some(val_gen.next().as_ref()))
                    .await
                    .unwrap();
            }
            ssts.push(writer.close().await.unwrap());
        }
        SortedRun { id: 0, ssts }
    }
}
