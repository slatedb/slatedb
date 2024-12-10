use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::iter::{KeyValueIterator, SeekToKey};
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use bytes::Bytes;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) struct SortedRunIterator<'a, H: AsRef<SsTableHandle> = &'a SsTableHandle> {
    current_iter: Option<SstIterator<'a, H>>,
    sorted_run_iter: SsTableHandleIter<'a, H>,
    table_store: Arc<TableStore>,
    blocks_to_fetch: usize,
    blocks_to_buffer: usize,
    cache_blocks: bool,
}

pub(crate) struct SsTableHandleIter<'a, H: AsRef<SsTableHandle> = &'a SsTableHandle> {
    vec: VecDeque<H>,
    _marker: PhantomData<&'a H>,
}

impl<H: AsRef<SsTableHandle>> SsTableHandleIter<'_, H> {
    fn new(vec: VecDeque<H>) -> Self {
        Self {
            vec,
            _marker: PhantomData,
        }
    }

    pub(crate) fn next(&mut self) -> Option<H> {
        self.vec.pop_front()
    }

    pub(crate) fn peek(&self) -> Option<&H> {
        self.vec.front()
    }
}

impl<'a> SortedRunIterator<'a, Box<SsTableHandle>> {
    pub(crate) async fn new_from_range(
        sorted_run: SortedRun,
        range: BytesRange,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        assert!(!sorted_run.ssts.is_empty());
        let ssts = sorted_run
            .tables_covering_range(&range)
            .into_iter()
            .map(|table| Box::new(table.clone()))
            .collect();
        let sorted_run_iter = SsTableHandleIter::new(ssts);
        let start_key = range.start_bound_opt();

        Self::new_from_iter(
            sorted_run_iter,
            start_key.clone(),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            true,
            cache_blocks,
        )
        .await
    }
}

impl<'a> SortedRunIterator<'a, &'a SsTableHandle> {
    pub(crate) async fn new_from_key(
        sorted_run: &'a SortedRun,
        key: Bytes,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        assert!(!sorted_run.ssts.is_empty());
        Self::new_opts(
            sorted_run,
            Some(key),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
            cache_blocks,
        )
        .await
    }

    pub(crate) async fn new_spawn(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            sorted_run,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            true,
            cache_blocks,
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn new(
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            sorted_run,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
            cache_blocks,
        )
        .await
    }

    pub(crate) async fn new_opts(
        sorted_run: &'a SortedRun,
        from_key: Option<Bytes>,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        let sorted_run_iter = Self::find_iter_from_key(from_key.clone(), sorted_run);
        Self::new_from_iter(
            sorted_run_iter,
            from_key,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            spawn,
            cache_blocks,
        )
        .await
    }

    fn find_iter_from_key(
        from_key: Option<Bytes>,
        sorted_run: &'a SortedRun,
    ) -> SsTableHandleIter<'a, &'a SsTableHandle> {
        let sorted_runs = from_key
            .and_then(|key| sorted_run.find_sst_with_range_covering_key_idx(key.as_ref()))
            .map(|idx| sorted_run.ssts[idx..].iter())
            .unwrap_or(sorted_run.ssts.iter())
            .collect();
        SsTableHandleIter::new(sorted_runs)
    }
}

impl<'a, H: AsRef<SsTableHandle>> SortedRunIterator<'a, H> {
    async fn new_from_iter(
        mut sorted_run_iter: SsTableHandleIter<'a, H>,
        from_key: Option<Bytes>,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        let current_iter = match sorted_run_iter.next() {
            None => None,
            Some(h) => Some(
                SstIterator::new_opts(
                    h,
                    BytesRange::with_start_key(from_key),
                    table_store.clone(),
                    max_fetch_tasks,
                    blocks_to_fetch,
                    spawn,
                    cache_blocks,
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
            cache_blocks,
        })
    }

    fn peek_next_table(&self) -> Option<&H> {
        self.sorted_run_iter.peek()
    }

    async fn advance_current_iter(&mut self) -> Result<(), SlateDBError> {
        self.current_iter = match self.sorted_run_iter.next() {
            None => None,
            Some(h) => Some(
                SstIterator::new(
                    h,
                    self.table_store.clone(),
                    self.blocks_to_fetch,
                    self.blocks_to_buffer,
                    self.cache_blocks,
                )
                .await?,
            ),
        };
        Ok(())
    }
}

impl<'a, H: AsRef<SsTableHandle>> KeyValueIterator for SortedRunIterator<'a, H> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            if let Some(iter) = &mut self.current_iter {
                if let Some(kv) = iter.next_entry().await? {
                    return Ok(Some(kv));
                } else {
                    self.advance_current_iter().await?;
                }
            } else {
                return Ok(None);
            }
        }
    }
}

impl<'a, H: AsRef<SsTableHandle>> SeekToKey for SortedRunIterator<'a, H> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(next_table) = self.peek_next_table() {
            let next_table_first_key = next_table.as_ref().info.first_key.as_ref();
            match next_table_first_key {
                Some(key) if key < next_key => self.advance_current_iter().await?,
                _ => break,
            }
        }
        if let Some(iter) = &mut self.current_iter {
            iter.seek(next_key).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::OrderedBytesGenerator;
    use crate::db_state::SsTableId;
    use crate::proptest_util;
    use crate::proptest_util::sample;
    use crate::sst::SsTableFormat;
    use crate::test_utils::{assert_kv, gen_attrs};

    use bytes::{BufMut, BytesMut};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use proptest::test_runner::TestRng;
    use rand::distributions::uniform::SampleRange;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_one_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_kv(b"key1", Some(b"value1"), gen_attrs(1))
            .unwrap();
        builder
            .add_kv(b"key2", Some(b"value2"), gen_attrs(2))
            .unwrap();
        builder
            .add_kv(b"key3", Some(b"value3"), gen_attrs(3))
            .unwrap();
        let encoded = builder.build().unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let handle = table_store.write_sst(&id, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle],
        };

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1, false)
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
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_kv(b"key1", Some(b"value1"), gen_attrs(1))
            .unwrap();
        builder
            .add_kv(b"key2", Some(b"value2"), gen_attrs(2))
            .unwrap();
        let encoded = builder.build().unwrap();
        let id1 = SsTableId::Compacted(Ulid::new());
        let handle1 = table_store.write_sst(&id1, encoded).await.unwrap();
        let mut builder = table_store.table_builder();
        builder
            .add_kv(b"key3", Some(b"value3"), gen_attrs(3))
            .unwrap();
        let encoded = builder.build().unwrap();
        let id2 = SsTableId::Compacted(Ulid::new());
        let handle2 = table_store.write_sst(&id2, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle1, handle2],
        };

        let mut iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1, false)
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
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
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
                SortedRunIterator::new_from_key(&sr, from_key, table_store.clone(), 1, 1, false)
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
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut expected_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_from_key(
            &sr,
            Bytes::from_static(&[b'a', 10]),
            table_store.clone(),
            1,
            1,
            false,
        )
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
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_from_key(
            &sr,
            Bytes::from_static(&[b'z', 30]),
            table_store.clone(),
            1,
            1,
            false,
        )
        .await
        .unwrap();

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_through_sorted_run() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            object_store,
            SsTableFormat::default(),
            root_path.clone(),
            None,
        ));

        let mut rng = proptest_util::rng::new_test_rng(None);
        let table = sample::table(&mut rng, 400, 10);
        let max_entries_per_sst = 20;
        let entries_per_sst = 1..max_entries_per_sst;
        let sr =
            build_sorted_run_from_table(&table, table_store.clone(), entries_per_sst, &mut rng)
                .await;
        let mut sr_iter = SortedRunIterator::new(&sr, table_store.clone(), 1, 1, false)
            .await
            .unwrap();

        let mut table_iter = table.iter();
        loop {
            let skip = rng.gen::<usize>() % (max_entries_per_sst * 2);
            let run = rng.gen::<usize>() % (max_entries_per_sst * 2);

            let Some((k, _)) = table_iter.nth(skip) else {
                break;
            };
            let seek_key = increment_length(k);
            sr_iter.seek(&seek_key).await.unwrap();

            for (key, value) in table_iter.by_ref().take(run) {
                let kv = sr_iter.next().await.unwrap().unwrap();
                assert_eq!(*key, kv.key);
                assert_eq!(*value, kv.value);
            }
        }
    }

    fn increment_length(b: &[u8]) -> Bytes {
        let mut buf = BytesMut::from(b);
        buf.put_u8(u8::MIN);
        buf.freeze()
    }

    async fn build_sorted_run_from_table<R: SampleRange<usize> + Clone>(
        table: &BTreeMap<Bytes, Bytes>,
        table_store: Arc<TableStore>,
        entries_per_sst: R,
        rng: &mut TestRng,
    ) -> SortedRun {
        let mut ssts = Vec::new();
        let mut entries = table.iter();
        loop {
            let sst_len = rng.gen_range(entries_per_sst.clone());
            let mut builder = table_store.table_builder();

            let sst_kvs: Vec<(&Bytes, &Bytes)> = entries.by_ref().take(sst_len).collect();
            if sst_kvs.is_empty() {
                break;
            }

            for (key, value) in sst_kvs {
                builder.add_kv(key, Some(value), gen_attrs(0)).unwrap();
            }

            let encoded = builder.build().unwrap();
            let id = SsTableId::Compacted(Ulid::new());
            let handle = table_store.write_sst(&id, encoded).await.unwrap();
            ssts.push(handle);
        }

        SortedRun { id: 0, ssts }
    }

    async fn build_sr_with_ssts(
        table_store: Arc<TableStore>,
        n: usize,
        keys_per_sst: usize,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> SortedRun {
        let mut ssts = Vec::<SsTableHandle>::new();
        for _ in 0..n {
            let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
            for _ in 0..keys_per_sst {
                let entry = RowEntry::new(key_gen.next(), Some(val_gen.next()), 0, None, None);
                writer.add(entry).await.unwrap();
            }
            ssts.push(writer.close().await.unwrap());
        }
        SortedRun { id: 0, ssts }
    }
}
