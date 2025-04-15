use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions, SstView};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

#[derive(Debug)]
enum SortedRunView<'a> {
    Owned(VecDeque<SsTableHandle>, BytesRange),
    Borrowed(
        VecDeque<&'a SsTableHandle>,
        (Bound<&'a [u8]>, Bound<&'a [u8]>),
    ),
}

impl<'a> SortedRunView<'a> {
    fn pop_sst(&mut self) -> Option<SstView<'a>> {
        match self {
            SortedRunView::Owned(tables, r) => tables
                .pop_front()
                .map(|table| SstView::Owned(table, r.clone())),
            SortedRunView::Borrowed(tables, r) => {
                tables.pop_front().map(|table| SstView::Borrowed(table, *r))
            }
        }
    }

    pub(crate) async fn build_next_iter(
        &mut self,
        table_store: Arc<TableStore>,
        sst_iterator_options: SstIteratorOptions,
    ) -> Result<Option<SstIterator<'a>>, SlateDBError> {
        let next_iter = if let Some(view) = self.pop_sst() {
            Some(SstIterator::new(view, table_store.clone(), sst_iterator_options).await?)
        } else {
            None
        };
        Ok(next_iter)
    }

    fn peek_next_table(&self) -> Option<&SsTableHandle> {
        match self {
            SortedRunView::Owned(tables, _) => tables.front(),
            SortedRunView::Borrowed(tables, _) => tables.front().copied(),
        }
    }
}

pub(crate) struct SortedRunIterator<'a> {
    table_store: Arc<TableStore>,
    sst_iter_options: SstIteratorOptions,
    view: SortedRunView<'a>,
    current_iter: Option<SstIterator<'a>>,
}

impl<'a> SortedRunIterator<'a> {
    async fn new(
        view: SortedRunView<'a>,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let mut res = Self {
            table_store,
            sst_iter_options,
            view,
            current_iter: None,
        };
        res.advance_table().await?;
        Ok(res)
    }

    pub(crate) async fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        sorted_run: SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let range = BytesRange::from(range);
        let tables = sorted_run.into_tables_covering_range(range.as_ref());
        let view = SortedRunView::Owned(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options).await
    }

    pub(crate) async fn new_borrowed<T: RangeBounds<&'a [u8]>>(
        range: T,
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let range = (range.start_bound().cloned(), range.end_bound().cloned());
        let tables = sorted_run.tables_covering_range(range);
        let view = SortedRunView::Borrowed(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options).await
    }

    pub(crate) async fn for_key(
        sorted_run: &'a SortedRun,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<SortedRunIterator<'a>, SlateDBError> {
        Self::new_borrowed(key..=key, sorted_run, table_store, sst_iter_options).await
    }

    async fn advance_table(&mut self) -> Result<(), SlateDBError> {
        self.current_iter = self
            .view
            .build_next_iter(self.table_store.clone(), self.sst_iter_options)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl KeyValueIterator for SortedRunIterator<'_> {
    async fn take_and_next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(iter) = &mut self.current_iter {
            if let Some(kv) = iter.take_and_next_entry().await? {
                return Ok(Some(kv));
            } else {
                self.advance_table().await?;
            }
        }
        Ok(None)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(next_table) = self.view.peek_next_table() {
            if next_table.compacted_first_key() < next_key {
                self.advance_table().await?;
            } else {
                break;
            }
        }
        if let Some(iter) = &mut self.current_iter {
            iter.seek(next_key).await?;
        }
        Ok(())
    }

    fn peek(&self) -> Option<&RowEntry> {
        if let Some(iter) = &self.current_iter {
            iter.peek()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_generator::OrderedBytesGenerator;
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
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        builder.add_value(b"key3", b"value3", gen_attrs(3)).unwrap();
        let encoded = builder.build().unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let handle = table_store.write_sst(&id, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle],
        };

        let mut iter =
            SortedRunIterator::new_owned(.., sr, table_store, SstIteratorOptions::default())
                .await
                .unwrap();

        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap();
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
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        let encoded = builder.build().unwrap();
        let id1 = SsTableId::Compacted(Ulid::new());
        let handle1 = table_store.write_sst(&id1, encoded).await.unwrap();
        let mut builder = table_store.table_builder();
        builder.add_value(b"key3", b"value3", gen_attrs(3)).unwrap();
        let encoded = builder.build().unwrap();
        let id2 = SsTableId::Compacted(Ulid::new());
        let handle2 = table_store.write_sst(&id2, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle1, handle2],
        };

        let mut iter = SortedRunIterator::new_owned(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.take_and_next_kv().await.unwrap();
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
            let mut iter = SortedRunIterator::new_borrowed(
                from_key.as_ref()..,
                &sr,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap();
            for _ in 0..30 - i {
                assert_kv(
                    &iter.take_and_next_kv().await.unwrap().unwrap(),
                    expected_key_gen.next().as_ref(),
                    expected_val_gen.next().as_ref(),
                );
            }
            assert!(iter.take_and_next_kv().await.unwrap().is_none());
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
        let mut iter = SortedRunIterator::new_borrowed(
            [b'a', 10].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        for _ in 0..30 {
            assert_kv(
                &iter.take_and_next_kv().await.unwrap().unwrap(),
                expected_key_gen.next().as_ref(),
                expected_val_gen.next().as_ref(),
            );
        }
        assert!(iter.take_and_next_kv().await.unwrap().is_none());
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

        let mut iter = SortedRunIterator::new_borrowed(
            [b'z', 30].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        assert!(iter.take_and_next_kv().await.unwrap().is_none());
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
        let mut sr_iter = SortedRunIterator::new_owned(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
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
                let kv = sr_iter.take_and_next_kv().await.unwrap().unwrap();
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
                builder.add_value(key, value, gen_attrs(0)).unwrap();
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
                let entry =
                    RowEntry::new_value(key_gen.next().as_ref(), val_gen.next().as_ref(), 0);
                writer.add(entry).await.unwrap();
            }
            ssts.push(writer.close().await.unwrap());
        }
        SortedRun { id: 0, ssts }
    }
}
