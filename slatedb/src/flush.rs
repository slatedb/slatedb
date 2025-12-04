use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::{KVTable, MemTableIterator};
use std::sync::Arc;
use crate::reader::DbStateReader;
use crate::retention_iterator::RetentionIterator;

impl DbInner {
    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        // let mut iter = imm_table.iter();
        // let mut last_key: Option<Bytes> = None;
        // while let Some(entry) = iter.next_entry().await? {
        //     if matches!(entry.value, ValueDeletable::Merge(_)) {
        //         sst_builder.add(entry)?;
        //     } else if last_key.as_ref() != Some(&entry.key) {
        //         last_key = Some(entry.key.clone());
        //         sst_builder.add(entry)?;
        //     }
        // }
        let mut iter = self.load_iterators(imm_table.clone()).await?;
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self
            .table_store
            .write_sst(id, encoded_sst, write_cache)
            .await?;

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());

        Ok(handle)
    }

    async fn load_iterators(&self, imm_table: Arc<KVTable>) -> Result<RetentionIterator<MemTableIterator>, SlateDBError> {
        let state = self.state.read().view();
        println!("min active seq: {:?}", self.txn_manager.min_active_seq());
        let mut iter = RetentionIterator::new(
            imm_table.iter(),
            None,
            self.txn_manager.min_active_seq(),
            false,
            self.system_clock.now().timestamp(),
            self.system_clock.clone(),
            Arc::new(state.core().sequence_tracker.clone()),
        ).await?;
        iter.init().await?;
        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use crate::block_iterator::BlockIterator;
    use crate::db::Db;
    use crate::db_state::{SsTableHandle, SsTableId};
    use crate::iter::KeyValueIterator;
    use crate::mem_table::WritableKVTable;
    use crate::object_store::memory::InMemory;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use std::sync::Arc;
    use rstest::rstest;
    use ulid::Ulid;

    async fn setup_test_db() -> Db {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Db::open("/tmp/test_flush_imm_table", object_store)
            .await
            .unwrap()
    }

    async fn verify_sst(
        db: &Db,
        sst_handle: &SsTableHandle,
        entries: &[(Bytes, u64, ValueDeletable)],
    ) {
        let index = db.inner.table_store.read_index(sst_handle).await.unwrap();
        let block_count = index.borrow().block_meta().len();
        let blocks = db
            .inner
            .table_store
            .read_blocks(sst_handle, 0..block_count)
            .await
            .unwrap();
        let mut found_entries = Vec::new();
        for block in blocks {
            let mut block_iter = BlockIterator::new_ascending(block);
            block_iter.init().await.unwrap();

            while let Some(entry) = block_iter.next_entry().await.unwrap() {
                found_entries.push((entry.key.clone(), entry.seq, entry.value.clone()));
            }
        }
        assert_eq!(entries.len(), found_entries.len());
        for i in 0..found_entries.len() {
            let (actual_key, actual_seq, actual_value) = &found_entries[i];
            let (expected_key, expected_seq, expected_value) = &entries[i];
            assert_eq!(expected_key, actual_key);
            assert_eq!(expected_seq, actual_seq);
            assert_eq!(expected_value, actual_value);
        }
    }

    #[tokio::test]
    async fn test_flush_empty_table() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(&db, &sst_handle, &[]).await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_single_entry() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"key1", b"value1", 1));
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &[(
                Bytes::from("key1"),
                1,
                ValueDeletable::Value(Bytes::from("value1")),
            )],
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_multiple_unique_keys() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(b"key1", b"value1", 1));
        table.put(RowEntry::new_value(b"key2", b"value2", 2));
        table.put(RowEntry::new_value(b"key3", b"value3", 3));
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &[
                (
                    Bytes::from("key1"),
                    1,
                    ValueDeletable::Value(Bytes::from("value1")),
                ),
                (
                    Bytes::from("key2"),
                    2,
                    ValueDeletable::Value(Bytes::from("value2")),
                ),
                (
                    Bytes::from("key3"),
                    3,
                    ValueDeletable::Value(Bytes::from("value3")),
                ),
            ],
        )
        .await;

        db.close().await.unwrap();
    }

    struct FlushImmTableTestCase {
        min_active_seq: u64,
        row_entries: Vec<RowEntry>,
        expected_entries: Vec<(Bytes, u64, ValueDeletable)>,
    }

    #[rstest]
    #[case(FlushImmTableTestCase {
        min_active_seq: 1,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value_v1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value_v3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value_v2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 1, ValueDeletable::Value(Bytes::from("value_v1"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value_v2"))),
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value_v3"))),
        ],
    })]
    #[case(FlushImmTableTestCase {
        min_active_seq: 3,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value_v1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value_v3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value_v2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value_v3")))
        ],
    })]
    #[tokio::test]
    async fn test_flush_deduplicates_keeping_highest_seq(#[case] test_case: FlushImmTableTestCase)
    {
        // Given
        let db = setup_test_db().await;
        {
            let mut state = db.inner.state.write();
            state
                .modify(|modifier|
                    modifier.state.manifest.value.core.recent_snapshot_min_seq = test_case.min_active_seq);
        }
        let table = WritableKVTable::new();
        let row_entries_length = test_case.row_entries.len();
        for row_entry in test_case.row_entries {
            table.put(row_entry);
        }
        assert_eq!(table.table().metadata().entry_num, row_entries_length);
        let id = SsTableId::Compacted(Ulid::new());

        // When
        println!("{:?}", db.inner.txn_manager.min_active_seq());
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &test_case.expected_entries,
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_deduplicates_multiple_keys() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        let key1 = Bytes::from("key1");
        let value3 = Bytes::from("value3");
        table.put(RowEntry::new_value(&key1, b"v1", 1));
        table.put(RowEntry::new_value(&key1, &value3, 3));
        table.put(RowEntry::new_value(&key1, b"v2", 2));
        let key2 = Bytes::from("key2");
        let value7 = Bytes::from("value7");
        table.put(RowEntry::new_value(&key2, b"v5", 5));
        table.put(RowEntry::new_value(&key2, &value7, 7));
        let key3 = Bytes::from("key3");
        let value4 = Bytes::from("value4");
        table.put(RowEntry::new_value(&key3, &value4, 4));
        assert_eq!(table.table().metadata().entry_num, 6);
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &[
                (key1, 3, ValueDeletable::Value(value3)),
                (key2, 7, ValueDeletable::Value(value7)),
                (key3, 4, ValueDeletable::Value(value4)),
            ],
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_tombstones() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");
        let value3 = Bytes::from("value4");
        table.put(RowEntry::new_value(&key1, b"value", 1));
        table.put(RowEntry::new_tombstone(&key1, 2));
        table.put(RowEntry::new_tombstone(&key2, 3));
        table.put(RowEntry::new_tombstone(&key3, 4));
        table.put(RowEntry::new_value(&key3, &value3, 5));
        assert_eq!(table.table().metadata().entry_num, 5);
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &[
                (key1, 2, ValueDeletable::Tombstone),
                (key2, 3, ValueDeletable::Tombstone),
                (key3, 5, ValueDeletable::Value(value3)),
            ],
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_merge() {
        // Given
        let db = setup_test_db().await;
        let table = WritableKVTable::new();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");
        let value3 = Bytes::from("value3");
        let value4 = Bytes::from("value4");
        let value5 = Bytes::from("value5");
        let value6 = Bytes::from("value6");
        table.put(RowEntry::new_merge(&key1, &value1, 1));
        table.put(RowEntry::new_value(&key2, &value2, 2));
        table.put(RowEntry::new_merge(&key1, &value3, 3));
        table.put(RowEntry::new_merge(&key3, &value4, 4));
        table.put(RowEntry::new_merge(&key2, &value5, 5));
        table.put(RowEntry::new_value(&key3, &value6, 6));
        assert_eq!(table.table().metadata().entry_num, 6);
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(
            &db,
            &sst_handle,
            &[
                (key1.clone(), 3, ValueDeletable::Merge(value3)),
                (key1, 1, ValueDeletable::Merge(value1)),
                (key2.clone(), 5, ValueDeletable::Merge(value5)),
                (key2, 2, ValueDeletable::Value(value2)),
                (key3.clone(), 6, ValueDeletable::Value(value6)),
                (key3, 4, ValueDeletable::Merge(value4)),
            ],
        )
        .await;

        db.close().await.unwrap();
    }
}
