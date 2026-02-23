use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::KVTable;
use crate::merge_operator::{MergeOperatorIterator, MergeOperatorRequiredIterator};
use crate::oracle::Oracle;
use crate::reader::DbStateReader;
use crate::retention_iterator::RetentionIterator;
use std::sync::Arc;

impl DbInner {
    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = self.iter_imm_table(imm_table.clone()).await?;
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry).await?;
        }

        let encoded_sst = sst_builder.build().await?;
        let handle = self
            .table_store
            .write_sst(id, encoded_sst, write_cache)
            .await?;

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());

        Ok(handle)
    }

    async fn iter_imm_table(
        &self,
        imm_table: Arc<KVTable>,
    ) -> Result<RetentionIterator<Box<dyn KeyValueIterator>>, SlateDBError> {
        let state = self.state.read().view();

        // Compute retention boundary using both active transactions AND durable watermark.
        // Remote readers (DurabilityLevel::Remote) cap visibility at last_remote_persisted_seq,
        // so we must retain at least one version at or below that boundary for each key.
        // Otherwise, if we only keep a newer non-durable version, remote readers would skip
        // it and incorrectly fall back to an even older value.
        let durable_seq = self.oracle.last_remote_persisted_seq();
        let min_retention_seq = match self.txn_manager.min_active_seq() {
            Some(active_seq) => Some(active_seq.min(durable_seq)),
            None => Some(durable_seq),
        };

        let merge_iter = if let Some(merge_operator) = self.settings.merge_operator.clone() {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                imm_table.iter(),
                false,
                imm_table.last_tick(),
                min_retention_seq,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(imm_table.iter()))
                as Box<dyn KeyValueIterator>
        };
        let mut iter = RetentionIterator::new(
            merge_iter,
            None,
            min_retention_seq,
            false,
            imm_table.last_tick(),
            self.system_clock.clone(),
            Arc::new(state.core().sequence_tracker.clone()),
        )
        .await?;
        iter.init().await?;
        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use crate::block_iterator::BlockIteratorLatest;
    use crate::db::Db;
    use crate::db_state::{SsTableHandle, SsTableId};
    use crate::error::SlateDBError;
    use crate::error::SlateDBError::MergeOperatorMissing;
    use crate::iter::KeyValueIterator;
    use crate::mem_table::WritableKVTable;
    use crate::object_store::memory::InMemory;
    use crate::test_utils::StringConcatMergeOperator;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use rstest::rstest;
    use std::sync::Arc;
    use ulid::Ulid;

    async fn setup_test_db_with_merge_operator() -> Db {
        setup_test_db(true).await
    }

    async fn setup_test_db_without_merge_operator() -> Db {
        setup_test_db(false).await
    }

    async fn setup_test_db(set_merge_operator: bool) -> Db {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let builder = Db::builder("/tmp/test_flush_imm_table", object_store);
        let builder = if set_merge_operator {
            builder.with_merge_operator(Arc::new(StringConcatMergeOperator))
        } else {
            builder
        };
        builder.build().await.unwrap()
    }

    async fn verify_sst(
        db: &Db,
        sst_handle: &SsTableHandle,
        entries: &[(Bytes, u64, ValueDeletable)],
    ) {
        let index = db
            .inner
            .table_store
            .read_index(sst_handle, true)
            .await
            .unwrap();
        let block_count = index.borrow().block_meta().len();
        let blocks = db
            .inner
            .table_store
            .read_blocks(sst_handle, 0..block_count)
            .await
            .unwrap();
        let mut found_entries = Vec::new();
        for block in blocks {
            let mut block_iter = BlockIteratorLatest::new_ascending(block);
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

    struct FlushImmTableTestCase {
        min_active_seq: u64,
        row_entries: Vec<RowEntry>,
        expected_entries: Vec<(Bytes, u64, ValueDeletable)>,
    }

    #[rstest]
    #[case::flush_empty_table(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![],
        expected_entries: vec![],
    })]
    #[case::flush_single_entry(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 1, ValueDeletable::Value(Bytes::from("value1"))),
        ],
    })]
    #[case::flush_multiple_unique_keys(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1),
            RowEntry::new_value(b"key2", b"value2", 2),
            RowEntry::new_value(b"key3", b"value3", 3),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 1, ValueDeletable::Value(Bytes::from("value1"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 3, ValueDeletable::Value(Bytes::from("value3"))),
        ],
    })]
    #[case::flush_all_seqs(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key"), 1, ValueDeletable::Value(Bytes::from("value1"))),
        ],
    })]
    #[case::flush_some_highest_seqs(FlushImmTableTestCase {
        min_active_seq: 2,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3"))),
            (Bytes::from("key"), 2, ValueDeletable::Value(Bytes::from("value2"))),
        ],
    })]
    #[case::flush_only_highest_seq(FlushImmTableTestCase {
        min_active_seq: 3,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key"), b"value2", 2),
        ],
        expected_entries: vec![
            (Bytes::from("key"), 3, ValueDeletable::Value(Bytes::from("value3")))
        ],
    })]
    #[case::flush_highest_seqs_multiple_key(FlushImmTableTestCase {
        min_active_seq: 6,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key1"), b"value2", 2),
            RowEntry::new_value(&Bytes::from("key2"), b"value3", 3),
            RowEntry::new_value(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_value(&Bytes::from("key1"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key2"), b"value6", 6),
        ],
        expected_entries: vec![
            // This is the expected results, because for each key slate needs to
            // a value at or before the min_active_seq
            // (see retention_iterator for more details)
            (Bytes::from("key1"), 5, ValueDeletable::Value(Bytes::from("value5"))),
            (Bytes::from("key2"), 6, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 4, ValueDeletable::Value(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_tombstones(FlushImmTableTestCase {
        min_active_seq: 5,
        row_entries: vec![
            RowEntry::new_value(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_tombstone(&Bytes::from("key1"), 2),
            RowEntry::new_tombstone(&Bytes::from("key2"), 3),
            RowEntry::new_tombstone(&Bytes::from("key3"), 4),
            RowEntry::new_value(&Bytes::from("key3"), b"value3", 5),
            RowEntry::new_tombstone(&Bytes::from("key2"), 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 2, ValueDeletable::Tombstone),
            (Bytes::from("key2"), 6, ValueDeletable::Tombstone),
            (Bytes::from("key2"), 3, ValueDeletable::Tombstone),
            (Bytes::from("key3"), 5, ValueDeletable::Value(Bytes::from("value3"))),
        ],
    })]
    #[case::flush_merges_with_earlier_active_seqs(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value3"))),
            (Bytes::from("key1"), 1, ValueDeletable::Merge(Bytes::from("value1"))),
            (Bytes::from("key2"), 5, ValueDeletable::Merge(Bytes::from("value5"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 6, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 4, ValueDeletable::Merge(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_merges_and_tombstones(FlushImmTableTestCase {
        min_active_seq: 0,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_tombstone(&Bytes::from("key1"), 4),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 5),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 6),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 7),
            RowEntry::new_tombstone(&Bytes::from("key3"), 8),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 4, ValueDeletable::Tombstone),
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value3"))),
            (Bytes::from("key1"), 1, ValueDeletable::Merge(Bytes::from("value1"))),
            (Bytes::from("key2"), 6, ValueDeletable::Merge(Bytes::from("value5"))),
            (Bytes::from("key2"), 2, ValueDeletable::Value(Bytes::from("value2"))),
            (Bytes::from("key3"), 8, ValueDeletable::Tombstone),
            (Bytes::from("key3"), 7, ValueDeletable::Value(Bytes::from("value6"))),
            (Bytes::from("key3"), 5, ValueDeletable::Merge(Bytes::from("value4"))),
        ],
    })]
    #[case::flush_merges_with_recent_active_seqs(FlushImmTableTestCase {
        min_active_seq: 6,
        row_entries: vec![
            RowEntry::new_merge(&Bytes::from("key1"), b"value1", 1),
            RowEntry::new_value(&Bytes::from("key2"), b"value2", 2),
            RowEntry::new_merge(&Bytes::from("key1"), b"value3", 3),
            RowEntry::new_merge(&Bytes::from("key3"), b"value4", 4),
            RowEntry::new_merge(&Bytes::from("key2"), b"value5", 5),
            RowEntry::new_value(&Bytes::from("key3"), b"value6", 6),
        ],
        expected_entries: vec![
            (Bytes::from("key1"), 3, ValueDeletable::Merge(Bytes::from("value1value3"))),
            (Bytes::from("key2"), 5, ValueDeletable::Value(Bytes::from("value2value5"))),
            (Bytes::from("key3"), 6, ValueDeletable::Value(Bytes::from("value6"))),
        ],
    })]
    #[tokio::test]
    async fn test_flush(#[case] test_case: FlushImmTableTestCase) {
        // Given
        let db = setup_test_db_with_merge_operator().await;
        db.inner
            .txn_manager
            .new_snapshot(Some(test_case.min_active_seq));
        // Set durable watermark high so it doesn't interfere with transaction-based retention tests
        db.inner.oracle.last_remote_persisted_seq.store(u64::MAX);
        let table = WritableKVTable::new();
        let row_entries_length = test_case.row_entries.len();
        for row_entry in test_case.row_entries {
            table.put(row_entry);
        }
        assert_eq!(table.table().metadata().entry_num, row_entries_length);
        let id = SsTableId::Compacted(Ulid::new());

        // When
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then
        verify_sst(&db, &sst_handle, &test_case.expected_entries).await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_err_when_merge_operator_not_set_and_merges_exist() {
        // Given
        let db = setup_test_db_without_merge_operator().await;
        db.inner.txn_manager.new_snapshot(Some(0));
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value1", 1));
        table.put(RowEntry::new_merge(&Bytes::from("key"), b"value2", 2));
        let id = SsTableId::Compacted(Ulid::new());

        // When
        db.inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .map_or_else(
                |err| match err {
                    MergeOperatorMissing => Ok::<(), SlateDBError>(()),
                    _ => panic!("Should return MergeOperatorMissing error"),
                },
                |_| panic!("Should return MergeOperatorMissing error"),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_no_err_merge_operator_not_set_and_no_merges() {
        // Given
        let db = setup_test_db_without_merge_operator().await;
        db.inner.txn_manager.new_snapshot(Some(0));
        let table = WritableKVTable::new();
        table.put(RowEntry::new_value(&Bytes::from("key1"), b"value1", 1));
        table.put(RowEntry::new_tombstone(&Bytes::from("key2"), 2));
        let id = SsTableId::Compacted(Ulid::new());

        // When
        db.inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn should_retain_versions_up_to_durable_boundary_when_no_active_txn() {
        // Given: DB with no active transactions, but durable watermark at seq=2.
        // This can happen when the WAL has flushed up to seq=2, but newer writes
        // (seq=3) are still in the memtable and not yet durable.
        let db = setup_test_db_with_merge_operator().await;
        db.inner.oracle.last_remote_persisted_seq.store(2);
        // No active transaction created - min_active_seq() will return None

        let table = WritableKVTable::new();
        // Add versions: seq=1 (durable), seq=2 (durable), seq=3 (not durable)
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value1", 1));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value2", 2));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value3", 3));
        let id = SsTableId::Compacted(Ulid::new());

        // When: Flush the table
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then: Should retain seq=3 (latest) and seq=2 (boundary at durable watermark)
        // seq=1 can be dropped because seq=2 is the boundary value for remote readers
        verify_sst(
            &db,
            &sst_handle,
            &[
                (
                    Bytes::from("key"),
                    3,
                    ValueDeletable::Value(Bytes::from("value3")),
                ),
                (
                    Bytes::from("key"),
                    2,
                    ValueDeletable::Value(Bytes::from("value2")),
                ),
            ],
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_use_min_of_active_seq_and_durable_boundary() {
        // Given: DB with active transaction at seq=3, durable watermark at seq=1
        let db = setup_test_db_with_merge_operator().await;
        db.inner.oracle.last_remote_persisted_seq.store(1);
        db.inner.txn_manager.new_snapshot(Some(3)); // Active txn at seq=3

        let table = WritableKVTable::new();
        // Add versions: seq=1, seq=2, seq=3, seq=4
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value1", 1));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value2", 2));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value3", 3));
        table.put(RowEntry::new_value(&Bytes::from("key"), b"value4", 4));
        let id = SsTableId::Compacted(Ulid::new());

        // When: Flush the table
        let sst_handle = db
            .inner
            .flush_imm_table(&id, table.table().clone(), false)
            .await
            .unwrap();

        // Then: Should use min(3, 1) = 1 as boundary
        // Retains seq=4 (latest), seq=3, seq=2, and seq=1 (boundary)
        // Without the fix, would use active_seq=3, retaining only seq=4 and seq=3
        verify_sst(
            &db,
            &sst_handle,
            &[
                (
                    Bytes::from("key"),
                    4,
                    ValueDeletable::Value(Bytes::from("value4")),
                ),
                (
                    Bytes::from("key"),
                    3,
                    ValueDeletable::Value(Bytes::from("value3")),
                ),
                (
                    Bytes::from("key"),
                    2,
                    ValueDeletable::Value(Bytes::from("value2")),
                ),
                (
                    Bytes::from("key"),
                    1,
                    ValueDeletable::Value(Bytes::from("value1")),
                ),
            ],
        )
        .await;

        db.close().await.unwrap();
    }
}
