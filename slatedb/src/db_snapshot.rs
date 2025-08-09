use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;

use crate::db::DbInner;
use crate::transaction_manager::{TransactionManager, TransactionState};
use crate::DbRead;

pub struct DbSnapshot {
    /// txn_state holds the seq number of the transaction that created this snapshot
    txn_state: Arc<TransactionState>,
    /// Unique ID assigned by the transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Reference to the database
    db_inner: Arc<DbInner>,
}

impl DbSnapshot {
    pub(crate) fn new(
        db_inner: Arc<DbInner>,
        txn_manager: Arc<TransactionManager>,
        seq: u64,
    ) -> Arc<Self> {
        let txn_state = txn_manager.new_txn(seq);

        Arc::new(Self {
            txn_state,
            txn_manager,
            db_inner,
        })
    }

    /// Get a value from the snapshot with default read options.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`: the value if it exists, None otherwise
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the snapshot with custom read options.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`: the value if it exists, None otherwise
    pub async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error> {
        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .get_with_options(key, options, &db_state, Some(self.txn_state.seq))
            .await
            .map_err(Into::into)
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys with the provided options.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the scan options to use
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        // TODO: this range conversion logic can be extract to an util
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = (start, end);
        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .scan_with_options(
                BytesRange::from(range),
                options,
                &db_state,
                Some(self.txn_state.seq),
            )
            .await
            .map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl DbRead for DbSnapshot {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, options).await
    }

    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, options).await
    }
}

/// Unregister from transaction manager when dropped.
impl Drop for DbSnapshot {
    fn drop(&mut self) {
        self.txn_manager.remove_txn(self.txn_state.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::config::{CompactorOptions, PutOptions, Settings, WriteOptions};
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStore;
    use crate::{Db, Error};
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    type SnapshotTestCaseSetupFunc =
        fn(&Db) -> Pin<Box<dyn Future<Output = Result<Arc<DbSnapshot>, Error>> + Send + '_>>;

    struct SnapshotGetTestCase {
        name: &'static str,
        setup: SnapshotTestCaseSetupFunc,
        expected_snapshot_results: Vec<(&'static str, Option<&'static str>)>,
        expected_db_results: Option<Vec<(&'static str, Option<&'static str>)>>,
    }

    struct SnapshotScanTestCase {
        name: &'static str,
        setup: SnapshotTestCaseSetupFunc,
        scan_start_key: &'static str,
        expected_snapshot_results: Vec<(&'static str, &'static str)>, // (key, value) pairs
        expected_db_results: Option<Vec<(&'static str, &'static str)>>,
    }

    async fn create_test_db() -> Db {
        let object_store = Arc::new(InMemory::new());
        let config = Settings {
            flush_interval: Some(Duration::from_millis(100)),
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            compactor_options: Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            }),
            max_unflushed_bytes: 16 * 1024,
            min_filter_keys: 0,
            l0_sst_size_bytes: 4 * 4096,
            ..Default::default()
        };

        Db::builder("/tmp/snapshot_test", object_store)
            .with_settings(config)
            .build()
            .await
            .expect("Failed to create test database")
    }

    #[rstest]
    #[case(SnapshotGetTestCase {
        name: "snapshot_after_put",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.snapshot().await
        }),
        expected_snapshot_results: vec![("key1", Some("value1"))],
        expected_db_results: None,
    })]
    #[case(SnapshotGetTestCase {
        name: "snapshot_after_delete", 
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.delete(b"key1").await?;
            db.snapshot().await
        }),
        expected_snapshot_results: vec![("key1", None)],
        expected_db_results: None,
    })]
    #[case(SnapshotGetTestCase {
        name: "write_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"original").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key1", b"modified").await?;
            db.put(b"key2", b"new_value").await?;
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("original")), ("key2", None)],
        expected_db_results: Some(vec![("key1", Some("modified")), ("key2", Some("new_value"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "snapshot_overwrites",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key1", b"value2").await?;
            db.put(b"key1", b"final_value").await?;
            db.snapshot().await
        }),
        expected_snapshot_results: vec![("key1", Some("final_value"))],
        expected_db_results: None,
    })]
    #[case(SnapshotGetTestCase {
        name: "overwrite_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"original").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key1", b"overwrite1").await?;
            db.put(b"key1", b"overwrite2").await?;
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("original"))],
        expected_db_results: Some(vec![("key1", Some("overwrite2"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "delete_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            let snapshot = db.snapshot().await?;
            db.delete(b"key1").await?;
            db.put(b"key3", b"value3").await?;
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("value1")), ("key2", Some("value2")), ("key3", None)],
        expected_db_results: Some(vec![("key1", None), ("key2", Some("value2")), ("key3", Some("value3"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "missing_keys",
        setup: |db| Box::pin(async move {
            db.put(b"existing", b"value").await?;
            db.snapshot().await
        }),
        expected_snapshot_results: vec![("existing", Some("value")), ("nonexistent", None)],
        expected_db_results: None,
    })]
    #[case(SnapshotGetTestCase {
        name: "flush_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key3", b"value3").await?;
            db.flush().await?; // Trigger flush after snapshot creation
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("value1")), ("key2", Some("value2")), ("key3", None)],
        expected_db_results: Some(vec![("key1", Some("value1")), ("key2", Some("value2")), ("key3", Some("value3"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "flush_then_write_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"original").await?;
            let snapshot = db.snapshot().await?;
            db.flush().await?; // Flush first
            db.put(b"key1", b"modified").await?; // Then write
            db.put(b"key2", b"new_value").await?;
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("original")), ("key2", None)],
        expected_db_results: Some(vec![("key1", Some("modified")), ("key2", Some("new_value"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "write_flush_delete_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key3", b"value3").await?;
            db.flush().await?; // Flush the new write
            db.delete(b"key1").await?; // Delete after flush
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("value1")), ("key2", Some("value2")), ("key3", None)],
        expected_db_results: Some(vec![("key1", None), ("key2", Some("value2")), ("key3", Some("value3"))]),
    })]
    #[case(SnapshotGetTestCase {
        name: "multiple_flush_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key2", b"batch1").await?;
            db.flush().await?; // First flush
            db.put(b"key3", b"batch2").await?;
            db.flush().await?; // Second flush
            db.put(b"key1", b"overwritten").await?;
            db.flush().await?; // Third flush
            Ok(snapshot)
        }),
        expected_snapshot_results: vec![("key1", Some("value1")), ("key2", None), ("key3", None)],
        expected_db_results: Some(vec![("key1", Some("overwritten")), ("key2", Some("batch1")), ("key3", Some("batch2"))]),
    })]
    #[tokio::test]
    async fn test_snapshot_get(#[case] test_case: SnapshotGetTestCase) -> Result<(), Error> {
        let db = create_test_db().await;
        let snapshot = (test_case.setup)(&db).await?;

        // Verify snapshot results
        for (key, expected_value) in &test_case.expected_snapshot_results {
            let result = snapshot.get(key.as_bytes()).await?;
            let expected = expected_value.map(Bytes::from);
            assert_eq!(
                result, expected,
                "test_case: {}, snapshot key: {}",
                test_case.name, key
            );
        }

        // Verify DB results if specified
        if let Some(db_expected) = &test_case.expected_db_results {
            for (key, expected_value) in db_expected {
                let result = db.get(key.as_bytes()).await?;
                let expected = expected_value.map(Bytes::from);
                assert_eq!(
                    result, expected,
                    "test_case: {}, DB Key: {}",
                    test_case.name, key
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_snapshots() -> Result<(), Error> {
        let db = create_test_db().await;

        // Version 1
        db.put(b"key1", b"version1").await?;
        let snapshot1 = db.snapshot().await?;

        // Version 2
        db.put(b"key1", b"version2").await?;
        let snapshot2 = db.snapshot().await?;

        // Version 3
        db.put(b"key1", b"version3").await?;
        let snapshot3 = db.snapshot().await?;

        // Verify each snapshot sees its respective version
        let result1 = snapshot1.get(b"key1").await?;
        assert_eq!(result1, Some(Bytes::from("version1")));

        let result2 = snapshot2.get(b"key1").await?;
        assert_eq!(result2, Some(Bytes::from("version2")));

        let result3 = snapshot3.get(b"key1").await?;
        assert_eq!(result3, Some(Bytes::from("version3")));

        Ok(())
    }

    #[rstest]
    #[case(SnapshotScanTestCase {
        name: "scan_from_start",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            db.put(b"key3", b"value3").await?;
            db.snapshot().await
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")],
        expected_db_results: None,
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_from_middle",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            db.put(b"key3", b"value3").await?;
            db.put(b"key4", b"value4").await?;
            db.snapshot().await
        }),
        scan_start_key: "key2",
        expected_snapshot_results: vec![("key2", "value2"), ("key3", "value3"), ("key4", "value4")],
        expected_db_results: None,
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_after_write_isolation",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"original1").await?;
            db.put(b"key2", b"original2").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key1", b"modified1").await?;
            db.put(b"key3", b"new_value").await?;
            Ok(snapshot)
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "original1"), ("key2", "original2")],
        expected_db_results: Some(vec![("key1", "modified1"), ("key2", "original2"), ("key3", "new_value")]),
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_empty_range",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.snapshot().await
        }),
        scan_start_key: "key5",
        expected_snapshot_results: vec![],
        expected_db_results: None,
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_with_delete_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            db.put(b"key3", b"value3").await?;
            let snapshot = db.snapshot().await?;
            db.delete(b"key2").await?;
            Ok(snapshot)
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")],
        expected_db_results: Some(vec![("key1", "value1"), ("key3", "value3")]),
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_flush_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key2", b"value2").await?;
            let snapshot = db.snapshot().await?;
            db.put(b"key3", b"value3").await?;
            db.put(b"key4", b"value4").await?;
            db.flush().await?; // Flush after snapshot creation
            Ok(snapshot)
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "value1"), ("key2", "value2")],
        expected_db_results: Some(vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3"), ("key4", "value4")]),
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_flush_then_write_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"original1").await?;
            db.put(b"key2", b"original2").await?;
            let snapshot = db.snapshot().await?;
            db.flush().await?; // Flush first
            db.put(b"key1", b"modified1").await?; // Overwrite existing
            db.put(b"key3", b"new_value").await?; // Add new
            db.delete(b"key2").await?; // Delete existing
            Ok(snapshot)
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "original1"), ("key2", "original2")],
        expected_db_results: Some(vec![("key1", "modified1"), ("key3", "new_value")]),
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_multiple_flush_after_snapshot",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key3", b"value3").await?;
            let snapshot = db.snapshot().await?;
            // First batch write and flush
            db.put(b"key2", b"batch1_key2").await?;
            db.put(b"key4", b"batch1_key4").await?;
            db.flush().await?;
            // Second batch write and flush
            db.put(b"key1", b"batch2_modified").await?;
            db.put(b"key5", b"batch2_key5").await?;
            db.flush().await?;
            // Third batch with delete
            db.delete(b"key3").await?;
            db.put(b"key6", b"batch3_key6").await?;
            db.flush().await?;
            Ok(snapshot)
        }),
        scan_start_key: "key1",
        expected_snapshot_results: vec![("key1", "value1"), ("key3", "value3")],
        expected_db_results: Some(vec![("key1", "batch2_modified"), ("key2", "batch1_key2"), ("key4", "batch1_key4"), ("key5", "batch2_key5"), ("key6", "batch3_key6")]),
    })]
    #[case(SnapshotScanTestCase {
        name: "scan_flush_with_range_changes",
        setup: |db| Box::pin(async move {
            db.put(b"a", b"before_a").await?;
            db.put(b"c", b"before_c").await?;
            db.put(b"e", b"before_e").await?;
            let snapshot = db.snapshot().await?;
            // Add keys in between existing ones
            db.put(b"b", b"after_b").await?;
            db.put(b"d", b"after_d").await?;
            db.put(b"f", b"after_f").await?;
            db.flush().await?;
            // Modify existing keys after flush
            db.put(b"a", b"modified_a").await?;
            db.delete(b"c").await?;
            Ok(snapshot)
        }),
        scan_start_key: "a",
        expected_snapshot_results: vec![("a", "before_a"), ("c", "before_c"), ("e", "before_e")],
        expected_db_results: Some(vec![("a", "modified_a"), ("b", "after_b"), ("d", "after_d"), ("e", "before_e"), ("f", "after_f")]),
    })]
    #[tokio::test]
    async fn test_snapshot_scan(#[case] test_case: SnapshotScanTestCase) -> Result<(), Error> {
        let db = create_test_db().await;
        let snapshot = (test_case.setup)(&db).await?;

        // Verify snapshot scan results
        let mut iterator = snapshot.scan(test_case.scan_start_key.as_bytes()..).await?;
        let mut actual_results = Vec::new();
        while let Some(kv) = iterator.next().await? {
            actual_results.push((
                String::from_utf8(kv.key.to_vec()).unwrap(),
                String::from_utf8(kv.value.to_vec()).unwrap(),
            ));
        }

        assert_eq!(
            actual_results.len(),
            test_case.expected_snapshot_results.len(),
            "test_case: {}, snapshot scan result count mismatch",
            test_case.name
        );

        for (i, ((actual_key, actual_value), (expected_key, expected_value))) in actual_results
            .iter()
            .zip(test_case.expected_snapshot_results.iter())
            .enumerate()
        {
            assert_eq!(
                actual_key, expected_key,
                "test_case: {}, snapshot scan key mismatch at index {}",
                test_case.name, i
            );
            assert_eq!(
                actual_value, expected_value,
                "test_case: {}, snapshot scan value mismatch at index {}",
                test_case.name, i
            );
        }

        // Verify DB scan results if specified
        if let Some(db_expected) = &test_case.expected_db_results {
            let mut db_iterator = db.scan(test_case.scan_start_key.as_bytes()..).await?;
            let mut db_actual_results = Vec::new();
            while let Some(kv) = db_iterator.next().await? {
                db_actual_results.push((
                    String::from_utf8(kv.key.to_vec()).unwrap(),
                    String::from_utf8(kv.value.to_vec()).unwrap(),
                ));
            }

            assert_eq!(
                db_actual_results.len(),
                db_expected.len(),
                "test_case: {}, DB scan result count mismatch",
                test_case.name
            );

            for (i, ((actual_key, actual_value), (expected_key, expected_value))) in
                db_actual_results.iter().zip(db_expected.iter()).enumerate()
            {
                assert_eq!(
                    actual_key, expected_key,
                    "test_case: {}, DB scan key mismatch at index {}",
                    test_case.name, i
                );
                assert_eq!(
                    actual_value, expected_value,
                    "test_case: {}, DB scan value mismatch at index {}",
                    test_case.name, i
                );
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_snapshot_with_committed_failpoint() -> Result<(), Error> {
        use bytes::Bytes;
        use fail_parallel::FailPointRegistry;
        use std::sync::Arc;

        // Create a test database with a failpoint registry
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fp_registry = Arc::new(FailPointRegistry::new());

        let config = Settings {
            flush_interval: Some(Duration::from_millis(100)),
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            compactor_options: Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            }),
            max_unflushed_bytes: 16 * 1024,
            min_filter_keys: 0,
            l0_sst_size_bytes: 4 * 4096,
            ..Default::default()
        };

        let db = Arc::new(
            Db::builder("/tmp/failpoint_test", object_store)
                .with_settings(config)
                .with_fp_registry(fp_registry.clone())
                .build()
                .await
                .expect("Failed to create test database"),
        );
        db.put(b"key1", b"value1").await?;

        // Configure the failpoint to "pause", blocking the put after memtable write but before commit
        fail_parallel::cfg(fp_registry.clone(), "write-batch-pre-commit", "pause").unwrap();

        // Start the put operation; it will pause at the failpoint
        let db_clone = Arc::clone(&db);
        let put_result = tokio::spawn(async move {
            db_clone
                .put_with_options(
                    b"key1",
                    b"value2",
                    &PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                    },
                )
                .await
                .unwrap();
        });

        // Sleep for 1 second to ensure the put is in the memtable but not committed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // At this point the data is in the memtable but not committed; create the snapshot
        let snapshot = db.snapshot().await?;

        // Turn off the failpoint to let the put complete
        fail_parallel::cfg(fp_registry.clone(), "write-batch-pre-commit", "off").unwrap();

        // Wait for the put to complete
        put_result.await.unwrap();

        // Assert the snapshot should not contain the new value
        let snapshot_result = snapshot.get(b"key1").await?;
        assert_eq!(snapshot_result, Some(Bytes::from("value1")));

        let db_result = db.get(b"key1").await?;
        assert_eq!(db_result, Some(Bytes::from("value2")));
        Ok(())
    }
}
