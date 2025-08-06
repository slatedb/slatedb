use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;

use crate::db::DbInner;
use crate::transaction_manager::{TransactionManager, TransactionState};

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

impl Drop for DbSnapshot {
    fn drop(&mut self) {
        // Unregister from transaction manager when dropped
        self.txn_manager.remove_txn(self.txn_state.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::config::{CompactorOptions, Settings};
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStore;
    use crate::{Db, Error};
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    struct SnapshotTestCase {
        name: &'static str,
        setup: fn(&Db) -> Pin<Box<dyn Future<Output = Result<Arc<DbSnapshot>, Error>> + Send + '_>>,
        expected_snapshot_results: Vec<(&'static str, Option<&'static str>)>,
        expected_db_results: Option<Vec<(&'static str, Option<&'static str>)>>,
    }

    async fn create_test_db() -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    #[case(SnapshotTestCase {
        name: "snapshot_after_put",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            Ok(db.snapshot().await?)
        }),
        expected_snapshot_results: vec![("key1", Some("value1"))],
        expected_db_results: None,
    })]
    #[case(SnapshotTestCase {
        name: "snapshot_after_delete", 
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.delete(b"key1").await?;
            Ok(db.snapshot().await?)
        }),
        expected_snapshot_results: vec![("key1", None)],
        expected_db_results: None,
    })]
    #[case(SnapshotTestCase {
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
    #[case(SnapshotTestCase {
        name: "snapshot_overwrites",
        setup: |db| Box::pin(async move {
            db.put(b"key1", b"value1").await?;
            db.put(b"key1", b"value2").await?;
            db.put(b"key1", b"final_value").await?;
            Ok(db.snapshot().await?)
        }),
        expected_snapshot_results: vec![("key1", Some("final_value"))],
        expected_db_results: None,
    })]
    #[case(SnapshotTestCase {
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
    #[case(SnapshotTestCase {
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
    #[case(SnapshotTestCase {
        name: "missing_keys",
        setup: |db| Box::pin(async move {
            db.put(b"existing", b"value").await?;
            Ok(db.snapshot().await?)
        }),
        expected_snapshot_results: vec![("existing", Some("value")), ("nonexistent", None)],
        expected_db_results: None,
    })]
    #[tokio::test]
    async fn test_snapshot_operations(#[case] test_case: SnapshotTestCase) -> Result<(), Error> {
        let db = create_test_db().await;
        let snapshot = (test_case.setup)(&db).await?;

        // Verify snapshot results
        for (key, expected_value) in &test_case.expected_snapshot_results {
            let result = snapshot.get(key.as_bytes()).await?;
            let expected = expected_value.map(|v| Bytes::from(v));
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
                let expected = expected_value.map(|v| Bytes::from(v));
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
}
