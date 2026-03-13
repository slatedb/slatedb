use crate::*;
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path;
use std::sync::{Arc, OnceLock};
use tempfile::tempdir;
use uuid::Uuid;

struct CounterMergeOperator;

impl MergeOperator for CounterMergeOperator {
    fn merge(
        &self,
        _key: Vec<u8>,
        existing_value: Option<Vec<u8>>,
        operand: Vec<u8>,
    ) -> Result<Vec<u8>, MergeOperatorCallbackError> {
        let existing = existing_value
            .map(|value| u64::from_le_bytes(value.try_into().expect("8-byte existing value")))
            .unwrap_or(0);
        let delta = u64::from_le_bytes(operand.try_into().expect("8-byte delta"));
        Ok((existing + delta).to_le_bytes().to_vec())
    }
}

struct FailingMergeOperator;

impl MergeOperator for FailingMergeOperator {
    fn merge(
        &self,
        _key: Vec<u8>,
        _existing_value: Option<Vec<u8>>,
        _operand: Vec<u8>,
    ) -> Result<Vec<u8>, MergeOperatorCallbackError> {
        Err(MergeOperatorCallbackError::Failed {
            message: "bad operand".to_owned(),
        })
    }
}

static LOGGING_INIT: OnceLock<()> = OnceLock::new();

fn unique_path(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

fn memory_store() -> Arc<ObjectStore> {
    resolve_object_store("memory:///".to_owned()).unwrap()
}

async fn collect_iterator(iter: Arc<DbIterator>) -> Vec<KeyValue> {
    let mut items = Vec::new();
    while let Some(item) = iter.next().await.unwrap() {
        items.push(item);
    }
    items
}

#[tokio::test]
async fn resolves_memory_object_store() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let location = Path::from("hello");
    let payload = Bytes::from_static(b"world");

    store
        .inner
        .put(&location, payload.clone().into())
        .await
        .unwrap();
    let result = store
        .inner
        .get(&location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(result, payload);
}

#[tokio::test]
async fn resolves_local_object_store() {
    let temp_dir = tempdir().unwrap();
    let prefix_path = temp_dir.path().join("prefix-store");
    let url = format!("file://{}", prefix_path.display());
    let store = resolve_object_store(url).unwrap();
    let location = Path::from("nested/file.txt");
    let payload = Bytes::from_static(b"payload");

    store
        .inner
        .put(&location, payload.clone().into())
        .await
        .unwrap();

    let stored = tokio::fs::read(prefix_path.join("nested").join("file.txt"))
        .await
        .unwrap();
    assert_eq!(stored, payload.to_vec());
}

#[tokio::test]
async fn builds_database_and_rejects_builder_reuse() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("test-db".to_owned(), store);
    let db = builder.build().await.unwrap();

    assert!(builder.with_seed(7).is_err());
    db.close().await.unwrap();
}

#[tokio::test]
async fn supports_put_get_snapshot_and_close() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("test-db".to_owned(), store);
    let db = builder.build().await.unwrap();

    db.put(b"k1".to_vec(), b"v1".to_vec()).await.unwrap();
    let snapshot = db.snapshot().await.unwrap();
    db.put(b"k2".to_vec(), b"v2".to_vec()).await.unwrap();

    assert_eq!(db.get(b"k1".to_vec()).await.unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2".to_vec()).await.unwrap(), Some(b"v2".to_vec()));
    assert_eq!(snapshot.get(b"k2".to_vec()).await.unwrap(), None);

    db.close().await.unwrap();
    assert!(db.status().is_err());
}

#[tokio::test]
async fn supports_merge_operator_and_merge_writes() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("merge-db".to_owned(), store);
    builder
        .with_merge_operator(Box::new(CounterMergeOperator))
        .unwrap();
    let db = builder.build().await.unwrap();

    db.put(b"counter".to_vec(), 1_u64.to_le_bytes().to_vec())
        .await
        .unwrap();
    db.merge(b"counter".to_vec(), 2_u64.to_le_bytes().to_vec())
        .await
        .unwrap();

    let value = db.get(b"counter".to_vec()).await.unwrap().unwrap();
    assert_eq!(u64::from_le_bytes(value.try_into().unwrap()), 3);
}

#[tokio::test]
async fn propagates_merge_operator_failures() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("merge-db-error".to_owned(), store);
    builder
        .with_merge_operator(Box::new(FailingMergeOperator))
        .unwrap();
    let db = builder.build().await.unwrap();

    db.put(b"counter".to_vec(), 1_u64.to_le_bytes().to_vec())
        .await
        .unwrap();
    let err = db
        .merge(b"counter".to_vec(), 2_u64.to_le_bytes().to_vec())
        .await
        .unwrap_err();
    assert!(matches!(err, SlatedbError::Invalid { .. }));
    assert!(err.to_string().contains("bad operand"));
}

#[tokio::test]
async fn supports_scan_iterator_seek_and_transaction_lifecycle() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("scan-db".to_owned(), store);
    let db = builder.build().await.unwrap();

    db.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
    db.put(b"b".to_vec(), b"2".to_vec()).await.unwrap();
    db.put(b"c".to_vec(), b"3".to_vec()).await.unwrap();

    let iter = db
        .scan(DbKeyRange {
            start: Some(b"a".to_vec()),
            start_inclusive: true,
            end: Some(b"z".to_vec()),
            end_inclusive: false,
        })
        .await
        .unwrap();

    assert_eq!(iter.next().await.unwrap().unwrap().key, b"a".to_vec());
    iter.seek(b"c".to_vec()).await.unwrap();
    assert_eq!(iter.next().await.unwrap().unwrap().key, b"c".to_vec());
    assert!(iter.seek(Vec::new()).await.is_err());

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.put(b"tx".to_vec(), b"value".to_vec()).await.unwrap();
    assert_eq!(
        txn.get(b"tx".to_vec()).await.unwrap(),
        Some(b"value".to_vec())
    );
    let handle = txn.commit().await.unwrap().unwrap();
    assert!(handle.seqnum > 0);
    assert!(txn.commit().await.is_err());
}

#[tokio::test]
async fn rejects_invalid_settings_json() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let builder = DbBuilder::new("test-db".to_owned(), store);
    assert!(builder.with_settings_json("{not-json}".to_owned()).is_err());
}

#[tokio::test]
async fn supports_first_class_write_batches() {
    let store = memory_store();
    let builder = DbBuilder::new(unique_path("write-batch-db"), store);
    builder
        .with_merge_operator(Box::new(CounterMergeOperator))
        .unwrap();
    let db = builder.build().await.unwrap();

    db.put(b"counter".to_vec(), 1_u64.to_le_bytes().to_vec())
        .await
        .unwrap();
    db.put(b"gone".to_vec(), b"bye".to_vec()).await.unwrap();

    let batch = WriteBatch::new();
    batch.put(b"alpha".to_vec(), b"one".to_vec()).unwrap();
    batch
        .put_with_options(b"beta".to_vec(), b"two".to_vec(), DbPutOptions::default())
        .unwrap();
    batch
        .merge(b"counter".to_vec(), 2_u64.to_le_bytes().to_vec())
        .unwrap();
    batch
        .merge_with_options(
            b"counter".to_vec(),
            3_u64.to_le_bytes().to_vec(),
            DbMergeOptions::default(),
        )
        .unwrap();
    batch.delete(b"gone".to_vec()).unwrap();

    db.write_batch(batch.clone()).await.unwrap();
    batch.close().unwrap();

    assert_eq!(
        db.get(b"alpha".to_vec()).await.unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(
        db.get(b"beta".to_vec()).await.unwrap(),
        Some(b"two".to_vec())
    );
    assert_eq!(db.get(b"gone".to_vec()).await.unwrap(), None);

    let counter = db.get(b"counter".to_vec()).await.unwrap().unwrap();
    assert_eq!(u64::from_le_bytes(counter.try_into().unwrap()), 6);

    db.close().await.unwrap();
}

#[tokio::test]
async fn enforces_write_batch_lifecycle() {
    let store = memory_store();
    let db = DbBuilder::new(unique_path("write-batch-lifecycle"), store)
        .build()
        .await
        .unwrap();

    let batch = WriteBatch::new();
    batch.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    db.write_batch(batch.clone()).await.unwrap();

    assert!(matches!(
        batch.put(b"k2".to_vec(), b"v2".to_vec()).unwrap_err(),
        SlatedbError::Invalid { message } if message == "write batch has already been consumed"
    ));
    assert!(matches!(
        db.write_batch(batch.clone()).await.unwrap_err(),
        SlatedbError::Invalid { message } if message == "write batch has already been consumed"
    ));

    batch.close().unwrap();
    assert!(matches!(
        batch.close().unwrap_err(),
        SlatedbError::Invalid { message } if message == "write batch is already closed"
    ));

    let closed_batch = WriteBatch::new();
    closed_batch.close().unwrap();
    assert!(matches!(
        closed_batch.delete(b"k3".to_vec()).unwrap_err(),
        SlatedbError::Invalid { message } if message == "write batch is already closed"
    ));
    assert!(matches!(
        db.write_batch_with_options(
            closed_batch.clone(),
            DbWriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap_err(),
        SlatedbError::Invalid { message } if message == "write batch is already closed"
    ));

    let batch_with_options = WriteBatch::new();
    batch_with_options
        .put(b"opt".to_vec(), b"value".to_vec())
        .unwrap();
    db.write_batch_with_options(
        batch_with_options.clone(),
        DbWriteOptions {
            await_durable: false,
        },
    )
    .await
    .unwrap();
    batch_with_options.close().unwrap();

    assert_eq!(
        db.get(b"opt".to_vec()).await.unwrap(),
        Some(b"value".to_vec())
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn supports_vector_write_operations() {
    let store = memory_store();
    let db = DbBuilder::new(unique_path("vector-write"), store)
        .build()
        .await
        .unwrap();

    db.put(b"gone".to_vec(), b"bye".to_vec()).await.unwrap();
    db.write(vec![
        DbWriteOperation::Put {
            key: b"alpha".to_vec(),
            value_bytes: b"one".to_vec(),
            options: DbPutOptions::default(),
        },
        DbWriteOperation::Delete {
            key: b"gone".to_vec(),
        },
    ])
    .await
    .unwrap();

    assert_eq!(
        db.get(b"alpha".to_vec()).await.unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(db.get(b"gone".to_vec()).await.unwrap(), None);

    db.close().await.unwrap();
}

#[tokio::test]
async fn supports_db_reader_builder_reads_and_scans() {
    let store = memory_store();
    let path = unique_path("reader-db");
    let db = DbBuilder::new(path.clone(), store.clone())
        .build()
        .await
        .unwrap();

    for (key, value) in [
        (b"item:01".to_vec(), b"first".to_vec()),
        (b"item:02".to_vec(), b"second".to_vec()),
        (b"item:03".to_vec(), b"third".to_vec()),
        (b"other:01".to_vec(), b"other".to_vec()),
    ] {
        db.put(key, value).await.unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let builder = DbReaderBuilder::new(path, store);
    let reader = builder.build().await.unwrap();

    assert_eq!(
        reader
            .get_with_options(b"item:01".to_vec(), DbReadOptions::default())
            .await
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(reader.get(b"missing".to_vec()).await.unwrap(), None);

    let range_items = collect_iterator(
        reader
            .scan_with_options(
                DbKeyRange {
                    start: Some(b"item:01".to_vec()),
                    start_inclusive: true,
                    end: Some(b"item:99".to_vec()),
                    end_inclusive: false,
                },
                DbScanOptions {
                    read_ahead_bytes: 1024,
                    cache_blocks: true,
                    max_fetch_tasks: 2,
                    ..DbScanOptions::default()
                },
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(range_items.len(), 3);

    let prefix_items = collect_iterator(
        reader
            .scan_prefix_with_options(
                b"item:".to_vec(),
                DbScanOptions {
                    read_ahead_bytes: 1024,
                    max_fetch_tasks: 2,
                    ..DbScanOptions::default()
                },
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(prefix_items.len(), 3);

    reader.close().await.unwrap();
}

#[tokio::test]
async fn validates_db_reader_checkpoint_ids() {
    let builder = DbReaderBuilder::new(unique_path("reader-builder"), memory_store());
    assert!(matches!(
        builder.with_checkpoint_id("not-a-uuid".to_owned()).unwrap_err(),
        SlatedbError::Invalid { message } if message.starts_with("invalid checkpoint_id UUID:")
    ));
}

#[tokio::test]
async fn opens_db_reader_with_non_default_options() {
    let store = memory_store();
    let path = unique_path("reader-options");
    let db = DbBuilder::new(path.clone(), store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let builder = DbReaderBuilder::new(path, store);
    builder
        .with_options(DbReaderOptions {
            manifest_poll_interval_ms: 1_000,
            checkpoint_lifetime_ms: 5_000,
            max_memtable_bytes: 8 * 1024,
            skip_wal_replay: true,
        })
        .unwrap();
    let reader = builder.build().await.unwrap();

    assert_eq!(
        reader.get(b"k".to_vec()).await.unwrap(),
        Some(b"v".to_vec())
    );
    reader.close().await.unwrap();
}

#[tokio::test]
async fn supports_wal_inspection_apis() {
    let store = memory_store();
    let path = unique_path("wal-reader");
    let db = DbBuilder::new(path.clone(), store.clone())
        .build()
        .await
        .unwrap();

    db.put_with_options(
        b"k_value".to_vec(),
        b"hello".to_vec(),
        DbPutOptions {
            ttl: Ttl::ExpireAfterTicks(60_000),
        },
        DbWriteOptions::default(),
    )
    .await
    .unwrap();
    db.delete(b"k_tomb".to_vec()).await.unwrap();
    db.merge(b"k_merge".to_vec(), b"operand".to_vec())
        .await
        .unwrap();
    db.flush_with_options(DbFlushOptions {
        flush_type: FlushType::Wal,
    })
    .await
    .unwrap();
    db.close().await.unwrap();

    let reader = WalReader::new(path, store);
    let files = reader.list(None, None).await.unwrap();
    assert!(!files.is_empty());

    let first_file = reader.get(files[0].id());
    assert_eq!(first_file.id(), files[0].id());
    assert_eq!(first_file.next_id(), files[0].id() + 1);

    let next_file = first_file.next_file();
    assert_eq!(next_file.id(), files[0].id() + 1);
    next_file.close().unwrap();
    next_file.close().unwrap();

    let metadata = first_file.metadata().await.unwrap();
    assert!(metadata.last_modified_seconds > 0);
    assert!(metadata.size_bytes > 0);
    assert!(!metadata.location.is_empty());

    let ranged_files = reader
        .list(Some(files[0].id()), Some(files[0].id() + 1))
        .await
        .unwrap();
    assert_eq!(ranged_files.len(), 1);

    let mut saw_value = false;
    let mut saw_tombstone = false;
    let mut saw_merge = false;
    for file in files {
        let iter = file.iterator().await.unwrap();
        while let Some(entry) = iter.next().await.unwrap() {
            match entry.key.as_slice() {
                b"k_value" => {
                    saw_value = true;
                    assert_eq!(entry.kind, RowEntryKind::Value);
                    assert_eq!(entry.value, Some(b"hello".to_vec()));
                    assert!(entry.create_ts.is_some());
                    assert!(entry.expire_ts.is_some());
                }
                b"k_tomb" => {
                    saw_tombstone = true;
                    assert_eq!(entry.kind, RowEntryKind::Tombstone);
                    assert_eq!(entry.value, None);
                }
                b"k_merge" => {
                    saw_merge = true;
                    assert_eq!(entry.kind, RowEntryKind::Merge);
                    assert_eq!(entry.value, Some(b"operand".to_vec()));
                    assert!(entry.create_ts.is_some());
                }
                _ => {}
            }
        }

        iter.close().unwrap();
        iter.close().unwrap();
        file.close().unwrap();
        file.close().unwrap();
    }

    assert!(saw_value);
    assert!(saw_tombstone);
    assert!(saw_merge);

    first_file.close().unwrap();
    first_file.close().unwrap();
    reader.close().unwrap();
    reader.close().unwrap();
}

#[tokio::test]
async fn snapshots_db_metrics() {
    let store = memory_store();
    let db = DbBuilder::new(unique_path("metrics-db"), store)
        .build()
        .await
        .unwrap();

    let batch = WriteBatch::new();
    batch.put(b"metric".to_vec(), b"value".to_vec()).unwrap();
    db.write_batch(batch.clone()).await.unwrap();
    batch.close().unwrap();
    assert_eq!(
        db.get(b"metric".to_vec()).await.unwrap(),
        Some(b"value".to_vec())
    );

    let metrics = db.metrics().unwrap();
    assert!(!metrics.is_empty());
    assert!(metrics.contains_key("db/get_requests"));
    assert!(metrics.contains_key("db/write_batch_count"));
    assert!(metrics["db/get_requests"] >= 1);
    assert!(metrics["db/write_batch_count"] >= 1);

    db.close().await.unwrap();
}

#[tokio::test]
async fn logging_helpers_succeed() {
    LOGGING_INIT.get_or_init(|| {
        init_default_logging().unwrap();
    });

    set_logging_level(LogLevel::Debug).unwrap();
}

#[tokio::test]
async fn reports_clean_close_reason_after_shutdown() {
    let store = memory_store();
    let db = DbBuilder::new(unique_path("close-reason-db"), store)
        .build()
        .await
        .unwrap();

    db.close().await.unwrap();

    assert!(matches!(
        db.status().unwrap_err(),
        SlatedbError::Closed {
            reason: CloseReason::Clean,
            ..
        }
    ));
}

#[test]
fn maps_core_close_reasons() {
    assert_eq!(
        CloseReason::from(slatedb::CloseReason::Clean),
        CloseReason::Clean
    );
    assert_eq!(
        CloseReason::from(slatedb::CloseReason::Fenced),
        CloseReason::Fenced
    );
    assert_eq!(
        CloseReason::from(slatedb::CloseReason::Panic),
        CloseReason::Panic
    );
}
