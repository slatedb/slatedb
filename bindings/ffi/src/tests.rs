use crate::*;
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path;
use tempfile::tempdir;

struct CounterMergeOperator;

impl MergeOperator for CounterMergeOperator {
    fn merge(&self, _key: Vec<u8>, existing_value: Option<Vec<u8>>, value: Vec<u8>) -> Vec<u8> {
        let existing = existing_value
            .map(|value| u64::from_le_bytes(value.try_into().expect("8-byte existing value")))
            .unwrap_or(0);
        let delta = u64::from_le_bytes(value.try_into().expect("8-byte delta"));
        (existing + delta).to_le_bytes().to_vec()
    }
}

#[tokio::test]
async fn resolves_memory_object_store() {
    let store = resolve_object_store("memory:///".to_owned()).unwrap();
    let location = Path::from("hello");
    let payload = Bytes::from_static(b"world");

    store.inner.put(&location, payload.clone().into()).await.unwrap();
    let result = store.inner.get(&location).await.unwrap().bytes().await.unwrap();

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

    store.inner.put(&location, payload.clone().into()).await.unwrap();

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
    assert_eq!(txn.get(b"tx".to_vec()).await.unwrap(), Some(b"value".to_vec()));
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
