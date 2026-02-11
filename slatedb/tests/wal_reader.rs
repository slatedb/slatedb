#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use object_store::memory::InMemory;
use object_store::ObjectStore;
use slatedb::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
use slatedb::{Db, WalReader};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_path(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}", prefix, nanos)
}

#[tokio::test]
async fn test_wal_reader_lists_and_reads() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = unique_path("/test_wal_reader_integration");
    let db = Db::open(path.clone(), object_store.clone()).await.unwrap();

    db.put_with_options(
        b"k1",
        b"v1",
        &PutOptions::default(),
        &WriteOptions::default(),
    )
    .await
    .unwrap();
    db.put_with_options(
        b"k2",
        b"v2",
        &PutOptions::default(),
        &WriteOptions::default(),
    )
    .await
    .unwrap();

    db.flush_with_options(FlushOptions {
        flush_type: FlushType::Wal,
    })
    .await
    .unwrap();

    let wal_reader = WalReader::new(path, object_store.clone());
    let wal_files = wal_reader.list(..).await.unwrap();
    assert!(!wal_files.is_empty());

    let mut rows = Vec::new();
    for wal_file in wal_files {
        let mut iter = wal_file
            .iterator()
            .await
            .unwrap()
            .expect("WAL file listed by WalReader should be readable");
        while let Some(entry) = iter.next_entry().await.unwrap() {
            rows.push(entry);
        }
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].key.as_ref(), b"k1");
    assert_eq!(rows[1].key.as_ref(), b"k2");
}
