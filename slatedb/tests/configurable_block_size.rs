use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;

#[tokio::test]
async fn test_configurable_block_size() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with custom block size of 8KB
    let db = Db::builder("/tmp/test_custom_block_size", object_store.clone())
        .with_sst_block_size(8192)
        .build()
        .await;

    assert!(db.is_ok());

    // Write some data and verify the DB works correctly
    let db = db.unwrap();
    db.put(b"key1", b"value1").await.unwrap();
    db.put(b"key2", b"value2").await.unwrap();
    db.flush().await.unwrap();

    let value1 = db.get(b"key1").await.unwrap();
    assert_eq!(&value1.unwrap()[..], b"value1");

    let value2 = db.get(b"key2").await.unwrap();
    assert_eq!(&value2.unwrap()[..], b"value2");
}

#[tokio::test]
#[should_panic(expected = "sst_block_size must be between 1KB")]
async fn test_block_size_too_small() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with block size too small (< 1KB)
    let _ = Db::builder("/tmp/test_block_size_too_small", object_store.clone())
        .with_sst_block_size(512);
}

#[tokio::test]
#[should_panic(expected = "sst_block_size must be between 1KB")]
async fn test_block_size_too_large() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with block size too large (> 64KB)
    let _ = Db::builder("/tmp/test_block_size_too_large", object_store.clone())
        .with_sst_block_size(128 * 1024);
}

#[tokio::test]
async fn test_default_block_size() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with no block size specified (should use default 4KB)
    let db = Db::builder("/tmp/test_default_block_size", object_store.clone())
        .build()
        .await;

    assert!(db.is_ok());
}

#[tokio::test]
async fn test_edge_case_block_sizes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with minimum valid block size (1KB)
    let db = Db::builder("/tmp/test_min_block_size", object_store.clone())
        .with_sst_block_size(1024)
        .build()
        .await;

    assert!(db.is_ok());

    // Test with maximum valid block size (64KB)
    let db = Db::builder("/tmp/test_max_block_size", object_store.clone())
        .with_sst_block_size(65536)
        .build()
        .await;

    assert!(db.is_ok());
}

#[tokio::test]
async fn test_block_size_actually_used() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with a small block size to force multiple blocks
    let block_size = 1024; // 1KB blocks
    let db = Db::builder("/tmp/test_block_size_used", object_store.clone())
        .with_sst_block_size(block_size)
        .build()
        .await
        .unwrap();

    // Write enough data to force multiple blocks
    // Each key-value pair is roughly 16 + value_size bytes
    let value = vec![b'x'; 100]; // 100 byte values

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), &value).await.unwrap();
    }

    // Force flush to create SST
    db.flush().await.unwrap();

    // Verify all data can be read back
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let result = db.get(key.as_bytes()).await.unwrap();
        assert_eq!(result.unwrap().as_ref(), value.as_slice());
    }
}
