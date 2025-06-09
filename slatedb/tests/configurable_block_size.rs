use slatedb::config::SstBlockSize;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;

#[tokio::test]
async fn test_configurable_block_size() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with custom block size of 8KB
    let db = Db::builder("/tmp/test_custom_block_size", object_store.clone())
        .with_sst_block_size(SstBlockSize::Block8Kb)
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
        .with_sst_block_size(SstBlockSize::Block1Kb)
        .build()
        .await;

    assert!(db.is_ok());

    // Test with maximum valid block size (64KB)
    let db = Db::builder("/tmp/test_max_block_size", object_store.clone())
        .with_sst_block_size(SstBlockSize::Block64Kb)
        .build()
        .await;

    assert!(db.is_ok());
}

#[tokio::test]
async fn test_block_size_actually_used() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test with a small block size to force multiple blocks
    let db = Db::builder("/tmp/test_block_size_used", object_store.clone())
        .with_sst_block_size(SstBlockSize::Block1Kb) // 1KB blocks
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

#[tokio::test]
async fn test_all_block_sizes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    
    // Test all available block sizes
    let block_sizes = [
        (SstBlockSize::Block1Kb, 1024),
        (SstBlockSize::Block2Kb, 2048),
        (SstBlockSize::Block4Kb, 4096),
        (SstBlockSize::Block8Kb, 8192),
        (SstBlockSize::Block16Kb, 16384),
        (SstBlockSize::Block32Kb, 32768),
        (SstBlockSize::Block64Kb, 65536),
    ];
    
    for (block_size_enum, expected_bytes) in block_sizes {
        // Verify the enum returns the correct byte value
        assert_eq!(block_size_enum.as_bytes(), expected_bytes);
        
        // Verify we can create a DB with each size
        let db = Db::builder(
            format!("/tmp/test_block_size_{}", expected_bytes),
            object_store.clone()
        )
        .with_sst_block_size(block_size_enum)
        .build()
        .await;
        
        assert!(db.is_ok());
    }
}
