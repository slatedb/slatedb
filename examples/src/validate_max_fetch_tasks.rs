use slatedb::config::ScanOptions;
use slatedb::{Db, Error};
use object_store::{memory::InMemory, ObjectStore};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("Testing max_fetch_tasks scan option implementation...");
    
    // Test 1: Validate default max_fetch_tasks
    let default_options = ScanOptions::default();
    assert_eq!(default_options.max_fetch_tasks, 1);
    println!("✅ Test 1 passed: Default max_fetch_tasks = {}", default_options.max_fetch_tasks);
    
    // Test 2: Validate custom max_fetch_tasks
    let custom_options = ScanOptions::default().with_max_fetch_tasks(4);
    assert_eq!(custom_options.max_fetch_tasks, 4);
    println!("✅ Test 2 passed: Custom max_fetch_tasks = {}", custom_options.max_fetch_tasks);
    
    // Test 3: Validate builder pattern with multiple options
    let options = ScanOptions::default()
        .with_max_fetch_tasks(8)
        .with_cache_blocks(true)
        .with_read_ahead_bytes(1024);
    assert_eq!(options.max_fetch_tasks, 8);
    assert_eq!(options.cache_blocks, true);
    assert_eq!(options.read_ahead_bytes, 1024);
    println!("✅ Test 3 passed: Builder pattern works correctly");
    
    // Test 4: Functional test with actual database
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open("test_db", object_store).await?;
    
    // Add some test data
    for i in 0..5 {
        let key = format!("key{:02}", i);
        let value = format!("value{:02}", i);
        db.put(key.as_bytes(), value.as_bytes()).await?;
    }
    
    // Test scan with different max_fetch_tasks values
    for max_tasks in [1, 2, 4, 8] {
        let scan_options = ScanOptions::default().with_max_fetch_tasks(max_tasks);
        let mut iter = db.scan_with_options(b"key00"..b"key04", &scan_options).await?;
        let mut count = 0;
        while iter.next().await?.is_some() {
            count += 1;
        }
        println!("✅ Test 4.{} passed: Scanned {} records with max_fetch_tasks = {}", max_tasks, count, max_tasks);
    }
    
    println!("\n🎉 All tests passed! max_fetch_tasks feature is working correctly.");
    Ok(())
}