use slatedb::config::Settings;
use slatedb::{Db, Error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize database
    let object_store = Arc::new(slatedb::object_store::memory::InMemory::new());
    let db = Db::builder("my_db", object_store)
        .with_settings(Settings::default())
        .build()
        .await?;

    // Write some data to the database
    db.put(b"apple", b"red").await?;
    db.put(b"banana", b"yellow").await?;
    db.put(b"cherry", b"red").await?;

    // Create a snapshot
    let snapshot = db.snapshot().await?;
    println!("Snapshot created successfully");

    // After creating snapshot, make changes to the database
    db.put(b"apple", b"green").await?; // Modify existing
    db.put(b"date", b"brown").await?; // Add new key
    db.delete(b"banana").await?; // Delete existing

    // Scan from snapshot - should see original data
    println!("\nScanning snapshot:");
    let mut snapshot_iter = snapshot.scan(b"a".to_vec()..=b"z".to_vec()).await?;
    while let Some(kv) = snapshot_iter.next().await? {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    // Scan from database - should see updated data
    println!("\nScanning database:");
    let mut db_iter = db.scan(b"a".to_vec()..=b"z".to_vec()).await?;
    while let Some(kv) = db_iter.next().await? {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    Ok(())
}
