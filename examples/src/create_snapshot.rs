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
    db.put(b"key1", b"original_value").await?;
    db.put(b"key2", b"data_before_snapshot").await?;

    // Create a snapshot
    let snapshot = db.snapshot().await?;
    println!("Snapshot created successfully");

    // After creating snapshot, make changes to the database
    db.put(b"key1", b"modified_value").await?;

    // Read from snapshot - should see original data
    let snapshot_value1 = snapshot.get(b"key1").await?;
    println!(
        "Snapshot key1: {:?}",
        snapshot_value1.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Read from database - should see updated data
    let db_value1 = db.get(b"key1").await?;
    println!(
        "Database key1: {:?}",
        db_value1.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    Ok(())
}
