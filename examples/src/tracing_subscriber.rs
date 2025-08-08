#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::disallowed_macros)]
use slatedb::{bytes::Bytes, object_store::memory::InMemory, Db};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber to see the logs
    tracing_subscriber::fmt::init();

    // Setup
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("/tmp/slatedb_tracing_subscriber", object_store).await?;

    // Put
    let key = b"test_key";
    let value = b"test_value";
    db.put(key, value).await?;

    // Get
    assert_eq!(db.get(key).await?, Some(Bytes::from_static(value)));

    // Delete
    db.delete(key).await?;
    assert!(db.get(key).await?.is_none());

    // Close
    db.close().await?;

    Ok(())
}
