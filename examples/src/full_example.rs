#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::disallowed_macros)]
use slatedb::{object_store::memory::InMemory, Db, Error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Setup
    let object_store = Arc::new(InMemory::new());
    let kv_store = Db::open("/tmp/slatedb_full_example", object_store).await?;

    // Put
    let key = b"test_key";
    let value = b"test_value";
    kv_store.put(key, value).await?;

    // Get
    assert_eq!(kv_store.get(key).await?, Some("test_value".into()));

    // Delete
    kv_store.delete(key).await?;
    assert!(kv_store.get(key).await?.is_none());

    kv_store.put(b"test_key1", b"test_value1").await?;
    kv_store.put(b"test_key2", b"test_value2").await?;
    kv_store.put(b"test_key3", b"test_value3").await?;
    kv_store.put(b"test_key4", b"test_value4").await?;

    // Scan over unbound range
    let mut iter = kv_store.scan::<Vec<u8>, _>(..).await?;
    let mut count = 1;
    while let Ok(Some(item)) = iter.next().await {
        assert_eq!(item.key, format!("test_key{count}").into_bytes());
        assert_eq!(item.value, format!("test_value{count}").into_bytes());
        count += 1;
    }

    // Scan over bound range
    let mut iter = kv_store.scan("test_key1"..="test_key2").await?;
    assert_eq!(
        iter.next().await?,
        Some((b"test_key1", b"test_value1").into())
    );
    assert_eq!(
        iter.next().await?,
        Some((b"test_key2", b"test_value2").into())
    );

    // Seek ahead to next key
    let mut iter = kv_store.scan::<Vec<u8>, _>(..).await?;
    let next_key = b"test_key4";
    iter.seek(next_key).await?;
    assert_eq!(
        iter.next().await?,
        Some((b"test_key4", b"test_value4").into())
    );
    assert_eq!(iter.next().await?, None);

    // Close
    kv_store.close().await?;

    Ok(())
}
