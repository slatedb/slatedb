use slatedb::object_store::memory::InMemory;
use slatedb::{Db, Error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("/tmp/slatedb_range_scans", object_store).await?;

    db.put(b"test_key1", b"test_value1").await?;
    db.put(b"test_key2", b"test_value2").await?;
    db.put(b"test_key3", b"test_value3").await?;
    db.put(b"test_key4", b"test_value4").await?;
    db.put(b"test_key10", b"test_value10").await?;

    // Scan over unbound range
    let mut iter = db.scan(..).await?;
    // Scans return bytewise key order
    let expected = [
        ("test_key1", "test_value1"),
        ("test_key10", "test_value10"),
        ("test_key2", "test_value2"),
        ("test_key3", "test_value3"),
        ("test_key4", "test_value4"),
    ];
    for (expected_key, expected_value) in expected {
        let entry = iter.next().await?.unwrap();
        assert_eq!(entry.key.as_ref(), expected_key.as_bytes());
        assert_eq!(entry.value.as_ref(), expected_value.as_bytes());
    }
    assert_eq!(iter.next().await?, None);

    // Scan over bound range
    let mut iter = db.scan("test_key2"..="test_key3").await?;
    let kv2 = iter.next().await?.unwrap();
    assert_eq!(kv2.key, b"test_key2".as_slice());
    assert_eq!(kv2.value, b"test_value2".as_slice());

    let kv3 = iter.next().await?.unwrap();
    assert_eq!(kv3.key, b"test_key3".as_slice());
    assert_eq!(kv3.value, b"test_value3".as_slice());
    assert_eq!(iter.next().await?, None);

    // Seek ahead to next key
    let mut iter = db.scan(..).await?;
    let next_key = b"test_key4";
    iter.seek(next_key).await?;
    let kv4 = iter.next().await?.unwrap();
    assert_eq!(kv4.key, b"test_key4".as_slice());
    assert_eq!(kv4.value, b"test_value4".as_slice());
    assert_eq!(iter.next().await?, None);

    // Scan over prefix
    let mut prefixed = db.scan_prefix(b"test_key1", ..).await?;
    let expected_keys: [&[u8]; 2] = [b"test_key1", b"test_key10"];
    for expected_key in expected_keys {
        let kv = prefixed.next().await?.unwrap();
        assert_eq!(kv.key.as_ref(), expected_key);
    }
    assert_eq!(prefixed.next().await?, None);

    db.close().await?;

    Ok(())
}
