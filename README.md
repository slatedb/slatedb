<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb-website/blob/main/assets/png/gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

<a href="https://crates.io/crates/slatedb">![Crates.io Version](https://img.shields.io/crates/v/slatedb?style=flat-square)</a>
![GitHub License](https://img.shields.io/github/license/slatedb/slatedb?style=flat-square)
<a href="https://slatedb.io">![slatedb.io](https://img.shields.io/badge/site-slatedb.io-00A1FF?style=flat-square)</a>
<a href="https://discord.gg/mHYmGy5MgA">![Discord](https://img.shields.io/discord/1232385660460204122?style=flat-square)</a>
<a href="https://docs.rs/slatedb/latest/slatedb/">![Docs](https://img.shields.io/badge/docs-docs.rs-00A1FF?style=flat-square)</a>

## Introduction

[SlateDB](https://slatedb.io) is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

To mitigate write latency, SlateDB provides an async `put` method. Clients that prefer strong durability can `await` on `put` until the MemTable is flushed to object storage (trading latency for durability). Clients that prefer lower latency can simply ignore the future returned by `put`.

To mitigate read latency and read API costs (GETs), SlateDB will use standard LSM-tree caching techniques: in-memory block caches, compression, bloom filters, and local SST disk caches.

Checkout [slatedb.io](https://slatedb.io) to learn more.

## Get Started

Add the following to your `Cargo.toml`:

```toml
[dependencies]
slatedb = "*"
bytes = "*"
object_store = "*"
tokio = "*"
```

Then you can use SlateDB in your Rust code:

```rust
use bytes::Bytes;
use slatedb::db::Db;
use slatedb::config::DbOptions;
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = DbOptions::default();
    let kv_store = Db::open_with_opts(
        "/tmp/test_kv_store",
        options,
        object_store,
    )
    .await
    .unwrap();

    // Put
    let key = b"test_key";
    let value = b"test_value";
    kv_store.put(key, value).await;

    // Get
    assert_eq!(
        kv_store.get(key).await.unwrap(),
        Some(Bytes::from_static(value))
    );

    // Delete
    kv_store.delete(key).await;
    assert!(kv_store.get(key).await.unwrap().is_none());

    kv_store.put(b"test_key1", b"test_value1").await;
    kv_store.put(b"test_key2", b"test_value2").await;
    kv_store.put(b"test_key3", b"test_value3").await;
    kv_store.put(b"test_key4", b"test_value4").await;

    // Scan over unbound range
    let mut iter = kv_store.scan(..).await.unwrap();
    let mut count = 1;
    while let Ok(Some(item)) = iter.next().await {
        assert_eq!(
            item.key,
            Bytes::from(format!("test_key{count}").into_bytes())
        );
        assert_eq!(
            item.value,
            Bytes::from(format!("test_value{count}").into_bytes())
        );
        count += 1;
    }

    // Scan over bound range
    let start_key = Bytes::from_static(b"test_key1");
    let end_key = Bytes::from_static(b"test_key2");
    let mut iter = kv_store.scan(start_key..=end_key).await.unwrap();
    assert_eq!(
        iter.next().await.unwrap(),
        Some((b"test_key1" as &[u8], b"test_value1" as &[u8]).into())
    );
    assert_eq!(
        iter.next().await.unwrap(),
        Some((b"test_key2" as &[u8], b"test_value2" as &[u8]).into())
    );

    // Seek ahead to next key
    let mut iter = kv_store.scan(..).await.unwrap();
    let next_key = Bytes::from_static(b"test_key4");
    iter.seek(next_key).await;
    assert_eq!(
        iter.next().await.unwrap(),
        Some((b"test_key4" as &[u8], b"test_value4" as &[u8]).into())
    );
    assert_eq!(iter.next().await.unwrap(), None);

    // Close
    kv_store.close().await.unwrap();
}
```

SlateDB uses the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate to interact with object storage, and therefore supports any object storage that implements the `ObjectStore` trait. You can use the crate in your project to interact with any object storage that implements the `ObjectStore` trait. SlateDB also re-exports the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate for your convenience.

## Documentation

Visit [slatedb.io](https://slatedb.io) to learn more.

## Features

SlateDB is currently in the early stages of development. It is not yet ready for production use.

- [x] Basic API (get, put, delete)
- [x] SSTs on object storage
- [x] Range queries ([#8](https://github.com/slatedb/slatedb/issues/8))
- [x] Block cache ([#15](https://github.com/slatedb/slatedb/issues/15))
- [x] Disk cache ([#9](https://github.com/slatedb/slatedb/issues/9))
- [x] Compression ([#10](https://github.com/slatedb/slatedb/issues/10))
- [x] Bloom filters ([#11](https://github.com/slatedb/slatedb/issues/11))
- [x] Manifest persistence ([#14](https://github.com/slatedb/slatedb/issues/14))
- [x] Compaction ([#7](https://github.com/slatedb/slatedb/issues/7))
- [ ] Transactions
- [ ] Merge operator ([#328](https://github.com/slatedb/slatedb/issues/328))

## License

SlateDB is licensed under the Apache License, Version 2.0.
