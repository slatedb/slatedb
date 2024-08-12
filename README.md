<div align="center">
  <a href="https://slatedb.io"><img src="https://github.com/slatedb/slatedb-website/blob/main/assets/png/gh-banner.png?raw=true" alt="SlateDB"></a>
</div>

## Introduction

SlateDB is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

To mitigate write latency, SlateDB provides an async `put` method. Clients that prefer strong durability can `await` on `put` until the MemTable is flushed to object storage (trading latency for durability). Clients that prefer lower latency can simply ignore the future returned by `put`.

To mitigate read latency and read API costs (GETs), SlateDB uses standard LSM-tree caching techniques: in-memory block caches, compression, bloom filters, and local SST disk caches.

SlateDB has a configurable compaction layer. See the [compaction design document](https://github.com/slatedb/slatedb/blob/main/docs/0002-compaction.md) for more information.

## Get Started

SlateDB is not published to crates.io yet. To use SlateDB, add the following to your `Cargo.toml`:

```toml
[dependencies]
slatedb = { git = "https://github.com/slatedb/slatedb.git" }
```

Then you can use SlateDB in your Rust code:

```rust
use bytes::Bytes;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use slatedb::db:Db;
use slatedb::config::{CompactorOptions, DbOptions};
use std::{sync::Arc, time::Duration};

#[tokio::main]
fn main() {
    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = DbOptions {
        flush_ms: 100,
        manifest_poll_interval: Duration::from_millis(100),
        min_filter_keys: 10,
        l0_sst_size_bytes: 128,
        compactor_options: Some(CompactorOptions::default()),
    };
    let kv_store = Db::open(
        Path::from("/tmp/test_kv_store"),
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

    // Close
    kv_store.close().await.unwrap();
}
```

SlateDB uses the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate to interact with object storage, and therefore supports any object storage that implements the `ObjectStore` trait.

## Roadmap

SlateDB is currently in the early stages of development. It is not yet ready for production use.

- [x] Basic API (get, put, delete)
- [x] SSTs on object storage
- [ ] Range queries ([#8](https://github.com/slatedb/slatedb/issues/8))
- [ ] Block cache ([#15](https://github.com/slatedb/slatedb/issues/15))
- [ ] Disk cache ([#9](https://github.com/slatedb/slatedb/issues/9))
- [ ] Compression ([#10](https://github.com/slatedb/slatedb/issues/10))
- [x] Bloom filters ([#11](https://github.com/slatedb/slatedb/issues/11))
- [ ] Manifest persistence ([#14](https://github.com/slatedb/slatedb/issues/14))
- [ ] Compaction ([#7](https://github.com/slatedb/slatedb/issues/7))
- [ ] Transactions

## License

SlateDB is licensed under the Apache License, Version 2.0.
