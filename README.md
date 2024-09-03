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

Add the following to your `Cargo.toml` to use SlateDB:

```toml
[dependencies]
slatedb = "*"
```

Then you can use SlateDB in your Rust code:

```rust
use bytes::Bytes;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use slatedb::db::Db;
use slatedb::in_memory::BlockCacheOptions;
use slatedb::config::{CompactorOptions, DbOptions};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = DbOptions {
        flush_interval: Duration::from_millis(100),
        manifest_poll_interval: Duration::from_millis(100),
        #[cfg(feature = "wal_disable")] wal_enabled: true,
        min_filter_keys: 10,
        l0_sst_size_bytes: 128,
        l0_max_ssts: 8,
        max_unflushed_memtable: 2,
        compactor_options: Some(CompactorOptions::default()),
        compression_codec: None,
        block_cache_options: Some(BlockCacheOptions::default()),
    };
    let kv_store = Db::open_with_opts(
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

## Documentation

Visit [slatedb.io](https://slatedb.io) to learn more.

## Features

SlateDB is currently in the early stages of development. It is not yet ready for production use.

- [x] Basic API (get, put, delete)
- [x] SSTs on object storage
- [ ] Range queries ([#8](https://github.com/slatedb/slatedb/issues/8))
- [ ] Block cache ([#15](https://github.com/slatedb/slatedb/issues/15))
- [ ] Disk cache ([#9](https://github.com/slatedb/slatedb/issues/9))
- [x] Compression ([#10](https://github.com/slatedb/slatedb/issues/10))
- [x] Bloom filters ([#11](https://github.com/slatedb/slatedb/issues/11))
- [x] Manifest persistence ([#14](https://github.com/slatedb/slatedb/issues/14))
- [x] Compaction ([#7](https://github.com/slatedb/slatedb/issues/7))
- [ ] Transactions

## License

SlateDB is licensed under the Apache License, Version 2.0.
