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

`put()` returns a `Future` that resolves when the data is durably persisted. Clients that prefer lower latency at the cost of durability can instead use `put_with_options` with `await_durable` set to `false`.

To mitigate read latency and read API costs (GETs), SlateDB will use standard LSM-tree caching techniques: in-memory block caches, compression, bloom filters, and local SST disk caches.

Checkout [slatedb.io](https://slatedb.io) to learn more.

## Get Started

Add the following to your `Cargo.toml`:

```toml
[dependencies]
slatedb = "*"
tokio = "*"
```

Then you can use SlateDB in your Rust code:

```rust
use slatedb::{Db, SlateDBError};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), SlateDBError> {
    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let kv_store = Db::open("/tmp/test_kv_store", object_store).await?;

    // Put
    let key = b"test_key";
    let value = b"test_value";
    kv_store.put(key, value).await?;

    // Get
    assert_eq!(
        kv_store.get(key).await?,
        Some("test_value".into())
    );

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
        assert_eq!(
            item.key,
            format!("test_key{count}").into_bytes()
        );
        assert_eq!(
            item.value,
            format!("test_value{count}").into_bytes()
        );
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
- [x] Clones ([#49](https://github.com/slatedb/slatedb/issues/49))

## Adopters

See who's using SlateDB.

- [Embucket](https://www.embucket.com)
- [Responsive](https://responsive.dev)
- [Tensorlake](https://www.tensorlake.ai)

## Talks

- [Internals of SlateDB: An Embedded Key-Value Store Built on Object Storage](https://www.datacouncil.ai/talks25/internals-of-slatedb-an-embedded-key-value-store-built-on-object-storage) (Vignesh Chandramohan, 2025)
- [Internals of SlateDB — by Vignesh Chandramohan](https://www.youtube.com/watch?v=qqF_zFWqFYk) (Vignesh Chandramohan, 2025)
- [Database Internals - SlateDB](https://www.youtube.com/watch?v=wEAcNoJOBFI) (Chris Riccomini, 2024)
- [Building a Cloud Native LSM on Object Storage](https://www.p99conf.io/session/building-a-cloud-native-lsm-on-object-storage/) (Rohan Desai/Chris Riccomini, 2024)

## License

SlateDB is licensed under the Apache License, Version 2.0.

## Foundation

SlateDB is a member of the [Commonhaus Foundation](https://www.commonhaus.org/).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_reverse_200px.png?raw=true">
  <img src="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_default_200px.png?raw=true">
</picture>
