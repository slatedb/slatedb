---
title: Logging
description: Learn how to configure logging and tracing in SlateDB
---

## Tracing in SlateDB

SlateDB uses the [`tracing`](https://github.com/tokio-rs/tracing) library for logging and diagnostics. This provides structured, contextual logging that helps debug and monitor your database operations.

Here's a basic example showing how to consume tracing logs with `tracing_subscriber` and SlateDB:

```rust
use bytes::Bytes;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use slatedb::Db;
use slatedb::config::DbOptions;
use std::sync::Arc;

#[tokio::main]
async fn main() {

    // Initialize tracing subscriber to see the logs
    tracing_subscriber::fmt::init();

    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = DbOptions::default();
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