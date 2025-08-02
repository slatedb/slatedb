---
title: Checkpoint and Restore
description: Learn how to checkpoint and restore SlateDB databases
---

## Creating a Checkpoint

To create a checkpoint, use the `create_checkpoint` function provided by SlateDB.  
This operation captures a consistent and durable view of the database state, anchored to a specific manifest version.
```rust
use slatedb::{
    admin,
    config::{DbOptions, CheckpointOptions},
};
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let path = Path::from("/my/test/db");

    let db = slatedb::Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
        .await?;


    db.close().await?;

    let result = admin::create_checkpoint(
        path,
        object_store.clone(),
        &CheckpointOptions {
            lifetime: Some(Duration::from_secs(3600)), // expires in 1 hour
            ..Default::default()
        },
    )
    .await?;

    println!("Created checkpoint: ID = {}, Manifest = {}", result.id, result.manifest_id);

    Ok(())
}

## Refreshing a Checkpoint

To extend the lifetime of an existing checkpoint, use `Db::refresh_checkpoint`. 
This updates the checkpoint’s expiration time, ensuring that the referenced SSTs remain protected from garbage collection for the specified duration.

```rust
use slatedb::{
    admin,
    config::{DbOptions, CheckpointOptions},
    Db,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/tmp/refresh_checkpoint");

    let db = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone()).await?;
    db.close().await?;

    let result = admin::create_checkpoint(
        path.clone(),
        object_store.clone(),
        &CheckpointOptions {
            lifetime: Some(Duration::from_secs(300)),
            ..Default::default()
        },
    )
    .await?;

    println!("Created checkpoint with 5 min lifetime: ID = {}", result.id);

    Db::refresh_checkpoint(
        &path,
        object_store.clone(),
        result.id,
        Some(Duration::from_secs(7200)),
    )
    .await?;

    println!("Checkpoint refreshed with extended 2-hour lifetime.");

    Ok(())
}

## Deleting a Checkpoint

To remove a checkpoint, use `Db::delete_checkpoint`. 
This deletes the checkpoint metadata from the manifest, allowing the garbage collector to eventually reclaim any unreferenced SST files associated with that checkpoint.

```rust
use slatedb::{
    admin,
    config::{DbOptions, CheckpointOptions},
    Db,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/tmp/delete_checkpoint");

    let db = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone()).await?;
    db.close().await?;

    let result = admin::create_checkpoint(
        path.clone(),
        object_store.clone(),
        &CheckpointOptions::default(),
    )
    .await?;

    println!("Created checkpoint: ID = {}", result.id);

    Db::delete_checkpoint(&path, object_store.clone(), result.id).await?;
    println!("Checkpoint deleted.");

    Ok(())
}
```