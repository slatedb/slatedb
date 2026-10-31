//! `DbBuilder::with_write_runtime` puts the batch-writer on a current-thread runtime the writing
//! thread drives, so a write is a task switch rather than a cross-thread wake-up. The caller then
//! drives that runtime for everything that goes through the batch-writer — writes, `flush`,
//! `close`, `create_checkpoint` — or the call hangs. Reads can run anywhere.

use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::Db;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

fn main() -> anyhow::Result<()> {
    let local_root = std::env::temp_dir().join("slatedb-write-runtime-tutorial");
    std::fs::create_dir_all(&local_root)?;
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(local_root)?);

    // Opens the Db, serves reads, and hosts the flusher, compactor and GC.
    let background = Runtime::new()?;

    // Hosts the batch-writer. Timer on for the monotonic clock; no IO driver, which a
    // current-thread runtime would poll on every write.
    let writer = Builder::new_current_thread().enable_time().build()?;

    let db = background.block_on(
        Db::builder("db", object_store)
            .with_write_runtime(writer.handle().clone())
            .build(),
    )?;

    // Write path — on the write runtime.
    writer.block_on(db.put(b"hello", b"world"))?;
    writer.block_on(db.flush())?;
    let checkpoint = writer
        .block_on(db.create_checkpoint(CheckpointScope::All, &CheckpointOptions::default()))?;
    println!("created checkpoint {}", checkpoint.id);

    // Reads — anywhere.
    let value = background.block_on(db.get(b"hello"))?;
    assert_eq!(value.as_deref(), Some(b"world".as_ref()));

    // close goes through the batch-writer, so it too uses the write runtime.
    writer.block_on(db.close())?;

    Ok(())
}
