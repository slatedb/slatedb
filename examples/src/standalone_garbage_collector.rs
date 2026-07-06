use slatedb::object_store::local::LocalFileSystem;
use slatedb::GarbageCollectorBuilder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let local_root = std::env::temp_dir().join("slatedb-standalone-gc-tutorial");
    std::fs::create_dir_all(&local_root)?;
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(local_root)?);

    // Build a garbage collector without opening a Db. The database must already
    // exist (i.e. the manifest has been created).
    let gc = Arc::new(GarbageCollectorBuilder::new("db", object_store).build());

    let gc_task = {
        let gc = Arc::clone(&gc);
        tokio::spawn(async move { gc.run().await })
    };

    println!("Garbage collector running. Press Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;

    gc.stop().await?;
    gc_task.await??;

    Ok(())
}
