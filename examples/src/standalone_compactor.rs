use slatedb::object_store::local::LocalFileSystem;
use slatedb::CompactorBuilder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let local_root = std::env::temp_dir().join("slatedb-standalone-compactor-tutorial");
    std::fs::create_dir_all(&local_root)?;
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(local_root)?);

    // Build a compactor without opening a Db. The database must already exist (i.e.
    // the manifest has been created).
    let compactor = Arc::new(CompactorBuilder::new("db", object_store).build());

    let compactor_task = {
        let compactor = Arc::clone(&compactor);
        tokio::spawn(async move { compactor.run().await })
    };

    println!("Compactor running. Press Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;

    compactor.stop().await?;
    compactor_task.await??;

    Ok(())
}
