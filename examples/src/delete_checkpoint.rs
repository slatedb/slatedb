use slatedb::admin::AdminBuilder;
use slatedb::config::CheckpointOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = "/tmp/slatedb-checkpoint";
    let object_store = Arc::new(InMemory::new());
    let db = Db::open(path, object_store.clone()).await?;

    db.close().await?;

    let admin = AdminBuilder::new(path, object_store).build();

    let result = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(Duration::from_secs(3600)), // expires in 1 hour
            ..Default::default()
        })
        .await?;

    admin.delete_checkpoint(result.id).await?;

    println!("Deleted checkpoint: ID = {}", result.id);

    Ok(())
}
