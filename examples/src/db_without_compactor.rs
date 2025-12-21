use slatedb::config::Settings;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::Db;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // This example uses a local object store so it can be shared with a separate
    // compactor process. In production, this could also be an object store like S3.
    let local_root = std::env::temp_dir().join("slatedb-standalone-compactor-tutorial");
    std::fs::create_dir_all(&local_root)?;
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(local_root)?);

    // Disable the embedded compactor by clearing compactor options.
    let settings = Settings {
        compactor_options: None,
        ..Default::default()
    };

    let db = Db::builder("db", object_store)
        .with_settings(settings)
        .build()
        .await?;

    db.put(b"hello", b"world").await?;
    db.flush().await?;
    db.close().await?;

    Ok(())
}
