use object_store::aws::AmazonS3Builder;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::Db;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Neon Object Storage is S3-compatible. `from_env` reads the variables that
    // `neon env pull` writes: AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID,
    // AWS_SECRET_ACCESS_KEY, and AWS_REGION. A custom endpoint makes the
    // builder send path-style requests, which Neon requires.
    let object_store = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("slatedb")
            .build()?,
    );

    let path = "/tmp/slatedb_neon_object_storage";
    let db = Db::open(path, object_store.clone()).await?;

    db.put(b"k1", b"value1").await?;
    println!("{:?}", db.get(b"k1").await?);

    for i in 0..1000 {
        db.put(format!("key{i}").as_bytes(), format!("value{i}").as_bytes())
            .await?;
    }

    // Write the memtable to an L0 SST in the `compacted` directory.
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;

    // Reopen the db and read a value back from Neon.
    let db = Db::open(path, object_store).await?;
    println!("{:?}", db.get(b"key999").await?);
    db.close().await?;

    Ok(())
}
