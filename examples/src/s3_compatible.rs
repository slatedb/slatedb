use object_store::aws::S3ConditionalPut;
use slatedb::Db;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            // These will be different if you are using real AWS
            .with_allow_http(true)
            .with_endpoint("http://localhost:4566")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_bucket_name("slatedb")
            .with_region("us-east-1")
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?,
    );

    let db = Db::open("/tmp/slatedb_s3_compatible", object_store.clone()).await?;

    // Call db.put with a key and a 64 meg value to trigger L0 SST flush
    let value: Vec<u8> = vec![0; 64 * 1024 * 1024];
    db.put(b"k1", value.as_slice()).await?;
    db.close().await?;

    Ok(())
}
