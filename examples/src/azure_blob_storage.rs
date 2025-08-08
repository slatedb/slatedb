#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::disallowed_macros)]
use object_store::azure::MicrosoftAzureBuilder;
use slatedb::{config::PutOptions, Db};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // construct azure blob object store.
    let blob_store = Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account("<REPLACEWITHACCOUNTNAME>")
            .with_access_key("<REPLACEWITHACCOUNTKEY>")
            .with_container_name("<REPLACEWITHCONTAINERNAME>")
            .build()?,
    );

    println!("Opening the db");
    let path = "/tmp/slatedb_azure_blob_storage";
    let db = Db::open(path, blob_store.clone()).await?;

    // Put a value and wait for the flush.
    println!("Writing a value and waiting for flush");
    db.put(b"k1", b"value1").await?;
    println!("{:?}", db.get(b"k1").await?);

    // Put 1000 keys, do not wait for it to be durable
    println!("Writing 1000 keys without waiting for flush");
    let write_options = slatedb::config::WriteOptions {
        await_durable: false,
    };
    for i in 0..1000 {
        db.put_with_options(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
            &PutOptions::default(),
            &write_options,
        )
        .await?;
    }

    // flush to make the writes durable.
    println!("Flushing the writes and closing the db");
    db.flush().await?;
    db.close().await?;

    // reopen the db and read the value.
    println!("Reopening the db");
    let db_reopened = Db::open(path, blob_store).await?;
    println!("Reading the value from the reopened db");

    // read 20 keys
    for i in 0..20 {
        println!(
            "{:?}",
            db_reopened.get(format!("key{}", i).as_bytes()).await
        );
    }
    db_reopened.close().await?;

    Ok(())
}
