#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::disallowed_macros)]
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, Error};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("/tmp/slatedb_simple_example", object_store).await?;

    db.put(b"key", b"value").await?;

    match db.get(b"key").await? {
        Some(value) => println!("value: {value:?}"),
        None => println!("value not found"),
    }

    db.close().await?;

    Ok(())
}
