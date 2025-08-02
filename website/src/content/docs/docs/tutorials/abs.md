---
title: Connect SlateDB to Azure Blob Storage
description: Learn how to connect SlateDB to Azure Blob Storage
---

This tutorial shows you how to use SlateDB on Azure Blob Storage (ABS). You would need an ABS account to complete the tutorial.

## Setup

[Install](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) the Azure CLI.

## Create Storage account

The following steps creates a storage account and list the keys. This section can be skipped if you already have a storage account created.

```bash
# Set storage account names
StorageAccountName=<ReplaceWithAccountName>
ContainerName=<ReplaceWithContainerName>
ResourceGroupName=<ReplaceWithResourceGroupName>

# Login
az login

# Create Resource Group in the default subscription.
az group create --name $ResourceGroupName --location westus

# Create Azure Storage account.
az storage account create --name $StorageAccountName --resource-group $ResourceGroupName --location westus --sku Standard_LRS

# Create a storage container
az storage container create --name $ContainerName --account-name $StorageAccountName

# Get the keys.
az storage account keys list --resource-group $ResourceGroupName --account-name $StorageAccountName
```

## Create a project

Let's start by creating a new Rust project:

```bash
cargo init slatedb-abs
cd slatedb-abs
```

## Add dependencies

Now add SlateDB and the `object_store` crate to your `Cargo.toml`:

```bash
cargo add slatedb object-store --features object-store/azure
```

:::note

If you see "`object_store::path::Path` and `object_store::path::Path` have similar names, but are actually distinct types", you might need to pin the `object_store` version to match `slatedb`'s `object_store` version. SlateDB also exports `slatedb::object_store` for convenience, if you'd rather use that.

:::

## Write some code

This code demonstrates puts that wait for results to be durable, and then puts that do not wait.

```rust
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::config::DbOptions;
use slatedb::db::Db;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // construct azure blob object store.
    let blob_store: Arc<dyn ObjectStore>  = Arc::new(MicrosoftAzureBuilder::new()
        .with_account("<REPLACEWITHACCOUNTNAME>")
        .with_access_key("<REPLACEWITHACCOUNTKEY>")
        .with_container_name("<REPLACEWITHCONTAINERNAME>")
        .build()
        .unwrap());

    // create the db.
    let db_options = DbOptions::default();
    let path = Path::from("test_slateDB");

    println!("Opening the db");
    let db = Db::open_with_opts(path.clone(), db_options, blob_store.clone())
        .await
        .expect("failed to open db");

    // Put a value and wait for the flush.
    println!("Writing a value and waiting for flush");
    db.put(b"k1", b"value1").await;
    println!("{:?}", db.get(b"k1").await.unwrap());

    // Put 1000 keys, do not wait for it to be durable
    println!("Writing 1000 keys without waiting for flush");
    let write_options = slatedb::config::WriteOptions {
        await_durable: false,
    };
    for i in 0..1000 {
        db.put_with_options(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
            &write_options,
        )
        .await;
    }

    // flush to make the writes durable.
    println!("Flushing the writes and closing the db");
    db.flush().await.expect("failed to flush");
    db.close().await.expect("failed to close db");

    // reopen the db and read the value.
    println!("Reopening the db");
    let db_reopened = Db::open_with_opts(path.clone(), DbOptions::default(), blob_store.clone())
        .await
        .expect("failed to open db");
    println!("Reading the value from the reopened db");

    // read 20 keys
    for i in 0..20 {
        println!(
            "{:?}",
            db_reopened
                .get(format!("key{}", i).as_bytes())
                .await
                .unwrap()
        );
    }
    db_reopened.close().await.expect("failed to close db");
}

```

## Check the blob contents

```bash
az storage blob list --container-name $ContainerName --account-name $StorageAccountName --prefix "test_slateDB/" --delimiter "/" --output table
                           wal/
                           manifest/
```

There are three folders:

- `manifest`: Contains the manifest files. Manifest files defines the state of the DB, including the set of SSTs that are part of the DB.
- `wal`: Contains the write-ahead log files.
- `compacted`: Contains the compacted SST files. This short example does not create compacted files.

Let's check the `wal` folder. 

```bash
az storage blob list --container-name $ContainerName --account-name $StorageAccountName --prefix "test_slateDB/wal/" --delimiter "/" --output table

Name                                       Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-----------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
test_slateDB/wal/00000000000000000001.sst  BlockBlob    Hot          64        application/octet-stream  2024-09-07T01:15:49+00:00
test_slateDB/wal/00000000000000000002.sst  BlockBlob    Hot          138       application/octet-stream  2024-09-07T01:15:49+00:00
test_slateDB/wal/00000000000000000003.sst  BlockBlob    Hot          23388     application/octet-stream  2024-09-07T01:15:49+00:00
test_slateDB/wal/00000000000000000004.sst  BlockBlob    Hot          64        application/octet-stream  2024-09-07T01:15:50+00:00

```

Each of these SST files is a write-ahead log (WAL) entry. They get flushed based on the `flush_interval` config or when `flush` is called explicitly.

```
