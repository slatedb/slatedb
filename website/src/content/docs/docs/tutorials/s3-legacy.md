---
title: Connect SlateDB to S3 (Legacy)
description: Learn how to connect SlateDB to Amazon S3 using the legacy approach
---

This tutorial shows you how to connect SlateDB to S3. We'll use LocalStack to simulate S3 and DynamoDB.

:::warning

This tutorial is deprecated. See the [Connect SlateDB to S3](/docs/tutorials/s3) tutorial.

SlateDB no longer requires DynamoDB when working with S3. However, we've kept this tutorial for users that wish to use DynamoDB to manage [compare-and-swap](https://docs.rs/object_store/latest/object_store/aws/enum.S3ConditionalPut.html#variant.Dynamo) (CAS) anyway.

:::

## Create a project

Let's start by creating a new Rust project:

```bash
cargo init slatedb-playground
cd slatedb-playground
```

## Add dependencies

Now add SlateDB and the `object_store` crate to your `Cargo.toml`:

```bash
cargo add slatedb object-store tokio --features object-store/aws
```

:::note

If you see "`object_store::path::Path` and `object_store::path::Path` have similar names, but are actually distinct types", you might need to pin the `object_store` version to match `slatedb`'s `object_store` version.

:::

## Setup

You will need to have [LocalStack](https://localstack.cloud/) running. You can install it using [Homebrew](https://brew.sh/):

```bash
brew install localstack/tap/localstack-cli
```

```bash
localstack start -d
```

For a more detailed setup, see the [LocalStack documentation](https://docs.localstack.cloud/).

You'll also need the AWS CLI:

```bash
brew install awscli
```

## Initialize AWS

SlateDB requires a bucket and a DynamoDB table to work with S3.

### Create your S3 bucket:

```bash
# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket slatedb --region us-east-1
```

### Create your DynamoDB table:

SlateDB has not yet implemented CAS for S3 ([#164](https://github.com/slatedb/slatedb/issues/164)). Instead, SlateDB uses DynamoDB to manage CAS (via the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate).

```bash
# Create DynamoDB table
aws --endpoint-url=http://localhost:4566 dynamodb create-table --table-name slatedb --key-schema AttributeName=path,KeyType=HASH AttributeName=etag,KeyType=RANGE --attribute-definitions AttributeName=path,AttributeType=S AttributeName=etag,AttributeType=S --billing-mode PAY_PER_REQUEST
aws --endpoint-url=http://localhost:4566 dynamodb update-time-to-live --table-name slatedb --time-to-live-specification Enabled=true,AttributeName=ttl
```

## Write some code

Stick this into your `src/main.rs` file:

```rust
use object_store::aws::{DynamoCommit, S3ConditionalPut};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::config::DbOptions;
use slatedb::db::Db;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let db_options = DbOptions::default();
    let path = Path::from("test");
    let os: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            // These will be different if you are using real AWS
            .with_allow_http(true)
            .with_endpoint("http://localhost:4566")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_bucket_name("slatedb")
            .with_region("us-east-1")
            .with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new("slatedb".to_string())))
            .build()
            .expect("failed to create object store")
    );
    let db = Db::open_with_opts(path.clone(), db_options, os.clone())
        .await
        .expect("failed to open db");

    // Call db.put with a key and a 64 meg value to trigger L0 SST flush
    let value: Vec<u8> = vec![0; 64 * 1024 * 1024];
    db.put(b"k1", value.as_slice()).await.expect("failed to put");
    db.close().await.expect("failed to close db");
}
```

:::note

Our code example writes a 64 MiB value to object storage to trigger an L0 SST flush. This is just to show the
`compacted` directory in your bucket.

:::

## Run the code

Now you can run the code:

```bash
cargo run
```

This will write a 64 MiB value to SlateDB.

## Check the results

Now' let's check the root of the bucket:

```bash
% aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb/test/
                           PRE compacted/
                           PRE manifest/
                           PRE wal/
```

There are three folders:

- `compacted`: Contains the compacted SST files.
- `manifest`: Contains the manifest files.
- `wal`: Contains the write-ahead log files.

Let's check the `wal` folder:

```bash
% aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb/test/wal/
2024-09-04 18:05:57         64 00000000000000000001.sst
2024-09-04 18:05:58   67108996 00000000000000000002.sst
```

Each of these SST files is a write-ahead log entry. They get flushed based on the `flush_interval` config. The last entry is 64 MiB, which is the value we wrote.

Finally, let's check the `compacted` folder:

```bash
% aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb/test/compacted/ 
2024-09-04 18:05:59   67108996 01J6ZVEZ394GCJT1PHZYY1NZGP.sst
```
Again, we see the 64 MiB SST file. This is the L0 SST file that was flushed with our value. Over time, the WAL entries will be removed, and the L0 SSTs will be compacted into higher levels.
