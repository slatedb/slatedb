1. Fix the first item in the list.

- website/src/content/docs/docs/get-started/quickstart.mdx:13 says to install tokio with only tokio/macros, but the example it embeds uses #[tokio::main] in examples/src/
  full_example.rs:4. Tokio’s default features are empty, and rt-multi-thread pulls in rt in /Users/chrisriccomini/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-
  1.47.1/Cargo.toml:73. As written, that quick start command is insufficient.
- website/src/content/docs/docs/tutorials/checkpoint.mdx:20 says to use Db::refresh_checkpoint, and website/src/content/docs/docs/tutorials/checkpoint.mdx:27 says to use
  Db::delete_checkpoint. The public methods actually live on Admin, not Db, in slatedb/src/admin.rs:411, and the bundled examples use AdminBuilder in examples/src/
  refresh_checkpoint.rs:15 and examples/src/delete_checkpoint.rs:15.
- website/src/content/docs/docs/design/files.mdx:5 says a DB has three main directories and omits compactions/. Current code has a dedicated compactions store in slatedb/src/
  compactions_store.rs:203, wires it into DB startup in slatedb/src/db/builder.rs:410, and creates the compactions file on compactor init in slatedb/src/
  compactor_state_protocols.rs:178. The storage-layout docs are stale.
- The storage tutorials repeat the same stale layout. website/src/content/docs/docs/tutorials/s3.mdx:101, website/src/content/docs/docs/tutorials/gcs.mdx:90, and website/src/
  content/docs/docs/tutorials/abs.mdx:80 describe only three folders, even though current DBs also use compactions/.
- website/src/content/docs/docs/tutorials/s3.mdx:95 shows objects under s3://slatedb/test/, but the actual example opens /tmp/slatedb_s3_compatible in examples/src/s3_compati
  ble.rs:20. website/src/content/docs/docs/tutorials/abs.mdx:75 shows test_slateDB/, but the example uses /tmp/slatedb_azure_blob_storage in examples/src/azure_blob_storage.r
  s:17. Those sample paths no longer match the code they document.
- website/src/content/docs/docs/tutorials/s3.mdx:115, website/src/content/docs/docs/tutorials/gcs.mdx:109, and website/src/content/docs/docs/tutorials/abs.mdx:100 say each
  WAL .sst file is a WAL “entry”. That is wrong now: a WAL file contains many RowEntrys, as documented in website/src/content/docs/docs/design/change-data-capture.mdx:25 and
  implemented in slatedb/src/wal_reader.rs:11.
- website/src/content/docs/docs/get-started/introduction.mdx:20 says SlateDB has “no local state”. That’s no longer universally true: current config supports a local disk-
  backed object-store cache, including cache_puts, in slatedb/src/config.rs:1223.
- website/src/content/docs/docs/get-started/introduction.mdx:13 says “All object stores support compare-and-swap (CAS) operations.” That blanket claim is too strong for the
  current setup. Even the official S3 example has to explicitly opt into conditional puts via with_conditional_put(...) in examples/src/s3_compatible.rs:16.
- The AWS comparison block in website/src/content/docs/docs/get-started/faq.mdx:48 is stale. The DynamoDB storage/request pricing quoted there is outdated, and website/src/co
  ntent/docs/docs/get-started/faq.mdx:52 compares DynamoDB’s 99.999% figure to S3 Standard, but AWS’s current docs say DynamoDB Standard SLA is 99.99%, Global Tables is 99.99
  9%, and S3 Standard’s service commitment is 99.9%. Sources: DynamoDB pricing (https://aws.amazon.com/dynamodb/pricing/), DynamoDB SLA (https://aws.amazon.com/dynamodb/sla/),
  S3 SLA (https://aws.amazon.com/s3/sla/).
- website/src/content/docs/docs/get-started/faq.mdx:44 quotes old EFS pricing ($0.30/GB-month, $0.03/GB reads, $0.06/GB writes). AWS’s current EFS pricing is throughput/activ
  ity-based and no longer matches that fixed schedule. Source: EFS pricing (https://aws.amazon.com/efs/pricing/).

2. Commit your change using conventional commit syntax.
3. Overwrite PROMPT.md with this prompt again, but with the completed item from the list removed. Everything else should remain identical.

If there are no items in the list remaining, write an empty PROMPT.md file and make codex fail with a non-zero exit code.