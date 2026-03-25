1. Fix the first item in the list.

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
