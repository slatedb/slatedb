---
title: FAQ
description: Frequently asked questions about SlateDB
---

## Is SlateDB for OLTP or OLAP?

SlateDB is designed for key/value (KV) online transaction processing (OLTP) workloads. It is optimized for lowish-latency, high-throughput writes. It is not optimized for analytical queries that scan large amounts of columnar data. For online analytical processing (OLAP) workloads, we recommend checking out [Tonbo](https://github.com/tonbo-io/tonbo).

## Can't I use S3 as a key-value store?

You can definitely use S3 as a key-value store. An object path would represent a key and the object its value. But then you pay one PUT per write, which gets expensive.

To make writes cheaper, you will probably want to batch writes (write multiple key-value pairs in a single `PUT` call). Batched key-value pairs need to be encoded/decoded, and a sorted strings table (SST) is a natural fit. Once you have SSTs, a log-structured merge-tree (LSM) is a natural fit.

## Why does SlateDB have a write-ahead log?

Some developers have asked why SlateDB needs a write-ahead log (WAL) if we're batching writes. Couldn't we just write the batches directly to level 0 (L0) as an SST?

We opted to have the WAL separate from the L0 SST so that we could frequently write SSTs without increasing the number of L0 SSTs we have. Since reads are served from L0 SSTs, having too many of them would result in a very large amount of metadata that needs to be managed in memory. By contrast, we don't serve reads from the durable WAL, we only use it for recovery.

A separate WAL lets SlateDB frequently flush writes to object storage (to reduce durable write latency) without having to worry about the number of L0 SSTs we have.

## How is this different from RocksDB-cloud?

RocksDB-cloud does not write its write-ahead log (WAL) to object storage. Instead, it supports either local disk, Kafka, or Kinesis for the WAL. Users must decide whether they want to increase operation complexity by adding a distributed system like Kafka or Kinesis to their stack, whether they want to use local disk for the WAL, or whether they want to use EBS or EFS for the WAL.

SlateDB, by contrast, writes everything (including the WAL) to object storage. This offers a simpler architecture, better durability, and potentially better availability at the cost of increased write latency (compared to local).

RocksDB-cloud also does not cache writes, so recently written data must be read from object storage even if reads occur immediately after writes. SlateDB, by contrast, caches recent writes in memory. Eventually, we will also add on-disk caching as well.

Finally, it's unclear how open RocksDB-cloud is to outside contributors. The code is available, but there is very little documentation or community.

## How is this different from RocksDB on EBS?

Amazon Web Services (AWS) elastic block storage (EBS) runs in a single availability zone (AZ). To get SlateDB's durability and availability (when run on S3 standard buckets), you would need to replicate the across three AZs. This complicates the main write path (you would need synchronous replication) and add to cost.

S3 is inherently much more flexible and elastic. You don't need to overprovision, you don't need to manage volume sizes, and you don't have to worry about transient space amplification from compaction.

S3 also allows for more cost/perf/availability tradeoffs. For example, users can sacrifice some availability by running one node in front of S3 and replacing it on a failure. [The Cloud Storage Triad: Latency, Cost, Durability](https://materializedview.io/p/cloud-storage-triad-latency-cost-durability) talks more about these tradeoffs.

## How is this different from RocksDB on EFS?

Amazon Web Services (AWS) elastic file system (EFS) is very expensive ($0.30/GB-month for storage, $0.03/GB for reads, and $0.06/GB for writes). It's also unclear how well RocksDB works with network file system (NFS) mounted filesystems, and whether [close-to-open consistency](https://docs.aws.amazon.com/efs/latest/ug/features.html#consistency) breaks any internal assumptions in RocksDB.

## How is this different from DynamoDB?

DynamoDB has a different cost structure and API than SlateDB. In general, SlateDB will be cheaper. DynamoDB charges $0.1/GiB for storage. If you use S3 standard with SlateDB, storage starts at $0.023/GiB (nearly 5 times cheaper).

S3 standard charges $0.005 per-1000 writes (PUT, DELETE, etc.) and $0.0004 per-1000 reads. DynamoDB charges in read and write request units (RRU and WRU, respectively). Writes cost $1.25 per-million write units and $0.25 per-million read units. Depending on consistency and data size, a single request can cost multiple units (see [here](https://aws.amazon.com/dynamodb/pricing/on-demand/) for details). SlateDB batches writes by default, so it's usually going to have a less expensive API bill. If you batch DyanmoDB writes, you might be able to get similar fees.

DynamoDB offers 99.999% SLA while an S3 standard bucket offers 99.99%, so DynamoDB is more available.

DynamoDB also requires partitioning. SlateDB doesn't have partitioning. Instead, you must build a partitioning scheme on top of SlateDB if you need it. Though, since SlateDB fences stale writers, partition management should be fairly straightforward.

SlateDB also offers some unique features like the ability to create snapshot clones of a database at a specific point in time.

## What happens if the process goes down before SlateDB flushes data to object storage?

Any in-flight data that hasn't yet been flushed to object storage will be lost.

To prevent data loss, SlateDB's `put()` API will block until the data has been flushed to object storage. Client processes can block until their data has been durably written. Blocking can be disabled with [`WriteOptions`](https://docs.rs/slatedb/latest/slatedb/config/struct.WriteOptions.html) for clients that don't need this durability guarantee.

## Does SlateDB support column families?

SlateDB does not support [column families](https://github.com/facebook/rocksdb/wiki/column-families). Opening multiple SlateDB databases is cheap. This is what we recommend if you need to separate data.

## Are there any limits to key and value sizes?

Keys are limited to a maximum of 65 KiB (65,535 bytes). Values are limited to a maximum of 4 GiB (4,294,967,295 bytes). Larger values require more memory and will take longer to write, so we recommend testing performance at your expected value size to ensure it meets your requirements.
