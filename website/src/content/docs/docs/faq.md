---
title: FAQ
description: Frequently asked questions about SlateDB
---

## General

### What is SlateDB?

SlateDB is an embedded storage engine built as a log-structured merge-tree (LSM-tree). Unlike traditional LSM-tree storage engines, SlateDB writes all data to object storage.

### Why object storage?

Object storage provides highly-durable, highly-scalable, highly-available storage at a great cost. Recent advancements have made it even more attractive:

* Google Cloud Storage supports multi-region and dual-region buckets for high availability.
* All object stores support compare-and-swap (CAS) operations.
* Amazon Web Service's S3 Express One Zone has single-digit millisecond latency.

We believe that the future of object storage are multi-region, low latency buckets that support atomic CAS operations.

### What object stores does SlateDB support?

SlateDB supports all object stores that are supported by the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate, including:

* Amazon S3
* Google Cloud Storage
* Azure Blob Storage
* MinIO
* Tigris
* And many more

### Is SlateDB production ready?

SlateDB is currently in development and is not yet production ready. We are actively working on features and stability.

## Architecture

### How does SlateDB handle writes?

SlateDB's write path is as follows:

1. A `put` call is made on the client.
2. The key/value pair is written to the mutable, in-memory WAL table.
3. After `flush_ms` milliseconds, the mutable WAL table is frozen and an asynchronous write to object storage is triggered.
4. When the write succeeds, insert the immutable WAL into the mutable memtable and notify `await`'ing clients.
5. When the memtable reaches a `l0_sst_size_bytes`, it is frozen and written as an L0 SSTable in the object store's `compacted` directory.

### How does SlateDB handle reads?

SlateDB's read path is as follows:

1. A `get` call is made on the client.
2. The value is returned from the mutable memtable if found.
3. The value is returned from the immutable memtable(s) if found.
4. The value is returned from the L0 SSTables if found (searched from newest to oldest using bloom filtering).
5. The value is returned from the sorted runs if found (searched from newest to oldest using bloom filtering).

### How does SlateDB handle compaction?

SlateDB's compactor is responsible for merging SSTs from L0 into lower levels (L1, L2, and so on). These lower levels are referred to as _sorted runs_ in SlateDB. Each SST in a sorted run contains a distinct subset of the keyspace.

### How does SlateDB handle garbage collection?

SlateDB garbage collects old manifests and SSTables. The collector runs in the client process. It will periodically delete:

- Manifests older than `min_age` that are not referenced by any current snapshot.
- WAL SSTables older than `min_age` and older than `wal_id_last_compacted`.
- L0 SSTables older than `min_age` and not referenced by the current manifest or any active snapshot.

## Performance

### What is the expected write latency?

SlateDB's write latency is dominated by object store PUT operations. The following table shows expected latencies for different object stores:

| Object Store | Expected Write Latency | Notes |
|--------------|----------------------|-------|
| S3 Standard | 50-100ms | Network latency dominates |
| S3 Express One Zone | 5-10ms | Single-digit millisecond latency |
| Google Cloud Storage | 50-100ms | Network latency dominates |
| Azure Blob Storage | 50-100ms | Network latency dominates |
| MinIO | 5-20ms | Depends on network and disk |

### What is the expected read latency?

Read latency is primarily determined by:

1. **Bloom filter misses**: If a bloom filter indicates a key might exist in an SST, SlateDB must read the SST from object storage.
2. **Object store GET latency**: The time it takes to download an SST from object storage.
3. **SST size**: Larger SSTs take longer to download and parse.

### How can I tune SlateDB for better performance?

SlateDB provides several configuration options to tune performance:

* **`flush_ms`**: The time to wait before flushing the mutable WAL to object storage. Lower values reduce write latency but increase object store PUT frequency.
* **`l0_sst_size_bytes`**: The size of L0 SSTs. Larger SSTs provide better compression but take longer to upload.
* **`bloom_filter_false_positive_rate`**: The false positive rate for bloom filters. Lower values reduce unnecessary SST reads but increase bloom filter size.
* **`block_size`**: The size of blocks within SSTs. Larger blocks provide better compression but take longer to read individual keys.

## Use Cases

### What use cases is SlateDB good for?

SlateDB is a great fit for use cases that are tolerant to 50-100ms write latency, are tolerant to data loss during failure, or are willing to pay for frequent API PUT calls. Such use cases include:

* Stream processing
* Serverless functions
* Durable execution
* Workflow orchestration
* Durable caches
* Data lakes

### What use cases is SlateDB not good for?

SlateDB is not a good fit for use cases that require:

* Sub-millisecond write latency
* High write throughput (more than 3,500 writes per second)
* Strong consistency guarantees
* Low API costs

## Development

### How can I contribute to SlateDB?

We welcome contributions! Please see our [contributing guide](https://github.com/slatedb/slatedb/blob/main/CONTRIBUTING.md) for details.

### Where can I report bugs?

Please report bugs on our [GitHub issues page](https://github.com/slatedb/slatedb/issues).

### Where can I ask questions?

Please ask questions on our [GitHub discussions page](https://github.com/slatedb/slatedb/discussions).
