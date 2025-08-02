---
title: Performance
description: Learn about SlateDB's performance characteristics and tuning
---

SlateDB's performance characteristics are primarily determined by the object store being used. This page provides guidance on expected performance and how to tune SlateDB for your use case.

## Latency

SlateDB's write latency is dominated by object store PUT operations. The following table shows expected latencies for different object stores:

| Object Store | Expected Write Latency | Notes |
|--------------|----------------------|-------|
| S3 Standard | 50-100ms | Network latency dominates |
| S3 Express One Zone | 5-10ms | Single-digit millisecond latency |
| Google Cloud Storage | 50-100ms | Network latency dominates |
| Azure Blob Storage | 50-100ms | Network latency dominates |
| MinIO | 5-20ms | Depends on network and disk |

Read latency is primarily determined by:

1. **Bloom filter misses**: If a bloom filter indicates a key might exist in an SST, SlateDB must read the SST from object storage.
2. **Object store GET latency**: The time it takes to download an SST from object storage.
3. **SST size**: Larger SSTs take longer to download and parse.

## Throughput

SlateDB's write throughput is limited by:

1. **Object store PUT rate limits**: Most object stores limit PUT operations to 3,500 requests per second per prefix.
2. **Network bandwidth**: The time it takes to upload SSTs to object storage.
3. **SST size**: Larger SSTs provide better compression but take longer to upload.

Read throughput is limited by:

1. **Object store GET rate limits**: Most object stores limit GET operations to 5,500 requests per second per prefix.
2. **Network bandwidth**: The time it takes to download SSTs from object storage.
3. **SST size**: Larger SSTs provide better compression but take longer to download.

## Tuning

SlateDB provides several configuration options to tune performance:

### Write Performance

* **`flush_ms`**: The time to wait before flushing the mutable WAL to object storage. Lower values reduce write latency but increase object store PUT frequency.
* **`l0_sst_size_bytes`**: The size of L0 SSTs. Larger SSTs provide better compression but take longer to upload.
* **`max_unflushed_memtable`**: The maximum number of unflushed memtables. Lower values reduce memory usage but may increase write latency.

### Read Performance

* **`bloom_filter_false_positive_rate`**: The false positive rate for bloom filters. Lower values reduce unnecessary SST reads but increase bloom filter size.
* **`block_size`**: The size of blocks within SSTs. Larger blocks provide better compression but take longer to read individual keys.

### Memory Usage

* **`max_memtable_size_bytes`**: The maximum size of the in-memory memtable. Lower values reduce memory usage but may increase write latency.
* **`max_unflushed_memtable`**: The maximum number of unflushed memtables. Lower values reduce memory usage but may increase write latency.

## Benchmarks

SlateDB's performance has been benchmarked on various object stores. See the [benchmarks repository](https://github.com/slatedb/slatedb-bencher) for detailed results.

## Best Practices

1. **Use S3 Express One Zone for low latency**: If you need single-digit millisecond write latency, use S3 Express One Zone.
2. **Tune SST sizes**: Larger SSTs provide better compression but take longer to upload/download. Find the right balance for your use case.
3. **Monitor bloom filter effectiveness**: If bloom filters are causing too many false positives, consider reducing the false positive rate.
4. **Use appropriate object stores**: Choose an object store that matches your latency and throughput requirements.