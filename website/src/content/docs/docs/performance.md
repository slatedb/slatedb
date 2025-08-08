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

Read latency dominated by the database's working set size and read pattern:

1. Working sets that fit in local (memorya and disk) caches will respond very quickly (< 1ms).
2. Read patterns that do sequential reads will respond very quickly (<1ms) since SST blocks are pre-fetched sequentially and cached locally.
3. Read patterns that access keys that are (lexicographically) close to each other will respond very quickly (< 1ms) since blocks are cached locally after the first read.
4. Reads that access data across the keyspace, and whose dataset is larger than a single machine's memory or disk will, are more likely to see latency spikes similar to object storage latency levels (50-100ms for S3 standard). Such workloads can still work well, but require more tuning, partitioning, and so on.

## Throughput

SlateDB's write throughput is limited by:

1. **Object store PUT rate limits**: Most object stores limit PUT operations to 3,500 requests per second per prefix.
2. **Network bandwidth**: The time it takes to upload SSTs to object storage.

Read throughput is limited by:

1. **Object store GET rate limits**: Most object stores limit GET operations to 5,500 requests per second per prefix.
2. **Network bandwidth**: The time it takes to download SSTs from object storage.
3. **Disk I/O**: The time it takes to read SSTs from disk when object store caching or [Foyer hybrid caching](https://foyer.rs/docs/tutorial/hybrid-cache) are enabled.

## Tuning

SlateDB provides several configuration options to tune performance. See [Settings](https://docs.rs/slatedb/0.7.0/slatedb/config/struct.Settings.html) for a complete reference.

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

SlateDB currently has two benchmarking tools: **bencher** and **microbenchmarks**.

[bencher](https://github.com/slatedb/slatedb/tree/main/slatedb-bencher) is a tool to benchmark put/get operations against an object store. You can configure the tool with a variety of options. See [bencher/README.md](https://github.com/slatedb/slatedb/blob/main/slatedb-bencher/README.md) for details. [benchmark-db.sh](https://github.com/slatedb/slatedb/blob/main/slatedb-bencher/benchmark-db.sh) also serves as an example.

Microbenchmarks run with [Criterion](https://bheisler.github.io/criterion.rs/). They are located in [benches](https://github.com/slatedb/slatedb/tree/main/slatedb/benches) and run for specific internal SlateDB functions. A comment is left on all PRs when a > 200% slowdown is detected.

### Nightly Benchmarks

We run both **bencher** and **microbenchmarks** [nightly](https://github.com/slatedb/slatedb/actions/workflows/nightly.yaml). The [results](https://github.com/slatedb/slatedb/actions/workflows/nightly.yaml) are published in the Github action summary.

- **Bencher** benchmarks run on [WarpBuild](https://warpbuild.com)'s [warp-ubuntu-latest-x64-16x](https://docs.warpbuild.com/cloud-runners) runners, which use [Hetzner](https://hetzner.com/) machines in Frankfurt. We use [Tigris](https://www.tigrisdata.com/) for object storage with the `auto` region setting, which resolves to Frankfurt as well. Bandwidth between WarpBuild (Hetzner) and Tigris seems to be about 500MiB/s down and 130MiB/s up. We routinely max out the bandwidth in the nightly tests.

- **Microbenchmarks** run on [standard Linux Github action runners](https://docs.github.com/en/actions/using-github-hosted-runners/using-github-hosted-runners/about-github-hosted-runners#standard-github-hosted-runners-for-public-repositories) with the [pprof-rs](https://github.com/tikv/pprof-rs) profiler. The resulting profiler protobuf files are published to [pprof.me](https://pprof.me) and links to each microbenchmark are provided in the Github action summary.
