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

SlateDB currently has two benchmarking tools: **bencher** and **microbenchmarks**.

It comes with a [bencher](https://github.com/slatedb/slatedb/tree/main/slatedb-bencher) tool to benchmark put/get operations against an object store. You can configure the tool with a variety of options. See [bencher/README.md](https://github.com/slatedb/slatedb/blob/main/slatedb-bencher/README.md) for details. [benchmark-db.sh](https://github.com/slatedb/slatedb/blob/main/slatedb-bencher/benchmark-db.sh) also serves as an example.

### Nightly

We run bencher [nightly](https://github.com/slatedb/slatedb/blob/main/.github/workflows/nightly.yaml) and publish the results [here](https://slatedb.io/performance/benchmarks/main). The benchmark runs on [WarpBuild](https://warpbuild.com)'s [warp-ubuntu-latest-x64-16x](https://docs.warpbuild.com/cloud-runners) runners, which use Hetzner machines in Frankfurt. We use [Tigris](https://www.tigrisdata.com/) for object storage with the `auto` region setting, which resolves to Frankfurt as well. Bandwidth between WarpBuild (Hetzner) and Tigris seems to be about 500MiB/s down and 130MiB/s up. We routinely max out the bandwidth in the nightly tests.

<iframe src="https://slatedb.io/performance/benchmarks/main" width="100%" height="540px"></iframe>

## Microbenchmarks

We use [Criterion](https://bheisler.github.io/criterion.rs/) to run microbenchmarks (located in [benches](https://github.com/slatedb/slatedb/tree/main/slatedb/benches)) for specific internal SlateDB functions. A comment is left on all PRs when a > 200% slowdown is detected.

### Nightly

Microbenchmarks also run [nightly](https://github.com/slatedb/slatedb/blob/main/.github/workflows/nightly.yaml) with the [pprof-rs](https://github.com/tikv/pprof-rs) profiler. The resulting profiler protobuf files are published [here](https://github.com/slatedb/slatedb-website/tree/gh-pages/performance/microbenchmark-pprofs/main). The microbenchmarks run on [standard Linux Github action runners](https://docs.github.com/en/actions/using-github-hosted-runners/using-github-hosted-runners/about-github-hosted-runners#standard-github-hosted-runners-for-public-repositories).

We highly recommend using [pprof.me](https://pprof.me/) to view the `<microbenchmar>.pb` files, though any [pprof](https://github.com/google/pprof) compatible tool may be used.