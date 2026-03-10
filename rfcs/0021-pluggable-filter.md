# Pluggable Filter Policies

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  * [Traits](#traits)
  * [Built-in Implementations](#built-in-implementations)
  * [Configuration](#configuration)
  * [SST Format Changes](#sst-format-changes)
  * [Read Path Integration](#read-path-integration)
  * [Write Path Integration](#write-path-integration)
- [Impact Analysis](#impact-analysis)
  * [Core API and Query Semantics](#core-api-and-query-semantics)
  * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
  * [Time, Retention, and Derived State](#time-retention-and-derived-state)
  * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
  * [Compaction](#compaction)
  * [Storage Engine Internals](#storage-engine-internals)
  * [Ecosystem and Operations](#ecosystem-and-operations)
- [Operations](#operations)
  * [Performance and Cost](#performance-and-cost)
  * [Observability](#observability)
  * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

This RFC proposes replacing SlateDB's hard-coded bloom filter with a pluggable
filter policy abstraction. Users will be able to supply their own filter
implementation (e.g., cuckoo filters, range filters) through traits. The
existing bloom filter becomes the default implementation behind this trait,
preserving full backwards compatibility. This enables prefix scan acceleration
and other advanced filtering strategies without coupling the storage engine to
a single filter design.

## Motivation

SlateDB currently uses a bloom filter that hashes full keys. This is effective for
point lookups (`get`) but provides no benefit for prefix scans, which are a
critical access pattern for many applications (see issue
[#1334](https://github.com/slatedb/slatedb/issues/1334) and
[#1302](https://github.com/slatedb/slatedb/issues/1302)).

Today, a prefix scan must check all sorted runs and any SSTs whose key range
could overlap, even though many of those SSTs contain no keys with the target
prefix. A prefix-aware filter could eliminate these unnecessary SST reads.

There are many filter designs that could help here — each with different
trade-offs. Rather than committing to one, we propose a pluggable
filter policy that lets SlateDB ship with a default (the existing bloom filter)
and an optional built-in prefix bloom filter, while enabling users to plug in
specialized filters without modifying the engine.

## Goals

- Define traits that abstract filter construction, serialization, and querying.
- Refactor the existing bloom filter as the default implementation.
- Support prefix bloom filters by allowing a user-defined key transformation before hashing.
- Support future filter implementations without engine changes.
- Maintain full backwards compatibility: existing databases with bloom filters continue to work.
- Support both point-lookup filtering and prefix-scan.
- Optimize prefix scans where the caller only needs a recent item for a given
  prefix, by combining prefix filtering with a sequential (non-merging) scan
  mode that visits sources in recency order.

## Non-Goals

- Shipping additional filter implementations beyond the existing bloom filter and a prefix bloom filter.

## Design

### Traits

The core abstraction is a `FilterPolicy` trait with associated builder and
filter types.

```rust
/// A named, configurable filter policy.
pub trait FilterPolicy {
    /// An identifier for this policy. Stored per-SST so the reader
    /// knows whether it can decode and use the filter block.
    fn name(&self) -> &str;

    /// Creates a new builder for constructing a filter.
    fn create_builder(&self) -> Box<dyn FilterBuilder>;

    /// Decodes a previously encoded filter.
    fn decode(&self, data: &[u8]) -> Arc<dyn Filter>;

    /// Estimates the encoded size in bytes for a filter with `num_keys` keys.
    fn estimate_size(&self, num_keys: u32) -> usize;
}

/// Accumulator for keys keys during SST construction and produces a [`Filter`].
pub trait FilterBuilder {
    /// Adds a key to the filter being built.
    fn add_key(&mut self, key: &[u8]);

    /// Finalizes and return the completed filter.
    fn build(&self) -> Arc<dyn Filter>;
}

/// A read-only filter that answers membership queries.
pub trait Filter {
    /// Returns `true` if the filter cannot rule out the query.
    /// A return value of `false` guarantees no matching key exists.
    fn might_match(&self, query: &FilterQuery) -> bool;

    /// Encodes the filter into a byte buffer for persistence.
    fn encode(&self) -> Bytes;

    /// Returns the in-memory size of this filter in bytes.
    fn size(&self) -> usize;
}

/// Extractor for a prefix from a key for use in prefix-based filtering.
///
/// Used on the write path to hash prefixes into the filter during SST
/// construction.
pub trait PrefixExtractor {
    /// A unique name identifying this extractor's configuration.
    ///
    /// Changing the extractor (e.g. switching from a 4-byte fixed prefix
    /// to a delimiter-based one) changes which hashes are stored in the
    /// filter, so existing filters become invalid. The built-in
    /// `PrefixBloomFilterPolicy` includes this name in the policy name
    /// it writes to SST metadata, which lets the reader detect the
    /// mismatch and skip the filter instead of returning wrong results.
    ///
    /// Custom filter policies that use a `PrefixExtractor` should do
    /// the same.
    fn name(&self) -> &str;

    /// Extracts the prefix from a key. Returns `None` if the key does not
    /// contain a recognizable prefix.
    fn extract<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]>;
}

/// Determines what type of query a filter can answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterQuery {
    /// Used to test whether a specific key might exist in the SST.
    Point(Bytes),
    /// Used to est whether any key with the given prefix might exist in the SST.
    Prefix(Bytes),
}
```

Key design decisions:

1. **`name()` for safety**: The policy name is stored per-SST in the SST info.
   When reading an SST, if the stored name doesn't match the current policy,
   the filter block is skipped rather than misinterpreted. This allows safe
   policy migration without manifest-level validation (same pattern as
   LevelDB's `FilterPolicy::Name()`).

2. **`FilterQuery` enum**: Instead of just accepting a hash, the `might_match`
   method takes a `FilterQuery` that distinguishes point lookups and prefix
   lookups. Filters that only support point lookups can conservatively return
   `true` for prefix queries.

3. **`encode` on `Filter`, `decode` on `FilterPolicy`**: Encoding is on the
   `Filter` instance because it knows its own internal representation.
   Decoding is on the `FilterPolicy` because the caller needs the policy
   (which knows the format) to reconstruct a `Filter` from raw bytes.

### Built-in Implementations

#### `BloomFilterPolicy` (default)

The existing `BloomFilter` and `BloomFilterBuilder` will implement the `Filter`
and `FilterBuilder` traits. The policy name would be `"slatedb.BloomFilter"`.

```rust
#[derive(Debug)]
pub struct BloomFilterPolicy {
    bits_per_key: u32,
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str { "slatedb.BloomFilter" }

    fn create_builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(BloomFilterBuilder::new(self.bits_per_key))
    }

    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        Arc::new(BloomFilter::decode(data))
    }

    fn estimate_size(&self, num_keys: u32) -> usize {
        BloomFilter::estimate_encoded_size(num_keys, self.bits_per_key)
    }
}
```

`BloomFilter` implements `Filter`:

```rust
impl Filter for BloomFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        match query {
            FilterQuery::Point(key) => {
                let hash = filter_hash(key.as_ref());
                self.might_contain(hash)
            }
            // A full-key bloom filter cannot answer prefix queries.
            FilterQuery::Prefix(_) => true,
        }
    }
    // ... encode, size
}
```

#### `PrefixBloomFilterPolicy`

A bloom filter that hashes key prefixes instead of full keys. The user supplies
a `PrefixExtractor` (see [Traits](#traits)) to define how prefixes are extracted.

```rust
#[derive(Debug)]
pub struct PrefixBloomFilterPolicy {
    bits_per_key: u32,
    prefix_extractor: Arc<dyn PrefixExtractor>,
    name: String,
}

impl PrefixBloomFilterPolicy {
    pub fn new(bits_per_key: u32, prefix_extractor: Arc<dyn PrefixExtractor>) -> Self {
        let name = format!("slatedb.PrefixBloomFilter:{}", prefix_extractor.name());
        Self { bits_per_key, prefix_extractor, name }
    }
}

impl FilterPolicy for PrefixBloomFilterPolicy {
    fn name(&self) -> &str {
        &self.name
    }

    fn create_builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(PrefixBloomFilterBuilder {
            inner: BloomFilterBuilder::new(self.bits_per_key),
            prefix_extractor: self.prefix_extractor.clone(),
        })
    }
    // ...
}
```

During construction, the builder extracts the prefix and adds it to the
underlying bloom filter:

```rust
impl FilterBuilder for PrefixBloomFilterBuilder {
    fn add_key(&mut self, key: &[u8]) {
        if let Some(prefix) = self.prefix_extractor.extract(key) {
            self.inner.add_key(prefix);
        }
        // Keys without a recognizable prefix are skipped. No false
        // negatives: might_match() returns true when extraction fails.
    }
    // ...
}
```

The filter answers prefix queries only:

```rust
impl Filter for PrefixBloomFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        match query {
            // Point lookups are not filtered — use DualFilterPolicy for that.
            FilterQuery::Point(_) => true,
            FilterQuery::Prefix(prefix) => {
                let hash = filter_hash(prefix.as_ref());
                self.inner.might_contain(hash)
            }
        }
    }
}
```

**Note:** A prefix bloom filter only answers prefix queries — point lookups
always return `true` (no filtering). Use `DualFilterPolicy` below if you need
both point-lookup filtering and prefix scan filtering.

#### `DualFilterPolicy`

For users who need both low-FPR point lookups and prefix scan filtering,
`DualFilterPolicy` maintains two bloom filters per SST — one for full keys and
one for prefixes — and routes queries to the appropriate filter:

```rust
pub struct DualFilterPolicy {
    point_policy: BloomFilterPolicy,
    prefix_policy: PrefixBloomFilterPolicy,
    name: String,
}

impl FilterPolicy for DualFilterPolicy {
    fn name(&self) -> &str { &self.name }

    fn create_builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(DualFilterBuilder {
            point_builder: self.point_policy.create_builder(),
            prefix_builder: self.prefix_policy.create_builder(),
        })
    }
    // ...
}
```

The `DualFilter` routes queries:

```rust
impl Filter for DualFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        match query {
            FilterQuery::Point(_) => self.point_filter.might_match(query),
            FilterQuery::Prefix(_) => self.prefix_filter.might_match(query),
        }
    }
    // encode serializes both sub-filters; decode reconstructs both.
}
```

The trade-off is additional filter space per SST (one full-key filter + one
prefix filter).

### Configuration

The `Settings` struct replaces `filter_bits_per_key` with a `filter_policy`
field. This is a **breaking change** — the old field is removed entirely.

```rust
pub struct Settings {
    // ... existing fields ...

    /// Write SSTables with a filter if the number of keys in the SSTable
    /// is greater than or equal to this value.
    pub min_filter_keys: u32,

    /// The filter policy to use. Defaults to `BloomFilterPolicy` with
    /// 10 bits per key. Set to `None` to disable filters.
    pub filter_policy: Option<Arc<dyn FilterPolicy>>,
}
```

Usage:

```rust
// Default: full-key bloom filter (backwards compatible)
let settings = Settings::default();

// Prefix bloom filter with a user-defined extractor
let settings = Settings {
    filter_policy: Some(Arc::new(PrefixBloomFilterPolicy::new(
        10, // bits per key
        Arc::new(MyPrefixExtractor::new()),
    ))),
    ..Settings::default()
};

// Custom filter
let settings = Settings {
    filter_policy: Some(Arc::new(MyCustomFilterPolicy::new(...))),
    ..Settings::default()
};
```

### SST Format Changes

The SST needs a new field to identify which filter policy produced the filter block:

```fbs
table SstInfo {
    // ... existing fields ...
    filter_policy_name: string;
}
```

When reading an SST, SlateDB checks that the `filter_policy_name` matches the
current policy's `name()`. If they don't match, the filter block is ignored
(treated as if no filter exists) and a warning is logged. This allows safe
rollout: old SSTs with the previous policy's filter are simply not used for
filtering until they are compacted and rewritten with the new policy.

### Read Path Integration

The current read path uses `filter_hash()` and `BloomFilter::might_contain()`
directly. We replace this with the `FilterQuery` abstraction.

**Point lookups (`get`):**

The `BloomFilterEvaluator` in `sst_iter.rs` currently hashes the key and calls
`might_contain`. With the pluggable design:

```rust
// Before:
let key_hash = filter::filter_hash(self.key.as_ref());
filter.might_contain(key_hash)

// After:
let query = FilterQuery::Point(self.key.clone());
filter.might_match(&query)
```

**Prefix scans:**

SlateDB already provides `scan_prefix` and `scan_prefix_with_options` methods
that accept a prefix. Internally, the prefix will be wired through the reader and
iterator chain so each SST's filter can be checked before opening it, skipping
SSTs whose filter returns `false`.

The default `BloomFilterPolicy` returns `true` for prefix queries (it only
answers point lookups), so prefix filtering only takes effect when a
`PrefixBloomFilterPolicy` or a custom policy is configured.

**Sequential scan mode:**

The default merge-based scan initializes all sources upfront to produce globally
sorted output. For use cases where the caller only needs the most recently
written entries for a prefix, a sequential scan mode can be opted into via `ScanOptions`:

```rust
pub struct ScanOptions {
    // ... existing fields ...
    pub sequential_scan: bool,
}
```

Sequential scan visits sources one at a time in recency order (memtables → L0 →
sorted runs) similar to `get` API. Entries within each source are ordered by key
, but entries across sources are NOT globally sorted. Combined with prefix
filtering, most sources are skipped entirely before any I/O.

### Write Path Integration

The SST builder receives a `FilterPolicy` and calls `create_builder()` to
obtain a `FilterBuilder`. Each key is fed to the builder via `add_key()`. On
finalization, `build()` produces the filter, `encode()` writes it to the filter
block, and `policy.name()` is stored in the SST info.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API and Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [x] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [x] SST format or block format

### Ecosystem and Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance and Cost

- **Point lookup latency**: Negligible change. The dynamic dispatch overhead of
  `dyn Filter` is a single vtable lookup per SST which shouldn't be a bottleneck
  compared to I/O costs, so the default bloom filter performs identically to today.
- **Prefix scan latency**: Significant improvement when a prefix-aware filter is
  configured. SSTs that don't contain the target prefix are skipped entirely,
  avoiding block reads.
- **Write throughput**: No meaningful change. Filter construction cost is
  comparable.
- **Space**: No change for the default bloom filter. Prefix bloom filters may
  use slightly less space (fewer unique prefixes than keys). Other filter types
  have their own space characteristics.

### Observability

- **Configuration**: New `filter_policy` setting (programmatic only; not
  serializable to TOML/JSON). `filter_bits_per_key` has been removed.
- **Metrics**: Existing `sst_filter_positives`, `sst_filter_negatives`,
  `sst_filter_false_positives` metrics are preserved.

### Compatibility

- **On-disk format**: The filter block encoding is policy-specific but the SST
  envelope is unchanged. A new `filter_policy_name` field is added to SST info.
  Old SSTs without this field are assumed to use `"slatedb.BloomFilter"`.
- **Public API**: `filter_bits_per_key` on `Settings` is removed and replaced
  by `filter_policy: Option<Arc<dyn FilterPolicy>>`. See [Configuration](#configuration)
  for migration examples.
- **Rolling upgrades**: Old readers that don't understand the filter policy name
  field will ignore it (FlatBuffers forward compatibility). They will continue
  to decode filters as bloom filters, which is correct as long as the bloom
  filter policy is still in use.

## Testing

- **Unit tests**: Filter policy correctness (no false negatives, expected false
  positive rates) for each built-in implementation.
- **Integration tests**: Prefix scan SST skipping, mismatched filter policy
  graceful degradation, backwards compatibility with old SSTs, and sequential
  scan source-recency ordering.
- **Performance tests**: Benchmark prefix scans with and without prefix bloom
  filters. Benchmark sequential scan for first-item-in-prefix lookups vs
  regular merge-based scan. The value should be very clear to justify the
  effort here.

## Rollout

Implementation will be in multiple phases:

1. **Pluggable filter abstraction**: Trait definitions, bloom filter
   implementation, SST format changes, and refactoring the write/read paths
   to use `dyn FilterPolicy`. Breaking config change: `filter_bits_per_key`
   replaced by `filter_policy`.
2. **Prefix bloom filter**: `PrefixExtractor` trait, `PrefixBloomFilterPolicy`
   implementation, and wiring the prefix through the read path so SST filters
   are checked before opening. Skip SSTs whose filter rejects the prefix.
3. **Dual filter policy**: `DualFilterPolicy` that maintains both key and
   prefix bloom filters per SST.
4. **Sequential scan mode**: Non-merging, opt-in iteration through sources in
   recency order for use cases like fetching the most recent item for a prefix.

## Alternatives

**1. Prefix extractor without pluggable policy (RocksDB approach):**
Add only a `prefix_extractor` option that modifies the existing bloom filter's
hashing. This is simpler but tightly couples the engine to bloom filters and
makes it harder to adopt fundamentally different filter designs
in the future. Rejected because the abstraction cost is low and the flexibility
gain is high.

**2. Ship DIVA or SuRF as a built-in filter:**
The optimal filter design is workload-dependent. Rather than picking one, the
pluggable trait lets users implement their own policy. Rejected as a built-in;
users can integrate these as plugins.

**3. Status quo (no prefix filtering):**
Prefix scans continue to read all overlapping SSTs. This is viable for small
databases but becomes a bottleneck for large datasets with many sorted runs,
as described in issues #1334 and #1302. Rejected because the performance
impact is significant and the fix is well-understood.

## Open Questions

### Should `scan` / `scan_with_options` also support prefix filtering?

Currently, prefix filtering is only available through `scan_prefix` /
`scan_prefix_with_options`, which accept an explicit prefix. For plain
`scan` / `scan_with_options` calls (which accept a `BytesRange`), the engine
could infer the prefix using the `PrefixExtractor`:

**Option A: No inference — prefix filtering only via `scan_prefix`.**
`scan` / `scan_with_options` never use prefix filters. Users who want prefix
filtering use the dedicated `scan_prefix` methods.

- Pro: Simple, no ambiguity about when filtering is active.
- Con: Users who build their own prefix ranges don't get filtering.

**Option B: Engine infers using the `PrefixExtractor`.**
The engine calls `extract()` on the start and end bounds of the range. If both
yield the same prefix, that prefix is used for filter evaluation.

- Pro: Prefix filtering works transparently for any range that happens to be a prefix range.
- Con: Implicit behavior — the caller has no control over whether the filter is used.

### Should range scan filtering be added?

This RFC only adds prefix-based SST filtering. Range scan filtering (skipping
SSTs based on arbitrary range bounds) is not included but can be added with the
current design — a `FilterQuery::Range` variant and a range-aware filter policy
would slot into the existing `might_match` dispatch without structural changes.
The read path would need to pass the full scan range bounds to the filter
instead of just a prefix.

## References

- [Issue #1334: Improve prefix scan performance with filtering](https://github.com/slatedb/slatedb/issues/1334)
- [Issue #1302: High scan operation latency](https://github.com/slatedb/slatedb/issues/1302)
- [LevelDB FilterPolicy](https://github.com/google/leveldb/blob/main/include/leveldb/filter_policy.h)
- [RocksDB Bloom Filter wiki](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)
- [RocksDB Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek)
- [DIVA: Dynamic Range Filter for Var-Length Keys (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p3923-eslami.pdf)
- [SuRF: Succinct Range Filters (SIGMOD 2018)](https://dl.acm.org/doi/fullHtml/10.1145/3375660)

## Updates

