# Prefix Bloom Filters via Pluggable Filter Policies

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Traits](#traits)
   - [Built-in Implementation: BloomFilterPolicy](#built-in-implementation-bloomfilterpolicy)
   - [SST Format Changes](#sst-format-changes)
   - [Write Path Integration](#write-path-integration)
   - [Read Path Integration](#read-path-integration)
- [Impact Analysis](#impact-analysis)
   - [Core API and Query Semantics](#core-api-and-query-semantics)
   - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   - [Time, Retention, and Derived State](#time-retention-and-derived-state)
   - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   - [Compaction](#compaction)
   - [Storage Engine Internals](#storage-engine-internals)
   - [Ecosystem and Operations](#ecosystem-and-operations)
- [Operations](#operations)
   - [Performance and Cost](#performance-and-cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Future Enhancements](#future-enhancements)
   - [Recency-Based Iterator](#recency-based-iterator)
   - [Block-Level Filter Granularity](#block-level-filter-granularity)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

This RFC proposes adding prefix bloom filters to SlateDB, enabling prefix
scans to skip SSTs that don't contain matching keys. To support this cleanly,
the hard-coded bloom filter is replaced with a pluggable filter policy
abstraction where users can supply their own filter implementation (e.g., cuckoo
filters, range filters) through traits. The existing bloom filter becomes the
default implementation behind this trait, preserving full backwards
compatibility.

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
- Support multiple concurrent filter policies per SST with AND logic evaluation.
- Enable safe migration from one filter policy to another without losing filtering on existing SSTs.
- Maintain full backwards compatibility: existing databases with bloom filters continue to work.
- Support both point-lookup filtering and prefix-scan.

## Non-Goals

- Shipping additional filter implementations beyond the existing bloom filter and a prefix bloom filter.

## Design

### Traits

The core abstraction is a `FilterPolicy` trait with associated builder and
filter types.

```rust
/// A named, configurable filter policy.
pub trait FilterPolicy {
    /// An identifier for this policy. Stored per-SST so the reader knows
    /// whether it can decode and use the filter block.
    ///
    /// The name should encode anything that affects **compatibility** —
    /// i.e., anything that would make a filter unreadable or produce wrong
    /// results if mismatched. For the built-in bloom filter:
    /// - `bits_per_key` is not included — changing it affects quality (FP
    ///   rate) but not the encoding format, so any reader can decode any
    ///   bloom filter regardless of bits_per_key.
    /// - `prefix_extractor` name is included — it changes which hashes are
    ///   stored, so querying with a different extractor produces false
    ///   negatives.
    ///
    /// Examples: `"slatedb.BloomFilter"`,
    /// `"slatedb.BloomFilter:prefix=fixed3"`.
    fn name(&self) -> &str;

    /// Creates a new builder for constructing a filter.
    fn builder(&self) -> Box<dyn FilterBuilder>;

    /// Decodes a previously encoded filter.
    ///
    /// The engine matches the policy name stored in the composite filter
    /// block against `self.name()` before calling this to ensure the policy
    /// can deserialize the data.
    fn decode(&self, data: &[u8]) -> Arc<dyn Filter>;

    /// Estimates the encoded size in bytes for a filter with `num_keys` keys.
    ///
    /// This is a hint used by the SST builder to reserve buffer space before
    /// the filter is built. It does not need to be exact.
    ///
    /// For the built-in bloom filter, the actual filter is sized from the
    /// real number of hashes collected during `add_entry`, not from this estimate,
    /// so overestimates waste a small allocation and underestimates just trigger
    /// a reallocation.
    fn estimate_size(&self, num_keys: usize) -> usize;
}

/// Accumulator for entries during SST construction that produces a [Filter].
pub trait FilterBuilder {
    /// Feeds an SST entry to the filter being built.
    ///
    /// The builder receives the full `RowEntry` (key, value, sequence
    /// number, timestamps) so that filter implementations are not limited
    /// to key-only hashing.  A bloom filter will typically hash only the
    /// key (or a prefix of it), but other filters — for example a min/max
    /// timestamp filter or a sequence-number range filter — can inspect
    /// whichever fields they need.
    fn add_entry(&mut self, entry: &RowEntry);

    /// Finalizes and return the completed filter.
    fn build(&self) -> Arc<dyn Filter>;
}

/// A read-only filter that answers membership queries.
pub trait Filter {
    /// Returns `true` if the filter cannot rule out the query.
    /// A return value of `false` guarantees no matching key exists.
    fn might_match(&self, query: &FilterQuery) -> bool;

    /// Serializes the filter into the provided buffer.
    fn encode(&self, writer: &mut impl BufMut);

    /// Returns the size of the filter's data in bytes.
    ///
    /// This should reflect the underlying data structure size (e.g., the
    /// bit array length for a bloom filter). Can be used for memory
    /// tracking, cache accounting, etc.
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
    /// `BloomFilterPolicy` includes this name in the policy name it
    /// writes to SST metadata (e.g. `"slatedb.BloomFilter:prefix=fixed3"`),
    /// which lets the reader detect the mismatch and skip the filter
    /// instead of returning wrong results.
    ///
    /// Custom filter policies that use a `PrefixExtractor` should do
    /// the same.
    fn name(&self) -> &str;

    /// Returns whether the given prefix is a valid output of `extract()`.
    ///
    /// This is used on the read path to verify that a scan prefix provided
    /// by the user matches the prefix format indexed in the filter. If this
    /// returns `false`, the filter must NOT be consulted; doing so can
    /// produce false negatives (the filter says "not present" for data that
    /// actually exists).
    ///
    /// **Example of incorrect behavior without this check:**
    /// Assume a prefix extractor that extracts the first 3 bytes of a
    /// key and an SST contains keys `abc_1`, `abc_2`, `abx_1`.
    /// During SST construction, the filter indexes the extracted prefixes:
    /// `abc` and `abx`.
    /// At read time, a user calls `scan_prefix("ab")`. The 2-byte prefix
    /// `"ab"` was never inserted into the filter, so `might_match("ab")`
    /// returns `false`. The SST is skipped even though all three keys match
    /// the scan prefix `"ab"`. This is a false negative.
    ///
    /// With `in_domain`: `in_domain("ab")` returns `false` (`"ab"` is not
    /// a valid 3-byte prefix), so the engine skips the filter check and
    /// falls back to scanning the SST directly.
    fn in_domain(&self, prefix: &[u8]) -> bool;

    /// Extracts the prefix from a key. Returns `None` if the key does not
    /// contain a recognizable prefix (i.e., `in_domain` would return `false`
    /// for the key).
    fn extract<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]>;
}

/// A membership query passed to [`Filter::might_match`].
pub struct FilterQuery {
    /// The kind of query (point or prefix).
    pub kind: FilterQueryKind,
    /// Opaque hints provided by the caller (e.g., version bounds).
    /// Keyed by a string name so custom filters can look up relevant hints.
    pub hints: HashMap<String, Bytes>,
}

/// The kind of filter query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterQueryKind {
    /// Used to test whether a specific key might exist in the SST.
    Point(Bytes),
    /// Used to test whether any key with the given prefix might exist in the SST.
    Prefix(Bytes),
}
```

Key design decisions:

1. **`FilterQuery` with hints**: `FilterQuery` is a struct rather than a plain
   enum so it can carry opaque `hints` alongside the query kind. Hints are a
   `HashMap<String, Bytes>` that the caller populates and custom filters inspect.
   For example, a min/max filter could use a `"version_bounds"` hint to narrow
   its check, or a time-range filter could use a `"timestamp_range"` hint. The
   built-in bloom filter ignores hints. Users pass hints via `ScanOptions::filter_hints`
   which are threaded through the iterator chain to `FilterQuery::with_hints`.

2. **`name()` for safety**: Each policy's name is stored alongside its filter
   data in the composite filter block. When reading an SST, if a stored name
   doesn't match any configured policy, that sub-filter is skipped rather than
   misinterpreted. This allows safe policy migration without manifest-level
   validation (same pattern as LevelDB's `FilterPolicy::Name()`).

3. **`FilterQueryKind` enum**: Instead of just accepting a hash, the `might_match`
   method takes a `FilterQuery` whose `kind` field distinguishes point lookups
   and prefix lookups. Filters that only support point lookups can conservatively
   return `true` for prefix queries.

4. **`encode` on `Filter`, `decode` on `FilterPolicy`**: Encoding is on the
   `Filter` instance because it knows its own internal representation.
   Decoding is on the `FilterPolicy` because the caller needs the policy
   (which knows the format) to reconstruct a `Filter` from raw bytes.

### Built-in Implementation: `BloomFilterPolicy`

A single `BloomFilterPolicy` handles both full-key and prefix filtering using
one bloom filter per SST. Both full-key hashes and prefix hashes are added to
the same bit array. The policy would look like:
```rust
#[derive(Debug)]
pub struct BloomFilterPolicy {
    bits_per_key: u32,

    /// When true, full-key hashes are added to the filter. Point lookups
    /// (`get`) probe the filter with the full-key hash.
    /// Default: true.
    whole_key_filtering: bool,

    /// When set, prefix hashes are also added to the filter. Prefix scans
    /// probe the filter with the prefix hash.
    /// Default: None (no prefix filtering).
    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    name: String,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            whole_key_filtering: true,
            prefix_extractor: None,
            name: "slatedb.BloomFilter".to_string(),
        }
    }

    pub fn with_prefix_extractor(mut self, extractor: Arc<dyn PrefixExtractor>) -> Self {
        self.name = format!("slatedb.BloomFilter:prefix={}", extractor.name());
        self.prefix_extractor = Some(extractor);
        self
    }

    pub fn with_whole_key_filtering(mut self, enabled: bool) -> Self {
        self.whole_key_filtering = enabled;
        self
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str { &self.name }

    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(BloomFilterBuilder::new(
            self.bits_per_key,
            self.whole_key_filtering,
            self.prefix_extractor.clone(),
        ))
    }

    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        Arc::new(BloomFilter::decode(
            data,
            self.whole_key_filtering,
            self.prefix_extractor.is_some(),
        ))
    }

    fn estimate_size(&self, num_keys: usize) -> usize {
        BloomFilter::estimate_encoded_size(num_keys, self.bits_per_key)
    }
}
```

#### Configuration

The `filter_bits_per_key` field is removed from `Settings`. Filter policies
are configured on `DbBuilder` and `CompactorBuilder` via `with_filter_policies`,
following the same pattern as `merge_operator` and `block_transformer` which
are also trait-object-based configuration that lives on the builders rather
than the serializable `Settings` struct.

`Settings` keeps `min_filter_keys` (a plain `u32` that is serializable) since
it controls *whether* a filter is created, not *which* filter:

```rust
pub struct Settings {
    // ... existing fields ...

    /// Write SSTables with a filter if the number of keys in the SSTable
    /// is greater than or equal to this value.
    pub min_filter_keys: u32,

    // filter_bits_per_key is removed — replaced by filter_policies on
    // DbBuilder / CompactorBuilder.
}
```

`DbBuilder` and `CompactorBuilder` gain a `with_filter_policies` method:

```rust
impl<P: Into<Path>> DbBuilder<P> {
    /// Sets the filter policies for this database. Each policy produces a
    /// separate filter per SST, stored in a composite filter block. On
    /// read, all filters are evaluated with AND logic — an SST is skipped
    /// if any filter returns `false`.
    ///
    /// Defaults to `vec![Arc::new(BloomFilterPolicy::new(10))]`.
    /// Pass an empty vec to disable filters.
    pub fn with_filter_policies(
        mut self,
        policies: Vec<Arc<dyn FilterPolicy>>,
    ) -> Self {
        self.filter_policies = policies;
        self
    }
}

impl<P: Into<Path>> CompactorBuilder<P> {
    /// Sets the filter policies used when the compactor rewrites SSTs.
    /// Must match the writer's policies to avoid silently dropping filters.
    pub fn with_filter_policies(
        mut self,
        policies: Vec<Arc<dyn FilterPolicy>>,
    ) -> Self {
        self.filter_policies = policies;
        self
    }
}
```

`ScanOptions` gets a `filter_hints` field for passing opaque hints to custom
filters:

```rust
pub struct ScanOptions {
    // ... existing fields ...

    /// Opaque hints passed to custom filters at query time.
    /// Keyed by a string name so custom filters can look up relevant hints
    /// (e.g., version bounds for a min/max filter).
    pub filter_hints: HashMap<String, Bytes>,
}
```

**Configuration modes (for BloomFilterPolicy):**

| `whole_key_filtering` | `prefix_extractor` | Behavior                                                   |
|-----------------------|--------------------|------------------------------------------------------------|
| `true` (default)      | `None`             | Full-key bloom only, today's default, backwards compatible |
| `true`                | `Some(...)`        | Both point lookups and prefix scans are filtered           |
| `false`               | `Some(...)`        | Prefix-only — smaller filter, no point-lookup filtering    |

Usage:

```rust
// Default: full-key bloom filter (backwards compatible, no explicit call needed)
let db = Db::builder("path", object_store).build().await?;

// Full-key + prefix bloom filter (recommended for prefix scan workloads)
let db = Db::builder("path", object_store)
    .with_filter_policies(vec![Arc::new(BloomFilterPolicy::new(10)
        .with_prefix_extractor(Arc::new(MyPrefixExtractor::new())))])
    .build()
    .await?;

// Prefix-only bloom filter (no point-lookup filtering)
let db = Db::builder("path", object_store)
    .with_filter_policies(vec![Arc::new(BloomFilterPolicy::new(10)
        .with_prefix_extractor(Arc::new(MyPrefixExtractor::new()))
        .with_whole_key_filtering(false))])
    .build()
    .await?;

// Multiple filters: bloom + custom min/max filter
let db = Db::builder("path", object_store)
    .with_filter_policies(vec![
        Arc::new(BloomFilterPolicy::new(10)
            .with_prefix_extractor(Arc::new(MyPrefixExtractor::new()))),
        Arc::new(MyMinMaxFilterPolicy::new(...)),
    ])
    .build()
    .await?;

// Passing hints to custom filters at scan time
let scan_options = ScanOptions {
    filter_hints: HashMap::from([
        ("version_upper_bound".to_string(), Bytes::from("v42")),
    ]),
    ..ScanOptions::default()
};
```

#### Write path: building the filter

During SST construction, the builder adds both hashes to the same underlying
bloom filter. Consecutive keys sharing the same prefix only store the prefix
hash once (deduplication), keeping the overhead minimal.

#### Read path: querying the filter

The same filter is probed with different hashes depending on the query type:

```rust
impl Filter for BloomFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        match &query.kind {
            FilterQueryKind::Point(key) => {
                if !self.whole_key_filtering {
                    return true; // Cannot answer point queries
                }
                self.might_contain(filter_hash(key.as_ref()))
            }
            FilterQueryKind::Prefix(prefix) => {
                if !self.has_prefix_filter {
                    return true; // Cannot answer prefix queries
                }
                self.might_contain(filter_hash(prefix.as_ref()))
            }
        }
    }
}
```

### SST Format Changes

The SST gains a `filter_version` field to distinguish the new composite block
format from the legacy single-bloom format:

```fbs
table SsTableInfo {
    // ... existing fields (filter_offset, filter_len, etc.) ...

    // Version of the filter block format.
    // Absent or 1: legacy single bloom filter (raw bytes).
    // 2: composite block list of named filters.
    filter_version: uint16;
}
```

#### Composite filter block encoding

When `filter_version` is `2`, the filter block at
`filter_offset`/`filter_len` contains a self-describing list of named filters:

```
[num_filters: u32]
[name_len: u32][name: bytes][data_len: u32][data: bytes]   // filter 0
[name_len: u32][name: bytes][data_len: u32][data: bytes]   // filter 1
...
```

Each sub-filter self-identifies by its policy's `name()`. Even with a single
policy, the block uses this format (a list of one).

#### Reading logic (backwards compatibility)

When reading an SST's filter block:

- **`filter_version` absent or `1`**: Legacy pre-RFC SST. The raw bytes are
  decoded using the built-in `BloomFilterPolicy`. If no matching bloom policy
  is configured, the filter is skipped.
- **`filter_version` = `2`**: Parse the composite block. For each
  `(name, data)` entry, find a matching policy in the configured `filter_policies`
  by `name()` and call `policy.decode(data)`. Unknown names are skipped
  (reduces filtering power but never produces incorrect results).

### Write Path Integration

The SST builder receives the `filter_policies` array. For each policy, it calls
`builder()` to obtain a `FilterBuilder`. Each key is fed to all builders via
`add_entry()`. On finalization, each builder produces a filter via `build()`, and
all filters are encoded into a composite filter block. The SST info's
`filter_version` is set to `2`.

### Read Path Integration

The current read path uses `filter_hash()` and `BloomFilter::might_contain()`
directly. We replace this with the `FilterQuery` abstraction and AND-logic
evaluation across all filters.

**Point lookups (`get`):**

The `BloomFilterEvaluator` in `sst_iter.rs` currently hashes the key and calls
`might_contain`. With the pluggable design:

```rust
// Before:
let key_hash = filter::filter_hash(self.key.as_ref());
filter.might_contain(key_hash)

// After:
let query = FilterQuery::point(self.key.clone());
// AND logic: skip SST if ANY filter returns false
filters.iter().all(|f| f.might_match(&query))
```

**Prefix scans:**

SlateDB already provides `scan_prefix` and `scan_prefix_with_options` methods
that accept a prefix. Internally, the prefix will be wired through the reader and
iterator chain so each SST's filters can be checked before opening it, skipping
SSTs where any filter returns `false`.

When a `prefix_extractor` is configured on the `BloomFilterPolicy`, prefix
scans probe the bloom filter with `filter_hash(prefix)`. The default
configuration (no `prefix_extractor`) returns `true` for prefix queries, so no
filtering. This is safe: point lookups still use the full-key hash. Other
filters in the array may still reject the SST based on hints or other criteria.

Note: prefix filtering alone is not ideal for recency access patterns where
the caller only needs a recent entry for a prefix. See
[Recency-Based Iterator](#recency-based-iterator) in Future Enhancements.

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
- [x] Block cache
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
- **Space**: No change for the default bloom filter (no `prefix_extractor`).
  With a `prefix_extractor`, the filter grows slightly to accommodate prefix
  hashes, but deduplication of consecutive same-prefix hashes keeps the
  overhead minimal. Much less overhead than maintaining two separate filters.

### Observability

- **Configuration**: New `with_filter_policies` on `DbBuilder` /
  `CompactorBuilder` (programmatic only; not serializable to TOML/JSON).
  `filter_bits_per_key` has been removed from `Settings`.
- **Metrics**: Existing `sst_filter_positives`, `sst_filter_negatives`,
  `sst_filter_false_positives` metrics are preserved.

### Compatibility

- **On-disk format**: The filter block encoding is policy-specific but the SST
  envelope is unchanged. A new `filter_version` field is added to SST info.
  Old SSTs without this field are assumed to use the legacy single-bloom format
  (see [SST Format Changes](#sst-format-changes)).
- **Public API**: `filter_bits_per_key` is removed from `Settings`.
  `DbBuilder` and `CompactorBuilder` gain `with_filter_policies`. `ScanOptions`
  gains a `filter_hints: HashMap<String, Bytes>` field. See
  [Configuration](#configuration) for migration examples.
- **Rolling upgrades**: Old readers that don't understand the `filter_version`
  field will ignore it (FlatBuffers forward compatibility). They will continue
  to decode filters as bloom filters, which is correct as long as the bloom
  filter policy is still in use.
- **Prefix extractor changes**: Changing the prefix extractor changes the policy
  name (e.g., `"slatedb.BloomFilter:prefix=fixed3"` →
  `"slatedb.BloomFilter:prefix=delim:"`). With the array design, the old
  policy can remain in `filter_policies` alongside the new one, so existing
  SSTs' filters remain usable while new SSTs are written with both. After
  compaction rewrites all SSTs, the old policy can be removed.
- **Compactor policy**: If the compactor runs in a separate process from the
  writer (e.g., distributed compaction), it must be configured with the same
  `filter_policies`. Otherwise, compacted SSTs will be written with different
  (or no) filter policies, and existing filters may be silently dropped during
  compaction.

## Testing

- **Unit tests**: Filter policy correctness (no false negatives, expected false
  positive rates) for each built-in implementation.
- **Integration tests**: Prefix scan SST skipping, mismatched filter policy
  graceful degradation, backwards compatibility with old SSTs.
- **Performance tests**: Benchmark prefix scans with and without prefix bloom
  filters. The value should be very clear for prefix bloom filter results.

## Rollout

Implementation will be in two phases:

1. **Pluggable filter abstraction**: Trait definitions (`FilterPolicy`,
   `FilterBuilder`, `Filter`, `PrefixExtractor`), refactor the existing bloom
   filter as the default `BloomFilterPolicy`, SST format changes
   (`filter_version`, composite filter block), and refactoring the write/read
   paths to use the pluggable filter policies with AND-logic evaluation.
   Breaking config change: `filter_bits_per_key` removed from `Settings`;
   `with_filter_policies` added to `DbBuilder` / `CompactorBuilder`.
2. **Prefix bloom filter**: Add `prefix_extractor` and `whole_key_filtering`
   to `BloomFilterPolicy`. Wire the prefix through the read path so each
   SST's filters can be probed with `FilterQuery::Prefix` before opening.
   Skip SSTs where any filter rejects the prefix.

## Future Enhancements

### Recency-Based Iterator

Prefix bloom filters skip SSTs that don't contain matching keys, but the
merge-based scan still reads from every *matching* SST and merges them to
produce global sort order. For workloads where the caller only needs the most
recent entry for a prefix (e.g., latest version of a versioned key, most
recent reading from a sensor), this is wasteful — the answer is almost always
in the newest SST.

A recency-based iterator would return entries newest-first (most recent SST
first, then next most recent) without merging, similar to how `get()` walks
sources in recency order and returns the first match. Combined with prefix
bloom filters, the iterator would skip SSTs with no matching prefix, read
matching SSTs in recency order, and stop early once the caller has enough
results.

### Block-Level Filter Granularity

This RFC's filters operate at SST granularity; the reader either skips an
entire SST or reads it. This is appropriate for bloom filters (which need many
keys for good false-positive rates) but too coarse for metadata-style filters.

For example, a min/max version filter on an SST whose blocks span versions
`[1,50]`, `[51,100]`, and `[101,150]` can only report the SST-level range
`[1,150]`. A query for version ≤ 110 must read the entire SST even though only
the first block is relevant. With per-block metadata, the remaining blocks
would be skipped without loading them from storage.

The `FilterPolicy`, `FilterBuilder`, and `Filter` traits defined in this RFC
are sufficient to support block-level granularity. The core change is
configuration: each policy would declare whether it applies at SST level,
block level, or both. On the write path, block-level builders are finalized
and reset per block in `finish_block()` instead of once at SST completion.
On the read path, per-block filter data stored in `BlockMeta` is checked
before loading a data block, using the same `might_match` interface.

## Alternatives

**1. Separate filter structures per SST:**
Maintain two separate bloom filters per SST: one for full keys and one for
prefixes then route queries to the appropriate filter. This is conceptually
clean but wastes space: two separate bit arrays have higher overhead than a
single shared one. Rejected in favor of the single-filter approach.

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

### Should `filter_policies` be an array or a single policy?

This RFC proposes `filter_policies: Vec<Arc<dyn FilterPolicy>>` (an array of
policies with a composite filter block and AND-logic evaluation). The
alternative is a simpler `filter_policy: Option<Arc<dyn FilterPolicy>>`
(singular), where each SST has exactly one filter.

**Why array is chosen:**

- **Migration**: The old policy stays in the array alongside the new one, so
  existing SSTs' filters remain usable during the transition. A singular slot
  forces an all-or-nothing switch where old filters are disabled until
  compaction rewrites all SSTs.
- **Multi-filter composition**: Users can combine complementary filter types
  (e.g., bloom + min/max) as separate policies without implementing a custom
  composite `FilterPolicy`.
- **API stability**: Starting with a singular slot and migrating to an array
  later would be a breaking API change. Starting with the array avoids this.

**Tradeoff**: The array adds complexity like composite filter block encoding, the
write path iterates over policies instead of calling one, and the read path
evaluates multiple filters in sequence. The trait interfaces themselves are
unchanged.

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

