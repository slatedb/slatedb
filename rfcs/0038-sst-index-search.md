# Faster SST Index Search

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Flat arrays](#flat-arrays)
  - [Prefix values](#prefix-values)
  - [Static B+ tree](#static-b-tree)
  - [Search](#search)
  - [API changes inside the crate](#api-changes-inside-the-crate)
  - [Safety](#safety)
  - [Disk cache format](#disk-cache-format)
  - [Optional optimization: interpolation guess](#optional-optimization-interpolation-guess)
  - [Optional optimization: SIMD](#optional-optimization-simd)
  - [Optional optimization: end search for range scans](#optional-optimization-end-search-for-range-scans)
- [Impact Analysis](#impact-analysis)
  - [Core API & Query Semantics](#core-api--query-semantics)
  - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
  - [Time, Retention, and Derived State](#time-retention-and-derived-state)
  - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
  - [Compaction](#compaction)
  - [Storage Engine Internals](#storage-engine-internals)
  - [Ecosystem & Operations](#ecosystem--operations)
- [Operations](#operations)
  - [Performance & Cost](#performance--cost)
  - [Observability](#observability)
  - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Yiming Fang](https://github.com/yiming-fang)

## Summary

Every read searches the SST index to find the data block that can hold a key. Today the index stays in memory as the FlatBuffer from the file. The search is a binary search that follows FlatBuffer offsets and compares index keys. This RFC proposes a new in-memory form of the index, built each time the index is loaded into memory. The new form has two parts: flat arrays, and a static B+ tree over numeric prefix values. Each node of the tree has 128 bytes. Its prefix values are the 8 key bytes after the prefix that the keys of the node share. In a prototype on AWS Graviton, a point search over an index of 65K blocks with synthetic keys is about 4× faster, both when the index is in the CPU cache and when it is not. The file format, the verifier, and the public API do not change.

## Motivation

Today the search works like this:

1. The cache keeps the index as the FlatBuffer from the file (`SsTableIndexOwned`). The search reads it in place with `root_unchecked`.
2. The search is a binary search over the `block_meta` vector. A compacted SST of the default size has about 65K blocks of 4 KiB, so a search takes about 16 steps.
3. Each step reads the index key of one block and compares it with the search key. The index key is a lower bound for the keys in the block. It is the shortest prefix of the first key of the block that is above the last key of the block before it. In most SSTs, the first block has an empty index key. The FlatBuffer stores the index key in the field `first_key`.
4. After the search, `partitions_covering_key` compares at least one more index key, to find the blocks whose index key equals the search key. It compares one more index key for each such block. This one-search path for point keys came from [#2203](https://github.com/slatedb/slatedb/pull/2203).

This approach has these costs:

1. One search step makes up to 12 reads, in a chain up to 9 reads long. It reads the root table and its vtable, the vector, the `BlockMeta` table and its vtable, and the key vector. The vector slot and the `BlockMeta` table, with its key, can miss the CPU cache. The vtables are shared and stay in the cache.
2. The steps of the binary search run one after the other, because each step needs the result of the step before. If the index is not in the CPU cache, each step waits for main memory before the next step can start.
3. Each step compares the search key with the index key of one block. The index key is the shortest prefix that separates the block from the block before it. Both keys have a variable length, so the compare loads the length, calls memcmp, and then compares the lengths. It is not one compare of two fixed-size values. After the search, one more equality check loads one index key length. It reads the key bytes only when the lengths are equal.

Both point gets and scans pay for the index search. A point get searches the index of each SST that its filter does not rule out. Its random keys often land in indexes that are not in the CPU cache. A scan searches the index of each SST that it reads, one time for each bound.

A profile of point gets on AWS Graviton shows the size of this cost. In the test, one SST with an index of about 65K blocks holds the key, and all data is in the block cache. A get takes about 11.6 µs, and the index search takes about 6.6% of its CPU (about 760 ns).

## Goals

- Make the index search faster, both when the index is in the CPU cache and when it is not.
- Keep the public API (`SstIndex`) unchanged.
- Use no more memory than today for indexes of the default size.
- Make sure that the search cannot panic or read out of bounds for any index that passes the verifier. The verifier does not check that the keys are sorted, so this includes indexes with unsorted keys.

## Non-Goals

- Changing the SST file format, the index schema, or the verifier. The new form is built when the index is loaded, so it needs no new format version. A later format change, such as a partitioned index ([#1068](https://github.com/slatedb/slatedb/issues/1068)), can build the same form.
- The seek inside a data block. A separate issue covers it ([#2242](https://github.com/slatedb/slatedb/issues/2242)).
- The memtable search and the filter lookup.
- Where the cache keeps the index. RFC 0037 ([#2206](https://github.com/slatedb/slatedb/pull/2206)) pins indexes on the SST handle. This RFC changes only the in-memory form of the index, so it works with either place.

## Design

### Flat arrays

```text
SsTableIndexOwned
  Arc<IndexArrays>
```

```text
IndexArrays
  offsets       [u64; n]          block offset
  encoded_lens  Option<[u32; n]>  block length (None when no block has padding)
  index_keys    [u8]              all index keys, one after the other
  key_ends      [u32; n + 1]      start of each key in `index_keys`, then the end
  tree          Option<StaticBTree>
```

When `SsTableIndexOwned::new` accepts an index, it copies the index into one array for each field and drops the FlatBuffer bytes. All index keys are in one buffer, with an array of start positions. One buffer needs one allocation, not one for each key. It also has no header for each key: a `Vec` header takes 24 bytes, more than many index keys. The keys of adjacent blocks are adjacent in memory.

In memory, the flat arrays replace the FlatBuffer as the form of the index that the search reads. The file keeps the FlatBuffer. This form makes each search step cheaper:

1. To find key `i`, the search reads `key_ends[i]` and `key_ends[i + 1]`, which are next to each other. It then reads the key bytes. There are no offsets or vtables to follow. In the FlatBuffer, the same step makes many reads through offsets and a vtable.
2. The search reads only `index_keys` and `key_ends`. The block offsets and lengths are in other arrays, so they do not take space in the cache lines that the search loads.

In an isolated test with 65K blocks (section Performance & Cost), a binary search over the flat arrays is 2.6× to 2.7× faster than the same search over the FlatBuffer when the index is in the CPU cache (164 to 189 ns against 438 to 499 ns). It is 1.8× to 2.0× faster when the index is not in the CPU cache (598 to 747 ns against 1,164 to 1,393 ns).

### Prefix values

With flat arrays alone, the search still compares index keys in each step. To make these compares cheaper, the search compares prefix values. The prefix value of a key is the 8 bytes of the key after a shared prefix. The bytes are padded with zeros and read as a big-endian `u64`. Take two keys that start with the shared prefix. If one key is smaller, its prefix value is smaller or equal. So the search can compare two `u64` values, and only equal values need an index key compare.

A shared prefix belongs to a range of keys, and its boundary keys define it. Take a group of consecutive index keys, and the index key after the group. These two boundary keys bound the group: every key of the group is at or above the first one and below the second one. The keys are sorted, so every key between the two boundary keys starts with their shared prefix. A search key between the boundary keys also starts with it, so the index needs to store only the length of the shared prefix, not its bytes.

The size of the group sets how well prefix values work. With one group for the whole SST, a single key in a different part of the key space shortens the shared prefix for all keys. With small groups, such a key affects only its own group. The next section uses groups of 15 keys, one for each node of the B+ tree. "B-Trees Are Back" takes the prefix of each B-tree node from its boundary keys in the same way, and RocksDB `format_version` 8 stores the common key prefix of each index block one time ([RocksDB PR 15117](https://github.com/facebook/rocksdb/pull/15117)).

Smaller groups keep more prefix values distinct, but they cost more. Each group stores the length of its shared prefix. The search also computes the prefix value of the key again for each group that it visits. Larger groups, such as one for the whole SST, store fewer lengths and compute fewer prefix values, but one unusual key or one large parent then makes more values equal, and each tie costs an index key compare. The Open Questions ask whether the size should be tunable.

In the isolated test, a binary search over prefix values is 2.0× to 2.3× faster than a binary search over the index keys in the flat arrays when the index is in the CPU cache (77 to 83 ns against 164 to 189 ns). It is 1.2× to 1.5× faster when the index is not in the CPU cache (413 to 532 ns against 598 to 747 ns). Both searches read the same arrays and use no tree.

### Static B+ tree

The tree holds the prefix values. A B+ tree keeps one value for each block in its leaf level, and its upper levels only guide the search.

Each node has 128 bytes and is aligned to 128 bytes. This is one cache line on CPUs with 128-byte lines, such as Apple M-series CPUs. On x86 and AWS Graviton, which have 64-byte lines, a node is two adjacent lines, and these CPUs usually fetch the two lines together. A node of one 64-byte line holds only 7 values, so the tree needs one more level (section Alternatives). Each node holds one group of keys from the previous section. One slot of the node holds the length of the shared prefix of the group. The other 15 slots hold the prefix values of its keys. The boundary keys of a node are its first index key and the first index key of the next node on the same level.

```text
StaticBTree   (5 levels for n = 65K)
  levels[4]     1 node
  levels[3]     n / 15^4 nodes
  levels[2]     n / 15^3 nodes
  levels[1]     n / 15^2 nodes
  levels[0]     n / 15 nodes      (leaves)
```

Each value of a leaf is the prefix value of the index key of one block. Each value of an upper node is the prefix value of the index key of one child. A search reads one node for each level. It computes the prefix value of the search key at the shared prefix of the node. Then it counts the values below it, and at or below it, with a plain loop.

The tree causes fewer cache misses than a binary search when the index is not fully in the CPU cache. A binary search over 65K values makes 16 steps that wait on each other, and each step can miss the cache. The tree makes 5 steps, one for each level, and its upper levels (about 40 KB for 65K blocks) often stay in the cache.

In the isolated test, with 64 indexes of 65K blocks that are not in the CPU cache, the tree is 1.5× to 1.9× faster than a binary search over the same prefix values (233 to 346 ns against 413 to 532 ns). When the whole index is in the CPU cache, the binary search is a little faster. The tree uses about 9.2 bytes for each block, about 15% more than a plain array of prefix values (8 bytes for each block).

### Search

Point gets and scans use the same search. The search goes down the tree from the root. In each node, it does these steps:

1. Take the prefix value of the key at the shared prefix of the node.
2. Find the values of the node that equal it. These are the tied children. They are next to each other.
3. If there are no tied children, the values decide. The search goes to the last child whose value is below the prefix value of the key. If there is none, it goes to the first child.
4. If there are tied children, a binary search over their index keys finds the last tied child at or below the key. The search goes to that child. If there is none, it goes to the child before the tied children.

In the leaf level, the tied children are the candidate blocks: the blocks whose prefix value equals the prefix value of the key. The search compares index keys only inside the candidates:

- A point get calls `partitions_covering_key(key)`. It returns the block before the first block whose index key is at or above the key. It also returns the blocks whose index key equals the key. There can be several, because the versions of one key can fill more than one block.
- A scan calls `partition_point_le` and `partition_point_lt` for its start and end bounds. Each returns the number of blocks whose index key is at or below the bound (`le`) or below the bound (`lt`).

The pseudocode for the search is:

```text
candidates(key, cmp):    # cmp is <= for partition_point_le, < otherwise
    if the index is empty: return 0..0
    node = root
    for each level from the root down to the leaves:
        v = prefix_value(key, node.prefix_len)
        first = count of values < v in node
        last = count of values <= v in node                 # tied values: first..last
        if level is the leaves:
            return node.start + first .. node.start + last
        child = last child in first..last whose index key cmp key   # index key compares
                or first - 1 if there is none
        node = child (clamped to the nodes of the level below)

partition_point_le(key):    # partition_point_lt uses < in place of <=
    c = candidates(key, <=)
    return first block in c whose index key is not <= key   # index key compares

partitions_covering_key(key):
    c = candidates(key, <)
    first_equal = first block in c whose index key is not below key   # index key compares
    end = first block in first_equal..n whose index key is not equal to key
    return first_equal.saturating_sub(1)..end
```

The index builds the tree when it has at least one block. For an empty index, `IndexArrays::tree` is `None`, and the search finds no blocks.

### API changes inside the crate

`RangePartitionedKeySpace` gets three methods with default bodies that keep the current behavior:

- `partition_point_le(key)`: the number of partitions whose index key is at or below `key`.
- `partition_point_lt(key)`: the number of partitions whose index key is below `key`.
- `partitions_covering_key(key)`: the partitions that can hold `key`.

The free functions in `partitioned_keyspace.rs` call these methods. `SsTableIndexOwned` and the public `SstIndex` override them. So point gets, scans, seeks, cache warming, and cache eviction all use the new search. `SstIndex::partition_point(pred)` takes a predicate, so it keeps the binary search over index keys.

`SsTableIndexOwned` gets plain accessors (`len`, `offset(i)`, `first_key(i)`, `encoded_len(i)`) in place of `borrow().block_meta()`. The code moves to `format/sst_index.rs`.

### Safety

The search uses no `unsafe` code. Nothing makes sure that the keys of an index are sorted. With unsorted keys, a search can return a wrong block, as it can today. But it cannot panic or read out of bounds. The search clamps every node index, and every range stays inside `0..n`.

### Disk cache format

The Foyer hybrid cache keeps entries in memory and on local disk. Today a disk cache entry for an index holds the FlatBuffer. With the new form, a FlatBuffer entry costs more: each write to disk encodes the FlatBuffer again, and each disk hit runs the verifier, builds the arrays, and builds the tree.

The disk cache does not have to use the format of the SST. Its entries have versions (`SerializedCachedEntry::V1` and `V2`), so a new version stores the arrays as they are in memory:

1. A write copies the arrays. It does not encode a FlatBuffer.
2. A read copies the arrays back and makes sure that they are valid, in place of the verifier. The array lengths must match. `key_ends` must start at 0, never decrease, and end at the length of `index_keys`.
3. The read builds the tree. The entry does not store it.
4. The cache still reads old entries that hold a FlatBuffer.

### Optional optimization: interpolation guess

When the prefix values are evenly spaced, the index can guess the position of a key from its value. This is interpolation, as RocksDB uses for index blocks ([RocksDB blog](https://rocksdb.org/blog/2026/05/04/interpolation-search.html)). The load fits one line through the values and measures the exact maximum error of the guess. It then picks the lowest tree level whose nodes cover all positions that the error allows. The search starts there instead of at the root.

1. The guess gives an exact bound, so the search is always correct.
2. The load cost is one more pass over the prefix values.
3. We expect it to help evenly spaced keys, such as big-endian sequence numbers. For those keys, the error is small, so the search can skip the upper levels of the tree.
4. We expect it to help little for hashed keys, and not at all for text keys, whose prefix values are not evenly spaced.

This RFC proposes the guess as a later, optional phase.

### Optional optimization: SIMD

The search counts the values in each node with a plain loop, and relies on the compiler to turn it into SIMD compares. The compiler does this on some targets but not on others. This phase writes the node count with explicit SIMD and a scalar fallback. It picks the code for the CPU at build time or at runtime.

The expected benefit is mainly for indexes in the CPU cache. There, the compares take a large part of each node visit, and SIMD replaces the 15 compares of a node with a few instructions. For indexes not in the CPU cache, the search mostly waits for memory, so the gain is small.

### Optional optimization: end search for range scans

A scan searches the index two times: one time for the start bound and one time for the end bound. For a short scan, the second search looks wasted. We measured six ways to find the end block in indexes of 65K blocks, for ranges of 1 to 64 blocks:

- Full: a second search, as today.
- Bounded: a binary search over the blocks after the start block.
- Step: a step forward one block at a time, up to 16 blocks, then a bounded search.
- Gallop: a compare with the block 1, 2, 4, and so on after the start block, then a binary search inside the last jump.
- Peek: a compare with the next 2 blocks, then a second search.
- Leaf: a search for the end block in the leaf of the start block, which the first search just read. If the end is past that leaf, a second search.

| Range | Full | Bounded | Step | Gallop | Peek | Leaf |
|---|---|---|---|---|---|---|
| 1 block | 310 / 720 ns | 323 / 1,223 ns | 204 / 639 ns | 208 / 639 ns | 206 / 642 ns | 225 / 653 ns |
| 2 blocks | 305 / 733 ns | 332 / 1,234 ns | 207 / 651 ns | 204 / 656 ns | 202 / 649 ns | 233 / 668 ns |
| 4 blocks | 308 / 748 ns | 328 / 1,235 ns | 218 / 791 ns | 219 / 788 ns | 343 / 892 ns | 248 / 700 ns |
| 8 blocks | 306 / 741 ns | 331 / 1,247 ns | 237 / 824 ns | 222 / 809 ns | 329 / 900 ns | 283 / 733 ns |
| 16 blocks | 309 / 740 ns | 344 / 1,251 ns | 280 / 880 ns | 231 / 840 ns | 331 / 898 ns | 332 / 779 ns |
| 32 blocks | 310 / 777 ns | 348 / 1,257 ns | 428 / 1,551 ns | 248 / 893 ns | 328 / 944 ns | 334 / 799 ns |
| 64 blocks | 309 / 812 ns | 355 / 1,282 ns | 435 / 1,566 ns | 267 / 973 ns | 349 / 997 ns | 330 / 809 ns |

The second full search is cheap. It takes the same tree path as the first search until the two keys go to different children, so most of its nodes are already in the CPU cache. It compares only prefix values, so its time changes little with the range.

Bounded, Step, Gallop, and Peek compare index keys, which are stored apart from the nodes. Each compare can read a new cache line. In the CPU cache, Gallop is faster than Full for every range up to 64 blocks, but gets slower as the number blocks to scan increases. Out of the CPU cache, the forward searches are about 10% faster for 1 or 2 blocks, but they are slower from 4 blocks on.

Leaf compares only the prefix values of a leaf that the first search just read. It is faster than Full for ranges of 1 to 8 blocks, by 8% to 27% in the CPU cache and by 1% to 9% out of it. For longer ranges, it falls back to a second search, and it is at most 8% slower than Full. Since the index search cost is relatively more significant for short scans, this optimization might be worth doing.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

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

- [ ] Manifest format
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
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

The read semantics do not change. Get, scan, and seek return the same blocks as today, and run faster.

## Operations

### Performance & Cost

We measured the prototype with the `sst_index_search` benchmark on AWS Graviton and on x86. Each number is the Criterion estimate of the time of one point search. A point search finds the blocks that can hold a key, and reads the first block offset. Each search waits for the one before it, as the SST lookups of a get do. "Not in the CPU cache" means 64 indexes of 65K blocks (about 140 to 215 MB), searched in random order.

On AWS Graviton:

| Case | Keys | Today | Prototype | Gain |
|---|---|---|---|---|
| 65K blocks, in the CPU cache | text | 452 to 454 ns | 114 ns | 4.0× |
| 65K blocks, in the CPU cache | hashed | 402 to 405 ns | 107 ns | 3.8× |
| 65K blocks, in the CPU cache | sequential | 401 to 406 ns | 109 to 111 ns | 3.7× |
| 64 × 65K blocks, not in the CPU cache | text | 1,159 to 1,189 ns | 292 to 294 ns | 4.0× |
| 64 × 65K blocks, not in the CPU cache | hashed | 1,040 to 1,063 ns | 250 to 264 ns | 4.1× |
| 64 × 65K blocks, not in the CPU cache | sequential | 1,053 to 1,062 ns | 261 to 265 ns | 4.0× |

On x86:

| Case | Keys | Today | Prototype | Gain |
|---|---|---|---|---|
| 65K blocks, in the CPU cache | text | 438 to 449 ns | 130 to 133 ns | 3.4× |
| 65K blocks, in the CPU cache | hashed | 442 to 466 ns | 111 to 112 ns | 4.1× |
| 65K blocks, in the CPU cache | sequential | 436 to 469 ns | 130 to 133 ns | 3.4× |
| 64 × 65K blocks, not in the CPU cache | text | 1,134 to 1,599 ns | 262 to 421 ns | 4× |
| 64 × 65K blocks, not in the CPU cache | hashed | 957 to 1,395 ns | 245 to 455 ns | 3.5× |
| 64 × 65K blocks, not in the CPU cache | sequential | 963 to 1,376 ns | 260 to 394 ns | 3.6× |

The isolated test times each step of the design on its own, on Graviton. It finds `partition_point_le` for random keys in the same synthetic indexes, with each search waiting for the one before it, and with no read of the block offset. It compares a binary search over the index keys in the FlatBuffer, the same search over the flat arrays, a binary search over prefix values (with one shared prefix for each SST and no tree), and the tree. The sections above give its results.

- Latency: point gets spend less CPU in each SST they search. A get takes 10.99 to 11.29 µs, against 11.45 to 11.70 µs today, which is about 4% less. The index search drops from 6.6% to 2.3% of the CPU of a get. Scans and seeks gain on the search when they open an iterator or seek.
- Memory: the cache weight of an index is the memory of its arrays and its tree. In the prototype, an index of 65K blocks uses 8% to 15% less memory than today, with the tree included, because the arrays drop the FlatBuffer table, offsets, and padding of each block.
- Index load: the load also builds the arrays and the tree. At 65K blocks, load and insert take 4.68 to 4.74 ms, against 6.09 to 6.19 ms today. The load alone takes longer than today (4.68 to 4.74 ms against 3.02 to 3.03 ms). But today the cache insert copies the index and runs the verifier again, while the new insert shares the arrays, so the verifier runs one time. A disk cache hit reads the arrays without the verifier and builds the tree (section Disk cache format).

### Observability

- Configuration changes: none.
- New components/services: none.
- Metrics: none. The existing read latency and CPU metrics show the effect.
- Logging: none.

### Compatibility

- Existing data on object storage / on-disk formats: the SST format and the index schema do not change. The Foyer disk cache gets a new entry version for indexes, and still reads old entries. An older SlateDB cannot read the new entries. It treats them as cache misses and reads the index from object storage.
- Existing public APIs (including bindings): no change. `SstIndex` keeps its methods and results.
- Rolling upgrades / mixed-version behavior: the SST does not change. A disk cache that both versions share gives the older version cache misses for the new entries.

## Testing

- Unit tests: the decode of each field, an empty index, and the round trip through the disk cache entry, with invalid arrays rejected. Other unit tests cover the node count (for repeated values, 0, and `u64::MAX`), the tree levels, the shared prefix of each node, and unsorted keys.
- Property tests: the search gives the same results as the current binary search over index keys, for `partition_point_le`, `partition_point_lt`, and `partitions_covering_key`. The keys have shared prefixes of up to 24 bytes, an optional empty first index key, and bytes near `0xFF`. The search keys include keys outside the first and last nodes, and keys that tie in upper nodes. The indexes have up to 5,000 blocks and up to 4 tree levels.
- Integration tests: the existing tests of `sst_iter`, `tablestore`, and `db` use the new index.
- Fault-injection/chaos tests: none needed. The SST does not change.
- Deterministic simulation tests: the existing tests run with the new index.
- Formal methods verification: none.
- Performance tests: a new Criterion benchmark, `slatedb/benches/sst_index_search.rs`, behind the `bench-internal` feature. It runs point searches on 1 index and on 64 indexes, with text, hashed, and sequential keys.

## Rollout

- Milestones / phases:
  1. The new index: flat arrays, the static B+ tree with a shared prefix for each node, the search, the new trait methods, and the disk cache format.
  2. Optional: the interpolation guess.
  3. Optional: explicit SIMD for the node search.
- Feature flags / opt-in: none. The change is internal, and it gives the same results.
- Docs updates: none for users. The code docs in `format/sst_index.rs` describe the layout.

## Alternatives

- Status quo. The search cost grows with the number of SSTs that a get searches.
- Flat arrays only. This gives up the gains of the prefix values and the tree.
- One shared prefix for each SST. It needs no slot in each node, and the search computes the prefix value of the key one time. But one key from another part of the key space shortens the prefix of the whole SST, and more values then tie.
- A binary search over the prefix values, with no tree. It misses the CPU cache more often (section Static B+ tree).
- Nodes of one 64-byte cache line on x86 and Graviton. A node holds 7 values, so 65K blocks need 6 levels. In the benchmark, 128-byte nodes are 10% to 17% faster.
- Delta-encoded keys with restart points, as RocksDB uses for data blocks. RocksDB stores index keys in full by default (`index_block_restart_interval` = 1). Delta encoding saves memory, but the search must decode keys one by one.

## Open Questions

- Should the granularity of the shared prefix be tunable? This RFC takes one shared prefix for each node. A more general design takes one shared prefix for each group of nodes, from one node up to the whole SST. A larger group needs fewer prefix lengths and fewer prefix values of the search key, but keeps fewer values distinct.
- The prefix value has 8 bytes. Is this the right size? A wider value keeps more values distinct, but a node then holds fewer values.
- Should the prefix length of a node be in a slot of the node, or in a separate array for each level? A slot needs no extra memory read, but it costs one value in each node.
- Are evenly spaced keys, such as big-endian sequence numbers, common among SlateDB users? The answer decides whether the interpolation guess is worth its own phase.
- How much faster is the node search with explicit SIMD, on x86 and on ARM?

## References

- [Issue #1069: Consider dropping the index and/or manifest flatbuffer verification](https://github.com/slatedb/slatedb/issues/1069)
- [Issue #1068: Partitioned index](https://github.com/slatedb/slatedb/issues/1068)
- [Issue #2242: Speed up the seek inside a V2 data block](https://github.com/slatedb/slatedb/issues/2242)
- [PR #2206: RFC 0037, Pinned SST filters and indexes](https://github.com/slatedb/slatedb/pull/2206)
- [PR #2203: Trim per-SST CPU on the point-lookup path](https://github.com/slatedb/slatedb/pull/2203)
- [RocksDB PR 15117: `format_version` 8 and `optimize_key_common_prefix`](https://github.com/facebook/rocksdb/pull/15117)
- [RocksDB blog: Interpolation search for SST index blocks](https://rocksdb.org/blog/2026/05/04/interpolation-search.html)
- [Pebble `PrefixBytes`](https://github.com/cockroachdb/pebble/blob/master/sstable/colblk/prefix_bytes.go)
- [Müller, Benson, and Leis, "B-Trees Are Back", SIGMOD 2025](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrees-are-back.pdf)
- [Algorithmica, "Static B-Trees"](https://en.algorithmica.org/hpc/data-structures/s-tree/)

## Updates

- 2026-11-09: first draft.
- 2026-11-17: one shared prefix for each tree node, not for each SST. Updated the prototype and the performance numbers.
- 2026-11-19: the disk cache stores the arrays, not the FlatBuffer.
