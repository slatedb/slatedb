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
  - [Optional optimization: interpolation guess](#optional-optimization-interpolation-guess)
  - [Optional optimization: SIMD](#optional-optimization-simd)
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

Every read searches the SST index to find the data block that can hold a key. Today the index stays in memory as the FlatBuffer from the file. The search is a binary search that follows FlatBuffer offsets and compares full keys. This RFC proposes a new in-memory form of the index, built each time the index is loaded into memory. The new form has three parts: flat arrays, one numeric prefix value for each block, and a static B+ tree over these values. Each node of the tree fills one cache line. In a prototype, a point search is 11× to 15× faster when the index is in the CPU cache, and 4× to 9× faster when it is not. The file format, the verifier, and the public API do not change.

## Motivation

Today the search works like this:

1. The cache keeps the index as the FlatBuffer from the file (`SsTableIndexOwned`). The search reads it in place with `root_unchecked`.
2. The search is a binary search over the `block_meta` vector. A compacted SST of the default size has about 65K blocks of 4 KiB, so a search takes about 16 steps.
3. Each step reads the index key of one block (a lower bound for the keys in the block) and compares it with the search key.
4. After the search, `partitions_covering_key` compares one more key, to find blocks that start with the search key. This one-search path for point keys came from [#2203](https://github.com/slatedb/slatedb/pull/2203).

This approach has these costs:

1. One search step makes about 9 dependent reads to reach one key: the vector, the `BlockMeta` table and its vtable, and the key vector. Some of these reads can miss the CPU cache.
2. The steps of the binary search run one after the other, because each step needs the result of the step before. If the index is not in the CPU cache, each step waits for main memory before the next step can start.
3. Each step compares two full keys byte by byte, not two fixed-size values. After the search, one more compare reads a full key.

Both point gets and scans pay for the index search, but point gets pay the most. A point get searches the index of each SST that its filter does not rule out. Its random keys often land in indexes that are not in the CPU cache.

A profile of point gets shows the size of this cost. In the test, one SST holds the key, all data is in the block cache, and a get takes about 6 to 6.5 µs. The index search takes about 11% of the CPU of a get for an index of 65K blocks (about 720 ns).

## Goals

- Make the index search faster, both when the index is in the CPU cache and when it is not.
- Keep the SST file format, the index FlatBuffer schema, and the FlatBuffer verifier unchanged.
- Keep the public API (`SstIndex`) unchanged.
- Use no more memory than today for indexes of the default size.
- Keep the search memory safe for any index that passes the verifier, including indexes with unsorted keys.

## Non-Goals

- Making the index load cheaper. Dropping or replacing the verifier is the subject of [#1069](https://github.com/slatedb/slatedb/issues/1069).
- Changing the index format in the file, such as a partitioned index ([#1068](https://github.com/slatedb/slatedb/issues/1068)).
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
  encoded_lens  Option<[u32; n]>  block length (None when all are zero)
  keys          [u8]              all first keys, one after the other
  key_ends      [u32; n]          end of each key in `keys`
  prefixes      Option<PrefixArray>
```

When `SsTableIndexOwned::new` accepts an index, it copies the index into one array for each field and drops the FlatBuffer bytes. All first keys are in one buffer, with an array of end positions.

In memory, the flat arrays replace the FlatBuffer as the form of the index that the search reads. The file keeps the FlatBuffer. This form makes each search step cheaper:

1. To find key `i`, the search reads `key_ends[i - 1]` and `key_ends[i]`, which are next to each other. It then reads the key bytes. There are no offsets or vtables to follow. In the FlatBuffer, the same step makes many reads through offsets and a vtable.
2. The search reads only `keys` and `key_ends`. The block offsets and lengths are in other arrays, so they do not take space in the cache lines that the search loads.

In an isolated test with 65K blocks in the CPU cache, a binary search over the flat arrays takes 254 ns, against 468 ns for the FlatBuffer.

### Prefix values

With flat arrays alone, the search still compares full keys in each step. To make these compares cheaper, the load first computes the shared prefix of the SST. The shared prefix is the longest prefix that every key in `keys` starts with, except the first key, which is always empty today. The keys are sorted, so the load compares only the second key and the last key. Their common prefix is also a prefix of every key between them. The shared prefix is empty if the second key and the last key start with different bytes.

If the shared prefix is short (for example, at most 16 bytes), the index stores it inline. Otherwise, the search reads the first `shared_len` bytes of the last key in `keys`.

With the shared prefix, the index stores a prefix value for each block: a fixed-size number that the search can compare cheaply. The prefix value is the 8 key bytes after the shared prefix, padded with zeros and read as a big-endian `u64`. The empty first key has the value 0. If key `a` is smaller than key `b`, the value of `a` is smaller than or equal to the value of `b`. So a smaller value means a smaller key, and a larger value means a larger key. Only equal values need a full key compare, so most search steps compare two `u64` values.

RocksDB `format_version` 8 uses a similar idea: it stores the common key prefix of each index block one time and compares only the rest of each key ([RocksDB PR 15117](https://github.com/facebook/rocksdb/pull/15117)).

```text
PrefixArray
  shared_len      usize             length of the shared prefix
  shared_inline   [u8; 16]          shared prefix, if it is at most 16 bytes
  btree           StaticBTree       the prefix values, in a B+ tree
```

As an optimization, the load builds `IndexArrays::prefixes` only if all of these conditions are true:

1. The index has at least 2 blocks.
2. The first key is empty, and no other key is empty.
3. At least half of the prefix values are distinct.

Otherwise, `IndexArrays::prefixes` is `None`, and the search falls back to a binary search over the full keys in the arrays.

The rationale for doing so is that with many equal values, the search must compare full keys inside long runs of equal values. The prefix values then give a small gain for their memory. The "at least half" condition is part of the open questions.

In an isolated test with 65K blocks in the CPU cache, a binary search over the prefix values takes 47 ns, against 254 ns over the flat arrays.

### Static B+ tree

The prefix values are the leaf level of a static B+ tree. A B+ tree keeps all values in its leaf level, and its upper levels only guide the search.

Each node is one cache line, aligned to the line. The node size comes from the target architecture at compile time. This section assumes 128-byte cache lines.

```text
StaticBTree                         (4 levels for n = 65K)
  levels[3]     1 node              first value of each node below
  levels[2]     n / 4096 nodes      first value of each node below
  levels[1]     n / 256 nodes       first value of each node below
  levels[0]     n / 16 nodes        one prefix value for each block
```

1. Each upper level holds the first value of each node of the level below, as the index holds the first key of each block.
2. `u64::MAX` pads each level to whole nodes.
3. A search reads one node for each level. In each node, it counts the values at or below the target with a plain loop.
4. The descent goes to the last child whose first value is at or below the target.

The tree causes fewer cache misses than a binary search when the index is not fully in the CPU cache. A binary search over 65K values makes 16 steps that wait on each other, and each step can miss the cache. The tree makes 4 steps, one for each level, and its upper levels (about 35 KB for 65K blocks) often stay in the cache. An isolated test used only indexes that are not in the CPU cache. In this test, a search takes about 200 to 230 ns with the tree, against 550 to 670 ns with a binary search over the prefix values. When the whole index is in the CPU cache, the two are about as fast. The tree adds about 7% to the memory of the prefix values.

### Search

Point gets and scans use the same search. Each search first uses the prefix values to find the candidate blocks. These are the blocks whose prefix value equals the prefix value of the key. Only these blocks need a full key compare. The steps for finding candidate blocks are:

1. If the key does not start with the shared prefix, the answer follows from the shared prefix alone, with no full key compare. If the key is below the shared prefix, only the first block can hold it. If the key is above the shared prefix, only the last block can hold it. This works because index keys are lower bounds, not row keys.
2. Take the prefix value of the key, and count the values at or below it in the tree. The count is `end`.
3. If the prefix value of block `end - 1` does not equal the prefix value of the key, no block has the same value, so the candidates are empty.
4. If the prefix value of block `end - 1` equals the prefix value of the key, the candidates are all blocks with that value. They are next to each other and end at `end - 1`, so the search walks back from `end - 1` to find the first one.

The search then compares full keys only inside the candidates:

- A point get calls `partitions_covering_key(key)`. It returns the block before the first block whose first key is at or above the key. It also returns the blocks whose first key equals the key. There can be several, because the versions of one key can fill more than one block.
- A scan calls `partition_point_le` and `partition_point_lt` for its start and end bounds. Each returns the number of blocks whose first key is at or below the bound (`le`) or below the bound (`lt`).

The pseudocode for the search is:

```text
candidates(key):
    if no prefix values: return 0..n
    if not key.starts_with(shared_prefix):
        return n..n if key > shared_prefix, else (0..1 if key is empty, else 1..1)
    v = prefix_value(key)
    end = tree.count_le(v)
    if end == 0 or prefix_value(block end - 1) != v:
        return end..end
    start = first position of the run of equal values before end
    return start..end

partition_point_le(key):    # partition_point_lt uses < in place of <=
    c = candidates(key)
    return first block in c whose first key is not <= key   # full compares

partitions_covering_key(key):
    c = candidates(key)
    first_equal = first block in c whose first key is not below key   # full compares
    end = first block in first_equal..c.end whose first key is not equal to key
    return first_equal.saturating_sub(1)..end
```

### API changes inside the crate

`RangePartitionedKeySpace` gets three methods with default bodies that keep the current behavior:

- `partition_point_le(key)`: the number of partitions whose first key is at or below `key`.
- `partition_point_lt(key)`: the number of partitions whose first key is below `key`.
- `partitions_covering_key(key)`: the partitions that can hold `key`.

The free functions in `partitioned_keyspace.rs` call these methods. `SsTableIndexOwned` and the public `SstIndex` override them. So point gets, scans, seeks, cache warming, and cache eviction all use the new search. `SstIndex::partition_point(pred)` takes a predicate, so it keeps the binary search over full keys.

`SsTableIndexOwned` gets plain accessors (`len`, `offset(i)`, `first_key(i)`, `encoded_len(i)`) in place of `borrow().block_meta()`. The code moves to `format/sst_index.rs`.

### Safety

The search uses no `unsafe` code. Nothing makes sure that the keys of an index are sorted. With unsorted keys, a search can return a wrong block, as it can today. But it cannot panic or read out of bounds. The search clamps every node index, and every range stays inside `0..n`.

### Optional optimization: interpolation guess

When the prefix values are evenly spaced, the index can guess the position of a key from its value using interpolation, as RocksDB does for index blocks ([RocksDB blog](https://rocksdb.org/blog/2026/05/04/interpolation-search.html)). The load fits one line through the values and measures the exact maximum error of the guess. It then picks the lowest tree level whose nodes cover all positions that the error allows, and the search starts there instead of at the root.

1. The guess gives an exact bound, so the search is always correct.
2. The load cost is one pass over the values, about 45 µs for 65K blocks.
3. It helps evenly spaced keys, such as big-endian sequence numbers. For those keys, a search in the CPU cache is about 2 times faster, and a search out of the cache is about 23% faster.
4. It gives no gain for text keys or hashed keys.

This RFC proposes the guess as a later, optional phase.

### Optional optimization: SIMD

The search counts the values in each node with a plain loop, and relies on the compiler to turn it into SIMD compares. The compiler does this on some targets but not on others. This phase writes the node count with explicit SIMD, with a scalar fallback, and picks the code for the CPU at build time or at runtime.

The expected benefit is mainly for indexes in the CPU cache. There, the compares take a large part of each node visit, and SIMD replaces about 16 compares with a few instructions. For indexes not in the CPU cache, the search mostly waits for memory, so the gain is small.

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

We measured the prototype on an Apple M4 Pro, with synthetic keys and 65K blocks. The numbers are the thread CPU time of one point search. "Not in the CPU cache" means 48 indexes searched in random order, with each search waiting for the one before it. The table measures the full search, so its numbers differ from the isolated tests in the Design section.

| Case | Keys | Today | Prototype | Gain |
|---|---|---|---|---|
| Index in the CPU cache | text | 516 ns | 45 ns | 11× |
| Index in the CPU cache | hashed | 471 ns | 31 ns | 15× |
| Index not in the CPU cache | text | 2.0 to 2.4 µs | 279 ns | 7 to 9× |
| Index not in the CPU cache | hashed | 1.1 µs | 263 ns | 4× |

- Latency: point gets spend less CPU in each SST they search. The new search removes about 76% to 93% of the search time, so a point get uses about 8% to 10% less CPU in the profile from Motivation. This is an estimate from the measurements above, not an A/B run of full gets. Scans and seeks gain on the search when they open an iterator or seek.
- Throughput: independent searches of indexes not in the CPU cache are 18 to 22 times faster (about 110 ns against 2.0 to 2.5 µs, text keys).
- Object-store requests: no change.
- Memory: the cache weight of an index is the memory of its arrays and its tree. An index of 65K blocks uses 10% to 16% less memory than today, with the tree included, because the arrays drop the FlatBuffer table, offsets, and padding of each block.
- Index load: the load also builds the arrays and the tree. At 65K blocks, load and insert take about 2.4 ms, against 3.6 ms today. A disk cache hit also builds the arrays and the tree. Today the cache insert copies the index and runs the verifier again, but the new insert shares the arrays, so the verifier runs one time.
- Disk cache writes of an index encode a FlatBuffer again. We did not measure this cost.

The numbers come from two runs on one Apple M4 Pro, with synthetic keys whose prefix values are almost all distinct. Real keys can have fewer distinct values. If the guard then drops the prefix values, the search is only about 2 times faster than today.

### Observability

- Configuration changes: none.
- New components/services: none.
- Metrics: none. The existing read latency and CPU metrics show the effect.
- Logging: none.

### Compatibility

- Existing data on object storage / on-disk formats: no change. The SST format, the index schema, and the disk cache entries stay the same. The Foyer disk cache still stores the index as a FlatBuffer: `data()` encodes the arrays again when the cache writes the index to disk.
- Existing public APIs (including bindings): no change. `SstIndex` keeps its methods and results.
- Rolling upgrades / mixed-version behavior: not affected, because nothing that is written changes.

## Testing

- Unit tests: the decode of each field, an empty index, and the round trip through `data()`. Other unit tests cover the guard for distinct values, the tree levels, and unsorted keys.
- Property tests: the search gives the same results as the current binary search over full keys, for `partition_point_le`, `partition_point_lt`, and `partitions_covering_key`. The keys have shared prefixes of up to 24 bytes, an optional empty first key, and bytes near `0xFF`. The indexes have up to 5,000 blocks and up to 4 tree levels.
- Property tests: the tree count gives the same result as `slice::partition_point` for repeated values, 0, and `u64::MAX`.
- Integration tests: the existing tests of `sst_iter`, `tablestore`, and `db` use the new index.
- Fault-injection/chaos tests: none needed. Nothing that is written changes.
- Deterministic simulation tests: the existing tests run with the new index.
- Formal methods verification: none.
- Performance tests: a new Criterion benchmark, `slatedb/benches/sst_index_search.rs`, behind the `bench-internal` feature. It runs point searches on 1 index and on 64 indexes, with text, hashed, and sequential keys.

## Rollout

- Milestones / phases:
  1. The new index: flat arrays, prefix values, the static B+ tree, the search, and the new trait methods.
  2. Optional: the interpolation guess.
  3. Optional: explicit SIMD for the node search.
- Feature flags / opt-in: none. The change is internal, and it gives the same results.
- Docs updates: none for users. The code docs in `format/sst_index.rs` describe the layout.

## Alternatives

- Status quo. The search stays at 470 ns to 2.4 µs for each SST, and the cost grows with the number of SSTs that a get searches.
- Flat arrays only, with no prefix values. The search is about 2 times faster than today. The prefix values give most of the remaining gain.
- A binary search over the prefix values, with no tree. It is as fast as the tree when the index is in the CPU cache. It is about 2.5 to 3 times slower when the index is not in the CPU cache.
- Delta-encoded keys with restart points, as RocksDB uses for data blocks. RocksDB stores index keys in full by default (`index_block_restart_interval` = 1). Delta encoding saves memory, but the search must decode keys one by one.

## Open Questions

- The guard keeps the prefix values only if at least half of them are distinct. Is this the right threshold for real keys? Keys with long shared parts after the shared prefix, such as `user:` and `session:` in one SST, can fail it.
- The prefix value has 8 bytes, and the index stores the shared prefix inline only up to 16 bytes. Are these the right sizes? A wider value keeps more values distinct, but a node then holds fewer values.
- Are evenly spaced keys, such as big-endian sequence numbers, common among SlateDB users? The answer decides whether the interpolation guess is worth its own phase.
- All measurements come from an Apple M4 Pro with 128-byte cache lines. On x86 and AWS Graviton, with 64-byte lines, is a node of one cache line faster than a node of 128 bytes? A node of one line needs more levels.
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
