# Batched Point Reads (`multi_get`)

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
  - [What a `get` in a loop repeats](#what-a-get-in-a-loop-repeats)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Overview](#overview)
  - [Public API](#public-api)
  - [Options](#options)
  - [Walk 0: memory](#walk-0-memory)
  - [Layer walks](#layer-walks)
  - [Request bound](#request-bound)
  - [Same results as `get`](#same-results-as-get)
  - [Transactions](#transactions)
  - [Failure handling](#failure-handling)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

This RFC adds `multi_get`, a call that reads a batch of keys at one time. Today
an application that needs 1000 keys calls `get` 1000 times. Each call repeats
the same setup, and reads the same filters and indexes again.

`multi_get` walks the tree one layer at a time, and not one key at a time. A
layer is one L0 SST or one sorted run. The walk has these steps:

- It answers what it can from the write batch and the memtables.
- It reads each layer, newest first, for the keys that are still open, with
  one filter probe, one index, and one read per SST.
- A key leaves the walk when a layer answers it.

Up to `lookahead` layers are in flight at one time, default 4, which is the
window that `get` uses over SSTs. So a batch sends no more object store
requests than a loop of gets.

Each key returns the same value as a `get`, and all keys of a batch read from
one state view. The change is additive: `get`, the SST format, and the
manifest stay as they are. It has one breaking change: external types that
implement `DbReadOps` must add the new methods.

## Motivation

A typical read pattern of an ML feature store is to fan out one request into
many point reads. A ranking service is a good example:
* It gets a list of candidate keys, usually 100 to 1000 of them.
* It loads a set of features for each candidate. The dataset is usually
  100+ GB.
* It runs the ML inference.

Other stores have a call for this pattern: `MultiGet` in RocksDB, `MGET` in
Redis, and `BatchGetItem` in DynamoDB. SlateDB does not, so the application
must call `get` in a loop. The RFC author maintains
[murrdb](https://github.com/murrdb/murr), which uses this pattern on RocksDB.

### What a `get` in a loop repeats

Random keys spread over the whole key space, so the SSTs at the top of the
tree serve many keys of one batch. An L0 SST covers the whole key space, and
it is a candidate for each key. A `get` knows nothing about the other keys of
the batch, so:
* Each `get` takes its own state view, builds its own iterators, and reads
  the same filters and indexes again. This work scales with the number of
  keys, and not with the number of SSTs.
* Two keys in adjacent blocks of one SST send two GET requests. A batch can
  send one.
* Parallel gets hide the latency, but not the cost. Each `get` also sees its
  own DB state, so the keys of one batch can see different states.

In the following table, N is the number of keys, M is the number of memtables,
and S is the number of candidate SSTs for a key:

| Step of `get`                     | Loop of N gets | One batch needs      |
|-----------------------------------|----------------|----------------------|
| State view, `max_seq`, trace span | N              | 1                    |
| Iterator per memtable and SST     | N × (M + S)    | 0                    |
| Binary search in each sorted run  | N per run      | 1 pass per run       |
| Filter read (cache or GET)        | N × S          | S                    |
| Index read (cache or GET)         | up to N × S    | up to S              |
| Block read (task, cache or GET)   | N              | 1 per distinct block |

## Goals

- Return the same results as a `get` for each key, with all keys reading from
  one state view.
- Share the work that a `get` in a loop repeats. Do the setup one time per
  batch, and the filter and index reads one time per SST.
- Keep the request bound of `get`. For each key, the batch reads at most
  `lookahead - 1` layers past the layer that answers it. This is the window
  that `get` uses, so a batch sends no more object store requests than a loop
  of gets.
- Bound the object store requests of one batch in flight.

## Non-Goals

- Change `get`, the SST format, the manifest, or the cache behavior. This is
  a purely additive change.
- Add new result shapes. Results come back in input order, one slot per key.
- Batch across calls or across operations. Separate `multi_get` calls do not
  share work, and range scans stay as they are.
- Beat a loop of concurrent gets on latency in every case. A batch waits for
  the slowest read of each layer in its window, and a small batch shares
  little work. With a few keys, or on a deep tree with many absent keys, the
  tail latency can be above the loop.

## Design

### Overview

`multi_get` runs one memory walk and then a series of layer walks:

- Walk 0 uses only what is in memory. It takes the state view and `max_seq`,
  sorts and deduplicates the keys, and answers what it can from the write
  batch and the memtables. It does no I/O.
- A layer walk reads one layer for all keys that are open when it starts. A
  layer is one L0 SST or one sorted run. The layer walks start newest first,
  and `lookahead` of them run at one time.

```text
keys
  |
  v
walk 0:  write batch, memtables                     in memory, no I/O
  |
  |  keys that are still open
  v
layer walks, newest first, `lookahead` = 4 in flight
  +-----------+  +-----------+  +-----------+  +-----------+
  | L0 SST #2 |  | L0 SST #1 |  | run #1    |  | run #2    |  run #3 starts
  | filters   |  | filters   |  | filters   |  | filters   |  when L0 SST #2
  | reads     |  | reads     |  | reads     |  | reads     |  is applied
  +-----------+  +-----------+  +-----------+  +-----------+
        |              |              |              |
        v              v              v              v
apply in layer order, newest first. A key with an answer leaves the walk
  |
  v
results, one slot per key
```

The walk ends when no key is open, or when the last layer is applied. One SST
serves all of its keys with one filter probe, one index, and one read per
distinct block.

The shape comes from two places:

- The window is what `get` does. It opens up to four SSTs at one time, and
  it drops the rest when one of them answers. The batch keeps the same
  window over layers, so it keeps the same request bound.
- The walk is what `MultiGet` in RocksDB does: one level at a time, for all
  keys of the batch. RocksDB reads a local disk, where one level is a short
  wait. An object store has a much higher tail latency, so the batch adds
  the window to hide the slowest read of a layer.

### Public API

`DbReadOps` gets four methods that mirror the `get` family. `Db`, `DbReader`,
`DbSnapshot`, and `DbTransaction` implement them.

```rust
async fn multi_get<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &MultiGetOptions,
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_key_value<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<KeyValue>>, Error>;

async fn multi_get_key_value_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &MultiGetOptions,
) -> Result<Vec<Option<KeyValue>>, Error>;
```

`K` needs `Sync` because the future borrows `keys: &[K]` across its awaits.
All common key types are `Sync`, so callers do not see the bound.

The result has one slot per input key, in input order:

- `None` means that the key is absent or deleted.
- A duplicate key is read one time. Its result goes to each of its slots.
- An empty input returns an empty vector.
- There is no limit on the number of keys. The caller owns the batch size, as
  it does for `WriteBatch` and for scans.

### Options

`MultiGetOptions` is a separate struct. It repeats the fields of `ReadOptions`,
in the same way that `ScanOptions` does, and adds four fields for the batch.

```rust
pub struct MultiGetOptions {
    // Same meaning as in ReadOptions.
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
    pub filter_context: Option<FilterContext>,
    pub tracing_options: Option<TracingOptions>,

    /// Layer walks in flight at one time. Default: 4, the window of `get`.
    pub lookahead: usize,
    /// Max object store requests of one batch in flight. Default: 256.
    pub max_fetch_tasks: usize,
    /// Two blocks go into one ranged GET when the gap between them is at
    /// most this many bytes. With 0, only adjacent blocks merge.
    /// Default: 64 KiB.
    pub coalesce_gap_bytes: usize,
    /// Upper size of one merged ranged GET. Default: 512 KiB.
    pub max_coalesced_bytes: usize,
}
```

Notes on the fields:

- `lookahead` trades requests for latency. With 1, the batch reads one layer
  at a time and sends the fewest requests. A value above 4 sends more
  requests than a loop of gets, see [Request bound](#request-bound).
- `max_fetch_tasks` is one semaphore over block, filter, and index reads. It
  counts requests and not SSTs.

### Walk 0: memory

Walk 0 runs before any I/O, in three steps:

1. Setup. It takes one state view and one `max_seq` for the batch, and it
   sorts and deduplicates the keys into `Bytes`. A `get` repeats this per
   key.
2. Memory. For each key, it reads the write batch, the memtable, and the
   immutable memtables, newest first. A value or a tombstone answers the
   key. A merge operand is kept, and the key stays open for its base value.
3. Hand-off. The keys that are still open become the open set of the layer
   walks.

Walk 0 builds no iterators and loads no filter. It looks at the same
in-memory tables as `get`, in the same order, with the same `max_seq`.

### Layer walks

The layers form one list in manifest order: the L0 SSTs newest first, then
the sorted runs newest first. With segments, each key walks the list of its
segment. One layer walk does three steps for the keys that are open when it
starts:

1. Candidates. It maps each key to the SST of the layer that can hold it.
   An L0 SST can hold any key. In a sorted run, the binary search of `get`
   finds the SST, or two adjacent SSTs when the versions of a key can span
   both. This step does no I/O.
2. Filters. It probes the filter of each candidate SST, and it drops the
   keys that the filter rejects. Filters that are not in the cache load in
   parallel, one request per SST.
3. Reads. For each SST that still has keys, it reads the index, finds the
   block of each key, and reads the blocks. Adjacent blocks merge into one
   ranged GET, and cached blocks cost no request. All ranges of an SST go
   out at the same time.

Up to `lookahead` layer walks run at one time. A walk takes the open set
when it starts, and the entries of the walks apply in layer order:

```text
open: a b c d e                                          time -->
L0 #2   [a b c d e] === b found ===>                     b leaves
L0 #1   [a b c d e] ====== d found ======>               d leaves
run #1  [a b c d e] ==== nothing ====>
run #2  [a b c d e] ========= a, c found =========>      a, c leave
run #3                              [e] starts when L0 #2 is applied
run #4                                     [e] starts when L0 #1 is applied
        |<-------- lookahead = 4 layers in flight -------->|
```

```text
for entries in layers.map(walk_layer).buffered(lookahead):
    apply(entries)              # newest layer first, per key
    open -= answered keys       # a value or a tombstone answers a key
```

A walk starts when the oldest walk in the window is applied, so it never
reads a key that an applied walk answered. A key with merge operands keeps
them and stays open for its base value. The walks are futures on the task
of the batch, not spawned tasks, and one semaphore bounds their requests.

Most of a walk is existing code:

| Step of a walk          | Code it calls                                |
|-------------------------|----------------------------------------------|
| Candidates in a run     | `tables_covering_point_key`, as in `get`     |
| Filter probe and load   | `TableStore::read_filters`, with the meta cache |
| Index and block reads   | `TableStore::read_index`, `read_blocks_using_index` |
| Block merging with gaps | New, in the `multi_get` module               |
| Entry rules per key     | The merge operator iterator of `get`         |

### Request bound

A batch sends at most the requests of a loop of concurrent gets, plus three
filter probes per key that the newest layer answers. The reasons:

- A walk starts only when the oldest walk in the window is applied, so a
  key that layer j answers is open in layers j+1 to j+3 at most. These are
  the SSTs that `get` touches with its window, see [Overview](#overview).
  A touch is a filter probe, and a block read only when the filter passes.
- The one difference is the start. The batch opens the first `lookahead`
  layers at once, and `get` opens the newest SST alone. In a batch of 100
  keys, some keys always miss the newest layer, so the batch reaches the
  next layers anyway. The extra probes belong to the keys that the newest
  layer answers, and only a false positive turns a probe into a block read.

Per SST, the batch sends fewer requests than the loop. One filter load and
one index load serve all keys of the SST, adjacent blocks go out in one
ranged GET, and a duplicate key is read one time.

With `lookahead = 1` the batch sends no speculative request, and it waits
for the slowest read of every layer in sequence. This setting measures the
price of the window.

### Same results as `get`

Each slot holds what a `get` of that key returns, because the batch reuses
the rules of `get`:

- The entries of a key are collected newest first: write batch, memtables,
  then the layers in order.
- An entry above `max_seq` is dropped when it is collected, as in `get`.
  Write batch entries skip this filter.
- The final value comes from the merge operator iterator of `get`. A
  tombstone gives `None`.
- A candidate SST passes the same key range test as in `get`, which
  includes the visible range of a clone.

Edge cases:

- Two adjacent SSTs of one run can hold versions of one key. The walk reads
  both and applies them newest first.
- A key with merge operands and no base value in any layer resolves from
  the operands alone, as in `get`.
- A block larger than `max_coalesced_bytes` is read alone.
- A layer with no candidate for any open key is skipped with no I/O.

### Transactions

`DbTransaction::multi_get` follows `get`:

- It reads the write batch first. Entries of the write batch skip the
  `max_seq` filter.
- It looks up each key in the write batch under the read guard, as `get` does.
  It never clones the write batch, which can be large.
- Under SSI, it records each key of the batch with `track_read_keys`,
  including the keys that return `None`. Under SI, it records no keys, as
  `get` does.

### Failure handling

- An error in a filter, index, or block read fails the batch with that error.
- The other loads and reads of the batch are dropped.
- There are no partial results.
- A retry of the batch is safe. `multi_get` has no side effects except cache
  fills.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

`DbReadOps` gets four new methods and a new `MultiGetOptions` struct. `get`
does not change. There are no new error kinds: a batch fails with the same
errors as a `get`.

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [x] Snapshots
- [x] Sequence numbers

- A batch computes `max_seq` one time and reads all keys from one state view.
  With `dirty: true`, `Memory` durability, and no snapshot, `max_seq` has no
  bound, so the batch is not atomic. This is the same as a `scan` with `dirty: true`.
- `DbSnapshot::multi_get` reads at the sequence number of the snapshot.
- `DbTransaction::multi_get` reads the write batch first. Under SSI, it records
  each key for conflict detection.

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [x] Merge operator
- [ ] Change Data Capture (CDC)

- A key with a merge operand stays open until the batch finds its base value.
  The batch then runs the same merge operator iterator as `get`.
- TTL is not affected. `get` does not filter expired rows at read time, and
  `multi_get` follows it.

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [x] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

A clone can see only part of an SST. A key outside the visible range of an SST
does not get that SST as a candidate, as in `get`.

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

- A walk loads the filter of an SST only when an open key reaches that SST,
  as `get` does. The load goes through the cache.
- The walks fill the cache as `get` does, and they respect `cache_blocks`.
- The batch reads each filter and index one time per SST, not one time per
  key. The formats do not change.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

- New metrics count the batches and the keys. The `slatedb.read` span gets
  new fields.
- The language bindings come in a later phase. See [Rollout](#rollout).

## Operations

### Performance & Cost

TODO: measure the layer walk and fill in this section. The plan is to use
`slatedb-bencher` (the `mget` command) with the same setup as for the
earlier versions of this RFC:

- Readers: a loop of `get`, 100 `get` calls in flight at once, and one
  batch.
- Data: 10M and 100M rows, random keys, batches of 100 and 1000 keys.
- Storage: all blocks in RAM, a local disk cache, and the `s3` and `s3x`
  latency profiles of the object store client.
- Settings: `lookahead` 1 and 4, to show the price of the window.

The numbers to report per case are p50 and p99 per batch, keys per second,
and GETs and bytes per batch.

Space and write amplification do not change. Read amplification per key is
the same as for `get`, or lower when keys share a block.

### Observability

Three new counters, as the write path counts batches and operations apart:

| Metric                                      | Grows by                      |
|---------------------------------------------|-------------------------------|
| `slatedb.db.request_count{op="multi_get"}`  | 1 per call                    |
| `slatedb.db.multi_get_keys`                 | Number of input keys per call |
| `slatedb.db.multi_get_layers`               | Layer walks started per call  |

`request_count{op="get"}` does not change, so dashboards for point reads do
not jump. The filter and cache counters count as they do for `get`.

A batch with `tracing_options` opens one `slatedb.read` span with two new
fields, `keys` and `layers`. The filter, index, and block spans stay one
span per SST. There is no new configuration outside `MultiGetOptions`, and
there are no new log lines.

### Compatibility

- Data on object storage does not change, and mixed versions are safe.
- `get`, `scan`, and the language bindings do not change.
- `DbReadOps` gets two methods with no default body. This breaks each type
  outside SlateDB that implements the trait. Code that only calls the trait is
  not affected.

A GitHub code search finds one such project,
[HelixDB](https://github.com/HelixDB/helix-db), with one production type and
four test doubles. The fix is a few lines per type: a wrapper forwards to the
`multi_get` of its inner type, and a test double calls its own `get` in a
loop.

A default body can avoid the break. See [Open Questions](#open-questions).

## Testing

`get` is the oracle. A batch is correct when each slot holds what a `get`
returns for that key on the same snapshot. The tests land in the same order
as the code, see [Rollout](#rollout):

1. Acceptance tests, with the public API. The first implementation is a
   loop of `get` over one state view. `tests/multi_get.rs` asserts against
   it that a batch agrees with a `get` loop on a `Db`, a `DbReader`, a
   `DbSnapshot`, and a transaction, with and without a merge operator, and
   after a compaction. These tests do not change when the layer walk lands.
2. Differential tests, with the layer walk:
   - A layered fixture forces a compaction and then adds new L0 SSTs, as
     `tests/scan_model.rs` does, so one batch sees keys in L0 and in sorted
     runs.
   - A counting object store asserts the request bound with `lookahead` 1
     and 4, and with a cold, a partly warm, and a warm cache.
   - Fault tests assert that a failed read fails the batch, and that a
     dropped batch leaves no request in flight.
   - Unit tests cover walk 0, the candidates of one layer, the window, and
     the read of one SST, as `rstest` tables next to the code.
3. Deterministic simulation. The `slatedb-dst` workload gets a `MultiGet`
   operation that asserts each slot, as `verify_get` does.
4. Performance. `slatedb-bencher` gets an `mget` read mode, an object store
   wrapper that injects latency from a percentile profile and counts
   requests, and key generators for random, fixed, and Zipf keys.
   `benches/db_operations.rs` compares a batch with a `get` loop.

## Rollout

The work lands as a series of pull requests into `main`, each one small
enough for a review of its own:

1. This RFC.
2. Public API and a reference implementation. The `DbReadOps` methods,
   `MultiGetOptions`, and a loop of `get` over one state view. The
   acceptance tests freeze here. `main` holds a correct and slow
   `multi_get` until step 4.
3. Bench harness. The `mget` read mode of `slatedb-bencher`, the object
   store wrapper with latency profiles and request counts, and the key
   generators. It measures the reference implementation first.
4. The layer walk. Walk 0, the layer walks with the window, and the SST
   reader with block merging.
5. Deterministic simulation tests.
6. Docs.
7. Language bindings, one pull request per binding.

There are no feature flags.

## Alternatives

### Per-key pipeline

Each key walks its own list of candidate SSTs. When a read returns, its keys
pick their next SST at once, and no key waits for another key. An event loop
drives the picks and the reads. This was v4 of this RFC.

- For: a batch pays about one tail latency of the object store. On the `s3`
  profile it measured 115 ms at p50 against 120 ms for the concurrent loop.
- Against: the event loop needs a scheduler per key, waiters per filter
  load, and rules for reads of one SST that overlap. The code is hard to
  follow and hard to maintain.
- Against: no plan per layer, so two keys of one SST can read it in two
  turns, and fewer blocks merge.

### Layer walk with no window

Read one layer at a time, and wait for every read of a layer before the
next layer starts. This is `lookahead: 1`.

- For: the simplest loop, and no speculative request.
- Against: a batch pays the slowest read of every layer in sequence. v2 of
  this RFC read in lockstep and was 25 to 40 percent slower than the
  concurrent loop on S3. A model of the `s3` profile puts the plain layer
  loop at about 2.3 times the concurrent loop at p99.

### An error per key

RocksDB returns a status per key, and a caller gets the values of the keys
that succeeded. This RFC fails the whole batch on the first error.

- For: a caller with 1000 keys keeps 999 values when one block is corrupt.
- Against: most errors in SlateDB hit a whole block or a whole SST, and one
  such error takes many keys of the batch with it. A partial result then
  needs a retry of the failed keys anyway.
- Against: the public `Error` is not `Clone`, so one failed block cannot
  give its error to each of its keys without an API change.

## Open Questions

The new `DbReadOps` methods get no default body. This is a decision that
asks for agreement, because it is the one breaking change of this RFC:

- A default body that calls `get` in a loop keeps external implementors
  compiling. A wrapper around a `Db` then reads each key from a different
  state view, and no compiler error tells the author.
- The default body cannot pin one view. `ReadOptions` has no public
  `max_seq`, and a bare `max_seq` does not protect old versions from
  compaction.
- One external project implements the trait today, see
  [Compatibility](#compatibility). The fix is a few lines per type.

## References

- an older draft slop-grenade PR (by the same author): https://github.com/slatedb/slatedb/pull/1810
- Issue: https://github.com/slatedb/slatedb/issues/301

## Updates

* v1: (xx.07.2026) initial draft
* v2: (21.09.2026) major update
* v3: (22.09.2026) the read phase is an event loop, not a loop of steps
* v3.1: (22.09.2026) measured numbers in Performance & Cost
* v4: (22.09.2026) filters are checked when a key reaches an SST, and the
  reads are futures on the batch task with one path for warm and cold data
* v5: (28.09.2026) the batch walks the tree one layer at a time, with a
  window of `lookahead` layers in flight as in `get`. The per-key pipeline
  moves to Alternatives, and the performance numbers wait for a rerun
