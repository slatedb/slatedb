# SlateDB Range Deletes

Table of Contents:

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background: RocksDB](#background-rocksdb)
- [Design](#design)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Future Work](#future-work)
- [Open Questions](#open-questions)
- [References](#references)

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC adds `delete_range` to SlateDB. One write deletes every key in a range.

The design adds a range tombstone: a start key, an end key, and a sequence number. The WAL and
memtable store it as a new entry kind. SSTs store it in the SST index, as non-overlapping
fragments. Reads already load the index, so they get the tombstones with no extra fetch.
Compaction drops covered entries, and later drops the tombstone.

## Motivation

SlateDB has no range delete. To clear a range, a user must scan it and write one tombstone per key.
This has three problems:

- It costs O(n) reads and writes. On object storage, this means real request and byte cost.
- It is not atomic. A concurrent writer can add keys to the range after the scan.
- It adds n tombstones that compaction must later remove.

Typical uses: drop a table or tenant stored as a key prefix, and truncate a log below an offset.

## Goals

- Add `Db::delete_range` and `WriteBatch::delete_range` with `[start, end)` semantics.
- Keep correct visibility for sequence numbers, snapshots, transactions, `get`, and `scan`.
- Make the write cost independent of the number of covered keys.
- Add no read cost to databases that never use range deletes.
- Let compaction reclaim covered data, then the tombstone.

## Non-Goals

- Atomic drop of whole segments (RFC 0024).
- Delete by predicate.

## Background: RocksDB

RocksDB supports `DeleteRange(begin, end)` with an exclusive end. Three points guide this RFC.

- **Separate storage.** RocksDB does not mix range tombstones with point keys. It writes them to a
  meta block in each SST. This RFC stores them in the SST index instead (see
  [Alternatives](#alternatives)).
- **Raw entries, fragmented in memory.** A block entry is the begin key plus seq, with the end key
  as the value. The reader splits overlapping tombstones into non-overlapping fragments when it
  loads the block. A binary search then finds the covering fragment. The block is a normal
  key-value block. It is loaded when the table opens, and it goes through the block cache.
- **Compaction.** RocksDB writes tombstones clipped to each output file's key range. It drops a
  tombstone at the bottom level when no snapshot can see older data. It also drops one when no
  data exists below the output level for that range.

## Design

### API

```rust
impl Db {
    /// Deletes every key in `range`. Either bound can be unbounded.
    /// Returns an error if the range is empty or inverted.
    pub async fn delete_range<K, T>(&self, range: T) -> Result<WriteHandle, Error>
    where K: AsRef<[u8]>, T: RangeBounds<K>;
    pub async fn delete_range_with_options<K, T>(
        &self, range: T, options: &WriteOptions,
    ) -> Result<WriteHandle, Error>
    where K: AsRef<[u8]>, T: RangeBounds<K>;
}

impl WriteBatch {
    pub fn delete_range<K, T>(&mut self, range: T)
    where K: AsRef<[u8]>, T: RangeBounds<K>;
}
```

SlateDB normalizes the bounds to `[start, end)`:

- `Included(s)` maps to `s`. `Excluded(s)` maps to `s` followed by `0x00`.
- An unbounded start maps to the key `0x00`. Keys cannot be empty, so `0x00` is the smallest key.
- `Excluded(e)` maps to `e`. `Included(e)` maps to `e` followed by `0x00`.
- An unbounded end has no end key. It sorts after every key.

`delete_range(..)` clears the whole database.

### Logical model

A range tombstone is `(start, end, seq_t)`. It covers a point entry `(key, seq_e)` when
`start <= key < end` and `seq_t > seq_e`.

A tombstone never covers an entry with the same seq. All operations in a `WriteBatch` share one
seq. The batch itself resolves their order (see Write path).

A reader ignores tombstones that its snapshot cannot see. The rule for point entries applies
unchanged.

A covering tombstone ends a merge chain. It acts like a point tombstone.

### Write path

1. `delete_range` adds a `RangeDelete { start, end }` operation to the `WriteBatch`, with the
   batch seq. A point `delete` replaces earlier operations on its key. In the same way,
   `delete_range` removes earlier operations in its range, at call time. Later operations have
   the same seq as the tombstone, so it does not cover them.
2. In memory, a range tombstone is a `RowEntry` with the start key and a new
   `ValueDeletable::RangeTombstone(end)` variant. `end` is `None` for an unbounded end. The WAL
   stores it as a new row kind. The pluggable WAL (RFC 0030) and CDC already pass rows as
   `RowEntry`, so they carry it with no new interface. Replay puts it in the memtable.
3. Each memtable keeps a list of range tombstones next to its skiplist. Few tombstones are
   expected, so an append-only list is enough. Reads on the active memtable scan the list. When
   the memtable becomes immutable, it fragments the list for faster reads.
4. Flush merges tombstones that have the same seq and overlap or touch. It fragments the result
   and writes the fragments to the SST index. It widens `first_entry` and `last_entry` to cover
   the tombstones, and it sets the tombstone summary fields in `SsTableInfo` (see
   [SST format](#sst-format)).

### SST format

The SST layout does not change. The index gets a list of range tombstone fragments:

```
table RangeTombstoneFragment {
    start: [ubyte] (required);
    end: [ubyte];               // absent means +infinity
    seqs: [ulong] (required);   // descending, not empty
}

table SsTableIndex {
    block_meta: [BlockMeta] (required);
    range_tombstones: [RangeTombstoneFragment];  // absent when the SST has no tombstones
}
```

The writer fragments the tombstones. Fragments sort by start key and do not overlap. A fragment
lists the seq of every tombstone that covers it. For example, `[a,z)@5` and `[f,k)@8` become
`[a,f)@5`, `[f,k)@8,5`, and `[k,z)@5`. The flush or compaction that writes the SST holds all its
tombstones in memory, so fragmenting costs little.

Why the index:

- `get` loads the index to find the data block, and a scan loads it when it opens an SST. The
  tombstones come in the same read and the same cache entry. They need no second fetch or cache
  lookup, and they cannot be evicted apart from the index.
- Readers binary-search the fragments in place, as they do `block_meta`. No load-time pass and no
  separate cached structure is needed.

A lookup is one binary search plus a scan of a short `seqs` list. Compaction keeps fragments
mostly to one seq (see [Compaction](#compaction)).

Tombstones add to the index size, and so to its cache weight. Few tombstones are expected. See
[Future Work](#future-work) for SSTs with many tombstones.

`SsTableInfo` gets optional fields. SSTs without tombstones omit them.

```
range_tombstone_min_key: [ubyte]; // smallest start key; absent when the SST has no tombstones
range_tombstone_max_key: [ubyte]; // largest end key
range_tombstone_max_seq: ulong;   // largest tombstone seq
```

When an SST has tombstones, an absent `range_tombstone_max_key` means +infinity.

The manifest holds `SsTableInfo`, so readers can skip the index fetch when a filter rules out
the key (see [Read path](#read-path)). `SstStats` gets a `num_range_deletes` counter.

**Bounds.** `first_entry` and `last_entry` cover both point entries and tombstones. Sorted-run
lookups, L0 pruning, and scan pruning use these fields. With point-only bounds, a tombstone
outside the point keys would be invisible, and a tombstone-only SST would have no start key.

- `first_entry` is the smaller of the first point key and the first tombstone start.
- `last_entry` is the larger of the last point key and the last key that a tombstone covers.
  For an end key `x` followed by `0x00`, that key is exactly `x`. For any other end key, use the
  end key itself. That bound is one key too wide, which is harmless.
- The compactor requires each output SST's `last_entry` to be at most the next SST's
  `first_entry`. Clipped tombstones meet this rule, including the boundary-key case in
  [Compaction](#compaction).
- An unbounded tombstone end leaves `last_entry` absent. An absent `last_entry` already means
  unbounded.

**Tombstone-only SSTs.** An SST with no data blocks is well-defined:

```
filter (absent) | index (tombstones only) | stats | SsTableInfo | footer
```

- The index holds an empty `block_meta` list and the tombstone fragments. Lookups and iterators
  find no point entries.
- `filter_len` is 0, which already means "no filter".
- Stats report zero puts, deletes, and merges.

### Read path

`get(key)`:

1. Walk the layers as today, newest first: the transaction's own `WriteBatch` if any,
   memtables, L0 SSTs, sorted runs.
2. In each layer, also find the highest visible tombstone seq that covers `key`.
3. A covering tombstone acts like a point tombstone at its seq. An entry in the same layer with
   a seq at or above the tombstone seq still wins (see [Logical model](#logical-model)). Entries
   with a lower seq, in this layer or any older layer, are hidden. So the lookup, or a merge
   chain, ends at that layer.

This relies on layer order. Within a tree, every write in a newer layer has a higher seq than
every write in an older layer. Memtables flush in seq order, and compactions merge only
consecutive sources. Point lookups already rely on this order for each key.

SSTs without tombstones cost one absent-field check. When the reader loads an SST's index for
the point lookup, it gets the tombstones at no extra cost. A fetch only for tombstones occurs
when the filter rules out the key. The reader can skip that fetch using the summary fields in
`SsTableInfo`:

- `key` is outside `[range_tombstone_min_key, range_tombstone_max_key)`.
- The SST holds the point entry that matched, and that entry's seq is above
  `range_tombstone_max_seq`. No tombstone in this SST can cover it.

`scan(range)`:

1. Each source supplies its tombstones as the scan reaches them. The sources are the
   transaction's own `WriteBatch` if any, memtables, L0 SSTs, and sorted runs. The active
   memtable fragments a copy of its list when the scan starts. Later tombstones have higher
   seqs, so the scan cannot see them.
2. A sorted run gets an SST's tombstones from its index when the scan's current key enters that
   SST's key range. Compaction clips tombstones to the SST that owns them, so the run needs no
   other SST.
3. An L0 SST gets its tombstones from its index when the scan opens it. Skip any SST whose
   tombstone bounds do not intersect the scan range.
4. Each source keeps a cursor into its fragments. The cursor follows the scan's current key, not
   the source's point iterator. A run's point iterator can leave an SST, or skip a
   tombstone-only SST, before the scan passes that SST's tombstones. At each key, a source has
   at most one covering fragment. An entry is hidden when the highest visible seq among those
   fragments is above the entry's seq.
5. The cursors work in both directions, because scans can be descending. `seek(k)` moves each
   cursor by binary search, so a scan can start inside a range.

### Compaction

All strategies and subcompactions follow these rules.

**Merging.** The compactor reads the tombstones of all inputs and applies these rules:

1. Tombstones with the same seq that overlap or touch merge into one range. This is always safe.
2. For overlapping tombstones with different seqs, keep the highest seq in each piece of the
   overlap. For example, `[a,z)@5` and `[f,k)@8` become `[a,f)@5`, `[f,k)@8`, and `[k,z)@5`.
   Do not widen the higher tombstone. That would delete keys written between the two seqs.
3. Rule 2 applies only when the higher seq is at most `retention_min_seq`. Then every snapshot
   reads at or above both seqs. Otherwise both entries stay, and they overlap.

**Covered entries.** A tombstone drops the point entries it covers only when
`seq_t <= retention_min_seq`, or when no snapshot exists. A snapshot below `seq_t` still sees the
entry and not the tombstone. `retention_min_seq` comes from `recent_snapshot_min_seq` in the
manifest, as for point entries in `RetentionIterator`.

**Dropping tombstones.** This follows the point-tombstone rule. A compaction drops a fragment
when both conditions hold:

- The destination is the last sorted run of the segment's tree, or the tree has no sorted runs.
  This is `is_dest_last_run` in `compaction_worker.rs`. The test covers the whole run. It does
  not check whether data below overlaps the range.
- Either no snapshot exists, or `seq_t <= retention_min_seq`.

A key-range-aware rule, as in RocksDB, is a possible later optimization.

**Splitting across SSTs.**

1. Each output SST owns the key range from its first key up to the next SST's first key. The
   first SST of a run owns from the run's lower bound. The last SST owns up to the run's upper
   bound. A key's versions can span two adjacent SSTs, because a run allows a key in more than
   one SST. When an SST's last key equals the next SST's first key, the SST's range also
   includes that key. Both SSTs then cover the key. The duplicate coverage is harmless, because
   the pieces share one seq. Pebble documents the failure that this rule prevents.
2. The compactor clips each tombstone to every output SST it overlaps. It writes one entry per
   SST. A wide tombstone, such as `delete_range(..)`, appears once in every SST of the run.
3. The output SST size limit counts tombstone bytes as well as point bytes.
4. Tombstone-only SSTs are allowed. They occur in these cases:
   - Flush of a memtable that holds only range deletes.
   - Compaction into a run that is not the last, where the tombstone drops every covered entry
     but must stay to hide older runs. For example, `delete_range(..)` compacts to one SST with
     one tombstone.
   - Any output region that has tombstones but no point entries.

   Compaction into the last run with no snapshots can drop both the entries and the tombstone.
   The output run is then empty. Point tombstones already cause this case: a last-run compaction
   drops every key whose oldest retained version is a tombstone. Range deletes use the same path.
5. A view's effective range also clips its tombstones. A fragment outside that range must not
   hide keys in other SSTs, including SSTs from another union source.
6. Subcompaction split points follow the same rules.
7. Each segment tree (RFC 0024) is independent. Flush splits tombstones per segment (see
   [Segments](#segments)). Compaction and the last-run rule then apply per tree.

**Whole-SST drop (optimization).** The compactor drops an input SST without reading it when a
tombstone from a newer input layer covers the SST's whole bounds, `first_entry..=last_entry`,
and `seq_t <= retention_min_seq`. Layer order makes `seq_t` higher than every seq in the SST. The
bounds include the SST's own tombstones, so those are covered too.

Compaction filters (RFC 0017) do not see range tombstones. `Modify` has no meaning for a range.
Dropping a tombstone outside the last run brings back older data. Only `Keep` remains, and CDC
and metrics already report range deletes. As with point tombstones, hidden data never reaches
the filter. A later RFC can add a callback with a default of `Keep` without breaking filters.

### Segments

RFC 0024 gives each segment its own tree. One memtable holds writes for all segments. Flush
writes L0 SSTs per segment tree. A range delete can span many segments.

- **WAL and memtable.** They hold one unsplit tombstone. CDC sees one entry, and transaction
  conflict checks use the full range.
- **Flush.** Flush clips the tombstone to each segment it overlaps. A segment with prefix `p`
  covers `[p, successor(p))`. A prefix of only `0xFF` bytes has no successor, so its range has no
  end. Each piece goes to the L0 of its segment, with the original seq. All pieces share one
  seq, so readers see the delete atomically.
- **Why flush, not call time.** A concurrent write can create a segment in the range, with a seq
  below the tombstone seq. A split at `WriteBatch::delete_range` time would miss that segment,
  and its data would survive. At flush, every segment with data below `seq_t` is known. It is in
  the manifest, or in this memtable or an earlier one.
- **Segments created later.** A segment created in the same memtable at a higher seq also gets a
  piece. This is harmless, because the tombstone does not cover higher seqs.
- **Gaps.** RFC 0024 requires every key to map to a segment. No data exists between segments, so
  those parts of the range need no tombstone.
- **Validation.** A range delete creates no segment. It skips the route and antichain checks.
- **No extractor.** The database has one segment with the empty prefix. No split happens.
- **Write amplification.** A range delete that spans many segments writes one tombstone piece
  to the L0 of each segment. For example, `delete_range(..)` over 10,000 segments costs up to
  10,000 L0 SST writes at flush, plus the compactions that follow. To remove whole segments,
  RFC 0024 segment drop is the cheaper tool.

### Other interactions

- **Transactions.** See [Transaction conflicts](#transaction-conflicts).
- **CDC.** The WAL holds range tombstones as their own row kind. CDC (RFC 0019) returns them as
  rows with the new `ValueDeletable` variant, in WAL order. Consumers must handle the variant.
  The batch has already dropped covered operations, so the WAL replays to the same state.
- **TTL.** No change. Range tombstones have no TTL.
- **Union and projection.** Each union source keeps its own seqs, so a tombstone must never apply
  to another source's keys. Union requires disjoint effective ranges, and a tombstone applies only
  inside its view's effective range. In a projected clone, `delete_range` behaves like `put`
  today: SlateDB does not check it against the clone's range. A range delete wider than the
  clone's data widens the clone's range, and a later union can reject the clone. Projecting each
  source with `projection_range` avoids this, as the admin API recommends.
- **Clones and checkpoints.** No change from point tombstones. A checkpoint pins a manifest and
  its SSTs, so compaction does not change what it sees. Only in-process snapshots feed
  `recent_snapshot_min_seq`.

### Transaction conflicts

`TransactionState` tracks `write_keys`, `read_keys`, and `read_ranges`. At commit, it checks
them against transactions that committed after it started. SI checks write-write conflicts. SSI
also checks read-write conflicts.

A range delete writes every key in its range, so `TransactionState` gets a `write_ranges` list.
Each check gains range cases. "Other" means a transaction that committed after this one started.

Write-write check. Existing: my `write_keys` meet other `write_keys`. Added:

- my `write_keys` fall in other `write_ranges`
- my `write_ranges` contain other `write_keys`
- my `write_ranges` overlap other `write_ranges`

Read-write check (SSI). Existing: my `read_keys` meet other `write_keys`, and my `read_ranges`
contain other `write_keys`. Added:

- my `read_keys` fall in other `write_ranges`
- my `read_ranges` overlap other `write_ranges`

Non-transactional `WriteBatch` commits record `write_ranges` too. So a range delete conflicts
with a concurrent point write in its range, as a point delete does.

Cost: point-only transactions pay nothing. Range checks scan the other side's `write_keys`, as
the `read_ranges` check does today.

## Impact Analysis

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [x] Range deletions
- [x] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [x] Snapshots
- [x] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [x] Compaction filters
- [x] Merge operator
- [x] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [x] Manifest format
- [x] Checkpoints
- [x] Clones
- [ ] Garbage collection
- [x] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [x] Compaction filters
- [x] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [x] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [x] SST format or block format

### Ecosystem & Operations

- [x] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Writes:** O(1) per range delete in the WAL and memtable. With segments, flush writes one
  piece per spanned segment (see [Segments](#segments)).
- **Point reads:** one binary search per SST that has tombstones. The tombstones come with the
  index, so they add no fetch in the common case (see [Read path](#read-path)).
- **Scans:** O(log n) per seek, O(1) amortized per entry.
- **Compaction:** space returns in proportion to the covered data. Long-lived snapshots can pin
  many small tombstones. This raises read cost.
- **Space:** covered data stays until compaction reaches it. Tombstones stay until the bottom run.

### Observability

- Metrics: tombstones written, tombstones consulted per read, entries dropped by tombstones, SSTs
  dropped whole.
- Logging: compaction logs show tombstone counts in and out.
- Configuration: none in the first version.

### Compatibility

- **On disk:** new optional `SsTableInfo` fields, a new optional index field, and a new row
  kind.
- **Why the SST version is not enough.** Old processes check an SST's format version only when
  they open a data block or read the SST footer. They read `SsTableInfo`, the filter, and the
  index from the manifest copy without a check. So an old reader skips a tombstone-only SST, or
  an SST whose filter rules out the key, and returns deleted data from an older layer. An old
  process that rewrites the manifest, such as a reader that writes a checkpoint, also drops the
  new `SsTableInfo` fields (see #779). Then even new readers miss the tombstones.
- **Manifest version.** This RFC bumps the manifest format version to 3. The decoder rejects
  unknown versions with `InvalidVersion`, so old processes fail at open. They cannot read or
  rewrite the manifest. The bump is blanket: every database gets v3, whether or not it uses
  range deletes. v3 keeps the `ManifestV2` schema, and only the number changes. RFC 0024 plans a
  later `ManifestV3` schema cleanup, which would take the next number.
- **SST version.** New SSTs, WAL SSTs included, use SST format version 3. This protects tools
  that read SSTs directly, such as the WAL reader for CDC. `decode_flags` also rejects the new
  row kind.
- **API:** the `Db` and `WriteBatch` methods are additive. `ValueDeletable` is public and
  exhaustive, so the new variant breaks code that matches on it. This includes compaction
  filters, CDC consumers, and custom WAL implementations (RFC 0030). Custom WAL implementations
  must also persist the variant. Bindings add `delete_range`.
- **Upgrades:** upgrade every reader, compactor, garbage collector, and CLI before the writer.
  After the writer runs the new version, older versions cannot open the database, and downgrade
  is not possible.

## Testing

- Unit: fragmentation, covering-seq lookup, SST index codec, WAL codec, merge rules with and
  without snapshots, clipping at SST, view, and subcompaction bounds, summary-field skip rules.
- Integration: `get` and `scan` across all layers; snapshots, checkpoints, clones, transactions;
  merge operands; reopen and WAL replay.
- Fault injection: crash between WAL write and flush; flush of a tombstone-only memtable.
- DST: add range deletes and compare with a `BTreeMap` model.
- Formal: extend the fizzbee specs if they cover compaction visibility.
- Performance: `get` and `scan` with 0, 1, 1k, and 1M tombstones.

## Rollout

1. Types, WAL, SST format, and read path behind a non-default feature flag. The flag also
   controls the manifest and SST version bumps.
2. Compaction support and whole-SST drop.
3. Public Rust API, then bindings and CLI.
4. Remove the flag when DST coverage and format gating are done.

Docs: update API docs and the SST format docs.

## Alternatives

- **Scan and delete per key (status quo).** Not atomic. O(n) cost.
- **Separate tombstone block, as in RocksDB.** The SST stores raw tombstone entries in their own
  block, and the reader fragments them at load time. A large tombstone set does not grow the
  index. But a read needs a second fetch and a second cache entry. Placing the block next to the
  index allows one read for both, but it needs a new loader, and the two cache entries can still
  be evicted apart. Few tombstones are expected, so the index is the better default.
- **Range row inline in key order.** The row sits at `start` and carries `end`. `get(k)` cannot
  find a tombstone that starts before `k`, which can sit in an earlier block or SST. Bloom
  filters do not help. A scan that starts inside the range never visits `start`. Repeating the
  row at each block start fixes `get`, but it bloats blocks and complicates compaction.
- **Segment drop (RFC 0024).** Cheap and atomic, but only along fixed boundaries. It
  complements range delete.
- **Drop files in range, as in RocksDB `DeleteFilesInRange`.** Handles only SSTs fully inside the
  range. It gives no logical delete for partial overlap or snapshots. Possible later
  optimization.
- **Prefix-only delete.** Smaller surface, but it cannot truncate by offset.
- **Application-level generation prefixes.** Works today for dropping a collection. Needs
  app-side cleanup. Does not handle arbitrary ranges.
- **Compaction filter with a seq cutoff.** A filter drops entries in the range at or below a seq.
  Deletion is lazy, and reads see the data until compaction reaches it. A filter also sees one
  row at a time, so it cannot apply rules that depend on other keys.
- **Read-side filtering.** The application hides data in the range at or below a seq. The data
  stays on disk, and every reader must apply the rule.

## Future Work

- **Inline small tombstone sets.** The summary fields are a union. A small set of tombstones in
  the manifest would avoid the index fetch when a filter rules out the key, but it would grow the
  manifest.
- **Spill large tombstone sets.** Many tombstones in one SST grow its index. Above a size limit,
  the writer could move the fragments to a separate block, or partition them. The index would
  then point to that block.
- **Sorted-run-level tombstones.** A wide tombstone repeats in every SST of a run. The manifest
  could hold it once per run instead.
- **Per-SST seq ranges.** `min_seq` and `max_seq` in `SsTableInfo` would let other features skip
  SSTs, such as snapshot reads and change following. Range deletes do not need them, because
  they use layer order. RFC 0024 chose list position over seq ranges for read precedence.
- **Tombstone-triggered compaction.** Compact SSTs that hold many tombstones, as RocksDB does
  with deletion-triggered compaction.

## Open Questions

- Should the version bumps wait for the first range tombstone? This RFC bumps the manifest and
  SST versions for every database, for simplicity. SST format v2 was also a blanket bump.
  Deferral keeps mixed versions and downgrade working for databases that never call
  `delete_range`. The writer would then need a sticky "range tombstones present" state, and the
  SST builder would set the version per SST.
- RFC 0004 says that writes outside a projected view's range fail. Today SlateDB accepts them,
  because the manifest stores no range for the whole database. A test on the current code
  confirms that a put outside a clone's projection succeeds. This RFC treats `delete_range` the
  same way. If the manifest stores a database range later, `delete_range` could clip to it.
  Clipping is exact, because keys outside the range do not exist. Should that change share the
  v3 manifest bump?

## References

- RocksDB wiki: [DeleteRange](https://github.com/facebook/rocksdb/wiki/DeleteRange)
- RocksDB blog: [DeleteRange: A New Native RocksDB Operation](https://rocksdb.org/blog/2018/11/21/delete-range.html)
- RocksDB source: `db/range_tombstone_fragmenter.h`, `db/range_del_aggregator.h`
- [RFC 0004: Checkpoints and Clones](0004-checkpoints.md)
- [RFC 0017: Compaction Filters](0017-compaction-filters.md)
- [RFC 0019: Change Data Capture](0019-change-data-capture.md)
- [RFC 0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC 0030: Pluggable WAL](0030-pluggable-wal.md)
- Pebble: [Range deletions](https://github.com/cockroachdb/pebble/blob/master/docs/range_deletions.md)
- Issue [#577: Support Delete Range operation](https://github.com/slatedb/slatedb/issues/577)
- Issue [#831: Tombstone-less deletes (SingleDelete)](https://github.com/slatedb/slatedb/issues/831)
- Issue [#779: Disallow manifest writes from mismatched versions](https://github.com/slatedb/slatedb/issues/779)

## Updates

- 2026-11-12: Initial draft.
- 2026-11-14: Store tombstone fragments in the SST index, not in a separate block.
