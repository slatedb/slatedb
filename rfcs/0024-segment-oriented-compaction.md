# SlateDB RFC: Segment-Oriented Compaction

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Target LSM Shape](#target-lsm-shape)
   - [LSM Segment Shaping Examples](#lsm-segment-shaping-examples)
      - [Example 1: Accumulated L0 state](#example-1-accumulated-l0-state)
      - [Example 2: L0 compaction for the oldest segment](#example-2-l0-compaction-for-the-oldest-segment)
      - [Example 3: New writes and backfill arrive together](#example-3-new-writes-and-backfill-arrive-together)
      - [Example 4: Compacting backfill within a segment](#example-4-compacting-backfill-within-a-segment)
      - [Example 5: Segment retention](#example-5-segment-retention)
- [Detailed Design](#detailed-design)
   - [Overview](#overview)
   - [Segment Extractor](#segment-extractor)
      - [Validation](#validation)
   - [WAL](#wal)
   - [Manifest](#manifest)
      - [Current Model](#current-model)
      - [Proposed Change: Per-Segment LSM State](#proposed-change-per-segment-lsm-state)
      - [Migration](#migration)
   - [Write Path](#write-path)
   - [Backpressure](#backpressure)
   - [Read Path](#read-path)
   - [Scans](#scans)
   - [Compaction](#compaction)
      - [Default Scheduler](#default-scheduler)
   - [Recovery and Progress Tracking](#recovery-and-progress-tracking)
- [Future Work](#future-work)
- [Alternatives](#alternatives)
   - [Time-Window Compaction Strategy (TWCS)](#time-window-compaction-strategy-twcs)
   - [Arbitrary-function segment mapper](#arbitrary-function-segment-mapper)
   - [Sequence-range bookkeeping and unordered sorted run bag](#sequence-range-bookkeeping-and-unordered-sorted-run-bag)
   - [Segment tag in WriteOptions instead of an extractor](#segment-tag-in-writeoptions-instead-of-an-extractor)
   - [One memtable per segment](#one-memtable-per-segment)
   - [Dynamic migration from unsegmented to segmented](#dynamic-migration-from-unsegmented-to-segmented)
   - [Global-sum L0 backpressure](#global-sum-l0-backpressure)
- [Appendix A: OpenData Timeseries Usage](#appendix-a-opendata-timeseries-usage)
   - [A.1 Ingestion Shape](#a1-ingestion-shape)
   - [A.2 Long-Term Reshaping](#a2-long-term-reshaping)
   - [A.3 Retention](#a3-retention)
- [Appendix B: OpenData Log Usage](#appendix-b-opendata-log-usage)
   - [B.1 Segment Model](#b1-segment-model)
   - [B.2 Compaction Needs](#b2-compaction-needs)
   - [B.3 Retention](#b3-retention)

<!-- TOC end -->

Status: Accepted

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

Append-only structures often organize data into ordered segments for metadata efficiency and retention. For example, a timeseries system may group new data and associated indexes into hourly buckets, while a log may group data by size- or time-based segments. As segments age out, they can be removed as an atomic unit along with their metadata.

This RFC proposes a segment-oriented compaction framework for SlateDB to align LSM compaction with that append-only structure. The central use case is append-ordered segments that map to contiguous key ranges (for example time buckets or log chunks). Segment membership is derived deterministically from keys via a prefix extractor, so segments partition the key space and can be compacted and retired independently. The proposal focuses on segment metadata propagation from writes through compaction, per-segment LSM state in the manifest, and explicit segment-level retention/deletion semantics.

## Motivation

SlateDB's current compaction primarily optimizes generic LSM shape. For append-heavy workloads with naturally ordered segments, that can cause avoidable rewrite cost:

- cold data is repeatedly rewritten with hotter data,
- compaction decisions are not aligned to segment lifecycle,
- and retention is often executed at row granularity even when whole segments are expired.

In append-oriented systems, writes concentrate on a single active segment at a time, which the application rolls over infrequently (for example, hourly for a timeseries, or at a size or time threshold for a log). Older segments are rarely updated except for occasional backfill. Segments are therefore a natural unit for lifecycle policy, including time-based and size-based retention.

Representative segment ranges include:

- a timeseries hour bucket (for example `ts/2026-03-09/14/*`),
- a daily bucket (`ts/2026-03-09/*`),
- or a log segment key range (`log/segment/042/*`).

This RFC aims to let compaction operate on those append-ordered boundaries directly, so older groups can be compacted and retired independently with less unnecessary rewrite work.

The design targets the common single-active-segment case. Concurrent writes to multiple segments are supported within this framework — e.g. to handle occasional data backfills— but the design is not specifically optimized for workloads that sustain writes to many concurrently-active segments.

## Goals

- Introduce a deterministic prefix-based segment extractor that derives segment membership from keys, propagated through L0 and sorted runs.
- Model each segment as an independent logical LSM tree in the manifest, sharing a single manifest structure alongside unsegmented data.
- Enable segment-aware compaction scheduling based on manifest-visible per-segment state.
- Support explicit segment-level deletion as part of compaction lifecycle and retention.
- Support automatic read-path pruning of segments based on query ranges.
- Keep the framework general enough to support both timeseries and log-style segmentation.

## Non-Goals

- Defining timestamp-native TWCS semantics (event-time watermarks, lateness contracts, clock policy).
- Supporting segment remapping/key-rewriting transforms (deferred to future work).
- Providing built-in segment-level retention or lifecycle policies (time-based retention, cold-segment auto-compaction, age-based rollups, etc.). The default scheduler handles structural compaction (L0→SR, SR→SR, empty-segment cleanup), but retention and hot/cold policy decisions are left to custom scheduler implementations.

## Target LSM Shape

This section describes the intuitive LSM shape this RFC is trying to enable.

A segment defines a scope for compaction and retention. Each named segment is an independent logical LSM tree with its own L0 list and its own ordered list of sorted runs. A deterministic prefix extractor derives the segment from each key at memtable flush time, producing one L0 SST per segment touched by that flush. Because segments own disjoint key intervals, each segment's chain is read independently and there is no ordering relationship between segments. The read path routes queries to the segments whose key intervals overlap the query range.

```text
prefix extractor derives segment prefixes from keys at flush time
                     |
                     v

L0: per-segment SSTs produced at memtable flush
  [hour=15] [hour=14] [hour=13] [hour=10 backfill]

            compact by segment (parallel-safe)
                     v

SRs (per-segment, list-ordered):
  hour=15: [SR1]    hour=14: [SR1]    hour=13: [SR1]    hour=10: [SR1]
                     |
                     | age / rollup compactions (future work)
                     v
SRs:  day=2026-03-10: [SR1]   day=2026-03-09: [SR1]
                     |
                     | retention policy
                     v
             expired segments dropped atomically
```

Each segment provides an isolated compaction scope and a natural boundary for parallelism. Cold segments can be compacted independently of hot ones. Retention operates at segment granularity, removing all L0 SSTs and sorted runs for a segment atomically.

### LSM Segment Shaping Examples

This section works through several examples to build an intuition about the rules that control segment-aware shaping. We will use the example of a time-series database in which each segment corresponds to a single hour "bucket" of time. Typically inbound metric samples would be grouped into the latest hour buckets, but backfills into older hours are also possible.

We represent the LSM state as a table where each row is a segment. The entries in each row form the merge chain for that segment, listed from newest to oldest (i.e. in manifest list order). Each entry is either an L0 SST or a sorted run, annotated with its sequence range `{seq=a..b}`. The sequence ranges are illustrative only — within a segment, list position (not seq range) determines read precedence, and the ranges are *not* stored in the manifest.

```text
segment    chain (newest → oldest)
hour=12    L0{seq=200..250}, SR{seq=100..150}
hour=11    SR{seq=100..150}
hour=10    L0{seq=300..350}, SR{seq=1..99}
```

Within a segment, list position determines read precedence — the first entry wins for overlapping keys. This is the same rule the existing single-tree read path applies to the current `compacted` list. Across segments no ordering is needed because the prefix extractor guarantees disjoint key spaces.

For a range scan, the reader identifies the segments whose key intervals overlap the scan range and spins up a per-segment merge iterator for each.

#### Example 1: Accumulated L0 state

After several rounds of writes and flushes, the LSM has accumulated L0 SSTs across three segments:

```text
segment    chain
hour=12    L0{seq=700..800}
hour=11    L0{seq=500..600}, L0{seq=400..500}, L0{seq=300..400}
hour=10    L0{seq=200..300}, L0{seq=100..200}, L0{seq=1..100}
```

Within each segment, the L0 SSTs have disjoint sequence ranges. Across segments, sequence ranges may interleave since concurrent writes to different segments share sequence number space — this is expected and has no correctness impact, since each segment's chain is merged independently.

#### Example 2: L0 compaction for the oldest segment

The compaction scheduler targets `hour=10`, the oldest segment. Its three L0s are merged into a single sorted run:

```text
segment    chain
hour=12    L0{seq=700..800}
hour=11    L0{seq=500..600}, L0{seq=400..500}, L0{seq=300..400}
hour=10    SR{seq=1..300}
```

The other segments remain in L0. Compactions are segment-scoped, so the scheduler can compact segments independently based on its own policy (e.g. size, age, or L0 count).

#### Example 3: New writes and backfill arrive together

A single memtable flush carries new data for `hour=12` (the current hour) alongside a backfill for `hour=10`. The flush produces two L0 SSTs — one per segment — sharing the same sequence range, and they become visible together in one atomic manifest update:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=500..600}, L0{seq=400..500}, L0{seq=300..400}
hour=10    L0{seq=801..900}, SR{seq=1..300}
```

Both new L0s are recorded in their respective segments' `l0` lists. The `hour=10` row now has a new L0 ahead of its existing SR — the backfill takes precedence over the older compacted data for any overlapping keys. The matching `seq=801..900` range on the two new L0s shows they come from a single flush; because they live in disjoint key spaces, no ordering between them is needed on the read path.

#### Example 4: Compacting backfill within a segment

The scheduler compacts `hour=10`, merging its L0 and existing SR into a new sorted run whose seq range is the union of the inputs:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=500..600}, L0{seq=400..500}, L0{seq=300..400}
hour=10    SR{seq=1..900}
```

The `hour=10` SR now covers the full seq range of the merged inputs.

#### Example 5: Segment retention

Retention expires `hour=10`. Its entire LSM state — all L0 SSTs, all sorted runs, and the segment entry itself — is dropped atomically from the manifest:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=500..600}, L0{seq=400..500}, L0{seq=300..400}
```

## Detailed Design

### Overview

This RFC introduces a deterministic prefix-based segment extractor that derives segment membership from keys. The extractor is configured at database open time and returns a key prefix that identifies the segment for each key. Because segments are prefix-based, each segment owns a contiguous key interval, and segments partition the key space into disjoint ranges. All segments share a common WAL and a single manifest, but each segment carries its own independent LSM state (L0 SSTs and sorted runs) within that manifest.

When no segment extractor is configured, all data is unsegmented and the system behaves identically to today.

### Segment Extractor

Segment membership is derived via a `PrefixExtractor` — the same trait introduced in [RFC 22](./0022-pluggable-filter.md) for prefix bloom filters. For segmentation, the trait is hoisted out of the `BloomFilterPolicy` scope into a neutral location (e.g. `slatedb::config`) so it can be used independently of the filter subsystem. The two uses are configured independently — in practice they generally should differ, since a prefix bloom filter indexed on the segment prefix itself matches every key in the segment and provides no pruning.

```rust
pub struct DbOptions {
    pub segment_extractor: Option<Arc<dyn PrefixExtractor>>,
    // ... existing fields
}
```

The trait's single extraction method takes a `FilterTarget` (either `Point(key)` for a stored or looked-up key, or `Prefix(p)` for a scan prefix) and returns `Option<usize>`. On the write and point-read paths, we call `prefix_len(FilterTarget::Point(key))`: if it returns `Some(n)`, the key belongs to the segment identified by `key[..n]`; if it returns `None`, the key is unsegmented and lands in the shared unsegmented LSM state (see [Manifest](#manifest)). The application defines the key encoding such that segment boundaries are represented in the key itself — for a timeseries database, the prefix encodes a time bucket; for a log, a segment identifier chosen by the writer when advancing to the next segment.

Two properties follow directly from the trait contract:

- **Determinism.** `prefix_len` is a pure function of the target. For `Point(k)`, the invariant that `prefix_len(Point(k)) = Some(n)` implies `prefix_len(Point(k')) = Some(n)` for every `k'` sharing the first `n` bytes with `k` means a given key always maps to the same segment across flushes, compactions, and restarts.
- **Disjoint, non-nesting segment intervals.** Because the extracted prefix is always a prefix of the key, each segment prefix `p` owns exactly the key interval `[p, p++)` (where `p++` is the lexicographic successor of `p`). The Point-variant invariant further prevents nested prefixes: a single extractor cannot simultaneously assign some `users:*` keys to a segment `users` and others to `users:foo`. If any key in `[p, p++)` maps to segment `p`, every key in that interval must. The extractor therefore picks one consistent prefix structure, and segment intervals sit side-by-side in the key space.

Together these give the active segments a natural total order by prefix, with pairwise disjoint key intervals. The [Scans](#scans) design leverages this to iterate segment-by-segment in prefix order without any key-wise merging across segments. The RFC does not constrain how unsegmented data relates to segment intervals; the unsegmented tree is treated as an independent iteration branch.

The extractor's `name()` is persisted in the manifest. On restart, if the configured extractor's name does not match the persisted one, the database refuses to open rather than silently routing data differently than before. The extractor must be configured at database creation time or never configured — introducing an extractor on an existing non-empty database, or removing a previously-configured extractor, causes the open to fail. See [Migration](#migration) for the correctness rationale.

#### Validation

The `name()` check is soft: a user can keep the name and change the logic, and new routing decisions would diverge silently. Three structural checks add defense against changes that slip past the name. None of them prove the extractor is unchanged, but each fails fast when a changed extractor would produce a manifest the rest of the system could not rely on.

- **Per-segment acknowledgment on open.** For each existing segment with prefix `p`, the writer checks `prefix_len(Prefix(p)) == Some(p.len())`. If the current extractor no longer treats `p` as a valid segment prefix — it returns `None`, or a different length — the database refuses to open. Catches changes that reshape extraction length or drop an existing prefix from the extractor's domain.

- **Antichain invariant on segment prefixes.** The manifest maintains the invariant that no segment prefix is a proper prefix of another. Any manifest update that would introduce a nested prefix is rejected. Catches changes that would produce overlapping segmentation — the structural violation that would otherwise force cross-segment key-wise merging on reads.

- **Route-consistency at write.** Each incoming write is evaluated by the extractor before it is appended to the WAL. If the resulting prefix would introduce a nested prefix (violating the antichain invariant against the current segment set), the write is rejected and the caller sees an error — no WAL append, no memtable insertion. Checking before the WAL is the only way to surface errors at the call site; a check deferred to memtable flush would fail after the write has already been durably committed and acknowledged.

- **Route-consistency at compaction.** When compaction reads input SSTs, each key is passed through the current extractor and checked against the target segment. If a key's extracted prefix does not match the target tree, the compaction fails and the manifest is not updated. This catches extractor drift that slipped past the write-time antichain check — for example, an extractor swap whose new behavior produces antichain-compatible prefixes for future writes but routes existing keys differently than they were originally assigned. The existing keys would survive the structural checks but misroute on read; compaction is the first point where the system has each key in hand to verify.

These do not cover every kind of extractor change — an extractor swap whose routing matches the existing assignment for every stored key will still slip through. The checks above narrow the surface to changes that cannot produce an inconsistent manifest undetected.

### WAL

The WAL requires no format changes to support segments. Because the extractor is deterministic and configured at database open time, segment membership can be recomputed from keys during WAL replay — there is no need to persist segment information in the WAL.

Replay runs the same antichain-invariant check that the write path applies (see [Validation](#validation)). This catches the case where an old WAL, written under a different extractor, contains writes that under the current extractor would nest with existing segment prefixes. If a replayed write would violate the invariant, the database refuses to complete replay rather than silently accepting an inconsistent state.

The extractor must be configured consistently across restarts. The persisted extractor `name()` in the manifest guards against accidental reconfiguration; see the [Segment Extractor](#segment-extractor) section for details.

### Manifest

The manifest is responsible for tracking the logical structure of the LSM tree: the set of L0 SSTs and compacted sorted runs that together represent the current state of the database. With segments, the manifest tracks multiple logical LSM trees — one per segment plus the shared unsegmented tree — within a single shared structure.

#### Current Model

Today, the manifest holds a single LSM state consisting of:

- `l0_last_compacted: Ulid` — the last L0 SST that has been compacted (gates L0 visibility).
- `l0: [CompactedSsTable]` — the set of L0 SSTs visible above `l0_last_compacted`.
- `compacted: [SortedRun]` — the set of sorted runs, ordered by descending `u32` ID (list position encodes read precedence).

This structure works for a single logical LSM tree. Segment-oriented compaction needs multiple independent trees that can evolve (flush, compact, retire) on their own schedule without coordinating ID assignment or ordering with each other.

#### Proposed Change: Per-Segment LSM State

This RFC preserves the existing manifest fields as the unsegmented LSM state and adds a new `segments` list. Each entry in `segments` carries its own independent LSM state, structurally identical to the existing top-level fields:

```flatbuffer
// Existing fields, now interpreted as the unsegmented LSM state ("None" segment):
l0_last_compacted: Ulid;
l0: [CompactedSsTable] (required);
compacted: [SortedRun] (required);

// New field — one entry per named segment:
segments: [Segment];

// New:
table Segment {
    prefix: [ubyte] (required);
    l0_last_compacted: Ulid;
    l0: [CompactedSsTable] (required);
    compacted: [SortedRun] (required);
}

// Extractor identity is persisted at the manifest root so the writer
// can detect accidental reconfiguration on startup.
segment_extractor_name: string;
```

Within each segment, `l0` and `compacted` follow today's semantics exactly: `l0` is the set of L0 SSTs above `l0_last_compacted`, and `compacted` is an ordered list of sorted runs where list position determines read precedence. Sorted run `u32` IDs remain globally unique across the database — a single shared counter allocates IDs monotonically regardless of which segment (or the unsegmented tree) a run belongs to. Within a segment, list position and ID order agree (newer runs have higher IDs), so list-position reads and ID-based debugging align.

This scoping has two consequences:

- **No cross-segment ordering.** Because the extractor guarantees disjoint key spaces, two sorted runs in different segments cannot contain overlapping keys and therefore need no ordering relationship. Each segment's read path is identical to today's single-tree read path.
- **Parallel compaction across segments is safe.** Two compactions in different segments each draw a distinct destination ID from the shared counter and apply their manifest updates to disjoint `Segment` entries (or to disjoint lists within the unsegmented tree vs. a segment), so their commits do not conflict. Parallel compaction *within* a single segment remains constrained by the existing `l0_last_compacted` watermark design and is out of scope for this RFC.

#### Migration

Because the change is purely additive, migration is a one-step schema bump to `ManifestV3`, which is `ManifestV2` plus the `segments` list and `segment_extractor_name` field.

The version is bumped lazily — only when segments are actually used. A database that never configures a segment extractor continues to write V2 manifests indefinitely; the `segments` list and `segment_extractor_name` field are absent. The first manifest write that persists segmented state (either a configured extractor or a non-empty `segments` list) emits V3. This keeps the version number meaningful: V3 marks the presence of segmented state, not merely a compile-time capability.

- A V3 manifest with an empty `segments` list is semantically identical to a V2 manifest — the existing `l0`/`compacted` fields carry the unsegmented state.
- No SST metadata rereads are required. Existing sorted runs and L0 SSTs keep their current IDs and representation in the top-level fields; V3 adds alongside rather than restructuring.
- The manifest decoder is updated to handle both V2 and V3. The standalone compactor reads whichever version the writer has published and does not independently upgrade.

Broader concerns about safe manifest evolution across mismatched process versions — writers, standalone compactors, readers, CLI tools — are tracked by [issue #779](https://github.com/slatedb/slatedb/issues/779). This RFC does not propose an inner solution to that problem; it assumes whatever mechanism lands from that work will apply to V3 manifests as it does to V2.

The extractor must be configured when the database is first created, or never configured. If a database has existing data and the open-time configuration disagrees with the manifest's persisted extractor state — configuring an extractor where none was before, or removing an extractor that was previously set — the database refuses to open. See [Alternatives](#alternatives) for additional discussion and potential room for future work.

### Write Path

The write path consults the extractor for each incoming write to validate the antichain invariant (see [Validation](#validation)) before appending to the WAL. If the check passes, the write proceeds through the WAL and memtable as today. At memtable flush, the extractor's output (either recomputed or carried alongside each entry) is used to group entries by target tree, producing one L0 SST per target: one per named segment that received entries from this flush, plus at most one for the unsegmented tree.

All L0 SSTs from a single flush are uploaded (in parallel) and then added to the manifest in a single atomic update. This update appends the new L0 SST to each affected tree's `l0` list and advances the corresponding `l0_last_compacted` cursors as appropriate. Partial visibility of a multi-segment flush is not supported — the manifest either reflects the entire flush or none of it.

Atomicity is needed for two reasons, depending on whether the WAL is enabled.

- **With WAL enabled**, it keeps the WAL replay cursor single-valued. The cursor advances past a contiguous sequence range once the corresponding writes are durably captured in L0. A partially visible multi-segment flush would force replay to track a per-segment flush frontier and selectively replay WAL entries per tree. Forcing atomicity at the manifest level keeps WAL replay logic unchanged.

- **With WAL disabled**, it is a correctness requirement for reader snapshots. The flush itself is the durability boundary, and atomic publication of the flush's L0 SSTs is what makes sequence-number advancement observable to readers as a single step. Without it, a reader could see some of a memtable's writes (those in segments already published) while missing others at the same sequence numbers (those in segments not yet published), breaking the invariant that if sequence `N` is visible, all sequences `≤ N` are too.

**Operational consideration.** In typical append-oriented workloads, writes concentrate on one or two segments (e.g. the current hour plus occasional backfill), so a flush produces a small number of L0 SSTs and parallel upload keeps latency comparable to today. A pathological wide backfill touching many segments in a single memtable does produce a correspondingly wide flush, and the memtable cannot be released until all uploads land and the manifest update succeeds. Near-term mitigation is to bound memtable width (size- or segment-count-based) so such flushes are frozen earlier; finer-grained WAL tracking to allow partial multi-segment flushes is deferred to [Future Work](#future-work).

### Backpressure

SlateDB has two backpressure mechanisms today: a memory-based one driven by `max_unflushed_bytes` (pauses writers when unflushed bytes exceed the threshold) and an L0-count-based one driven by `l0_max_ssts` (pauses memtable flushes when uncompacted L0 SSTs pile up faster than compaction can drain them). Segmentation interacts with each differently.

**Memory-based backpressure is unchanged.** Unflushed bytes are tracked at the WAL and memtable level, above the extractor. The threshold applies to the total in-memory footprint regardless of how it will later split into per-segment L0 SSTs. Writers pause when the total exceeds the configured limit, exactly as today.

**L0-count-based backpressure is applied per tree.** `l0_max_ssts` is compared against each individual tree's `l0` length — the unsegmented tree and every named segment — rather than against their sum. Backpressure fires when *any* tree exceeds the threshold. Each segment's backpressure therefore reflects only its own compaction lag; cold segments with small L0 counts do not delay flushes elsewhere.

Applying the threshold per tree does allow the *total* L0 count to grow with the number of segments. This is an accepted tradeoff: for point reads and short range scans the read path routes to a single tree, so read amplification is governed by per-tree L0 count regardless; long range scans that touch multiple trees see cost proportional to total L0 count, which is inherent to segmentation. See [Alternatives](#alternatives) for the rejected global-sum approach.

### Read Path

Because each named segment is its own LSM tree with its own ordered `compacted` list, read-path logic within a segment is identical to today's single-tree read path. The only new behavior is routing: for a given query, the reader uses the extractor to determine which trees need to be consulted. Routing is automatic and requires no changes to `ReadOptions` or the query APIs.

**Point lookups.** For `get(key)`, the reader applies the extractor:

- If `prefix_len(Point(key))` returns `Some(n)`, the reader consults only the segment identified by `key[..n]`. If no segment with that prefix exists, the key is not present.
- If `prefix_len(Point(key))` returns `None`, the reader consults only the unsegmented tree.

Point lookups never need to fan out across multiple trees because a given key belongs to exactly one tree by construction.

**Range scans.** See [Scans](#scans) below.

Within each consulted tree, the reader builds a merge iterator from that tree's `l0` and `compacted` lists using list-position precedence, exactly as today. For scans that touch multiple trees, per-tree streams are merged by key; the details are described in [Scans](#scans).

### Scans

A range scan `scan(lo..hi)` decomposes into two branches:

1. **Segment branch.** A chained iterator over the segments whose intervals `[prefix, prefix++)` overlap `[lo, hi)`, stepping from one segment to the next in prefix order. Within each segment the existing merge iterator runs against that segment's `l0` and `compacted` lists using list-position precedence. Because segment intervals are pairwise disjoint (see [Segment Extractor](#segment-extractor)), this chain produces a single key-ordered stream with no per-key merging across segments.
2. **Unsegmented branch.** An iterator over the unsegmented tree's `l0` and `compacted` lists.

The outer merge is a key-ordered merge of the two branches.

Pruning the unsegmented branch requires knowing whether any key in `[lo, hi)` could have `prefix_len(Point(key)) == None`. In the general case the reader cannot decide this without examining keys, so by default the unsegmented branch is consulted for every scan. The `Prefix` variant of `prefix_len` provides the optimization lever: the trait's contract says `prefix_len(Prefix(p)) = Some(n)` holds only if every extension `q` of `p` satisfies `prefix_len(Point(q)) = Some(n)` and `q[..n] == p[..n]`. That is exactly the guarantee needed to confine a scan to a single segment.

- **`scan_prefix(p)` that maps to one segment.** If `prefix_len(Prefix(p))` returns `Some(n)` with `n <= p.len()`, every key extending `p` belongs to the segment identified by `p[..n]`, so the scan collapses to that segment and the unsegmented branch can be skipped. If it returns `None`, the reader falls back to the general two-branch case.
- **Range scans confined to one segment.** For `scan(lo..hi)`, if `prefix_len(Prefix(lo))` and `prefix_len(Prefix(hi_exclusive_floor))` both return `Some(n)` with `lo[..n] == hi[..n]`, the range lies entirely within one segment — skip the unsegmented branch.

Databases that use the extractor consistently (i.e. all keys segmented) can configure the scan path to assume an empty unsegmented tree and skip that branch entirely. The default is conservative: include the unsegmented branch unless pruning is provably safe.

**Implications for existing APIs.** `scan`, `scan_prefix`, `scan_with_options`, and `scan_prefix_with_options` on `Db`, `DbReader`, `DbSnapshot`, and `DbTransaction` pick up segment-aware routing automatically — they all resolve to a range scan internally, and the two-branch logic lives beneath that. No API additions are required.

### Compaction

Segmentation extends the existing compaction model in two small ways:

1. The existing `TieredCompactionSpec` gains an optional `segment` field identifying the target tree. A spec whose `segment` is absent or empty targets the unsegmented tree (same as today); a spec whose `segment` is a named prefix targets that segment.
2. A new `DropSegmentSpec` variant is added to the top-level `CompactionSpec` union, expressing wholesale segment retention as a distinct operation from merge.

The schema changes are:

```flatbuffer
union CompactionSpec {
    TieredCompactionSpec,
    DropSegmentSpec,           // new
}

table TieredCompactionSpec {
    ssts: [Ulid];              // unchanged (deprecated)
    sorted_runs: [uint32];     // unchanged
    l0_view_ids: [Ulid];       // unchanged
    segment: [ubyte];          // new — absent/empty = unsegmented tree
}

table DropSegmentSpec {
    segment: [ubyte] (required);
}
```

On the Rust side, `CompactionSpec` gains an optional `segment` field alongside its existing `sources` and `destination`. A new `DropSegmentSpec` type is introduced:

```rust
pub struct CompactionSpec {
    /// `None` targets the unsegmented tree; `Some(prefix)` targets the
    /// segment with that prefix.
    segment: Option<Bytes>,
    sources: Vec<SourceId>,
    destination: u32,
}

pub struct DropSegmentSpec {
    segment: Bytes,
}
```

**Scope of each operation.**

- `CompactionSpec` (merge) draws all sources from the target tree and writes a single output sorted run with a globally-unique `u32` destination ID. Sources in a single compaction must be drawn from the same target tree — a spec that mixes trees is invalid and is rejected during validation. The scheduler obtains the destination ID from the shared counter via `ManifestCore::next_sorted_run_id()`:

```rust
impl ManifestCore {
    /// Generate the next sorted run ID. IDs are globally unique across
    /// all segments and the unsegmented tree, allocated monotonically
    /// for the lifetime of the database.
    pub fn next_sorted_run_id(&self) -> u32 { ... }
}
```

  The helper is a convenience — the scheduler could equivalently track the counter itself — but centralizing allocation in `ManifestCore` keeps the counter authoritative and avoids drift between planning and commit.

- `DropSegmentSpec` targets a named segment wholesale. The executor drops whatever L0 SSTs and sorted runs are currently in that segment at commit time and removes the segment entry from the manifest. There is no partial drop — retention is atomic over the whole segment. Only the latest manifest's references are removed; the garbage collector is responsible for cleaning up the underlying SST files once no remaining manifest versions (including those pinned by snapshots or checkpoints) reference them. The unsegmented tree cannot be dropped by this operation.

**Segment-aware scheduling.** To support scheduling, `ManifestCore` exposes the segment list directly:

```rust
impl ManifestCore {
    /// Returns the set of named segments and their LSM state.
    /// Unsegmented state is accessible through the existing `l0()`
    /// and `compacted()` accessors.
    pub fn segments(&self) -> &[Segment];
}
```

The scheduler uses this to plan work: selecting segments with many L0s, merging sorted runs within a segment, or identifying expired segments for retention. Segmented and unsegmented data may coexist in the same database — for example, a TSDB might segment time-bucketed metric data while keeping permanent configuration unsegmented. The scheduler handles both by planning per-segment compactions from `segments()` and unsegmented compactions from the top-level state.

#### Default Scheduler

SlateDB provides a default segment-aware compaction scheduler so that databases using segmentation have a working baseline without requiring the application to supply its own. The default applies the existing tiered compaction policy per tree:

- **L0 → SR compaction** within each tree, using the existing tiered-policy config knobs scoped per tree.
- **Intra-tree SR compaction** combining smaller sorted runs into larger ones, again using today's tiered rules scoped per tree.
- **Empty segment cleanup.** When a named segment's `l0` and `compacted` lists are both empty, the default drops the segment entry from the manifest.

**Alignment with backpressure.** The default scheduler's L0 compaction trigger is configured strictly below `l0_max_ssts` so that compaction fires before backpressure engages. A scheduler whose L0 trigger threshold is greater than or equal to `l0_max_ssts` would deadlock writes — backpressure engages before compaction can drain the segment. The existing alignment between the default tiered scheduler and `l0_max_ssts` carries over per tree; applications that replace the scheduler inherit responsibility for maintaining this invariant.

**Cross-segment prioritization.** When more than one tree is eligible for compaction simultaneously, the default prefers trees with larger L0 counts — the segment closest to backpressure gets attention first. This keeps the scheduler responsive to write pressure without relying on any segment metadata beyond what the manifest already exposes. Finer ordering (tie-breaks among equally-pressured trees, secondary criteria based on SR shape, etc.) is left to the implementation; applications that need different prioritization can replace the default.

**Retention is the application's responsibility.** The default does not implement a segment-level retention policy. If an application wants to drop a segment that still contains data — hour buckets outside a retention window, log segments past a consumption marker, etc. — it must supply its own scheduler (or extend the default). Retention policy depends on application semantics: time-based for a TSDB, consumption-based for a log, whatever the operator has decided for a given use case. Default policies are deferred to [Future Work](#future-work).

### Recovery and Progress Tracking

The resume mechanism is unchanged: the executor reads the last output SST to compute a resume cursor. Since each compaction produces a single sorted run, the existing single-destination progress model applies without modification. The compactor epoch and fencing protocol are unaffected.

## Future Work

This RFC focuses on segment-oriented planning and explicit drop semantics. Natural next steps include key-rewriting transforms and multi-stage timeseries rollups built on top of these primitives.

**Finer-grained WAL tracking for partial multi-segment flushes.** The current design requires a multi-segment flush to become visible atomically via the manifest, which keeps a single WAL replay cursor but can extend flush latency for wide backfills (see [Write Path](#write-path)). A future iteration could track per-segment flush frontiers in the WAL or in the manifest, allowing partial flushes to become visible incrementally. This involves extending WAL replay to advance the frontier per segment and reconciling checkpoint semantics with per-segment progress; deferred until operational experience shows the wide-flush case is a real bottleneck.

**Parallel L0 compaction across segments.** Parallel compaction of disjoint *sorted-run* compactions already works today, because the `l0_last_compacted` watermark is unaffected when no L0 SSTs are involved. What segmentation unlocks is parallel *L0-sourced* compaction across segments: each segment has its own L0 list and its own `l0_last_compacted` watermark, so an L0-draining compaction in segment A can complete out of order relative to one in segment B without the watermark truncation issue that blocks parallel L0 compactions in a single-tree layout. The execution model in this RFC can be extended to exploit this by scheduling L0 compactions in disjoint segments concurrently. Parallel L0 compaction *within* a single segment is a separate concern tied to the watermark's single-cursor design and is not addressed here.

**Default segment retention policies.** The default scheduler (see [Default Scheduler](#default-scheduler)) does not implement segment-level retention. Natural extensions include a segment TTL that drops segments whose last-write timestamp exceeds a configured duration (requires tracking a `last_write_time` per segment in the manifest, updated on flush), or policy hooks that let applications plug retention decisions into the default without replacing it wholesale. The semantics questions — wall-clock vs. logical time, treatment of late backfills that reset the clock, interaction with checkpoints — deserve their own design pass before baking a particular policy into the default.

**Multi-stage timeseries rollups.** Daily timeseries compactions can build on hourly segmentation:

- First, compaction shapes data into hourly segment-aligned sorted runs.
- Then, daily compactions select exactly the 24 hourly segments for a target day as inputs.

This staged model gives a clean source-selection boundary for day rollups and avoids mixing unrelated segment data, reducing unnecessary write amplification. Implementing it requires key-rewriting transforms (to relabel hourly keys into daily keys), which are out of scope for this RFC.

An alternative to doing rollups as an internal compaction pass is to expose lower-level primitives — a bulk-write-SR API and an import-SR-as-segment API — and let applications perform the rollup computation externally, handing the result back as a new segment. This is appealing for TSDB-style rollups where the per-row consolidation logic (dictionaries, indexes, etc.) is highly application-specific, and keeps the rewrite-during-compaction machinery out of SlateDB. The in-SlateDB vs. out-of-SlateDB choice deserves its own design discussion once we have a concrete rollup use case to evaluate against.

A concrete edge case for the rewriting design: once `hour=00..23` have been rolled up into `day=1` and the hourly segments dropped, a late backfill write for `hour=04` would re-create the `hour=04` segment rather than landing in `day=1`. Two approaches look viable and both remain open:

- **Writer-side routing.** The writer detects backfill-age writes and rewrites their keys on ingest so they target the current rolled-up segment (e.g. an `hour=04` backfill becomes a `day=1` write at write time). Reads stay simple — every key lives in exactly one segment at any moment — but the write path has to carry rollup state and know the mapping rules.

- **Compactor-side routing.** The writer stays oblivious. A late `hour=04` backfill lands in a re-created `hour=04` segment, and a subsequent compactor pass detects the orphan and merges it into `day=1`. This fits naturally with the compactor's existing rollup work and keeps the write path uninvolved. The tradeoff is on the read side: until the compactor catches up, data for `hour=04` may live in both the re-created `hour=04` segment and `day=1`, so the query planner has to know about the "rolled up into" relationship and consult both. The merge iterator machinery already handles multi-segment reads, so this is mechanically tractable; the complexity is in tracking the relationship.

The choice hinges on how much rollup state we are willing to push into the write path versus the read path.

## Alternatives

### Time-Window Compaction Strategy (TWCS)

A more traditional approach would implement TWCS directly: the system defines time windows (e.g. 1 hour), routes data by event or ingestion time, and manages window lifecycle automatically — closing windows after a threshold, never merging across windows, and dropping expired windows based on a configured retention period. This provides good out-of-the-box behavior for timeseries workloads with minimal application involvement.

This RFC opts for a more general approach: opaque segment identifiers defined by the application's key encoding rather than time windows defined by the system. This is more flexible — segments can represent time buckets, log partitions, or any other application-defined scope — but requires a custom compaction scheduler and shifts more responsibility to the application. The generality also paves the way for key-rewriting transforms (e.g. rolling hourly buckets into daily ones), which would be difficult to retrofit into a TWCS model where window identity is system-managed.

### Arbitrary-function segment mapper

An alternative to the deterministic prefix extractor is an arbitrary `Fn(&[u8]) -> Option<Bytes>` mapper. This would give the application free rein to derive a segment tag from a key in any way it chose — including tags that are not prefixes of the key.

The prefix-extractor approach was chosen instead because:

- **Scan pruning falls out automatically.** With an arbitrary function, the reader cannot map a scan range to a segment set without enumerating keys. A prefix extractor means each segment owns a contiguous key interval `[tag, tag++)`, so scan pruning is a straightforward range intersection against the manifest's `segments` list.
- **Determinism is structurally enforced.** `PrefixExtractor`'s `prefix_len`/`name()` contract — in particular the `Prefix`-variant invariant — gives the read path a way to validate scan prefixes and the manifest a way to detect accidental reconfiguration. An arbitrary function has no such hooks.
- **Trait reuse with RFC 22.** The same extractor can power prefix bloom filters and segment routing, with no duplication of concepts or versioning logic.

The arbitrary-function mapper remains viable for use cases that genuinely need segment identity to depend on non-prefix key structure, but none of the target use cases (timeseries time buckets, log segment IDs) require it.

### Sequence-range bookkeeping and unordered sorted run bag

An alternative manifest layout would annotate every L0 SST and sorted run with explicit `min_seq`/`max_seq` ranges, identify sorted runs by `Ulid` rather than the existing `u32` counter, and treat the sorted run list as an unordered bag — determining read precedence from sequence ranges rather than list position. The attraction is that parallel segment compactions could produce sorted runs with uncoordinated identity (Ulids) and still be correctly ordered on reads via sequence ranges.

The tree-per-segment layout in this RFC does not need any of that:

- Each segment has its own ordered `compacted` list, so list-position ordering within a segment is sufficient to establish precedence — sequence ranges would be redundant bookkeeping.
- Parallel compactions in different segments commit to disjoint `Segment` entries with distinct destination IDs drawn from the shared `u32` counter, so Ulid-based identity is not needed to avoid collision.
- Across segments, disjoint key spaces eliminate any need for cross-tree ordering.

Avoiding sequence-range bookkeeping also keeps migration trivial: it does not require reading SST index metadata to backfill seq ranges for existing runs, and the manifest does not grow to carry per-SST seq data.

### Segment tag in WriteOptions instead of an extractor

An alternative to the deterministic extractor is to specify the segment tag directly in `WriteOptions` as `segment: Option<Bytes>`. This gives the caller full control over segment assignment per write batch, without requiring the segment to be derivable from the key.

This approach is more flexible — the caller can assign segments based on external context, not just key structure. However, it introduces significant complexity:

- **WAL format changes required.** Because the segment tag is transient (it exists only in `WriteOptions`), the WAL must persist it so that WAL replay can reconstruct segment groupings. This requires extending the WAL block format with a segment field and splitting blocks on segment transitions.
- **Cross-segment tombstone issues.** Without a deterministic mapping, the same key can appear in multiple segments. This makes segment retention unsafe: dropping a segment can resurface a shadowed key in another segment. Tombstone elision also becomes conservative — tombstones must be retained until the segment is dropped, since a key may exist in another segment with a lower sequence number.
- **Batch atomicity constraints.** Since `WriteOptions` is per-batch, all entries in a batch must belong to the same segment. Cross-segment batches are not supported.
- **Loss of scan pruning.** Because segment membership is not derivable from the key, the reader cannot prune segments based on a scan range — every scan must consider every segment.

This RFC uses the deterministic extractor approach because it avoids WAL changes, provides unconditionally safe retention, simplifies tombstone handling, and enables automatic scan pruning. The `WriteOptions` approach could be revisited if use cases emerge that require segment assignment independent of key structure.

### One memtable per segment

An alternative to the single shared memtable is to maintain one memtable per segment, so flush output is already segment-aligned and no flush-time grouping is needed. In principle, this would enable independent segment flushing to L0. This would mean that a slow flush for segment A could complete without waiting for segment B, so A's data becomes visible sooner. This is possible, but it means tracking a per-segment cursor within the WAL so that replay does not introduce duplicates. Since our initial effort is aimed at uses cases which primarily write to a single active segment (with occasional backfills), the benefit may be marginal. This can be revisited in [Future Work](#future-work).

### Dynamic migration from unsegmented to segmented

An earlier version of this design allowed an extractor to be introduced on an existing non-empty database. New writes would route through the extractor, while pre-existing keys remained in the unsegmented tree. This is rejected in favor of requiring the extractor to be fixed at database creation time (see [Migration](#migration)).

The core problem with dynamic migration is that pre-existing data is no longer addressable through the new routing:

- **Reads miss shadowed data.** A key written before the extractor existed lives in the unsegmented tree. After the extractor is configured, `get`/`scan` route the query to a segment — which does not contain the key — producing false-negative reads.
- **Tombstones never reach their targets.** A delete or tombstone written after the change lands in a segment while the key it targets remains in the unsegmented tree. Compaction operates per-tree, so the tombstone and the original key never meet. The delete effectively does nothing, and retention cannot expire the key.

These could in principle be addressed by routing queries and deletes to *both* the unsegmented tree and the matching segment tree, and merging results — effectively treating the unsegmented tree as an always-consulted fallback for any segmented query. That restores correctness at the cost of consulting two trees on every routed operation and retaining precedence logic across them. We have chosen to defer this until there is a clear need which justifies the additional complexity. 

### Global-sum L0 backpressure

An alternative to the per-tree backpressure policy (see [Backpressure](#backpressure)) is to compare `l0_max_ssts` against the sum of L0 entries across every tree — the unsegmented tree plus every named segment. This preserves a single global contract for operators and bounds the total L0 count in the database.

The per-tree approach was chosen instead because a global sum couples unrelated segments together. Each retired segment typically retains a small tail of L0s until compaction drains it, and with many retired segments those tails add up. Writers to the current active segment would stall on backpressure triggered by stale L0s in cold segments that aren't falling behind in any meaningful sense. The resulting behavior isn't a true deadlock — compaction can still drain cold segments to bring the total below the threshold — but it produces persistent spurious stalls that couple the active segment's write latency to unrelated compaction state. The per-tree check removes that coupling.

## Appendix A: OpenData Timeseries Usage

This appendix summarizes the OpenData timeseries shape relevant to this RFC. See the [OpenData timeseries storage RFC](https://github.com/opendata-oss/opendata/blob/main/timeseries/rfcs/0001-tsdb-storage.md) for full design details.

### A.1 Ingestion Shape

- Data is ingested in hour windows.
- Keys are encoded so each hour window occupies a contiguous key range.

Simplified conceptual key sketches (the actual OpenData encoding is binary with `version`, `record_tag`, and typed fields):

```text
Raw samples (TimeSeries):          <time_bucket>/<series_id>
Index example (InvertedIndex):    <time_bucket>/<label>/<value>
```

In the OpenData design, both raw and index records are bucket-scoped (for example, `TimeSeries`, `ForwardIndex`, `InvertedIndex`, and `SeriesDictionary` all include `time_bucket` in the key), so metadata ages out together with the corresponding bucket.

### A.2 Long-Term Reshaping

- As data ages, storage should move from hour buckets to day/week buckets.
- This RFC addresses the segment-selection and retention mechanics needed to support those rollups.
- Key-rewriting transforms for changing bucket key encodings are deferred to future work.

### A.3 Retention

- Retention windows eventually expire older buckets.
- For efficiency, the system should drop whole segment key ranges when possible.

## Appendix B: OpenData Log Usage

This appendix summarizes the OpenData log shape relevant to this RFC. See the [OpenData log storage RFC](https://github.com/opendata-oss/opendata/blob/main/log/rfcs/0001-storage.md) and [OpenData logical segmentation RFC](https://github.com/opendata-oss/opendata/blob/main/log/rfcs/0002-logical-segmentation.md) for full design details.

### B.1 Segment Model

- Data is organized into ordered log segments.
- Segment boundaries are encoded in the key via a `segment_id` prefix.
- Segments are therefore represented as contiguous key ranges and can be derived by a `PrefixExtractor` that extracts the `segment_id` portion of the key.

The writer decides when to advance `segment_id` — for example, when the current segment reaches a size threshold or a time boundary. That policy lives in the writer's key-encoding layer, not in SlateDB. From SlateDB's perspective the segment is a key prefix, which is what the extractor sees.

Simplified conceptual key sketches (actual OpenData encoding is binary with typed prefixes/fields):

```text
Log entries (segmented):      <segment_id>/<key>/<relative_seq>
Segment metadata record:      <segment_id>
```

In the OpenData segmented log model, entries and segment metadata are both scoped by `segment_id`. When an old segment is dropped for retention, its associated metadata record ages out with it.

### B.2 Compaction Needs

- Compaction should group work by segment boundaries instead of freely mixing unrelated ranges.
- This allows hot and cold segments to evolve at different rates.

### B.3 Retention

- Retention commonly removes older log segments.
- Segment-range dropping is preferable to row-by-row expiration when an entire segment is outside retention.
