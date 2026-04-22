# SlateDB RFC: Segment-Oriented Compaction

Status: Draft

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
- Supporting segment remapping/key-rewriting transforms (deferred to future work)

## Target LSM Shape

This section describes the intuitive LSM shape this RFC is trying to enable.

A segment is a scope for compaction and retention. Each named segment is an independent logical LSM tree with its own L0 list and its own ordered list of sorted runs. A deterministic prefix extractor derives the segment from each key at memtable flush time, producing one L0 SST per segment touched by that flush. Because segments own disjoint key intervals, each segment's chain is read independently and there is no ordering relationship between segments. The read path routes queries to the segments whose key intervals overlap the query range.

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

Each segment provides an isolated compaction scope and a natural boundary for parallelism. Cold segments can be compacted independently from hot ones. Retention operates at segment granularity, removing all L0 SSTs and sorted runs for a segment atomically.

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

The `hour=10` SR now covers the full seq range of the merged inputs. The range `1..900` overlaps what other segments are holding, but that's fine — disjoint key spaces make cross-segment ordering unnecessary.

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

Segment membership is derived via a `PrefixExtractor` — the same trait introduced in [RFC 22](./0022-pluggable-filter.md) for prefix bloom filters. For segmentation, the trait is hoisted out of the `BloomFilterPolicy` scope into a neutral location (e.g. `slatedb::config`) so it can be used independently of the filter subsystem. Users may configure the same extractor for both purposes, or configure them independently.

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

The extractor's `name()` is persisted in the manifest. On restart, if the configured extractor's name does not match the persisted one, the database refuses to open rather than silently routing data differently than before. Reconfiguring the extractor is therefore treated as a one-way, offline operation — in general, changing segmentation on existing data requires a rewrite, which is out of scope for this RFC.

### WAL

The WAL requires no format changes to support segments. Because the extractor is deterministic and configured at database open time, segment membership can be recomputed from keys during WAL replay. The extractor is applied at memtable flush time to group entries by segment — there is no need to persist segment information in the WAL.

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

Because the change is purely additive, migration is a one-step schema bump. The manifest is revved to `ManifestV3`, which is `ManifestV2` plus the `segments` list and `segment_extractor_name` field.

- A V3 manifest with an empty `segments` list is semantically identical to a V2 manifest — the existing `l0`/`compacted` fields carry the unsegmented state.
- The writer upgrades lazily: the first manifest write under a V3-capable binary emits V3. No SST metadata rereads are required — existing sorted runs and L0 SSTs keep their current IDs and representation in the top-level fields.
- Databases that never configure a segment extractor continue to operate entirely through the top-level fields, with `segments` always empty.

The manifest decoder is updated to handle both V2 and V3. The standalone compactor reads whichever version the writer has published and does not independently upgrade. Because no field changes require backfill, there is no decoder-first/encoder-later rollout phase.

When an extractor is configured for the first time on an existing database, the existing unsegmented data remains in the top-level fields. New writes are routed through the extractor: a key whose `prefix_len(Point(key))` returns `Some(n)` flows into the segment identified by `key[..n]`, and a key whose `prefix_len` returns `None` continues to flow into the top-level fields. Existing unsegmented data is not automatically migrated into segments — if the application wants old data segmented, it must rewrite it (out of scope for this RFC).

### Write Path

The write path is unchanged up to memtable flush. Writes go through the WAL and memtable as today — the extractor is not involved until flush time. When a memtable is frozen and flushed, the extractor is applied to each entry's key to determine its target tree. Entries are grouped accordingly and one L0 SST is produced per target: one per named segment that received entries from this flush, plus at most one for the unsegmented tree.

All L0 SSTs from a single flush are uploaded (in parallel) and then added to the manifest in a single atomic update. This update appends the new L0 SST to each affected tree's `l0` list and advances the corresponding `l0_last_compacted` cursors as appropriate. Partial visibility of a multi-segment flush is not supported — the manifest either reflects the entire flush or none of it.

The reason is the **flush cursor invariant**, not durability. The WAL replay cursor advances past a single contiguous sequence range once the corresponding writes have been durably captured in L0. If a multi-segment flush could be partially visible, replay would need to track a per-segment flush frontier and replay WAL entries selectively per target tree. Forcing atomicity at the manifest level keeps the cursor single-valued and keeps WAL replay logic unchanged. This holds regardless of whether the WAL is enabled — the simplification is in the state machine, not the durability guarantee.

**Operational consideration.** In typical append-oriented workloads, writes concentrate on one or two segments (e.g. the current hour plus occasional backfill), so a flush produces a small number of L0 SSTs and parallel upload keeps latency comparable to today. A pathological wide backfill touching many segments in a single memtable does produce a correspondingly wide flush, and the memtable cannot be released until all uploads land and the manifest update succeeds. Near-term mitigation is to bound memtable width (size- or segment-count-based) so such flushes are frozen earlier; finer-grained WAL tracking to allow partial multi-segment flushes is deferred to [Future Work](#future-work).

### Backpressure

SlateDB has two backpressure mechanisms today: a memory-based one driven by `max_unflushed_bytes` (pauses writers when unflushed bytes exceed the threshold) and an L0-count-based one driven by `l0_max_ssts` (pauses memtable flushes when uncompacted L0 SSTs pile up faster than compaction can drain them). Segmentation interacts with each differently.

**Memory-based backpressure is unchanged.** Unflushed bytes are tracked at the WAL and memtable level, above the extractor. The threshold applies to the total in-memory footprint regardless of how it will later split into per-segment L0 SSTs. Writers pause when the total exceeds the configured limit, exactly as today.

**L0-count-based backpressure uses a global count across all trees.** `l0_max_ssts` is compared against the sum of L0 entries in the unsegmented tree and across every named segment's `l0` list. This preserves today's operator-facing contract — "if the database has more than N uncompacted L0s, slow down flushes" — and is robust to pathological cases where a workload spreads writes thinly across many segments. The tradeoff is that a wide multi-segment flush contributes multiple entries at once (one per touched segment), so the threshold is reached sooner than in a single-tree configuration with the same data volume. This is aligned with the single-active-segment workload assumption in [Motivation](#motivation): a typical flush touches one or two segments, so global counting matches per-segment counting in the common case. Multi-active-segment workloads are where per-segment policies would begin to pay off — see [Future Work](#future-work) for segment-aware backpressure.

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

Each compaction reads and writes within a single logical LSM tree — either a named segment or the unsegmented tree. Because sorted run IDs are globally unique across the database, compaction spec construction and execution follow today's single-tree logic with the target tree identified by an explicit segment prefix on the spec.

A compaction spec identifies the target tree via an optional segment prefix, lists source SSTs and sorted runs from that tree, and specifies an action:

```rust
pub enum CompactionAction {
    /// Merge sources into a new sorted run with the given (globally
    /// unique) u32 ID, appended to the target tree's `compacted` list.
    Merge { destination: u32 },
    /// Drop sources without producing output (segment retention).
    Drop,
}

pub struct CompactionSpec {
    /// `None` targets the unsegmented tree; `Some(prefix)` targets the
    /// segment with that prefix.
    segment: Option<Bytes>,
    l0_view_ids: Vec<Ulid>,
    sorted_runs: Vec<u32>,
    action: CompactionAction,
}
```

All sources in a single compaction must be drawn from the target tree (either all from the unsegmented state or all from the same named segment). A `CompactionSpec` that mixes trees is invalid and is rejected during validation.

For `Merge`, the executor merges inputs and produces one output sorted run appended to the target tree's `compacted` list with the specified `u32` ID. The scheduler obtains the destination ID from the manifest's shared counter via a convenience helper:

```rust
impl ManifestCore {
    /// Generate the next sorted run ID. IDs are globally unique across
    /// all segments and the unsegmented tree, allocated monotonically
    /// for the lifetime of the database.
    pub fn next_sorted_run_id(&self) -> u32 { ... }
}
```

The helper is a convenience — the scheduler could equivalently track the counter itself — but centralizing allocation in `ManifestCore` keeps the counter authoritative and avoids drift between planning and commit.

For `Drop`, the sources are removed from the target tree with no output. Dropping every L0 SST and sorted run in a named segment (combined with removing the segment entry itself) is how segment retention is expressed. `Drop` only removes references from the latest manifest; the underlying SST files are not deleted immediately. The garbage collector is responsible for cleaning up SST files once no remaining manifest versions (including those pinned by snapshots or checkpoints) reference them.

To support segment-aware scheduling, `ManifestCore` exposes the segment list directly:

```rust
impl ManifestCore {
    /// Returns the set of named segments and their LSM state.
    /// Unsegmented state is accessible through the existing `l0()`
    /// and `compacted()` accessors.
    pub fn segments(&self) -> &[Segment];
}
```

The scheduler uses this to plan work: selecting segments with many L0s, merging sorted runs within a segment, or identifying expired segments for retention. Segmented and unsegmented data may coexist in the same database — for example, a TSDB might segment time-bucketed metric data while keeping permanent configuration unsegmented. The scheduler handles both by planning per-segment compactions from `segments()` and unsegmented compactions from the top-level state.

The `.compactions` file schema is updated to carry the segment prefix alongside the existing fields:

```flatbuffer
enum CompactionActionType : byte {
    Merge = 0,
    Drop,
}

table CompactionSpecV2 {
    segment: [ubyte];         // absent/empty = unsegmented tree
    l0_view_ids: [Ulid];
    sorted_runs: [uint];      // globally unique SR IDs, all from the target tree
    destination: uint;        // globally unique SR ID; only meaningful for Merge
    action: CompactionActionType;
}
```

Compared to the existing `TieredCompactionSpec`, the V2 schema adds the `segment` prefix and replaces the single-action shape with an explicit `action` enum to express `Drop` alongside `Merge`. The ID fields remain `uint` (globally unique), matching today's `u32` SR IDs.

### Recovery and Progress Tracking

The resume mechanism is unchanged: the executor reads the last output SST to compute a resume cursor. Since each compaction produces a single sorted run, the existing single-destination progress model applies without modification. The compactor epoch and fencing protocol are unaffected.

## Future Work

This RFC focuses on segment-oriented planning and explicit drop semantics. Natural next steps include key-rewriting transforms and multi-stage timeseries rollups built on top of these primitives.

**Finer-grained WAL tracking for partial multi-segment flushes.** The current design requires a multi-segment flush to become visible atomically via the manifest, which keeps a single WAL replay cursor but can extend flush latency for wide backfills (see [Write Path](#write-path)). A future iteration could track per-segment flush frontiers in the WAL or in the manifest, allowing partial flushes to become visible incrementally. This involves extending WAL replay to advance the frontier per segment and reconciling checkpoint semantics with per-segment progress; deferred until operational experience shows the wide-flush case is a real bottleneck.

**Segment-aware L0 backpressure for multi-active-segment workloads.** The first iteration applies `l0_max_ssts` as a global count across all trees (see [Backpressure](#backpressure)), which matches the single-active-segment assumption in [Motivation](#motivation). Workloads that sustain writes to multiple active segments in parallel would benefit from per-segment accounting — for example, allowing cold segments to accumulate more L0s without blocking flushes to hot segments, or exposing per-segment thresholds. Such policies would require the memtable flusher to consult per-segment L0 counts when deciding whether to stall, and the scheduler to prioritize compactions for segments near their threshold.

**Parallel execution of independent segment compactions.** The execution model in this RFC can be extended to run compactions for disjoint segments concurrently. Each segment has its own L0 list, its own `l0_last_compacted` watermark, and its own `compacted` list in the manifest, so parallel compactions in different segments commit to disjoint `Segment` entries with distinct destination IDs drawn from the shared counter — no cross-segment ordering conflict arises. Parallel compaction *within* a single segment is a separate concern tied to the `l0_last_compacted` watermark's single-cursor design and is not addressed here.

**Multi-stage timeseries rollups.** Daily timeseries compactions can build on hourly segmentation:

- First, compaction shapes data into hourly segment-aligned sorted runs.
- Then, daily compactions select exactly the 24 hourly segments for a target day as inputs.

This staged model gives a clean source-selection boundary for day rollups and avoids mixing unrelated segment data, reducing unnecessary write amplification. Implementing it requires key-rewriting transforms (to relabel hourly keys into daily keys), which are out of scope for this RFC.

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

### Unifying L0 and sorted runs

The structural distinction between L0 SSTs and sorted runs is independent of segmentation. An L0 SST is semantically equivalent to a sorted run containing a single SST, and unifying the two into a single abstraction would simplify the manifest schema. This RFC preserves the existing L0/SR distinction to limit scope — unification is a natural follow-on that would apply equally to segmented and unsegmented LSM state.

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

### Default segment-aware scheduler

This RFC requires a custom compaction scheduler when segments are in use. A natural question is whether SlateDB should provide a default segment-aware scheduler. Efficient retention enforcement is a primary motivation for this feature, but retention policy depends on application-level durability guarantees — for example, time-based retention for a TSDB differs from consumption-based retention for a log. A default scheduler may be introduced in the future once common patterns emerge, but initially the application is responsible for providing a scheduler that understands its own segment and retention semantics.

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
