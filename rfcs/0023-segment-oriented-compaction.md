# SlateDB RFC: Segment-Oriented Compaction

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

Append-only structures often organize data into ordered segments for metadata efficiency and retention. For example, a timeseries system may group new data and associated indexes into hourly buckets, while a log may group data by size- or time-based segments. As segments age out, they can be removed as an atomic unit along with their metadata.

This RFC proposes a segment-oriented compaction framework for SlateDB to align LSM compaction with that append-only structure. The central use case is append-ordered segments that map to contiguous key ranges (for example time buckets or log chunks). The proposal focuses on segment metadata propagation from writes through compaction, unordered sorted run management, and explicit segment-level retention/deletion semantics.

## Motivation

SlateDB's current compaction primarily optimizes generic LSM shape. For append-heavy workloads with naturally ordered segments, that can cause avoidable rewrite cost:

- cold data is repeatedly rewritten with hotter data,
- compaction decisions are not aligned to segment lifecycle,
- and retention is often executed at row granularity even when whole segments are expired.

In append-oriented systems, writes are concentrated in the newest segment(s), while older segments are rarely updated. Segments are therefore a natural unit for lifecycle policy, including time-based and size-based retention.

Representative segment ranges include:

- a timeseries hour bucket (for example `ts/2026-03-09/14/*`),
- a daily bucket (`ts/2026-03-09/*`),
- or a log segment key range (`log/segment/042/*`).

This RFC aims to let compaction operate on those append-ordered boundaries directly, so older groups can be compacted and retired independently with less unnecessary rewrite work.

## Goals

- Introduce a deterministic segment mapper (`key -> Option<Bytes>`) that derives segment metadata from keys, propagated through L0 and sorted runs.
- Enable segment-aware compaction scheduling based on manifest-visible segment annotations.
- Support explicit segment-level deletion as part of compaction lifecycle and retention.
- Replace ordered sorted run lists with unordered (bag) semantics, using sequence number ranges for read-path correctness.
- Support segment-scoped queries to prune the merge tree at read time.
- Keep the framework general enough to support both timeseries and log-style segmentation.

## Non-Goals

- Defining timestamp-native TWCS semantics (event-time watermarks, lateness contracts, clock policy).
- Supporting segment remapping/key-rewriting transforms (deferred to future work)

## Target LSM Shape

This section describes the intuitive LSM shape this RFC is trying to enable.

A segment is a scope for compaction and retention. Each L0 SST and sorted run belongs to exactly one segment. A deterministic segment mapper derives the segment from each key at memtable flush time, producing one L0 SST per segment. Sorted runs are managed as an unordered bag — there is no global ordering between them. The read path resolves precedence using key ranges and sequence numbers. It does not use segments for ordering, but can filter on them to prune irrelevant data before building the merge iterator.

```text
segment mapper derives segments from keys at flush time
                     |
                     v

L0: per-segment SSTs produced at memtable flush
  [hour=15] [hour=14] [hour=13] [hour=10 backfill]

            compact by segment (parallel-safe)
                     v

SRs (bag):  {hour=15}  {hour=14}  {hour=13}  {hour=10}
                     |
                     | age / rollup compactions (future work)
                     v
SRs (bag):  {day=2026-03-10}  {day=2026-03-09}
                     |
                     | retention policy
                     v
             expired segments dropped atomically
```

Each segment provides an isolated compaction scope and a natural boundary for parallelism. Cold segments can be compacted independently from hot ones. Retention operates at segment granularity, removing all L0 SSTs and sorted runs for a segment atomically.

### LSM Segment Shaping Examples

This section works through several examples to build an intuition about the rules that control segment-aware shaping. We will use the example of a time-series database in which each segment corresponds to a single hour "bucket" of time. Typically inbound metric samples would be grouped into the latest hour buckets, but backfills into older hours are also possible.

We represent the LSM state as a table where each row is a segment. The entries in each row form the merge chain for that segment, listed from newest to oldest. Each entry is either an L0 SST or a sorted run, annotated with its sequence range:

```text
segment    chain (newest → oldest)
hour=12    L0{seq=200..250}, SR{seq=100..150}
hour=11    SR{seq=100..150}
hour=10    L0{seq=300..350}, SR{seq=1..99}
```

Within a segment, sequence ranges are disjoint and determine read precedence (higher wins). Across segments, sequence ranges may overlap — this is expected and has no correctness impact since each segment's chain is merged independently.

For a range scan, the reader merges across segment chains by key. A segment filter on the query can prune entire rows from the table before iteration begins.

#### Example 1: Accumulated L0 state

After several rounds of writes and flushes, the LSM has accumulated L0 SSTs across three segments:

```text
segment    chain
hour=12    L0{seq=700..800}
hour=11    L0{seq=300..400}, L0{seq=400..500}, L0{seq=500..600}
hour=10    L0{seq=1..100}, L0{seq=100..200}, L0{seq=200..300}
```

Within each segment, the L0 SSTs have disjoint sequence ranges. Across segments, sequence ranges may overlap (e.g. `hour=11` and `hour=10` both cover the `seq=300..400` range) — this is expected since concurrent writes to different segments share sequence number space.

#### Example 2: L0 compaction for the oldest segment

The compaction scheduler targets `hour=10`, the oldest segment. Its three L0s are merged into a single sorted run:

```text
segment    chain
hour=12    L0{seq=700..800}
hour=11    L0{seq=300..400}, L0{seq=400..500}, L0{seq=500..600}
hour=10    SR{seq=1..300}
```

The other segments remain in L0. Compactions are segment-scoped, so the scheduler can compact segments independently based on its own policy (e.g. size, age, or L0 count).

#### Example 3: New writes and backfill arrive together

New data for `hour=12` (the current hour) arrives alongside a backfill for `hour=10`. After flush:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=300..400}, L0{seq=400..500}, L0{seq=500..600}
hour=10    L0{seq=801..900}, SR{seq=1..300}
```

The new `hour=12` and `hour=10` L0 SSTs share the same sequence range (same memtable flush), but belong to different segments. The `hour=10` row now has a new L0 entry ahead of its existing SR — the backfill data has a higher sequence range and takes precedence during reads.

#### Example 4: Compacting backfill within a segment

The scheduler compacts `hour=10`, merging its L0 and SR into a single sorted run:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=300..400}, L0{seq=400..500}, L0{seq=500..600}
hour=10    SR{seq=1..900}
```

The `hour=10` SR now spans a wider sequence range that overlaps with other segments. This is fine — the deterministic mapper guarantees that key ranges across segments do not overlap, so no ordering relationship is needed between them.

#### Example 5: Segment retention

Retention expires `hour=10`. All L0 SSTs and sorted runs annotated with `segment=hour=10` are dropped atomically:

```text
segment    chain
hour=12    L0{seq=801..900}, L0{seq=700..800}
hour=11    L0{seq=300..400}, L0{seq=400..500}, L0{seq=500..600}
```

## Detailed Design

### Overview

This RFC introduces a deterministic segment mapper that derives segment metadata from keys. The mapper is configured at database open time as a function `key -> Option<Bytes>`. Because the mapping is deterministic, the system guarantees that a given key always belongs to the same segment. All segments share a common WAL, but each segment is effectively a complete LSM tree in its own right, with its own L0 and sorted runs. This structure is logical and maintained by the shared manifest.

When no segment mapper is configured, all data is unsegmented and the system behaves identically to today.

### Segment Mapper

The segment mapper is configured at database open time:

```rust
pub struct DbOptions {
    pub segment_mapper: Option<Arc<dyn Fn(&[u8]) -> Option<Bytes> + Send + Sync>>,
    // ... existing fields
}
```

The mapper receives a key and returns an optional segment tag. Keys for which the mapper returns `None` are unsegmented. The segment tag is an opaque byte sequence whose semantics are defined by the application — for a timeseries database, it might encode a time bucket; for a log, a segment identifier based on size or time. Ordering, age, and expiry semantics on segments are the application's responsibility. The application (via its custom compaction scheduler) defines what "oldest" or "expired" means for its segments.

Because the mapping is deterministic, a given key always maps to the same segment. This guarantees disjoint key spaces across segments, which makes segment retention unconditionally safe — dropping a segment cannot affect keys in other segments.

### WAL

The WAL requires no format changes to support segments. Because the segment mapper is deterministic and configured at database open time, segment membership can be recomputed from keys during WAL replay. The mapper is applied at memtable flush time to group entries by segment — there is no need to persist segment information in the WAL.

The segment mapper must be configured consistently across restarts. If the mapper changes between the time data was written and when the WAL is replayed, entries may be assigned to different segments than intended. The mapper should be treated as a fixed property of the database.

### Manifest

The manifest is responsible for tracking the logical structure of the LSM tree: the set of L0 SSTs and compacted sorted runs that together represent the current state of the database. With segments, the manifest effectively tracks multiple logical LSM trees — one per segment — within a single shared structure. This section describes the current manifest model, the changes needed to support segments, and the migration path.

#### Current Model: Ordered Sorted Runs

Today, sorted runs are stored as an ordered list (`compacted: [SortedRunV2]`), where each run has a `u32` identifier. The list is maintained in strictly descending ID order, and this ordering encodes read precedence — earlier entries in the list take priority over later ones for overlapping keys. The compaction scheduler assigns IDs to maintain this invariant: for example, when L0 SSTs are compacted into a new sorted run, the new run receives an ID higher than all existing runs.

This design works well for a single logical LSM tree, but creates friction for segment-oriented compaction. When multiple segments are compacted independently and potentially in parallel, coordinating sequential ID assignment across concurrent compactions becomes complex. More fundamentally, a single global ordering between sorted runs is unnecessary when segments partition the compaction and retention scope — there is no need to order runs from different segments relative to each other.

#### Proposed Change: Ulid Identity and Sequence-Based Ordering

This RFC replaces the `u32` sorted run identifier with a `Ulid`. Ulids provide globally unique identifiers without coordination, which is essential for parallel segment-scoped compactions. The sorted run list becomes an unordered bag — the manifest no longer maintains or enforces an ordering between sorted runs.

Read precedence is determined by sequence number ranges rather than sorted run position. To avoid requiring the reader to load SST metadata before every query, the sequence range is made explicit in the manifest on both L0 SSTs and sorted runs. Within a segment, sorted runs have disjoint sequence ranges, and the run with the higher range takes precedence for overlapping keys. Across segments, sequence ranges may overlap, but this has no correctness impact since the reader merges per-segment chains independently.

#### New Manifest Fields

Both L0 SSTs and sorted runs gain two new fields: an explicit sequence range and an optional segment annotation.

The sequence range (`min_seq`, `max_seq`) records the minimum and maximum sequence numbers across all entries in the L0 SST or sorted run. This allows the reader to determine merge precedence directly from the manifest without loading SST index metadata. Combined with key ranges (already available from SST metadata in the manifest), the reader has everything it needs to build the merge DAG.

The segment annotation (`segment: [ubyte]`) is an optional field recording the segment derived from the mapper at flush time. L0 SSTs and sorted runs without a segment annotation are unsegmented and are handled by the existing compaction path.

Every sorted run has a `Ulid` identifier. Legacy sorted runs migrated from `ManifestV2` also retain their original `u32` ID so that the read path can fall back to list-position ordering for those runs. The FlatBuffer schema introduces `SortedRunV3`:

```flatbuffer
table SortedRunV3 {
    id: Ulid (required);
    legacy_id: uint32;          // set only for migrated runs, absent for new runs

    min_seq: ulong;
    max_seq: ulong;
    segment: [ubyte];
    ssts: [CompactedSsTableView] (required);
}
```

For L0 SSTs, `CompactedSsTableView` gains sequence range and segment fields:

```flatbuffer
table CompactedSsTableViewV2 {
    id: Ulid (required);
    sst_id: Ulid (required);
    visible_range: BytesRange;
    min_seq: ulong;
    max_seq: ulong;
    segment: [ubyte];
}
```

The public `SortedRun` struct exposed through `ManifestCore` is updated to use an opaque `SortedRunId` (defined in the [Compaction](#compaction) section below) in place of the current `u32`:

```rust
pub struct SortedRun {
    pub id: SortedRunId,
    pub segment: Option<Bytes>,
    pub sst_views: Vec<SsTableView>,
}
```

Together, these changes allow the manifest to represent multiple logical LSM trees within a single structure. The reader can determine merge order, the compaction scheduler can plan segment-scoped work, and segment-filtered queries can prune irrelevant data — all from manifest metadata alone.

#### Migration

The migration is performed in two phases: first the decoder is updated to understand V3, then the encoder is switched to write V3.

**Phase 1: Decoder rollout.** The manifest decoder is updated to handle both V2 and V3 formats. This can be deployed independently — the encoder continues writing V2, so no state changes occur. Both the writer and the compactor gain the ability to read V3 manifests before any V3 manifests exist. This is a prerequisite for Phase 2.

**Phase 2: Writer-driven upgrade.** The writer is responsible for performing the V2→V3 migration. On its first manifest write after the encoder is switched to V3, the writer reads the existing V2 manifest, migrates the in-memory representation, and writes it back as V3. The migration involves:

- Assigning a Ulid to each existing `u32`-identified sorted run and L0 SST view.
- Retaining the original `u32` in the `legacy_id` field for each sorted run.
- Computing the sequence range (`min_seq`, `max_seq`) for each sorted run and L0 SST. This requires reading SST index metadata from object storage to determine the first and last sequence numbers. This is the most expensive part of the migration — for a database with many sorted runs, it involves one read per SST. However, it is a one-time cost.
- Leaving all existing runs and L0 SSTs unsegmented (no segment annotation).

The standalone compactor must not upgrade the manifest version on its own. If it encounters a V2 manifest, it continues operating in V2 mode. This prevents a compactor from writing a V3 manifest that the writer does not yet understand. The compactor only begins writing V3 after it reads a V3 manifest that the writer has already produced.

After the upgrade, the read path handles both run types: runs with a `legacy_id` use their list position for precedence (as today), while runs without one use their explicit sequence ranges. The compaction scheduler and `CompactionSpec` reference sorted runs through the opaque `SortedRunId` type — the scheduler is unaware of the legacy distinction. Because every sorted run has a Ulid after migration (including legacy ones), the post-upgrade `CompactionSpecV2` can reference all runs uniformly by Ulid. The compactor resolves each Ulid through the manifest, which knows whether the underlying run has a `legacy_id` and handles the indirection.

New compactions always produce runs without a `legacy_id`. Over time, as legacy runs are compacted into newer ones, the `legacy_id` field is no longer present on any run and the database is fully migrated.

### Write Path

The write path is unchanged up to memtable flush. Writes go through the WAL and memtable as today — the segment mapper is not involved until flush time. When a memtable is frozen and flushed, the mapper is applied to each entry's key to determine its segment. Entries are grouped by segment and one L0 SST is produced per segment. Each L0 SST is recorded in the manifest with its segment annotation.

When the WAL is enabled, each batch is individually durable in the WAL, and the multi-segment split at flush time is an internal concern. When the WAL is disabled, all L0 SSTs from a single flush must be uploaded and then added to the manifest in a single atomic update. Partial visibility of a multi-segment flush would break durability semantics for batches that have not yet been persisted to L0.

### Read Path

Queries accept an optional segment filter predicate:

```rust
pub struct ReadOptions {
    pub segment_filter: Option<Box<dyn Fn(Option<&Bytes>) -> bool>>,
    // ... existing fields
}
```

The predicate receives `None` for unsegmented data and `Some(&bytes)` for tagged segments. When set, the reader prunes L0 SSTs and sorted runs that do not match before building the merge iterator.

After filtering, the reader builds a merge iterator from the remaining L0 SSTs and sorted runs. For runs with explicit sequence ranges (`Sequenced` runs), entries are ordered by sequence number (higher wins). Legacy runs (those with a `legacy_id` from the V2 migration) continue to use their list position for precedence until they are compacted into newer runs. Across segments, per-segment streams are merged by key, again with sequence number precedence. The reader does not interpret segment metadata beyond filtering — all merge ordering is derived from key ranges, sequence numbers, and (for legacy runs) list position.

### Compaction

Today, `CompactionSpec` references sources by a mix of `Ulid` (for L0 SST views) and `u32` (for sorted runs), and specifies a single `u32` destination SR ID. Because the manifest version determines whether sorted runs are identified by `u32` (V2) or `Ulid` (V3), the compaction scheduler needs a way to reference sorted runs without being coupled to either representation. Otherwise, the scheduler would need version-specific branching to construct specs.

To solve this, the sorted run ID is abstracted behind an opaque `SortedRunId` type:

```rust
/// Opaque sorted run identifier. Taken from the manifest or generated
/// via ManifestCore::next_sorted_run_id(). The scheduler never inspects
/// the inner representation.
pub struct SortedRunId(SortedRunIdInner);

enum SortedRunIdInner {
    Legacy(u32),
    Sequenced(Ulid),
}

pub enum SourceId {
    SortedRun(SortedRunId),
    SstView(Ulid),
}

pub enum Action {
    /// Merge sources into a new sorted run with the given ID.
    Merge(SortedRunId),
    /// Drop sources without producing output (retention).
    Drop,
}

pub struct CompactionSpec {
    sources: Vec<SourceId>,
    action: Action,
}
```

The scheduler takes sorted run IDs directly from the manifest when building sources and calls `ManifestCore::next_sorted_run_id()` to generate the destination for a merge:

```rust
impl ManifestCore {
    /// Generate a new sorted run ID appropriate for the current manifest version.
    /// Returns Legacy(u32) for V2 manifests, Sequenced(Ulid) for V3.
    pub fn next_sorted_run_id(&self) -> SortedRunId { ... }
}
```

This keeps the scheduler version-agnostic. It never constructs or inspects `SortedRunId` internals — it copies them from the manifest for sources and uses the generation hook for destinations. The compactor resolves the opaque ID back to the appropriate storage references.

All sources in a single compaction must share the same segment annotation. A `CompactionSpec` with sources from different segments is invalid and will be rejected during validation. This invariant ensures that compaction remains segment-scoped — each compaction reads and writes within a single segment's logical LSM tree.

For `Merge`, the executor merges inputs and produces one output sorted run, annotated with the same segment. For `Drop`, the sources are removed from the current manifest with no output — this is how segment retention is expressed. Note that `Drop` only removes references from the latest manifest; the underlying SST files are not deleted immediately. The garbage collector is responsible for cleaning up SST files once no remaining manifest versions (including those pinned by snapshots or checkpoints) reference them.

To support segment-aware scheduling, `ManifestCore` exposes a segment index:

```rust
pub struct SegmentView {
    pub l0: Vec<SsTableView>,
    pub sorted_runs: Vec<SortedRun>,
}

impl ManifestCore {
    /// Returns a map from segment tag to the L0 SSTs and sorted runs
    /// belonging to that segment. Unsegmented data is not included.
    /// Unsegmented L0 SSTs and sorted runs are accessible through the
    /// existing l0 and compacted fields.
    pub fn segments(&self) -> HashMap<Bytes, SegmentView> { ... }
}
```

The scheduler uses this index to plan work: selecting segments with many L0s, merging sorted runs within a segment, or identifying expired segments for retention.

Segmented and unsegmented data may coexist in the same database. For example, a TSDB might use segments for time-bucketed metric data while keeping permanent configuration state as unsegmented. Migration from an unsegmented to a segmented layout is another scenario. When both are present, the scheduler is responsible for handling both: using `segments()` to plan segment-scoped compactions and the existing `l0`/`compacted` fields for unsegmented data.

The `.compactions` file schema is updated to match. Before the V3 upgrade, the existing `TieredCompactionSpec` continues to be used. After the upgrade, a new `CompactionSpecV2` is used with Ulid-based sorted run references:

```flatbuffer
union CompactionSpecUnion {
    TieredCompactionSpec,
    CompactionSpecV2,
}

enum CompactionActionType : byte {
    Merge = 0,
    Drop,
}

table CompactionSpecV2 {
    l0_view_ids: [Ulid];
    sorted_runs: [Ulid];
    destination: Ulid;
    output: CompactionActionType;
}
```

### Recovery and Progress Tracking

The resume mechanism is unchanged: the executor reads the last output SST to compute a resume cursor. Since each compaction produces a single sorted run, the existing single-destination progress model applies without modification. The compactor epoch and fencing protocol are unaffected.

## Future Work

This RFC focuses on segment-oriented planning and explicit drop semantics. Natural next steps include key-rewriting transforms and multi-stage timeseries rollups built on top of these primitives.

An additional optimization opportunity is to parallelize independent segment merges. The execution model in this RFC can be extended in the future to process disjoint segments concurrently.

For example, daily timeseries compactions can build on hourly segmentation:

- First, compaction shapes data into hourly segment-aligned sorted runs.
- Then, daily compactions select exactly the 24 hourly segments for a target day as inputs.

This staged model gives a clean source-selection boundary for day rollups and avoids mixing unrelated segment data, reducing unnecessary write amplification.

## Alternatives

### Time-Window Compaction Strategy (TWCS)

A more traditional approach would implement TWCS directly: the system defines time windows (e.g. 1 hour), routes data by event or ingestion time, and manages window lifecycle automatically — closing windows after a threshold, never merging across windows, and dropping expired windows based on a configured retention period. This provides good out-of-the-box behavior for timeseries workloads with minimal application involvement.

This RFC opts for a more general approach: opaque segment metadata defined by the application rather than time windows defined by the system. This is more flexible — segments can represent time buckets, log partitions, or any other application-defined scope — but requires a custom compaction scheduler and shifts more responsibility to the application. The generality also paves the way for key-rewriting transforms (e.g. rolling hourly buckets into daily ones), which would be difficult to retrofit into a TWCS model where window identity is system-managed.

### Unifying L0 and sorted runs

With the move to sequence-based ordering and bag semantics, the structural distinction between L0 SSTs and sorted runs becomes less meaningful. An L0 SST is semantically equivalent to a sorted run containing a single SST — both carry a segment annotation and a sequence range, and the read path treats them uniformly when building the merge DAG. Collapsing the two into a single abstraction would simplify the manifest schema and the read/compaction paths. This RFC preserves the existing L0/SR distinction to limit scope, but unification is a natural follow-on.

### Segment tag in WriteOptions instead of a mapper

An alternative to the deterministic mapper is to specify the segment tag directly in `WriteOptions` as `segment: Option<Bytes>`. This gives the caller full control over segment assignment per write batch, without requiring the segment to be derivable from the key.

This approach is more flexible — the caller can assign segments based on external context, not just key structure. However, it introduces significant complexity:

- **WAL format changes required.** Because the segment tag is transient (it exists only in `WriteOptions`), the WAL must persist it so that WAL replay can reconstruct segment groupings. This requires extending the WAL block format with a segment field and splitting blocks on segment transitions.
- **Cross-segment tombstone issues.** Without a deterministic mapping, the same key can appear in multiple segments. This makes segment retention unsafe: dropping a segment can resurface a shadowed key in another segment. Tombstone elision also becomes conservative — tombstones must be retained until the segment is dropped, since a key may exist in another segment with a lower sequence number.
- **Batch atomicity constraints.** Since `WriteOptions` is per-batch, all entries in a batch must belong to the same segment. Cross-segment batches are not supported.

This RFC uses the deterministic mapper approach because it avoids WAL changes, provides unconditionally safe retention, and simplifies tombstone handling. The `WriteOptions` approach could be revisited if use cases emerge that require segment assignment independent of key structure.

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
- Segment boundaries may be defined by time windows or by size thresholds.
- Segments are represented as contiguous key ranges.

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
