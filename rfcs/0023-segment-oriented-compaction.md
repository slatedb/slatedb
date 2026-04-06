# SlateDB RFC: Segment-Oriented Compaction

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

Append-only structures often organize data into ordered segments for metadata efficiency and retention. For example, a timeseries system may group new data and associated indexes into hourly buckets, while a log may group data by size- or time-based segments. As segments age out, they can be removed as an atomic unit along with their metadata.

This RFC proposes a segment-oriented compaction framework for SlateDB to align LSM compaction with that append-only structure. The central use case is append-ordered segments that map to contiguous key ranges (for example time buckets or log chunks). The proposal focuses on segment-aware compaction planning, multi-output compactions, and explicit segment-level retention/deletion semantics.

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

- Enable segment-aware compaction for append-ordered data structures.
- Support explicit segment-level deletion as part of compaction lifecycle and retention.
- Preserve deterministic, recoverable compaction behavior under backfill and retries.
- Keep the framework general enough to support both timeseries and log-style segmentation.

## Non-Goals

- Defining timestamp-native TWCS semantics (event-time watermarks, lateness contracts, clock policy).
- Supporting segment remapping/key-rewriting transforms in this first cut.
- Defining read-path segment pruning/query APIs.

## Target LSM Shape

This section describes the intuitive LSM shape this RFC is trying to enable.

### Invariants

- **Segment-to-SR mapping**: each sorted run contains data for exactly one segment (as defined by the configured `key -> segment` mapping).
- **Per-segment run order**: when a segment spans multiple sorted runs (for example due to backfill), those runs preserve insertion order for that segment.
- **Cross-segment order**: relative ordering between different segments is not required for correctness.
- **Lifecycle alignment**: new writes concentrate in the newest segment(s), while older segments are mostly stable and eventually dropped by retention.
- **Backfill tolerance**: one segment may temporarily span multiple sorted runs (for example when late data is compacted separately), including with gaps between those runs. Each run still remains single-segment.
- **Convergent layout goal**: compaction should tend toward an intuitive global shape where sorted runs are arranged by segment order over time.

### Visual Model

```text
L0: mixed recent writes across multiple segments
  (for example hour=15, hour=14, hour=13, plus occasional backfill)

            compact by segment
                     v

SR (newer):  [hour=15]  [hour=14]  [hour=13]
                     |
                     | age / rollup compactions
                     v
SR (older):  [day=2026-03-10]  [day=2026-03-09]
                     |
                     | retention policy
                     v
             expired segments dropped atomically
```

### Why This Shape

- Compaction scope is naturally aligned to append boundaries instead of arbitrary source groups.
- Cold segments can be compacted independently from hot segments.
- Retention can operate at segment granularity, reducing row-by-row rewrite and cleanup work.

### Segment-to-SR ID Ordering Examples

Proposed SR ID rules:

- When segment-aware L0 compaction creates new segment runs, assign new SR IDs in segment order.
- When compacting runs that belong to the same segment, the merged output reuses the lowest input SR ID.

The examples below illustrate these rules.

#### Example 1: Mapping segments found in L0

Assume L0 contains keys for segments `hour=10`, `hour=11`, and `hour=12`, and existing SR ids are:

```text
SR 8 -> hour=09
SR 7 -> hour=08
```

After segment-aware L0 compaction, new SR ids are assigned in segment order:

```text
SR 11 -> hour=12
SR 10 -> hour=11
SR 9  -> hour=10
SR 8  -> hour=09
SR 7  -> hour=08
```

Requirement: when compacting L0, the framework must discover each segment present in the input and assign a sorted run ID for each resulting segment run.

#### Example 2: Backfill creates a gapped split for an existing segment

Continuing from Example 1, assume late backfill for `hour=10` arrives after newer segments have advanced. Compaction can temporarily produce:

```text
SR 12 -> hour=10   (late backfill)
SR 11 -> hour=12
SR 10 -> hour=11
SR 9  -> hour=10   (existing older run)
SR 8  -> hour=09
SR 7  -> hour=08
```

The segment `hour=10` now spans multiple runs with a visual gap (`hour=12` and `hour=11`) between them. This is a temporary state; each run is still single-segment.

Requirement: the framework must allow backfill data for an existing segment to be materialized as an additional run for that same segment, even when other segments lie between those runs.

#### Example 3: Merging runs of the same segment

Continuing the previous state, compacting `SR 12` and `SR 9` (both `hour=10`) produces:

```text
SR 11 -> hour=12
SR 10 -> hour=11
SR 9  -> hour=10   (merged output keeps lower input id)
SR 8  -> hour=09
SR 7  -> hour=08
```

Rule: when merging runs for the same segment, the output run takes the lower SR id of the merged inputs.

Requirement: the framework must support compaction of runs that share a segment and allow deterministic output ID selection (here: lowest input SR id).

#### Example 4: Deleting the oldest segment

Continuing the previous state, assume retention expires `hour=08`. Segment-level deletion removes that segment atomically:

```text
SR 11 -> hour=12
SR 10 -> hour=11
SR 9  -> hour=10
SR 8  -> hour=09
```

Rule: when a segment is expired, all sorted runs belonging to that segment are removed together.

Requirement: the framework must support segment-level deletion as an explicit operation that can remove all runs for a segment atomically.

## API Direction (Draft)

This section sketches the API direction required to support segment-oriented compaction while preserving existing behavior.

### CompactionSpec Output Mode

`CompactionSpec` gains an output mode enum:

```rust
enum CompactionOutput {
    SortedRun(u32),      // explicit single destination (current behavior)
    DerivedBySegment,    // destinations discovered during execution via segment mapping
}
```

When `output = SortedRun(id)`, behavior remains equivalent to current compaction semantics.

When `output = DerivedBySegment`, outputs are discovered by the compactor at runtime as it iterates inputs and applies the configured `key -> segment` mapping.

Retry/recovery tradeoff:

- Explicit outputs (`SortedRun(id)`) are easier to replay deterministically because destination IDs are fixed in `CompactionSpec`.
- Derived outputs (`DerivedBySegment`) improve flexibility for L0-driven segment discovery, but require additional recovery state so retries reproduce the same output IDs and source-to-output mapping.

### Explicit Drop Intent

`CompactionSpec` includes an explicit `drop_sources` field.

- The compaction scheduler is responsible for constructing `CompactionSpec`, including `drop_sources`.
- Sources listed in `drop_sources` must be removed.
- All sources in `sources` that are not listed in `drop_sources` must be merged into outputs.
- Validation must ensure `drop_sources` is a subset of `sources`.

### Segment Mapping Hook

Compactor configuration provides a segment mapping hook:

- If no segment mapper is configured, SlateDB runs the existing compaction path.
- If a segment mapper is configured and `output = DerivedBySegment`, SlateDB routes execution through an internal `SegmentCompactor`.

At a high level, the mapper provides a deterministic `key -> segment` function used by the segment merge algorithm described above.

#### Segment Representation

This proposal does not currently persist segment IDs in manifest state.

- Segment identity is derived during compaction via the configured `key -> segment` mapping.
- Segment representation is intentionally flexible (for example, `Bytes`).
- This flexibility allows key-prefix-based segment identity, which is the expected model for the log and timeseries examples mentioned in the appendices.

### SegmentCompactor (Internal)

`SegmentCompactor` is an internal component responsible for:

1. Applying `key -> segment` while iterating merged L0 input.
2. Coordinating per-segment merge state with any SR inputs for the same segment.
3. Producing one output run per segment present in the inputs.
4. Applying explicit drop behavior for sources listed in `drop_sources`.
5. Producing manifest updates atomically at compaction completion.

This RFC does not require `SegmentCompactor` to be user-pluggable; it is an internal execution strategy selected by compaction mode and mapper presence.

### Segment Merge Algorithm Sketch

Assume a deterministic `key -> segment` function. The primary shape in this RFC is ordered contiguous segments; broader mappings are possible but are not the main target of the examples in this section.

Terms used below:

- **Input frontier**: the current key of the merged L0 iterator. After the frontier advances to key `K`, no future L0 row can have key `< K`.
- **Per-segment L0 queue**: rows already read from L0 for a specific segment but not yet emitted to that segment's output.

High-level execution sketch (streaming):

1. Build a map from `segment -> input SR iterators` for selected sorted-run sources.
2. Build one merged L0 iterator across selected L0 inputs.
3. For each row from merged L0:
   - Compute `segment = map(key)`.
   - Append the row to that segment's L0 queue.
   - Update the input frontier to the row's key.
4. Whenever the frontier advances, run `try_emit(frontier)` for each active segment:
   - Merge that segment's L0 queue and SR iterators.
   - Advance/copy that segment's merge inputs up to the frontier.
   - Emit only rows with key `<= input frontier`.
   - This is safe because future L0 rows cannot appear below the current frontier key.
5. Keep one output run per segment encountered in the inputs.
6. If there are no L0 inputs, treat the frontier as `+inf` from the start.
   - In this case, the compactor can iterate and copy remaining segment SR iterators directly to outputs.
7. After L0 is exhausted, set frontier to `+inf`, drain all segment state, and finalize all outputs.

## Open Questions

- Should segment-oriented compaction require a custom scheduler, or should SlateDB provide a default segment-aware scheduler?
- For `DerivedBySegment`, what compaction-intent and SR-ID-assignment state must be persisted so retries/crash recovery can replay this policy deterministically?
- How should segment retention interact with snapshots and tombstone semantics?

## Future Work

This RFC focuses on segment-oriented planning and explicit drop semantics. Natural next steps include key-rewriting transforms and multi-stage timeseries rollups built on top of these primitives.

An additional optimization opportunity is to parallelize independent segment merges. The execution model in this RFC can be extended in the future to process disjoint segments concurrently.

For example, daily timeseries compactions can build on hourly segmentation:

- First, compaction shapes data into hourly segment-aligned sorted runs.
- Then, daily compactions select exactly the 24 hourly segments for a target day as inputs.

This staged model gives a clean source-selection boundary for day rollups and avoids mixing unrelated segment data, reducing unnecessary write amplification.

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
