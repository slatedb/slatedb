# SlateDB RFC: Stat-Driven Compaction Triggers

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [0. Preface on safeguards of size-tiered compaction](#0-preface-on-safeguards-of-size-tiered-compaction)
   * [1. `SstCompactionStats` on `SsTableInfo`](#1-sstcompactionstats-on-sstableinfo)
   * [2. `max_age` trigger (data-age)](#2-max_age-trigger-data-age)
   * [3. `tombstone_ratio` trigger](#3-tombstone_ratio-trigger)
   * [4. `periodic` trigger (file-age)](#4-periodic-trigger-file-age)
   * [5. Trigger ordering](#5-trigger-ordering)
   * [6. Scheduler safeguards and cooldowns](#6-scheduler-safeguards-and-cooldowns)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api-query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   * [Performance & Cost](#performance-cost)
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

* [Kirin Rastogi](https://github.com/kirinrastogi)

## Summary

This RFC extends SlateDB's compaction subsystem with three new scheduler triggers, a **`max_age`** trigger (best-effort `expire_ts` reclamation), a **`tombstone_ratio`** trigger, and a **`periodic`** trigger. To drive these decisions, `SsTableInfo` gains an `SstCompactionStats` block carrying per-SST metadata: put/delete/merge counts, `file_creation_time`, and `oldest_ancestor_time`. Today's `SizeTieredCompactionScheduler` only fires on size/source-count heuristics, which leaves `expire_ts` expiration dependent on incidental compaction and gives no direct lever for reclaiming space from delete-heavy regions. The three new triggers compose with the existing size trigger. This RFC summarizes the design of the new triggers, the priority they will be picked in, and safeguards to prevent runaway cascading compaction specs.

## Motivation

**`expire_ts` reclamation needs a data-age forcing function (`max_age`).** SlateDB already supports `expire_ts` end-to-end: writes carry an expiry timestamp, and the `RetentionIterator` tombstones expired entries when it reaches them. What SlateDB does not have is a trigger to initiate compaction runs on cold data. A sorted run that is never picked up by the size-tiered scheduler carries its expired entries indefinitely.

**Tombstone-heavy regions need a direct lever (`tombstone_ratio`).** A region that accumulates deletes faster than size pressure compacts it can sit on a large number of tombstones. Read fanout grows with the count of live tombstones for any given point lookup, and space amplification grows with their cumulative size. Furthermore, the bottom-two SRs may accumulate tombstones (due to snapshot retention or other reasons), with no trigger for the `RetentionIterator` to drop them when safe. A `tombstone_ratio` trigger acts to push down SRs, regardless of how long ago the data was written. The `tombstone_ratio` trigger will apply on any SR, and will have a mechanism to bypass `clamp_min` to target the bottom-two SRs individually so that tombstones can be dropped.

**File-age hygiene needs a separate trigger (`periodic`).** Even without the use of `expire_ts` on entries, cold SSTs benefit from a cadence-based rewrite: bloom filters re-index against the current key distribution, and SSTs migrate naturally to the current format after an encoding/compression bump. This is triggered on `file_creation_time` being too old.

**The metadata to drive these decisions doesn't exist yet.** `SsTableInfo` today carries no timestamps and no top-level put/delete/merge counters. `SstStats` already aggregates `num_puts`/`num_deletes`/`num_merges` per SST and per block, but it lives in a separately-fetched footer block referenced via `stats_offset`/`stats_len`. Loading it for every SST in every scheduler poll would add an object-store round trip per SST. The triggers need a small, eagerly-loaded surface on `SsTableInfo` itself.

## Goals

- Support a `max_age` compaction trigger (data-age driven, best-effort `expire_ts` reclamation) configured per database.
- Support a `tombstone_ratio` compaction trigger configured per database.
- Support a `periodic` compaction trigger (file-age driven, hygiene) configured per database.
- Carry the per-SST metadata these triggers consume in a new `SstCompactionStats` block on `SsTableInfo`.
- Keep the metadata generic enough that other compaction strategies (e.g. leveled) can leverage this.

## Non-Goals

- **Guaranteed expiry** CompactionSpecs need to be safe to execute, and guaranteeing entry expiration could have unwanted side effects like cascading compaction.
- **Subcompaction.** Splitting a single compaction into parallel sub-jobs is out of scope. It could be tracked somewhere else after 0025-distributed-compaction.md ships.
- **Leveled compaction itself.** Tracked in [issue #1598](https://github.com/slatedb/slatedb/issues/1598). This RFC only adds the metadata it would consume.
- **New compactor-wide settings or `BackpressureChecker` changes.** Compactors will continue to rely on existing safeguards; some new cooldowns will be added for these triggers.

## Design

### 0. Preface on safeguards of size-tiered compaction

This section summarizes the safeguards already in `SizeTieredCompactionScheduler` so the rest of the Design section can reference them without re-explaining. Every new trigger in this RFC plugs into the existing `pick_next_compaction` path and inherits these mechanics unchanged, with the exception of `tombstone_ratio` allowing for a single-SR re-write targeting one of the bottom-two SRs.

**Source clamps.** `clamp_min` and `clamp_max` enforce that the sources for the compaction fall within `min_compaction_sources` and `max_compaction_sources`.

**Fairness and priority across segment trees.** `propose()` builds a `TreeState` per tree, and then loops round-robin across them. Trees are sorted by L0 length descending.

**`BackpressureChecker` and the size-similarity threshold.** These checks prevent emitting a compaction whose output would immediately need to be re-compacted with a saturated neighbor tier.

**Consecutive-source validation and `ConflictChecker`.** These validate that the input sources are consecutive, and that the output does not target any in-use or reserved SR ids.

The `min_compaction_sources` setting will need to be bypassed for just the `tombstone_ratio` trigger. This is so a bottom SR can be re-written when it reaches the `tombstone_ratio`.
These new triggers need to be implemented in a way that aligns with these other checks and safeguards. An SR that has hit the `tombstone_ratio` or `max_age` should not bypass the `BackpressureChecker`, for example.

Additionally, the priority of the triggers needs to be picked in a way where L0 compactions can continue to execute. The priority should be the default size-based heuristic, then `max_age`, then `tombstone_ratio`, then `periodic`.

### 1. `SstCompactionStats` on `SsTableInfo`

`SsTableInfo` gains an optional nested table, embedded so the scheduler can read it from the manifest path without needing object-store fetches per SST in the `propose()` function. Schema addition in `schemas/sst.fbs`:

```flatbuffer
// New table: per-SST metadata used by the compaction scheduler.
table SstCompactionStats {
    // Number of put entries in the SST. Copied verbatim from SstStats.num_puts.
    num_puts: ulong;
    // Number of delete entries (tombstones). Copied from SstStats.num_deletes.
    num_deletes: ulong;
    // Number of merge entries. Copied from SstStats.num_merges.
    num_merges: ulong;
    // Unix epoch milliseconds at which the SST builder finalized this file.
    file_creation_time: ulong;
    // Unix epoch milliseconds tracking the age of the *data*, not the file.
    // Flush path: equal to file_creation_time (no ancestors).
    // Compaction path: min(oldest_ancestor_time) across all input SSTs.
    oldest_ancestor_time: ulong;
}

// Has metadata about a SST file.
table SsTableInfo {
    // ... existing fields ...

    // Present on SSTs written by versions that support stat-driven
    // compaction triggers; absent on older SSTs.
    compaction_stats: SstCompactionStats;
}
```

**Why on `SsTableInfo` instead of `SortedRun`?** Other yet-to-be-implemented compaction strategies like leveled compaction operate on individual SSTs instead of entire SRs. Setting compaction stats per SST allows them to be consumed by other strategies.

**Derivation rules.**

- **Num puts, deletes, merges.** These will be duplicated from the `SstStats`, and set when the `SstBuilder` creates an SST.
- **Flush path.** When a memtable flush produces an SST, `file_creation_time = oldest_ancestor_time = now_ms()`. The SST has no ancestors; the data is as old as the file.
- **Compaction path.** When a compaction produces an output SST, `file_creation_time = now_ms()` and `oldest_ancestor_time = min(input.oldest_ancestor_time)` across all input SSTs. The ancestor timestamp propagates forward so the age of the data is preserved through arbitrarily many compaction generations.

**Rust-side wiring.**

- `EncodedSsTableFooterBuilder` threads the new optional `SstCompactionStats` table through to flatbuffer encoding.
- Aggregation of `num_puts`/`num_deletes`/`num_merges` per SST already happens during `finish_block()` in the `SstBuilder`. These totals will be copied into `SstCompactionStats` at this time.
- In `CompactionExecutor`, when issuing the output builder, compute `min(input.oldest_ancestor_time)` from the L0 `SstView`s and the input SSTs of the source `SortedRun`s on the compaction job. Pass it into the builder.
- `EncodedSsTableBuilder::new` (or an analogous builder setter) accepts an `oldest_ancestor_time: Option<i64>`. The flush call site passes `None`; the compaction call site computes the min across input SSTs and passes that.
- At `EncodedSsTableBuilder::build`, record `file_creation_time` from the injected clock, resolve `oldest_ancestor_time` (defaulting to `file_creation_time` when `None`), and attach `SstCompactionStats` to the `SsTableInfo` constructed by the footer builder.

**Clock source.** Use the system clock injected via SlateDB's existing `Clock` abstraction so deterministic-simulation tests can mock it. All times are Unix epoch milliseconds.

**Backwards compatibility.** The new field is appended to the end of `SsTableInfo`. SSTs written by older versions decode `compaction_stats = None`. The scheduler treats absent stats as "ineligible for `max_age`, `tombstone_ratio`, and `periodic` triggers" — the SST remains fully eligible for the existing size trigger. After a few size-driven compactions, all live SSTs have been re-emitted with the new field populated.

### 2. `max_age` trigger (data-age)

A new setting:

```rust
// in slatedb/src/config.rs, CompactorOptions
pub struct CompactorOptions {
    // ... existing fields ...

    /// If set, a sorted run is eligible for `max_age` compaction once
    /// `now - oldest_ancestor_time >= max_age`. Reclamation is multi-cycle:
    /// each firing moves the source one level closer to the bottom, so an
    /// expired entry is physically dropped within roughly
    /// `max_age × N + compactor drain time`, where `N` is the number of
    /// levels between the entry's containing source and the bottom SR.
    /// Tune `max_age` to `expected_expiry / expected_levels` for a target
    /// reclamation latency.
    /// Default `None` = `max_age` trigger disabled.
    pub max_age: Option<Duration>,
}
```

**Best-effort framing.** `max_age` does *not* honor `expire_ts` at exactly `expire_ts`. Data may move closer to the bottom per `max_age` window, it will still be subject to clamping sources and Backpressure. The first firing on data containing an expired entry causes `RetentionIterator` to synthesize a tombstone for that entry in the next-lower SR. The tombstone may be elided once it eventually reaches the bottom SR, subject to the `RetentionIterator`. It may reach the bottom SR via further `max_age` firings, opportunistic size compaction, or `tombstone_ratio`. The trigger replaces "best-effort by incidental compaction" with "best-effort with a bounded per-cycle latency knob."

**Eligibility.** An L0 SST or an SR is eligible when both conditions are true:

- `now - oldest_ancestor_time >= max_age` — the oldest ancestor is older than or equal to `max_age`.
- `now - file_creation_time >= max_age` — the `file_creation_time` is older than or equal to `max_age`, to act as a cooldown.

For eligibility of an SR, the `min(oldest_ancestor_time)` and `max(file_creation_time)` across its SSTs will be used. Per-tree evaluation, mirroring the existing size-tiered loop. The cooldown prevents the rewrite-loop hazard: `oldest_ancestor_time` propagates as min across compactions, so the output of a single-level rewrite still carries the old `oldest_ancestor_time` even though its `file_creation_time` has just been reset. Without the cooldown, the trigger would re-qualify the output on the next poll and keep firing on it within the same `max_age` window.

**How it fires.** When `max_age` fires on sources, the trigger emits a compaction spec that moves sources exactly one sorted run closer to the bottom. The shape mirrors a normal size-tiered SR -> SR pick and respects existing safeguards (`clamp_min`, `clamp_max`, `BackpressureChecker`, `ConflictChecker`).

### 3. `tombstone_ratio` trigger

A new option:

```rust
// in slatedb/src/config.rs, CompactorOptions
pub struct CompactorOptions {
    // ... existing fields ...

    /// If set, an SST or SR is eligible for `tombstone_ratio` compaction
    /// when its tombstone fraction is at least this value.
    /// Default `None` = disabled. Recommended: 0.25.
    pub max_tombstone_ratio: Option<f32>,
}
```

**Ratio definition.** For a single SST:

```
ratio = num_deletes / (num_puts + num_deletes + num_merges)
```

For an SR, aggregate `num_deletes` and the denominator across all SSTs in the run, then compute. A single SST containing many tombstones should not trigger compaction by itself, unless it moves the aggregate ratio of the entire SR above the threshold.

**Eligibility.** An SST or SR is eligible when its computed `ratio >= max_tombstone_ratio`. All SRs may be eligible. There is a special case where the bottom-two SRs will be eligible for compaction with a `clamp_min` bypass, as the source input would be of size 1. SSTs lacking `compaction_stats` are treated as ineligible. An absolute number of deletes needs to exist in an SR before it is eligible. See [Open Questions](#open-questions) for why a time-based cooldown may also be warranted.

**How it fires.** Similar mechanics to `max_age`. When `S` is a non-bottom SR: sources = `[S, next_sr_below_S]`, destination = `next_sr_below_S.id`. The tombstones in `S` may be dropped in `next_sr_below_S`, subject to the `RetentionIterator`. The tombstones themselves continue migrating down one level per firing until they reach the bottom and may be elided. When `S` is the bottom SR: self-rewrite drops tombstones outright.

**Source selection.** The trigger picks the **highest-ratio** eligible source per tree per round-robin pass, where `max_age` picks the oldest. The two triggers have different priority orderings within their candidate sets.

### 4. `periodic` trigger (file-age)

A new setting:

```rust
// in slatedb/src/config.rs, CompactorOptions
pub struct CompactorOptions {
    // ... existing fields ...

    /// If set, an L0 SST or SR is eligible for `periodic` compaction once
    /// `now - file_creation_time >= periodic_compaction_interval`.
    /// Default `None` = disabled. Recommended: 90 days or longer — this
    /// is hygiene, not correctness, so the cadence should be much longer
    /// than `max_age`.
    pub periodic_compaction_interval: Option<Duration>,
}
```

**Eligibility.** An L0 SST or SR is eligible when `now - file_creation_time >= periodic_compaction_interval`. For an SR, use `min(file_creation_time)` across its SSTs. No separate cooldown is required — each rewrite resets `file_creation_time` on the output, so the trigger is naturally cooldown-bounded by its own interval.

**How it fires.** Same single-level rewrite mechanics as `max_age` and `tombstone_ratio` (see [`max_age` trigger (data-age)](#2-max_age-trigger-data-age)). Sources = `[S, next_sr_below_S]` (or `[S]` self-rewrite if `S` is the bottom or an L0 SST emitting to a fresh SR). Hygiene goals — bloom filter refresh, format-upgrade rollover, opportunistic tombstone cleanup — are met by any rewrite; bottom-reaching is not required.

**Source selection.** The trigger picks the **oldest eligible source by `file_creation_time`** per tree per round-robin pass.

### 5. Trigger ordering

Within each `propose()` round-robin pass (per tree), evaluate triggers in order and return the first spec produced:

1. **Size** runs first (existing logic in `pick_next_compaction`, `size_tiered_compaction.rs:331-369`). Only trigger that relieves write-side L0 backpressure; must win when eligible. Within the size trigger, the existing structural priority is preserved: the L0 → SR phase is attempted first and only falls through to the SR → SR phase when the L0 candidate set fails one of the existing gates. This preserves today's behavior that writer-side relief takes precedence over lower-level merging.
2. If size produced no spec, evaluate **`max_age`**: pick the oldest eligible contiguous sources by `oldest_ancestor_time`. Emit one `CompactionSpec` that is subject to the gates and validations.
3. If `max_age` produced no spec, evaluate **`tombstone_ratio`**: pick the eligible sources, prioritizing contiguous sources over the bottom-two SRs. Emit one `CompactionSpec` which bypasses `clamp_min` if it is a single-SR re-write.
4. If `tombstone_ratio` produced no spec, evaluate **`periodic`**: pick the oldest eligible contiguous sources by `file_creation_time`. Emit one `CompactionSpec` that is subject to the usual gates and validations.

**Rationale.** Size pressure relieves write stalls — the only user-visible failure mode that compaction can address directly — so size wins first. `max_age` runs before `tombstone_ratio` because `expire_ts` is a contract the application has made with its callers, and contract correctness trumps space optimization. `tombstone_ratio` runs before `periodic` because reducing read fanout has immediate user-facing benefit, while `periodic` is hygiene with no time-bound contract. The ordering chains: avoid write stalls → honor `expire_ts` → reclaim tombstone space → keep files fresh.

**Interaction across triggers.** Single-level rewrites lock only `S` and at most one neighbor SR under `ConflictChecker` (`size_tiered_compaction.rs:25-63`), so overlap with size picks is bounded. The round-robin loop (`size_tiered_compaction.rs:235-250`) continues to drive fairness across trees, and other size picks on the same tree (touching non-overlapping SRs) can still run in parallel up to `max_concurrent_compactions`.

### 6. Scheduler safeguards and cooldowns

The new pickers introduce no operator-facing tuning knob beyond the four trigger configurations themselves. All triggers emit single-level rewrites that compose with existing size-tiered safeguards rather than bypassing them. The following safeguards already live inside the scheduler:

- **`max_compaction_sources` bounds per-spec size.** Every trigger respects `clamp_max`, so no single firing produces a tree-spanning compaction. This is the load-bearing safeguard that lets the design work without subcompaction support.
- **`BackpressureChecker` still applies.** A single-level rewrite is structurally a normal SR → SR pick, so the existing check naturally guards against pathological output-size patterns.
- **Size clamping.** `CompactionSpec`s with too little or too many bytes are rejected, except for `tombstone_ratio` targeting the bottom-two SRs.
- **Per-tree round-robin fairness** unchanged from existing `propose()`. One pick per tree per pass means a single segment cannot monopolize `max_concurrent_compactions`.

These new cooldowns will be added for the triggers:

- **Refire cooldown on `max_age`.** The cooldown will be that the `file_creation_time` is older than `max_age`.
- **Minimum delete count on `tombstone_ratio`.** For an SR to be eligible, it must have at least N tombstones in it.

## Impact Analysis

SlateDB features and components that this RFC interacts with.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [x] Time to live (TTL) — `max_age` trigger turns existing `expire_ts` from best-effort-by-incidental-compaction into eventually-correct with a bounded latency knob.
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [x] Manifest format — only indirectly. `SsTableInfo` lives inside `CompactedSsTableV2`, so the new optional field rides along automatically. FlatBuffers tolerates added optional fields; no version bump.
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence — `CompactionSpec` shape is unchanged; the new triggers reuse the existing single-source primitive. No change to `compactions_store.rs`.
- [ ] Compaction filters
- [x] Compaction strategies and scheduling
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [x] SST format or block format — `SsTableInfo` gains an optional `SstCompactionStats` table.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc) — non-breaking config additions only.
- [x] Observability (metrics/logging/tracing) — new per-trigger counters.

## Operations

### Performance & Cost

- **Latency.** A small per-SST write overhead: the new `SstCompactionStats` table adds a fixed handful of bytes (three `ulong` counters, two `ulong` timestamps) to the SST footer / `SsTableInfo`. No WAL write-path impact — `SstCompactionStats` lives in SSTs only, so writes hit the WAL unchanged. Read path unaffected. Compactions themselves run the existing executor unchanged. The three new triggers add compaction frequency in steady state; the alternative — letting expired data and tombstones accumulate — has its own slow-burn costs.
- **Throughput.** Each `max_age` firing rewrites at most `max_compaction_sources` worth of bytes — bounded per spec. Steady-state write amp scales with `(DB size / max_age) × N` for tree depth `N`, because data must traverse `N` levels to reach the bottom. For a 100 GB database with a 7-day `max_age` and `N` ≈ 5 levels: ~70 GB/day of rewrite work in the worst case. That is similar in order of magnitude to a single-pass bottom-reaching design (same total bytes rewritten) but spread across many smaller compactions, which is the explicit trade for avoiding tree-spanning single jobs. `periodic`'s write amp is bounded by `(DB size / periodic_compaction_interval) × N` by the same argument.
- **Object-store request / cost.** Mirrors the added write amplification above. Per-spec size is bounded by `max_compaction_sources`; global parallelism by `max_concurrent_compactions`; per-tree fairness by the round-robin loop.
- **Space amplification.** `tombstone_ratio` directly reduces space amp at the level of the firing source. `max_age` reduces it indirectly by migrating expired entries (synthesized as tombstones) toward the bottom over multiple cycles. `periodic` provides a floor on staleness-driven amp by guaranteeing a rewrite cadence on cold data.
- **Read amplification.** `tombstone_ratio` reduces read fanout for delete-heavy regions by removing shadowed entries one level at a time. `max_age` produces synthesized tombstones that incrementally migrate down — bound on read fanout is "one extra tombstone per level still holding the entry," shrinking by one per `max_age` cycle until the entry is fully reclaimed.

### Observability

- **Configuration.** Three new fields on `CompactorOptions`:
  - `max_age: Option<Duration>` (default `None`).
  - `max_tombstone_ratio: Option<f32>` (default `None`).
  - `periodic_compaction_interval: Option<Duration>` (default `None`).
- **Metrics.**
  - `compaction.trigger.size.fires` (counter)
  - `compaction.trigger.max_age.fires` (counter)
  - `compaction.trigger.tombstone.fires` (counter)
  - `compaction.trigger.periodic.fires` (counter)
- **Logs.** Every emitted `CompactionSpec` gains a `trigger` label (`size`, `max_age`, `tombstone`, or `periodic`) so operators can attribute compaction load to its driver.

### Compatibility

- **Existing data on object storage.** Old SSTs lack `compaction_stats` → the scheduler treats them as ineligible for `max_age`, `tombstone_ratio`, and `periodic` triggers but they remain eligible for size. After a few size-driven compactions any live SST has been re-emitted with the field populated. No migration step required.
- **Existing public APIs (including bindings).** `CompactorOptions` additions are non-breaking — three new optional fields.
- **Rolling upgrades / mixed-version behavior.** Old reader + new writer: the new field is ignored (FlatBuffers tolerance). New reader + old writer: the reader sees `None` and evaluates only size triggers, falling back to today's behavior. Safe in both directions.

## Testing

- **Unit tests.**
  - `sst_builder` populates `compaction_stats` with correct counts and timestamps in the flush path (`oldest_ancestor_time == file_creation_time`).
  - Compaction-path builder computes `oldest_ancestor_time = min(inputs)`.
  - Scheduler picks the oldest eligible source by `oldest_ancestor_time` for `max_age` given a mocked clock.
  - Scheduler picks the oldest eligible source by `file_creation_time` for `periodic` given a mocked clock.
  - Scheduler picks the highest-ratio source for `tombstone_ratio` given handcrafted stats.
  - Trigger ordering: when all four triggers are eligible on the same tree, the emitted spec corresponds to size. When size is ineligible but the other three are eligible, the spec corresponds to `max_age`. And so on down the priority order.
  - All three triggers respect `clamp_max` and `BackpressureChecker`: a non-bottom firing on `S` with a saturated next-lower tier is rejected by `BackpressureChecker`, exactly like a normal size-tiered SR → SR pick.
  - `max_age` refire cooldown: a freshly-rewritten SR whose `file_creation_time` is within `max_age / 4` of `now` is *not* eligible for `max_age`, even when its `oldest_ancestor_time` qualifies.
  - `SizeTieredCompactionScheduler` still prioritizes L0 → SR compaction over other triggers.
- **Integration tests.**
  - Build a DB whose entries carry `expire_ts`, age it with the deterministic clock past `max_age`, and run a compaction, and verify that the data has tombstones now. Confirm expired entries are physically dropped within that bound.
  - Build a DB with a high delete ratio, confirm `tombstone_ratio` fires and bottom-two SRs can be compacted individually.
  - Build a DB with no `expire_ts` and no deletes, age it past `periodic_compaction_interval`, confirm `periodic` fires and rewrites cold SSTs at least once within the configured interval.
  - Mixed-mode test: alongside the three new triggers, drive size-pressure compactions and confirm round-robin fairness across trees.
- **Fault-injection / chaos tests.** No new failure modes introduced — the new triggers reuse the existing compaction pipeline. Existing chaos coverage applies unchanged.
- **Deterministic simulation tests.** N/A.
- **Formal methods verification.** N/A.
- **Performance tests.** None required for this change; the additions are scheduling-level decisions and reuse the existing hot path.

## Rollout

- Flatbuffers change
- Scheduler changes to support multiple compaction triggers
- Priority of triggers defined and testing
- `max_age` and `periodic` implementation
- `tombstone_ratio` implementation
- Expose settings in SlateDB configuration
- Documentation and metrics

## Alternatives

1. **Store the new fields on `SstStats` instead of as a new `SstCompactionStats` table.** Rejected because `SstStats` lives in a separately-fetched footer block. Scheduling decisions touch every SST in every tree per poll, and a per-SST object-store fetch is not acceptable on that path. Embedding a small fixed-size table on `SsTableInfo` keeps the data in the always-resident manifest path.

2. **Use ULID-embedded creation timestamps instead of an explicit field.** ULIDs encode a 48-bit Unix-millisecond timestamp that the codebase already extracts in `garbage_collector.rs:1408`. Rejected because (a) the data's `oldest_ancestor_time` is logically distinct from any single SST's ULID time and must be tracked separately, and (b) once we are persisting one timestamp explicitly, persisting the other for free avoids a reader-side hidden assumption that the SST ID encoding is the durable source of truth for the creation time.

3. **A single combined "compaction priority score" trigger** (one knob, scheduler computes a score from age and tombstone ratio). Rejected because the triggers serve different purposes — `max_age` is about `expire_ts` correctness, `tombstone_ratio` is about space efficiency, `periodic` is about file hygiene — and benefit from independent tuning. Combining them obscures the operator's intent and makes per-trigger metrics impossible.

4. **Single age trigger spanning both data-age and file-age.** Rejected because `max_age` and `periodic` answer different questions with very different recommended values (`max_age` ≈ longest `expire_ts`, often hours to a few days; `periodic` ≈ 90 days or longer for hygiene). A single cadence forces the operator to choose: short enough wastes rewrites on data that isn't expiring; long enough lets `expire_ts` entries outlive their horizon. They also key on different fields — only `oldest_ancestor_time` survives size-driven churn intact, so a periodic-on-file-age trigger could never substitute for `max_age`.

5. **Operator-facing `max_compaction_bytes` knob.** An earlier draft proposed clamping single-spec source bytes via a `max_compaction_bytes` setting, with an `oversized_emissions` counter to surface clamp events. Rejected because the load it tries to bound is already bounded by `max_concurrent_compactions`, clamping on number of sources, the per-tree round-robin, the `max_age` refire cooldown, and `ConflictChecker`-driven serialization on overlapping sources. Adding another tuning surface complicates operator decisions without measurably improving burst behavior.

## Open Questions

- **Bypassing `clamp_min`.** Size-based clamp is useful, but it hurts when you want `max_age` and `tombstone_ratio` to kick in more frequently. It's possible that the bottom SR isn't targeted by `tombstone_ratio` at all if `clamp_min=2` is respected. Should `max_age` also have a bypass? It could help enforce expiry more frequently.

- **Preventing `tombstone_ratio` from creating a compaction spec that will take too long.** It may be possible for a bottom SR to be large enough to exhaust the compactor and cause write backpressure.
- **Preventing `tombstone_ratio` from cascading.** A high-delete SR compacting could increase lower-SR tombstone ratios and make them immediately picked for compaction. Some type of safeguard here would be useful.

- **Should the `periodic` trigger be leveraged as a migration tool?** When `SstCompactionStats` is `None`, the `periodic` trigger could act as a migration tool. Right now the proposal is to do nothing.

- **Clock skew across writer restarts.** `oldest_ancestor_time` propagates from input SSTs across compaction generations, so a single skewed write — e.g. clock jumping forward briefly — could pollute long-lived ancestor times in ways that are hard to recover from. Mitigation: clamp `oldest_ancestor_time = min(file_creation_time, computed_min)` so it can never be in the future relative to the building clock. Worth confirming with reviewers; the implementation cost is small.

## References

- [RFC 0002: SlateDB Compaction](./0002-compaction.md) — original compaction design.
- [RFC 0024: Segment-Oriented Compaction](./0024-segment-oriented-compaction.md) — per-segment trigger evaluation; new triggers respect segment scoping unchanged.
- [RFC 0025: Distributed Compaction](./0025-distributed-compaction.md) — distributes whole `CompactionSpec` jobs across workers but does *not* introduce subcompactions. This is the reason all three new triggers emit single-level rewrites rather than RocksDB-style `PickCompactionToOldest` specs; without subcompactions, a tree-spanning compaction is a single-worker serial operation that would lock the lower tree for hours on multi-GB databases.
- [Issue #1598 — Leveled compaction](https://github.com/slatedb/slatedb/issues/1598) — the future consumer of per-SST `compaction_stats`.
- RocksDB `periodic_compaction_seconds` (file-age) and `ttl` (data-age) — the two-trigger split this RFC mirrors. RocksDB implements both via `PickCompactionToOldest`, which is safe there because subcompactions split the work across N workers; SlateDB intentionally diverges by using multi-cycle single-level rewrites until subcompactions exist.
- `slatedb/src/size_tiered_compaction.rs` — scheduler extension point.
- `slatedb/src/sst_builder.rs`, `slatedb/src/format/sst.rs` — SST format extension surface.
- `slatedb/src/retention_iterator.rs` — existing TTL/tombstone elision; the synthesis path at `retention_iterator.rs:108-132` is the mechanism the multi-cycle design relies on.

## Updates
