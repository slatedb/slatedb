# Manifest-Diff Garbage Collection

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Manifest-Diff Garbage Collection](#manifest-diff-garbage-collection)
  - [Summary](#summary)
  - [Background](#background)
  - [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Design](#design)
    - [Two Tiers of GC](#two-tiers-of-gc)
    - [The GC Cursor](#the-gc-cursor)
    - [Tier 1: The Diff Algorithm](#tier-1-the-diff-algorithm)
    - [Coordinating With Manifest Retention](#coordinating-with-manifest-retention)
    - [Optional: Persisting the Diff in the Manifest](#optional-persisting-the-diff-in-the-manifest)
    - [Relationship to the GC Boundary RFC](#relationship-to-the-gc-boundary-rfc)
  - [Impact Analysis](#impact-analysis)
    - [Core API \& Query Semantics](#core-api--query-semantics)
    - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
    - [Time, Retention, and Derived State](#time-retention-and-derived-state)
    - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
    - [Compaction](#compaction)
    - [Storage Engine Internals](#storage-engine-internals)
    - [Ecosystem \& Operations](#ecosystem--operations)
  - [Operations](#operations)
    - [Performance \& Cost](#performance--cost)
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

* [AsukaMilet](https://github.com/KarinaMilet)

## Summary

SlateDB's garbage collector finds deletable SSTs by listing the entire `compacted/` prefix on every run, then deleting objects that are old enough and no longer referenced by any manifest. Listing cost is proportional to the total number of objects in the bucket, not to the garbage actually produced, and the default schedule (GC enabled, 60s interval) runs it roughly **1,440 times per day**.

This RFC keeps the full listing as a rare safety sweep and adds a cheap, frequent path that finds garbage by diffing manifest versions instead of listing. A compacted SST can only become garbage when a compaction commits a manifest that drops it from the live set, so "what just became garbage" is derivable from changes of manifest history, with no `LIST`. A small durable cursor records how far this cheap path has processed.

The change is additive and self-contained: it does not alter the manifest format, changes do not affect correctness, and reuses the existing deletion-safety rules.

## Background

GC today is several independent tasks (manifest, WAL, compacted SST, compactions, detach), each on its own ticker. This RFC is about the **compacted SST** task (`compacted_gc.rs`), with a smaller follow-on for the WAL task.

The compacted SST task, per run:

1. Read the compaction low watermark (from the `compactions` files).
2. Read the latest manifest plus every manifest referenced by an active checkpoint, and union their referenced SSTs into the **live set**.
3. Compute a `cutoff` time as the minimum of three watermarks (below).
4. **List the whole `compacted/` prefix.**
5. Keep objects older than `cutoff` and not in the live set.
6. Delete them.

Three facts about SlateDB make a diff-based approach possible:

- **The live set is fully described by the manifests.** Step 2 already builds it by walking `manifest.core.trees()` (L0 + compacted sorted runs). Nothing outside the manifests is live.
- **Manifests are full snapshots with monotonic `u64` IDs**

- **Compacted SST IDs are ULIDs**, so age is embedded in the ID; GC reads it straight from the ID and never needs the object store's `last_modified`.

The three watermarks that form the deletion `cutoff` are the safety core of GC and are unchanged by this RFC:

- **`configured_min_age`** — the user's minimum retention (`now - min_age`).
- **`compaction_low_watermark`** — the earliest start time of any running or recently finished compaction, protecting the window where a compaction has written output SSTs but not yet committed the manifest referencing them.
- **`newest_l0`** — the timestamp of the newest L0 SST, protecting the window where an L0 has been flushed but its manifest entry not yet written.

These watermarks plus the checkpoint-referenced live set are what make deletion safe.
This RFC changes only **how the candidate set is produced** (a diff instead of a `LIST`).

## Motivation

The whole cost is step 4, the listing:

- **`LIST` is not free.** S3's `ListObjectsV2` returns at most 1,000 keys per page, is billed per request, and costs 10–100 ms per round trip; a million-SST bucket takes a thousand requests and minutes to enumerate.
- **Cost scales with total objects, not garbage produced.** In steady state almost all SSTs are live, yet every run re-enumerates them all to find the small fraction that died since last time.
- **The default schedule multiplies this ~1,440×/day.** Shortening the interval to delete dead objects sooner only makes listing cost worse.

## Goals

- Make the common GC path cost proportional to the number of changes since the last run, not to the total number of objects in the bucket.
- Keep a full-listing sweep as a rare backstop for objects the cheap path can't see.
- Reuse the existing deletion-safety rules (the three watermarks and the checkpoint-aware live set).
- Change no default that affects correctness, and add no required manifest-format change.

## Non-Goals

- Replacing or removing the full-listing path; it remains the safety net for orphaned objects (written but never committed to a manifest because a process crashed between the two steps).
- Redesigning the manifest into a delta/log format. A persisted per-version diff is an **optional** later optimization, not a requirement.
- Changing the deletion rules, watermarks, or `min_age` semantics.
- Compacted SST write fencing or sequenced-metadata safety — the GC Boundary RFC's subject; this RFC only consumes its results where convenient.

## Design

### Two Tiers of GC

Every comparable system splits GC into two tiers; this RFC adopts the same split:

```
                  ┌──────────────────────────────────────────────┐
   frequent ────► │ Tier 1 — Diff GC (new)                        │
   (e.g. 60s)     │   diff the manifests in (cursor, latest]      │
                  │   apply the existing cutoff + live-set filter │
                  │   delete, then advance the cursor             │
                  │   never lists the object store                │
                  └──────────────────────────────────────────────┘
                  ┌──────────────────────────────────────────────┐
   rare ────────► │ Tier 2 — Full Sweep (today's compacted_gc)    │
   (e.g. 6h)      │   LIST compacted/, same cutoff + live filter  │
                  │   collects orphans and anything Tier 1 missed │
                  └──────────────────────────────────────────────┘
```

Both tiers apply the **same** eligibility test; they differ only in how candidates are discovered (manifest diff vs. listing) and how often they run. Today's `compacted_gc` *is* Tier 2; this RFC adds Tier 1 in front and lowers Tier 2's frequency.

Tier 2 is still needed because an SST can be written and never committed to any manifest if the compactor or flush crashes between writing the object and the manifest. Such an **orphan** is invisible to a manifest diff — no version ever referenced it — so only a full listing finds it.

### The GC Cursor

Tier 1 must remember how far it has processed. We add one small durable object,
following the existing per-namespace `gc/<name>.boundary` convention:

- Path: `gc/compacted.cursor`. Contents: a single ASCII-encoded `u64` — the highest manifest version Tier 1 has fully processed.
- A missing cursor reads as `0` (the first run degrades to one Tier 2-style pass, then establishes the cursor).
- Updates are **monotonic** and conditional (compare-and-set on the object's version).
The cursor advances only after every candidate in a window deletes successfully; `NotFound` counts as success (delete is idempotent). On partial failure it does not advance, and the window is reprocessed next run — safe, because reprocessing already-deleted objects is a no-op.
- The cursor read must **not** be served from a stale cache (no `CachedObjectStore`).

**Why compacted GC needs a persisted cursor** 
Compacted SSTs are sparse ULIDs with no invariant and no cheap probe to reconstruct "what was already garbage collected," so Tier 1 must persist progress explicitly.

### Tier 1: The Diff Algorithm

```text
F = read_gc_cursor()                       # u64, 0 if missing
M = read_latest_manifest().id
if F >= M: return                          # nothing new; this run is ~free

# 1) Every SST referenced anywhere in the window (cursor, latest].
seen = {}
for v in (F .. M]:
    mv = read_manifest(v)                  # small object; read_manifest already exists
    seen |= referenced_ssts(mv)            # reuse collect_active_ssts on one manifest

# 2) The current live set, including checkpoints — the existing path, unchanged.
live = referenced_ssts(read_referenced_manifests(M))

# 3) Candidates = referenced sometime in the window, but not live now.
candidates = seen - live

# 4) The SAME cutoff filter GC uses today.
cutoff = min(configured_min_age, compaction_low_watermark, newest_l0)
to_delete = [id for id in candidates
             if id.timestamp() < cutoff and id not in live]

# 5) Delete (batched).
delete_ssts(to_delete)

# 6) Advance the cursor only if every delete succeeded.
advance_gc_cursor(M)
```

### Coordinating With Manifest Retention

Tier 1 reads the manifests in `(F, M]`, but the manifest GC task deletes old manifests, so part of the window may already be gone. The rule: **the cursor `F` must never fall behind the oldest manifest that still exists.**

- If manifests at or below some version in the window are gone (the window is "broken"), Tier 1 does not read them. It resets `F` forward to the oldest manifest still present and processes exist manifests; stale SSTs referenced by missing manifest are left for Tier 2.
- Skipping the broken span is always safe. The worst that happens is some garbage sticks around a bit longer until the next full sweep picks it up. Skipping can never cause Tier 1 to delete something it shouldn't.

How "the oldest manifest still present" is determined is covered in the next section.

### Optional: Persisting the Diff in the Manifest

When a compactor or flush commits a manifest, it already computes the new tree from the old one, so it already knows which SSTs were dropped. That could be recorded as an additive manifest field (e.g. `obsolete_ssts`) so Tier 1 reads it directly instead of recomputing from snapshots.

- **Benefit:** Tier 1 reads and unions one field per manifest, with no snapshot recomputation, and no longer depends on intermediate manifests surviving (the field
travels with each manifest).
- **Cost:** it touches the manifest write path (compactor and L0 flush) and adds a format field. The field is additive and backward-compatible — older readers ignore it, and when absent Tier 1 falls back to the snapshot diff.

This is explicitly a **later, optional** optimization. The core of this RFC (the snapshot diff) requires no manifest-format change.

### Relationship to the GC Boundary RFC
The two RFCs are complementary: boundaries protect *write* safety for sequenced metadata; this RFC speeds up *deletion* of compacted SSTs. 
Because the shared primitive already exists, this RFC depends on **nothing unmerged** and degrades gracefully where the boundary work is incomplete:

| Concern | What this RFC does today |
|---|---|
| **GC cursor storage** | Reuse the existing `ObjectStoreBoundaryObject` — just create one named `compacted.cursor`. We only move it forward (`advance`) and never use it to reject writes (`check`), so there's nothing new to build. |
| **"Oldest manifest" gate** | Use the merged `manifest.boundary` as the authoritative gate: require `cursor >= manifest.boundary`; if behind, reset the cursor to the boundary and let Tier 2 sweep the gap. |

## Impact Analysis

SlateDB features and components that this RFC interacts with:

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

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format <!-- only if the optional obsolete_ssts field is adopted -->
- [x] Checkpoints
- [x] Garbage collection
- [ ] Clones
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Object-store requests:** 
  the common path drops from one full `compacted/` listing per run $(O(total\ objects))$ to a handful of small manifest `GET`s $(O(changes))$. Full listings fall from ~1,440/day to the Tier 2 interval (~4/day at 6h).
- **Latency / throughput:** 
  unaffected — GC runs in the background and this RFC touches
  neither read nor write path. (The optional manifest field adds a trivial write-time computation the compactor already performs.)
- **Space amplification:** 
  Tier 1 deletes garbage as promptly as the unchanged `min_age`/watermark rules allow, and can run more often — cheaply, now that it does not list.
- **New durable objects:** 
  one tiny `gc/compacted.cursor`. Negligible space.

### Observability

- **Configuration:** 
  add one new setting, `full_sweep_interval`, for how often the rare Tier 2 full sweep runs (default e.g. 6h). The existing `interval` now controls how often the cheap Tier 1 diff runs. `min_age` and `dry_run` keep their current meaning and apply to both tiers. The defaults are picked so the two tiers together are at least as safe as GC is today.
- **Metrics:** 
  `gc_diff_deleted`, `gc_full_sweep_deleted`,
  `gc_diff_manifests_read`, `gc_cursor_lag` (latest manifest ID minus cursor).
- **Logging:** 
  distinguish Tier 1 vs. Tier 2 runs; warn on window-breakage fallback and cursor-advance failures.

### Compatibility

- **On-disk format:** additive. A missing cursor reads as `0`. The optional manifest field is backward-compatible (older readers ignore it; Tier 1 falls back to the snapshot diff when absent).
- **Public APIs / bindings:** none affected.
- **Rolling upgrades / mixed versions:** safe. An old binary just keeps doing full sweeps; a new one adds Tier 1. 

## Testing

- **Unit tests:** reuse the existing `compacted_gc` watermark/checkpoint tests, asserting Tier 1 produces the same deletions as a full sweep for the same state. Add cursor monotonicity and idempotent-reprocessing tests.
- **Integration tests:** a short-lived SST born and dropped inside one window is collected by Tier 1; an orphan is collected only by Tier 2; a broken window resets the cursor and Tier 2 cleans the gap.
- **Fault-injection/chaos:** delete failures mid-batch leave the cursor un-advanced and the next run completes cleanly.

## Rollout

None

## Alternatives


- **Just lower the GC frequency.** Rejected: it increases dead-object lifetime and does not address the O(total) cost — it only moves the slider.
- **Reference-count SSTs in memory (Pebble `obsoleteTables`/`zombieObjects`, RocksDB
  version refcounts).** Rejected for SlateDB: those engines run GC inside the process holding the live version set, whereas SlateDB's GC can run as a separate process with no in-memory version state. A durable manifest-derived cursor fits; in-memory refcounts do not.
- **Persisted manifest deltas as a hard requirement (≈ Hummock's `HummockVersionDelta`).** Rejected as a *requirement* because it forces an up-front format change; kept as an [optional later optimization](#optional-persisting-the-diff-in-the-manifest).

## Open Questions

- Default for `full_sweep_interval` (Tier 2). 6h is a starting proposal; the right value depends on the observed orphan rate, which Tier 2 metrics will reveal.
- Should Tier 2 run opportunistically (e.g. on the first run after start, when the cursor is `0` or far behind), independent of its interval?

## References

- [slatedb/slatedb#1714: batch delete for GC][issue-1714]
- [slatedb/slatedb#1638: exponential HEAD probe + binary search for `last_seen_wal_id`][issue-1638]
- [GC Boundary RFC: Garbage Collector Boundary Files for Sequenced Metadata][rfc-boundary]

[issue-1714]: https://github.com/slatedb/slatedb/issues/1714
[issue-1638]: https://github.com/slatedb/slatedb/pull/1638
[rfc-boundary]: ./0026-garbage-collector-boundary.md

## Updates

None
