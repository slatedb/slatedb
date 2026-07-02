# Reachability-Based Garbage Collection

Status: Draft

Authors:

* [Pierre Barre](https://github.com/Barre)

## Summary

Compacted-SST GC currently deletes by a wall-clock cutoff (`min_age`,
`newest_l0_dt`, `compaction_low_watermark`) over SST ULID timestamps. The cutoff
is a proxy for reachability; it is unsound under clock skew, parallel-upload
ordering, publish stalls, and same-millisecond ties, yielding broken DBs and data loss.

This RFC deletes by reachability instead: an object is removed only if it is
unreachable from durable roots and below a durable producer fence, computed from
durable state plus an in-process read-pin registry, with no clock on the safety
path. Time is used only to reclaim abandoned pins (liveness), where the failure
mode is bounded over-retention, never loss. GC becomes safe at `min_age = 0`.

## Motivation

Current rule (`garbage_collector/compacted_gc.rs:199-219`):

```
cutoff = min(now - min_age, compaction_low_watermark, newest_l0_dt)
delete O  iff  ulid_ts(O) < cutoff  AND  O ∉ active_ssts
```

`active_ssts` (manifest + checkpoints) is sound. The cutoff is not.

| Mode | Cause | Result |
|------|-------|--------|
| F1/F5 | mint on a clock behind committed state (failover, remote worker) | new SST below cutoff → deleted |
| F3 | physical ULID minted in parallel uploader; publish is sequence-ordered | staged L0 timestamp below watermark |
| F4 | publish stall (head-of-line upload retry) exceeds `min_age` | staged L0 deleted, later publish dangles |
| #1707 | `retain_active_and_last_finished` selects by `max()` over full ULID | `compaction_low_watermark` mis-set at ms ties |

`min_age` for correctness: at `min_age = 0` this GC loses data.
A reachability GC loses nothing at retention 0.

## Goals

- GC never deletes an object that is referenced or that a live producer or
  consumer can still reference, proven by case analysis over an enumerated holder
  set, with no clock on the safety path.
- Cover every topology: embedded coordinator+worker, standalone coordinator,
  remote workers (RFC-0025), separate `DbReader`, leader→standby failover.
- Confine clock use to liveness (reclaiming abandoned pins).
- Reclaim all genuine garbage.
- No public API change.

## Non-Goals

- Replacing ULIDs as SST ids or object names. Their *ordering* is provided by
  RFC-0029.
- The compactor scheduling against inputs absent from the live manifest
  (producer-side validation; tracked separately).
- Multi-coordinator concurrency. Single-coordinator invariant preserved.

## Background — reference holders

Every entity that can hold or create a reference to a `compacted/` SST:

| # | Holder | Visible to GC via | Status today |
|---|--------|-------------------|--------------|
| R1 | Latest manifest (L0 + sorted runs) | manifest read | enforced |
| R2 | Checkpoints | `read_referenced_manifests` | enforced |
| R3 | In-flight compaction inputs | `.compactions` job `sources` | not unioned |
| R4 | In-flight compaction outputs (pre-commit) | `.compactions` `output_ssts` + fence | `min_age`/watermark |
| R5 | Writer staged L0 uploads (pre-publish) | producer fence | `min_age`/`newest_l0_dt` |
| R6 | In-process reads (`Db::get`/`scan`/`snapshot`) | in-process read-pin registry | `min_age` + 900s checkpoint |
| R7 | External reads (`DbReader`) | durable checkpoint (= R2) | enforced |
| R8 | Future participants | committed state only (= R1/R2) | enforced |

Gaps: R3 (cheap union), R4, R5 (fence), R6 (registry). R6 is unpinned today —
`db.rs:234` reads `self.state.read().view()` with no checkpoint; its only shields
are `min_age` (300s) and a 900s compactor checkpoint documented as a temporary
`min_age`-style hack (`compactor_state_protocols.rs:244-261`).

Completeness: five independent passes over the fork (reader call sites, long-lived
handle holders, non-flush producers, durable referrers, lifecycle windows) found
no holder outside R1–R8. The only findings were unbuilt mechanisms for R4/R5/R6.

## Design

### Invariant

> GC may delete `O` iff `O ∉ protected` and `O.id < gc_horizon`. Both operands
> derive from durable state and the GC process's read-pin registry. Neither reads
> wall-clock time.

### Protected set

```
protected =
      reachable(latest_manifest)                                    # R1
    ∪ ⋃_{cp ∈ live_checkpoints}        reachable(cp.manifest)       # R2, R7
    ∪ ⋃_{j ∈ active(.compactions)}     j.sources ∪ j.output_ssts    # R3, R4
    ∪ ⋃_{g ∈ in_process_read_pins}     reachable(g)                 # R6
```

`reachable(m)`: every tree's L0 ids + every sorted-run `sst_view` id, across the
unsegmented tree and all segments. `active`: `Submitted|Scheduled|Running|Compacted`.

### Producer fence — `gc_horizon`

Durable SST id in the manifest, monotonically non-decreasing. Contract: every id
`< gc_horizon` is accounted (reachable or genuine garbage); no in-flight producer
can publish a new reference below it. GC treats `O.id ≥ gc_horizon` as untouchable.

Advance (coordinator):

```
gc_horizon = max( persisted_gc_horizon,
                  min( writer_low, min_{j ∈ running} j.output_floor ) )
```

- `writer_low`: id of the writer's oldest staged-but-unpublished L0, else the
  allocator's next-mint position. In-process value.
- `j.output_floor`: allocator position captured when job `j` is Scheduled,
  persisted in `j`'s `.compactions` entry. By RFC-0029 the worker mints all of
  `j`'s outputs `≥ output_floor`. Hence every unrecorded output has
  `id ≥ output_floor ≥ gc_horizon`. No per-output durable write.

Monotonic by construction; clamped against regression by `persisted_gc_horizon`
on failover (RFC-0029 prevents minting below it).

### Consumers

- R7: external reads pin a checkpoint (`db_reader.rs`); covered by R2.
- R6: in-process reads register their `DbState` generation in an in-memory
  registry owned by the GC process. Register on read start, deregister on iterator
  drop. `protected` unions `reachable(g)` over live pins. RCU/epoch style; no
  durable write. Empty after restart (no inherited in-flight reads).

### Algorithm

```
for O in list(compacted/):
    if O.id < gc_horizon and O ∉ protected:
        delete O
```

### Safety

For any `O` referenced or able to become referenced:

| O ∈ | Kept by |
|-----|---------|
| R1, R2 | `O ∈ reachable(manifest/checkpoint)` ⊆ protected |
| R3, R4 recorded | `.compactions` union ⊆ protected |
| R4 unrecorded | `O.id ≥ output_floor ≥ gc_horizon` |
| R5 | `O.id ≥ writer_low ≥ gc_horizon` |
| R6 | `reachable(live pin)` ⊆ protected |
| R7 | checkpoint ⊆ protected |

Any `O` that is dereferenced, uncheckpointed, in no active job, in no live read,
and fully accounted satisfies `O.id < gc_horizon ∧ O ∉ protected` and is deleted.
GC deletes all and only genuine garbage. No clock consulted.

### Liveness — sole clock use

| Leak | Reclaimed by |
|------|--------------|
| Crashed external reader's checkpoint | ephemeral checkpoint TTL |
| Dead worker's job (holds fence down) | `worker_heartbeat_timeout` reclaim → `output_floor` drops |
| In-process read pin | iterator drop (no timer) |

Time decides when a leak is reclaimed, never whether live data survives.

### Failover

New coordinator reads `persisted_gc_horizon` (no regression), starts an empty
read-pin registry, re-derives `protected` from durable state. RFC-0029's skew
check blocks minting below the fence.

## Impact Analysis

### Metadata, Coordination, and Lifecycles
- [x] Manifest format — adds `gc_horizon`
- [x] Checkpoints — relied on for R2/R7 (no format change)
- [x] Garbage collection — predicate replaced

### Compaction
- [x] Compaction state persistence — `output_floor` per job
- [x] Distributed compaction — outputs minted `≥ output_floor` (RFC-0029)

### Consistency, Isolation, and Multi-Versioning
- [x] Snapshots — in-process read-pin registry (R6)

## Dependencies

- RFC-0029 (monotonic SST id allocator + activation skew check): cross-process id
  ordering and mint-above-floor. Hard dependency for R4/R5. Not yet authored.
- RFC-0026 (boundary files): metadata analogue of `gc_horizon`.
- RFC-0025 (`.compactions` schema): durable job records for R3/R4; extended with
  `output_floor`.

## Rollout

Each phase only enlarges `protected` or lowers deletions until phase 4.

1. Union `.compactions` `sources` + `output_ssts` into `protected`. Independent.
2. In-process read-pin registry (R6). No external dependency.
3. `gc_horizon` + `output_floor` + predicate `O.id < gc_horizon ∧ O ∉ protected`.
   Requires RFC-0029.
4. Remove `newest_l0_dt` / `compaction_low_watermark`; demote `min_age` to a
   non-critical knob.

## Alternatives

| Option | Rejected because |
|--------|------------------|
| Status quo / RFC-0029 only | keeps `min_age` for R5/R6; not complete |
| Larger `min_age` | does not eliminate the unbounded stall/skew windows |
| Per-object reservation before upload | a write per flush/output; fence + `output_floor` match it with none |

## Open Questions

1. Read-pin granularity: pin whole `DbState` generation (simple) vs per-read SST
   set (tighter reclamation). Start coarse.
2. Advance `gc_horizon` per manifest commit (tightest) vs a GC-fence tick.
3. ~~Clones~~ Resolved. Durable `final_checkpoint_id` (`lifetime: None`) lands in
   `create_clone_manifest` before `copy_wal_ssts` (clone.rs:53-64 precede :72).
   `external_dbs[].sst_ids` are pinned in the parent by that checkpoint (R2) and
   mirrored into the clone's trees by `prune_external_sst_ids`. The 300s ephemeral
   parent checkpoint covers only metadata construction. No fence change needed.
