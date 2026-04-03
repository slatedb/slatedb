# Distributed Compaction

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Ryan Dielhenn](https://github.com/ryandielhenn)

## Summary

<!-- Briefly explain what this RFC changes and why. Prefer 3–6 sentences. -->

SlateDB currently runs compaction on a single process. Compaction is either embedded in the DB writer or as a standalone daemon. This caps compaction throughput at one node's network bandwidth and CPU. This RFC extends the existing `.compactions` coordination file (RFC-0013) to separate a **Compaction Coordinator** (scheduler and manifest committer) from one or more **Compaction Workers** (stateless job executors). Workers poll `.compactions` for submitted jobs, claim them via CAS, execute the existing compaction code path, and report results back; the coordinator alone commits manifest updates, preserving the single-writer invariant. The design is backward-compatible with existing stateful and standalone compaction modes and adds no external dependencies beyond the object store.

## Motivation

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why "do nothing" is insufficient. -->

In write-heavy workloads, compaction is the bottleneck. When the rate of SST flushes exceeds the rate at which a single compactor can merge them, the L0 file count grows unbounded. SlateDB responds to excessive L0 depth with back-pressure that stalls writes, degrading the entire system. Scaling the DB writer or adding read replicas does nothing to relieve this.

Both existing compaction modes share the same single-node ceiling: in-process compaction competes with the write path for CPU and I/O, and the standalone compactor offloads compute but still cannot scale horizontally. The only way to break this ceiling is to distribute compaction execution across multiple workers. Since SlateDB already uses the object store as its sole coordination primitive, the `.compactions` file provides everything needed to schedule and claim work across processes. 


## Goals

- Increase compaction throughput by enabling horizontal scale-out across multiple worker processes, targeting roughly linear throughput scaling with worker count.
- Tolerate individual worker failures without losing compaction progress, by checkpointing output SSTs and reclaiming stalled jobs.
- Preserve the single-writer invariant: only the coordinator commits manifest updates.
- Add no external dependencies beyond the object store already required by SlateDB.
- Remain backward-compatible with standalone and in-process compaction modes; no migration required.

## Non-Goals

- **Changing the compaction scheduling strategy.** The scheduler logic is unchanged; only execution is distributed.
- **Peer-to-peer or leaderless compaction.** A dedicated coordinator is required; no peer election mechanism is introduced (see Alternatives).
- **Dynamic worker discovery.** Workers are registered by starting them; chitchat-based auto-discovery is deferred (see Rollout phase 4).
- **Sharding `.compactions` across multiple files.** CAS contention at very high worker counts (50+) is an open question, but multi-file sharding is out of scope for this RFC.
- **Multi-coordinator support.** The single-coordinator invariant is preserved; leader election across coordinators is a future concern.
- **Changes to the public read/write API.** Distributed compaction is transparent to DB clients.

## Design

### Architecture Overview

Separates the **Compaction Coordinator** (scheduler + manifest committer) from one or more **Compaction Workers** (stateless executors). The `.compactions` file from [RFC-0013](0013-compaction-state-persistence.md) is the coordination primitive. Primary additions: worker identity, a CAS-based claim protocol, and heartbeat-based failure detection.

```
+----------------------------------+
|      Compaction Coordinator      |
|   (single process, owns epoch)   |
|                                  |
|  +-------------+  +------------+ |
|  |  Scheduler  |  | State Mgmt | |
|  | (unchanged) |  | .manifest  | |
|  |             |  |.compactions| |
|  +------+------+  +------+-----+ |
+---------|----------------|------ +
          | writes specs    | commits results
          v                 v
   +--------------------------+
   |      Object Store        |
   |   .compactions file      |
   | (coordination interface) |
   +------------+-------------+
                | poll for work
      +---------+---------+
      v         v         v
  +--------+ +--------+ +--------+
  |Worker 1| |Worker 2| |Worker N|
  | (exec) | | (exec) | | (exec) |
  +--------+ +--------+ +--------+
```

- **Coordinator** — owns `compactor_epoch`, runs the scheduler, writes `CompactionSpec`s to `.compactions`, commits manifest updates. Scheduler logic is unchanged.
- **Workers** — poll `.compactions`, claim `Submitted` jobs via CAS, execute using the existing `execute_compaction_job` path, report completion. Workers do not touch the manifest or run the scheduler.

### Schema Changes

Extend the `Compaction` table in `compactor.fbs`:

```fbs
table Compaction {
    // Existing fields (unchanged)
    id: Ulid (required);
    spec: CompactionSpec (required);
    status: CompactionStatus;
    output_ssts: [CompactedSsTable];

    // NEW
    worker_id: string;          // empty = unclaimed
    last_heartbeat_ms: uint64;  // wall-clock ms of last progress write
}
```

Both fields are optional (default: `""`, `0`); existing `.compactions` files require no migration.

### Work Claim Protocol

Workers use optimistic concurrency (CAS) on `.compactions`:

1. Poll `.compactions` every `worker_poll_interval_ms`.
2. Find a `Compaction` with `status == Submitted` and empty `worker_id`.
3. CAS-write: `status = Running`, `worker_id = <self>`, `last_heartbeat_ms = now()`.
4. On CAS success: begin execution. On CAS failure: re-read and retry from step 2.

Workers claim one job at a time to minimize contention.

### Heartbeat and Failure Detection

Workers update `last_heartbeat_ms` each time an output SST is written (piggy-backing on the RFC-0013 progress-persistence writes, roughly every ~256MB).

The coordinator detects stale workers during its periodic poll: for each `Running` compaction where `now() - last_heartbeat_ms > worker_heartbeat_timeout_ms`, reset `status = Submitted` and clear `worker_id`. The reclaimed compaction retains its `output_ssts`, so the next worker resumes from the last checkpoint via `ResumingIterator`.

### Worker Lifecycle

1. **Start** — generate a ULID `worker_id`, load config.
2. **Poll** — read `.compactions` for `Submitted` work.
3. **Claim** — CAS transition to `Running` (see claim protocol).
4. **Execute** — run `execute_compaction_job`: build iterators from `CompactionSpec`, apply filters/merge ops, write output SSTs to `compacted/`, persist progress at each SST boundary.
5. **Complete** — write `status = Completed` with final `output_ssts` to `.compactions`.
6. **Loop** — return to step 2.

### Manifest Commit Protocol

Only the coordinator commits manifest updates (preserves single-writer invariant):

1. Observe a `Completed` compaction in `.compactions`.
2. Validate all output SSTs exist in the object store.
3. Update the manifest: remove source SRs/SSTs, insert output SR, update `l0_last_compacted`.
4. Trim the finished compaction from `.compactions` (per RFC-0013 GC conventions).

If the coordinator crashes between steps 3 and 4, the `Completed` entry is trimmed on the next cycle with no further action needed.

### Backward Compatibility: Existing Modes

The existing compaction modes continue to work unchanged:

| Mode | Description | Changes |
|------|-------------|---------|
| **Stateful** (in-process) | Coordinator + single worker in the same process as the DB writer | None |
| **Standalone** | Coordinator + single worker as a separate process | None |
| **Distributed** | Coordinator + N workers as separate processes |  Coordinator delegates execution to remote workers |

The coordinator can optionally execute compactions itself (acting as both coordinator and worker), so a single-node deployment has zero overhead.

### Configuration

```rust
pub struct DistributedCompactionOptions {
    pub worker_poll_interval_ms: u64,
    pub worker_heartbeat_timeout_ms: u64,
    pub max_workers: Option<u32>,
}
```

New `CompactorWorkerBuilder` entrypoint for worker processes:

```rust
let worker = CompactorWorkerBuilder::new("/path/to/db", object_store.clone())
    .with_poll_interval_ms(5000)
    .build()
    .await?;

worker.run().await?;
```

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

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

- [x] Manifest format — coordinator-only manifest commits after worker completion
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection — multiple workers producing SSTs requires GC awareness of in-flight distributed work
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence — extends `.compactions` schema with `worker_id`, `last_heartbeat_ms`
- [ ] Compaction filters
- [ ] Compaction strategies
- [x] Distributed compaction — this RFC
- [x] Compactions format — extends FlatBuffer schema with new fields

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing) — per-worker metrics, coordinator dashboards

## Operations

### Performance & Cost

- **Latency**: No change to read/write latency. Compaction latency decreases with more workers.
- **Throughput**: Scales roughly linearly with worker count, bounded by per-worker object store bandwidth.
- **Object-store requests**: ~1 GET per poll interval + ~1 PUT per claim + ~1 PUT per output SST. At N=10 workers polling every 5s: ~120 GETs/min overhead.
- **Space/write/read amplification**: Unchanged. Concurrent workers may create slightly more transient in-flight space, bounded by `max_compactions`.

### Observability

New metrics:
- `slatedb_compaction_workers_active` — active worker gauge (coordinator)
- `slatedb_compaction_jobs_claimed_total` — per-worker counter
- `slatedb_compaction_jobs_reclaimed_total` — stale worker reclaims (coordinator)
- `slatedb_compaction_claim_conflicts_total` — CAS failures (worker)

Existing `bytes_processed`, `ssts_written`, `job_duration_seconds` metrics apply per-worker unchanged. Worker lifecycle events logged at INFO; CAS conflicts at DEBUG.

### Compatibility

- **Object storage**: Backward compatible. New fields default to unclaimed/0; no migration needed.
- **Public API**: DB read/write API and `CompactorBuilder` unchanged. `CompactorWorkerBuilder` is additive.
- **Rolling upgrades**: Upgrade coordinator first, then start workers. Old standalone compactors safely ignore new fields.

## Testing

- **Unit**: CAS claim (success/conflict/retry), heartbeat timeout and reclamation, manifest commit, simultaneous claims from N workers.
- **Integration**: Coordinator + N workers against in-memory object store; data integrity with compaction filters.
- **Fault-injection**: Worker crash mid-compaction (timeout → reclaim → resume), coordinator crash, object store failures during CAS.
- **Simulation**: N workers + 1 coordinator with injected latency/failures; verify no lost compactions, no duplicate manifest commits.
- **Performance**: Throughput scaling 1→N workers; CAS contention at high worker counts; end-to-end write throughput comparison.

## Rollout

Phases:
1. **Schema extension** — add `worker_id` and `last_heartbeat_ms` to `compactor.fbs`; no behavior change.
2. **Worker implementation** — implement `CompactorWorkerBuilder`; coordinator defers execution to remote workers when present.
3. **Failure detection** — heartbeat timeout and reclamation on the coordinator; resume via `ResumingIterator`.
4. **Discovery (optional)** — chitchat integration for dynamic worker discovery (Kubernetes etc.); deferrable.

Distributed mode is opt-in: users explicitly start worker processes. No feature flag needed — the presence of workers is the signal. Docs needed: deployment guide (K8s, docker-compose, bare metal) and API docs for `CompactorWorkerBuilder`/`DistributedCompactionOptions`.

## Alternatives

### Status quo (single compactor)

Compaction throughput stays bounded by one node. Rejected — can't meet the scaling goal.

### Peer-to-peer leader election via object store

All compactors are peers; CAS elects a leader to run the scheduler. Adds complexity around leader transitions and scheduler handoff. Could be a future evolution.

### chitchat for work distribution/discovery

Use gossip to distribute jobs directly. Rejected — couples correctness to gossip consistency; object store remains the source of truth. chitchat is better as an optional discovery/health layer.

## Open Questions

- Should the coordinator also act as a worker by default, or be a pure scheduler? Acting as a worker simplifies single-node deployments but adds load to the coordinator process.
- What is the right default for `worker_poll_interval_ms`? Should it be adaptive (e.g. exponential backoff when no work is available)?
- How should GC handle the window between a worker writing output SSTs and the coordinator committing the manifest? GC must not delete SSTs that are not yet manifest-referenced.
- Should workers validate their `CompactionSpec` against the current manifest before executing? Validating catches stale specs but adds a manifest read per claim.
- Is CAS-based claiming sufficient at high worker counts (50+), or will contention require sharding across multiple `.compactions` files?
- Should chitchat discovery be part of the initial implementation or deferred?

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [SlateDB Compaction documentation](https://slatedb.io/docs/design/compaction/)
- [chitchat documentation](https://docs.rs/chitchat/0.10.0/chitchat/)
- [RFC-0002: SlateDB Compaction](0002-compaction.md) — original compaction design, includes "Looking Ahead" section on distributed compaction
- [RFC-0013: Compaction State Persistence](0013-compaction-state-persistence.md) — `.compactions` file design, external process integration, resume support
- [RFC-0017: Compaction Filters](0017-compaction-filters.md) — compaction filter integration that workers must support
- [RFC-0001: Manifest](0001-manifest.md) — manifest format and CAS protocol

## Updates

Log major changes to this RFC over time (optional).
