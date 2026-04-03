# Distributed Compaction

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [Architecture Overview](#architecture-overview)
   * [Configuration](#configuration)
      + [Coordinator](#coordinator)
      + [Workers](#workers)
   * [Schema Changes](#schema-changes)
   * [Work Claim Protocol](#work-claim-protocol)
   * [Heartbeat and Failure Detection](#heartbeat-and-failure-detection)
   * [Worker Lifecycle](#worker-lifecycle)
   * [Manifest Commit Protocol](#manifest-commit-protocol)
   * [Backward Compatibility: Existing Modes](#backward-compatibility-existing-modes)
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
   * [Implementation](#implementation)
   * [Docs Updates](#docs-updates)
- [Alternatives](#alternatives)
   * [Status quo (single compactor)](#status-quo-single-compactor)
   * [Peer-to-peer leader election via object store](#peer-to-peer-leader-election-via-object-store)
   * [chitchat for work distribution/discovery](#chitchat-for-work-distributiondiscovery)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Ryan Dielhenn](https://github.com/ryandielhenn)

## Summary

SlateDB currently runs compaction on a single process. Compaction is either embedded in the DB writer or as a standalone daemon. This caps compaction throughput at one node's network bandwidth and CPU. This RFC extends the existing `.compactions` coordination file (RFC-0013) to separate a **Compaction Coordinator** (scheduler and manifest committer) from one or more **Compaction Workers** (stateless job executors). Workers poll `.compactions` for submitted jobs, claim them via optimistic concurrency (create-if-not-exists on numbered files), execute the existing compaction code path, and report results back; the coordinator alone commits manifest updates, preserving the single-writer invariant. The design is backward-compatible with existing stateful and standalone compaction modes and adds no external dependencies beyond the object store.

## Motivation

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
- **Sharding `.compactions` across multiple files.** Write contention at very high worker counts (50+) is an open question, but multi-file sharding is out of scope for this RFC.
- **Multi-coordinator support.** The single-coordinator invariant is preserved; leader election across coordinators is a future concern.
- **Changes to the public read/write API.** Distributed compaction is transparent to DB clients.

## Design

### Architecture Overview

Separates the **Compaction Coordinator** (scheduler + manifest committer) from one or more **Compaction Workers** (stateless executors). The `.compactions` file from [RFC-0013](0013-compaction-state-persistence.md) is the coordination primitive. Primary additions: worker identity, an optimistic claim protocol, and heartbeat-based failure detection.

```
+----------------------------------+
|      Compaction Coordinator      |
|   (single process, owns epoch)   |
|                                  |
|  +-------------+  +------------+ |
|  |  Scheduler  |  | State Mgmt | |
|  | (unchanged) |  |            | |
|  +------+------+  +------+-----+ |
+---------|----------------|------ +
          |                |
          | write specs    | read Completed, commit to .manifest
          v                v
   +--------------------+ +--------------------+
   |   Object Store     | |   Object Store     |
   |   .compactions     | |   .manifest        |
   +--------+-----------+ +--------------------+
            ^
            |
            | poll (worker read) 
            | claim / heartbeat / complete (worker write)
            |
      +-----+-----+-----+
      v       v         v
  +--------+ +--------+ +--------+
  |Worker 1| |Worker 2| |Worker N|
  | (exec) | | (exec) | | (exec) |
  +--------+ +--------+ +--------+
```

- **Coordinator** — owns `compactor_epoch`, runs the scheduler, writes `CompactionSpec`s to `.compactions`, commits manifest updates. Scheduler logic is unchanged.
- **Workers** — poll `.compactions`, claim `Submitted` jobs via optimistic concurrency, execute using the existing `execute_compaction_job` path, report completion. Workers do not touch the manifest or run the scheduler.

### Configuration

#### Coordinator

Distributed mode is opt-in via a `mode` field on `CompactorOptions`, configurable via `CompactorBuilder` or settings (TOML/JSON/YAML):

```rust
pub enum CompactorMode {
    /// Default. Coordinator executes jobs locally via TokioCompactionExecutor.
    Local,
    /// Coordinator writes Submitted jobs to `.compactions` and waits for remote workers
    /// to claim and execute them via RemoteCompactionExecutor.
    Distributed,
}

pub struct CompactorOptions {
    // ... existing fields unchanged ...

    pub mode: CompactorMode,
    /// How long before a worker with no heartbeat is considered stale and its job reclaimed.
    /// Only used in Distributed mode.
    pub worker_heartbeat_timeout_ms: u64,
    /// Maximum number of concurrent workers. None means unlimited.
    /// Only used in Distributed mode.
    pub max_workers: Option<u32>,
}
```

Via settings:

```toml
[compactor_options]
mode = "distributed"
worker_heartbeat_timeout_ms = 30000
max_workers = 10
```

When `mode = "distributed"`, the coordinator uses `RemoteCompactionExecutor` instead of `TokioCompactionExecutor`. Both implement the `CompactionExecutor` trait and the coordinator calls `executor.start_compaction_job()` the same way regardless of mode. `RemoteCompactionExecutor` writes a `Submitted` job to `.compactions` and polls for `Completed` rather than spawning a local task.

#### Workers

Workers don't use `CompactorOptions`. The primary setting is `worker_poll_interval_ms`, which controls how often a worker checks `.compactions` for new jobs.

New `CompactorWorkerBuilder` entrypoint for worker processes:

```rust
let worker = CompactorWorkerBuilder::new("/path/to/db", object_store.clone())
    .with_poll_interval_ms(5000)
    .build()
    .await?;

worker.run().await?;
```

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

Workers use optimistic concurrency on `.compactions`. SlateDB implements this uniformly across all object stores, including those with native CAS support, using create-if-not-exists on sequentially-numbered files (e.g. `00000000000000000003.compactions`). Writing a new version means writing the next numbered file; if another writer got there first the write fails with `AlreadyExists` and the worker retries. See [RFC-0001](0001-manifest.md) for the full protocol.

1. Poll `.compactions` every `worker_poll_interval_ms`.
2. Find a `Compaction` with `status == Submitted` and empty `worker_id`.
3. Write the full updated state with `status = Running`, `worker_id = <self>`, `last_heartbeat_ms = now()` to the next sequence number.
4. On success: begin execution. On `AlreadyExists`: re-read latest and retry from step 2.

Workers claim one job at a time to minimize contention.

### Heartbeat and Failure Detection

Workers update `last_heartbeat_ms` each time an output SST is written (piggy-backing on the RFC-0013 progress-persistence writes, roughly every ~256MB).

The coordinator detects stale workers during its periodic poll: for each `Running` compaction where `now() - last_heartbeat_ms > worker_heartbeat_timeout_ms`, reset `status = Submitted` and clear `worker_id`. The reclaimed compaction retains its `output_ssts`, so the next worker resumes from the last checkpoint via `ResumingIterator` (seeks input iterators past the last written key, avoiding re-processing already compacted data).

### Worker Lifecycle

1. **Start** — generate a ULID `worker_id`, load config.
2. **Poll** — read `.compactions` for `Submitted` work.
3. **Claim** — optimistic transition to `Running` (see claim protocol).
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
| **Distributed** | Coordinator + N workers as separate processes | Coordinator delegates execution to remote workers |

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
- [x] Observability (metrics/logging/tracing) — coordinator metrics

## Operations

### Performance & Cost

- **Latency**: No change to read/write latency. Compaction latency decreases with more workers.
- **Throughput**: Scales roughly linearly with worker count, bounded by per-worker object store bandwidth.
- **Object-store requests**: ~1 GET per poll interval + ~1 PUT per claim + ~1 PUT per output SST. At N=10 workers polling every 5s: ~120 GETs/min overhead.
- **Space/write/read amplification**: Unchanged.

### Observability

New metrics, all tracked by the coordinator from `.compactions` state:
- `slatedb_compaction_jobs_running` — count of jobs currently in `Running` state
- `slatedb_compaction_jobs_claimed_total` — incremented when the coordinator observes a `Submitted` → `Running` transition
- `slatedb_compaction_jobs_reclaimed_total` — incremented when the coordinator resets a stale job back to `Submitted`

Worker lifecycle events logged at INFO.

### Compatibility

- **Object storage**: Backward compatible. New fields default to unclaimed/0; no migration needed.
- **Public API**: DB read/write API and `CompactorBuilder` unchanged. `CompactorWorkerBuilder` is additive.
- **Rolling upgrades**: Upgrade coordinator first, then start workers. Old standalone compactors safely ignore new fields.

## Testing

- **Unit**: Claim (success/conflict/retry), heartbeat timeout and reclamation, manifest commit, simultaneous claims from N workers.
- **Integration**: Coordinator + N workers against in-memory object store; data integrity with compaction filters.
- **Fault-injection**: Worker crash mid-compaction (timeout → reclaim → resume), coordinator crash, object store failures during claim writes.
- **Simulation**: N workers + 1 coordinator with injected latency/failures; verify no lost compactions, no duplicate manifest commits.
- **Performance**: Throughput scaling 1→N workers; claim contention at high worker counts; end-to-end write throughput comparison.

## Rollout

### Implementation

Phases:
1. **Schema extension** — add `worker_id` and `last_heartbeat_ms` to `compactor.fbs`; no behavior change.
2. **Worker implementation** — implement `CompactorWorkerBuilder` and `RemoteCompactionExecutor`; coordinator uses `RemoteCompactionExecutor` when `mode = "distributed"`.
3. **Failure detection** — heartbeat timeout and reclamation on the coordinator; resume via `ResumingIterator`.

Distributed mode is opt-in via `mode = "distributed"` in `CompactorOptions`.

### Docs Updates

- Add examples to API documentation.
- Update compaction documentation to describe how to run distributed compaction.

## Alternatives

### Status quo (single compactor)

Compaction throughput stays bounded by one node. Rejected: can't meet the scaling goal.

### Peer-to-peer leader election via object store

All compactors are peers; optimistic concurrency on a numbered file elects a leader to run the scheduler. Rejected: Adds complexity around leader transitions and scheduler handoff. Could be a future evolution.

### chitchat for work distribution/discovery

Use gossip to distribute jobs directly. Rejected: couples correctness to gossip consistency; chitchat is better as an optional discovery/health layer.

## Open Questions

- Should the coordinator also act as a worker by default, or be a pure scheduler? Acting as a worker simplifies single-node deployments but adds load to the coordinator process.
- What is the right default for `worker_poll_interval_ms`? Should it be adaptive (e.g. exponential backoff when no work is available)?
- How should GC handle the window between a worker writing output SSTs and the coordinator committing the manifest? GC must not delete SSTs that are not yet manifest-referenced.
- Should workers validate their `CompactionSpec` against the current manifest before executing? Validating catches stale specs but adds a manifest read per claim.
- Is optimistic claiming sufficient at high worker counts (50+), or will contention require sharding across multiple `.compactions` files?
- How should existing per-compaction metrics (`bytes_processed`, `ssts_written`, `job_duration_seconds`) work for remote workers? Workers are separate processes with no metrics infrastructure: should they be reported by the coordinator based on what it observes in `.compactions`, or does each worker need its own metrics endpoint?
- What happens when a worker is reclaimed due to a missed heartbeat but is still executing (zombie worker)? Both the zombie and the new worker may write `Completed` to `.compactions`. Both writes can succeed as new numbered files. If the zombie finishes first, the new worker wastes its work and the coordinator may process the zombie's `Completed` entry; if the new worker finishes first, the zombie's `Completed` write becomes an orphaned entry the coordinator must ignore. The coordinator needs to be idempotent when processing `Completed` entries to handle this correctly. Additionally, do the zombie and the new worker write to conflicting output SST paths?

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [SlateDB Compaction documentation](https://slatedb.io/docs/design/compaction/)
- [RFC-0002: SlateDB Compaction](0002-compaction.md): original compaction design, includes "Looking Ahead" section on distributed compaction
- [RFC-0013: Compaction State Persistence](0013-compaction-state-persistence.md): `.compactions` file design, external process integration, resume support
- [RFC-0017: Compaction Filters](0017-compaction-filters.md): compaction filter integration that workers must support
- [RFC-0001: Manifest](0001-manifest.md): manifest format and create-if-not-exists protocol

## Updates

Log major changes to this RFC over time (optional).
