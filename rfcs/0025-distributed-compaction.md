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
   * [Deployment Patterns](#deployment-patterns)
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
- [Future Work](#future-work)
   * [Compaction routing](#compaction-routing)
   * [Multi-DB workers](#multi-db-workers)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Accepted

Authors:

* [Ryan Dielhenn](https://github.com/ryandielhenn)

## Summary

SlateDB currently runs compaction on a single process. Compaction is either embedded in the DB writer or run as a standalone process. This caps compaction throughput at one node's CPU and object store bandwidth. This RFC extends the existing `.compactions` coordination file (RFC-0013) to separate a **Compaction Coordinator** (scheduler and manifest committer) from one or more **Compaction Workers** (stateless job executors). Workers poll `.compactions` for submitted jobs, claim them via optimistic concurrency (create-if-not-exists on numbered files), execute the existing compaction code path, and report results back; the coordinator alone commits manifest updates, preserving the single-writer invariant. The design is backward-compatible with existing stateful and standalone compaction modes and adds no external dependencies beyond the object store.

## Motivation

Both existing compaction modes share the same single-node execution ceiling: in-process compaction competes with the write path for CPU and I/O, and the standalone compactor offloads compute but is still capped by a single node's resources. Distributed execution increases aggregate compaction capacity which can be utilized when the scheduler has independent work. Furthermore, offloading compute from an embedded compactor to a standalone compactor at runtime adds extra complexity to the single-compactor design. If a standalone compactor is started alongside a compactor embedded in the writer, the compactor's epoch is rewritten by the new compaction process. This fences the Db, disabling all Db operations. A solution to this is to hand-off the job of coordination to the new process, but this requires additional complexity without the added benefit of horizontal scaling.

The design in this RFC sidesteps this complexity by separating coordination (scheduling, manifest commits) from execution (compaction jobs). Workers are stateless and interchangeable, so there is no leadership to hand off when a worker starts or stops. The coordinator can run embedded in the DB process or as a standalone `Compactor` process, and compaction compute can be offloaded to external workers without any coordination handoff. Since SlateDB already uses the object store as its sole coordination primitive, the `.compactions` file provides everything needed to schedule and claim work across processes.

## Goals

- Increase aggregate compaction execution throughput for concurrently runnable, non-conflicting compactions.
- Tolerate individual worker failures without losing compaction progress, by checkpointing output SSTs and reclaiming stalled jobs.
- Preserve the single-writer invariant: only the coordinator commits manifest updates.
- Add no external dependencies beyond the object store already required by SlateDB.
- Remain backward-compatible with standalone and in-process compaction modes; no migration required.

## Non-Goals

- **Changing the compaction scheduling strategy.** The scheduler logic is unchanged; only execution is distributed.
- **Eliminating L0 admission back-pressure for a single hot segment.** L0 capacity is released by manifest commits, and the current scheduler permits one L0-sourcing compaction per segment due to the single cursor L0 watermark design. Addressing this bottleneck directly requires faster execution of a single logical L0 compaction (for example, through subcompactions), or a separate redesign of L0 scheduling, commit ordering, and watermark semantics.
- **Multi-coordinator support.** The single-coordinator invariant is preserved; leader election across coordinators or purely distributed coordination is a future concern. This includes split-brain handling (e.g. a zombie coordinator that resumes writing after a new coordinator has taken over): deployments are responsible for ensuring at most one coordinator is active at a time to avoid fencing the DB.
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
|  | (unchanged) |  | (unchanged)| |
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

- **Coordinator:** runs either embedded in the DB process or as a standalone `Compactor` process. Polls the manifest, asks the scheduler for `CompactionSpec` proposals, creates `Compaction` entities, calls `executor.start_compaction_job()`, and commits completed results to the manifest. Scheduler logic is unchanged.
- **Workers:** poll `.compactions`, claim `Submitted` jobs via optimistic concurrency, execute using the existing `execute_compaction_job` path, report completion. Workers do not touch the manifest or run the scheduler.

### Configuration

#### Coordinator

```rust
pub struct CompactorOptions {
    // ... existing coordinator-only fields (aside from max_sst_size, max_fetch_tasks) unchanged ...

    /// How long before a worker with no heartbeat is considered stale and its job reclaimed.
    pub worker_heartbeat_timeout: Duration,

    /// Interval at which the coordinator commits compactions marked `Compacted` to the manifest.
    pub commit_compacted_interval: Duration,

    /// Options for the in-process CompactionWorker spawned alongside the
    /// coordinator. `Some(..)` (the default) spawns an embedded worker; `None`
    /// runs the coordinator alone and expects workers as separate processes.
    pub worker: Option<CompactionWorkerOptions>,
}
```

Execution-only knobs (`max_sst_size`, `max_fetch_tasks`) live on `CompactionWorkerOptions`, not `CompactorOptions`. The coordinator never executes compactions itself, so it has no use for them. An embedded worker is configured through `CompactorOptions::worker`.

Via settings:

```toml
[compactor_options]
worker_heartbeat_timeout = "10s"
commit_compacted_interval = "1s"

[compactor_options.worker]
max_sst_size = 268435456
```

`worker = Some(..)` (the default) spawns a `CompactionWorker` in the same process. `worker = None` expects workers to run as separate `CompactionWorker` processes.

#### Workers

Workers use `CompactorWorkerOptions` instead of `CompactorOptions`. The primary settings are:

```rust
pub struct CompactionWorkerOptions {
  // How many jobs a single worker may hold simultaneously.
  pub max_concurrent_compactions: usize,

  // How often a worker checks `.compactions` for new jobs.
  pub compactions_poll_interval: Duration,

  // How many bytes a worker must process before emitting a heartbeat.
  pub heartbeat_bytes: u64,

  // Minimum wall-clock time between heartbeat writes.
  pub heartbeat_min_interval: Duration,

  // Max size of an output SST produced by a compaction.
  pub max_sst_size: usize,

  // Max number of source SSTs fetched concurrently during a compaction.
  pub max_fetch_tasks: usize,
}
```

`max_sst_size` and `max_fetch_tasks` are execution-only knobs that previously lived on `CompactorOptions`; they now belong to the worker, which is the only component that executes compactions.

- `max_concurrent_compactions` controls how many jobs a single worker may hold simultaneously.

- `compactions_poll_interval` is used for polling frequency. To prevent workers from synchronizing on `.compactions` reads, the poll ticker jitters each wait: instead of waiting exactly `compactions_poll_interval`, each tick waits a random duration picked uniformly between `compactions_poll_interval/2` and `3 * compactions_poll_interval/2` (the interval plus or minus half). Because the range is centered on `compactions_poll_interval`, each worker still polls once per `compactions_poll_interval` on average. Jitter is a feature of the shared task dispatcher (defaulting to off) that the worker enables for its poll ticker; the randomized wait happens inside the ticker rather than in the message handler, so waiting on it never blocks the worker from processing job-progress or job-finished messages. It requires no user configuration.

- `heartbeat_bytes` is used to tie heartbeats to compaction progress and gives the coordinator a liveness guarantee. A worker that falls behind this rate will be reclaimed and its job handed off, regardless of whether its event loop is still alive.

- `heartbeat_min_interval` suppresses heartbeats triggered by `heartbeat_bytes` when processing is fast and should be set well below the coordinator's `worker_heartbeat_timeout`.

New `CompactionWorkerBuilder` entrypoint for `CompactionWorker` processes:

```rust
let options = CompactionWorkerOptions {
    max_concurrent_compactions: 2,
    compactions_poll_interval: Duration::from_secs(1),
    heartbeat_bytes: 100_000,
    heartbeat_min_interval: Duration::from_secs(10),
};

let worker = CompactionWorkerBuilder::new("/path/to/db", object_store.clone())
    .with_options(options)
    .build()
    .await?;

worker.run().await?;
```

### Schema Changes

Extend the `Compaction` table in `compactor.fbs`:

```fbs
table WorkerSpec {
    worker_id: string;          // worker_id of worker that owns compaction
    last_heartbeat_ms: uint64;  // wall-clock ms of last progress write
}

table Compaction {
    // Existing fields (unchanged)
    id: Ulid (required);
    spec: CompactionSpec (required);
    status: CompactionStatus;
    output_ssts: [CompactedSsTable];

    // NEW
    worker: WorkerSpec; // null = unclaimed compaction
}
```
### Work Claim Protocol

Workers use optimistic concurrency on `.compactions`. SlateDB implements this uniformly across all object stores, including those with native CAS support, using create-if-not-exists on sequentially-numbered files (e.g. `00000000000000000003.compactions`). Writing a new version means writing the next numbered file; if another writer got there first the write fails with `AlreadyExists` and the worker retries. See [RFC-0001](0001-manifest.md) for the full protocol.

1. Poll `.compactions` every `compactions_poll_interval`.
2. Find up to `max_concurrent_compactions` `Compaction` entries with `status == Submitted` and empty `worker_id`.
3. Write the full updated state with `status = Running`, `worker_id = <self>`, `last_heartbeat_ms = now()` to the next sequence number.
4. On success: begin execution. On `AlreadyExists`: re-read latest and retry from step 2.

Workers claim up to `max_concurrent_compactions` jobs at a time, limiting the number of compactions affected by a worker crash and distributing work evenly across workers.

### Heartbeat and Failure Detection

**Heartbeat Protocol** (worker):

1. On each output SST write, piggyback `last_heartbeat_ms = now()` onto the RFC-0013 progress-persistence write to `.compactions`.
2. Additionally, after every `heartbeat_bytes` bytes processed: if `now() - last_heartbeat_ms >= heartbeat_min_interval_ms`, write updated `.compactions` with `last_heartbeat_ms = now()` for all `Running` jobs owned by this worker. This ties liveness directly to compaction throughput. A degraded machine that is alive but slow will miss the threshold and be reclaimed.
3. On `AlreadyExists`: re-read latest. If the compaction is now `Submitted` or claimed by another `worker_id`, the worker has lost the assignment: discard local state for that compaction, abort execution, and return to the poll/claim loop. Otherwise retry the write.

Polls do not emit heartbeats. Liveness is driven entirely by compaction progress.

**Failure Detection Protocol** (coordinator):

1. On each coordinator poll tick, read latest `.compactions`.
2. For each `Running` compaction where `now() - last_heartbeat_ms > worker_heartbeat_timeout`: set `status = Submitted`, clear `worker_id`, retain `output_ssts` and `id`.
3. If any compactions were reclaimed in step 2, write updated state via `write_compactions_safely()`. The reclaimed compaction retains its `output_ssts`, so the next worker resumes from the last checkpoint via `ResumingIterator`.
4. On `AlreadyExists`: re-read latest and retry from step 1.

### Worker Lifecycle

1. **Start:** generate a ULID `worker_id`, load config.
2. **Poll:** If the worker has fewer than `max_concurrent_compactions` active jobs, read `.compactions`, look for `Submitted` entries to claim. Polls do not write heartbeats.
3. **Claim:** optimistic transition to `Running` (see claim protocol).
4. **Execute:** run `execute_compaction_job`: build iterators from `CompactionSpec`, apply filters/merge ops, write output SSTs to `compacted/`, persist progress at each SST boundary.
5. **Complete:** write `status = Compacted` with final `output_ssts` to `.compactions`.
6. **Loop:** return to step 2.
7. **Graceful shutdown:** on cancellation, reset all `Running` compactions owned by this worker back to `Submitted` and clear their `worker_id` in object storage. This lets other workers reclaim the jobs immediately rather than waiting for the heartbeat timeout to expire.

### Manifest Commit Protocol

**Existing protocol (single-process compaction)**

Today the executor runs in-process and the compactor receives a `CompactionJobFinished` event on completion. The existing state transitions are:

```
Submitted <-> Running --> Completed
    |             |
    |             v
    +----------> Failed
```

The compactor transitions the job to `Completed` after updating the manifest in memory, then flushes both in a single `write_state_safely()` call (`.manifest` first, then `.compactions`). If a crash occurs between the two writes, the job remains `Running` in `.compactions` on restart. Recovery resets it to `Submitted`; the next attempt fails `validate_compaction()` because the sources were already removed from the manifest, so the job is marked `Failed`. This is safe.

**Why this breaks for remote workers**

When the executor is a separate process, the coordinator never receives an in-process `CompactionJobFinished` signal. The worker must signal completion by writing to `.compactions`. The naive approach of having the worker write `Completed` directly, breaks the recovery invariant. On restart the coordinator cannot distinguish "completed before manifest write" from "completed after manifest write" without retrying the manifest write for every `Completed` entry, not just `Running` ones.

**The `Compacted` state**

This RFC introduces `Compacted` as an intermediate state with a precise semantic: *the worker finished execution and wrote its final output SSTs; the manifest may or may not have been updated yet.* It is the distributed equivalent of the in-process `CompactionJobFinished` signal.

```
Submitted <-> Running --> Compacted --> Completed
    |             |           |
    |             v           |
    +----------> Failed <-----+
```

The coordinator is solely responsible for transitions from `Compacted → Completed` (or `Compacted → Failed`), and transitions the state only after attempting the manifest write. This preserves the single-writer invariant and gives recovery a clean, unambiguous signal for `Compacted` entries: they always need a manifest write retry.

**The `Scheduled` state**

This RFC introduces the `Scheduled` state so that the coordinator can signal that a compaction is ready to be claimed by a worker.

```text
Submitted --> Scheduled <-> Running --> Compacted --> Completed
    |                          |           |
    |                          v           |
    +-----------------------> Failed <-----+
```

The coordinator is solely responsible for transitions from `Submitted → Scheduled`, and transitions the state only after validating the compaction against the current manifest and updating its local state to be aware of the newly scheduled jobs. Workers exclusively claim `Scheduled` entries. They never act on `Submitted` which keeps the coordinator the single gatekeeper for validation and ensures the coordinator has the entry in local state before any worker can transition it onward.

**New Protocol (distributed compaction)**

Only the coordinator validates compactions and commits manifest updates (preserves single-writer invariant):

1. Observe a `Submitted` entry in `.compactions`.
2. Validate the compaction, if it succeeds, update local state with the new compaction and transition it to `Scheduled`. If validation fails the compaction is transitioned to `Failed`.
3. Observe a `Compacted` entry in `.compactions` (written by the worker on job completion).
4. Update `.manifest` via `write_manifest_safely()`.
5. Update `.compactions` via `write_compactions_safely()`, transitioning `Compacted` → `Completed`.

On coordinator restart, the recovery logic is:

1. Leave `Running` jobs alone. If the owning worker survived the coordinator restart it continues executing and emitting heartbeats; if it died, the failure detection protocol reclaims it once the heartbeat exceeds `worker_heartbeat_timeout` (see step 2 of the failure detection protocol). *(Interim: until heartbeat reclamation lands, restart resets `Running` → `Submitted` instead.)*
2. For each `Compacted` job, retry steps 2–3 of the normal flow above. `validate_compaction()` is called before the manifest write and will fail if the job's sources are already absent from the manifest (i.e. step 2 already completed before the crash). In that case the job is marked `Failed` in `.compactions`. This is safe: the manifest was already updated, the output SSTs are already referenced and protected from GC, and the scheduler has no dependency on whether the entry reads `Completed` or `Failed`.
3. Retain active (`Submitted`, `Scheduled`, `Running`, `Compacted`) and last finished (`Completed`, `Failed`) entries.

### Deployment Patterns

`compactor_options: None` in `Settings` means no coordinator runs in that process; a standalone `Compactor` process owns coordination instead.

1. **Coordinator + embedded worker:** coordinator and worker run together in the DB process.

```rust
let db = Db::builder("db", object_store)
    .with_settings(Settings {
        compactor_options: Some(CompactorOptions {
            worker_heartbeat_timeout: Duration::from_secs(30),
            ..Default::default() // worker defaults to Some(CompactionWorkerOptions::default())
        }),
        ..Default::default()
    })
    .build()
    .await?;
```

`compactor_options.worker: None` in `Settings` means no worker runs in that process; a standalone `CompactionWorker` process owns claiming and execution instead.

2. **Coordinator + remote workers:** coordinator runs in the DB process; workers are separate processes.

```rust
// Disable the embedded compaction worker by setting worker to None.
let db = Db::builder("db", object_store)
    .with_settings(Settings {
        compactor_options: Some(CompactorOptions {
            worker: None,
            worker_heartbeat_timeout: Duration::from_secs(30),
            ..Default::default()
        }),
        ..Default::default()
    })
    .build()
    .await?;

// Worker process(es)
let worker = CompactionWorkerBuilder::new("db", object_store)
    .with_options(
      CompactionWorkerOptions {
        max_concurrent_compactions: 2,
        compactions_poll_interval: Duration::from_secs(1),
        heartbeat_bytes: 100_000,
        heartbeat_min_interval: Duration::from_secs(5),
      }
    )
    .build()
    .await?;
worker.run().await?;
```

3. **Standalone coordinator + embedded worker:** coordinator and worker run together in a separate process; the DB process does no compaction.

```rust
// Disable the embedded compaction coordinator and worker by clearing compactor options.
let db = Db::builder("db", object_store)
    .with_settings(Settings { compactor_options: None, ..Default::default() })
    .build()
    .await?;

// Standalone coordinator process
let compactor = CompactorBuilder::new("db", object_store)
    .with_options(CompactorOptions {
        worker_heartbeat_timeout:Duration::from_secs(30),
        ..Default::default() // worker defaults to Some(CompactionWorkerOptions::default())
    })
    .build();
compactor.run().await?;
```

4. **Standalone coordinator + remote workers:** coordinator runs in a separate process; workers are their own processes; the DB process does no compaction.

```rust
// Disable the embedded compaction coordinator and worker by clearing compactor options.
let db = Db::builder("db", object_store)
    .with_settings(Settings { compactor_options: None, ..Default::default() })
    .build()
    .await?;

// Standalone coordinator process
let compactor = CompactorBuilder::new("db", object_store)
    .with_options(CompactorOptions {
        worker: None,
        worker_heartbeat_timeout: Duration::from_secs(30),
        ..Default::default()
    })
    .build();
compactor.run().await?;

// Worker process(es)
let worker = CompactionWorkerBuilder::new("db", object_store)
    .with_options(
      CompactionWorkerOptions {
        max_concurrent_compactions: 2,
        compactions_poll_interval: Duration::from_secs(1),
        heartbeat_bytes: 100_000,
        heartbeat_min_interval_ms: Duration::from_secs(5),
      }
    )
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
- [x] Observability (metrics/logging/tracing) — coordinator metrics

## Operations

### Performance & Cost
- **Latency**: Read/write latency is unchanged. The distributed model adds one extra round-trip to the L0 drain cycle that does not exist in the embedded case: the coordinator writes a `Submitted` job to `.compactions`, then a worker picks it up on its next poll. In the worst case this delays the start of an L0 compaction by up to the upper end of the poll ticker's randomized wait (`3 * compactions_poll_interval_ms / 2`). Whether the end-to-end drain time (submit → claim → compact → manifest commit) remains competitive with the current single-node path warrants benchmarking, particularly at the default `compactions_poll_interval_ms`.

- **Throughput**: Scales up to the available independent, non-conflicting compaction work. It is bounded by how much independent work the scheduler produces, source conflicts, and worker/object store bandwidth. The coordinator may batch completed jobs into one manifest update, but manifest commits remain a serial process done by the coordinator rather than a separate commit lane per worker.
- **Object-store requests**: ~1 GET per poll interval + ~1 PUT per claim + ~1 PUT per output SST. At N=10 workers polling every 5s: ~120 GETs/min overhead.
- **Space/write/read amplification**: Unchanged.

### Observability

Both the coordinator and workers use the `MetricsRecorder` infrastructure introduced in RFC-21.

**Coordinator metrics**:

The following metrics are additive. In distributed deployment, `slatedb.compactor.bytes_compacted` and `slatedb.compactor.running_compactions` from `CompactionStats` will no longer be updated by the coordinator since the executor runs out-of-process. Per-worker equivalents are emitted by workers instead (see below).

| Metric | Instrument | Labels | Description |
|--------|------------|--------|-------------|
| `slatedb.compactor.jobs_claimed` | Counter | | `Submitted` → `Running` transitions observed by the coordinator |
| `slatedb.compactor.jobs_reclaimed` | Counter | | Stale jobs reset from `Running` → `Submitted` by the coordinator |
| `slatedb.compactor.worker_last_heartbeat_ms` | Gauge | `{worker_id=<id>}` | Last seen heartbeat timestamp for each known worker |

**Worker metrics**:

`CompactorWorkerBuilder` accepts its own `MetricsRecorder`. Workers emit the following per-worker metrics tagged with `{worker_id=<id>}`:

| Metric | Instrument | Labels | Description |
|--------|------------|--------|-------------|
| `slatedb.compactor.bytes_compacted` | Counter | `{worker_id=<id>}` | Bytes compacted by this worker |
| `slatedb.compactor.running_compactions` | UpDownCounter | `{worker_id=<id>}` | Compaction jobs currently running on this worker |
| `slatedb.compactor.ssts_written` | Counter | `{worker_id=<id>}` | Output SSTs written by this worker |

Worker lifecycle events (claimed, reclaimed, heartbeat timeout) are logged at INFO.

### Compatibility

- **Object storage**: Backward compatible. New fields default to unclaimed/0; no migration needed.
- **Public API**: DB read/write API unchanged. `CompactionWorkerBuilder` and `CompactionWorker` are additive. **Breaking:** execution-only knobs (`max_sst_size`, `max_fetch_tasks`, `max_concurrent_compactions`) move from `CompactorOptions` to `CompactionWorkerOptions`. Users setting any of these on `CompactorOptions` must move them onto the embedded worker via `CompactorOptions::worker` (or onto the remote worker's `CompactionWorkerOptions`).
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
1. **Schema extension:** add `WorkerSpec` containing `worker_id` and `last_heartbeat_ms` to `Compaction` in `compactor.fbs`.
2. **Manifest Commit Protocol:** coordinator merges compactions with `Compacted` status from remote workers, transitions them to either `Failed` or `Completed`, and commits their output to the manifest.
3. **Worker implementation:** implement `CompactionWorkerBuilder` and `CompactionWorker`; coordinator always uses `CompactionWorker`. Move `max_concurrent_compactions`, `max_sst_size`, `max_fetch_tasks` from `CompactorOptions` to `CompactionWorkerOptions` (breaking change; see Compatibility).
4. **Failure detection:** heartbeat timeout and reclamation on the coordinator; resume via `ResumingIterator`.

### Docs Updates

- Add examples to API documentation.
- Update compaction documentation to describe how to run distributed compaction.

## Alternatives

### Status quo (single compactor)

Only run one compaction process per instance of SlateDb, either embedded or standalone.
**Rejected:** Can't meet the scaling goal. Introduces complexity around offloading compute from embedded to standalone-compactor at runtime (see [PR #1529](https://github.com/slatedb/slatedb/pull/1529)).


### Peer-to-peer leader election via object store

All compactors are peers; optimistic concurrency on a numbered file elects a leader to run the scheduler.
**Rejected:** Adds complexity around leader transitions and scheduler handoff. Could be a future evolution.

### chitchat for work distribution/discovery

Use gossip to distribute jobs directly.
**Rejected:** couples correctness to gossip consistency; chitchat is better as an optional discovery/health layer.

## Future Work

### Compaction routing

Routing compactions to specific workers or worker classes (e.g. L0 jobs to an embedded worker, major compactions to a beefy short-lived node). The claim protocol in this RFC is designed so that selectivity can be added entirely on the worker side without coordinator or schema changes: a worker can filter `Submitted` entries by `CompactionSpec` properties (level, input bytes, etc.) before attempting a claim. Common policies worth exploring:

- **L0 affinity:** embedded workers preferentially claim L0 jobs to keep flush-side latency low; remote workers handle larger compactions. Naively prioritizing L0 jobs interacts with `max_concurrent_compactions`: a single worker could greedily claim every available L0 job, bottlenecking them on one worker's CPU/IO rather than spreading them across the pool. In practice we expect only one or two L0 compactions outstanding at a time, so simple prioritization is likely fine; a pluggable work-scheduling trait is a natural extension if fairer policies are ever needed.
- **Size-class pools:** small jobs go to long-lived workers; large major compactions go to a separate pool of beefy short-lived nodes that can scale to zero between jobs.

### Multi-DB workers

Some SlateDB users run thousands of DBs. A single worker process today is bound to one DB via its `CompactionWorkerBuilder` path/object-store pair. A natural extension is a multi-DB worker that polls `.compactions` across many DBs from a shared pool, sharing CPU, network, and process overhead across them. Open design questions:

- **Discovery:** how does a worker learn the set of DBs to service (static config, prefix scan, control-plane registry)?
- **Fairness and prioritization:** how is poll budget and concurrency allocated across DBs so a hot DB doesn't starve quiet ones?
- **Resource isolation:** per-DB bytes-in-flight and concurrency caps so one DB's major compaction can't exhaust the worker.
- **Identity and metrics:** worker IDs and metric labels likely need a `{db, worker_id}` shape rather than just `{worker_id}`.

## Open Questions

1. ~~What is the right default for `compactions_poll_interval`? Should it be adaptive (e.g. exponential backoff when no work is available)?~~
**Resolved:** Exponential backoff does not make sense for `compactions_poll_interval` because GETs to object storage are cheap and it is critical that L0 compactions are started as soon as possible. A reasonable default is one second (e.g. `compactions_poll_interval="1s"`).

2. ~~Is optimistic claiming sufficient at high worker counts (50+), or will contention require sharding across multiple `.compactions` files?~~
**Resolved:** Claim contention is naturally low because compaction jobs run far longer than the claim operation itself. The poll ticker also randomizes each wait around `compactions_poll_interval_ms` (see `compactions_poll_interval` above), spreading poll timing across workers without any additional configuration.

3. ~~How should existing per-compaction metrics (`bytes_processed`, `ssts_written`) work for remote workers? Workers are separate processes with no metrics infrastructure: should they be reported by the coordinator based on what it observes in `.compactions`, or does each worker need its own metrics endpoint?~~
**Resolved:** Workers should have the same metrics infrastructure introduced by the metrics RFC and users can wire in reporting as they'd like. The worker tags the metrics with the worker id.

4. ~~What happens when a worker is reclaimed due to a missed heartbeat but is still executing (zombie worker)? Both the zombie and the new worker may write `Completed` to `.compactions`. Both writes can succeed as new numbered files. If the zombie finishes first, the new worker wastes its work and the coordinator may process the zombie's `Completed` entry; if the new worker finishes first, the zombie's `Completed` write becomes an orphaned entry the coordinator must ignore. The coordinator needs to be idempotent when processing `Completed` entries to handle this correctly.~~
**Resolved:** Reviewers mentioned that a zombie wasting work is not a major issue and expected. The coordinator doesn't need to be idempotent when processing as long as job status can only be updated by the worker that claimed it. A worker trying to write `Completed` status to a compaction must present the same worker_id that is tied to the `Running` job. Zombie processes attempting to update the job status with a mismatched worker_id are unsuccessful and no operation occurs.

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [SlateDB Compaction documentation](https://slatedb.io/docs/design/compaction/)
- [RFC-0002: SlateDB Compaction](0002-compaction.md): original compaction design, includes "Looking Ahead" section on distributed compaction
- [RFC-0013: Compaction State Persistence](0013-compaction-state-persistence.md): `.compactions` file design, external process integration, resume support
- [RFC-0017: Compaction Filters](0017-compaction-filters.md): compaction filter integration that workers must support
- [RFC-0001: Manifest](0001-manifest.md): manifest format and create-if-not-exists protocol
- [Github Issue](https://github.com/slatedb/slatedb/issues/1165)

## Updates

Log major changes to this RFC over time (optional).
