# SlateDB DST Harness

`slatedb-dst` is SlateDB's deterministic simulation testing crate.

It packages the pieces needed to run seeded, reproducible simulations against a
real `slatedb::Db`:

- a seeded, deterministic, current-thread Tokio runtime
- a shared `MockSystemClock`
- a shared `FailPointRegistry`
- deterministic main and optional WAL object-store stacks
- a shared installed `Arc<slatedb::Db>` slot that actors can read or replace
- actor-local `slatedb::DbRand` instances derived from the root seed
- an optional shared `MergeOperator` handle for scenarios that exercise merge
  operands

This is primarily a test crate for SlateDB itself and is currently
`publish = false`. It is intended to be used from a workspace checkout or as an
internal path dependency rather than from crates.io.

## What the crate provides

- `Harness`: builder and executor for deterministic simulations
- `Actor`: async actor trait run by the harness
- `actors`: reusable scenario actors, including workload, flusher, standalone
  compactor, shutdown, and bank-transfer/auditor actors
- `DeterministicLocalFilesystem`: filesystem-backed `ObjectStore` with stable
  metadata, in-memory attribute preservation, and deterministic listing behavior
- `FailingObjectStore`: harness-installed `ObjectStore` wrapper that injects
  deterministic latency, bandwidth, reset-peer, slow-close, and synthetic HTTP
  failures
- `Toxic`: fault-injection building blocks
- `HttpFailBefore` and `HttpStatusError`: synthetic HTTP failures injected
  before request dispatch
- `utils`: helpers for generating randomized deterministic settings,
  compactor/GC options, object-store toxics, and test logging

## Determinism model

The harness is built around one root seed:

1. `Harness::new(name, seed, factory)` creates the root `DbRand`.
2. The harness derives seeds for the Tokio runtime, database startup
   (`StartupCtx::rand()`), the harness-owned background clock task, and the
   failure controller.
3. The simulation runs on a seeded current-thread Tokio runtime.
4. After startup, the harness derives one actor-local seed per registered actor
   (`ActorCtx::rand()`).
5. The harness wraps the configured main and optional WAL object stores with
   internal fault-injecting and clock-aware layers.
6. The harness owns a seeded failure controller and exposes it through
   `StartupCtx::failure_controller()`.
7. The shared `MockSystemClock` advances from the harness-owned background
   clock task, when your test advances it explicitly, or when a configured
   toxic adds latency/bandwidth/slow-close delay.

Given the same seed and the same DST-compatible code paths, the harness is
designed to replay the exact same sequence of events every single time.

For reproducible tests, keep randomness and time inside the harness:

- use `StartupCtx::rand()` and `ActorCtx::rand()` instead of OS randomness
- use `ActorCtx::advance_time()` and the shared mock clock instead of wall clock
- use the wrapped object stores supplied by the harness rather than ad hoc
  external I/O paths

## Requirements

`slatedb-dst` relies on Tokio's unstable runtime seeding support. Build and run
it with `tokio_unstable` enabled:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo test -p slatedb-dst --lib
```

The scenario tests under `slatedb-dst/tests/` also require SlateDB's
`cfg(dst)` behavior:

```bash
RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test -p slatedb-dst --tests
```

`cfg(dst)` enables SlateDB's deterministic testing branches, including code
paths that avoid known sources of non-determinism during simulation.

## Using the harness

At a high level, a simulation has three parts:

1. Configure the harness seed, path, clock, and object stores.
2. Provide a startup factory to `Harness::new(...)` that opens the `Db`.
3. Register one or more actors and call `run()`.

### End-to-end example

The example below mirrors the intended usage pattern: deterministic filesystem
object stores, seeded database construction, a registered actor, and injected
object-store latency.

```rust,ignore
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::Db;
use slatedb_common::clock::MockSystemClock;
use slatedb_dst::{
    actors::{ShutdownActor, WorkloadActor, WorkloadActorOptions}, utils::build_settings,
    DeterministicLocalFilesystem, Harness, Operation, StreamDirection, Toxic, ToxicKind,
};
use tempfile::TempDir;

#[test]
fn dst_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let system_clock = Arc::new(MockSystemClock::new());
    let main_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?);
    let wal_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?);
    let shutdown_at_millis = 10i64;
    let workload_options = WorkloadActorOptions::default();
    let harness = Harness::new("smoke", 7, move |ctx| async move {
        ctx.failure_controller().add_toxic(Toxic {
            name: "put-latency".into(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(2),
                jitter: Duration::from_millis(3),
            },
            direction: StreamDirection::Upstream,
            toxicity: 1.0,
            operations: vec![Operation::PutOpts],
            path_prefix: None,
        });

        let db_seed = ctx.rand().rng().next_u64();
        let settings = build_settings(ctx.rand()).await;

        let mut builder = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_wal_object_store(ctx.wal_object_store().expect("configured"))
            .with_system_clock(ctx.system_clock())
            .with_fp_registry(ctx.fp_registry())
            .with_seed(db_seed)
            .with_settings(settings);
        if let Some(merge_operator) = ctx.merge_operator() {
            builder = builder.with_merge_operator(merge_operator);
        }
        let db = builder.build().await?;

        Ok(Arc::new(db))
    })
        .with_path(Path::from("dst/smoke"))
        .with_system_clock(system_clock)
        .with_main_object_store(main_store)
        .with_wal_object_store(wal_store);

    harness
        .actor("workload-1", WorkloadActor::new(workload_options)?)
        .actor("shutdown", ShutdownActor::new(shutdown_at_millis)?)
        .run()?;

    Ok(())
}
```

### `Harness`

`Harness` is the main entry point.

- `Harness::new(name, seed, factory)`: creates a new simulation builder and
  registers the startup factory that opens the database
- `with_path(path)`: overrides the default DB path
- `with_rand(rand)`: replaces the root RNG
- `with_system_clock(clock)`: injects a shared `MockSystemClock`
- `with_clock_advance(min_ms..=max_ms)`: overrides the inclusive millisecond
  range used by the harness-owned background clock task
- `with_main_object_store(store)`: replaces the default in-memory main base
  store, which the harness wraps before use
- `with_wal_object_store(store)`: configures a separate WAL base store, which
  the harness wraps before use
- `with_merge_operator(merge_operator)`: configures an optional shared
  `MergeOperator` handle for SlateDB writers, readers, and standalone compactors
- `actor(name, actor)`: registers one actor instance under a unique name
- `run()`: builds the seeded runtime, starts the harness-owned background
  clock task, opens the DB, spawns one Tokio task per registered actor, and
  runs until all actors finish or an actor fails. The bundled looping actors
  normally finish after the shared shutdown token is cancelled.

### `StartupCtx`

The `Harness::new(...)` factory receives a `StartupCtx`. It is the right place to
open the initial `Db`, construct settings, and attach any shared infrastructure
to the database builder.

`StartupCtx` exposes:

- `path()`
- `main_object_store()`
- `wal_object_store()`
- `system_clock()`
- `fp_registry()`
- `merge_operator()`
- `failure_controller()`
- `rand()`

Typical startup responsibilities:

- derive a deterministic SlateDB seed with `ctx.rand().rng().next_u64()`
- build randomized deterministic settings with `utils::build_settings(...)`
- open `Db::builder(...)` using the harness-provided object stores and clock
- pass through the shared failpoint registry
- pass `ctx.merge_operator()` to `DbBuilder::with_merge_operator(...)` when it
  is present
- install initial object-store toxics with `ctx.failure_controller()`

### `ActorCtx`

Each registered actor instance receives its own `ActorCtx`.

`ActorCtx` exposes:

- `name()` for actor identity
- `rand()` for actor-local deterministic randomness
- `startup_ctx()` to reuse the startup path, object stores, clock, failpoint
  registry, failure controller, and startup RNG when reopening a database
- `db()` to read the currently installed shared `Arc<Db>`
- `swap_db(new_db)` to replace the shared DB handle for all actors
- `swap_compactor(new_compactor)` to replace the shared standalone compactor handle
- `shutdown_token()` to request or observe harness shutdown
- `advance_time(duration)` to move the shared mock clock forward
- `path()`, `main_object_store()`, `wal_object_store()`
- `system_clock()` and `fp_registry()`
- `merge_operator()` to reuse the shared merge operator when opening readers,
  compactors, or replacement DB handles

`swap_db(...)` is useful for reopen scenarios and tests that intentionally
replace the database instance mid-run.

### Shutdown semantics

The harness owns the outer actor loop:

- it repeatedly calls `Actor::run(&mut self, &ActorCtx)` for one logical
  iteration
- actors request shutdown only by cancelling `ctx.shutdown_token()`
- once the shared shutdown token is cancelled, the harness stops calling
  `run()` and then calls `Actor::finish(&mut self, &ActorCtx)` once

If an actor returns `Err(e)`, the harness cancels shutdown, aborts the remaining
tasks, and returns `Err(e)`. Aborted actors do not get a graceful `finish(...)`
callback.

## Failure injection

The harness wraps configured object stores with `FailingObjectStore`
automatically. To inject faults, get the shared controller from
`StartupCtx::failure_controller()` during startup or through
`ActorCtx::startup_ctx().failure_controller()` while actors are running.

### Controller operations

- `add_toxic(toxic)`: appends a toxic to the active set
- `clear_toxics()`: removes all toxics
- `set_http_fail_before(failure)`: installs a synthetic HTTP failure policy
- `clear_http_failures()`: clears the synthetic HTTP failure policy

### `Toxic`

A `Toxic` is a probabilistic fault with optional operation and path filters.

- `name`: human-readable label
- `kind`: fault behavior
- `direction`: request side (`Upstream`) or response side (`Downstream`)
- `toxicity`: probability in the inclusive range `0.0..=1.0`
- `operations`: empty means "all operations"
- `path_prefix`: `None` means "all paths"

Supported `ToxicKind` values:

- `Latency { latency, jitter }`: advances the shared clock by fixed latency plus
  sampled jitter
- `Bandwidth { bytes_per_sec }`: advances the clock based on transfer size
- `ResetPeer`: returns a connection-reset style object-store error
- `SlowClose { delay }`: advances the clock on the response side. It is useful
  with `StreamDirection::Downstream`.

`Operation` lets you target specific request types:

- `PutOpts`
- `GetOpts`
- `GetRange`
- `GetRanges`
- `Head`
- `List`
- `ListWithOffset`
- `Delete`
- `Copy`
- `Rename`

### Synthetic HTTP failures

`HttpFailBefore` injects a synthetic HTTP-like error before a matching request
is dispatched to the wrapped store.

```rust,ignore
use slatedb_dst::{HttpFailBefore, Operation};

let failures = ctx.failure_controller();
failures.set_http_fail_before(HttpFailBefore {
    percentage: 100,
    status_code: 503,
    operations: vec![Operation::GetOpts],
    path_prefix: Some("wal/".into()),
});
```

The injected error surfaces as `object_store::Error::Generic` whose source is an
`HttpStatusError`. That lets tests assert on the synthetic status code when they
need to distinguish "HTTP failed before dispatch" from transport or SlateDB
errors.

## `DeterministicLocalFilesystem`

`DeterministicLocalFilesystem` is the recommended filesystem-backed store for
DST scenarios.

Compared to `object_store::local::LocalFileSystem`, it is intentionally more
predictable:

- it performs filesystem operations synchronously on the current task
- it avoids Tokio's blocking thread pool
- it synthesizes stable `last_modified` metadata
- it preserves object-store attributes in memory for the life of the store
- it returns sorted listing results
- it can optionally clean up empty parent directories on delete

Useful methods:

- `DeterministicLocalFilesystem::new_with_prefix(prefix)`: creates a store rooted
  at an existing directory
- `with_automatic_cleanup(bool)`: controls recursive cleanup of empty parent
  directories after deletes
- `path_to_filesystem(location)`: resolves an object-store path to an absolute
  filesystem path for assertions and inspection

Use this when you want real filesystem semantics without giving up deterministic
metadata or stable listing order.

## `utils` helpers

`utils::build_settings(rand)` produces a randomized deterministic
`slatedb::Settings` value from a supplied `DbRand`.

It currently randomizes several useful dimensions of a SlateDB run, including:

- flush interval
- manifest polling and update timeouts
- bloom filter configuration
- L0 SST size and count thresholds
- maximum unflushed bytes
- compression codec selection
- compactor options
- garbage collection intervals and minimum ages
- optional WAL enablement when the `wal_disable` feature is compiled

Object-store caching is intentionally left at its default disabled state. The
cache uses local filesystem state and blocking-task wakeups outside the seeded
current-thread runtime, which would undermine the harness's logical-clock
determinism.

Other helpers expose the pieces separately:

- `utils::build_settings_compactor(rng)`: randomized deterministic
  `CompactorOptions`
- `utils::build_settings_gc(rng)`: randomized deterministic
  `GarbageCollectorOptions`
- `utils::build_toxic(rand, root_path, index)`: randomized deterministic
  `Toxic` values for object-store fault injection

Because these values are derived from supplied RNGs, they expand scenario
coverage without sacrificing reproducibility.

## Guidance for writing stable scenarios

- Seed everything through the harness and avoid unseeded randomness.
- Use the harness-provided clock for time-sensitive behavior.
- Prefer `DeterministicLocalFilesystem` over ad hoc local filesystem wrappers.
- Install object-store faults on the shared `FailingObjectStoreController`
  before or during the scenario, depending on when they should take effect.
- Use `swap_db(...)` when the scenario includes reopen or failover behavior.
- Keep actor behavior inside the seeded current-thread Tokio runtime when you
  care about replayability.

## Running tests

Library tests:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo test -p slatedb-dst --lib
```

DST scenario tests:

```bash
RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test -p slatedb-dst --tests
```

If you want both in one pass, run the DST-gated command for the package:

```bash
RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test -p slatedb-dst
```
