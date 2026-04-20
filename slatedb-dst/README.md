# SlateDB DST Harness

`slatedb-dst` is SlateDB's deterministic scenario testing crate.

It packages the pieces needed to run seeded, reproducible simulations against a
real `slatedb::Db`:

- a seeded current-thread Tokio runtime
- a shared `MockSystemClock`
- a shared `FailPointRegistry`
- deterministic main and optional WAL object-store stacks
- a shared installed `Arc<slatedb::Db>` slot that actors can read or replace
- actor-local `slatedb::DbRand` instances derived from the root seed

This is primarily a test crate for SlateDB itself and is currently
`publish = false`, so it is intended to be used from a workspace checkout or as
an internal path dependency rather than from crates.io.

## What the crate provides

- `Harness`: builder and executor for deterministic simulations
- `StartupCtx`: context passed to the database factory configured with
  `Harness::with_db`
- `ActorCtx`: per-actor context with deterministic RNG, DB access, shared
  clock, failpoint registry, and failure controller
- `DeterministicLocalFilesystem`: filesystem-backed `ObjectStore` with stable
  metadata and deterministic listing behavior
- `FailingObjectStoreController`: shared controller used to install and clear
  object-store faults during a run
- `Toxic`, `ToxicKind`, `StreamDirection`, `Operation`: fault-injection
  building blocks
- `HttpFailBefore` and `HttpStatusError`: synthetic HTTP failures injected
  before request dispatch
- `utils::build_settings`: helper for generating randomized but deterministic
  `slatedb::Settings`

## Determinism model

The harness is built around one root seed:

1. `Harness::new(name, seed)` creates the root `DbRand`.
2. The harness derives a Tokio runtime seed from that root seed and runs the
   simulation on a current-thread runtime.
3. The harness derives additional deterministic seeds for:
   - database startup (`StartupCtx::rand()`)
   - object-store fault sampling
   - each actor instance (`ActorCtx::rand()`)
4. The harness wraps the configured object stores with:
   - a clock-aware layer that reports deterministic `last_modified` metadata
   - a failure-injecting layer driven by the shared controller
5. The shared `MockSystemClock` advances only when your test advances it or a
   configured toxic adds latency/bandwidth delay.

Given the same seed and the same DST-compatible code paths, the harness is
designed to replay the same scenario.

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
2. Provide a startup factory with `with_db(...)` that opens the `Db`.
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
use slatedb_dst::{
    utils::build_settings, DeterministicLocalFilesystem, Harness, Operation,
    StreamDirection, Toxic, ToxicKind,
};
use tempfile::TempDir;

#[test]
fn dst_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    let cache_dir = tempdir.path().join("cache");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;
    std::fs::create_dir_all(&cache_dir)?;

    let main_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?);
    let wal_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?);

    Harness::new("smoke", 7)
        .with_path(Path::from("dst/smoke"))
        .with_main_object_store(main_store)
        .with_wal_object_store(wal_store)
        .with_db(move |ctx| {
            let cache_dir = cache_dir.clone();
            async move {
                let db_seed = ctx.rand().rng().next_u64();
                let settings = build_settings(ctx.rand(), &cache_dir).await;

                let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
                    .with_wal_object_store(ctx.wal_object_store().expect("configured"))
                    .with_system_clock(ctx.system_clock())
                    .with_fp_registry(ctx.fp_registry())
                    .with_seed(db_seed)
                    .with_settings(settings)
                    .build()
                    .await?;

                Ok(Arc::new(db))
            }
        })
        .actor("writer", 1, |ctx| async move {
            ctx.failures().add_toxic(Toxic {
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

            let db = ctx.db();
            db.put(b"hello", b"world").await?;
            ctx.advance_time(Duration::from_millis(10)).await;
            db.close().await?;
            Ok(())
        })
        .run()?;

    Ok(())
}
```

### `Harness`

`Harness` is the main entry point.

- `Harness::new(name, seed)`: creates a new simulation builder
- `with_path(path)`: overrides the default DB path
- `with_rand(rand)`: replaces the root RNG
- `with_system_clock(clock)`: injects a shared `MockSystemClock`
- `with_main_object_store(store)`: replaces the default in-memory main store
- `with_wal_object_store(store)`: configures a separate WAL store
- `with_db(factory)`: registers the startup factory that opens the database
- `actor(role, count, actor_fn)`: registers `count` actors for the same role
- `actor_with_state(role, count, shared_state, actor_fn)`: same as `actor`, but
  clones user-supplied shared state into each actor
- `run()`: builds the seeded runtime, opens the DB, spawns actors, and waits
  for them to finish

If `with_path(...)` is not set, the harness uses:

```text
dst/<name>/seed-<seed-hex>
```

### `StartupCtx`

The `with_db(...)` factory receives a `StartupCtx`. It is the right place to
open the initial `Db`, construct settings, and attach any shared infrastructure
to the database builder.

`StartupCtx` exposes:

- `path()`
- `main_object_store()`
- `wal_object_store()`
- `system_clock()`
- `fp_registry()`
- `rand()`

Typical startup responsibilities:

- derive a deterministic SlateDB seed with `ctx.rand().rng().next_u64()`
- build randomized deterministic settings with `utils::build_settings(...)`
- open `Db::builder(...)` using the harness-provided object stores and clock
- pass through the shared failpoint registry

### `ActorCtx`

Each registered actor instance receives its own `ActorCtx`.

`ActorCtx` exposes:

- `role()` and `instance()` for actor identity
- `rand()` for actor-local deterministic randomness
- `db()` to read the currently installed shared `Arc<Db>`
- `swap_db(new_db)` to replace the shared DB handle for all actors
- `advance_time(duration)` to move the shared mock clock forward
- `failures()` to configure object-store faults
- `path()`, `main_object_store()`, `wal_object_store()`
- `system_clock()` and `fp_registry()`

`swap_db(...)` is useful for reopen scenarios and tests that intentionally
replace the database instance mid-run.

## Failure injection

The harness wraps the configured object stores with a shared
`FailingObjectStoreController`. Every actor sees the same controller through
`ctx.failures()`, so changes to the fault configuration affect the main store
and optional WAL store consistently.

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
- `SlowClose { delay }`: delays stream shutdown on the response side

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

ctx.failures().set_http_fail_before(HttpFailBefore {
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

## `utils::build_settings`

`utils::build_settings(rand, cache_root)` produces a randomized deterministic
`slatedb::Settings` value from a supplied `DbRand`.

It currently randomizes several useful dimensions of a SlateDB run, including:

- flush interval
- manifest polling and update timeouts
- bloom filter configuration
- L0 SST size and count thresholds
- maximum unflushed bytes
- compression codec selection
- garbage collection intervals and minimum ages
- whether the object-store cache is enabled

Because the settings are derived entirely from the supplied RNG, they expand
scenario coverage without sacrificing reproducibility.

## Guidance for writing stable scenarios

- Seed everything through the harness and avoid unseeded randomness.
- Use the harness-provided clock for time-sensitive behavior.
- Prefer `DeterministicLocalFilesystem` over ad hoc local filesystem wrappers.
- Install object-store faults through `ctx.failures()` so they remain visible to
  every actor.
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
