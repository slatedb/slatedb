# Deterministic Simulation Testing (DST)

This crate provides deterministic simulation tools and tests for SlateDB.

## Design

SlateDB's `Dst` struct is a scenario runner that executes multiple named
`!Send` async local tasks against one shared `Db`. Scenarios interact through a
cloneable `ScenarioContext`, which provides:

- checked mutating helpers for `put`, `delete`, `write_batch`, `flush`, and
  explicit clock advancement
- barrier-checked `get` and `scan` helpers that compare SlateDB results against
  a SQLite oracle
- scan validation that honors `ScanOptions.order`, including descending scans
- a run-scoped `shutdown_token()` that long-lived scenarios can observe, and
  cancel, to shut down the entire active `run_scenarios()` invocation
- a raw `db()` escape hatch for unchecked operations outside the oracle contract

The SQLite oracle tracks committed visibility, remote durability, tombstones,
and exact read metadata. It preserves `expire_ts` metadata on reads, but it
does not yet model flush-time TTL-to-tombstone rewrites, so the bundled DST
simulation avoids TTL writes for now. Checked reads run only at explicit
quiescent barriers, so mutating operations can still overlap while the harness
compares deterministic snapshots.

Everything in DST is deterministic, including the system clock, the runtime,
and any explicit database seed used by the caller. This means a failed test can
be re-run with the same seed to reproduce the failure.

See the crate API for `Scenario`, `ScenarioContext`, `Dst`, and
`utils::build_scenario_db`.

Scenarios may override `Scenario::runs_forever()` to indicate that they should
stay alive until the shared shutdown token is cancelled instead of defining
normal completion for the run.

## Usage

All of SlateDB's deterministic simulation tests are gated behind a `cfg(dst)`
attribute. The tests require a deterministic Tokio runtime, which must also
be enabled with `cfg(tokio_unstable)` and Tokio's `rt` feature.

To run DST tests, you must set `RUSTFLAGS` to include `--cfg dst` and
`--cfg tokio_unstable`.

```bash
$ RUSTFLAGS="--cfg dst --cfg tokio_unstable" \
  RUSTDOCFLAGS="--cfg tokio_unstable" \
  cargo test -p slatedb-dst --all-features
```

Or if you prefer nextest:

```bash
$ RUSTFLAGS="--cfg dst --cfg tokio_unstable" \
  RUSTDOCFLAGS="--cfg tokio_unstable" \
  cargo nextest run -p slatedb-dst --profile dst
```

## Example

```rust,ignore
use std::sync::Arc;

use object_store::memory::InMemory;
use slatedb::config::Settings;
use slatedb_common::clock::MockSystemClock;
use slatedb_dst::utils::{build_runtime, build_scenario_db};
use slatedb_dst::{Dst, Scenario, ScenarioContext};

#[async_trait::async_trait(?Send)]
impl Scenario for MyScenario {
    fn name(&self) -> &'static str {
        "my-scenario"
    }

    async fn run(&self, ctx: ScenarioContext) -> Result<(), slatedb::Error> {
        ctx.put(b"key", b"value", &Default::default()).await?;
        let _ = ctx.checked_get(b"key", &Default::default()).await?;
        Ok(())
    }
}

let runtime = build_runtime(42);
runtime.block_on(async {
    let settings = Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        ..Default::default()
    };
    let clock = Arc::new(MockSystemClock::new());
    let db = build_scenario_db(Arc::new(InMemory::new()), clock.clone(), 42, settings.clone())
        .await
        .unwrap();
    let dst = Dst::new(db, clock, settings).unwrap();
    dst.run_scenarios(vec![Box::new(MyScenario)]).await.unwrap();
});
```
