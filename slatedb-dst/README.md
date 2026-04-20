# SlateDB DST Harness

`slatedb-dst` contains the deterministic test harness and DST-specific object
store wrappers for SlateDB.

The harness owns:

- a seeded current-thread Tokio runtime
- a shared `MockSystemClock`
- a shared `FailPointRegistry`
- deterministic main and optional WAL object-store stacks
- a shared installed `Arc<slatedb::Db>` slot with atomic `swap_db`
- actor-local `slatedb::DbRand` instances derived from the root seed

The crate is only available when Tokio unstable runtime seeding is enabled:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo test -p slatedb-dst --lib
```

Scenario tests that also rely on `slatedb`'s `cfg(dst)` behavior can add
`--cfg dst` on top of that when needed.
