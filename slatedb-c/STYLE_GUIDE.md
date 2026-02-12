# SlateDB C Style Guide

This guide defines coding and API conventions for the `slatedb-c` package.

## Goals

- Keep the C ABI close to SlateDB Rust semantics.
- Prefer explicit ownership and lifecycle rules.
- Keep APIs C-idiomatic and stable.
- Make invalid usage fail fast with clear `SLATEDB_ERROR_KIND_INVALID` errors.

## ABI Surface and Naming

- Public C entrypoints use `slatedb_*` snake_case names.
- Public C structs/types use `slatedb_*_t`.
- Public C constants use `SLATEDB_*` upper snake case.
- Keep parameter ordering consistent:
  - Required inputs first
  - Optional config pointer(s)
  - Optional output indicator and values in this order:
    - `out_present`
    - `out_X`
    - `out_X_len` (if applicable)

## Optional Output Convention

- Any optional return value is represented by:
  - `bool *out_present`
  - one or more value out params (`out_val`, `out_key`, etc.)
- Do not use mixed names like `out_found` or `out_has_item`.
- On entry, initialize optional outputs to an empty state:
  - `*out_present = false`
  - pointers to `NULL`
  - lengths to `0`

## Errors and Results

- Every FFI function returns `slatedb_result_t`.
- Success is `SLATEDB_ERROR_KIND_NONE`.
- SlateDB errors map via `map_error`/`error_from_slate_error`.
- Closed errors must preserve `close_reason`.
- Human-readable messages are allocated by Rust and freed with `slatedb_result_free`.

## Pointer Validation

- Use shared helpers for pointer checks:
  - `require_handle(...)`
  - `require_out_ptr(...)`
- Use `bytes_from_ptr(...)` for `(ptr,len)` byte inputs.
- Treat null pointers and invalid handles as `SLATEDB_ERROR_KIND_INVALID`.

## Write Batch Handling

- Use centralized helpers:
  - `with_write_batch_mut(...)` for mutable batch operations.
  - `take_write_batch(...)` when ownership transfer is required.
- `slatedb_db_write*` consume the write batch handle’s inner batch on call (regardless of write outcome).
- A consumed batch must return `"write batch has been consumed"` with invalid kind.

## Ownership and Memory

- Opaque handles (`db`, `builder`, `iterator`, `object_store`, `write_batch`) are heap-allocated Rust objects.
- Handle free APIs (`*_close`) must reclaim with `Box::from_raw`.
- Byte buffers returned to callers are Rust-allocated; callers must free with `slatedb_bytes_free`.

## Unsafe and Safety Docs

- Public FFI functions that dereference raw pointers must be `pub unsafe extern "C" fn`.
- Every unsafe public API must document `## Safety` requirements.
- Keep `///` docs above attributes (for example above `#[no_mangle]`).
- Include `## Arguments`, `## Returns`, `## Errors`, and `## Safety` where applicable.

## Selector Fields and Constants

- Keep wire fields as fixed-width integers (`u8`, etc.) in FFI structs.
- Expose symbolic constants for selectors (do not rely on magic numbers), e.g.:
  - durability filter
  - TTL type
  - flush type
  - bound kind
  - SST block size
- Validate selector values explicitly and return invalid errors for unknown values.

## Refactoring Rules

- Eliminate repeated pointer checks via shared helpers.
- Centralize repeated lifecycle logic (for example write-batch consumed-state checks).
- Keep extern wrappers thin and move shared logic into internal helpers when practical.
- Maintain existing public ABI names unless a deliberate breaking change is approved.

## Documentation and Header Generation

- Add `//!` module docs to each source file.
- Add `///` docs to public items and critical internal helpers.
- Ensure cbindgen exports new public constants/types through `cbindgen.toml`.
- Generated header is `slatedb-c/include/slatedb.h`.

## Testing

- Prefer integration tests in `slatedb-c/tests/` that call only public APIs exported by `slatedb_c`.
- Cover all user-facing functions with at least one happy-path assertion.
- Add targeted negative tests for:
  - null required pointers
  - invalid selector values
  - consumed-handle behavior (e.g. write batches after `slatedb_db_write*`)
- Validate ownership contracts in tests:
- When optional outputs are used, assert all states:
  - `out_present = true` with valid output buffers
  - `out_present = false` with null/zero outputs
- Keep test names explicit (`test_db_*`, `test_builder_*`, etc.) and align files with API areas (`db.rs`, `builder.rs`, etc.) when splitting suites.
- Test one logical behavior per test function (for example, `test_db_put` should cover a successful put, while `test_db_put_invalid_key` should cover invalid key handling).

## Quality Gate

For behavior/code changes in `slatedb-c`, run:

1. `cargo fmt --all`
2. `cargo clippy -p slatedb-c --all-targets`
3. `cargo test -p slatedb-c`
