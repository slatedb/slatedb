# SlateDB C Bindings

C FFI bindings for [SlateDB](https://slatedb.io). This crate builds the C ABI
library (`libslatedb_c`) and generates the C header via cbindgen.

## Building

```bash
# From the slatedb root directory
cargo build --release -p slatedb-c
```

Artifacts:

- Header: `slatedb-c/include/slatedb.h`
- Library: `target/release/libslatedb_c.{a,dylib,so}`

## Usage (C/C++)

- Add `slatedb-c/include` to your include path.
- Link against `libslatedb_c` from `target/release`.
- Ensure your runtime library path can find `libslatedb_c`.

## Logging

- Initialize process-global logging with `slatedb_logging_init(...)`.
- Update filtering with `slatedb_logging_set_level(...)`.
- Register a callback with `slatedb_logging_set_callback(...)` and remove it
  with `slatedb_logging_clear_callback(...)`.
- Callback string pointers are borrowed and valid only for the callback
  duration.
