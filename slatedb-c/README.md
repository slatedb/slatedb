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
