# SlateDB UniFFI Go Bindings

This module contains the raw Go bindings generated from `bindings/ffi` with
`uniffi-bindgen-go`. It is intentionally separate from the existing handwritten
bindings in `slatedb-go`.

## Requirements

- Rust toolchain from the repository
- Go 1.24+
- CGO enabled
- `uniffi-bindgen-go` installed if you want to regenerate bindings

## Regenerate Bindings

From `bindings/go`:

```bash
./generate.sh
```

Or:

```bash
go generate ./...
```

`generate.sh` builds `slatedb-ffi`, runs `uniffi-bindgen-go` in library mode,
and copies the generated `slatedb.go` and `slatedb.h` into this module.

## Test

From repository root:

```bash
cargo build -p slatedb-ffi
cd bindings/go
go test ./...
```

The package links `libslatedb_ffi` from `../../target/debug` and
`../../target/release` via `#cgo` directives in `link.go`.

## API Notes

- The exported API mirrors the UniFFI surface from `bindings/ffi`.
- Rust async functions and methods are exposed as blocking Go calls.
- `Db` should be shut down with `Shutdown()`.
- FFI-backed helper handles such as `ObjectStore`, `DbIterator`,
  `DbSnapshot`, and `DbTransaction` should be released with `Destroy()`.
