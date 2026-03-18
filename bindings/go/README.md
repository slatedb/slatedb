# SlateDB Go UniFFI Bindings

This module contains low-level Go bindings generated from `bindings/uniffi` with UniFFI.

The generated API lives in `slatedb.io/slatedb-go/ffi`. The module root is intentionally left open for a future idiomatic wrapper layer.

## Prerequisites

- Rust toolchain from this repository
- Go 1.25+
- CGO enabled
- A working C toolchain
- `uniffi-bindgen-go` `v0.5.0+v0.29.5`

Install the generator with:

```bash
cargo install --git https://github.com/NordSecurity/uniffi-bindgen-go.git --tag v0.5.0+v0.29.5 uniffi-bindgen-go
```

## Regenerate Bindings

Run from the repository root:

```bash
./scripts/generate_go_bindings.sh
```

Optional environment variables:

- `UNIFFI_BINDGEN_GO_BIN`: path to the generator binary
- `CARGO_PROFILE`: `debug` or `release` (`debug` by default)
- `GO_BINDINGS_OUT_DIR`: alternate output directory (`bindings/go/ffi` by default)

Generated files are checked in. Normal `go build` and `go test` do not require the generator unless you are regenerating the low-level bindings.

## Build and Test

Build the Rust library first:

```bash
cargo build -p slatedb-uniffi
```

Then run Go tests from `bindings/go`:

```bash
go test ./...
```

## Runtime Library Loading

- macOS: ensure `target/debug` or `target/release` is present in `DYLD_LIBRARY_PATH`
- Linux: ensure `target/debug` or `target/release` is present in `LD_LIBRARY_PATH`
- Windows: ensure the matching `target\debug` or `target\release` directory is present in `PATH`

Windows regeneration requires a bash-compatible shell such as Git Bash, MSYS2, or WSL.

## Layout

- `ffi/`: generated low-level Go package plus minimal cgo glue
- `../uniffi/`: Rust UniFFI source crate used as the generation source of truth
- `../../scripts/generate_go_bindings.sh`: reproducible regeneration entrypoint

## Minimal Example

```go
package main

import (
	"log"

	"slatedb.io/slatedb-go/ffi"
)

func main() {
	store, err := ffi.ObjectStoreResolve("memory:///")
	if err != nil {
		log.Fatal(err)
	}

	builder := ffi.NewDbBuilder("/tmp/slatedb-go-cache", store)
	db, err := builder.Build()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Shutdown()

	if _, err := db.Put([]byte("key"), []byte("value")); err != nil {
		log.Fatal(err)
	}
}
```
