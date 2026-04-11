# SlateDB Go Binding

Official Go binding for SlateDB.

## Install

```bash
go get slatedb.io/slatedb-go
```

## Runtime Requirements

This binding uses cgo and links against the `slatedb_uniffi` shared library. To
build and run code that imports it you need:

- Go 1.25 or newer
- `CGO_ENABLED=1`
- a working C toolchain
- the `slatedb_uniffi` shared library on your platform loader path

If you are working from this repository, that usually means building
`slatedb-uniffi` and pointing `LD_LIBRARY_PATH` or `DYLD_LIBRARY_PATH` at
`target/debug` or `target/release`.

## Quick Start

```go
package main

import (
	"bytes"
	"fmt"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func main() {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		panic(err)
	}
	defer store.Destroy()

	builder := slatedb.NewDbBuilder("example-db", store)
	defer builder.Destroy()

	db, err := builder.Build()
	if err != nil {
		panic(err)
	}
	defer db.Destroy()

	if _, err := db.Put([]byte("hello"), []byte("world")); err != nil {
		panic(err)
	}

	value, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	if value == nil || !bytes.Equal(*value, []byte("world")) {
		panic("unexpected value")
	}

	fmt.Println(string(*value))

	if err := db.Shutdown(); err != nil {
		panic(err)
	}
}
```

## API Model

- `ObjectStoreResolve(...)` opens an object store from a URL such as
  `memory:///`
- `DbBuilder` opens a writable database and `DbReaderBuilder` opens a read-only
  reader
- `Db`, `DbReader`, and `DbSnapshot` support point reads, range scans, and
  prefix scans
- `Db.Begin(...)` opens a transaction at a chosen isolation level
- handle types own Rust-side resources, so call `Shutdown()` on databases and
  readers and `Destroy()` on handles when you are done with them

## Metrics

The Go binding exposes both custom metrics callbacks and the built-in
`DefaultMetricsRecorder`:

- `DbBuilder.WithMetricsRecorder(...)`
- `DbReaderBuilder.WithMetricsRecorder(...)`
- `NewDefaultMetricsRecorder()`
- `MetricByNameAndLabels(...)`

## Development

Maintainer workflows for regenerating bindings, running Go tests, and other
repository-specific tasks live in [DEVELOPMENT.md](DEVELOPMENT.md).
