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
`target/debug` or `target/release`. See [DEVELOPMENT.md](DEVELOPMENT.md) for
more details.

Writable databases and readers enter a dedicated multi-threaded Tokio runtime
while opening so SlateDB's long-lived background tasks capture a multi-threaded
Tokio handle. Foreground binding calls are still made through the normal Go API.
By default, the runtime uses the host's available parallelism. Set
`SLATEDB_UNIFFI_RUNTIME_THREADS` to a positive integer before the first
`DbBuilder.Build()` or `DbReaderBuilder.Build()` call to override the worker
thread count for the lifetime of the process.

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

## Local Object Mirror

`ObjectStoreMirrorBuilder` wraps an object store and keeps the compacted SSTs of one database on
local disk. The database then reads those SSTs from local disk. WAL SSTs and manifests stay in the
wrapped store only.

```go
remote, err := slatedb.ObjectStoreResolve("s3://my-bucket")
if err != nil {
	panic(err)
}
defer remote.Destroy()

mirrorBuilder := slatedb.NewObjectStoreMirrorBuilder("/var/lib/slatedb/mirror", remote)
defer mirrorBuilder.Destroy()
mirror, err := mirrorBuilder.Build()
if err != nil {
	panic(err)
}
defer mirror.Destroy()

builder := slatedb.NewDbBuilder("demo-db", mirror)
defer builder.Destroy()
```

- Build one mirror for each database, and use it each time you open that database.
- Each mirror locks its local directory. `build` fails with an `Unavailable` error if another
  mirror holds the lock. The lock is released after the mirror store and all handles opened with
  it are destroyed.
- The local volume must hold the whole database. The mirror does not evict SSTs.
- Create a clone with the wrapped store, not with a mirror. Then open the clone with its own
  mirror.

### Mirrored Segments

By default, the mirror keeps all segments. Give it a `SegmentFilter` to keep only some segments.
The database reads the other segments from the wrapped store.

```go
type hotSegments struct{}

func (hotSegments) ShouldMirror(segment []byte) (bool, error) {
	return string(segment) != "cold", nil
}

if err := mirrorBuilder.WithSegmentFilter(hotSegments{}); err != nil {
	panic(err)
}
```

- The mirror keeps one answer for each segment and manifest version. It asks again when the
  database writes a new manifest, at a flush or a compaction.
- The empty prefix is the root segment. A database without a segment extractor has only the root
  segment, so `false` for it turns the mirror off.
- A changed answer takes effect at the next manifest. Then the mirror downloads the SSTs of a
  segment that it starts to keep, and removes the local copies of a segment that it stops keeping.
- While those downloads run, reads of the segment can fail with a `Data` error.
- An error from the filter counts as `false` for that manifest version. The mirror logs a warning.
- The filter blocks a runtime thread that all databases share, so it must return quickly.

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
