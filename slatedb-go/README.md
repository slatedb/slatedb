# SlateDB Go Bindings

Go bindings for [SlateDB](https://slatedb.io), implemented over the `slatedb-c` FFI layer.

## Overview

This package lives in `slatedb-go/go` and exposes a Go API for:

- Opening a writable `DB`
- Opening a read-only `DbReader`
- Point reads/writes and scans
- Batched writes
- Settings-driven builder configuration

## Prerequisites

- Rust toolchain from the repository (`rust-toolchain.toml`)
- Go 1.25+
- CGO enabled
- C toolchain (`cc`)

## Build

### 1. Build `slatedb-c`

From repository root:

```bash
cargo build -p slatedb-c
# or
cargo build --release -p slatedb-c
```

### 2. Build / Test Go bindings

```bash
cd slatedb-go/go
go build
go test ./...
```

The Go package links `libslatedb_c` from `target/debug` and `target/release`.

### 3. Runtime library path (shared library)

If you run binaries that dynamically link `libslatedb_c`, ensure the loader can find it:

```bash
# From repository root
export DYLD_LIBRARY_PATH="$(pwd)/target/debug:$(pwd)/target/release"  # macOS
export LD_LIBRARY_PATH="$(pwd)/target/debug:$(pwd)/target/release"    # Linux
```

## Object Store Configuration

`Open`, `OpenReader`, and `Builder.Build` all resolve object-store config the same way:

1. If `WithUrl(...)` is provided, call `slatedb_object_store_from_url`.
2. Else if `SLATEDB_OBJECT_STORE_URL` or `OBJECT_STORE_URL` is set (process env first, then optional env file), call `slatedb_object_store_from_url`.
3. Else call `slatedb_object_store_from_env` (provider-driven config via `CLOUD_PROVIDER` and related vars in `slatedb::admin`).

`WithEnvFile(...)` is passed through to `slatedb_object_store_from_env` and uses the same dotenv loading semantics as Rust SlateDB.

## Basic Usage

```go
import (
    "io"
    "slatedb.io/slatedb-go"
)

// Explicit object-store URL (recommended)
db, err := slatedb.Open("/tmp/cache", slatedb.WithUrl[slatedb.DbConfig]("memory:///"))
if err != nil {
    panic(err)
}
defer db.Close()

_ = db.Put([]byte("key"), []byte("value"))
val, err := db.Get([]byte("key"))
_ = val
_ = err

iter, err := db.Scan([]byte("k"), nil)
if err != nil {
    panic(err)
}
defer iter.Close()

for {
    kv, err := iter.Next()
    if err == io.EOF {
        break
    }
    if err != nil {
        panic(err)
    }
    _ = kv
}
```

## Builder + Settings

```go
builder, _ := slatedb.NewBuilder("/tmp/cache")

settings := &slatedb.Settings{
    FlushInterval: "50ms",
    L0SstSizeBytes: 128 * 1024 * 1024,
}

db, err := builder.
    WithUrl("memory:///").
    WithSettings(settings).
    WithSstBlockSize(slatedb.SstBlockSize16Kib).
    Build()
if err != nil {
    panic(err)
}
defer db.Close()
```

Settings helpers:

- `SettingsDefault()`
- `SettingsFromFile(path)` (`.json`, `.toml`, `.yaml`, `.yml`)
- `SettingsFromEnv(prefix)`
- `SettingsLoad()`
- `MergeSettings(base, override)`

## WriteBatch

```go
batch, err := slatedb.NewWriteBatch()
if err != nil {
    panic(err)
}
defer batch.Close()

_ = batch.Put([]byte("k1"), []byte("v1"))
_ = batch.Delete([]byte("old"))

// Consumes the batch (cannot be reused)
_ = db.Write(batch)
```

## DbReader

```go
reader, err := slatedb.OpenReader(
    "/tmp/cache",
    slatedb.WithUrl[slatedb.DbReaderConfig]("memory:///"),
    slatedb.WithDbReaderOptions(slatedb.DbReaderOptions{
        ManifestPollInterval: 5000,
        CheckpointLifetime:   300000,
    }),
)
if err != nil {
    panic(err)
}
defer reader.Close()

value, err := reader.Get([]byte("key"))
_ = value
_ = err
```

You can also provide `WithCheckpointId(uuidString)` for checkpoint-bound reads.

## Logging

```go
_ = slatedb.InitLogging(slatedb.LogLevelInfo)
_ = slatedb.SetLoggingLevel(slatedb.LogLevelDebug)
```

## API Quick Reference

### Core DB

- `Open(path string, opts ...Option[DbConfig]) (*DB, error)`
- `(*DB).Put(key, value []byte) error`
- `(*DB).Get(key []byte) ([]byte, error)`
- `(*DB).Delete(key []byte) error`
- `(*DB).Flush() error`
- `(*DB).Close() error`

### Optioned DB operations

- `(*DB).PutWithOptions(key, value []byte, putOpts *PutOptions, writeOpts *WriteOptions) error`
- `(*DB).GetWithOptions(key []byte, readOpts *ReadOptions) ([]byte, error)`
- `(*DB).DeleteWithOptions(key []byte, writeOpts *WriteOptions) error`

### Batch

- `NewWriteBatch() (*WriteBatch, error)`
- `(*WriteBatch).Put(key, value []byte) error`
- `(*WriteBatch).PutWithOptions(key, value []byte, opts *PutOptions) error`
- `(*WriteBatch).Delete(key []byte) error`
- `(*DB).Write(batch *WriteBatch) error`
- `(*DB).WriteWithOptions(batch *WriteBatch, opts *WriteOptions) error`

### Scan / Iterator

- `(*DB).Scan(start, end []byte) (*Iterator, error)`
- `(*DB).ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error)`
- `(*DB).ScanPrefix(prefix []byte) (*Iterator, error)`
- `(*DB).ScanPrefixWithOptions(prefix []byte, opts *ScanOptions) (*Iterator, error)`
- `(*Iterator).Next() (KeyValue, error)`
- `(*Iterator).Seek(key []byte) error`
- `(*Iterator).Close() error`

### Reader

- `OpenReader(path string, opts ...Option[DbReaderConfig]) (*DbReader, error)`
- `(*DbReader).Get(key []byte) ([]byte, error)`
- `(*DbReader).GetWithOptions(key []byte, opts *ReadOptions) ([]byte, error)`
- `(*DbReader).Scan(start, end []byte) (*Iterator, error)`
- `(*DbReader).ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error)`
- `(*DbReader).ScanPrefix(prefix []byte) (*Iterator, error)`
- `(*DbReader).ScanPrefixWithOptions(prefix []byte, opts *ScanOptions) (*Iterator, error)`
- `(*DbReader).Close() error`

## Environment Variables

Example `.env` for local filesystem:

```bash
CLOUD_PROVIDER=local
LOCAL_PATH=/
```

Example `.env` for AWS S3:

```bash
CLOUD_PROVIDER=aws
AWS_BUCKET=my-slate-bucket
AWS_REGION=us-west-2
# Optional depending on your environment/auth setup:
# AWS_ENDPOINT=https://s3.us-west-2.amazonaws.com
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
```

Direct URL override:

```bash
SLATEDB_OBJECT_STORE_URL=memory:///
# or
OBJECT_STORE_URL=file:///tmp/slatedb
```
