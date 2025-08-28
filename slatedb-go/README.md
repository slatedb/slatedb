# SlateDB Go Bindings

Go bindings for [SlateDB](https://slatedb.io) - a cloud-native embedded key-value store built on top of object storage.

## Overview

This package provides Go language bindings for SlateDB through a Foreign Function Interface (FFI) layer.

## Prerequisites

- Rust toolchain (nightly-2025-04-12)
- Go 1.19+ with CGO enabled
- Build tools (gcc, pkg-config)

## Directory Structure

```
slatedb-go/
├── README.md           # This file
├── Cargo.toml          # Rust crate configuration  
├── cbindgen.toml       # C header generation config
├── build.rs            # Build script for header generation
├── src/
│   ├── lib.rs          # Module coordination and re-exports
│   ├── db.rs           # Main database FFI functions
│   ├── db_reader.rs    # DbReader FFI implementation
│   ├── iterator.rs     # Scan and iterator FFI functions  
│   ├── batch.rs        # WriteBatch FFI functions
│   ├── config.rs       # JSON configuration parsing
│   ├── error.rs        # Error handling and conversion
│   ├── types.rs        # FFI type definitions
│   ├── object_store.rs # Object store creation
│   └── memory.rs       # Memory management utilities
├── go/
│   ├── go.mod          # Go module
│   ├── config.go       # Configuration structs and options
│   ├── db.go           # Main database operations
│   ├── db_reader.go    # Read-only database access
│   ├── iterator.go     # Iterator methods (Next, Seek, Close)
│   ├── batch.go        # WriteBatch operations
│   ├── slatedb.h       # Generated C header (auto-generated)
│   ├── db_test.go      # Database operation tests
│   ├── batch_test.go   # WriteBatch operation tests
│   ├── iterator_test.go # Iterator method tests
│   └── suite_test.go   # Test suite setup
```

## Building

### 1. Build Rust FFI Library

```bash
# From the slatedb root directory
cd slatedb  # The main slatedb directory
cargo build --release -p slatedb-go
```

### 2. Build Go Bindings

```bash
# From the slatedb-go/go directory
cd slatedb-go/go

# Set library path for CGO
export CGO_LDFLAGS="-L$(pwd)/../../target/release"

# Build Go bindings
go build

# Run tests
go test -v
```

### 3. Runtime Library Path

For running programs that use SlateDB:

```bash
# From the slatedb root directory
export DYLD_LIBRARY_PATH="$(pwd)/target/release"  # macOS
export LD_LIBRARY_PATH="$(pwd)/target/release"    # Linux

# Then run your program
./your-program
```


### Basic Usage

```go
import (
    "io"
    "slatedb.io/slatedb-go"
)

// Local storage (development)
db, _ := slatedb.Open("/tmp/cache", &slatedb.StoreConfig{
    Provider: slatedb.ProviderLocal,
}, nil)
defer db.Close()

// S3 (production)
db, _ := slatedb.Open("/tmp/cache", &slatedb.StoreConfig{
    Provider: slatedb.ProviderAWS,
    AWS: &slatedb.AWSConfig{
        Bucket: "bucket",
        Region: "us-west-2",
    },
}, nil)
defer db.Close()

// Environment variables (automatic fallback)
db, _ := slatedb.Open("/tmp/cache", nil, nil) // nil = use environment
defer db.Close()

// Basic operations
db.Put([]byte("key"), []byte("value"))
value, _ := db.Get([]byte("key"))

// Range scanning with iterator  
iter, _ := db.Scan([]byte("prefix:"), []byte("prefix;"))
defer iter.Close()
for {
    kv, err := iter.Next()
    if err == io.EOF { break }
    // Process kv.Key and kv.Value
}

// Scanning with custom options
opts := &slatedb.ScanOptions{
    DurabilityFilter: slatedb.DurabilityRemote,
    ReadAheadBytes:   1024,
}
iter, _ := db.ScanWithOptions([]byte("prefix:"), []byte("prefix;"), opts)
defer iter.Close()
for {
    kv, err := iter.Next()
    if err == io.EOF { break }
    // Process kv.Key and kv.Value
}
```

### Advanced Configuration

```go
opts := &slatedb.SlateDBOptions{
    L0SstSizeBytes: 128 * 1024 * 1024,     // 128MB (default: 64MB)
    FlushInterval:  50 * time.Millisecond, // 50ms (default: 100ms)
    CacheFolder:    "/tmp/sst-cache",      // Local SST cache
    SstBlockSize:   slatedb.SstBlockSize8Kib, // 8KiB blocks
    CompactorOptions: &slatedb.CompactorOptions{
        PollInterval:            30 * time.Second, // Compaction check interval
        MaxSSTSizeBytes:         256 * 1024 * 1024, // 256MB max SST size
        MaxConcurrentCompactions: 4,                // Parallel compactions
    },
}
db, _ := slatedb.Open("/tmp/cache", &slatedb.StoreConfig{
    Provider: slatedb.ProviderAWS,
    AWS: &slatedb.AWSConfig{
        Bucket: "bucket",
        Region: "us-west-2",
    },
}, opts)
```

### Batch Operations

```go
// Atomic multi-operation transactions
batch, _ := slatedb.NewWriteBatch()
defer batch.Close()

batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
batch.Delete([]byte("old_key"))

// Execute all operations atomically
db.Write(batch)

// With custom options
opts := &slatedb.WriteOptions{AwaitDurable: false}
db.WriteWithOptions(batch, opts)
```

### Read-Only Database Access (DbReader)

```go
// DbReader provides concurrent read-only access
reader, _ := slatedb.OpenReader("/tmp/cache", &slatedb.StoreConfig{
    Provider: slatedb.ProviderLocal,
}, nil, nil)
defer reader.Close()

// All read operations available
value, _ := reader.Get([]byte("key"))
value, _ := reader.GetWithOptions([]byte("key"), &slatedb.ReadOptions{
    DurabilityFilter: slatedb.DurabilityRemote,
})

// Scanning with DbReader
iter, _ := reader.Scan([]byte("prefix:"), []byte("prefix;"))
defer iter.Close()
for {
    kv, err := iter.Next()
    if err == io.EOF { break }
    // Process read-only data
}

// DbReader with custom scan options
iter, _ := reader.ScanWithOptions([]byte("prefix:"), []byte("prefix;"), 
    &slatedb.ScanOptions{DurabilityFilter: slatedb.DurabilityRemote})
defer iter.Close()
for {
    kv, err := iter.Next()
    if err == io.EOF { break }
    // Process read-only data
}

// Reading from specific checkpoint
checkpointReader, _ := slatedb.OpenReader("/tmp/cache", storeConfig, 
    &checkpointId, &slatedb.DbReaderOptions{
        ManifestPollInterval: 5000,     // 5 seconds
        CheckpointLifetime:   300000,   // 5 minutes  
        MaxMemtableBytes:     67108864, // 64MB
    })
```

## API Reference

SlateDB Go bindings provide a clean, structured API organized into the following categories:

### Core Operations
- `Put(key, value []byte) error` - Store a key-value pair
- `Get(key []byte) ([]byte, error)` - Retrieve a value by key  
- `Delete(key []byte) error` - Remove a key

### Operations with Options
- `PutWithOptions(key, value []byte, putOpts *PutOptions, writeOpts *WriteOptions) error`
- `GetWithOptions(key []byte, readOpts *ReadOptions) ([]byte, error)`
- `DeleteWithOptions(key []byte, writeOpts *WriteOptions) error`

### Batch Operations
- `NewWriteBatch() (*WriteBatch, error)` - Create a new batch
- `Write(batch *WriteBatch) error` - Execute batch with default options
- `WriteWithOptions(batch *WriteBatch, opts *WriteOptions) error` - Execute with custom options

### Scan and Iterator
- `Scan(start, end []byte) (*Iterator, error)` - Create range iterator with default options
- `ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error)` - Create range iterator with custom options
- `Iterator.Next() (*KeyValue, error)` - Get next key-value pair
- `Iterator.Seek(key []byte) error` - Seek to specific key
- `Iterator.Close() error` - Close iterator

### Database Management  
- `Open(path string, storeConfig *StoreConfig, opts *SlateDBOptions) (*DB, error)`
- `Close() error` - Close database connection
- `Flush() error` - Flush pending writes to storage

### Read-Only Access (DbReader)
- `OpenReader(path string, storeConfig *StoreConfig, checkpointId *string, opts *DbReaderOptions) (*DbReader, error)`
- All read operations: `Get()`, `GetWithOptions()`, `Scan()`, `ScanWithOptions()`
- Same iterator API as main database

## Configuration

See `go doc` for complete API reference. Key types:

```go
type SlateDBOptions struct {
    L0SstSizeBytes   uint64            // Default: 64MB
    FlushInterval    time.Duration     // Default: 100ms
    CacheFolder      string            // Local SST cache path
    SstBlockSize     SstBlockSize      // Default: 4KiB
    CompactorOptions *CompactorOptions // Compaction settings
}

type CompactorOptions struct {
    PollInterval              time.Duration // Default: 5s
    ManifestUpdateTimeout     time.Duration // Default: 300s
    MaxSSTSizeBytes          uint64        // Default: 256MB
    MaxConcurrentCompactions uint32        // Default: 4
}

type StoreConfig struct {
    Provider Provider   // ProviderLocal, ProviderAWS
    AWS      *AWSConfig // AWS S3 configuration
}

type AWSConfig struct {
    Bucket         string        // S3 bucket name
    Region         string        // AWS region
    Endpoint       string        // Custom S3 endpoint (optional)
    RequestTimeout time.Duration // HTTP timeout (optional)
}

type DbReaderOptions struct {
    ManifestPollInterval uint64 // How often to poll for updates (ms)
    CheckpointLifetime   uint64 // How long checkpoints live (ms)
    MaxMemtableBytes     uint64 // Memory buffer size for WAL replay
}

type ScanOptions struct {
    DurabilityFilter DurabilityLevel // Filter for scan durability
    Dirty            bool            // Include uncommitted writes
    ReadAheadBytes   uint64          // Buffer size for read-ahead
    CacheBlocks      bool            // Whether to cache blocks
}
```



## Environment Variables

Automatic object store configuration via environment variables (fallback when `storeConfig` is `nil`):

```bash
# AWS
export CLOUD_PROVIDER=aws
export AWS_BUCKET=bucket AWS_REGION=us-west-2
export AWS_ACCESS_KEY_ID=key AWS_SECRET_ACCESS_KEY=secret
```
