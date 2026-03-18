// Package uniffi exposes SlateDB's UniFFI-generated Go bindings.
//
// The module import path is:
//
//	import "slatedb.io/slatedb-go"
//
// The package name is `uniffi`. The API is intentionally close to the Rust
// UniFFI surface rather than a fully idiomatic handwritten Go wrapper, so most
// types, option structs, and lifecycle methods map directly to the underlying
// SlateDB API.
//
// # Runtime Requirements
//
// This package uses cgo and links against the shared library produced by the
// repository's `slatedb-uniffi` crate. To build and run code that imports this
// package you need:
//
//   - Go 1.25 or newer
//   - `CGO_ENABLED=1`
//   - a working C toolchain
//   - the `slatedb_uniffi` shared library available to the platform loader
//
// During local development that usually means building the Rust shared library
// from the repository and adding `target/debug` or `target/release` to
// `LD_LIBRARY_PATH` on Linux or `DYLD_LIBRARY_PATH` on macOS.
//
// # Opening A Database
//
// Most programs start by constructing an [ObjectStore], then using a
// [DbBuilder] to open a writable [Db]:
//
//	store, err := uniffi.ObjectStoreResolve("memory:///")
//	if err != nil {
//		panic(err)
//	}
//	defer store.Destroy()
//
//	builder := uniffi.NewDbBuilder("example-db", store)
//	defer builder.Destroy()
//
//	db, err := builder.Build()
//	if err != nil {
//		panic(err)
//	}
//	defer db.Destroy()
//
//	if _, err := db.Put([]byte("hello"), []byte("world")); err != nil {
//		panic(err)
//	}
//
//	value, err := db.Get([]byte("hello"))
//	if err != nil {
//		panic(err)
//	}
//	if value != nil {
//		println(string(*value))
//	}
//
//	if err := db.Shutdown(); err != nil {
//		panic(err)
//	}
//
// [ObjectStoreResolve] accepts SlateDB object-store URLs such as `memory:///`.
// [ObjectStoreFromEnv] builds a store from environment-driven configuration.
//
// [DbBuilder] is the main entry point for writable databases. It can be
// customized with methods such as [DbBuilder.WithSettings],
// [DbBuilder.WithMergeOperator], [DbBuilder.WithSstBlockSize],
// [DbBuilder.WithWalObjectStore], and [DbBuilder.WithDbCacheDisabled] before
// calling [DbBuilder.Build].
//
// # Reading Data
//
// [Db], [DbReader], and [DbSnapshot] all support point reads and range scans.
// Point reads use [Db.Get], [DbReader.Get], or [DbSnapshot.Get]. When callers
// need row metadata such as sequence number or timestamps they can use the
// corresponding `GetKeyValue` variants, which return [KeyValue].
//
// Range and prefix queries use [KeyRange], [Db.Scan], [Db.ScanPrefix],
// [DbReader.Scan], [DbSnapshot.Scan], and the related `WithOptions` methods.
// These APIs return a [DbIterator]. Repeated calls to [DbIterator.Next] return
// rows in order until `nil` is returned, which signals end of iteration.
// [DbIterator.Seek] repositions an iterator to the first row at or after a key.
//
// [ReadOptions] and [ScanOptions] let callers tune visibility and performance,
// including durability filtering, dirty-read behavior, read-ahead, cache
// insertion, and scan fetch parallelism.
//
// For long-lived read-only access, open a [DbReader] with
// [NewDbReaderBuilder]. A reader can be pinned to an existing checkpoint with
// [DbReaderBuilder.WithCheckpointId], configured with [ReaderOptions], and
// given a [MergeOperator] for merge-aware reads.
//
// [Db.Snapshot] creates a consistent read-only [DbSnapshot] from a writable
// database handle.
//
// # Writing Data
//
// The writable [Db] exposes single-key operations such as [Db.Put],
// [Db.Delete], and [Db.Merge], plus batch and durability controls through
// [PutOptions], [MergeOptions], [WriteOptions], and [FlushOptions].
//
// [WriteHandle] reports metadata assigned to a successful write, including the
// sequence number and creation timestamp.
//
// [WriteBatch] collects multiple mutations and applies them atomically through
// [Db.Write] or [Db.WriteWithOptions]. Batches are single-use once submitted.
//
// TTL behavior is configured with [Ttl] implementations such as [TtlDefault],
// [TtlNoExpiry], and [TtlExpireAfterTicks].
//
// # Transactions
//
// [Db.Begin] opens a [DbTransaction] at a chosen [IsolationLevel].
// Transactions support reads, scans, puts, deletes, merges, read marking for
// conflict detection, and either [DbTransaction.Commit] or
// [DbTransaction.Rollback]. A committed transaction returns a [WriteHandle] or
// `nil` if it performed no writes.
//
// A transaction is no longer usable after commit or rollback.
//
// # Configuration, Metrics, And Callbacks
//
// [Settings] is a mutable configuration object for [DbBuilder]. It can be
// created from defaults, environment variables, files, or JSON strings with
// [SettingsDefault], [SettingsFromEnv], [SettingsFromFile],
// [SettingsFromJsonString], and [SettingsLoad]. [Settings.Set] updates fields
// by dotted path using JSON literal values, and [Settings.ToJsonString]
// serializes the resulting configuration.
//
// A writable [Db] also exposes [Db.Status], [Db.Metrics], [Db.Flush], and
// [Db.FlushWithOptions] for health checks, instrumentation, and manual flushes.
//
// Custom merge logic is supplied through the [MergeOperator] callback
// interface. Rust-side logging can be forwarded into Go code with
// [InitLogging] and a [LogCallback].
//
// # WAL Inspection
//
// [NewWalReader] opens a [WalReader] for inspecting WAL files under a database
// path. [WalReader.List] enumerates [WalFile] handles, [WalFile.Metadata]
// returns object-store metadata, and [WalFile.Iterator] returns a
// [WalFileIterator] that yields raw [RowEntry] values. This is primarily useful
// for debugging, diagnostics, and low-level tooling.
//
// # Errors
//
// Most fallible operations return a Go `error` whose concrete type unwraps to
// [Error]. Callers can use `errors.Is` with the exported sentinels
// [ErrErrorTransaction], [ErrErrorClosed], [ErrErrorUnavailable],
// [ErrErrorInvalid], [ErrErrorData], and [ErrErrorInternal] to branch on broad
// error categories.
//
// For example, invalid keys, malformed ranges, and reusing consumed objects
// typically surface as [ErrErrorInvalid]. Operations on closed handles surface
// as [ErrErrorClosed].
//
// # Resource Management
//
// Most exported handle types own a Rust-side resource and provide an explicit
// `Destroy` method, including [ObjectStore], [DbBuilder], [Db], [DbReader],
// [DbSnapshot], [DbTransaction], [DbIterator], [WalReader], [WalFile],
// [WalFileIterator], [Settings], and [WriteBatch].
//
// These handles install Go finalizers, but callers should not rely on garbage
// collection for timely cleanup. Prefer calling `Destroy` explicitly when a
// handle is no longer needed. For [Db] and [DbReader], call `Shutdown` first to
// close the database or reader cleanly, then call `Destroy` to release the Go
// binding handle.
//
// Builders are single-use after `Build`. [WriteBatch] is single-use after
// `Write`. Iterator `Next` methods return `nil` when exhausted, and transaction
// commit methods may return `nil` when no write was emitted.
package uniffi
