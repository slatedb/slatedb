// Package ffi exposes the generated low-level Go bindings for SlateDB.
//
// This package is generated from the Rust UniFFI crate in `bindings/ffi` and is
// intentionally a close mechanical translation of that surface. It is the
// lowest public Go layer over the Rust implementation: names remain Rust/FFI
// flavored, optional values are represented with pointers, and asynchronous Rust
// operations are surfaced as ordinary blocking Go method calls.
//
// The package is meant for:
//
//   - consumers that need direct access to the full UniFFI surface today
//   - wrapper code that will build a more idiomatic Go API on top of these
//     bindings
//   - debugging or validating the Rust FFI contract from Go
//
// If you want an ergonomic Go-first API, build it on top of this package rather
// than changing the generated files directly.
//
// # Main Entry Points
//
// The most common flow is:
//
//  1. Create or resolve an object store with [ObjectStoreResolve] or
//     [ObjectStoreFromEnv].
//  2. Construct a builder with [NewDbBuilder] or [NewDbReaderBuilder].
//  3. Optionally apply [Settings], merge operators, WAL object stores, or
//     reader options.
//  4. Call `Build` to obtain a live [Db] or [DbReader].
//
// Core runtime types:
//
//   - [Db]: writable database handle
//   - [DbReader]: read-only database handle
//   - [DbSnapshot]: point-in-time read snapshot
//   - [DbTransaction]: optimistic transaction handle
//   - [DbIterator]: iterator returned by scans and prefix scans
//   - [WriteBatch]: mutable batch that can be written through [Db.Write]
//   - [WalReader], [WalFile], [WalFileIterator]: low-level WAL access
//   - [Settings]: mutable configuration object used by builders
//
// Supporting data and option types include [KeyRange], [KeyValue],
// [RowEntry], [WriteHandle], [ReadOptions], [WriteOptions],
// [PutOptions], [MergeOptions], [FlushOptions], [ScanOptions], and
// [ReaderOptions].
//
// # Basic Example
//
//	var store, err = ObjectStoreResolve("memory:///")
//	if err != nil {
//		return err
//	}
//	defer store.Destroy()
//
//	builder := NewDbBuilder("/tmp/slatedb-cache", store)
//	defer builder.Destroy()
//
//	db, err := builder.Build()
//	if err != nil {
//		return err
//	}
//	defer db.Shutdown()
//
//	if _, err := db.Put([]byte("key"), []byte("value")); err != nil {
//		return err
//	}
//
//	value, err := db.Get([]byte("key"))
//	if err != nil {
//		return err
//	}
//	if value == nil {
//		return fmt.Errorf("missing key")
//	}
//
// # Blocking Behavior
//
// Many Rust APIs behind this package are asynchronous. The generated Go
// bindings do not expose futures or contexts; instead, methods such as
// `Build`, `Get`, `Put`, `Scan`, `Next`, `Commit`, and `Shutdown` block until
// the underlying Rust future completes.
//
// This means:
//
//   - these methods behave like ordinary synchronous Go calls
//   - they can be used from goroutines naturally
//   - they currently provide no built-in cancellation hook
//
// If your higher-level API needs timeouts or cancellation, add them above this
// package.
//
// # Optional Values and End-of-Stream
//
// UniFFI maps Rust `Option<T>` values to pointer-shaped Go results.
//
// Common examples:
//
//   - `Get` and `GetWithOptions` return `*[]byte`
//   - `GetKeyValue` returns `*KeyValue`
//   - transaction `Commit` returns `*WriteHandle`
//   - iterator `Next` methods return `nil, nil` at end-of-stream
//
// A nil pointer usually means "no value" rather than an error. Callers should
// check both the returned error and whether the returned pointer is nil.
//
// Optional input parameters are also sometimes represented with pointers. For
// example [InitLogging] takes `*LogCallback` because the callback itself
// is optional.
//
// # Object Lifetime and Resource Management
//
// Most exported handle types wrap a Rust object and also expose a `Destroy`
// method. Generated finalizers are installed for many of these objects, but
// callers should not rely on the Go garbage collector for timely cleanup.
//
// Prefer explicit cleanup:
//
//   - call [Db.Shutdown] for writable databases
//   - call [DbReader.Close], [WalReader.Close], [WalFile.Close], and
//     [WalFileIterator.Close] where available
//   - call [DbTransaction.Rollback] or `Commit` exactly once
//   - call `Destroy` on Rust-backed helper objects when you are done with them
//
// Some objects are single-use by design:
//
//   - builders are logically consumed by `Build`
//   - a [WriteBatch] is consumed by `Write` or `WriteWithOptions`
//   - a transaction is completed by `Commit`, `CommitWithOptions`, or
//     `Rollback`
//
// Reusing a consumed object will return a generated [DbError].
//
// # Errors
//
// Rust failures are surfaced as Go errors via [DbError]. Use [errors.Is] with
// the sentinel values exported by the generated package and [errors.As] to
// recover the typed variant.
//
// Common sentinels:
//
//   - [ErrDbErrorTransaction]
//   - [ErrDbErrorClosed]
//   - [ErrDbErrorUnavailable]
//   - [ErrDbErrorInvalid]
//   - [ErrDbErrorData]
//   - [ErrDbErrorInternal]
//
// Example:
//
//	if err != nil {
//		if errors.Is(err, ErrDbErrorInvalid) {
//			var invalid *DbError
//			if errors.As(err, &invalid) {
//				// inspect the wrapped error value if needed
//			}
//		}
//	}
//
// Closed database errors can be unwrapped to [ErrorClosed], which includes a
// [CloseReason].
//
// # Callbacks
//
// The generated surface includes two callback interfaces:
//
//   - [MergeOperator] for custom merge logic
//   - [LogCallback] for receiving log records through [InitLogging]
//
// To provide a merge operator, implement the interface in Go and pass the value
// directly to builder methods such as `WithMergeOperator`.
//
// Logging is slightly different because the callback parameter is optional. To
// pass a logging callback, store the interface value in a variable and pass its
// address:
//
//	var cb LogCallback = myLogger{}
//	if err := InitLogging(LogLevelInfo, &cb); err != nil {
//		return err
//	}
//
// Callback implementations may be invoked from the Rust side while the database
// is operating. Keep callback code thread-safe, avoid re-entrancy hazards, and
// return promptly.
//
// # Iteration and Range Scans
//
// Range scans use [KeyRange], which mirrors Rust bound semantics:
//
//   - `Start` and `End` are optional
//   - `StartInclusive` and `EndInclusive` control bound inclusion
//   - nil bounds mean unbounded
//
// Database and snapshot scans return [DbIterator]. WAL iteration returns
// [WalFileIterator]. In both cases, `Next` yields a pointer to the current
// item and returns nil when iteration is exhausted.
//
// # Settings and Configuration
//
// [Settings] can be created with:
//
//   - [SettingsDefault]
//   - [SettingsFromFile]
//   - [SettingsFromJsonString]
//   - [SettingsFromEnv]
//   - [SettingsFromEnvWithDefault]
//   - [SettingsLoad]
//
// The `Set` method updates fields by dotted path using a JSON literal string.
// This is intentionally low-level and mirrors the Rust FFI API closely.
//
// # Generated Code Policy
//
// `slatedb.go` and `slatedb.h` are generated artifacts. Do not edit them by
// hand. Regenerate them from the repository root with:
//
//	./scripts/generate_go_bindings.sh
//
// Keep handwritten documentation and cgo glue in separate files such as
// `doc.go` and `link.go`.
package ffi
