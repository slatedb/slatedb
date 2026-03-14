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
//  1. Create or resolve an object store with [FfiObjectStoreResolve] or
//     [FfiObjectStoreFromEnv].
//  2. Construct a builder with [NewFfiDbBuilder] or [NewFfiDbReaderBuilder].
//  3. Optionally apply [FfiSettings], merge operators, WAL object stores, or
//     reader options.
//  4. Call `Build` to obtain a live [FfiDb] or [FfiDbReader].
//
// Core runtime types:
//
//   - [FfiDb]: writable database handle
//   - [FfiDbReader]: read-only database handle
//   - [FfiDbSnapshot]: point-in-time read snapshot
//   - [FfiDbTransaction]: optimistic transaction handle
//   - [FfiDbIterator]: iterator returned by scans and prefix scans
//   - [FfiWriteBatch]: mutable batch that can be written through [FfiDb.Write]
//   - [FfiWalReader], [FfiWalFile], [FfiWalFileIterator]: low-level WAL access
//   - [FfiSettings]: mutable configuration object used by builders
//
// Supporting data and option types include [FfiKeyRange], [FfiKeyValue],
// [FfiRowEntry], [FfiWriteHandle], [FfiReadOptions], [FfiWriteOptions],
// [FfiPutOptions], [FfiMergeOptions], [FfiFlushOptions], [FfiScanOptions], and
// [FfiReaderOptions].
//
// # Basic Example
//
//	var store, err = FfiObjectStoreResolve("memory:///")
//	if err != nil {
//		return err
//	}
//	defer store.Destroy()
//
//	builder := NewFfiDbBuilder("/tmp/slatedb-cache", store)
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
//   - `GetKeyValue` returns `*FfiKeyValue`
//   - transaction `Commit` returns `*FfiWriteHandle`
//   - iterator `Next` methods return `nil, nil` at end-of-stream
//
// A nil pointer usually means "no value" rather than an error. Callers should
// check both the returned error and whether the returned pointer is nil.
//
// Optional input parameters are also sometimes represented with pointers. For
// example [FfiInitLogging] takes `*FfiLogCallback` because the callback itself
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
//   - call [FfiDb.Shutdown] for writable databases
//   - call [FfiDbReader.Close], [FfiWalReader.Close], [FfiWalFile.Close], and
//     [FfiWalFileIterator.Close] where available
//   - call [FfiDbTransaction.Rollback] or `Commit` exactly once
//   - call `Destroy` on Rust-backed helper objects when you are done with them
//
// Some objects are single-use by design:
//
//   - builders are logically consumed by `Build`
//   - a [FfiWriteBatch] is consumed by `Write` or `WriteWithOptions`
//   - a transaction is completed by `Commit`, `CommitWithOptions`, or
//     `Rollback`
//
// Reusing a consumed object will return a generated [FfiError].
//
// # Errors
//
// Rust failures are surfaced as Go errors via [FfiError]. Use [errors.Is] with
// the sentinel values exported by the generated package and [errors.As] to
// recover the typed variant.
//
// Common sentinels:
//
//   - [ErrFfiErrorTransaction]
//   - [ErrFfiErrorClosed]
//   - [ErrFfiErrorUnavailable]
//   - [ErrFfiErrorInvalid]
//   - [ErrFfiErrorData]
//   - [ErrFfiErrorInternal]
//
// Example:
//
//	if err != nil {
//		if errors.Is(err, ErrFfiErrorInvalid) {
//			var invalid *FfiError
//			if errors.As(err, &invalid) {
//				// inspect the wrapped error value if needed
//			}
//		}
//	}
//
// Closed database errors can be unwrapped to [FfiErrorClosed], which includes a
// [FfiCloseReason].
//
// # Callbacks
//
// The generated surface includes two callback interfaces:
//
//   - [FfiMergeOperator] for custom merge logic
//   - [FfiLogCallback] for receiving log records through [FfiInitLogging]
//
// To provide a merge operator, implement the interface in Go and pass the value
// directly to builder methods such as `WithMergeOperator`.
//
// Logging is slightly different because the callback parameter is optional. To
// pass a logging callback, store the interface value in a variable and pass its
// address:
//
//	var cb FfiLogCallback = myLogger{}
//	if err := FfiInitLogging(FfiLogLevelInfo, &cb); err != nil {
//		return err
//	}
//
// Callback implementations may be invoked from the Rust side while the database
// is operating. Keep callback code thread-safe, avoid re-entrancy hazards, and
// return promptly.
//
// # Iteration and Range Scans
//
// Range scans use [FfiKeyRange], which mirrors Rust bound semantics:
//
//   - `Start` and `End` are optional
//   - `StartInclusive` and `EndInclusive` control bound inclusion
//   - nil bounds mean unbounded
//
// Database and snapshot scans return [FfiDbIterator]. WAL iteration returns
// [FfiWalFileIterator]. In both cases, `Next` yields a pointer to the current
// item and returns nil when iteration is exhausted.
//
// # Settings and Configuration
//
// [FfiSettings] can be created with:
//
//   - [FfiSettingsDefault]
//   - [FfiSettingsFromFile]
//   - [FfiSettingsFromJsonString]
//   - [FfiSettingsFromEnv]
//   - [FfiSettingsFromEnvWithDefault]
//   - [FfiSettingsLoad]
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
