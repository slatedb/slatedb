//go:generate ./generate.sh

// Package slatedb exposes the raw UniFFI-generated Go bindings for SlateDB.
//
// This package is generated from `bindings/ffi` and intentionally does not
// reuse the handwritten bindings in `slatedb-go`. Rust async APIs appear here
// as blocking Go calls, and non-DB FFI handles such as ObjectStore,
// DbIterator, DbSnapshot, and DbTransaction should be released with Destroy
// when they are no longer needed.
package slatedb
