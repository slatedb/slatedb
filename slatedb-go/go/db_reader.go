package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

// DbReader represents a read-only SlateDB connection
// This can read from the latest state or from a specific checkpoint
type DbReader struct {
	handle C.CSdbReaderHandle
}

// OpenReader opens a read-only database reader
//
// Parameters:
//   - path: Local path for database metadata and WAL files
//   - storeConfig: Object storage provider configuration
//   - checkpointId: Optional checkpoint ID to read from. Use nil for latest state.
//   - opts: Optional reader configuration. Use nil for defaults.
//
// Example for reading latest state:
//
//	// Read from latest state with auto-refreshing checkpoint
//	reader, err := slatedb.OpenReader("/tmp/mydb", WithDbReaderOptions(slatedb.DbReaderOptions{
//	    ManifestPollInterval: 5000,  // Poll every 5 seconds
//	    CheckpointLifetime:   30000, // 30 second checkpoint lifetime
//	    MaxMemtableBytes:     1024 * 1024, // 1MB memtable buffer
//	}))
//
// Example using all defaults:
//
//	reader, err := slatedb.OpenReader("/tmp/mydb")
func OpenReader(path string, opts ...Option[DbReaderConfig]) (*DbReader, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cfg := &DbReaderConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	var (
		cURL, cEnvFile, cCheckpointId *C.char
		cOpts                         *C.CSdbReaderOptions
	)
	if cfg.url != nil {
		cURL = C.CString(*cfg.url)
		defer C.free(unsafe.Pointer(cURL))
	}
	if cfg.envFile != nil {
		cEnvFile = C.CString(*cfg.envFile)
		defer C.free(unsafe.Pointer(cEnvFile))
	}
	if cfg.checkpointId != nil {
		cCheckpointId = C.CString(*cfg.checkpointId)
		defer C.free(unsafe.Pointer(cCheckpointId))
	}
	if cfg.opts != nil {
		cOpts = convertToCReaderOptions(cfg.opts)
	}

	result := C.slatedb_reader_open(cPath, cURL, cEnvFile, cCheckpointId, cOpts)
	defer C.slatedb_free_result(result.result)

	if result.result.error != C.Success {
		return nil, resultToError(result.result)
	}

	// Check if handle is null (indicates error)
	if unsafe.Pointer(result.handle._0) == unsafe.Pointer(uintptr(0)) {
		return nil, errors.New("failed to open database reader")
	}

	return &DbReader{handle: result.handle}, nil
}

// Get retrieves a value by key from the database reader with default options
// Returns ErrNotFound if the key doesn't exist
func (r *DbReader) Get(key []byte) ([]byte, error) {
	return r.GetWithOptions(key, &ReadOptions{})
}

// GetWithOptions retrieves a value by key from the database reader with custom read options
// Returns ErrNotFound if the key doesn't exist
//
// Example:
//
//	readOpts := &slatedb.ReadOptions{
//	    DurabilityFilter: slatedb.DurabilityMemory, // Default
//	    Dirty:           false,
//	}
//	value, err := reader.GetWithOptions([]byte("user:123"), readOpts)
func (r *DbReader) GetWithOptions(key []byte, opts *ReadOptions) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidArgument
	}

	keyPtr := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	var value C.CSdbValue
	cOpts := convertToCReadOptions(opts)

	result := C.slatedb_reader_get_with_options(
		r.handle,
		keyPtr,
		C.uintptr_t(len(key)),
		cOpts,
		&value,
	)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return nil, resultToError(result)
	}

	if result.none {
		return nil, ErrNotFound
	}

	if value.data == nil || value.len == 0 {
		return []byte{}, nil
	}

	// Copy the data to Go memory
	goValue := C.GoBytes(unsafe.Pointer(value.data), C.int(value.len))

	// Free the C memory
	C.slatedb_free_value(value)

	return goValue, nil
}

// Scan creates a streaming iterator for the specified range with default options
// This provides full parity with Rust's range syntax:
//
//	start=nil, end=nil     -> full scan (..)
//	start=key, end=nil     -> from key forward (key..)
//	start=nil, end=key     -> up to key (..key)
//	start=key1, end=key2   -> between keys (key1..key2)
//
// SAFETY CONTRACT: Iterator MUST be closed before DbReader is closed.
//
// Example usage:
//
//	iter, err := reader.Scan([]byte("user:"), []byte("user:\xFF"))
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF {
//	        break // End of iteration
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    process(kv.Key, kv.Value)
//	}
func (r *DbReader) Scan(start, end []byte) (*Iterator, error) {
	return r.ScanWithOptions(start, end, nil)
}

// ScanWithOptions creates a streaming iterator for the specified range with custom scan options
//
// Example:
//
//	opts := &ScanOptions{DurabilityFilter: DurabilityMemory, Dirty: false}
//	iter, err := reader.ScanWithOptions([]byte("user:"), nil, opts)
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    process(kv.Key, kv.Value)
//	}
func (r *DbReader) ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error) {
	var startPtr *C.uint8_t
	var startLen C.uintptr_t
	if len(start) > 0 {
		startPtr = (*C.uint8_t)(unsafe.Pointer(&start[0]))
		startLen = C.uintptr_t(len(start))
	}

	var endPtr *C.uint8_t
	var endLen C.uintptr_t
	if len(end) > 0 {
		endPtr = (*C.uint8_t)(unsafe.Pointer(&end[0]))
		endLen = C.uintptr_t(len(end))
	}

	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.CSdbIterator
	result := C.slatedb_reader_scan_with_options(
		r.handle,
		startPtr,
		startLen,
		endPtr,
		endLen,
		cOpts,
		&iterPtr,
	)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return nil, resultToError(result)
	}

	return &Iterator{
		ptr:    iterPtr,
		closed: false,
	}, nil
}

// ScanPrefix creates a streaming iterator for all keys with the given prefix using default scan options.
//
// Returns an iterator that yields key-value pairs whose keys start with `prefix`.
// The iterator MUST be closed after use to prevent resource leaks.
//
// ## Arguments
// - `prefix`: key prefix to match (empty or nil scans all keys)
//
// ## Returns
// - `*Iterator`: streaming iterator over matching keys
// - `error`: if there was an error creating the iterator
//
// ## Examples
//
//	iter, err := reader.ScanPrefix([]byte("user:"))
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	    // process kv
//	}
func (r *DbReader) ScanPrefix(prefix []byte) (*Iterator, error) {
	return r.ScanPrefixWithOptions(prefix, nil)
}

// ScanPrefixWithOptions creates a streaming iterator for all keys with the given prefix and custom scan options.
//
// Returns an iterator that yields key-value pairs whose keys start with `prefix`.
// The iterator MUST be closed after use to prevent resource leaks.
//
// ## Arguments
// - `prefix`: key prefix to match (empty or nil scans all keys)
// - `opts`: scan options for durability, caching, read-ahead behavior
//
// ## Returns
// - `*Iterator`: streaming iterator over matching keys
// - `error`: if there was an error creating the iterator
//
// ## Examples
//
//	opts := &ScanOptions{DurabilityFilter: DurabilityRemote, Dirty: false}
//	iter, err := reader.ScanPrefixWithOptions([]byte("user:"), opts)
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	    // process kv
//	}
func (r *DbReader) ScanPrefixWithOptions(prefix []byte, opts *ScanOptions) (*Iterator, error) {
	var prefixPtr *C.uint8_t
	if len(prefix) > 0 {
		prefixPtr = (*C.uint8_t)(unsafe.Pointer(&prefix[0]))
	}

	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.CSdbIterator
	result := C.slatedb_reader_scan_prefix_with_options(
		r.handle,
		prefixPtr,
		C.uintptr_t(len(prefix)),
		cOpts,
		&iterPtr,
	)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return nil, resultToError(result)
	}

	return &Iterator{
		ptr:    iterPtr,
		closed: false,
	}, nil
}

// Close closes the database reader and releases all resources
// After calling Close, the DbReader instance should not be used
func (r *DbReader) Close() error {
	result := C.slatedb_reader_close(r.handle)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	// Mark this DbReader as invalid to prevent further use
	r.handle._0 = nil

	return nil
}
