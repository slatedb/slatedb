package slatedb

/*
#cgo LDFLAGS: -lslatedb_go
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
//	reader, err := slatedb.OpenReader("/tmp/mydb", &slatedb.StoreConfig{
//	    Provider: slatedb.ProviderLocal,
//	}, nil, &slatedb.DbReaderOptions{
//	    ManifestPollInterval: 5000,  // Poll every 5 seconds
//	    CheckpointLifetime:   30000, // 30 second checkpoint lifetime
//	    MaxMemtableBytes:     1024 * 1024, // 1MB memtable buffer
//	})
//
// Example using all defaults:
//
//	// Pass nil to use all defaults
//	reader, err := slatedb.OpenReader("/tmp/mydb", &slatedb.StoreConfig{
//	    Provider: slatedb.ProviderLocal,
//	}, nil, nil)
func OpenReader(path string, storeConfig *StoreConfig, checkpointId *string, opts *DbReaderOptions) (*DbReader, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Convert Go structs to JSON strings (reuse existing functions)
	storeConfigJSON, storeConfigPtr := convertStoreConfigToJSON(storeConfig)
	defer func() {
		if storeConfigPtr != nil {
			C.free(storeConfigPtr)
		}
	}()

	// Convert checkpoint ID
	var cCheckpointId *C.char
	if checkpointId != nil {
		cCheckpointId = C.CString(*checkpointId)
		defer C.free(unsafe.Pointer(cCheckpointId))
	}

	cOpts := convertToCReaderOptions(opts)

	handle := C.slatedb_reader_open(cPath, storeConfigJSON, cCheckpointId, cOpts)

	// Check if handle is null (indicates error)
	if unsafe.Pointer(handle._0) == unsafe.Pointer(uintptr(0)) {
		return nil, errors.New("failed to open database reader")
	}

	return &DbReader{handle: handle}, nil
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

	if result.error == C.NotFound {
		return nil, ErrNotFound
	}

	if result.error != C.Success {
		return nil, resultToError(result)
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
