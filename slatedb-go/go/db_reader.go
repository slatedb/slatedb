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

// DbReader represents a read-only SlateDB connection.
//
// A reader can target latest state or a specific checkpoint ID.
type DbReader struct {
	handle *C.slatedb_db_reader_t
}

// OpenReader opens a read-only database reader.
//
// Parameters:
//   - `path`: local database path
//   - `WithUrl`: object-store URL
//   - `WithEnvFile`: optional `.env` file for URL/provider resolution
//   - `WithCheckpointId`: optional checkpoint UUID string
//   - `WithDbReaderOptions`: optional reader tuning options
//
// Example:
//
//	reader, err := slatedb.OpenReader(
//	    "/tmp/mydb",
//	    slatedb.WithEnvFile[slatedb.DbReaderConfig](".env"),
//	    slatedb.WithDbReaderOptions(slatedb.DbReaderOptions{
//	        ManifestPollInterval: 5000,
//	        CheckpointLifetime:   30000,
//	    }),
//	)
func OpenReader(path string, opts ...Option[DbReaderConfig]) (*DbReader, error) {
	cfg := &DbReaderConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	objectStore, err := resolveObjectStoreHandle(cfg.url, cfg.envFile)
	if err != nil {
		return nil, err
	}
	defer closeObjectStoreHandle(objectStore)

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cCheckpointID *C.char
	if cfg.checkpointId != nil {
		cCheckpointID = C.CString(*cfg.checkpointId)
		defer C.free(unsafe.Pointer(cCheckpointID))
	}

	cOpts := convertToCReaderOptions(cfg.opts)

	var readerHandle *C.slatedb_db_reader_t
	result := C.slatedb_db_reader_open(cPath, objectStore, cCheckpointID, cOpts, &readerHandle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if readerHandle == nil {
		return nil, errors.New("failed to open database reader")
	}

	return &DbReader{handle: readerHandle}, nil
}

// Get retrieves a value by key with default read options.
//
// Returns `ErrNotFound` if the key does not exist.
func (r *DbReader) Get(key []byte) ([]byte, error) {
	return r.GetWithOptions(key, nil)
}

// GetWithOptions retrieves a value by key with explicit read options.
//
// Pass nil options to use defaults.
// Returns `ErrNotFound` if the key does not exist.
func (r *DbReader) GetWithOptions(key []byte, opts *ReadOptions) ([]byte, error) {
	if r == nil || r.handle == nil {
		return nil, ErrInvalidHandle
	}
	if len(key) == 0 {
		return nil, ErrInvalidArgument
	}

	keyPtr, keyLen := ptrFromBytes(key)
	cOpts := convertToCReadOptions(opts)

	var present C.bool
	var value *C.uint8_t
	var valueLen C.uintptr_t
	result := C.slatedb_db_reader_get_with_options(
		r.handle,
		keyPtr,
		keyLen,
		cOpts,
		&present,
		&value,
		&valueLen,
	)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}

	if present == C.bool(false) {
		return nil, ErrNotFound
	}
	return copyBytesAndFree(value, valueLen), nil
}

// Scan creates a streaming iterator for the range `[start, end)` with default options.
//
// `start=nil` means unbounded start; `end=nil` means unbounded end.
// The iterator must be closed after use and before closing the reader.
func (r *DbReader) Scan(start, end []byte) (*Iterator, error) {
	return r.ScanWithOptions(start, end, nil)
}

// ScanWithOptions creates a streaming iterator for `[start, end)` with explicit scan options.
//
// Pass nil options to use defaults.
// The iterator must be closed after use and before closing the reader.
func (r *DbReader) ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error) {
	if r == nil || r.handle == nil {
		return nil, ErrInvalidHandle
	}

	rangeValue := makeScanRange(start, end)
	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.slatedb_iterator_t
	result := C.slatedb_db_reader_scan_with_options(r.handle, rangeValue, cOpts, &iterPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if iterPtr == nil {
		return nil, errors.New("failed to create iterator")
	}

	return &Iterator{ptr: iterPtr}, nil
}

// ScanPrefix creates a streaming iterator for all keys that match `prefix`.
//
// The iterator must be closed after use and before closing the reader.
func (r *DbReader) ScanPrefix(prefix []byte) (*Iterator, error) {
	return r.ScanPrefixWithOptions(prefix, nil)
}

// ScanPrefixWithOptions creates a streaming iterator for `prefix` with explicit scan options.
//
// Pass nil options to use defaults.
// The iterator must be closed after use and before closing the reader.
func (r *DbReader) ScanPrefixWithOptions(prefix []byte, opts *ScanOptions) (*Iterator, error) {
	if r == nil || r.handle == nil {
		return nil, ErrInvalidHandle
	}

	prefixPtr, prefixLen := ptrFromBytes(prefix)
	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.slatedb_iterator_t
	result := C.slatedb_db_reader_scan_prefix_with_options(
		r.handle,
		prefixPtr,
		prefixLen,
		cOpts,
		&iterPtr,
	)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if iterPtr == nil {
		return nil, errors.New("failed to create iterator")
	}

	return &Iterator{ptr: iterPtr}, nil
}

// Close closes the database reader and releases all resources.
//
// The reader must not be used after `Close` returns successfully.
func (r *DbReader) Close() error {
	if r == nil || r.handle == nil {
		return ErrInvalidHandle
	}

	result := C.slatedb_db_reader_close(r.handle)
	if err := resultToErrorAndFree(result); err != nil {
		return err
	}

	r.handle = nil
	return nil
}
