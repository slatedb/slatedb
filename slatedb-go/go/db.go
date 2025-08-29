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
	"fmt"
	"unsafe"
)

// Error definitions
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("key not found")
	ErrAlreadyExists   = errors.New("key already exists")
	ErrIOError         = errors.New("I/O error")
	ErrInternalError   = errors.New("internal error")
	ErrNullPointer     = errors.New("null pointer")
	ErrInvalidHandle   = errors.New("invalid handle")
)

// DB represents a SlateDB database connection
type DB struct {
	handle C.CSdbHandle
}

// KeyValue represents a key-value pair from scan operations
type KeyValue struct {
	Key   []byte
	Value []byte
}

// ScanResult represents the result of a scan operation
type ScanResult struct {
	Items        []KeyValue
	HasMore      bool
	NextStartKey []byte // Key to use for next scan to avoid duplicates
}

// Helper function to convert C result to Go error
func resultToError(result C.struct_CSdbResult) error {
	var baseErr error

	switch result.error {
	case C.Success:
		return nil
	case C.InvalidArgument:
		baseErr = ErrInvalidArgument
	case C.NotFound:
		baseErr = ErrNotFound
	case C.AlreadyExists:
		baseErr = ErrAlreadyExists
	case C.IOError:
		baseErr = ErrIOError
	case C.InternalError:
		baseErr = ErrInternalError
	case C.NullPointer:
		baseErr = ErrNullPointer
	case C.InvalidHandle:
		baseErr = ErrInvalidHandle
	default:
		baseErr = ErrInternalError
	}

	// Include detailed error message if available
	if result.message != nil {
		message := C.GoString(result.message)
		return fmt.Errorf("%w: %s", baseErr, message)
	}

	return baseErr
}

// Open opens a SlateDB database with the specified object storage provider
// This replaces OpenMemory and OpenS3 functions for a unified API
//
// Parameters:
//   - path: Local path for database metadata and WAL files
//   - storeConfig: Object storage provider configuration
//   - opts: Optional database configuration. Use nil for defaults.
//
// Example for local storage (testing/development):
//
//	// Local storage (testing/development)
//	db, err := slatedb.Open("/tmp/mydb", &slatedb.StoreConfig{
//	    Provider: slatedb.ProviderLocal,
//	}, nil)
//
//	// AWS S3 storage with custom timeout
//	db, err := slatedb.Open("/tmp/mydb", &slatedb.StoreConfig{
//	    Provider: slatedb.ProviderAWS,
//	    AWS: &slatedb.AWSConfig{
//	        Bucket: "my-slatedb-bucket",
//	        Region: "us-east-1",
//	        RequestTimeout: 30 * time.Second,
//	    },
//	}, &slatedb.SlateDBOptions{
//	    FlushInterval: 200 * time.Millisecond,
//	})
func Open(path string, storeConfig *StoreConfig, opts *SlateDBOptions) (*DB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Convert Go structs to JSON strings
	storeConfigJSON, storeConfigPtr := convertStoreConfigToJSON(storeConfig)
	optionsJSON, optionsPtr := convertOptionsToJSON(opts)

	defer func() {
		// Free JSON string memory
		if storeConfigPtr != nil {
			C.free(storeConfigPtr)
		}
		if optionsPtr != nil {
			C.free(optionsPtr)
		}
	}()

	handle := C.slatedb_open(cPath, storeConfigJSON, optionsJSON)

	// Check if handle is null (indicates error)
	// We need to check if the pointer inside the handle is null
	if unsafe.Pointer(handle._0) == unsafe.Pointer(uintptr(0)) {
		return nil, errors.New("failed to open database")
	}

	return &DB{handle: handle}, nil
}

// Put stores a key-value pair in the database
// The operation is durable - data is persisted to object storage
func (db *DB) Put(key, value []byte) error {
	return db.PutWithOptions(key, value, nil, nil)
}

// Get retrieves a value by key from the database
// Returns ErrNotFound if the key doesn't exist
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetWithOptions(key, nil)
}

// Delete removes a key from the database
// Returns successfully even if the key doesn't exist
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, nil)
}

// PutWithOptions stores a key-value pair in the database with custom put and write options
// This provides control over TTL and durability behavior
//
// Example with TTL:
//
//	putOpts := &slatedb.PutOptions{
//	    TTLType:  slatedb.TTLExpireAfter,
//	    TTLValue: 3600000, // 1 hour in milliseconds
//	}
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: true}
//	err := db.PutWithOptions([]byte("session:123"), []byte("data"), putOpts, writeOpts)
func (db *DB) PutWithOptions(key, value []byte, putOpts *PutOptions, writeOpts *WriteOptions) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	var keyPtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	var valuePtr *C.uint8_t
	if len(value) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	cPutOpts := convertToCPutOptions(putOpts)
	cWriteOpts := convertToCWriteOptions(writeOpts)

	result := C.slatedb_put_with_options(
		db.handle,
		keyPtr,
		C.uintptr_t(len(key)),
		valuePtr,
		C.uintptr_t(len(value)),
		cPutOpts,
		cWriteOpts,
	)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}

// DeleteWithOptions removes a key from the database with custom write options
// Returns successfully even if the key doesn't exist
//
// Example:
//
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: false}  // Fast delete
//	err := db.DeleteWithOptions([]byte("temp:123"), writeOpts)
func (db *DB) DeleteWithOptions(key []byte, writeOpts *WriteOptions) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyPtr := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	cWriteOpts := convertToCWriteOptions(writeOpts)

	result := C.slatedb_delete_with_options(
		db.handle,
		keyPtr,
		C.uintptr_t(len(key)),
		cWriteOpts,
	)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}

// GetWithOptions retrieves a value by key from the database with custom read options
// Returns ErrNotFound if the key doesn't exist
//
// Example for reading only durably committed data:
//
//	readOpts := &slatedb.ReadOptions{
//	    DurabilityFilter: slatedb.DurabilityRemote,
//	    Dirty:           false,
//	}
//	value, err := db.GetWithOptions([]byte("user:123"), readOpts)
func (db *DB) GetWithOptions(key []byte, readOpts *ReadOptions) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidArgument
	}

	keyPtr := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	var value C.struct_CSdbValue
	cReadOpts := convertToCReadOptions(readOpts)

	result := C.slatedb_get_with_options(
		db.handle,
		keyPtr,
		C.uintptr_t(len(key)),
		cReadOpts,
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

// Write executes a WriteBatch atomically with default WriteOptions
//
// The batch is consumed by this operation and cannot be reused.
// Always call batch.Close() after this operation to free resources.
//
// Example:
//
//	batch, err := slatedb.NewWriteBatch()
//	if err != nil {
//	    return err
//	}
//	defer batch.Close() // Always close to prevent memory leaks
//
//	batch.Put([]byte("key1"), []byte("value1"))
//	batch.Delete([]byte("key2"))
//
//	err = db.Write(batch)
//	// batch is now consumed and cannot be reused
func (db *DB) Write(batch *WriteBatch) error {
	return db.WriteWithOptions(batch, nil)
}

// WriteWithOptions executes a WriteBatch atomically with custom WriteOptions
//
// The batch is consumed by this operation and cannot be reused.
// Always call batch.Close() after this operation to free resources.
//
// Example:
//
//	batch, err := slatedb.NewWriteBatch()
//	if err != nil {
//	    return err
//	}
//	defer batch.Close() // Always close to prevent memory leaks
//
//	batch.Put([]byte("key1"), []byte("value1"))
//	batch.Delete([]byte("key2"))
//
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: false}
//	err = db.WriteWithOptions(batch, writeOpts)
//	// batch is now consumed and cannot be reused
func (db *DB) WriteWithOptions(batch *WriteBatch, opts *WriteOptions) error {
	if batch == nil {
		return errors.New("batch cannot be nil")
	}
	if batch.closed {
		return errors.New("batch is closed")
	}
	if batch.consumed {
		return errors.New("batch already consumed")
	}

	// Set default options if nil
	if opts == nil {
		opts = &WriteOptions{AwaitDurable: true}
	}

	cOpts := convertToCWriteOptions(opts)

	result := C.slatedb_write_batch_write(
		db.handle,
		batch.ptr,
		cOpts,
	)

	if err := resultToError(result); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	// Mark batch as consumed to prevent reuse
	batch.consumed = true
	return nil
}

// Flush flushes in-memory writes to persistent storage
// This ensures all pending data is durably written to object storage
// Call this before opening a DbReader if you need to read recently written data
func (db *DB) Flush() error {
	result := C.slatedb_flush(db.handle)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}

// Close closes the database connection and releases all resources
// After calling Close, the DB instance should not be used
func (db *DB) Close() error {
	result := C.slatedb_close(db.handle)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	// Mark this DB as invalid to prevent further use
	db.handle._0 = nil

	return nil
}

// Scan creates a streaming iterator for the specified range with default scan options
//
// Returns an iterator that yields key-value pairs in the range [start, end).
// The iterator MUST be closed after use to prevent resource leaks.
//
// ## Arguments
// - `start`: start key (inclusive). Use nil for beginning of database
// - `end`: end key (exclusive). Use nil for end of database
//
// ## Returns
// - `*Iterator`: streaming iterator for the range
// - `error`: if there was an error creating the iterator
//
// ## Examples
//
//	iter, err := db.Scan([]byte("user:"), []byte("user;"))
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	    process(kv.Key, kv.Value)
//	}
func (db *DB) Scan(start, end []byte) (*Iterator, error) {
	return db.ScanWithOptions(start, end, &ScanOptions{})
}

// ScanWithOptions creates a streaming iterator for the specified range with custom scan options
//
// Returns an iterator that yields key-value pairs in the range [start, end).
// The iterator MUST be closed after use to prevent resource leaks.
//
// ## Arguments
// - `start`: start key (inclusive). Use nil for beginning of database
// - `end`: end key (exclusive). Use nil for end of database
// - `opts`: scan options for durability, caching, read-ahead behavior
//
// ## Returns
// - `*Iterator`: streaming iterator for the range
// - `error`: if there was an error creating the iterator
//
// ## Examples
//
//	opts := &ScanOptions{DurabilityFilter: DurabilityRemote, Dirty: false}
//	iter, err := db.ScanWithOptions([]byte("user:"), []byte("user;"), opts)
//	if err != nil { return err }
//	defer iter.Close()  // Essential!
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	    process(kv.Key, kv.Value)
//	}
func (db *DB) ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error) {
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
	result := C.slatedb_scan_with_options(
		db.handle,
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
