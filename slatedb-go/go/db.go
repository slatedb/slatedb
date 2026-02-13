package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unsafe"
)

// Error definitions.
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("key not found")
	ErrAlreadyExists   = errors.New("key already exists")
	ErrIOError         = errors.New("I/O error")
	ErrInternalError   = errors.New("internal error")
	ErrNullPointer     = errors.New("null pointer")
	ErrInvalidHandle   = errors.New("invalid handle")
	ErrInvalidProvider = errors.New("invalid provider")
	ErrTransaction     = errors.New("transaction error")
)

// DB represents a SlateDB database connection.
type DB struct {
	handle *C.slatedb_db_t
}

// KeyValue represents a key-value pair from scan operations.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// ScanResult represents the result of a scan operation.
type ScanResult struct {
	Items        []KeyValue
	HasMore      bool
	NextStartKey []byte
}

func resultToError(result C.struct_slatedb_result_t) error {
	if result.kind == C.SLATEDB_ERROR_KIND_NONE {
		return nil
	}

	var baseErr error
	switch result.kind {
	case C.SLATEDB_ERROR_KIND_INVALID:
		baseErr = ErrInvalidArgument
	case C.SLATEDB_ERROR_KIND_TRANSACTION:
		baseErr = ErrTransaction
	case C.SLATEDB_ERROR_KIND_CLOSED:
		baseErr = ErrInvalidHandle
	case C.SLATEDB_ERROR_KIND_UNAVAILABLE:
		baseErr = ErrIOError
	case C.SLATEDB_ERROR_KIND_DATA:
		baseErr = ErrInternalError
	case C.SLATEDB_ERROR_KIND_INTERNAL:
		baseErr = ErrInternalError
	default:
		baseErr = ErrInternalError
	}

	if result.message != nil {
		return fmt.Errorf("%w: %s", baseErr, C.GoString(result.message))
	}
	if result.kind == C.SLATEDB_ERROR_KIND_CLOSED {
		return fmt.Errorf("%w: close_reason=%d", baseErr, int(result.close_reason))
	}
	return baseErr
}

func resultToErrorAndFree(result C.struct_slatedb_result_t) error {
	defer C.slatedb_result_free(result)
	return resultToError(result)
}

func resolveObjectStoreHandle(url *string, envFile *string) (*C.slatedb_object_store_t, error) {
	resolvedURL, hasURL, err := resolveObjectStoreURL(url, envFile)
	if err != nil {
		return nil, err
	}

	var objectStore *C.slatedb_object_store_t
	if hasURL {
		cURL := C.CString(resolvedURL)
		defer C.free(unsafe.Pointer(cURL))

		result := C.slatedb_object_store_from_url(cURL, &objectStore)
		if err := resultToErrorAndFree(result); err != nil {
			return nil, err
		}
	} else {
		var cEnvFile *C.char
		if envFile != nil && strings.TrimSpace(*envFile) != "" {
			cEnvFile = C.CString(strings.TrimSpace(*envFile))
			defer C.free(unsafe.Pointer(cEnvFile))
		}

		result := C.slatedb_object_store_from_env(cEnvFile, &objectStore)
		if err := resultToErrorAndFree(result); err != nil {
			return nil, err
		}
	}

	if objectStore == nil {
		return nil, errors.New("failed to resolve object store")
	}

	return objectStore, nil
}

func closeObjectStoreHandle(objectStore *C.slatedb_object_store_t) {
	if objectStore == nil {
		return
	}
	_ = resultToErrorAndFree(C.slatedb_object_store_close(objectStore))
}

func ptrFromBytes(data []byte) (*C.uint8_t, C.uintptr_t) {
	if len(data) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&data[0])), C.uintptr_t(len(data))
}

func copyBytesAndFree(data *C.uint8_t, dataLen C.uintptr_t) []byte {
	if data == nil || dataLen == 0 {
		return []byte{}
	}
	defer C.slatedb_bytes_free(data, dataLen)
	return C.GoBytes(unsafe.Pointer(data), C.int(dataLen))
}

func makeScanRange(start, end []byte) C.slatedb_range_t {
	rangeValue := C.slatedb_range_t{
		start: C.slatedb_bound_t{kind: C.uint8_t(C.SLATEDB_BOUND_KIND_UNBOUNDED)},
		end:   C.slatedb_bound_t{kind: C.uint8_t(C.SLATEDB_BOUND_KIND_UNBOUNDED)},
	}

	if len(start) > 0 {
		rangeValue.start.kind = C.uint8_t(C.SLATEDB_BOUND_KIND_INCLUDED)
		rangeValue.start.data = (*C.uint8_t)(unsafe.Pointer(&start[0]))
		rangeValue.start.len = C.uintptr_t(len(start))
	}

	if len(end) > 0 {
		rangeValue.end.kind = C.uint8_t(C.SLATEDB_BOUND_KIND_EXCLUDED)
		rangeValue.end.data = (*C.uint8_t)(unsafe.Pointer(&end[0]))
		rangeValue.end.len = C.uintptr_t(len(end))
	}

	return rangeValue
}

func settingsHandleFromSettings(settings *Settings) (*C.slatedb_settings_t, error) {
	if settings == nil {
		return nil, nil
	}

	defaults, err := SettingsDefault()
	if err != nil {
		return nil, err
	}
	finalSettings := MergeSettings(defaults, settings)

	settingsJSON, err := json.Marshal(finalSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal settings: %w", err)
	}

	cSettingsJSON := C.CString(string(settingsJSON))
	defer C.free(unsafe.Pointer(cSettingsJSON))

	var handle *C.slatedb_settings_t
	result := C.slatedb_settings_from_json(cSettingsJSON, &handle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, errors.New("failed to create settings handle")
	}

	return handle, nil
}

func closeBuilderHandle(builder *C.slatedb_db_builder_t) {
	if builder == nil {
		return
	}
	_ = resultToErrorAndFree(C.slatedb_db_builder_close(builder))
}

// Open opens a writable SlateDB database.
//
// Object-store configuration is resolved from `opts`:
//   - `WithUrl`: explicit object-store URL (for example `memory:///`, `file:///tmp/db`)
//   - `WithEnvFile`: optional `.env` file used when resolving URL/provider settings
//
// For advanced configuration (custom `Settings`, SST block size), use `NewBuilder`.
func Open(path string, opts ...Option[DbConfig]) (*DB, error) {
	cfg := &DbConfig{}
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

	var dbHandle *C.slatedb_db_t
	result := C.slatedb_db_open(cPath, objectStore, &dbHandle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if dbHandle == nil {
		return nil, errors.New("failed to open database")
	}

	return &DB{handle: dbHandle}, nil
}

// Put stores a key-value pair in the database using default put/write options.
//
// The write is durable based on SlateDB defaults.
func (db *DB) Put(key, value []byte) error {
	return db.PutWithOptions(key, value, nil, nil)
}

// Get retrieves a value by key with default read options.
//
// Returns `ErrNotFound` if the key does not exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.GetWithOptions(key, nil)
}

// Delete removes a key using default write options.
//
// Returns successfully even if the key does not exist.
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, nil)
}

// PutWithOptions stores a key-value pair with explicit put/write options.
//
// `putOpts` controls TTL behavior and `writeOpts` controls durability waiting.
// Pass nil options to use SlateDB defaults.
//
// Example:
//
//	putOpts := &slatedb.PutOptions{
//	    TTLType:  slatedb.TTLExpireAfter,
//	    TTLValue: 3600000, // 1 hour in milliseconds
//	}
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: true}
//	err := db.PutWithOptions([]byte("session:123"), []byte("data"), putOpts, writeOpts)
func (db *DB) PutWithOptions(key, value []byte, putOpts *PutOptions, writeOpts *WriteOptions) error {
	if db == nil || db.handle == nil {
		return ErrInvalidHandle
	}
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyPtr, keyLen := ptrFromBytes(key)
	valuePtr, valueLen := ptrFromBytes(value)
	cPutOpts := convertToCPutOptions(putOpts)
	cWriteOpts := convertToCWriteOptions(writeOpts)

	result := C.slatedb_db_put_with_options(
		db.handle,
		keyPtr,
		keyLen,
		valuePtr,
		valueLen,
		cPutOpts,
		cWriteOpts,
	)
	return resultToErrorAndFree(result)
}

// DeleteWithOptions removes a key with explicit write options.
//
// Pass nil options to use defaults.
//
// Example:
//
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: false}
//	err := db.DeleteWithOptions([]byte("temp:123"), writeOpts)
func (db *DB) DeleteWithOptions(key []byte, writeOpts *WriteOptions) error {
	if db == nil || db.handle == nil {
		return ErrInvalidHandle
	}
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	keyPtr, keyLen := ptrFromBytes(key)
	cWriteOpts := convertToCWriteOptions(writeOpts)

	result := C.slatedb_db_delete_with_options(
		db.handle,
		keyPtr,
		keyLen,
		cWriteOpts,
	)
	return resultToErrorAndFree(result)
}

// GetWithOptions retrieves a value by key with explicit read options.
//
// Pass nil options to use defaults.
// Returns `ErrNotFound` if the key does not exist.
//
// Example:
//
//	readOpts := &slatedb.ReadOptions{
//	    DurabilityFilter: slatedb.DurabilityRemote,
//	    Dirty:            false,
//	    CacheBlocks:      true,
//	}
//	value, err := db.GetWithOptions([]byte("user:123"), readOpts)
func (db *DB) GetWithOptions(key []byte, readOpts *ReadOptions) ([]byte, error) {
	if db == nil || db.handle == nil {
		return nil, ErrInvalidHandle
	}
	if len(key) == 0 {
		return nil, ErrInvalidArgument
	}

	keyPtr, keyLen := ptrFromBytes(key)
	cReadOpts := convertToCReadOptions(readOpts)

	var present C.bool
	var value *C.uint8_t
	var valueLen C.uintptr_t
	result := C.slatedb_db_get_with_options(
		db.handle,
		keyPtr,
		keyLen,
		cReadOpts,
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

// Write executes a WriteBatch atomically with default write options.
//
// The batch is consumed by this operation and cannot be reused.
// Always call `batch.Close()` when finished to release resources.
//
// Example:
//
//	batch, err := slatedb.NewWriteBatch()
//	if err != nil {
//	    return err
//	}
//	defer batch.Close()
//
//	batch.Put([]byte("key1"), []byte("value1"))
//	batch.Delete([]byte("key2"))
//
//	err = db.Write(batch)
func (db *DB) Write(batch *WriteBatch) error {
	return db.WriteWithOptions(batch, nil)
}

// WriteWithOptions executes a WriteBatch atomically with explicit write options.
//
// The batch is consumed by this operation and cannot be reused.
// Always call `batch.Close()` when finished to release resources.
//
// Example:
//
//	batch, err := slatedb.NewWriteBatch()
//	if err != nil {
//	    return err
//	}
//	defer batch.Close()
//
//	batch.Put([]byte("key1"), []byte("value1"))
//	writeOpts := &slatedb.WriteOptions{AwaitDurable: false}
//	err = db.WriteWithOptions(batch, writeOpts)
func (db *DB) WriteWithOptions(batch *WriteBatch, opts *WriteOptions) error {
	if db == nil || db.handle == nil {
		return ErrInvalidHandle
	}
	if batch == nil {
		return errors.New("batch cannot be nil")
	}
	if batch.closed {
		return errors.New("batch is closed")
	}
	if batch.consumed {
		return errors.New("batch already consumed")
	}
	if batch.ptr == nil {
		return errors.New("invalid batch")
	}

	cOpts := convertToCWriteOptions(opts)
	result := C.slatedb_db_write_with_options(db.handle, batch.ptr, cOpts)
	batch.consumed = true
	if err := resultToErrorAndFree(result); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	return nil
}

// Flush flushes pending writes using SlateDB default flush behavior.
//
// Call this before creating `DbReader` instances when you need to read freshly
// written data immediately.
func (db *DB) Flush() error {
	if db == nil || db.handle == nil {
		return ErrInvalidHandle
	}
	result := C.slatedb_db_flush(db.handle)
	return resultToErrorAndFree(result)
}

// Close closes the database connection and releases all resources.
//
// The `DB` must not be used after `Close` returns successfully.
func (db *DB) Close() error {
	if db == nil || db.handle == nil {
		return ErrInvalidHandle
	}

	result := C.slatedb_db_close(db.handle)
	if err := resultToErrorAndFree(result); err != nil {
		return err
	}
	db.handle = nil
	return nil
}

// Scan creates a streaming iterator for the range `[start, end)` with default options.
//
// `start=nil` means unbounded start; `end=nil` means unbounded end.
// The iterator must be closed after use.
//
// Example:
//
//	iter, err := db.Scan([]byte("user:"), []byte("user;"))
//	if err != nil { return err }
//	defer iter.Close()
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	    process(kv.Key, kv.Value)
//	}
func (db *DB) Scan(start, end []byte) (*Iterator, error) {
	return db.ScanWithOptions(start, end, nil)
}

// ScanWithOptions creates a streaming iterator for the range `[start, end)` with explicit scan options.
//
// Pass nil options to use defaults.
// The iterator must be closed after use.
//
// Example:
//
//	opts := &slatedb.ScanOptions{
//	    DurabilityFilter: slatedb.DurabilityRemote,
//	    Dirty:            false,
//	    ReadAheadBytes:   1024,
//	    CacheBlocks:      true,
//	    MaxFetchTasks:    2,
//	}
//	iter, err := db.ScanWithOptions([]byte("user:"), []byte("user;"), opts)
func (db *DB) ScanWithOptions(start, end []byte, opts *ScanOptions) (*Iterator, error) {
	if db == nil || db.handle == nil {
		return nil, ErrInvalidHandle
	}

	rangeValue := makeScanRange(start, end)
	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.slatedb_iterator_t
	result := C.slatedb_db_scan_with_options(db.handle, rangeValue, cOpts, &iterPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	if iterPtr == nil {
		return nil, errors.New("failed to create iterator")
	}

	return &Iterator{ptr: iterPtr}, nil
}

// ScanPrefix creates a streaming iterator for all keys that start with `prefix`.
//
// The iterator must be closed after use.
func (db *DB) ScanPrefix(prefix []byte) (*Iterator, error) {
	return db.ScanPrefixWithOptions(prefix, nil)
}

// ScanPrefixWithOptions creates a streaming iterator for `prefix` with explicit scan options.
//
// Pass nil options to use defaults.
// The iterator must be closed after use.
func (db *DB) ScanPrefixWithOptions(prefix []byte, opts *ScanOptions) (*Iterator, error) {
	if db == nil || db.handle == nil {
		return nil, ErrInvalidHandle
	}

	prefixPtr, prefixLen := ptrFromBytes(prefix)
	cOpts := convertToCScanOptions(opts)

	var iterPtr *C.slatedb_iterator_t
	result := C.slatedb_db_scan_prefix_with_options(
		db.handle,
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

// Metrics returns a snapshot of current database metrics.
//
// The returned map is decoded from the JSON payload produced by
// `slatedb_db_metrics`.
func (db *DB) Metrics() (map[string]int64, error) {
	if db == nil || db.handle == nil {
		return nil, ErrInvalidHandle
	}

	var jsonPtr *C.uint8_t
	var jsonLen C.uintptr_t
	result := C.slatedb_db_metrics(db.handle, &jsonPtr, &jsonLen)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer C.slatedb_bytes_free(jsonPtr, jsonLen)

	metrics := map[string]int64{}
	if jsonPtr == nil || jsonLen == 0 {
		return metrics, nil
	}

	if err := json.Unmarshal(C.GoBytes(unsafe.Pointer(jsonPtr), C.int(jsonLen)), &metrics); err != nil {
		return nil, err
	}
	return metrics, nil
}

// Builder mirrors SlateDB's Rust DbBuilder API.
type Builder struct {
	path         string
	url          *string
	envFile      *string
	settings     *Settings
	sstBlockSize *SstBlockSize
}

// NewBuilder creates a new database builder for `path`.
func NewBuilder(path string) (*Builder, error) {
	return &Builder{path: path}, nil
}

// WithUrl sets the object-store URL (for example `memory:///`, `file:///tmp/db`).
func (b *Builder) WithUrl(url string) *Builder {
	b.url = &url
	return b
}

// WithEnvFile sets the env file used when resolving object-store configuration.
func (b *Builder) WithEnvFile(envFile string) *Builder {
	b.envFile = &envFile
	return b
}

// WithSettings sets custom SlateDB settings for the builder.
func (b *Builder) WithSettings(settings *Settings) *Builder {
	b.settings = settings
	return b
}

// WithSstBlockSize sets the SST block size for the database.
func (b *Builder) WithSstBlockSize(size SstBlockSize) *Builder {
	b.sstBlockSize = &size
	return b
}

// Build constructs and opens a DB using the configured builder options.
//
// On success, the returned DB owns the open handle.
func (b *Builder) Build() (*DB, error) {
	objectStore, err := resolveObjectStoreHandle(b.url, b.envFile)
	if err != nil {
		return nil, err
	}
	defer closeObjectStoreHandle(objectStore)

	cPath := C.CString(b.path)
	defer C.free(unsafe.Pointer(cPath))

	var builderPtr *C.slatedb_db_builder_t
	newResult := C.slatedb_db_builder_new(cPath, objectStore, &builderPtr)
	if err := resultToErrorAndFree(newResult); err != nil {
		return nil, err
	}
	if builderPtr == nil {
		return nil, errors.New("failed to create database builder")
	}

	builderOwned := true
	defer func() {
		if builderOwned {
			closeBuilderHandle(builderPtr)
		}
	}()

	if b.settings != nil {
		settingsHandle, err := settingsHandleFromSettings(b.settings)
		if err != nil {
			return nil, err
		}
		if settingsHandle != nil {
			withSettingsResult := C.slatedb_db_builder_with_settings(builderPtr, settingsHandle)
			closeSettingsHandle(settingsHandle)
			if err := resultToErrorAndFree(withSettingsResult); err != nil {
				return nil, err
			}
		}
	}

	if b.sstBlockSize != nil {
		withSstResult := C.slatedb_db_builder_with_sst_block_size(builderPtr, C.uint8_t(*b.sstBlockSize))
		if err := resultToErrorAndFree(withSstResult); err != nil {
			return nil, err
		}
	}

	var dbHandle *C.slatedb_db_t
	buildResult := C.slatedb_db_builder_build(builderPtr, &dbHandle)
	builderOwned = false
	if err := resultToErrorAndFree(buildResult); err != nil {
		return nil, err
	}
	if dbHandle == nil {
		return nil, errors.New("failed to build database")
	}

	return &DB{handle: dbHandle}, nil
}
