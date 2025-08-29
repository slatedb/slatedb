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

// WriteBatch represents a batch of write operations that can be executed atomically
type WriteBatch struct {
	ptr      *C.CSdbWriteBatch // Pointer to the Rust WriteBatch instance
	closed   bool              // Track if batch is closed
	consumed bool              // Track if batch was consumed by Write()
}

// NewWriteBatch creates a new WriteBatch for atomic operations
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
//	batch.Put([]byte("key2"), []byte("value2"))
//	batch.Delete([]byte("key3"))
//
//	err = db.Write(batch)
func NewWriteBatch() (*WriteBatch, error) {
	var batchPtr *C.CSdbWriteBatch

	result := C.slatedb_write_batch_new(&batchPtr)
	if err := resultToError(result); err != nil {
		return nil, fmt.Errorf("failed to create WriteBatch: %w", err)
	}

	return &WriteBatch{
		ptr:      batchPtr,
		closed:   false,
		consumed: false,
	}, nil
}

// Put adds a key-value pair to the batch with default options
func (b *WriteBatch) Put(key, value []byte) error {
	if b.closed {
		return errors.New("batch is closed")
	}
	if b.consumed {
		return errors.New("batch already consumed")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	var keyPtr *C.uint8_t
	var valuePtr *C.uint8_t

	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.slatedb_write_batch_put(
		b.ptr,
		keyPtr, C.size_t(len(key)),
		valuePtr, C.size_t(len(value)),
	)

	if err := resultToError(result); err != nil {
		return fmt.Errorf("failed to put key-value: %w", err)
	}
	return nil
}

// PutWithOptions adds a key-value pair to the batch with custom put options
//
// Example:
//
//	putOpts := &slatedb.PutOptions{
//	    TTLType:  slatedb.TTLExpireAfter,
//	    TTLValue: 3600000, // 1 hour in milliseconds
//	}
//	err := batch.PutWithOptions([]byte("session:123"), []byte("data"), putOpts)
func (b *WriteBatch) PutWithOptions(key, value []byte, opts *PutOptions) error {
	if b.closed {
		return errors.New("batch is closed")
	}
	if b.consumed {
		return errors.New("batch already consumed")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	var keyPtr *C.uint8_t
	var valuePtr *C.uint8_t

	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	cOpts := convertToCPutOptions(opts)

	result := C.slatedb_write_batch_put_with_options(
		b.ptr,
		keyPtr, C.size_t(len(key)),
		valuePtr, C.size_t(len(value)),
		cOpts,
	)

	if err := resultToError(result); err != nil {
		return fmt.Errorf("failed to put key-value with options: %w", err)
	}
	return nil
}

// Delete adds a delete operation to the batch
func (b *WriteBatch) Delete(key []byte) error {
	if b.closed {
		return errors.New("batch is closed")
	}
	if b.consumed {
		return errors.New("batch already consumed")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	var keyPtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.slatedb_write_batch_delete(
		b.ptr,
		keyPtr, C.size_t(len(key)),
	)

	if err := resultToError(result); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	return nil
}

// Close releases the resources associated with the WriteBatch
// This must always be called to prevent memory leaks, even if the batch was consumed by Write()
func (b *WriteBatch) Close() error {
	if b.closed {
		return errors.New("batch is already closed")
	}

	result := C.slatedb_write_batch_close(b.ptr)
	if err := resultToError(result); err != nil {
		return fmt.Errorf("failed to close WriteBatch: %w", err)
	}

	b.closed = true
	b.ptr = nil
	return nil
}
