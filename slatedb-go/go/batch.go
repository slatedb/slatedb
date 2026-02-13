package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"errors"
	"fmt"
)

// WriteBatch represents a batch of write operations that can be executed atomically.
type WriteBatch struct {
	ptr      *C.slatedb_write_batch_t
	closed   bool
	consumed bool
}

// NewWriteBatch creates a new WriteBatch for atomic operations.
func NewWriteBatch() (*WriteBatch, error) {
	var batchPtr *C.slatedb_write_batch_t
	result := C.slatedb_write_batch_new(&batchPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, fmt.Errorf("failed to create WriteBatch: %w", err)
	}
	if batchPtr == nil {
		return nil, errors.New("failed to create WriteBatch")
	}

	return &WriteBatch{
		ptr:      batchPtr,
		closed:   false,
		consumed: false,
	}, nil
}

func (b *WriteBatch) ensureOpen() error {
	if b.closed {
		return errors.New("batch is closed")
	}
	if b.consumed {
		return errors.New("batch already consumed")
	}
	if b.ptr == nil {
		return errors.New("invalid batch")
	}
	return nil
}

// Put adds a key-value pair to the batch with default options.
func (b *WriteBatch) Put(key, value []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	keyPtr, keyLen := ptrFromBytes(key)
	valuePtr, valueLen := ptrFromBytes(value)
	result := C.slatedb_write_batch_put(b.ptr, keyPtr, keyLen, valuePtr, valueLen)
	if err := resultToErrorAndFree(result); err != nil {
		return fmt.Errorf("failed to put key-value: %w", err)
	}
	return nil
}

// PutWithOptions adds a key-value pair to the batch with custom put options.
func (b *WriteBatch) PutWithOptions(key, value []byte, opts *PutOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	keyPtr, keyLen := ptrFromBytes(key)
	valuePtr, valueLen := ptrFromBytes(value)
	cOpts := convertToCPutOptions(opts)

	result := C.slatedb_write_batch_put_with_options(b.ptr, keyPtr, keyLen, valuePtr, valueLen, cOpts)
	if err := resultToErrorAndFree(result); err != nil {
		return fmt.Errorf("failed to put key-value with options: %w", err)
	}
	return nil
}

// Delete adds a delete operation to the batch.
func (b *WriteBatch) Delete(key []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	keyPtr, keyLen := ptrFromBytes(key)
	result := C.slatedb_write_batch_delete(b.ptr, keyPtr, keyLen)
	if err := resultToErrorAndFree(result); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	return nil
}

// Close releases the resources associated with the WriteBatch.
func (b *WriteBatch) Close() error {
	if b.closed {
		return errors.New("batch is already closed")
	}
	if b.ptr == nil {
		return errors.New("invalid batch")
	}

	result := C.slatedb_write_batch_close(b.ptr)
	if err := resultToErrorAndFree(result); err != nil {
		return fmt.Errorf("failed to close WriteBatch: %w", err)
	}

	b.closed = true
	b.ptr = nil
	return nil
}
