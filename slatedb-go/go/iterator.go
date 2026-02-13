package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"errors"
	"io"
)

// Iterator represents a streaming iterator over key-value pairs.
type Iterator struct {
	ptr    *C.slatedb_iterator_t
	closed bool
}

// Next returns the next key-value pair from the iterator.
// Returns io.EOF when iteration is complete.
func (iter *Iterator) Next() (KeyValue, error) {
	if iter.closed {
		return KeyValue{}, errors.New("iterator is closed")
	}
	if iter.ptr == nil {
		return KeyValue{}, errors.New("invalid iterator")
	}

	var present C.bool
	var keyPtr *C.uint8_t
	var keyLen C.uintptr_t
	var valuePtr *C.uint8_t
	var valueLen C.uintptr_t

	result := C.slatedb_iterator_next(
		iter.ptr,
		&present,
		&keyPtr,
		&keyLen,
		&valuePtr,
		&valueLen,
	)
	if err := resultToErrorAndFree(result); err != nil {
		return KeyValue{}, err
	}

	if present == C.bool(false) {
		return KeyValue{}, io.EOF
	}

	return KeyValue{
		Key:   copyBytesAndFree(keyPtr, keyLen),
		Value: copyBytesAndFree(valuePtr, valueLen),
	}, nil
}

// Seek moves the iterator to the specified key position.
func (iter *Iterator) Seek(key []byte) error {
	if iter.closed {
		return errors.New("iterator is closed")
	}
	if iter.ptr == nil {
		return errors.New("invalid iterator")
	}
	if len(key) == 0 {
		return errors.New("seek key cannot be empty")
	}

	keyPtr, keyLen := ptrFromBytes(key)
	result := C.slatedb_iterator_seek(iter.ptr, keyPtr, keyLen)
	return resultToErrorAndFree(result)
}

// Close releases the iterator resources.
func (iter *Iterator) Close() error {
	if iter.closed {
		return nil
	}
	if iter.ptr == nil {
		return errors.New("invalid iterator")
	}

	result := C.slatedb_iterator_close(iter.ptr)
	iter.closed = true
	iter.ptr = nil
	return resultToErrorAndFree(result)
}
