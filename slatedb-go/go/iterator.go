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
	"io"
	"unsafe"
)

// Iterator represents a streaming iterator over key-value pairs
type Iterator struct {
	ptr    *C.CSdbIterator // Direct pointer to Rust iterator
	closed bool            // Track if iterator is closed
}

// Next returns the next key-value pair from the iterator
// Returns io.EOF when iteration is complete
// Returns other errors for actual failures
//
// CORRECT usage:
//
//	for {
//	    kv, err := iter.Next()
//	    if err == io.EOF {
//	        break // End of iteration
//	    }
//	    if err != nil {
//	        return err // Real error
//	    }
//	    process(kv)
//	}
func (iter *Iterator) Next() (KeyValue, error) {
	if iter.closed {
		return KeyValue{}, errors.New("iterator is closed")
	}

	if iter.ptr == nil {
		return KeyValue{}, errors.New("invalid iterator")
	}

	var cKeyValue C.CSdbKeyValue
	result := C.slatedb_iterator_next(iter.ptr, &cKeyValue)
	defer C.slatedb_free_result(result)

	if result.error == C.NotFound {
		return KeyValue{}, io.EOF // End of iteration
	}

	if result.error != C.Success {
		return KeyValue{}, resultToError(result)
	}

	// Convert C key-value to Go
	keyData := C.GoBytes(unsafe.Pointer(cKeyValue.key.data), C.int(cKeyValue.key.len))
	valueData := C.GoBytes(unsafe.Pointer(cKeyValue.value.data), C.int(cKeyValue.value.len))

	// Free the C memory allocated in Rust
	C.slatedb_free_value(cKeyValue.key)
	C.slatedb_free_value(cKeyValue.value)

	return KeyValue{
		Key:   keyData,
		Value: valueData,
	}, nil
}

// Seek moves the iterator to the specified key position
// After seek, Next() will return records starting from the seek key or the next available key
//
// Example:
//
//	iter.Seek([]byte("user:500"))
//	kv, err := iter.Next() // Returns first key >= "user:500"
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

	keyPtr := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	result := C.slatedb_iterator_seek(iter.ptr, keyPtr, C.uintptr_t(len(key)))
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}

// Close releases the iterator resources
// REQUIRED: Must be called before DB.Close() to prevent undefined behavior
//
// CORRECT usage:
//
//	iter, err := db.Scan(nil, nil, nil)
//	if err != nil { return err }
//	defer iter.Close()  // Always close iterator first
//	// ... use iterator ...
//	iter.Close()        // Explicit close before DB.Close()
//	db.Close()          // DB close after iterator close
func (iter *Iterator) Close() error {
	if iter.closed {
		return nil // Already closed
	}

	if iter.ptr == nil {
		return errors.New("invalid iterator")
	}

	result := C.slatedb_iterator_close(iter.ptr)
	defer C.slatedb_free_result(result)

	iter.closed = true
	iter.ptr = nil

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}
