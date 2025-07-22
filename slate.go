package slatedb

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR}/target/release -lslatedb
#include "slatedb.h"
#include <stdlib.h>

// Forward declaration (generated bigger header will match these types).
extern void goKvCollector(unsigned char* key_ptr,
                          unsigned int key_len,
                          unsigned char* val_ptr,
                          unsigned int val_len,
                          void* userdata);

void kv_trampoline(const unsigned char* key_ptr,
                   unsigned int key_len,
                   const unsigned char* val_ptr,
                   unsigned int val_len,
                   void* userdata);
*/
import "C"

import (
	cgoh "runtime/cgo"
	"unsafe"
)

// must panics on non-zero (error) return codes from the C side.
func must(ok C.uint) {
	if ok == 0 {
		panic("slatedb error")
	}
}

func lastErr() string {
	if ptr := C.slatedb_last_error(); ptr != nil {
		return C.GoString((*C.char)(ptr))
	}
	return "unknown"
}

// Open opens (or creates) a database at `path` using whichever object store
// is configured through environment variables (see README).
func Open(path string) C.SlateDbHandle {
	h := C.slatedb_open(C.CString(path))
	if h._0 == nil {
		panic("slatedb_open failed: " + lastErr())
	}
	return h
}

// Close closes the database handle.
func Close(h C.SlateDbHandle) {
	C.slatedb_close(h)
}

// Put inserts or overwrites a key/value pair.
func Put(h C.SlateDbHandle, key, val []byte) {
	if len(key) == 0 {
		panic("key must be non-empty")
	}
	if len(val) == 0 {
		panic("value must be non-empty")
	}
	must(C.slatedb_put(
		h,
		(*C.uchar)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		(*C.uchar)(unsafe.Pointer(&val[0])),
		C.uint(len(val)),
	))
}

// Delete removes a key.
func Delete(h C.SlateDbHandle, key []byte) {
	if len(key) == 0 {
		panic("key must be non-empty")
	}
	must(C.slatedb_delete(
		h,
		(*C.uchar)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
	))
}

// KV represents a key/value pair returned via iteration.
type KV struct {
	Key []byte
	Val []byte
}

//export goKvCollector
func goKvCollector(keyPtr *C.uchar, keyLen C.uint, valPtr *C.uchar, valLen C.uint, userdata unsafe.Pointer) {
	handle := cgoh.Handle(uintptr(userdata))
	slicePtr := handle.Value().(*[]KV)
	key := C.GoBytes(unsafe.Pointer(keyPtr), C.int(keyLen))
	val := C.GoBytes(unsafe.Pointer(valPtr), C.int(valLen))
	*slicePtr = append(*slicePtr, KV{Key: key, Val: val})
}

// IterateAll scans the entire key-space and returns the records found.
func IterateAll(h C.SlateDbHandle) []KV {
	var kvs []KV
	hdl := cgoh.NewHandle(&kvs)
	defer hdl.Delete()

	must(C.slatedb_iterate(h, C.SlateDbIterCallback(C.kv_trampoline), unsafe.Pointer(uintptr(hdl))))
	return kvs
}
