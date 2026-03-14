package ffi

// #include <slatedb.h>
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"unsafe"
)

// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer{
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer{
		capacity: C.uint64_t(b.Capacity()),
		len:      C.uint64_t(b.Len()),
		data:     (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer{
		inner: C.RustBuffer{
			capacity: C.uint64_t(b.Capacity()),
			len:      C.uint64_t(b.Len()),
			data:     (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func(status *C.RustCallStatus) bool {
		C.ffi_slatedb_ffi_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}

func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes{
		len:  C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}

	return rustCall(func(status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_slatedb_ffi_rustbuffer_from_bytes(foreign, status)
	})
}

type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}

func rustCallWithError[E any, U any](converter BufReader[*E], callback func(*C.RustCallStatus) U) (U, *E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[*E], status C.RustCallStatus) *E {
	switch status.code {
	case 0:
		return nil
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer{inner: status.errorBuf})
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{inner: status.errorBuf})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}

func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {

	FfiConverterFfiLogCallbackINSTANCE.register()
	FfiConverterFfiMergeOperatorINSTANCE.register()
	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 29
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_slatedb_ffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("slatedb: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_func_ffi_init_logging()
		})
		if checksum != 42559 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_ffi_init_logging: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_begin()
		})
		if checksum != 27320 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_begin: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_delete()
		})
		if checksum != 23170 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_delete_with_options()
		})
		if checksum != 36049 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_delete_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_flush()
		})
		if checksum != 8494 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_flush: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_flush_with_options()
		})
		if checksum != 49534 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_flush_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_get()
		})
		if checksum != 41469 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value()
		})
		if checksum != 4319 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value_with_options()
		})
		if checksum != 30550 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_get_with_options()
		})
		if checksum != 24693 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_merge()
		})
		if checksum != 17131 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_merge_with_options()
		})
		if checksum != 20187 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_metrics()
		})
		if checksum != 17180 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_metrics: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_put()
		})
		if checksum != 35423 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_put_with_options()
		})
		if checksum != 3469 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_scan()
		})
		if checksum != 13606 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix()
		})
		if checksum != 35855 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix_with_options()
		})
		if checksum != 28845 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_scan_with_options()
		})
		if checksum != 16422 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_shutdown()
		})
		if checksum != 62539 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_snapshot()
		})
		if checksum != 61667 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_snapshot: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_status()
		})
		if checksum != 6095 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_status: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_write()
		})
		if checksum != 50936 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidb_write_with_options()
		})
		if checksum != 566 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidb_write_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_build()
		})
		if checksum != 20111 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_db_cache_disabled()
		})
		if checksum != 29934 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_db_cache_disabled: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_merge_operator()
		})
		if checksum != 13534 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_seed()
		})
		if checksum != 50434 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_seed: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_settings()
		})
		if checksum != 21510 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_settings: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_sst_block_size()
		})
		if checksum != 43234 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_sst_block_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_wal_object_store()
		})
		if checksum != 54976 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbiterator_next()
		})
		if checksum != 35396 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbiterator_seek()
		})
		if checksum != 16218 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbiterator_seek: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_get()
		})
		if checksum != 46156 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_get_with_options()
		})
		if checksum != 13809 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan()
		})
		if checksum != 587 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix()
		})
		if checksum != 2092 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix_with_options()
		})
		if checksum != 2695 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_with_options()
		})
		if checksum != 19389 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreader_shutdown()
		})
		if checksum != 60272 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreader_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_build()
		})
		if checksum != 10134 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_checkpoint_id()
		})
		if checksum != 43622 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_checkpoint_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_merge_operator()
		})
		if checksum != 48295 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_options()
		})
		if checksum != 1731 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_wal_object_store()
		})
		if checksum != 35691 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get()
		})
		if checksum != 20211 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value()
		})
		if checksum != 59292 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value_with_options()
		})
		if checksum != 4124 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_with_options()
		})
		if checksum != 20033 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan()
		})
		if checksum != 5732 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix()
		})
		if checksum != 43819 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix_with_options()
		})
		if checksum != 41123 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_with_options()
		})
		if checksum != 10211 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit()
		})
		if checksum != 48317 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit_with_options()
		})
		if checksum != 23549 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_delete()
		})
		if checksum != 16039 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get()
		})
		if checksum != 22094 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value()
		})
		if checksum != 27481 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value_with_options()
		})
		if checksum != 41640 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_with_options()
		})
		if checksum != 43185 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_id()
		})
		if checksum != 54287 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_mark_read()
		})
		if checksum != 22753 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_mark_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge()
		})
		if checksum != 35715 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge_with_options()
		})
		if checksum != 25941 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put()
		})
		if checksum != 44076 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put_with_options()
		})
		if checksum != 21098 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_rollback()
		})
		if checksum != 9781 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_rollback: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan()
		})
		if checksum != 36573 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix()
		})
		if checksum != 49503 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix_with_options()
		})
		if checksum != 29076 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_with_options()
		})
		if checksum != 34855 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_seqnum()
		})
		if checksum != 22659 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_seqnum: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_unmark_write()
		})
		if checksum != 13895 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffidbtransaction_unmark_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffilogcallback_log()
		})
		if checksum != 63108 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffilogcallback_log: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffimergeoperator_merge()
		})
		if checksum != 64881 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffimergeoperator_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffisettings_set()
		})
		if checksum != 25271 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffisettings_set: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffisettings_to_json_string()
		})
		if checksum != 6471 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffisettings_to_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_id()
		})
		if checksum != 4476 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_iterator()
		})
		if checksum != 26463 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_iterator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_metadata()
		})
		if checksum != 53251 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_file()
		})
		if checksum != 583 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_id()
		})
		if checksum != 17961 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfile_shutdown()
		})
		if checksum != 1197 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfile_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_next()
		})
		if checksum != 25733 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_shutdown()
		})
		if checksum != 10991 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalreader_get()
		})
		if checksum != 42864 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalreader_list()
		})
		if checksum != 64696 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalreader_list: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwalreader_shutdown()
		})
		if checksum != 48499 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwalreader_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_delete()
		})
		if checksum != 48646 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwritebatch_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge()
		})
		if checksum != 16539 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge_with_options()
		})
		if checksum != 14093 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put()
		})
		if checksum != 9515 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put_with_options()
		})
		if checksum != 35097 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffidbbuilder_new()
		})
		if checksum != 22766 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffidbbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffidbreaderbuilder_new()
		})
		if checksum != 56685 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffidbreaderbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_from_env()
		})
		if checksum != 29333 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_resolve()
		})
		if checksum != 52192 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_resolve: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_default()
		})
		if checksum != 37692 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env()
		})
		if checksum != 42524 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env_with_default()
		})
		if checksum != 57925 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env_with_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_file()
		})
		if checksum != 47421 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_json_string()
		})
		if checksum != 32936 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffisettings_load()
		})
		if checksum != 61074 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffisettings_load: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffiwalreader_new()
		})
		if checksum != 2695 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffiwalreader_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_ffiwritebatch_new()
		})
		if checksum != 46470 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_ffiwritebatch_new: UniFFI API checksum mismatch")
		}
	}
}

type FfiConverterUint32 struct{}

var FfiConverterUint32INSTANCE = FfiConverterUint32{}

func (FfiConverterUint32) Lower(value uint32) C.uint32_t {
	return C.uint32_t(value)
}

func (FfiConverterUint32) Write(writer io.Writer, value uint32) {
	writeUint32(writer, value)
}

func (FfiConverterUint32) Lift(value C.uint32_t) uint32 {
	return uint32(value)
}

func (FfiConverterUint32) Read(reader io.Reader) uint32 {
	return readUint32(reader)
}

type FfiDestroyerUint32 struct{}

func (FfiDestroyerUint32) Destroy(_ uint32) {}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct{}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterInt64 struct{}

var FfiConverterInt64INSTANCE = FfiConverterInt64{}

func (FfiConverterInt64) Lower(value int64) C.int64_t {
	return C.int64_t(value)
}

func (FfiConverterInt64) Write(writer io.Writer, value int64) {
	writeInt64(writer, value)
}

func (FfiConverterInt64) Lift(value C.int64_t) int64 {
	return int64(value)
}

func (FfiConverterInt64) Read(reader io.Reader) int64 {
	return readInt64(reader)
}

type FfiDestroyerInt64 struct{}

func (FfiDestroyerInt64) Destroy(_ int64) {}

type FfiConverterBool struct{}

var FfiConverterBoolINSTANCE = FfiConverterBool{}

func (FfiConverterBool) Lower(value bool) C.int8_t {
	if value {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

func (FfiConverterBool) Write(writer io.Writer, value bool) {
	if value {
		writeInt8(writer, 1)
	} else {
		writeInt8(writer, 0)
	}
}

func (FfiConverterBool) Lift(value C.int8_t) bool {
	return value != 0
}

func (FfiConverterBool) Read(reader io.Reader) bool {
	return readInt8(reader) != 0
}

type FfiDestroyerBool struct{}

func (FfiDestroyerBool) Destroy(_ bool) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct{}

func (FfiDestroyerString) Destroy(_ string) {}

type FfiConverterBytes struct{}

var FfiConverterBytesINSTANCE = FfiConverterBytes{}

func (c FfiConverterBytes) Lower(value []byte) C.RustBuffer {
	return LowerIntoRustBuffer[[]byte](c, value)
}

func (c FfiConverterBytes) LowerExternal(value []byte) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterBytes) Write(writer io.Writer, value []byte) {
	if len(value) > math.MaxInt32 {
		panic("[]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := writer.Write(value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing []byte, expected %d, written %d", len(value), write_length))
	}
}

func (c FfiConverterBytes) Lift(rb RustBufferI) []byte {
	return LiftFromRustBuffer[[]byte](c, rb)
}

func (c FfiConverterBytes) Read(reader io.Reader) []byte {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading []byte, expected %d, read %d", length, read_length))
	}
	return buffer
}

type FfiDestroyerBytes struct{}

func (FfiDestroyerBytes) Destroy(_ []byte) {}

// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	pointer       unsafe.Pointer
	callCounter   atomic.Int64
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer
	freeFunction  func(unsafe.Pointer, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	pointer unsafe.Pointer,
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer,
	freeFunction func(unsafe.Pointer, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		pointer:       pointer,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) unsafe.Pointer {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter+1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) unsafe.Pointer {
		return ffiObject.cloneFunction(ffiObject.pointer, status)
	})
}

func (ffiObject *FfiObject) decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject) destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject) freeRustArcPtr() {
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.pointer, status)
		return 0
	})
}

type FfiDbInterface interface {
	Begin(isolationLevel FfiIsolationLevel) (*FfiDbTransaction, error)
	Delete(key []byte) (FfiWriteHandle, error)
	DeleteWithOptions(key []byte, options FfiWriteOptions) (FfiWriteHandle, error)
	Flush() error
	FlushWithOptions(options FfiFlushOptions) error
	Get(key []byte) (*[]byte, error)
	GetKeyValue(key []byte) (*FfiKeyValue, error)
	GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error)
	GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error)
	Merge(key []byte, operand []byte) (FfiWriteHandle, error)
	MergeWithOptions(key []byte, operand []byte, mergeOptions FfiMergeOptions, writeOptions FfiWriteOptions) (FfiWriteHandle, error)
	Metrics() (map[string]int64, error)
	Put(key []byte, value []byte) (FfiWriteHandle, error)
	PutWithOptions(key []byte, value []byte, putOptions FfiPutOptions, writeOptions FfiWriteOptions) (FfiWriteHandle, error)
	Scan(varRange FfiKeyRange) (*FfiDbIterator, error)
	ScanPrefix(prefix []byte) (*FfiDbIterator, error)
	ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error)
	ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error)
	Shutdown() error
	Snapshot() (*FfiDbSnapshot, error)
	Status() error
	Write(batch *FfiWriteBatch) (FfiWriteHandle, error)
	WriteWithOptions(batch *FfiWriteBatch, options FfiWriteOptions) (FfiWriteHandle, error)
}
type FfiDb struct {
	ffiObject FfiObject
}

func (_self *FfiDb) Begin(isolationLevel FfiIsolationLevel) (*FfiDbTransaction, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbTransaction {
			return FfiConverterFfiDbTransactionINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_begin(
			_pointer, FfiConverterFfiIsolationLevelINSTANCE.Lower(isolationLevel)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Delete(key []byte) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) DeleteWithOptions(key []byte, options FfiWriteOptions) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_delete_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Flush() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidb_flush(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDb) FlushWithOptions(options FfiFlushOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidb_flush_with_options(
			_pointer, FfiConverterFfiFlushOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDb) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) GetKeyValue(key []byte) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Merge(key []byte, operand []byte) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) MergeWithOptions(key []byte, operand []byte, mergeOptions FfiMergeOptions, writeOptions FfiWriteOptions) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterFfiMergeOptionsINSTANCE.Lower(mergeOptions), FfiConverterFfiWriteOptionsINSTANCE.Lower(writeOptions)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Metrics() (map[string]int64, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_ffidb_metrics(
				_pointer, _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue map[string]int64
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterMapStringInt64INSTANCE.Lift(_uniffiRV), nil
	}
}

func (_self *FfiDb) Put(key []byte, value []byte) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) PutWithOptions(key []byte, value []byte, putOptions FfiPutOptions, writeOptions FfiWriteOptions) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterFfiPutOptionsINSTANCE.Lower(putOptions), FfiConverterFfiWriteOptionsINSTANCE.Lower(writeOptions)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Scan(varRange FfiKeyRange) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_scan(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) ScanPrefix(prefix []byte) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_scan_with_options(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidb_shutdown(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDb) Snapshot() (*FfiDbSnapshot, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbSnapshot {
			return FfiConverterFfiDbSnapshotINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_snapshot(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) Status() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidb_status(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDb) Write(batch *FfiWriteBatch) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_write(
			_pointer, FfiConverterFfiWriteBatchINSTANCE.Lower(batch)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDb) WriteWithOptions(batch *FfiWriteBatch, options FfiWriteOptions) (FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDb")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWriteHandle {
			return FfiConverterFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidb_write_with_options(
			_pointer, FfiConverterFfiWriteBatchINSTANCE.Lower(batch), FfiConverterFfiWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *FfiDb) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDb struct{}

var FfiConverterFfiDbINSTANCE = FfiConverterFfiDb{}

func (c FfiConverterFfiDb) Lift(pointer unsafe.Pointer) *FfiDb {
	result := &FfiDb{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidb(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidb(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDb).Destroy)
	return result
}

func (c FfiConverterFfiDb) Read(reader io.Reader) *FfiDb {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDb) Lower(value *FfiDb) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDb")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDb) Write(writer io.Writer, value *FfiDb) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDb struct{}

func (_ FfiDestroyerFfiDb) Destroy(value *FfiDb) {
	value.Destroy()
}

type FfiDbBuilderInterface interface {
	Build() (*FfiDb, error)
	WithDbCacheDisabled() error
	WithMergeOperator(mergeOperator FfiMergeOperator) error
	WithSeed(seed uint64) error
	WithSettings(settings *FfiSettings) error
	WithSstBlockSize(sstBlockSize FfiSstBlockSize) error
	WithWalObjectStore(walObjectStore *FfiObjectStore) error
}
type FfiDbBuilder struct {
	ffiObject FfiObject
}

func NewFfiDbBuilder(path string, objectStore *FfiObjectStore) *FfiDbBuilder {
	return FfiConverterFfiDbBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffidbbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterFfiObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

func (_self *FfiDbBuilder) Build() (*FfiDb, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDb {
			return FfiConverterFfiDbINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbBuilder) WithDbCacheDisabled() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_db_cache_disabled(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbBuilder) WithMergeOperator(mergeOperator FfiMergeOperator) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_merge_operator(
			_pointer, FfiConverterFfiMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbBuilder) WithSeed(seed uint64) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_seed(
			_pointer, FfiConverterUint64INSTANCE.Lower(seed), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbBuilder) WithSettings(settings *FfiSettings) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_settings(
			_pointer, FfiConverterFfiSettingsINSTANCE.Lower(settings), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbBuilder) WithSstBlockSize(sstBlockSize FfiSstBlockSize) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_sst_block_size(
			_pointer, FfiConverterFfiSstBlockSizeINSTANCE.Lower(sstBlockSize), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbBuilder) WithWalObjectStore(walObjectStore *FfiObjectStore) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_wal_object_store(
			_pointer, FfiConverterFfiObjectStoreINSTANCE.Lower(walObjectStore), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiDbBuilder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbBuilder struct{}

var FfiConverterFfiDbBuilderINSTANCE = FfiConverterFfiDbBuilder{}

func (c FfiConverterFfiDbBuilder) Lift(pointer unsafe.Pointer) *FfiDbBuilder {
	result := &FfiDbBuilder{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbbuilder(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbBuilder).Destroy)
	return result
}

func (c FfiConverterFfiDbBuilder) Read(reader io.Reader) *FfiDbBuilder {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbBuilder) Lower(value *FfiDbBuilder) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbBuilder")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbBuilder) Write(writer io.Writer, value *FfiDbBuilder) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbBuilder struct{}

func (_ FfiDestroyerFfiDbBuilder) Destroy(value *FfiDbBuilder) {
	value.Destroy()
}

type FfiDbIteratorInterface interface {
	Next() (*FfiKeyValue, error)
	Seek(key []byte) error
}
type FfiDbIterator struct {
	ffiObject FfiObject
}

func (_self *FfiDbIterator) Next() (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbiterator_next(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbIterator) Seek(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbIterator")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbiterator_seek(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}
func (object *FfiDbIterator) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbIterator struct{}

var FfiConverterFfiDbIteratorINSTANCE = FfiConverterFfiDbIterator{}

func (c FfiConverterFfiDbIterator) Lift(pointer unsafe.Pointer) *FfiDbIterator {
	result := &FfiDbIterator{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbiterator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbIterator).Destroy)
	return result
}

func (c FfiConverterFfiDbIterator) Read(reader io.Reader) *FfiDbIterator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbIterator) Lower(value *FfiDbIterator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbIterator")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbIterator) Write(writer io.Writer, value *FfiDbIterator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbIterator struct{}

func (_ FfiDestroyerFfiDbIterator) Destroy(value *FfiDbIterator) {
	value.Destroy()
}

type FfiDbReaderInterface interface {
	Get(key []byte) (*[]byte, error)
	GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error)
	Scan(varRange FfiKeyRange) (*FfiDbIterator, error)
	ScanPrefix(prefix []byte) (*FfiDbIterator, error)
	ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error)
	ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error)
	Shutdown() error
}
type FfiDbReader struct {
	ffiObject FfiObject
}

func (_self *FfiDbReader) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) Scan(varRange FfiKeyRange) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_scan(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) ScanPrefix(prefix []byte) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_scan_with_options(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReader) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReader")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbreader_shutdown(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}
func (object *FfiDbReader) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbReader struct{}

var FfiConverterFfiDbReaderINSTANCE = FfiConverterFfiDbReader{}

func (c FfiConverterFfiDbReader) Lift(pointer unsafe.Pointer) *FfiDbReader {
	result := &FfiDbReader{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbreader(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbReader).Destroy)
	return result
}

func (c FfiConverterFfiDbReader) Read(reader io.Reader) *FfiDbReader {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbReader) Lower(value *FfiDbReader) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbReader")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbReader) Write(writer io.Writer, value *FfiDbReader) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbReader struct{}

func (_ FfiDestroyerFfiDbReader) Destroy(value *FfiDbReader) {
	value.Destroy()
}

type FfiDbReaderBuilderInterface interface {
	Build() (*FfiDbReader, error)
	WithCheckpointId(checkpointId string) error
	WithMergeOperator(mergeOperator FfiMergeOperator) error
	WithOptions(options FfiReaderOptions) error
	WithWalObjectStore(walObjectStore *FfiObjectStore) error
}
type FfiDbReaderBuilder struct {
	ffiObject FfiObject
}

func NewFfiDbReaderBuilder(path string, objectStore *FfiObjectStore) *FfiDbReaderBuilder {
	return FfiConverterFfiDbReaderBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffidbreaderbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterFfiObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

func (_self *FfiDbReaderBuilder) Build() (*FfiDbReader, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbReader {
			return FfiConverterFfiDbReaderINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbReaderBuilder) WithCheckpointId(checkpointId string) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_checkpoint_id(
			_pointer, FfiConverterStringINSTANCE.Lower(checkpointId), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbReaderBuilder) WithMergeOperator(mergeOperator FfiMergeOperator) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_merge_operator(
			_pointer, FfiConverterFfiMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbReaderBuilder) WithOptions(options FfiReaderOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_options(
			_pointer, FfiConverterFfiReaderOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiDbReaderBuilder) WithWalObjectStore(walObjectStore *FfiObjectStore) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_wal_object_store(
			_pointer, FfiConverterFfiObjectStoreINSTANCE.Lower(walObjectStore), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiDbReaderBuilder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbReaderBuilder struct{}

var FfiConverterFfiDbReaderBuilderINSTANCE = FfiConverterFfiDbReaderBuilder{}

func (c FfiConverterFfiDbReaderBuilder) Lift(pointer unsafe.Pointer) *FfiDbReaderBuilder {
	result := &FfiDbReaderBuilder{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbreaderbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbreaderbuilder(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbReaderBuilder).Destroy)
	return result
}

func (c FfiConverterFfiDbReaderBuilder) Read(reader io.Reader) *FfiDbReaderBuilder {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbReaderBuilder) Lower(value *FfiDbReaderBuilder) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbReaderBuilder")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbReaderBuilder) Write(writer io.Writer, value *FfiDbReaderBuilder) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbReaderBuilder struct{}

func (_ FfiDestroyerFfiDbReaderBuilder) Destroy(value *FfiDbReaderBuilder) {
	value.Destroy()
}

type FfiDbSnapshotInterface interface {
	Get(key []byte) (*[]byte, error)
	GetKeyValue(key []byte) (*FfiKeyValue, error)
	GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error)
	GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error)
	Scan(varRange FfiKeyRange) (*FfiDbIterator, error)
	ScanPrefix(prefix []byte) (*FfiDbIterator, error)
	ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error)
	ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error)
}
type FfiDbSnapshot struct {
	ffiObject FfiObject
}

func (_self *FfiDbSnapshot) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) GetKeyValue(key []byte) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) Scan(varRange FfiKeyRange) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) ScanPrefix(prefix []byte) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbSnapshot) ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_with_options(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}
func (object *FfiDbSnapshot) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbSnapshot struct{}

var FfiConverterFfiDbSnapshotINSTANCE = FfiConverterFfiDbSnapshot{}

func (c FfiConverterFfiDbSnapshot) Lift(pointer unsafe.Pointer) *FfiDbSnapshot {
	result := &FfiDbSnapshot{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbsnapshot(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbsnapshot(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbSnapshot).Destroy)
	return result
}

func (c FfiConverterFfiDbSnapshot) Read(reader io.Reader) *FfiDbSnapshot {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbSnapshot) Lower(value *FfiDbSnapshot) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbSnapshot")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbSnapshot) Write(writer io.Writer, value *FfiDbSnapshot) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbSnapshot struct{}

func (_ FfiDestroyerFfiDbSnapshot) Destroy(value *FfiDbSnapshot) {
	value.Destroy()
}

type FfiDbTransactionInterface interface {
	Commit() (*FfiWriteHandle, error)
	CommitWithOptions(options FfiWriteOptions) (*FfiWriteHandle, error)
	Delete(key []byte) error
	Get(key []byte) (*[]byte, error)
	GetKeyValue(key []byte) (*FfiKeyValue, error)
	GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error)
	GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error)
	Id() string
	MarkRead(keys [][]byte) error
	Merge(key []byte, operand []byte) error
	MergeWithOptions(key []byte, operand []byte, options FfiMergeOptions) error
	Put(key []byte, value []byte) error
	PutWithOptions(key []byte, value []byte, options FfiPutOptions) error
	Rollback() error
	Scan(varRange FfiKeyRange) (*FfiDbIterator, error)
	ScanPrefix(prefix []byte) (*FfiDbIterator, error)
	ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error)
	ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error)
	Seqnum() uint64
	UnmarkWrite(keys [][]byte) error
}
type FfiDbTransaction struct {
	ffiObject FfiObject
}

func (_self *FfiDbTransaction) Commit() (*FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiWriteHandle {
			return FfiConverterOptionalFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_commit(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) CommitWithOptions(options FfiWriteOptions) (*FfiWriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiWriteHandle {
			return FfiConverterOptionalFfiWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_commit_with_options(
			_pointer, FfiConverterFfiWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) GetKeyValue(key []byte) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) GetKeyValueWithOptions(key []byte, options FfiReadOptions) (*FfiKeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiKeyValue {
			return FfiConverterOptionalFfiKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) GetWithOptions(key []byte, options FfiReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterFfiReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) Id() string {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_id(
				_pointer, _uniffiStatus),
		}
	}))
}

func (_self *FfiDbTransaction) MarkRead(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_mark_read(
			_pointer, FfiConverterSequenceBytesINSTANCE.Lower(keys)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) MergeWithOptions(key []byte, operand []byte, options FfiMergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterFfiMergeOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) PutWithOptions(key []byte, value []byte, options FfiPutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterFfiPutOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) Rollback() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_rollback(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

func (_self *FfiDbTransaction) Scan(varRange FfiKeyRange) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) ScanPrefix(prefix []byte) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) ScanPrefixWithOptions(prefix []byte, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) ScanWithOptions(varRange FfiKeyRange, options FfiScanOptions) (*FfiDbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiDbIterator {
			return FfiConverterFfiDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_with_options(
			_pointer, FfiConverterFfiKeyRangeINSTANCE.Lower(varRange), FfiConverterFfiScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiDbTransaction) Seqnum() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_seqnum(
			_pointer, _uniffiStatus)
	}))
}

func (_self *FfiDbTransaction) UnmarkWrite(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiDbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_ffidbtransaction_unmark_write(
			_pointer, FfiConverterSequenceBytesINSTANCE.Lower(keys)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}
func (object *FfiDbTransaction) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiDbTransaction struct{}

var FfiConverterFfiDbTransactionINSTANCE = FfiConverterFfiDbTransaction{}

func (c FfiConverterFfiDbTransaction) Lift(pointer unsafe.Pointer) *FfiDbTransaction {
	result := &FfiDbTransaction{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffidbtransaction(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffidbtransaction(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiDbTransaction).Destroy)
	return result
}

func (c FfiConverterFfiDbTransaction) Read(reader io.Reader) *FfiDbTransaction {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiDbTransaction) Lower(value *FfiDbTransaction) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiDbTransaction")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiDbTransaction) Write(writer io.Writer, value *FfiDbTransaction) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiDbTransaction struct{}

func (_ FfiDestroyerFfiDbTransaction) Destroy(value *FfiDbTransaction) {
	value.Destroy()
}

type FfiLogCallback interface {
	Log(record FfiLogRecord)
}
type FfiLogCallbackImpl struct {
	ffiObject FfiObject
}

func (_self *FfiLogCallbackImpl) Log(record FfiLogRecord) {
	_pointer := _self.ffiObject.incrementPointer("FfiLogCallback")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffilogcallback_log(
			_pointer, FfiConverterFfiLogRecordINSTANCE.Lower(record), _uniffiStatus)
		return false
	})
}
func (object *FfiLogCallbackImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiLogCallback struct {
	handleMap *concurrentHandleMap[FfiLogCallback]
}

var FfiConverterFfiLogCallbackINSTANCE = FfiConverterFfiLogCallback{
	handleMap: newConcurrentHandleMap[FfiLogCallback](),
}

func (c FfiConverterFfiLogCallback) Lift(pointer unsafe.Pointer) FfiLogCallback {
	result := &FfiLogCallbackImpl{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffilogcallback(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffilogcallback(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiLogCallbackImpl).Destroy)
	return result
}

func (c FfiConverterFfiLogCallback) Read(reader io.Reader) FfiLogCallback {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiLogCallback) Lower(value FfiLogCallback) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := unsafe.Pointer(uintptr(c.handleMap.insert(value)))
	return pointer

}

func (c FfiConverterFfiLogCallback) Write(writer io.Writer, value FfiLogCallback) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiLogCallback struct{}

func (_ FfiDestroyerFfiLogCallback) Destroy(value FfiLogCallback) {
	if val, ok := value.(*FfiLogCallbackImpl); ok {
		val.Destroy()
	} else {
		panic("Expected *FfiLogCallbackImpl")
	}
}

type uniffiCallbackResult C.int8_t

const (
	uniffiIdxCallbackFree               uniffiCallbackResult = 0
	uniffiCallbackResultSuccess         uniffiCallbackResult = 0
	uniffiCallbackResultError           uniffiCallbackResult = 1
	uniffiCallbackUnexpectedResultError uniffiCallbackResult = 2
	uniffiCallbackCancelled             uniffiCallbackResult = 3
)

type concurrentHandleMap[T any] struct {
	handles       map[uint64]T
	currentHandle uint64
	lock          sync.RWMutex
}

func newConcurrentHandleMap[T any]() *concurrentHandleMap[T] {
	return &concurrentHandleMap[T]{
		handles: map[uint64]T{},
	}
}

func (cm *concurrentHandleMap[T]) insert(obj T) uint64 {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.currentHandle = cm.currentHandle + 1
	cm.handles[cm.currentHandle] = obj
	return cm.currentHandle
}

func (cm *concurrentHandleMap[T]) remove(handle uint64) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	delete(cm.handles, handle)
}

func (cm *concurrentHandleMap[T]) tryGet(handle uint64) (T, bool) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	val, ok := cm.handles[handle]
	return val, ok
}

//export slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackMethod0
func slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackMethod0(uniffiHandle C.uint64_t, record C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterFfiLogCallbackINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Log(
		FfiConverterFfiLogRecordINSTANCE.Lift(GoRustBuffer{
			inner: record,
		}),
	)

}

var UniffiVTableCallbackInterfaceFfiLogCallbackINSTANCE = C.UniffiVTableCallbackInterfaceFfiLogCallback{
	log: (C.UniffiCallbackInterfaceFfiLogCallbackMethod0)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackMethod0),

	uniffiFree: (C.UniffiCallbackInterfaceFree)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackFree),
}

//export slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackFree
func slatedb_ffi_cgo_dispatchCallbackInterfaceFfiLogCallbackFree(handle C.uint64_t) {
	FfiConverterFfiLogCallbackINSTANCE.handleMap.remove(uint64(handle))
}

func (c FfiConverterFfiLogCallback) register() {
	C.uniffi_slatedb_ffi_fn_init_callback_vtable_ffilogcallback(&UniffiVTableCallbackInterfaceFfiLogCallbackINSTANCE)
}

type FfiMergeOperator interface {
	Merge(key []byte, existingValue *[]byte, operand []byte) ([]byte, error)
}
type FfiMergeOperatorImpl struct {
	ffiObject FfiObject
}

func (_self *FfiMergeOperatorImpl) Merge(key []byte, existingValue *[]byte, operand []byte) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("FfiMergeOperator")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[FfiMergeOperatorCallbackError](FfiConverterFfiMergeOperatorCallbackError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_ffimergeoperator_merge(
				_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterOptionalBytesINSTANCE.Lower(existingValue), FfiConverterBytesINSTANCE.Lower(operand), _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue []byte
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterBytesINSTANCE.Lift(_uniffiRV), nil
	}
}
func (object *FfiMergeOperatorImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiMergeOperator struct {
	handleMap *concurrentHandleMap[FfiMergeOperator]
}

var FfiConverterFfiMergeOperatorINSTANCE = FfiConverterFfiMergeOperator{
	handleMap: newConcurrentHandleMap[FfiMergeOperator](),
}

func (c FfiConverterFfiMergeOperator) Lift(pointer unsafe.Pointer) FfiMergeOperator {
	result := &FfiMergeOperatorImpl{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffimergeoperator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffimergeoperator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiMergeOperatorImpl).Destroy)
	return result
}

func (c FfiConverterFfiMergeOperator) Read(reader io.Reader) FfiMergeOperator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiMergeOperator) Lower(value FfiMergeOperator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := unsafe.Pointer(uintptr(c.handleMap.insert(value)))
	return pointer

}

func (c FfiConverterFfiMergeOperator) Write(writer io.Writer, value FfiMergeOperator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiMergeOperator struct{}

func (_ FfiDestroyerFfiMergeOperator) Destroy(value FfiMergeOperator) {
	if val, ok := value.(*FfiMergeOperatorImpl); ok {
		val.Destroy()
	} else {
		panic("Expected *FfiMergeOperatorImpl")
	}
}

//export slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorMethod0
func slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorMethod0(uniffiHandle C.uint64_t, key C.RustBuffer, existingValue C.RustBuffer, operand C.RustBuffer, uniffiOutReturn *C.RustBuffer, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterFfiMergeOperatorINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	res, err :=
		uniffiObj.Merge(
			FfiConverterBytesINSTANCE.Lift(GoRustBuffer{
				inner: key,
			}),
			FfiConverterOptionalBytesINSTANCE.Lift(GoRustBuffer{
				inner: existingValue,
			}),
			FfiConverterBytesINSTANCE.Lift(GoRustBuffer{
				inner: operand,
			}),
		)

	if err != nil {
		var actualError *FfiMergeOperatorCallbackError
		if errors.As(err, &actualError) {
			*callStatus = C.RustCallStatus{
				code:     C.int8_t(uniffiCallbackResultError),
				errorBuf: FfiConverterFfiMergeOperatorCallbackErrorINSTANCE.Lower(actualError),
			}
		} else {
			*callStatus = C.RustCallStatus{
				code: C.int8_t(uniffiCallbackUnexpectedResultError),
			}
		}
		return
	}

	*uniffiOutReturn = FfiConverterBytesINSTANCE.Lower(res)
}

var UniffiVTableCallbackInterfaceFfiMergeOperatorINSTANCE = C.UniffiVTableCallbackInterfaceFfiMergeOperator{
	merge: (C.UniffiCallbackInterfaceFfiMergeOperatorMethod0)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorMethod0),

	uniffiFree: (C.UniffiCallbackInterfaceFree)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorFree),
}

//export slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorFree
func slatedb_ffi_cgo_dispatchCallbackInterfaceFfiMergeOperatorFree(handle C.uint64_t) {
	FfiConverterFfiMergeOperatorINSTANCE.handleMap.remove(uint64(handle))
}

func (c FfiConverterFfiMergeOperator) register() {
	C.uniffi_slatedb_ffi_fn_init_callback_vtable_ffimergeoperator(&UniffiVTableCallbackInterfaceFfiMergeOperatorINSTANCE)
}

type FfiObjectStoreInterface interface {
}
type FfiObjectStore struct {
	ffiObject FfiObject
}

func FfiObjectStoreFromEnv(envFile *string) (*FfiObjectStore, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffiobjectstore_from_env(FfiConverterOptionalStringINSTANCE.Lower(envFile), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiObjectStore
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiObjectStoreINSTANCE.Lift(_uniffiRV), nil
	}
}

func FfiObjectStoreResolve(url string) (*FfiObjectStore, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffiobjectstore_resolve(FfiConverterStringINSTANCE.Lower(url), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiObjectStore
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiObjectStoreINSTANCE.Lift(_uniffiRV), nil
	}
}

func (object *FfiObjectStore) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiObjectStore struct{}

var FfiConverterFfiObjectStoreINSTANCE = FfiConverterFfiObjectStore{}

func (c FfiConverterFfiObjectStore) Lift(pointer unsafe.Pointer) *FfiObjectStore {
	result := &FfiObjectStore{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffiobjectstore(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffiobjectstore(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiObjectStore).Destroy)
	return result
}

func (c FfiConverterFfiObjectStore) Read(reader io.Reader) *FfiObjectStore {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiObjectStore) Lower(value *FfiObjectStore) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiObjectStore")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiObjectStore) Write(writer io.Writer, value *FfiObjectStore) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiObjectStore struct{}

func (_ FfiDestroyerFfiObjectStore) Destroy(value *FfiObjectStore) {
	value.Destroy()
}

type FfiSettingsInterface interface {
	// Sets a settings field by dotted path using a JSON literal value.
	//
	// `key` identifies the field to update. Use `.` to address nested objects,
	// for example `compactor_options.max_sst_size` or
	// `object_store_cache_options.root_folder`.
	//
	// `value_json` must be a valid JSON literal matching the target field's
	// expected type. That means strings must be quoted JSON strings, numbers
	// should be passed as JSON numbers, booleans as `true`/`false`, and
	// optional fields can be cleared with `null`.
	//
	// Missing or `null` intermediate objects in the dotted path are created
	// automatically. If the update would produce an invalid `slatedb::Settings`
	// value, the method returns an error and leaves the current settings
	// unchanged.
	//
	// Examples:
	//
	// - `set("flush_interval", "\"250ms\"")`
	// - `set("default_ttl", "42")`
	// - `set("default_ttl", "null")`
	// - `set("compactor_options.max_sst_size", "33554432")`
	// - `set("object_store_cache_options.root_folder", "\"/tmp/slatedb-cache\"")`
	Set(key string, valueJson string) error
	ToJsonString() (string, error)
}
type FfiSettings struct {
	ffiObject FfiObject
}

func FfiSettingsDefault() *FfiSettings {
	return FfiConverterFfiSettingsINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_default(_uniffiStatus)
	}))
}

func FfiSettingsFromEnv(prefix string) (*FfiSettings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env(FfiConverterStringINSTANCE.Lower(prefix), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiSettings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

func FfiSettingsFromEnvWithDefault(prefix string, defaultSettings *FfiSettings) (*FfiSettings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env_with_default(FfiConverterStringINSTANCE.Lower(prefix), FfiConverterFfiSettingsINSTANCE.Lower(defaultSettings), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiSettings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

func FfiSettingsFromFile(path string) (*FfiSettings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_file(FfiConverterStringINSTANCE.Lower(path), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiSettings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

func FfiSettingsFromJsonString(json string) (*FfiSettings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_json_string(FfiConverterStringINSTANCE.Lower(json), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiSettings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

func FfiSettingsLoad() (*FfiSettings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffisettings_load(_uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *FfiSettings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterFfiSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

// Sets a settings field by dotted path using a JSON literal value.
//
// `key` identifies the field to update. Use `.` to address nested objects,
// for example `compactor_options.max_sst_size` or
// `object_store_cache_options.root_folder`.
//
// `value_json` must be a valid JSON literal matching the target field's
// expected type. That means strings must be quoted JSON strings, numbers
// should be passed as JSON numbers, booleans as `true`/`false`, and
// optional fields can be cleared with `null`.
//
// Missing or `null` intermediate objects in the dotted path are created
// automatically. If the update would produce an invalid `slatedb::Settings`
// value, the method returns an error and leaves the current settings
// unchanged.
//
// Examples:
//
// - `set("flush_interval", "\"250ms\"")`
// - `set("default_ttl", "42")`
// - `set("default_ttl", "null")`
// - `set("compactor_options.max_sst_size", "33554432")`
// - `set("object_store_cache_options.root_folder", "\"/tmp/slatedb-cache\"")`
func (_self *FfiSettings) Set(key string, valueJson string) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiSettings")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffisettings_set(
			_pointer, FfiConverterStringINSTANCE.Lower(key), FfiConverterStringINSTANCE.Lower(valueJson), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiSettings) ToJsonString() (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiSettings")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_ffisettings_to_json_string(
				_pointer, _uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}
func (object *FfiSettings) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiSettings struct{}

var FfiConverterFfiSettingsINSTANCE = FfiConverterFfiSettings{}

func (c FfiConverterFfiSettings) Lift(pointer unsafe.Pointer) *FfiSettings {
	result := &FfiSettings{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffisettings(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffisettings(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiSettings).Destroy)
	return result
}

func (c FfiConverterFfiSettings) Read(reader io.Reader) *FfiSettings {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiSettings) Lower(value *FfiSettings) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiSettings")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiSettings) Write(writer io.Writer, value *FfiSettings) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiSettings struct{}

func (_ FfiDestroyerFfiSettings) Destroy(value *FfiSettings) {
	value.Destroy()
}

type FfiWalFileInterface interface {
	Id() uint64
	Iterator() (*FfiWalFileIterator, error)
	Metadata() (FfiWalFileMetadata, error)
	NextFile() *FfiWalFile
	NextId() uint64
	Shutdown() error
}
type FfiWalFile struct {
	ffiObject FfiObject
}

func (_self *FfiWalFile) Id() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_ffiwalfile_id(
			_pointer, _uniffiStatus)
	}))
}

func (_self *FfiWalFile) Iterator() (*FfiWalFileIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *FfiWalFileIterator {
			return FfiConverterFfiWalFileIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffiwalfile_iterator(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiWalFile) Metadata() (FfiWalFileMetadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) FfiWalFileMetadata {
			return FfiConverterFfiWalFileMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffiwalfile_metadata(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiWalFile) NextFile() *FfiWalFile {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterFfiWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_method_ffiwalfile_next_file(
			_pointer, _uniffiStatus)
	}))
}

func (_self *FfiWalFile) NextId() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_ffiwalfile_next_id(
			_pointer, _uniffiStatus)
	}))
}

func (_self *FfiWalFile) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFile")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwalfile_shutdown(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiWalFile) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiWalFile struct{}

var FfiConverterFfiWalFileINSTANCE = FfiConverterFfiWalFile{}

func (c FfiConverterFfiWalFile) Lift(pointer unsafe.Pointer) *FfiWalFile {
	result := &FfiWalFile{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffiwalfile(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffiwalfile(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiWalFile).Destroy)
	return result
}

func (c FfiConverterFfiWalFile) Read(reader io.Reader) *FfiWalFile {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiWalFile) Lower(value *FfiWalFile) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiWalFile")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiWalFile) Write(writer io.Writer, value *FfiWalFile) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiWalFile struct{}

func (_ FfiDestroyerFfiWalFile) Destroy(value *FfiWalFile) {
	value.Destroy()
}

type FfiWalFileIteratorInterface interface {
	Next() (*FfiRowEntry, error)
	Shutdown() error
}
type FfiWalFileIterator struct {
	ffiObject FfiObject
}

func (_self *FfiWalFileIterator) Next() (*FfiRowEntry, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFileIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *FfiRowEntry {
			return FfiConverterOptionalFfiRowEntryINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffiwalfileiterator_next(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiWalFileIterator) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalFileIterator")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwalfileiterator_shutdown(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiWalFileIterator) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiWalFileIterator struct{}

var FfiConverterFfiWalFileIteratorINSTANCE = FfiConverterFfiWalFileIterator{}

func (c FfiConverterFfiWalFileIterator) Lift(pointer unsafe.Pointer) *FfiWalFileIterator {
	result := &FfiWalFileIterator{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffiwalfileiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffiwalfileiterator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiWalFileIterator).Destroy)
	return result
}

func (c FfiConverterFfiWalFileIterator) Read(reader io.Reader) *FfiWalFileIterator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiWalFileIterator) Lower(value *FfiWalFileIterator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiWalFileIterator")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiWalFileIterator) Write(writer io.Writer, value *FfiWalFileIterator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiWalFileIterator struct{}

func (_ FfiDestroyerFfiWalFileIterator) Destroy(value *FfiWalFileIterator) {
	value.Destroy()
}

type FfiWalReaderInterface interface {
	Get(id uint64) *FfiWalFile
	List(startId *uint64, endId *uint64) ([]*FfiWalFile, error)
	Shutdown() error
}
type FfiWalReader struct {
	ffiObject FfiObject
}

func NewFfiWalReader(path string, objectStore *FfiObjectStore) *FfiWalReader {
	return FfiConverterFfiWalReaderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffiwalreader_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterFfiObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

func (_self *FfiWalReader) Get(id uint64) *FfiWalFile {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalReader")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterFfiWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_method_ffiwalreader_get(
			_pointer, FfiConverterUint64INSTANCE.Lower(id), _uniffiStatus)
	}))
}

func (_self *FfiWalReader) List(startId *uint64, endId *uint64) ([]*FfiWalFile, error) {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[FfiError](
		FfiConverterFfiErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []*FfiWalFile {
			return FfiConverterSequenceFfiWalFileINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_ffiwalreader_list(
			_pointer, FfiConverterOptionalUint64INSTANCE.Lower(startId), FfiConverterOptionalUint64INSTANCE.Lower(endId)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_ffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

func (_self *FfiWalReader) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWalReader")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwalreader_shutdown(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiWalReader) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiWalReader struct{}

var FfiConverterFfiWalReaderINSTANCE = FfiConverterFfiWalReader{}

func (c FfiConverterFfiWalReader) Lift(pointer unsafe.Pointer) *FfiWalReader {
	result := &FfiWalReader{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffiwalreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffiwalreader(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiWalReader).Destroy)
	return result
}

func (c FfiConverterFfiWalReader) Read(reader io.Reader) *FfiWalReader {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiWalReader) Lower(value *FfiWalReader) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiWalReader")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiWalReader) Write(writer io.Writer, value *FfiWalReader) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiWalReader struct{}

func (_ FfiDestroyerFfiWalReader) Destroy(value *FfiWalReader) {
	value.Destroy()
}

type FfiWriteBatchInterface interface {
	Delete(key []byte) error
	Merge(key []byte, operand []byte) error
	MergeWithOptions(key []byte, operand []byte, options FfiMergeOptions) error
	Put(key []byte, value []byte) error
	PutWithOptions(key []byte, value []byte, options FfiPutOptions) error
}
type FfiWriteBatch struct {
	ffiObject FfiObject
}

func NewFfiWriteBatch() *FfiWriteBatch {
	return FfiConverterFfiWriteBatchINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_ffiwritebatch_new(_uniffiStatus)
	}))
}

func (_self *FfiWriteBatch) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwritebatch_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiWriteBatch) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwritebatch_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiWriteBatch) MergeWithOptions(key []byte, operand []byte, options FfiMergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwritebatch_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterFfiMergeOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiWriteBatch) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwritebatch_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *FfiWriteBatch) PutWithOptions(key []byte, value []byte, options FfiPutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*FfiWriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_ffiwritebatch_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterFfiPutOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *FfiWriteBatch) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterFfiWriteBatch struct{}

var FfiConverterFfiWriteBatchINSTANCE = FfiConverterFfiWriteBatch{}

func (c FfiConverterFfiWriteBatch) Lift(pointer unsafe.Pointer) *FfiWriteBatch {
	result := &FfiWriteBatch{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_ffiwritebatch(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_ffiwritebatch(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*FfiWriteBatch).Destroy)
	return result
}

func (c FfiConverterFfiWriteBatch) Read(reader io.Reader) *FfiWriteBatch {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterFfiWriteBatch) Lower(value *FfiWriteBatch) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*FfiWriteBatch")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterFfiWriteBatch) Write(writer io.Writer, value *FfiWriteBatch) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerFfiWriteBatch struct{}

func (_ FfiDestroyerFfiWriteBatch) Destroy(value *FfiWriteBatch) {
	value.Destroy()
}

type FfiFlushOptions struct {
	FlushType FfiFlushType
}

func (r *FfiFlushOptions) Destroy() {
	FfiDestroyerFfiFlushType{}.Destroy(r.FlushType)
}

type FfiConverterFfiFlushOptions struct{}

var FfiConverterFfiFlushOptionsINSTANCE = FfiConverterFfiFlushOptions{}

func (c FfiConverterFfiFlushOptions) Lift(rb RustBufferI) FfiFlushOptions {
	return LiftFromRustBuffer[FfiFlushOptions](c, rb)
}

func (c FfiConverterFfiFlushOptions) Read(reader io.Reader) FfiFlushOptions {
	return FfiFlushOptions{
		FfiConverterFfiFlushTypeINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiFlushOptions) Lower(value FfiFlushOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiFlushOptions](c, value)
}

func (c FfiConverterFfiFlushOptions) LowerExternal(value FfiFlushOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiFlushOptions](c, value))
}

func (c FfiConverterFfiFlushOptions) Write(writer io.Writer, value FfiFlushOptions) {
	FfiConverterFfiFlushTypeINSTANCE.Write(writer, value.FlushType)
}

type FfiDestroyerFfiFlushOptions struct{}

func (_ FfiDestroyerFfiFlushOptions) Destroy(value FfiFlushOptions) {
	value.Destroy()
}

type FfiKeyRange struct {
	Start          *[]byte
	StartInclusive bool
	End            *[]byte
	EndInclusive   bool
}

func (r *FfiKeyRange) Destroy() {
	FfiDestroyerOptionalBytes{}.Destroy(r.Start)
	FfiDestroyerBool{}.Destroy(r.StartInclusive)
	FfiDestroyerOptionalBytes{}.Destroy(r.End)
	FfiDestroyerBool{}.Destroy(r.EndInclusive)
}

type FfiConverterFfiKeyRange struct{}

var FfiConverterFfiKeyRangeINSTANCE = FfiConverterFfiKeyRange{}

func (c FfiConverterFfiKeyRange) Lift(rb RustBufferI) FfiKeyRange {
	return LiftFromRustBuffer[FfiKeyRange](c, rb)
}

func (c FfiConverterFfiKeyRange) Read(reader io.Reader) FfiKeyRange {
	return FfiKeyRange{
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiKeyRange) Lower(value FfiKeyRange) C.RustBuffer {
	return LowerIntoRustBuffer[FfiKeyRange](c, value)
}

func (c FfiConverterFfiKeyRange) LowerExternal(value FfiKeyRange) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiKeyRange](c, value))
}

func (c FfiConverterFfiKeyRange) Write(writer io.Writer, value FfiKeyRange) {
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.Start)
	FfiConverterBoolINSTANCE.Write(writer, value.StartInclusive)
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.End)
	FfiConverterBoolINSTANCE.Write(writer, value.EndInclusive)
}

type FfiDestroyerFfiKeyRange struct{}

func (_ FfiDestroyerFfiKeyRange) Destroy(value FfiKeyRange) {
	value.Destroy()
}

type FfiKeyValue struct {
	Key      []byte
	Value    []byte
	Seq      uint64
	CreateTs int64
	ExpireTs *int64
}

func (r *FfiKeyValue) Destroy() {
	FfiDestroyerBytes{}.Destroy(r.Key)
	FfiDestroyerBytes{}.Destroy(r.Value)
	FfiDestroyerUint64{}.Destroy(r.Seq)
	FfiDestroyerInt64{}.Destroy(r.CreateTs)
	FfiDestroyerOptionalInt64{}.Destroy(r.ExpireTs)
}

type FfiConverterFfiKeyValue struct{}

var FfiConverterFfiKeyValueINSTANCE = FfiConverterFfiKeyValue{}

func (c FfiConverterFfiKeyValue) Lift(rb RustBufferI) FfiKeyValue {
	return LiftFromRustBuffer[FfiKeyValue](c, rb)
}

func (c FfiConverterFfiKeyValue) Read(reader io.Reader) FfiKeyValue {
	return FfiKeyValue{
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterInt64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiKeyValue) Lower(value FfiKeyValue) C.RustBuffer {
	return LowerIntoRustBuffer[FfiKeyValue](c, value)
}

func (c FfiConverterFfiKeyValue) LowerExternal(value FfiKeyValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiKeyValue](c, value))
}

func (c FfiConverterFfiKeyValue) Write(writer io.Writer, value FfiKeyValue) {
	FfiConverterBytesINSTANCE.Write(writer, value.Key)
	FfiConverterBytesINSTANCE.Write(writer, value.Value)
	FfiConverterUint64INSTANCE.Write(writer, value.Seq)
	FfiConverterInt64INSTANCE.Write(writer, value.CreateTs)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.ExpireTs)
}

type FfiDestroyerFfiKeyValue struct{}

func (_ FfiDestroyerFfiKeyValue) Destroy(value FfiKeyValue) {
	value.Destroy()
}

type FfiLogRecord struct {
	Level      FfiLogLevel
	Target     string
	Message    string
	ModulePath *string
	File       *string
	Line       *uint32
}

func (r *FfiLogRecord) Destroy() {
	FfiDestroyerFfiLogLevel{}.Destroy(r.Level)
	FfiDestroyerString{}.Destroy(r.Target)
	FfiDestroyerString{}.Destroy(r.Message)
	FfiDestroyerOptionalString{}.Destroy(r.ModulePath)
	FfiDestroyerOptionalString{}.Destroy(r.File)
	FfiDestroyerOptionalUint32{}.Destroy(r.Line)
}

type FfiConverterFfiLogRecord struct{}

var FfiConverterFfiLogRecordINSTANCE = FfiConverterFfiLogRecord{}

func (c FfiConverterFfiLogRecord) Lift(rb RustBufferI) FfiLogRecord {
	return LiftFromRustBuffer[FfiLogRecord](c, rb)
}

func (c FfiConverterFfiLogRecord) Read(reader io.Reader) FfiLogRecord {
	return FfiLogRecord{
		FfiConverterFfiLogLevelINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiLogRecord) Lower(value FfiLogRecord) C.RustBuffer {
	return LowerIntoRustBuffer[FfiLogRecord](c, value)
}

func (c FfiConverterFfiLogRecord) LowerExternal(value FfiLogRecord) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiLogRecord](c, value))
}

func (c FfiConverterFfiLogRecord) Write(writer io.Writer, value FfiLogRecord) {
	FfiConverterFfiLogLevelINSTANCE.Write(writer, value.Level)
	FfiConverterStringINSTANCE.Write(writer, value.Target)
	FfiConverterStringINSTANCE.Write(writer, value.Message)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.ModulePath)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.File)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Line)
}

type FfiDestroyerFfiLogRecord struct{}

func (_ FfiDestroyerFfiLogRecord) Destroy(value FfiLogRecord) {
	value.Destroy()
}

type FfiMergeOptions struct {
	Ttl FfiTtl
}

func (r *FfiMergeOptions) Destroy() {
	FfiDestroyerFfiTtl{}.Destroy(r.Ttl)
}

type FfiConverterFfiMergeOptions struct{}

var FfiConverterFfiMergeOptionsINSTANCE = FfiConverterFfiMergeOptions{}

func (c FfiConverterFfiMergeOptions) Lift(rb RustBufferI) FfiMergeOptions {
	return LiftFromRustBuffer[FfiMergeOptions](c, rb)
}

func (c FfiConverterFfiMergeOptions) Read(reader io.Reader) FfiMergeOptions {
	return FfiMergeOptions{
		FfiConverterFfiTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiMergeOptions) Lower(value FfiMergeOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiMergeOptions](c, value)
}

func (c FfiConverterFfiMergeOptions) LowerExternal(value FfiMergeOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiMergeOptions](c, value))
}

func (c FfiConverterFfiMergeOptions) Write(writer io.Writer, value FfiMergeOptions) {
	FfiConverterFfiTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerFfiMergeOptions struct{}

func (_ FfiDestroyerFfiMergeOptions) Destroy(value FfiMergeOptions) {
	value.Destroy()
}

type FfiPutOptions struct {
	Ttl FfiTtl
}

func (r *FfiPutOptions) Destroy() {
	FfiDestroyerFfiTtl{}.Destroy(r.Ttl)
}

type FfiConverterFfiPutOptions struct{}

var FfiConverterFfiPutOptionsINSTANCE = FfiConverterFfiPutOptions{}

func (c FfiConverterFfiPutOptions) Lift(rb RustBufferI) FfiPutOptions {
	return LiftFromRustBuffer[FfiPutOptions](c, rb)
}

func (c FfiConverterFfiPutOptions) Read(reader io.Reader) FfiPutOptions {
	return FfiPutOptions{
		FfiConverterFfiTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiPutOptions) Lower(value FfiPutOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiPutOptions](c, value)
}

func (c FfiConverterFfiPutOptions) LowerExternal(value FfiPutOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiPutOptions](c, value))
}

func (c FfiConverterFfiPutOptions) Write(writer io.Writer, value FfiPutOptions) {
	FfiConverterFfiTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerFfiPutOptions struct{}

func (_ FfiDestroyerFfiPutOptions) Destroy(value FfiPutOptions) {
	value.Destroy()
}

type FfiReadOptions struct {
	DurabilityFilter FfiDurabilityLevel
	Dirty            bool
	CacheBlocks      bool
}

func (r *FfiReadOptions) Destroy() {
	FfiDestroyerFfiDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
}

type FfiConverterFfiReadOptions struct{}

var FfiConverterFfiReadOptionsINSTANCE = FfiConverterFfiReadOptions{}

func (c FfiConverterFfiReadOptions) Lift(rb RustBufferI) FfiReadOptions {
	return LiftFromRustBuffer[FfiReadOptions](c, rb)
}

func (c FfiConverterFfiReadOptions) Read(reader io.Reader) FfiReadOptions {
	return FfiReadOptions{
		FfiConverterFfiDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiReadOptions) Lower(value FfiReadOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiReadOptions](c, value)
}

func (c FfiConverterFfiReadOptions) LowerExternal(value FfiReadOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiReadOptions](c, value))
}

func (c FfiConverterFfiReadOptions) Write(writer io.Writer, value FfiReadOptions) {
	FfiConverterFfiDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
}

type FfiDestroyerFfiReadOptions struct{}

func (_ FfiDestroyerFfiReadOptions) Destroy(value FfiReadOptions) {
	value.Destroy()
}

type FfiReaderOptions struct {
	ManifestPollIntervalMs uint64
	CheckpointLifetimeMs   uint64
	MaxMemtableBytes       uint64
	SkipWalReplay          bool
}

func (r *FfiReaderOptions) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.ManifestPollIntervalMs)
	FfiDestroyerUint64{}.Destroy(r.CheckpointLifetimeMs)
	FfiDestroyerUint64{}.Destroy(r.MaxMemtableBytes)
	FfiDestroyerBool{}.Destroy(r.SkipWalReplay)
}

type FfiConverterFfiReaderOptions struct{}

var FfiConverterFfiReaderOptionsINSTANCE = FfiConverterFfiReaderOptions{}

func (c FfiConverterFfiReaderOptions) Lift(rb RustBufferI) FfiReaderOptions {
	return LiftFromRustBuffer[FfiReaderOptions](c, rb)
}

func (c FfiConverterFfiReaderOptions) Read(reader io.Reader) FfiReaderOptions {
	return FfiReaderOptions{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiReaderOptions) Lower(value FfiReaderOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiReaderOptions](c, value)
}

func (c FfiConverterFfiReaderOptions) LowerExternal(value FfiReaderOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiReaderOptions](c, value))
}

func (c FfiConverterFfiReaderOptions) Write(writer io.Writer, value FfiReaderOptions) {
	FfiConverterUint64INSTANCE.Write(writer, value.ManifestPollIntervalMs)
	FfiConverterUint64INSTANCE.Write(writer, value.CheckpointLifetimeMs)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxMemtableBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.SkipWalReplay)
}

type FfiDestroyerFfiReaderOptions struct{}

func (_ FfiDestroyerFfiReaderOptions) Destroy(value FfiReaderOptions) {
	value.Destroy()
}

type FfiRowEntry struct {
	Kind     FfiRowEntryKind
	Key      []byte
	Value    *[]byte
	Seq      uint64
	CreateTs *int64
	ExpireTs *int64
}

func (r *FfiRowEntry) Destroy() {
	FfiDestroyerFfiRowEntryKind{}.Destroy(r.Kind)
	FfiDestroyerBytes{}.Destroy(r.Key)
	FfiDestroyerOptionalBytes{}.Destroy(r.Value)
	FfiDestroyerUint64{}.Destroy(r.Seq)
	FfiDestroyerOptionalInt64{}.Destroy(r.CreateTs)
	FfiDestroyerOptionalInt64{}.Destroy(r.ExpireTs)
}

type FfiConverterFfiRowEntry struct{}

var FfiConverterFfiRowEntryINSTANCE = FfiConverterFfiRowEntry{}

func (c FfiConverterFfiRowEntry) Lift(rb RustBufferI) FfiRowEntry {
	return LiftFromRustBuffer[FfiRowEntry](c, rb)
}

func (c FfiConverterFfiRowEntry) Read(reader io.Reader) FfiRowEntry {
	return FfiRowEntry{
		FfiConverterFfiRowEntryKindINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiRowEntry) Lower(value FfiRowEntry) C.RustBuffer {
	return LowerIntoRustBuffer[FfiRowEntry](c, value)
}

func (c FfiConverterFfiRowEntry) LowerExternal(value FfiRowEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiRowEntry](c, value))
}

func (c FfiConverterFfiRowEntry) Write(writer io.Writer, value FfiRowEntry) {
	FfiConverterFfiRowEntryKindINSTANCE.Write(writer, value.Kind)
	FfiConverterBytesINSTANCE.Write(writer, value.Key)
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.Value)
	FfiConverterUint64INSTANCE.Write(writer, value.Seq)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.CreateTs)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.ExpireTs)
}

type FfiDestroyerFfiRowEntry struct{}

func (_ FfiDestroyerFfiRowEntry) Destroy(value FfiRowEntry) {
	value.Destroy()
}

type FfiScanOptions struct {
	DurabilityFilter FfiDurabilityLevel
	Dirty            bool
	ReadAheadBytes   uint64
	CacheBlocks      bool
	MaxFetchTasks    uint64
}

func (r *FfiScanOptions) Destroy() {
	FfiDestroyerFfiDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerUint64{}.Destroy(r.ReadAheadBytes)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
	FfiDestroyerUint64{}.Destroy(r.MaxFetchTasks)
}

type FfiConverterFfiScanOptions struct{}

var FfiConverterFfiScanOptionsINSTANCE = FfiConverterFfiScanOptions{}

func (c FfiConverterFfiScanOptions) Lift(rb RustBufferI) FfiScanOptions {
	return LiftFromRustBuffer[FfiScanOptions](c, rb)
}

func (c FfiConverterFfiScanOptions) Read(reader io.Reader) FfiScanOptions {
	return FfiScanOptions{
		FfiConverterFfiDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiScanOptions) Lower(value FfiScanOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiScanOptions](c, value)
}

func (c FfiConverterFfiScanOptions) LowerExternal(value FfiScanOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiScanOptions](c, value))
}

func (c FfiConverterFfiScanOptions) Write(writer io.Writer, value FfiScanOptions) {
	FfiConverterFfiDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterUint64INSTANCE.Write(writer, value.ReadAheadBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxFetchTasks)
}

type FfiDestroyerFfiScanOptions struct{}

func (_ FfiDestroyerFfiScanOptions) Destroy(value FfiScanOptions) {
	value.Destroy()
}

type FfiWalFileMetadata struct {
	LastModifiedSeconds int64
	LastModifiedNanos   uint32
	SizeBytes           uint64
	Location            string
}

func (r *FfiWalFileMetadata) Destroy() {
	FfiDestroyerInt64{}.Destroy(r.LastModifiedSeconds)
	FfiDestroyerUint32{}.Destroy(r.LastModifiedNanos)
	FfiDestroyerUint64{}.Destroy(r.SizeBytes)
	FfiDestroyerString{}.Destroy(r.Location)
}

type FfiConverterFfiWalFileMetadata struct{}

var FfiConverterFfiWalFileMetadataINSTANCE = FfiConverterFfiWalFileMetadata{}

func (c FfiConverterFfiWalFileMetadata) Lift(rb RustBufferI) FfiWalFileMetadata {
	return LiftFromRustBuffer[FfiWalFileMetadata](c, rb)
}

func (c FfiConverterFfiWalFileMetadata) Read(reader io.Reader) FfiWalFileMetadata {
	return FfiWalFileMetadata{
		FfiConverterInt64INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiWalFileMetadata) Lower(value FfiWalFileMetadata) C.RustBuffer {
	return LowerIntoRustBuffer[FfiWalFileMetadata](c, value)
}

func (c FfiConverterFfiWalFileMetadata) LowerExternal(value FfiWalFileMetadata) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiWalFileMetadata](c, value))
}

func (c FfiConverterFfiWalFileMetadata) Write(writer io.Writer, value FfiWalFileMetadata) {
	FfiConverterInt64INSTANCE.Write(writer, value.LastModifiedSeconds)
	FfiConverterUint32INSTANCE.Write(writer, value.LastModifiedNanos)
	FfiConverterUint64INSTANCE.Write(writer, value.SizeBytes)
	FfiConverterStringINSTANCE.Write(writer, value.Location)
}

type FfiDestroyerFfiWalFileMetadata struct{}

func (_ FfiDestroyerFfiWalFileMetadata) Destroy(value FfiWalFileMetadata) {
	value.Destroy()
}

type FfiWriteHandle struct {
	Seqnum   uint64
	CreateTs int64
}

func (r *FfiWriteHandle) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.Seqnum)
	FfiDestroyerInt64{}.Destroy(r.CreateTs)
}

type FfiConverterFfiWriteHandle struct{}

var FfiConverterFfiWriteHandleINSTANCE = FfiConverterFfiWriteHandle{}

func (c FfiConverterFfiWriteHandle) Lift(rb RustBufferI) FfiWriteHandle {
	return LiftFromRustBuffer[FfiWriteHandle](c, rb)
}

func (c FfiConverterFfiWriteHandle) Read(reader io.Reader) FfiWriteHandle {
	return FfiWriteHandle{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiWriteHandle) Lower(value FfiWriteHandle) C.RustBuffer {
	return LowerIntoRustBuffer[FfiWriteHandle](c, value)
}

func (c FfiConverterFfiWriteHandle) LowerExternal(value FfiWriteHandle) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiWriteHandle](c, value))
}

func (c FfiConverterFfiWriteHandle) Write(writer io.Writer, value FfiWriteHandle) {
	FfiConverterUint64INSTANCE.Write(writer, value.Seqnum)
	FfiConverterInt64INSTANCE.Write(writer, value.CreateTs)
}

type FfiDestroyerFfiWriteHandle struct{}

func (_ FfiDestroyerFfiWriteHandle) Destroy(value FfiWriteHandle) {
	value.Destroy()
}

type FfiWriteOptions struct {
	AwaitDurable bool
}

func (r *FfiWriteOptions) Destroy() {
	FfiDestroyerBool{}.Destroy(r.AwaitDurable)
}

type FfiConverterFfiWriteOptions struct{}

var FfiConverterFfiWriteOptionsINSTANCE = FfiConverterFfiWriteOptions{}

func (c FfiConverterFfiWriteOptions) Lift(rb RustBufferI) FfiWriteOptions {
	return LiftFromRustBuffer[FfiWriteOptions](c, rb)
}

func (c FfiConverterFfiWriteOptions) Read(reader io.Reader) FfiWriteOptions {
	return FfiWriteOptions{
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterFfiWriteOptions) Lower(value FfiWriteOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FfiWriteOptions](c, value)
}

func (c FfiConverterFfiWriteOptions) LowerExternal(value FfiWriteOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiWriteOptions](c, value))
}

func (c FfiConverterFfiWriteOptions) Write(writer io.Writer, value FfiWriteOptions) {
	FfiConverterBoolINSTANCE.Write(writer, value.AwaitDurable)
}

type FfiDestroyerFfiWriteOptions struct{}

func (_ FfiDestroyerFfiWriteOptions) Destroy(value FfiWriteOptions) {
	value.Destroy()
}

type FfiCloseReason uint

const (
	FfiCloseReasonNone    FfiCloseReason = 1
	FfiCloseReasonClean   FfiCloseReason = 2
	FfiCloseReasonFenced  FfiCloseReason = 3
	FfiCloseReasonPanic   FfiCloseReason = 4
	FfiCloseReasonUnknown FfiCloseReason = 5
)

type FfiConverterFfiCloseReason struct{}

var FfiConverterFfiCloseReasonINSTANCE = FfiConverterFfiCloseReason{}

func (c FfiConverterFfiCloseReason) Lift(rb RustBufferI) FfiCloseReason {
	return LiftFromRustBuffer[FfiCloseReason](c, rb)
}

func (c FfiConverterFfiCloseReason) Lower(value FfiCloseReason) C.RustBuffer {
	return LowerIntoRustBuffer[FfiCloseReason](c, value)
}

func (c FfiConverterFfiCloseReason) LowerExternal(value FfiCloseReason) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiCloseReason](c, value))
}
func (FfiConverterFfiCloseReason) Read(reader io.Reader) FfiCloseReason {
	id := readInt32(reader)
	return FfiCloseReason(id)
}

func (FfiConverterFfiCloseReason) Write(writer io.Writer, value FfiCloseReason) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiCloseReason struct{}

func (_ FfiDestroyerFfiCloseReason) Destroy(value FfiCloseReason) {
}

type FfiDurabilityLevel uint

const (
	FfiDurabilityLevelRemote FfiDurabilityLevel = 1
	FfiDurabilityLevelMemory FfiDurabilityLevel = 2
)

type FfiConverterFfiDurabilityLevel struct{}

var FfiConverterFfiDurabilityLevelINSTANCE = FfiConverterFfiDurabilityLevel{}

func (c FfiConverterFfiDurabilityLevel) Lift(rb RustBufferI) FfiDurabilityLevel {
	return LiftFromRustBuffer[FfiDurabilityLevel](c, rb)
}

func (c FfiConverterFfiDurabilityLevel) Lower(value FfiDurabilityLevel) C.RustBuffer {
	return LowerIntoRustBuffer[FfiDurabilityLevel](c, value)
}

func (c FfiConverterFfiDurabilityLevel) LowerExternal(value FfiDurabilityLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiDurabilityLevel](c, value))
}
func (FfiConverterFfiDurabilityLevel) Read(reader io.Reader) FfiDurabilityLevel {
	id := readInt32(reader)
	return FfiDurabilityLevel(id)
}

func (FfiConverterFfiDurabilityLevel) Write(writer io.Writer, value FfiDurabilityLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiDurabilityLevel struct{}

func (_ FfiDestroyerFfiDurabilityLevel) Destroy(value FfiDurabilityLevel) {
}

type FfiError struct {
	err error
}

// Convience method to turn *FfiError into error
// Avoiding treating nil pointer as non nil error interface
func (err *FfiError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err FfiError) Error() string {
	return fmt.Sprintf("FfiError: %s", err.err.Error())
}

func (err FfiError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrFfiErrorTransaction = fmt.Errorf("FfiErrorTransaction")
var ErrFfiErrorClosed = fmt.Errorf("FfiErrorClosed")
var ErrFfiErrorUnavailable = fmt.Errorf("FfiErrorUnavailable")
var ErrFfiErrorInvalid = fmt.Errorf("FfiErrorInvalid")
var ErrFfiErrorData = fmt.Errorf("FfiErrorData")
var ErrFfiErrorInternal = fmt.Errorf("FfiErrorInternal")

// Variant structs
type FfiErrorTransaction struct {
	Message string
}

func NewFfiErrorTransaction(
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorTransaction{
		Message: message}}
}

func (e FfiErrorTransaction) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorTransaction) Error() string {
	return fmt.Sprint("Transaction",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiErrorTransaction) Is(target error) bool {
	return target == ErrFfiErrorTransaction
}

type FfiErrorClosed struct {
	Reason  FfiCloseReason
	Message string
}

func NewFfiErrorClosed(
	reason FfiCloseReason,
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorClosed{
		Reason:  reason,
		Message: message}}
}

func (e FfiErrorClosed) destroy() {
	FfiDestroyerFfiCloseReason{}.Destroy(e.Reason)
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorClosed) Error() string {
	return fmt.Sprint("Closed",
		": ",

		"Reason=",
		err.Reason,
		", ",
		"Message=",
		err.Message,
	)
}

func (self FfiErrorClosed) Is(target error) bool {
	return target == ErrFfiErrorClosed
}

type FfiErrorUnavailable struct {
	Message string
}

func NewFfiErrorUnavailable(
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorUnavailable{
		Message: message}}
}

func (e FfiErrorUnavailable) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorUnavailable) Error() string {
	return fmt.Sprint("Unavailable",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiErrorUnavailable) Is(target error) bool {
	return target == ErrFfiErrorUnavailable
}

type FfiErrorInvalid struct {
	Message string
}

func NewFfiErrorInvalid(
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorInvalid{
		Message: message}}
}

func (e FfiErrorInvalid) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorInvalid) Error() string {
	return fmt.Sprint("Invalid",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiErrorInvalid) Is(target error) bool {
	return target == ErrFfiErrorInvalid
}

type FfiErrorData struct {
	Message string
}

func NewFfiErrorData(
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorData{
		Message: message}}
}

func (e FfiErrorData) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorData) Error() string {
	return fmt.Sprint("Data",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiErrorData) Is(target error) bool {
	return target == ErrFfiErrorData
}

type FfiErrorInternal struct {
	Message string
}

func NewFfiErrorInternal(
	message string,
) *FfiError {
	return &FfiError{err: &FfiErrorInternal{
		Message: message}}
}

func (e FfiErrorInternal) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiErrorInternal) Error() string {
	return fmt.Sprint("Internal",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiErrorInternal) Is(target error) bool {
	return target == ErrFfiErrorInternal
}

type FfiConverterFfiError struct{}

var FfiConverterFfiErrorINSTANCE = FfiConverterFfiError{}

func (c FfiConverterFfiError) Lift(eb RustBufferI) *FfiError {
	return LiftFromRustBuffer[*FfiError](c, eb)
}

func (c FfiConverterFfiError) Lower(value *FfiError) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiError](c, value)
}

func (c FfiConverterFfiError) LowerExternal(value *FfiError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiError](c, value))
}

func (c FfiConverterFfiError) Read(reader io.Reader) *FfiError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &FfiError{&FfiErrorTransaction{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &FfiError{&FfiErrorClosed{
			Reason:  FfiConverterFfiCloseReasonINSTANCE.Read(reader),
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &FfiError{&FfiErrorUnavailable{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 4:
		return &FfiError{&FfiErrorInvalid{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 5:
		return &FfiError{&FfiErrorData{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 6:
		return &FfiError{&FfiErrorInternal{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterFfiError.Read()", errorID))
	}
}

func (c FfiConverterFfiError) Write(writer io.Writer, value *FfiError) {
	switch variantValue := value.err.(type) {
	case *FfiErrorTransaction:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *FfiErrorClosed:
		writeInt32(writer, 2)
		FfiConverterFfiCloseReasonINSTANCE.Write(writer, variantValue.Reason)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *FfiErrorUnavailable:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *FfiErrorInvalid:
		writeInt32(writer, 4)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *FfiErrorData:
		writeInt32(writer, 5)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *FfiErrorInternal:
		writeInt32(writer, 6)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterFfiError.Write", value))
	}
}

type FfiDestroyerFfiError struct{}

func (_ FfiDestroyerFfiError) Destroy(value *FfiError) {
	switch variantValue := value.err.(type) {
	case FfiErrorTransaction:
		variantValue.destroy()
	case FfiErrorClosed:
		variantValue.destroy()
	case FfiErrorUnavailable:
		variantValue.destroy()
	case FfiErrorInvalid:
		variantValue.destroy()
	case FfiErrorData:
		variantValue.destroy()
	case FfiErrorInternal:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerFfiError.Destroy", value))
	}
}

type FfiFlushType uint

const (
	FfiFlushTypeMemTable FfiFlushType = 1
	FfiFlushTypeWal      FfiFlushType = 2
)

type FfiConverterFfiFlushType struct{}

var FfiConverterFfiFlushTypeINSTANCE = FfiConverterFfiFlushType{}

func (c FfiConverterFfiFlushType) Lift(rb RustBufferI) FfiFlushType {
	return LiftFromRustBuffer[FfiFlushType](c, rb)
}

func (c FfiConverterFfiFlushType) Lower(value FfiFlushType) C.RustBuffer {
	return LowerIntoRustBuffer[FfiFlushType](c, value)
}

func (c FfiConverterFfiFlushType) LowerExternal(value FfiFlushType) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiFlushType](c, value))
}
func (FfiConverterFfiFlushType) Read(reader io.Reader) FfiFlushType {
	id := readInt32(reader)
	return FfiFlushType(id)
}

func (FfiConverterFfiFlushType) Write(writer io.Writer, value FfiFlushType) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiFlushType struct{}

func (_ FfiDestroyerFfiFlushType) Destroy(value FfiFlushType) {
}

type FfiIsolationLevel uint

const (
	FfiIsolationLevelSnapshot             FfiIsolationLevel = 1
	FfiIsolationLevelSerializableSnapshot FfiIsolationLevel = 2
)

type FfiConverterFfiIsolationLevel struct{}

var FfiConverterFfiIsolationLevelINSTANCE = FfiConverterFfiIsolationLevel{}

func (c FfiConverterFfiIsolationLevel) Lift(rb RustBufferI) FfiIsolationLevel {
	return LiftFromRustBuffer[FfiIsolationLevel](c, rb)
}

func (c FfiConverterFfiIsolationLevel) Lower(value FfiIsolationLevel) C.RustBuffer {
	return LowerIntoRustBuffer[FfiIsolationLevel](c, value)
}

func (c FfiConverterFfiIsolationLevel) LowerExternal(value FfiIsolationLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiIsolationLevel](c, value))
}
func (FfiConverterFfiIsolationLevel) Read(reader io.Reader) FfiIsolationLevel {
	id := readInt32(reader)
	return FfiIsolationLevel(id)
}

func (FfiConverterFfiIsolationLevel) Write(writer io.Writer, value FfiIsolationLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiIsolationLevel struct{}

func (_ FfiDestroyerFfiIsolationLevel) Destroy(value FfiIsolationLevel) {
}

type FfiLogLevel uint

const (
	FfiLogLevelOff   FfiLogLevel = 1
	FfiLogLevelError FfiLogLevel = 2
	FfiLogLevelWarn  FfiLogLevel = 3
	FfiLogLevelInfo  FfiLogLevel = 4
	FfiLogLevelDebug FfiLogLevel = 5
	FfiLogLevelTrace FfiLogLevel = 6
)

type FfiConverterFfiLogLevel struct{}

var FfiConverterFfiLogLevelINSTANCE = FfiConverterFfiLogLevel{}

func (c FfiConverterFfiLogLevel) Lift(rb RustBufferI) FfiLogLevel {
	return LiftFromRustBuffer[FfiLogLevel](c, rb)
}

func (c FfiConverterFfiLogLevel) Lower(value FfiLogLevel) C.RustBuffer {
	return LowerIntoRustBuffer[FfiLogLevel](c, value)
}

func (c FfiConverterFfiLogLevel) LowerExternal(value FfiLogLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiLogLevel](c, value))
}
func (FfiConverterFfiLogLevel) Read(reader io.Reader) FfiLogLevel {
	id := readInt32(reader)
	return FfiLogLevel(id)
}

func (FfiConverterFfiLogLevel) Write(writer io.Writer, value FfiLogLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiLogLevel struct{}

func (_ FfiDestroyerFfiLogLevel) Destroy(value FfiLogLevel) {
}

type FfiMergeOperatorCallbackError struct {
	err error
}

// Convience method to turn *FfiMergeOperatorCallbackError into error
// Avoiding treating nil pointer as non nil error interface
func (err *FfiMergeOperatorCallbackError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err FfiMergeOperatorCallbackError) Error() string {
	return fmt.Sprintf("FfiMergeOperatorCallbackError: %s", err.err.Error())
}

func (err FfiMergeOperatorCallbackError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrFfiMergeOperatorCallbackErrorFailed = fmt.Errorf("FfiMergeOperatorCallbackErrorFailed")

// Variant structs
type FfiMergeOperatorCallbackErrorFailed struct {
	Message string
}

func NewFfiMergeOperatorCallbackErrorFailed(
	message string,
) *FfiMergeOperatorCallbackError {
	return &FfiMergeOperatorCallbackError{err: &FfiMergeOperatorCallbackErrorFailed{
		Message: message}}
}

func (e FfiMergeOperatorCallbackErrorFailed) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err FfiMergeOperatorCallbackErrorFailed) Error() string {
	return fmt.Sprint("Failed",
		": ",

		"Message=",
		err.Message,
	)
}

func (self FfiMergeOperatorCallbackErrorFailed) Is(target error) bool {
	return target == ErrFfiMergeOperatorCallbackErrorFailed
}

type FfiConverterFfiMergeOperatorCallbackError struct{}

var FfiConverterFfiMergeOperatorCallbackErrorINSTANCE = FfiConverterFfiMergeOperatorCallbackError{}

func (c FfiConverterFfiMergeOperatorCallbackError) Lift(eb RustBufferI) *FfiMergeOperatorCallbackError {
	return LiftFromRustBuffer[*FfiMergeOperatorCallbackError](c, eb)
}

func (c FfiConverterFfiMergeOperatorCallbackError) Lower(value *FfiMergeOperatorCallbackError) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiMergeOperatorCallbackError](c, value)
}

func (c FfiConverterFfiMergeOperatorCallbackError) LowerExternal(value *FfiMergeOperatorCallbackError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiMergeOperatorCallbackError](c, value))
}

func (c FfiConverterFfiMergeOperatorCallbackError) Read(reader io.Reader) *FfiMergeOperatorCallbackError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &FfiMergeOperatorCallbackError{&FfiMergeOperatorCallbackErrorFailed{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterFfiMergeOperatorCallbackError.Read()", errorID))
	}
}

func (c FfiConverterFfiMergeOperatorCallbackError) Write(writer io.Writer, value *FfiMergeOperatorCallbackError) {
	switch variantValue := value.err.(type) {
	case *FfiMergeOperatorCallbackErrorFailed:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterFfiMergeOperatorCallbackError.Write", value))
	}
}

type FfiDestroyerFfiMergeOperatorCallbackError struct{}

func (_ FfiDestroyerFfiMergeOperatorCallbackError) Destroy(value *FfiMergeOperatorCallbackError) {
	switch variantValue := value.err.(type) {
	case FfiMergeOperatorCallbackErrorFailed:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerFfiMergeOperatorCallbackError.Destroy", value))
	}
}

type FfiRowEntryKind uint

const (
	FfiRowEntryKindValue     FfiRowEntryKind = 1
	FfiRowEntryKindTombstone FfiRowEntryKind = 2
	FfiRowEntryKindMerge     FfiRowEntryKind = 3
)

type FfiConverterFfiRowEntryKind struct{}

var FfiConverterFfiRowEntryKindINSTANCE = FfiConverterFfiRowEntryKind{}

func (c FfiConverterFfiRowEntryKind) Lift(rb RustBufferI) FfiRowEntryKind {
	return LiftFromRustBuffer[FfiRowEntryKind](c, rb)
}

func (c FfiConverterFfiRowEntryKind) Lower(value FfiRowEntryKind) C.RustBuffer {
	return LowerIntoRustBuffer[FfiRowEntryKind](c, value)
}

func (c FfiConverterFfiRowEntryKind) LowerExternal(value FfiRowEntryKind) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiRowEntryKind](c, value))
}
func (FfiConverterFfiRowEntryKind) Read(reader io.Reader) FfiRowEntryKind {
	id := readInt32(reader)
	return FfiRowEntryKind(id)
}

func (FfiConverterFfiRowEntryKind) Write(writer io.Writer, value FfiRowEntryKind) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiRowEntryKind struct{}

func (_ FfiDestroyerFfiRowEntryKind) Destroy(value FfiRowEntryKind) {
}

type FfiSstBlockSize uint

const (
	FfiSstBlockSizeBlock1Kib  FfiSstBlockSize = 1
	FfiSstBlockSizeBlock2Kib  FfiSstBlockSize = 2
	FfiSstBlockSizeBlock4Kib  FfiSstBlockSize = 3
	FfiSstBlockSizeBlock8Kib  FfiSstBlockSize = 4
	FfiSstBlockSizeBlock16Kib FfiSstBlockSize = 5
	FfiSstBlockSizeBlock32Kib FfiSstBlockSize = 6
	FfiSstBlockSizeBlock64Kib FfiSstBlockSize = 7
)

type FfiConverterFfiSstBlockSize struct{}

var FfiConverterFfiSstBlockSizeINSTANCE = FfiConverterFfiSstBlockSize{}

func (c FfiConverterFfiSstBlockSize) Lift(rb RustBufferI) FfiSstBlockSize {
	return LiftFromRustBuffer[FfiSstBlockSize](c, rb)
}

func (c FfiConverterFfiSstBlockSize) Lower(value FfiSstBlockSize) C.RustBuffer {
	return LowerIntoRustBuffer[FfiSstBlockSize](c, value)
}

func (c FfiConverterFfiSstBlockSize) LowerExternal(value FfiSstBlockSize) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiSstBlockSize](c, value))
}
func (FfiConverterFfiSstBlockSize) Read(reader io.Reader) FfiSstBlockSize {
	id := readInt32(reader)
	return FfiSstBlockSize(id)
}

func (FfiConverterFfiSstBlockSize) Write(writer io.Writer, value FfiSstBlockSize) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFfiSstBlockSize struct{}

func (_ FfiDestroyerFfiSstBlockSize) Destroy(value FfiSstBlockSize) {
}

type FfiTtl interface {
	Destroy()
}
type FfiTtlDefault struct {
}

func (e FfiTtlDefault) Destroy() {
}

type FfiTtlNoExpiry struct {
}

func (e FfiTtlNoExpiry) Destroy() {
}

type FfiTtlExpireAfterTicks struct {
	Field0 uint64
}

func (e FfiTtlExpireAfterTicks) Destroy() {
	FfiDestroyerUint64{}.Destroy(e.Field0)
}

type FfiConverterFfiTtl struct{}

var FfiConverterFfiTtlINSTANCE = FfiConverterFfiTtl{}

func (c FfiConverterFfiTtl) Lift(rb RustBufferI) FfiTtl {
	return LiftFromRustBuffer[FfiTtl](c, rb)
}

func (c FfiConverterFfiTtl) Lower(value FfiTtl) C.RustBuffer {
	return LowerIntoRustBuffer[FfiTtl](c, value)
}

func (c FfiConverterFfiTtl) LowerExternal(value FfiTtl) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FfiTtl](c, value))
}
func (FfiConverterFfiTtl) Read(reader io.Reader) FfiTtl {
	id := readInt32(reader)
	switch id {
	case 1:
		return FfiTtlDefault{}
	case 2:
		return FfiTtlNoExpiry{}
	case 3:
		return FfiTtlExpireAfterTicks{
			FfiConverterUint64INSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterFfiTtl.Read()", id))
	}
}

func (FfiConverterFfiTtl) Write(writer io.Writer, value FfiTtl) {
	switch variant_value := value.(type) {
	case FfiTtlDefault:
		writeInt32(writer, 1)
	case FfiTtlNoExpiry:
		writeInt32(writer, 2)
	case FfiTtlExpireAfterTicks:
		writeInt32(writer, 3)
		FfiConverterUint64INSTANCE.Write(writer, variant_value.Field0)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterFfiTtl.Write", value))
	}
}

type FfiDestroyerFfiTtl struct{}

func (_ FfiDestroyerFfiTtl) Destroy(value FfiTtl) {
	value.Destroy()
}

type FfiConverterOptionalUint32 struct{}

var FfiConverterOptionalUint32INSTANCE = FfiConverterOptionalUint32{}

func (c FfiConverterOptionalUint32) Lift(rb RustBufferI) *uint32 {
	return LiftFromRustBuffer[*uint32](c, rb)
}

func (_ FfiConverterOptionalUint32) Read(reader io.Reader) *uint32 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint32INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint32) Lower(value *uint32) C.RustBuffer {
	return LowerIntoRustBuffer[*uint32](c, value)
}

func (c FfiConverterOptionalUint32) LowerExternal(value *uint32) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint32](c, value))
}

func (_ FfiConverterOptionalUint32) Write(writer io.Writer, value *uint32) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint32INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint32 struct{}

func (_ FfiDestroyerOptionalUint32) Destroy(value *uint32) {
	if value != nil {
		FfiDestroyerUint32{}.Destroy(*value)
	}
}

type FfiConverterOptionalUint64 struct{}

var FfiConverterOptionalUint64INSTANCE = FfiConverterOptionalUint64{}

func (c FfiConverterOptionalUint64) Lift(rb RustBufferI) *uint64 {
	return LiftFromRustBuffer[*uint64](c, rb)
}

func (_ FfiConverterOptionalUint64) Read(reader io.Reader) *uint64 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint64INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint64) Lower(value *uint64) C.RustBuffer {
	return LowerIntoRustBuffer[*uint64](c, value)
}

func (c FfiConverterOptionalUint64) LowerExternal(value *uint64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint64](c, value))
}

func (_ FfiConverterOptionalUint64) Write(writer io.Writer, value *uint64) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint64INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint64 struct{}

func (_ FfiDestroyerOptionalUint64) Destroy(value *uint64) {
	if value != nil {
		FfiDestroyerUint64{}.Destroy(*value)
	}
}

type FfiConverterOptionalInt64 struct{}

var FfiConverterOptionalInt64INSTANCE = FfiConverterOptionalInt64{}

func (c FfiConverterOptionalInt64) Lift(rb RustBufferI) *int64 {
	return LiftFromRustBuffer[*int64](c, rb)
}

func (_ FfiConverterOptionalInt64) Read(reader io.Reader) *int64 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterInt64INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalInt64) Lower(value *int64) C.RustBuffer {
	return LowerIntoRustBuffer[*int64](c, value)
}

func (c FfiConverterOptionalInt64) LowerExternal(value *int64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*int64](c, value))
}

func (_ FfiConverterOptionalInt64) Write(writer io.Writer, value *int64) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterInt64INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalInt64 struct{}

func (_ FfiDestroyerOptionalInt64) Destroy(value *int64) {
	if value != nil {
		FfiDestroyerInt64{}.Destroy(*value)
	}
}

type FfiConverterOptionalString struct{}

var FfiConverterOptionalStringINSTANCE = FfiConverterOptionalString{}

func (c FfiConverterOptionalString) Lift(rb RustBufferI) *string {
	return LiftFromRustBuffer[*string](c, rb)
}

func (_ FfiConverterOptionalString) Read(reader io.Reader) *string {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterStringINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalString) Lower(value *string) C.RustBuffer {
	return LowerIntoRustBuffer[*string](c, value)
}

func (c FfiConverterOptionalString) LowerExternal(value *string) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*string](c, value))
}

func (_ FfiConverterOptionalString) Write(writer io.Writer, value *string) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalString struct{}

func (_ FfiDestroyerOptionalString) Destroy(value *string) {
	if value != nil {
		FfiDestroyerString{}.Destroy(*value)
	}
}

type FfiConverterOptionalBytes struct{}

var FfiConverterOptionalBytesINSTANCE = FfiConverterOptionalBytes{}

func (c FfiConverterOptionalBytes) Lift(rb RustBufferI) *[]byte {
	return LiftFromRustBuffer[*[]byte](c, rb)
}

func (_ FfiConverterOptionalBytes) Read(reader io.Reader) *[]byte {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterBytesINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalBytes) Lower(value *[]byte) C.RustBuffer {
	return LowerIntoRustBuffer[*[]byte](c, value)
}

func (c FfiConverterOptionalBytes) LowerExternal(value *[]byte) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*[]byte](c, value))
}

func (_ FfiConverterOptionalBytes) Write(writer io.Writer, value *[]byte) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterBytesINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalBytes struct{}

func (_ FfiDestroyerOptionalBytes) Destroy(value *[]byte) {
	if value != nil {
		FfiDestroyerBytes{}.Destroy(*value)
	}
}

type FfiConverterOptionalFfiLogCallback struct{}

var FfiConverterOptionalFfiLogCallbackINSTANCE = FfiConverterOptionalFfiLogCallback{}

func (c FfiConverterOptionalFfiLogCallback) Lift(rb RustBufferI) *FfiLogCallback {
	return LiftFromRustBuffer[*FfiLogCallback](c, rb)
}

func (_ FfiConverterOptionalFfiLogCallback) Read(reader io.Reader) *FfiLogCallback {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterFfiLogCallbackINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalFfiLogCallback) Lower(value *FfiLogCallback) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiLogCallback](c, value)
}

func (c FfiConverterOptionalFfiLogCallback) LowerExternal(value *FfiLogCallback) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiLogCallback](c, value))
}

func (_ FfiConverterOptionalFfiLogCallback) Write(writer io.Writer, value *FfiLogCallback) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterFfiLogCallbackINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalFfiLogCallback struct{}

func (_ FfiDestroyerOptionalFfiLogCallback) Destroy(value *FfiLogCallback) {
	if value != nil {
		FfiDestroyerFfiLogCallback{}.Destroy(*value)
	}
}

type FfiConverterOptionalFfiKeyValue struct{}

var FfiConverterOptionalFfiKeyValueINSTANCE = FfiConverterOptionalFfiKeyValue{}

func (c FfiConverterOptionalFfiKeyValue) Lift(rb RustBufferI) *FfiKeyValue {
	return LiftFromRustBuffer[*FfiKeyValue](c, rb)
}

func (_ FfiConverterOptionalFfiKeyValue) Read(reader io.Reader) *FfiKeyValue {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterFfiKeyValueINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalFfiKeyValue) Lower(value *FfiKeyValue) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiKeyValue](c, value)
}

func (c FfiConverterOptionalFfiKeyValue) LowerExternal(value *FfiKeyValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiKeyValue](c, value))
}

func (_ FfiConverterOptionalFfiKeyValue) Write(writer io.Writer, value *FfiKeyValue) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterFfiKeyValueINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalFfiKeyValue struct{}

func (_ FfiDestroyerOptionalFfiKeyValue) Destroy(value *FfiKeyValue) {
	if value != nil {
		FfiDestroyerFfiKeyValue{}.Destroy(*value)
	}
}

type FfiConverterOptionalFfiRowEntry struct{}

var FfiConverterOptionalFfiRowEntryINSTANCE = FfiConverterOptionalFfiRowEntry{}

func (c FfiConverterOptionalFfiRowEntry) Lift(rb RustBufferI) *FfiRowEntry {
	return LiftFromRustBuffer[*FfiRowEntry](c, rb)
}

func (_ FfiConverterOptionalFfiRowEntry) Read(reader io.Reader) *FfiRowEntry {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterFfiRowEntryINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalFfiRowEntry) Lower(value *FfiRowEntry) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiRowEntry](c, value)
}

func (c FfiConverterOptionalFfiRowEntry) LowerExternal(value *FfiRowEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiRowEntry](c, value))
}

func (_ FfiConverterOptionalFfiRowEntry) Write(writer io.Writer, value *FfiRowEntry) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterFfiRowEntryINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalFfiRowEntry struct{}

func (_ FfiDestroyerOptionalFfiRowEntry) Destroy(value *FfiRowEntry) {
	if value != nil {
		FfiDestroyerFfiRowEntry{}.Destroy(*value)
	}
}

type FfiConverterOptionalFfiWriteHandle struct{}

var FfiConverterOptionalFfiWriteHandleINSTANCE = FfiConverterOptionalFfiWriteHandle{}

func (c FfiConverterOptionalFfiWriteHandle) Lift(rb RustBufferI) *FfiWriteHandle {
	return LiftFromRustBuffer[*FfiWriteHandle](c, rb)
}

func (_ FfiConverterOptionalFfiWriteHandle) Read(reader io.Reader) *FfiWriteHandle {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterFfiWriteHandleINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalFfiWriteHandle) Lower(value *FfiWriteHandle) C.RustBuffer {
	return LowerIntoRustBuffer[*FfiWriteHandle](c, value)
}

func (c FfiConverterOptionalFfiWriteHandle) LowerExternal(value *FfiWriteHandle) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*FfiWriteHandle](c, value))
}

func (_ FfiConverterOptionalFfiWriteHandle) Write(writer io.Writer, value *FfiWriteHandle) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterFfiWriteHandleINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalFfiWriteHandle struct{}

func (_ FfiDestroyerOptionalFfiWriteHandle) Destroy(value *FfiWriteHandle) {
	if value != nil {
		FfiDestroyerFfiWriteHandle{}.Destroy(*value)
	}
}

type FfiConverterSequenceBytes struct{}

var FfiConverterSequenceBytesINSTANCE = FfiConverterSequenceBytes{}

func (c FfiConverterSequenceBytes) Lift(rb RustBufferI) [][]byte {
	return LiftFromRustBuffer[[][]byte](c, rb)
}

func (c FfiConverterSequenceBytes) Read(reader io.Reader) [][]byte {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([][]byte, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterBytesINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceBytes) Lower(value [][]byte) C.RustBuffer {
	return LowerIntoRustBuffer[[][]byte](c, value)
}

func (c FfiConverterSequenceBytes) LowerExternal(value [][]byte) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[][]byte](c, value))
}

func (c FfiConverterSequenceBytes) Write(writer io.Writer, value [][]byte) {
	if len(value) > math.MaxInt32 {
		panic("[][]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterBytesINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceBytes struct{}

func (FfiDestroyerSequenceBytes) Destroy(sequence [][]byte) {
	for _, value := range sequence {
		FfiDestroyerBytes{}.Destroy(value)
	}
}

type FfiConverterSequenceFfiWalFile struct{}

var FfiConverterSequenceFfiWalFileINSTANCE = FfiConverterSequenceFfiWalFile{}

func (c FfiConverterSequenceFfiWalFile) Lift(rb RustBufferI) []*FfiWalFile {
	return LiftFromRustBuffer[[]*FfiWalFile](c, rb)
}

func (c FfiConverterSequenceFfiWalFile) Read(reader io.Reader) []*FfiWalFile {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]*FfiWalFile, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterFfiWalFileINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceFfiWalFile) Lower(value []*FfiWalFile) C.RustBuffer {
	return LowerIntoRustBuffer[[]*FfiWalFile](c, value)
}

func (c FfiConverterSequenceFfiWalFile) LowerExternal(value []*FfiWalFile) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]*FfiWalFile](c, value))
}

func (c FfiConverterSequenceFfiWalFile) Write(writer io.Writer, value []*FfiWalFile) {
	if len(value) > math.MaxInt32 {
		panic("[]*FfiWalFile is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterFfiWalFileINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceFfiWalFile struct{}

func (FfiDestroyerSequenceFfiWalFile) Destroy(sequence []*FfiWalFile) {
	for _, value := range sequence {
		FfiDestroyerFfiWalFile{}.Destroy(value)
	}
}

type FfiConverterMapStringInt64 struct{}

var FfiConverterMapStringInt64INSTANCE = FfiConverterMapStringInt64{}

func (c FfiConverterMapStringInt64) Lift(rb RustBufferI) map[string]int64 {
	return LiftFromRustBuffer[map[string]int64](c, rb)
}

func (_ FfiConverterMapStringInt64) Read(reader io.Reader) map[string]int64 {
	result := make(map[string]int64)
	length := readInt32(reader)
	for i := int32(0); i < length; i++ {
		key := FfiConverterStringINSTANCE.Read(reader)
		value := FfiConverterInt64INSTANCE.Read(reader)
		result[key] = value
	}
	return result
}

func (c FfiConverterMapStringInt64) Lower(value map[string]int64) C.RustBuffer {
	return LowerIntoRustBuffer[map[string]int64](c, value)
}

func (c FfiConverterMapStringInt64) LowerExternal(value map[string]int64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[map[string]int64](c, value))
}

func (_ FfiConverterMapStringInt64) Write(writer io.Writer, mapValue map[string]int64) {
	if len(mapValue) > math.MaxInt32 {
		panic("map[string]int64 is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(mapValue)))
	for key, value := range mapValue {
		FfiConverterStringINSTANCE.Write(writer, key)
		FfiConverterInt64INSTANCE.Write(writer, value)
	}
}

type FfiDestroyerMapStringInt64 struct{}

func (_ FfiDestroyerMapStringInt64) Destroy(mapValue map[string]int64) {
	for key, value := range mapValue {
		FfiDestroyerString{}.Destroy(key)
		FfiDestroyerInt64{}.Destroy(value)
	}
}

const (
	uniffiRustFuturePollReady      int8 = 0
	uniffiRustFuturePollMaybeReady int8 = 1
)

type rustFuturePollFunc func(C.uint64_t, C.UniffiRustFutureContinuationCallback, C.uint64_t)
type rustFutureCompleteFunc[T any] func(C.uint64_t, *C.RustCallStatus) T
type rustFutureFreeFunc func(C.uint64_t)

//export slatedb_uniffiFutureContinuationCallback
func slatedb_uniffiFutureContinuationCallback(data C.uint64_t, pollResult C.int8_t) {
	h := cgo.Handle(uintptr(data))
	waiter := h.Value().(chan int8)
	waiter <- int8(pollResult)
}

func uniffiRustCallAsync[E any, T any, F any](
	errConverter BufReader[*E],
	completeFunc rustFutureCompleteFunc[F],
	liftFunc func(F) T,
	rustFuture C.uint64_t,
	pollFunc rustFuturePollFunc,
	freeFunc rustFutureFreeFunc,
) (T, *E) {
	defer freeFunc(rustFuture)

	pollResult := int8(-1)
	waiter := make(chan int8, 1)

	chanHandle := cgo.NewHandle(waiter)
	defer chanHandle.Delete()

	for pollResult != uniffiRustFuturePollReady {
		pollFunc(
			rustFuture,
			(C.UniffiRustFutureContinuationCallback)(C.slatedb_uniffiFutureContinuationCallback),
			C.uint64_t(chanHandle),
		)
		pollResult = <-waiter
	}

	var goValue T
	var ffiValue F
	var err *E

	ffiValue, err = rustCallWithError(errConverter, func(status *C.RustCallStatus) F {
		return completeFunc(rustFuture, status)
	})
	if err != nil {
		return goValue, err
	}
	return liftFunc(ffiValue), nil
}

//export slatedb_uniffiFreeGorutine
func slatedb_uniffiFreeGorutine(data C.uint64_t) {
	handle := cgo.Handle(uintptr(data))
	defer handle.Delete()

	guard := handle.Value().(chan struct{})
	guard <- struct{}{}
}

func FfiInitLogging(level FfiLogLevel, callback *FfiLogCallback) error {
	_, _uniffiErr := rustCallWithError[FfiError](FfiConverterFfiError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_func_ffi_init_logging(FfiConverterFfiLogLevelINSTANCE.Lower(level), FfiConverterOptionalFfiLogCallbackINSTANCE.Lower(callback), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
