package slatedb

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

	FfiConverterCallbackInterfaceMergeOperatorINSTANCE.register()
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
			return C.uniffi_slatedb_ffi_checksum_func_default_settings_json()
		})
		if checksum != 41457 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_default_settings_json: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_func_init_default_logging()
		})
		if checksum != 46765 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_init_default_logging: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_func_init_logging()
		})
		if checksum != 36097 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_init_logging: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_func_resolve_object_store()
		})
		if checksum != 23127 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_resolve_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_func_set_logging_level()
		})
		if checksum != 63354 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_func_set_logging_level: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_begin()
		})
		if checksum != 5274 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_begin: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_delete()
		})
		if checksum != 53628 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_delete_with_options()
		})
		if checksum != 46529 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_delete_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_flush()
		})
		if checksum != 56183 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_flush: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_flush_with_options()
		})
		if checksum != 16447 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_flush_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_get()
		})
		if checksum != 16615 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_get_key_value()
		})
		if checksum != 30007 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_get_key_value_with_options()
		})
		if checksum != 20887 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_get_with_options()
		})
		if checksum != 34184 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_merge()
		})
		if checksum != 49680 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_merge_with_options()
		})
		if checksum != 29273 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_metrics()
		})
		if checksum != 4162 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_metrics: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_put()
		})
		if checksum != 49345 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_put_with_options()
		})
		if checksum != 19637 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_scan()
		})
		if checksum != 20253 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_scan_prefix()
		})
		if checksum != 50816 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_scan_prefix_with_options()
		})
		if checksum != 54339 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_scan_with_options()
		})
		if checksum != 26695 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_shutdown()
		})
		if checksum != 37181 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_snapshot()
		})
		if checksum != 7345 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_snapshot: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_status()
		})
		if checksum != 60950 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_status: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_write()
		})
		if checksum != 52274 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_write_batch()
		})
		if checksum != 39142 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_write_batch: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_write_batch_with_options()
		})
		if checksum != 57917 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_write_batch_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_db_write_with_options()
		})
		if checksum != 52159 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_db_write_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_build()
		})
		if checksum != 35713 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_db_cache_disabled()
		})
		if checksum != 30405 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_db_cache_disabled: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_merge_operator()
		})
		if checksum != 26111 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_seed()
		})
		if checksum != 9556 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_seed: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_settings_json()
		})
		if checksum != 26925 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_settings_json: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_sst_block_size()
		})
		if checksum != 16153 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_sst_block_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_wal_object_store()
		})
		if checksum != 61826 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbiterator_next()
		})
		if checksum != 30810 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbiterator_seek()
		})
		if checksum != 53625 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbiterator_seek: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_close()
		})
		if checksum != 28281 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_get()
		})
		if checksum != 38522 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_get_with_options()
		})
		if checksum != 17382 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_scan()
		})
		if checksum != 63531 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix()
		})
		if checksum != 2026 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix_with_options()
		})
		if checksum != 34184 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreader_scan_with_options()
		})
		if checksum != 4110 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreader_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_build()
		})
		if checksum != 63962 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_checkpoint_id()
		})
		if checksum != 35880 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_checkpoint_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_options()
		})
		if checksum != 2929 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get()
		})
		if checksum != 28442 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value()
		})
		if checksum != 21199 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value_with_options()
		})
		if checksum != 7853 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_with_options()
		})
		if checksum != 57868 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan()
		})
		if checksum != 14532 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix()
		})
		if checksum != 39527 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix_with_options()
		})
		if checksum != 26923 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_with_options()
		})
		if checksum != 14264 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit()
		})
		if checksum != 38520 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_commit: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit_with_options()
		})
		if checksum != 30206 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_commit_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_delete()
		})
		if checksum != 31021 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_get()
		})
		if checksum != 5920 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value()
		})
		if checksum != 26704 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value_with_options()
		})
		if checksum != 58235 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_with_options()
		})
		if checksum != 37338 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_id()
		})
		if checksum != 29935 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_mark_read()
		})
		if checksum != 23131 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_mark_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge()
		})
		if checksum != 47972 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge_with_options()
		})
		if checksum != 27318 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_put()
		})
		if checksum != 17459 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_put_with_options()
		})
		if checksum != 36745 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_rollback()
		})
		if checksum != 8231 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_rollback: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan()
		})
		if checksum != 63657 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix()
		})
		if checksum != 9925 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix_with_options()
		})
		if checksum != 4020 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_with_options()
		})
		if checksum != 8054 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_seqnum()
		})
		if checksum != 15980 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_seqnum: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_dbtransaction_unmark_write()
		})
		if checksum != 46328 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_dbtransaction_unmark_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_close()
		})
		if checksum != 60993 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_id()
		})
		if checksum != 57238 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_iterator()
		})
		if checksum != 39227 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_iterator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_metadata()
		})
		if checksum != 44420 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_next_file()
		})
		if checksum != 39455 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_next_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfile_next_id()
		})
		if checksum != 58518 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfile_next_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfileiterator_close()
		})
		if checksum != 59948 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfileiterator_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walfileiterator_next()
		})
		if checksum != 38392 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walfileiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walreader_close()
		})
		if checksum != 29323 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walreader_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walreader_get()
		})
		if checksum != 56288 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_walreader_list()
		})
		if checksum != 35955 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_walreader_list: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_close()
		})
		if checksum != 33348 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_delete()
		})
		if checksum != 39307 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_merge()
		})
		if checksum != 17630 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_merge_with_options()
		})
		if checksum != 54921 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_put()
		})
		if checksum != 54566 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_writebatch_put_with_options()
		})
		if checksum != 7339 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_writebatch_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_dbbuilder_new()
		})
		if checksum != 30406 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_dbbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_dbreaderbuilder_new()
		})
		if checksum != 37615 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_dbreaderbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_walreader_new()
		})
		if checksum != 44616 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_walreader_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_constructor_writebatch_new()
		})
		if checksum != 733 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_constructor_writebatch_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_ffi_checksum_method_mergeoperator_merge()
		})
		if checksum != 14285 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_ffi_checksum_method_mergeoperator_merge: UniFFI API checksum mismatch")
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

// Handle to an open SlateDB database.
//
// Instances of this type are created by [`crate::DbBuilder::build`].
type DbInterface interface {
	// Begin a new transaction at the requested isolation level.
	Begin(isolationLevel IsolationLevel) (*DbTransaction, error)
	// Delete a key using default write options.
	Delete(key []byte) (WriteHandle, error)
	// Delete a key using custom write options.
	DeleteWithOptions(key []byte, options DbWriteOptions) (WriteHandle, error)
	// Flush in-memory state using the database defaults.
	Flush() error
	// Flush in-memory state using explicit flush options.
	FlushWithOptions(options DbFlushOptions) error
	// Get the value for a key using default read options.
	Get(key []byte) (*[]byte, error)
	// Get the full row metadata for a key using default read options.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Get the full row metadata for a key using custom read options.
	GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error)
	// Get the value for a key using custom read options.
	GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error)
	// Merge an operand into a key using default options.
	Merge(key []byte, operand []byte) (WriteHandle, error)
	// Merge an operand into a key using custom merge and write options.
	MergeWithOptions(key []byte, operand []byte, mergeOptions DbMergeOptions, writeOptions DbWriteOptions) (WriteHandle, error)
	// Snapshot the current database metrics registry.
	Metrics() (map[string]int64, error)
	// Put a value for a key using default options.
	//
	// ## Errors
	// - `SlatedbError::Invalid`: if the key is empty or exceeds SlateDB limits.
	Put(key []byte, value []byte) (WriteHandle, error)
	// Put a value for a key using custom put and write options.
	PutWithOptions(key []byte, value []byte, putOptions DbPutOptions, writeOptions DbWriteOptions) (WriteHandle, error)
	// Scan a key range using default scan options.
	Scan(varRange DbKeyRange) (*DbIterator, error)
	// Scan all keys that share the provided prefix.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scan all keys that share the provided prefix using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error)
	// Scan a key range using custom scan options.
	ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error)
	// Close the database.
	Shutdown() error
	// Create a point-in-time snapshot of the database.
	Snapshot() (*DbSnapshot, error)
	// Check whether the database is still open.
	//
	// ## Returns
	// - `Result<(), SlatedbError>`: `Ok(())` if the database is open.
	Status() error
	// Apply a batch of operations atomically using default write options.
	Write(operations []DbWriteOperation) (WriteHandle, error)
	// Apply an existing write batch atomically using default write options.
	WriteBatch(batch *WriteBatch) (WriteHandle, error)
	// Apply an existing write batch atomically using custom write options.
	WriteBatchWithOptions(batch *WriteBatch, options DbWriteOptions) (WriteHandle, error)
	// Apply a batch of operations atomically using custom write options.
	WriteWithOptions(operations []DbWriteOperation, options DbWriteOptions) (WriteHandle, error)
}

// Handle to an open SlateDB database.
//
// Instances of this type are created by [`crate::DbBuilder::build`].
type Db struct {
	ffiObject FfiObject
}

// Begin a new transaction at the requested isolation level.
func (_self *Db) Begin(isolationLevel IsolationLevel) (*DbTransaction, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbTransaction {
			return FfiConverterDbTransactionINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_begin(
			_pointer, FfiConverterIsolationLevelINSTANCE.Lower(isolationLevel)),
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

// Delete a key using default write options.
func (_self *Db) Delete(key []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_delete(
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

// Delete a key using custom write options.
func (_self *Db) DeleteWithOptions(key []byte, options DbWriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_delete_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbWriteOptionsINSTANCE.Lower(options)),
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

// Flush in-memory state using the database defaults.
func (_self *Db) Flush() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_db_flush(
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

// Flush in-memory state using explicit flush options.
func (_self *Db) FlushWithOptions(options DbFlushOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_db_flush_with_options(
			_pointer, FfiConverterDbFlushOptionsINSTANCE.Lower(options)),
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

// Get the value for a key using default read options.
func (_self *Db) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_db_get(
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

// Get the full row metadata for a key using default read options.
func (_self *Db) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_get_key_value(
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

// Get the full row metadata for a key using custom read options.
func (_self *Db) GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Get the value for a key using custom read options.
func (_self *Db) GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_db_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Merge an operand into a key using default options.
func (_self *Db) Merge(key []byte, operand []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_merge(
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

// Merge an operand into a key using custom merge and write options.
func (_self *Db) MergeWithOptions(key []byte, operand []byte, mergeOptions DbMergeOptions, writeOptions DbWriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterDbMergeOptionsINSTANCE.Lower(mergeOptions), FfiConverterDbWriteOptionsINSTANCE.Lower(writeOptions)),
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

// Snapshot the current database metrics registry.
func (_self *Db) Metrics() (map[string]int64, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_db_metrics(
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

// Put a value for a key using default options.
//
// ## Errors
// - `SlatedbError::Invalid`: if the key is empty or exceeds SlateDB limits.
func (_self *Db) Put(key []byte, value []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_put(
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

// Put a value for a key using custom put and write options.
func (_self *Db) PutWithOptions(key []byte, value []byte, putOptions DbPutOptions, writeOptions DbWriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterDbPutOptionsINSTANCE.Lower(putOptions), FfiConverterDbWriteOptionsINSTANCE.Lower(writeOptions)),
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

// Scan a key range using default scan options.
func (_self *Db) Scan(varRange DbKeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_scan(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange)),
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

// Scan all keys that share the provided prefix.
func (_self *Db) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_scan_prefix(
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

// Scan all keys that share the provided prefix using custom scan options.
func (_self *Db) ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Scan a key range using custom scan options.
func (_self *Db) ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_scan_with_options(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Close the database.
func (_self *Db) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_db_shutdown(
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

// Create a point-in-time snapshot of the database.
func (_self *Db) Snapshot() (*DbSnapshot, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbSnapshot {
			return FfiConverterDbSnapshotINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_snapshot(
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

// Check whether the database is still open.
//
// ## Returns
// - `Result<(), SlatedbError>`: `Ok(())` if the database is open.
func (_self *Db) Status() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_db_status(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Apply a batch of operations atomically using default write options.
func (_self *Db) Write(operations []DbWriteOperation) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_write(
			_pointer, FfiConverterSequenceDbWriteOperationINSTANCE.Lower(operations)),
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

// Apply an existing write batch atomically using default write options.
func (_self *Db) WriteBatch(batch *WriteBatch) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_write_batch(
			_pointer, FfiConverterWriteBatchINSTANCE.Lower(batch)),
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

// Apply an existing write batch atomically using custom write options.
func (_self *Db) WriteBatchWithOptions(batch *WriteBatch, options DbWriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_write_batch_with_options(
			_pointer, FfiConverterWriteBatchINSTANCE.Lower(batch), FfiConverterDbWriteOptionsINSTANCE.Lower(options)),
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

// Apply a batch of operations atomically using custom write options.
func (_self *Db) WriteWithOptions(operations []DbWriteOperation, options DbWriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_db_write_with_options(
			_pointer, FfiConverterSequenceDbWriteOperationINSTANCE.Lower(operations), FfiConverterDbWriteOptionsINSTANCE.Lower(options)),
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
func (object *Db) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDb struct{}

var FfiConverterDbINSTANCE = FfiConverterDb{}

func (c FfiConverterDb) Lift(pointer unsafe.Pointer) *Db {
	result := &Db{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_db(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_db(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Db).Destroy)
	return result
}

func (c FfiConverterDb) Read(reader io.Reader) *Db {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDb) Lower(value *Db) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*Db")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDb) Write(writer io.Writer, value *Db) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDb struct{}

func (_ FfiDestroyerDb) Destroy(value *Db) {
	value.Destroy()
}

// Builder used to configure and open a [`Db`].
type DbBuilderInterface interface {
	// Open the database using the builder's current configuration.
	//
	// This consumes the builder state. Reusing the same builder after a
	// successful or failed call to `build()` returns an error.
	//
	// ## Returns
	// - `Result<Arc<Db>, SlatedbError>`: the opened database handle.
	//
	// ## Errors
	// - `SlatedbError`: if the builder was already consumed or the database cannot be opened.
	Build() (*Db, error)
	// Disable the database-level cache created by the builder.
	WithDbCacheDisabled() error
	// Configure the merge operator used for merge reads and writes.
	//
	// ## Arguments
	// - `merge_operator`: the callback implementation to use.
	WithMergeOperator(mergeOperator MergeOperator) error
	// Set the random seed used by the database.
	//
	// ## Arguments
	// - `seed`: the seed to use when constructing the database.
	WithSeed(seed uint64) error
	// Replace the default database settings with a JSON-encoded [`slatedb::Settings`] document.
	//
	// ## Arguments
	// - `settings_json`: the full settings document encoded as JSON.
	//
	// ## Errors
	// - `SlatedbError::Invalid`: if the JSON cannot be parsed.
	WithSettingsJson(settingsJson string) error
	// Override the SST block size used for new SSTs.
	//
	// ## Arguments
	// - `sst_block_size`: the block size to use.
	WithSstBlockSize(sstBlockSize SstBlockSize) error
	// Configure a separate object store for WAL data.
	//
	// ## Arguments
	// - `wal_object_store`: the object store to use for WAL files.
	WithWalObjectStore(walObjectStore *ObjectStore) error
}

// Builder used to configure and open a [`Db`].
type DbBuilder struct {
	ffiObject FfiObject
}

// Create a new builder for a database.
//
// ## Arguments
// - `path`: the database path within the object store.
// - `object_store`: the object store that will back the database.
//
// ## Returns
// - `Arc<DbBuilder>`: a new builder instance.
func NewDbBuilder(path string, objectStore *ObjectStore) *DbBuilder {
	return FfiConverterDbBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_dbbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Open the database using the builder's current configuration.
//
// This consumes the builder state. Reusing the same builder after a
// successful or failed call to `build()` returns an error.
//
// ## Returns
// - `Result<Arc<Db>, SlatedbError>`: the opened database handle.
//
// ## Errors
// - `SlatedbError`: if the builder was already consumed or the database cannot be opened.
func (_self *DbBuilder) Build() (*Db, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *Db {
			return FfiConverterDbINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_build(
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

// Disable the database-level cache created by the builder.
func (_self *DbBuilder) WithDbCacheDisabled() error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_db_cache_disabled(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Configure the merge operator used for merge reads and writes.
//
// ## Arguments
// - `merge_operator`: the callback implementation to use.
func (_self *DbBuilder) WithMergeOperator(mergeOperator MergeOperator) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_merge_operator(
			_pointer, FfiConverterCallbackInterfaceMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Set the random seed used by the database.
//
// ## Arguments
// - `seed`: the seed to use when constructing the database.
func (_self *DbBuilder) WithSeed(seed uint64) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_seed(
			_pointer, FfiConverterUint64INSTANCE.Lower(seed), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Replace the default database settings with a JSON-encoded [`slatedb::Settings`] document.
//
// ## Arguments
// - `settings_json`: the full settings document encoded as JSON.
//
// ## Errors
// - `SlatedbError::Invalid`: if the JSON cannot be parsed.
func (_self *DbBuilder) WithSettingsJson(settingsJson string) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_settings_json(
			_pointer, FfiConverterStringINSTANCE.Lower(settingsJson), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Override the SST block size used for new SSTs.
//
// ## Arguments
// - `sst_block_size`: the block size to use.
func (_self *DbBuilder) WithSstBlockSize(sstBlockSize SstBlockSize) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_sst_block_size(
			_pointer, FfiConverterSstBlockSizeINSTANCE.Lower(sstBlockSize), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Configure a separate object store for WAL data.
//
// ## Arguments
// - `wal_object_store`: the object store to use for WAL files.
func (_self *DbBuilder) WithWalObjectStore(walObjectStore *ObjectStore) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbbuilder_with_wal_object_store(
			_pointer, FfiConverterObjectStoreINSTANCE.Lower(walObjectStore), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *DbBuilder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbBuilder struct{}

var FfiConverterDbBuilderINSTANCE = FfiConverterDbBuilder{}

func (c FfiConverterDbBuilder) Lift(pointer unsafe.Pointer) *DbBuilder {
	result := &DbBuilder{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbbuilder(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbBuilder).Destroy)
	return result
}

func (c FfiConverterDbBuilder) Read(reader io.Reader) *DbBuilder {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbBuilder) Lower(value *DbBuilder) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbBuilder")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbBuilder) Write(writer io.Writer, value *DbBuilder) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbBuilder struct{}

func (_ FfiDestroyerDbBuilder) Destroy(value *DbBuilder) {
	value.Destroy()
}

// An asynchronous iterator over key-value pairs.
//
// Instances of this type are returned by scan operations on [`crate::Db`],
// [`crate::DbSnapshot`], and [`crate::DbTransaction`].
type DbIteratorInterface interface {
	// Return the next item from the iterator.
	//
	// ## Returns
	// - `Result<Option<KeyValue>, SlatedbError>`: the next key-value pair, or
	// `None` when the iterator is exhausted.
	Next() (*KeyValue, error)
	// Reposition the iterator to the first key greater than or equal to `key`.
	//
	// ## Arguments
	// - `key`: the key to seek to within the iterator's range.
	//
	// ## Errors
	// - `SlatedbError::Invalid`: if `key` is empty.
	// - `SlatedbError`: if the key falls outside the iterator's valid range.
	Seek(key []byte) error
}

// An asynchronous iterator over key-value pairs.
//
// Instances of this type are returned by scan operations on [`crate::Db`],
// [`crate::DbSnapshot`], and [`crate::DbTransaction`].
type DbIterator struct {
	ffiObject FfiObject
}

// Return the next item from the iterator.
//
// ## Returns
// - `Result<Option<KeyValue>, SlatedbError>`: the next key-value pair, or
// `None` when the iterator is exhausted.
func (_self *DbIterator) Next() (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbiterator_next(
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

// Reposition the iterator to the first key greater than or equal to `key`.
//
// ## Arguments
// - `key`: the key to seek to within the iterator's range.
//
// ## Errors
// - `SlatedbError::Invalid`: if `key` is empty.
// - `SlatedbError`: if the key falls outside the iterator's valid range.
func (_self *DbIterator) Seek(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbIterator")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbiterator_seek(
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
func (object *DbIterator) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbIterator struct{}

var FfiConverterDbIteratorINSTANCE = FfiConverterDbIterator{}

func (c FfiConverterDbIterator) Lift(pointer unsafe.Pointer) *DbIterator {
	result := &DbIterator{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbiterator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbIterator).Destroy)
	return result
}

func (c FfiConverterDbIterator) Read(reader io.Reader) *DbIterator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbIterator) Lower(value *DbIterator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbIterator")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbIterator) Write(writer io.Writer, value *DbIterator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbIterator struct{}

func (_ FfiDestroyerDbIterator) Destroy(value *DbIterator) {
	value.Destroy()
}

// A read-only database reader.
type DbReaderInterface interface {
	// Close the reader.
	Close() error
	// Get the value for a key using default read options.
	Get(key []byte) (*[]byte, error)
	// Get the value for a key using custom read options.
	GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error)
	// Scan a key range using default scan options.
	Scan(varRange DbKeyRange) (*DbIterator, error)
	// Scan all keys that share the provided prefix.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scan all keys that share the provided prefix using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error)
	// Scan a key range using custom scan options.
	ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error)
}

// A read-only database reader.
type DbReader struct {
	ffiObject FfiObject
}

// Close the reader.
func (_self *DbReader) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbreader_close(
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

// Get the value for a key using default read options.
func (_self *DbReader) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbreader_get(
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

// Get the value for a key using custom read options.
func (_self *DbReader) GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbreader_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Scan a key range using default scan options.
func (_self *DbReader) Scan(varRange DbKeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbreader_scan(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange)),
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

// Scan all keys that share the provided prefix.
func (_self *DbReader) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbreader_scan_prefix(
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

// Scan all keys that share the provided prefix using custom scan options.
func (_self *DbReader) ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbreader_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Scan a key range using custom scan options.
func (_self *DbReader) ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbreader_scan_with_options(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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
func (object *DbReader) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbReader struct{}

var FfiConverterDbReaderINSTANCE = FfiConverterDbReader{}

func (c FfiConverterDbReader) Lift(pointer unsafe.Pointer) *DbReader {
	result := &DbReader{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbreader(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbReader).Destroy)
	return result
}

func (c FfiConverterDbReader) Read(reader io.Reader) *DbReader {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbReader) Lower(value *DbReader) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbReader")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbReader) Write(writer io.Writer, value *DbReader) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbReader struct{}

func (_ FfiDestroyerDbReader) Destroy(value *DbReader) {
	value.Destroy()
}

// Builder used to configure and open a [`DbReader`].
type DbReaderBuilderInterface interface {
	// Build the configured database reader.
	Build() (*DbReader, error)
	// Set the checkpoint UUID for the reader and validate it immediately.
	WithCheckpointId(checkpointId string) error
	// Set reader options.
	WithOptions(options DbReaderOptions) error
}

// Builder used to configure and open a [`DbReader`].
type DbReaderBuilder struct {
	ffiObject FfiObject
}

// Create a new builder for a read-only database reader.
func NewDbReaderBuilder(path string, objectStore *ObjectStore) *DbReaderBuilder {
	return FfiConverterDbReaderBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_dbreaderbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Build the configured database reader.
func (_self *DbReaderBuilder) Build() (*DbReader, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbReader {
			return FfiConverterDbReaderINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbreaderbuilder_build(
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

// Set the checkpoint UUID for the reader and validate it immediately.
func (_self *DbReaderBuilder) WithCheckpointId(checkpointId string) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_checkpoint_id(
			_pointer, FfiConverterStringINSTANCE.Lower(checkpointId), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Set reader options.
func (_self *DbReaderBuilder) WithOptions(options DbReaderOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_options(
			_pointer, FfiConverterDbReaderOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *DbReaderBuilder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbReaderBuilder struct{}

var FfiConverterDbReaderBuilderINSTANCE = FfiConverterDbReaderBuilder{}

func (c FfiConverterDbReaderBuilder) Lift(pointer unsafe.Pointer) *DbReaderBuilder {
	result := &DbReaderBuilder{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbreaderbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbreaderbuilder(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbReaderBuilder).Destroy)
	return result
}

func (c FfiConverterDbReaderBuilder) Read(reader io.Reader) *DbReaderBuilder {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbReaderBuilder) Lower(value *DbReaderBuilder) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbReaderBuilder")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbReaderBuilder) Write(writer io.Writer, value *DbReaderBuilder) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbReaderBuilder struct{}

func (_ FfiDestroyerDbReaderBuilder) Destroy(value *DbReaderBuilder) {
	value.Destroy()
}

// A stable point-in-time view of a database.
type DbSnapshotInterface interface {
	// Get the value for a key from the snapshot using default read options.
	Get(key []byte) (*[]byte, error)
	// Get the full row metadata for a key from the snapshot using default read options.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Get the full row metadata for a key from the snapshot using custom read options.
	GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error)
	// Get the value for a key from the snapshot using custom read options.
	GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error)
	// Scan a key range from the snapshot using default scan options.
	Scan(varRange DbKeyRange) (*DbIterator, error)
	// Scan all keys with the provided prefix from the snapshot.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scan all keys with the provided prefix from the snapshot using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error)
	// Scan a key range from the snapshot using custom scan options.
	ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error)
}

// A stable point-in-time view of a database.
type DbSnapshot struct {
	ffiObject FfiObject
}

// Get the value for a key from the snapshot using default read options.
func (_self *DbSnapshot) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_get(
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

// Get the full row metadata for a key from the snapshot using default read options.
func (_self *DbSnapshot) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_get_key_value(
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

// Get the full row metadata for a key from the snapshot using custom read options.
func (_self *DbSnapshot) GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Get the value for a key from the snapshot using custom read options.
func (_self *DbSnapshot) GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Scan a key range from the snapshot using default scan options.
func (_self *DbSnapshot) Scan(varRange DbKeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_scan(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange)),
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

// Scan all keys with the provided prefix from the snapshot.
func (_self *DbSnapshot) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_prefix(
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

// Scan all keys with the provided prefix from the snapshot using custom scan options.
func (_self *DbSnapshot) ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Scan a key range from the snapshot using custom scan options.
func (_self *DbSnapshot) ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_with_options(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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
func (object *DbSnapshot) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbSnapshot struct{}

var FfiConverterDbSnapshotINSTANCE = FfiConverterDbSnapshot{}

func (c FfiConverterDbSnapshot) Lift(pointer unsafe.Pointer) *DbSnapshot {
	result := &DbSnapshot{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbsnapshot(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbsnapshot(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbSnapshot).Destroy)
	return result
}

func (c FfiConverterDbSnapshot) Read(reader io.Reader) *DbSnapshot {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbSnapshot) Lower(value *DbSnapshot) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbSnapshot")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbSnapshot) Write(writer io.Writer, value *DbSnapshot) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbSnapshot struct{}

func (_ FfiDestroyerDbSnapshot) Destroy(value *DbSnapshot) {
	value.Destroy()
}

// A read-write transaction over a [`crate::Db`].
//
// Transactions can be read from and written to until they are committed or
// rolled back. After completion, all further method calls return an error.
type DbTransactionInterface interface {
	// Commit the transaction using default write options.
	//
	// ## Returns
	// - `Result<Option<WriteHandle>, SlatedbError>`: metadata for the committed
	// write, or `None` if the transaction had no writes.
	Commit() (*WriteHandle, error)
	// Commit the transaction using custom write options.
	CommitWithOptions(options DbWriteOptions) (*WriteHandle, error)
	// Buffer a delete inside the transaction.
	Delete(key []byte) error
	// Get the value for a key using default read options.
	Get(key []byte) (*[]byte, error)
	// Get the full row metadata for a key using default read options.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Get the full row metadata for a key using custom read options.
	GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error)
	// Get the value for a key using custom read options.
	GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error)
	// Return the unique identifier assigned to this transaction.
	Id() string
	// Explicitly mark keys as read for conflict detection.
	MarkRead(keys [][]byte) error
	// Buffer a merge inside the transaction using default options.
	Merge(key []byte, operand []byte) error
	// Buffer a merge inside the transaction using custom merge options.
	MergeWithOptions(key []byte, operand []byte, options DbMergeOptions) error
	// Buffer a put inside the transaction using default options.
	Put(key []byte, value []byte) error
	// Buffer a put inside the transaction using custom put options.
	PutWithOptions(key []byte, value []byte, options DbPutOptions) error
	// Roll back the transaction.
	Rollback() error
	// Scan a key range using default scan options.
	Scan(varRange DbKeyRange) (*DbIterator, error)
	// Scan all keys that share the provided prefix.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scan all keys that share the provided prefix using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error)
	// Scan a key range using custom scan options.
	ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error)
	// Return the sequence number visible to this transaction.
	Seqnum() uint64
	// Exclude written keys from conflict tracking.
	UnmarkWrite(keys [][]byte) error
}

// A read-write transaction over a [`crate::Db`].
//
// Transactions can be read from and written to until they are committed or
// rolled back. After completion, all further method calls return an error.
type DbTransaction struct {
	ffiObject FfiObject
}

// Commit the transaction using default write options.
//
// ## Returns
// - `Result<Option<WriteHandle>, SlatedbError>`: metadata for the committed
// write, or `None` if the transaction had no writes.
func (_self *DbTransaction) Commit() (*WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *WriteHandle {
			return FfiConverterOptionalWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_commit(
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

// Commit the transaction using custom write options.
func (_self *DbTransaction) CommitWithOptions(options DbWriteOptions) (*WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *WriteHandle {
			return FfiConverterOptionalWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_commit_with_options(
			_pointer, FfiConverterDbWriteOptionsINSTANCE.Lower(options)),
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

// Buffer a delete inside the transaction.
func (_self *DbTransaction) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_delete(
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

// Get the value for a key using default read options.
func (_self *DbTransaction) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_get(
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

// Get the full row metadata for a key using default read options.
func (_self *DbTransaction) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_get_key_value(
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

// Get the full row metadata for a key using custom read options.
func (_self *DbTransaction) GetKeyValueWithOptions(key []byte, options DbReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Get the value for a key using custom read options.
func (_self *DbTransaction) GetWithOptions(key []byte, options DbReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
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
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterDbReadOptionsINSTANCE.Lower(options)),
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

// Return the unique identifier assigned to this transaction.
func (_self *DbTransaction) Id() string {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_method_dbtransaction_id(
				_pointer, _uniffiStatus),
		}
	}))
}

// Explicitly mark keys as read for conflict detection.
func (_self *DbTransaction) MarkRead(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_mark_read(
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

// Buffer a merge inside the transaction using default options.
func (_self *DbTransaction) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_merge(
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

// Buffer a merge inside the transaction using custom merge options.
func (_self *DbTransaction) MergeWithOptions(key []byte, operand []byte, options DbMergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterDbMergeOptionsINSTANCE.Lower(options)),
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

// Buffer a put inside the transaction using default options.
func (_self *DbTransaction) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_put(
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

// Buffer a put inside the transaction using custom put options.
func (_self *DbTransaction) PutWithOptions(key []byte, value []byte, options DbPutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterDbPutOptionsINSTANCE.Lower(options)),
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

// Roll back the transaction.
func (_self *DbTransaction) Rollback() error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_rollback(
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

// Scan a key range using default scan options.
func (_self *DbTransaction) Scan(varRange DbKeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_scan(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange)),
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

// Scan all keys that share the provided prefix.
func (_self *DbTransaction) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_scan_prefix(
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

// Scan all keys that share the provided prefix using custom scan options.
func (_self *DbTransaction) ScanPrefixWithOptions(prefix []byte, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Scan a key range using custom scan options.
func (_self *DbTransaction) ScanWithOptions(varRange DbKeyRange, options DbScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_scan_with_options(
			_pointer, FfiConverterDbKeyRangeINSTANCE.Lower(varRange), FfiConverterDbScanOptionsINSTANCE.Lower(options)),
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

// Return the sequence number visible to this transaction.
func (_self *DbTransaction) Seqnum() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_dbtransaction_seqnum(
			_pointer, _uniffiStatus)
	}))
}

// Exclude written keys from conflict tracking.
func (_self *DbTransaction) UnmarkWrite(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_ffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_ffi_fn_method_dbtransaction_unmark_write(
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
func (object *DbTransaction) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDbTransaction struct{}

var FfiConverterDbTransactionINSTANCE = FfiConverterDbTransaction{}

func (c FfiConverterDbTransaction) Lift(pointer unsafe.Pointer) *DbTransaction {
	result := &DbTransaction{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_dbtransaction(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_dbtransaction(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbTransaction).Destroy)
	return result
}

func (c FfiConverterDbTransaction) Read(reader io.Reader) *DbTransaction {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterDbTransaction) Lower(value *DbTransaction) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*DbTransaction")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterDbTransaction) Write(writer io.Writer, value *DbTransaction) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerDbTransaction struct{}

func (_ FfiDestroyerDbTransaction) Destroy(value *DbTransaction) {
	value.Destroy()
}

// A resolved object-store handle.
//
// Use [`resolve_object_store`] to create one of these handles and then pass it
// into [`crate::DbBuilder`] for the main database store or the WAL store.
type ObjectStoreInterface interface {
}

// A resolved object-store handle.
//
// Use [`resolve_object_store`] to create one of these handles and then pass it
// into [`crate::DbBuilder`] for the main database store or the WAL store.
type ObjectStore struct {
	ffiObject FfiObject
}

func (object *ObjectStore) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterObjectStore struct{}

var FfiConverterObjectStoreINSTANCE = FfiConverterObjectStore{}

func (c FfiConverterObjectStore) Lift(pointer unsafe.Pointer) *ObjectStore {
	result := &ObjectStore{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_objectstore(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_objectstore(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ObjectStore).Destroy)
	return result
}

func (c FfiConverterObjectStore) Read(reader io.Reader) *ObjectStore {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterObjectStore) Lower(value *ObjectStore) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*ObjectStore")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterObjectStore) Write(writer io.Writer, value *ObjectStore) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerObjectStore struct{}

func (_ FfiDestroyerObjectStore) Destroy(value *ObjectStore) {
	value.Destroy()
}

// A WAL file handle.
type WalFileInterface interface {
	// Close the WAL file handle.
	Close() error
	// Return this file's WAL ID.
	Id() uint64
	// Create an iterator over rows in this WAL file.
	Iterator() (*WalFileIterator, error)
	// Fetch metadata for this WAL file.
	Metadata() (WalFileMetadata, error)
	// Return a handle for the next WAL file after this one.
	NextFile() *WalFile
	// Return the next WAL ID after this file.
	NextId() uint64
}

// A WAL file handle.
type WalFile struct {
	ffiObject FfiObject
}

// Close the WAL file handle.
func (_self *WalFile) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_walfile_close(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Return this file's WAL ID.
func (_self *WalFile) Id() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_walfile_id(
			_pointer, _uniffiStatus)
	}))
}

// Create an iterator over rows in this WAL file.
func (_self *WalFile) Iterator() (*WalFileIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_ffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *WalFileIterator {
			return FfiConverterWalFileIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_walfile_iterator(
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

// Fetch metadata for this WAL file.
func (_self *WalFile) Metadata() (WalFileMetadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WalFileMetadata {
			return FfiConverterWalFileMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_walfile_metadata(
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

// Return a handle for the next WAL file after this one.
func (_self *WalFile) NextFile() *WalFile {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_method_walfile_next_file(
			_pointer, _uniffiStatus)
	}))
}

// Return the next WAL ID after this file.
func (_self *WalFile) NextId() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_ffi_fn_method_walfile_next_id(
			_pointer, _uniffiStatus)
	}))
}
func (object *WalFile) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterWalFile struct{}

var FfiConverterWalFileINSTANCE = FfiConverterWalFile{}

func (c FfiConverterWalFile) Lift(pointer unsafe.Pointer) *WalFile {
	result := &WalFile{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_walfile(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_walfile(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalFile).Destroy)
	return result
}

func (c FfiConverterWalFile) Read(reader io.Reader) *WalFile {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterWalFile) Lower(value *WalFile) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*WalFile")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterWalFile) Write(writer io.Writer, value *WalFile) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerWalFile struct{}

func (_ FfiDestroyerWalFile) Destroy(value *WalFile) {
	value.Destroy()
}

// An iterator over rows in a WAL file.
type WalFileIteratorInterface interface {
	// Close the WAL iterator.
	Close() error
	// Return the next WAL entry, or `None` when the iterator is exhausted.
	Next() (*RowEntry, error)
}

// An iterator over rows in a WAL file.
type WalFileIterator struct {
	ffiObject FfiObject
}

// Close the WAL iterator.
func (_self *WalFileIterator) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*WalFileIterator")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_walfileiterator_close(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Return the next WAL entry, or `None` when the iterator is exhausted.
func (_self *WalFileIterator) Next() (*RowEntry, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFileIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *RowEntry {
			return FfiConverterOptionalRowEntryINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_walfileiterator_next(
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
func (object *WalFileIterator) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterWalFileIterator struct{}

var FfiConverterWalFileIteratorINSTANCE = FfiConverterWalFileIterator{}

func (c FfiConverterWalFileIterator) Lift(pointer unsafe.Pointer) *WalFileIterator {
	result := &WalFileIterator{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_walfileiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_walfileiterator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalFileIterator).Destroy)
	return result
}

func (c FfiConverterWalFileIterator) Read(reader io.Reader) *WalFileIterator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterWalFileIterator) Lower(value *WalFileIterator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*WalFileIterator")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterWalFileIterator) Write(writer io.Writer, value *WalFileIterator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerWalFileIterator struct{}

func (_ FfiDestroyerWalFileIterator) Destroy(value *WalFileIterator) {
	value.Destroy()
}

// A WAL reader scoped to a single database path and object store.
type WalReaderInterface interface {
	// Close the WAL reader.
	Close() error
	// Return a handle for a specific WAL file ID.
	Get(id uint64) *WalFile
	// List WAL files in ascending ID order.
	List(startId *uint64, endId *uint64) ([]*WalFile, error)
}

// A WAL reader scoped to a single database path and object store.
type WalReader struct {
	ffiObject FfiObject
}

// Create a WAL reader for the provided database path and object store.
func NewWalReader(path string, objectStore *ObjectStore) *WalReader {
	return FfiConverterWalReaderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_walreader_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Close the WAL reader.
func (_self *WalReader) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_walreader_close(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Return a handle for a specific WAL file ID.
func (_self *WalReader) Get(id uint64) *WalFile {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_method_walreader_get(
			_pointer, FfiConverterUint64INSTANCE.Lower(id), _uniffiStatus)
	}))
}

// List WAL files in ascending ID order.
func (_self *WalReader) List(startId *uint64, endId *uint64) ([]*WalFile, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[SlatedbError](
		FfiConverterSlatedbErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_ffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []*WalFile {
			return FfiConverterSequenceWalFileINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_ffi_fn_method_walreader_list(
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
func (object *WalReader) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterWalReader struct{}

var FfiConverterWalReaderINSTANCE = FfiConverterWalReader{}

func (c FfiConverterWalReader) Lift(pointer unsafe.Pointer) *WalReader {
	result := &WalReader{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_walreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_walreader(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalReader).Destroy)
	return result
}

func (c FfiConverterWalReader) Read(reader io.Reader) *WalReader {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterWalReader) Lower(value *WalReader) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*WalReader")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterWalReader) Write(writer io.Writer, value *WalReader) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerWalReader struct{}

func (_ FfiDestroyerWalReader) Destroy(value *WalReader) {
	value.Destroy()
}

// A mutable batch of write operations that can be written atomically.
type WriteBatchInterface interface {
	// Explicitly close the batch handle.
	Close() error
	// Append a delete operation.
	Delete(key []byte) error
	// Append a merge operation using default merge options.
	Merge(key []byte, operand []byte) error
	// Append a merge operation using explicit merge options.
	MergeWithOptions(key []byte, operand []byte, options DbMergeOptions) error
	// Append a put operation using default put options.
	Put(key []byte, value []byte) error
	// Append a put operation using explicit put options.
	PutWithOptions(key []byte, value []byte, options DbPutOptions) error
}

// A mutable batch of write operations that can be written atomically.
type WriteBatch struct {
	ffiObject FfiObject
}

// Create a new empty write batch.
func NewWriteBatch() *WriteBatch {
	return FfiConverterWriteBatchINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_constructor_writebatch_new(_uniffiStatus)
	}))
}

// Explicitly close the batch handle.
func (_self *WriteBatch) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_close(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Append a delete operation.
func (_self *WriteBatch) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Append a merge operation using default merge options.
func (_self *WriteBatch) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Append a merge operation using explicit merge options.
func (_self *WriteBatch) MergeWithOptions(key []byte, operand []byte, options DbMergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterDbMergeOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Append a put operation using default put options.
func (_self *WriteBatch) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Append a put operation using explicit put options.
func (_self *WriteBatch) PutWithOptions(key []byte, value []byte, options DbPutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_method_writebatch_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterDbPutOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *WriteBatch) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterWriteBatch struct{}

var FfiConverterWriteBatchINSTANCE = FfiConverterWriteBatch{}

func (c FfiConverterWriteBatch) Lift(pointer unsafe.Pointer) *WriteBatch {
	result := &WriteBatch{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_ffi_fn_clone_writebatch(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_ffi_fn_free_writebatch(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WriteBatch).Destroy)
	return result
}

func (c FfiConverterWriteBatch) Read(reader io.Reader) *WriteBatch {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterWriteBatch) Lower(value *WriteBatch) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*WriteBatch")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterWriteBatch) Write(writer io.Writer, value *WriteBatch) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerWriteBatch struct{}

func (_ FfiDestroyerWriteBatch) Destroy(value *WriteBatch) {
	value.Destroy()
}

// Options for manual flushes.
type DbFlushOptions struct {
	// The flush mode to execute.
	FlushType FlushType
}

func (r *DbFlushOptions) Destroy() {
	FfiDestroyerFlushType{}.Destroy(r.FlushType)
}

type FfiConverterDbFlushOptions struct{}

var FfiConverterDbFlushOptionsINSTANCE = FfiConverterDbFlushOptions{}

func (c FfiConverterDbFlushOptions) Lift(rb RustBufferI) DbFlushOptions {
	return LiftFromRustBuffer[DbFlushOptions](c, rb)
}

func (c FfiConverterDbFlushOptions) Read(reader io.Reader) DbFlushOptions {
	return DbFlushOptions{
		FfiConverterFlushTypeINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbFlushOptions) Lower(value DbFlushOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbFlushOptions](c, value)
}

func (c FfiConverterDbFlushOptions) LowerExternal(value DbFlushOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbFlushOptions](c, value))
}

func (c FfiConverterDbFlushOptions) Write(writer io.Writer, value DbFlushOptions) {
	FfiConverterFlushTypeINSTANCE.Write(writer, value.FlushType)
}

type FfiDestroyerDbFlushOptions struct{}

func (_ FfiDestroyerDbFlushOptions) Destroy(value DbFlushOptions) {
	value.Destroy()
}

// A range of keys used for scans.
type DbKeyRange struct {
	// The optional lower bound of the range.
	Start *[]byte
	// Whether the lower bound is inclusive.
	StartInclusive bool
	// The optional upper bound of the range.
	End *[]byte
	// Whether the upper bound is inclusive.
	EndInclusive bool
}

func (r *DbKeyRange) Destroy() {
	FfiDestroyerOptionalBytes{}.Destroy(r.Start)
	FfiDestroyerBool{}.Destroy(r.StartInclusive)
	FfiDestroyerOptionalBytes{}.Destroy(r.End)
	FfiDestroyerBool{}.Destroy(r.EndInclusive)
}

type FfiConverterDbKeyRange struct{}

var FfiConverterDbKeyRangeINSTANCE = FfiConverterDbKeyRange{}

func (c FfiConverterDbKeyRange) Lift(rb RustBufferI) DbKeyRange {
	return LiftFromRustBuffer[DbKeyRange](c, rb)
}

func (c FfiConverterDbKeyRange) Read(reader io.Reader) DbKeyRange {
	return DbKeyRange{
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbKeyRange) Lower(value DbKeyRange) C.RustBuffer {
	return LowerIntoRustBuffer[DbKeyRange](c, value)
}

func (c FfiConverterDbKeyRange) LowerExternal(value DbKeyRange) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbKeyRange](c, value))
}

func (c FfiConverterDbKeyRange) Write(writer io.Writer, value DbKeyRange) {
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.Start)
	FfiConverterBoolINSTANCE.Write(writer, value.StartInclusive)
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.End)
	FfiConverterBoolINSTANCE.Write(writer, value.EndInclusive)
}

type FfiDestroyerDbKeyRange struct{}

func (_ FfiDestroyerDbKeyRange) Destroy(value DbKeyRange) {
	value.Destroy()
}

// Options for merge operations.
type DbMergeOptions struct {
	// TTL to apply to the merged value.
	Ttl Ttl
}

func (r *DbMergeOptions) Destroy() {
	FfiDestroyerTtl{}.Destroy(r.Ttl)
}

type FfiConverterDbMergeOptions struct{}

var FfiConverterDbMergeOptionsINSTANCE = FfiConverterDbMergeOptions{}

func (c FfiConverterDbMergeOptions) Lift(rb RustBufferI) DbMergeOptions {
	return LiftFromRustBuffer[DbMergeOptions](c, rb)
}

func (c FfiConverterDbMergeOptions) Read(reader io.Reader) DbMergeOptions {
	return DbMergeOptions{
		FfiConverterTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbMergeOptions) Lower(value DbMergeOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbMergeOptions](c, value)
}

func (c FfiConverterDbMergeOptions) LowerExternal(value DbMergeOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbMergeOptions](c, value))
}

func (c FfiConverterDbMergeOptions) Write(writer io.Writer, value DbMergeOptions) {
	FfiConverterTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerDbMergeOptions struct{}

func (_ FfiDestroyerDbMergeOptions) Destroy(value DbMergeOptions) {
	value.Destroy()
}

// Options for put operations.
type DbPutOptions struct {
	// TTL to apply to the written value.
	Ttl Ttl
}

func (r *DbPutOptions) Destroy() {
	FfiDestroyerTtl{}.Destroy(r.Ttl)
}

type FfiConverterDbPutOptions struct{}

var FfiConverterDbPutOptionsINSTANCE = FfiConverterDbPutOptions{}

func (c FfiConverterDbPutOptions) Lift(rb RustBufferI) DbPutOptions {
	return LiftFromRustBuffer[DbPutOptions](c, rb)
}

func (c FfiConverterDbPutOptions) Read(reader io.Reader) DbPutOptions {
	return DbPutOptions{
		FfiConverterTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbPutOptions) Lower(value DbPutOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbPutOptions](c, value)
}

func (c FfiConverterDbPutOptions) LowerExternal(value DbPutOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbPutOptions](c, value))
}

func (c FfiConverterDbPutOptions) Write(writer io.Writer, value DbPutOptions) {
	FfiConverterTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerDbPutOptions struct{}

func (_ FfiDestroyerDbPutOptions) Destroy(value DbPutOptions) {
	value.Destroy()
}

// Options for point reads.
type DbReadOptions struct {
	// The durability level that the read must observe.
	DurabilityFilter DurabilityLevel
	// Whether dirty state may be returned.
	Dirty bool
	// Whether fetched blocks should be inserted into the cache.
	CacheBlocks bool
}

func (r *DbReadOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
}

type FfiConverterDbReadOptions struct{}

var FfiConverterDbReadOptionsINSTANCE = FfiConverterDbReadOptions{}

func (c FfiConverterDbReadOptions) Lift(rb RustBufferI) DbReadOptions {
	return LiftFromRustBuffer[DbReadOptions](c, rb)
}

func (c FfiConverterDbReadOptions) Read(reader io.Reader) DbReadOptions {
	return DbReadOptions{
		FfiConverterDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbReadOptions) Lower(value DbReadOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbReadOptions](c, value)
}

func (c FfiConverterDbReadOptions) LowerExternal(value DbReadOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbReadOptions](c, value))
}

func (c FfiConverterDbReadOptions) Write(writer io.Writer, value DbReadOptions) {
	FfiConverterDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
}

type FfiDestroyerDbReadOptions struct{}

func (_ FfiDestroyerDbReadOptions) Destroy(value DbReadOptions) {
	value.Destroy()
}

// Options for constructing a read-only database reader.
type DbReaderOptions struct {
	// How often to poll manifests and WALs for refreshed reader state.
	ManifestPollIntervalMs uint64
	// How long reader-owned checkpoints should remain valid.
	CheckpointLifetimeMs uint64
	// Maximum WAL replay memtable size in bytes.
	MaxMemtableBytes uint64
	// Whether WAL replay should be skipped entirely.
	SkipWalReplay bool
}

func (r *DbReaderOptions) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.ManifestPollIntervalMs)
	FfiDestroyerUint64{}.Destroy(r.CheckpointLifetimeMs)
	FfiDestroyerUint64{}.Destroy(r.MaxMemtableBytes)
	FfiDestroyerBool{}.Destroy(r.SkipWalReplay)
}

type FfiConverterDbReaderOptions struct{}

var FfiConverterDbReaderOptionsINSTANCE = FfiConverterDbReaderOptions{}

func (c FfiConverterDbReaderOptions) Lift(rb RustBufferI) DbReaderOptions {
	return LiftFromRustBuffer[DbReaderOptions](c, rb)
}

func (c FfiConverterDbReaderOptions) Read(reader io.Reader) DbReaderOptions {
	return DbReaderOptions{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbReaderOptions) Lower(value DbReaderOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbReaderOptions](c, value)
}

func (c FfiConverterDbReaderOptions) LowerExternal(value DbReaderOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbReaderOptions](c, value))
}

func (c FfiConverterDbReaderOptions) Write(writer io.Writer, value DbReaderOptions) {
	FfiConverterUint64INSTANCE.Write(writer, value.ManifestPollIntervalMs)
	FfiConverterUint64INSTANCE.Write(writer, value.CheckpointLifetimeMs)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxMemtableBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.SkipWalReplay)
}

type FfiDestroyerDbReaderOptions struct{}

func (_ FfiDestroyerDbReaderOptions) Destroy(value DbReaderOptions) {
	value.Destroy()
}

// Options for range scans and prefix scans.
type DbScanOptions struct {
	// The durability level that the scan must observe.
	DurabilityFilter DurabilityLevel
	// Whether dirty state may be returned.
	Dirty bool
	// The number of bytes to read ahead while scanning.
	ReadAheadBytes uint64
	// Whether fetched blocks should be inserted into the cache.
	CacheBlocks bool
	// The maximum number of background fetch tasks.
	MaxFetchTasks uint64
}

func (r *DbScanOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerUint64{}.Destroy(r.ReadAheadBytes)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
	FfiDestroyerUint64{}.Destroy(r.MaxFetchTasks)
}

type FfiConverterDbScanOptions struct{}

var FfiConverterDbScanOptionsINSTANCE = FfiConverterDbScanOptions{}

func (c FfiConverterDbScanOptions) Lift(rb RustBufferI) DbScanOptions {
	return LiftFromRustBuffer[DbScanOptions](c, rb)
}

func (c FfiConverterDbScanOptions) Read(reader io.Reader) DbScanOptions {
	return DbScanOptions{
		FfiConverterDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterDbScanOptions) Lower(value DbScanOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbScanOptions](c, value)
}

func (c FfiConverterDbScanOptions) LowerExternal(value DbScanOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbScanOptions](c, value))
}

func (c FfiConverterDbScanOptions) Write(writer io.Writer, value DbScanOptions) {
	FfiConverterDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterUint64INSTANCE.Write(writer, value.ReadAheadBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxFetchTasks)
}

type FfiDestroyerDbScanOptions struct{}

func (_ FfiDestroyerDbScanOptions) Destroy(value DbScanOptions) {
	value.Destroy()
}

// Options that control write durability.
type DbWriteOptions struct {
	// Whether the call should wait for the write to become durable.
	AwaitDurable bool
}

func (r *DbWriteOptions) Destroy() {
	FfiDestroyerBool{}.Destroy(r.AwaitDurable)
}

type FfiConverterDbWriteOptions struct{}

var FfiConverterDbWriteOptionsINSTANCE = FfiConverterDbWriteOptions{}

func (c FfiConverterDbWriteOptions) Lift(rb RustBufferI) DbWriteOptions {
	return LiftFromRustBuffer[DbWriteOptions](c, rb)
}

func (c FfiConverterDbWriteOptions) Read(reader io.Reader) DbWriteOptions {
	return DbWriteOptions{
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterDbWriteOptions) Lower(value DbWriteOptions) C.RustBuffer {
	return LowerIntoRustBuffer[DbWriteOptions](c, value)
}

func (c FfiConverterDbWriteOptions) LowerExternal(value DbWriteOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbWriteOptions](c, value))
}

func (c FfiConverterDbWriteOptions) Write(writer io.Writer, value DbWriteOptions) {
	FfiConverterBoolINSTANCE.Write(writer, value.AwaitDurable)
}

type FfiDestroyerDbWriteOptions struct{}

func (_ FfiDestroyerDbWriteOptions) Destroy(value DbWriteOptions) {
	value.Destroy()
}

// A key-value pair returned by reads and iterators.
type KeyValue struct {
	// The row key.
	Key []byte
	// The row value.
	Value []byte
	// The sequence number that produced this row.
	Seq uint64
	// The creation timestamp assigned by SlateDB.
	CreateTs int64
	// The optional expiry timestamp assigned by SlateDB.
	ExpireTs *int64
}

func (r *KeyValue) Destroy() {
	FfiDestroyerBytes{}.Destroy(r.Key)
	FfiDestroyerBytes{}.Destroy(r.Value)
	FfiDestroyerUint64{}.Destroy(r.Seq)
	FfiDestroyerInt64{}.Destroy(r.CreateTs)
	FfiDestroyerOptionalInt64{}.Destroy(r.ExpireTs)
}

type FfiConverterKeyValue struct{}

var FfiConverterKeyValueINSTANCE = FfiConverterKeyValue{}

func (c FfiConverterKeyValue) Lift(rb RustBufferI) KeyValue {
	return LiftFromRustBuffer[KeyValue](c, rb)
}

func (c FfiConverterKeyValue) Read(reader io.Reader) KeyValue {
	return KeyValue{
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterInt64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterKeyValue) Lower(value KeyValue) C.RustBuffer {
	return LowerIntoRustBuffer[KeyValue](c, value)
}

func (c FfiConverterKeyValue) LowerExternal(value KeyValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[KeyValue](c, value))
}

func (c FfiConverterKeyValue) Write(writer io.Writer, value KeyValue) {
	FfiConverterBytesINSTANCE.Write(writer, value.Key)
	FfiConverterBytesINSTANCE.Write(writer, value.Value)
	FfiConverterUint64INSTANCE.Write(writer, value.Seq)
	FfiConverterInt64INSTANCE.Write(writer, value.CreateTs)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.ExpireTs)
}

type FfiDestroyerKeyValue struct{}

func (_ FfiDestroyerKeyValue) Destroy(value KeyValue) {
	value.Destroy()
}

// A row entry returned by WAL iteration.
type RowEntry struct {
	Kind     RowEntryKind
	Key      []byte
	Value    *[]byte
	Seq      uint64
	CreateTs *int64
	ExpireTs *int64
}

func (r *RowEntry) Destroy() {
	FfiDestroyerRowEntryKind{}.Destroy(r.Kind)
	FfiDestroyerBytes{}.Destroy(r.Key)
	FfiDestroyerOptionalBytes{}.Destroy(r.Value)
	FfiDestroyerUint64{}.Destroy(r.Seq)
	FfiDestroyerOptionalInt64{}.Destroy(r.CreateTs)
	FfiDestroyerOptionalInt64{}.Destroy(r.ExpireTs)
}

type FfiConverterRowEntry struct{}

var FfiConverterRowEntryINSTANCE = FfiConverterRowEntry{}

func (c FfiConverterRowEntry) Lift(rb RustBufferI) RowEntry {
	return LiftFromRustBuffer[RowEntry](c, rb)
}

func (c FfiConverterRowEntry) Read(reader io.Reader) RowEntry {
	return RowEntry{
		FfiConverterRowEntryKindINSTANCE.Read(reader),
		FfiConverterBytesINSTANCE.Read(reader),
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
		FfiConverterOptionalInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterRowEntry) Lower(value RowEntry) C.RustBuffer {
	return LowerIntoRustBuffer[RowEntry](c, value)
}

func (c FfiConverterRowEntry) LowerExternal(value RowEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[RowEntry](c, value))
}

func (c FfiConverterRowEntry) Write(writer io.Writer, value RowEntry) {
	FfiConverterRowEntryKindINSTANCE.Write(writer, value.Kind)
	FfiConverterBytesINSTANCE.Write(writer, value.Key)
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.Value)
	FfiConverterUint64INSTANCE.Write(writer, value.Seq)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.CreateTs)
	FfiConverterOptionalInt64INSTANCE.Write(writer, value.ExpireTs)
}

type FfiDestroyerRowEntry struct{}

func (_ FfiDestroyerRowEntry) Destroy(value RowEntry) {
	value.Destroy()
}

// Metadata for a single WAL file.
type WalFileMetadata struct {
	LastModifiedSeconds int64
	LastModifiedNanos   uint32
	SizeBytes           uint64
	Location            string
}

func (r *WalFileMetadata) Destroy() {
	FfiDestroyerInt64{}.Destroy(r.LastModifiedSeconds)
	FfiDestroyerUint32{}.Destroy(r.LastModifiedNanos)
	FfiDestroyerUint64{}.Destroy(r.SizeBytes)
	FfiDestroyerString{}.Destroy(r.Location)
}

type FfiConverterWalFileMetadata struct{}

var FfiConverterWalFileMetadataINSTANCE = FfiConverterWalFileMetadata{}

func (c FfiConverterWalFileMetadata) Lift(rb RustBufferI) WalFileMetadata {
	return LiftFromRustBuffer[WalFileMetadata](c, rb)
}

func (c FfiConverterWalFileMetadata) Read(reader io.Reader) WalFileMetadata {
	return WalFileMetadata{
		FfiConverterInt64INSTANCE.Read(reader),
		FfiConverterUint32INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterWalFileMetadata) Lower(value WalFileMetadata) C.RustBuffer {
	return LowerIntoRustBuffer[WalFileMetadata](c, value)
}

func (c FfiConverterWalFileMetadata) LowerExternal(value WalFileMetadata) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[WalFileMetadata](c, value))
}

func (c FfiConverterWalFileMetadata) Write(writer io.Writer, value WalFileMetadata) {
	FfiConverterInt64INSTANCE.Write(writer, value.LastModifiedSeconds)
	FfiConverterUint32INSTANCE.Write(writer, value.LastModifiedNanos)
	FfiConverterUint64INSTANCE.Write(writer, value.SizeBytes)
	FfiConverterStringINSTANCE.Write(writer, value.Location)
}

type FfiDestroyerWalFileMetadata struct{}

func (_ FfiDestroyerWalFileMetadata) Destroy(value WalFileMetadata) {
	value.Destroy()
}

// Metadata returned from a successful write.
type WriteHandle struct {
	// The sequence number assigned to the write.
	Seqnum uint64
	// The creation timestamp assigned to the write.
	CreateTs int64
}

func (r *WriteHandle) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.Seqnum)
	FfiDestroyerInt64{}.Destroy(r.CreateTs)
}

type FfiConverterWriteHandle struct{}

var FfiConverterWriteHandleINSTANCE = FfiConverterWriteHandle{}

func (c FfiConverterWriteHandle) Lift(rb RustBufferI) WriteHandle {
	return LiftFromRustBuffer[WriteHandle](c, rb)
}

func (c FfiConverterWriteHandle) Read(reader io.Reader) WriteHandle {
	return WriteHandle{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterInt64INSTANCE.Read(reader),
	}
}

func (c FfiConverterWriteHandle) Lower(value WriteHandle) C.RustBuffer {
	return LowerIntoRustBuffer[WriteHandle](c, value)
}

func (c FfiConverterWriteHandle) LowerExternal(value WriteHandle) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[WriteHandle](c, value))
}

func (c FfiConverterWriteHandle) Write(writer io.Writer, value WriteHandle) {
	FfiConverterUint64INSTANCE.Write(writer, value.Seqnum)
	FfiConverterInt64INSTANCE.Write(writer, value.CreateTs)
}

type FfiDestroyerWriteHandle struct{}

func (_ FfiDestroyerWriteHandle) Destroy(value WriteHandle) {
	value.Destroy()
}

// The reason a database handle was closed.
type CloseReason uint

const (
	// No close reason is available.
	CloseReasonNone CloseReason = 1
	// The database was closed cleanly.
	CloseReasonClean CloseReason = 2
	// The database instance was fenced by another writer.
	CloseReasonFenced CloseReason = 3
	// A background task panicked.
	CloseReasonPanic CloseReason = 4
	// The close reason was not recognized by this binding version.
	CloseReasonUnknown CloseReason = 5
)

type FfiConverterCloseReason struct{}

var FfiConverterCloseReasonINSTANCE = FfiConverterCloseReason{}

func (c FfiConverterCloseReason) Lift(rb RustBufferI) CloseReason {
	return LiftFromRustBuffer[CloseReason](c, rb)
}

func (c FfiConverterCloseReason) Lower(value CloseReason) C.RustBuffer {
	return LowerIntoRustBuffer[CloseReason](c, value)
}

func (c FfiConverterCloseReason) LowerExternal(value CloseReason) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[CloseReason](c, value))
}
func (FfiConverterCloseReason) Read(reader io.Reader) CloseReason {
	id := readInt32(reader)
	return CloseReason(id)
}

func (FfiConverterCloseReason) Write(writer io.Writer, value CloseReason) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerCloseReason struct{}

func (_ FfiDestroyerCloseReason) Destroy(value CloseReason) {
}

// A single operation in a batch write.
type DbWriteOperation interface {
	Destroy()
}

// Put a value for a key.
type DbWriteOperationPut struct {
	Key        []byte
	ValueBytes []byte
	Options    DbPutOptions
}

func (e DbWriteOperationPut) Destroy() {
	FfiDestroyerBytes{}.Destroy(e.Key)
	FfiDestroyerBytes{}.Destroy(e.ValueBytes)
	FfiDestroyerDbPutOptions{}.Destroy(e.Options)
}

// Merge an operand into a key.
type DbWriteOperationMerge struct {
	Key     []byte
	Operand []byte
	Options DbMergeOptions
}

func (e DbWriteOperationMerge) Destroy() {
	FfiDestroyerBytes{}.Destroy(e.Key)
	FfiDestroyerBytes{}.Destroy(e.Operand)
	FfiDestroyerDbMergeOptions{}.Destroy(e.Options)
}

// Delete a key.
type DbWriteOperationDelete struct {
	Key []byte
}

func (e DbWriteOperationDelete) Destroy() {
	FfiDestroyerBytes{}.Destroy(e.Key)
}

type FfiConverterDbWriteOperation struct{}

var FfiConverterDbWriteOperationINSTANCE = FfiConverterDbWriteOperation{}

func (c FfiConverterDbWriteOperation) Lift(rb RustBufferI) DbWriteOperation {
	return LiftFromRustBuffer[DbWriteOperation](c, rb)
}

func (c FfiConverterDbWriteOperation) Lower(value DbWriteOperation) C.RustBuffer {
	return LowerIntoRustBuffer[DbWriteOperation](c, value)
}

func (c FfiConverterDbWriteOperation) LowerExternal(value DbWriteOperation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DbWriteOperation](c, value))
}
func (FfiConverterDbWriteOperation) Read(reader io.Reader) DbWriteOperation {
	id := readInt32(reader)
	switch id {
	case 1:
		return DbWriteOperationPut{
			FfiConverterBytesINSTANCE.Read(reader),
			FfiConverterBytesINSTANCE.Read(reader),
			FfiConverterDbPutOptionsINSTANCE.Read(reader),
		}
	case 2:
		return DbWriteOperationMerge{
			FfiConverterBytesINSTANCE.Read(reader),
			FfiConverterBytesINSTANCE.Read(reader),
			FfiConverterDbMergeOptionsINSTANCE.Read(reader),
		}
	case 3:
		return DbWriteOperationDelete{
			FfiConverterBytesINSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterDbWriteOperation.Read()", id))
	}
}

func (FfiConverterDbWriteOperation) Write(writer io.Writer, value DbWriteOperation) {
	switch variant_value := value.(type) {
	case DbWriteOperationPut:
		writeInt32(writer, 1)
		FfiConverterBytesINSTANCE.Write(writer, variant_value.Key)
		FfiConverterBytesINSTANCE.Write(writer, variant_value.ValueBytes)
		FfiConverterDbPutOptionsINSTANCE.Write(writer, variant_value.Options)
	case DbWriteOperationMerge:
		writeInt32(writer, 2)
		FfiConverterBytesINSTANCE.Write(writer, variant_value.Key)
		FfiConverterBytesINSTANCE.Write(writer, variant_value.Operand)
		FfiConverterDbMergeOptionsINSTANCE.Write(writer, variant_value.Options)
	case DbWriteOperationDelete:
		writeInt32(writer, 3)
		FfiConverterBytesINSTANCE.Write(writer, variant_value.Key)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterDbWriteOperation.Write", value))
	}
}

type FfiDestroyerDbWriteOperation struct{}

func (_ FfiDestroyerDbWriteOperation) Destroy(value DbWriteOperation) {
	value.Destroy()
}

// Controls which durability level reads are allowed to observe.
type DurabilityLevel uint

const (
	// Return only data durable in remote object storage.
	DurabilityLevelRemote DurabilityLevel = 1
	// Return the latest visible data, including in-memory state.
	DurabilityLevelMemory DurabilityLevel = 2
)

type FfiConverterDurabilityLevel struct{}

var FfiConverterDurabilityLevelINSTANCE = FfiConverterDurabilityLevel{}

func (c FfiConverterDurabilityLevel) Lift(rb RustBufferI) DurabilityLevel {
	return LiftFromRustBuffer[DurabilityLevel](c, rb)
}

func (c FfiConverterDurabilityLevel) Lower(value DurabilityLevel) C.RustBuffer {
	return LowerIntoRustBuffer[DurabilityLevel](c, value)
}

func (c FfiConverterDurabilityLevel) LowerExternal(value DurabilityLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[DurabilityLevel](c, value))
}
func (FfiConverterDurabilityLevel) Read(reader io.Reader) DurabilityLevel {
	id := readInt32(reader)
	return DurabilityLevel(id)
}

func (FfiConverterDurabilityLevel) Write(writer io.Writer, value DurabilityLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerDurabilityLevel struct{}

func (_ FfiDestroyerDurabilityLevel) Destroy(value DurabilityLevel) {
}

// Selects which in-memory structures should be flushed.
type FlushType uint

const (
	// Flush the memtable contents.
	FlushTypeMemTable FlushType = 1
	// Flush the WAL contents.
	FlushTypeWal FlushType = 2
)

type FfiConverterFlushType struct{}

var FfiConverterFlushTypeINSTANCE = FfiConverterFlushType{}

func (c FfiConverterFlushType) Lift(rb RustBufferI) FlushType {
	return LiftFromRustBuffer[FlushType](c, rb)
}

func (c FfiConverterFlushType) Lower(value FlushType) C.RustBuffer {
	return LowerIntoRustBuffer[FlushType](c, value)
}

func (c FfiConverterFlushType) LowerExternal(value FlushType) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FlushType](c, value))
}
func (FfiConverterFlushType) Read(reader io.Reader) FlushType {
	id := readInt32(reader)
	return FlushType(id)
}

func (FfiConverterFlushType) Write(writer io.Writer, value FlushType) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerFlushType struct{}

func (_ FfiDestroyerFlushType) Destroy(value FlushType) {
}

// Isolation level used when starting a transaction.
type IsolationLevel uint

const (
	// Snapshot isolation.
	IsolationLevelSnapshot IsolationLevel = 1
	// Serializable snapshot isolation.
	IsolationLevelSerializableSnapshot IsolationLevel = 2
)

type FfiConverterIsolationLevel struct{}

var FfiConverterIsolationLevelINSTANCE = FfiConverterIsolationLevel{}

func (c FfiConverterIsolationLevel) Lift(rb RustBufferI) IsolationLevel {
	return LiftFromRustBuffer[IsolationLevel](c, rb)
}

func (c FfiConverterIsolationLevel) Lower(value IsolationLevel) C.RustBuffer {
	return LowerIntoRustBuffer[IsolationLevel](c, value)
}

func (c FfiConverterIsolationLevel) LowerExternal(value IsolationLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[IsolationLevel](c, value))
}
func (FfiConverterIsolationLevel) Read(reader io.Reader) IsolationLevel {
	id := readInt32(reader)
	return IsolationLevel(id)
}

func (FfiConverterIsolationLevel) Write(writer io.Writer, value IsolationLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerIsolationLevel struct{}

func (_ FfiDestroyerIsolationLevel) Destroy(value IsolationLevel) {
}

// The available logging levels.
type LogLevel uint

const (
	LogLevelTrace LogLevel = 1
	LogLevelDebug LogLevel = 2
	LogLevelInfo  LogLevel = 3
	LogLevelWarn  LogLevel = 4
	LogLevelError LogLevel = 5
)

type FfiConverterLogLevel struct{}

var FfiConverterLogLevelINSTANCE = FfiConverterLogLevel{}

func (c FfiConverterLogLevel) Lift(rb RustBufferI) LogLevel {
	return LiftFromRustBuffer[LogLevel](c, rb)
}

func (c FfiConverterLogLevel) Lower(value LogLevel) C.RustBuffer {
	return LowerIntoRustBuffer[LogLevel](c, value)
}

func (c FfiConverterLogLevel) LowerExternal(value LogLevel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[LogLevel](c, value))
}
func (FfiConverterLogLevel) Read(reader io.Reader) LogLevel {
	id := readInt32(reader)
	return LogLevel(id)
}

func (FfiConverterLogLevel) Write(writer io.Writer, value LogLevel) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerLogLevel struct{}

func (_ FfiDestroyerLogLevel) Destroy(value LogLevel) {
}

// Error returned by foreign merge operator callbacks.
type MergeOperatorCallbackError struct {
	err error
}

// Convience method to turn *MergeOperatorCallbackError into error
// Avoiding treating nil pointer as non nil error interface
func (err *MergeOperatorCallbackError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err MergeOperatorCallbackError) Error() string {
	return fmt.Sprintf("MergeOperatorCallbackError: %s", err.err.Error())
}

func (err MergeOperatorCallbackError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrMergeOperatorCallbackErrorFailed = fmt.Errorf("MergeOperatorCallbackErrorFailed")

// Variant structs
// The merge operator rejected the input or could not produce a merged value.
type MergeOperatorCallbackErrorFailed struct {
	Message string
}

// The merge operator rejected the input or could not produce a merged value.
func NewMergeOperatorCallbackErrorFailed(
	message string,
) *MergeOperatorCallbackError {
	return &MergeOperatorCallbackError{err: &MergeOperatorCallbackErrorFailed{
		Message: message}}
}

func (e MergeOperatorCallbackErrorFailed) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err MergeOperatorCallbackErrorFailed) Error() string {
	return fmt.Sprint("Failed",
		": ",

		"Message=",
		err.Message,
	)
}

func (self MergeOperatorCallbackErrorFailed) Is(target error) bool {
	return target == ErrMergeOperatorCallbackErrorFailed
}

type FfiConverterMergeOperatorCallbackError struct{}

var FfiConverterMergeOperatorCallbackErrorINSTANCE = FfiConverterMergeOperatorCallbackError{}

func (c FfiConverterMergeOperatorCallbackError) Lift(eb RustBufferI) *MergeOperatorCallbackError {
	return LiftFromRustBuffer[*MergeOperatorCallbackError](c, eb)
}

func (c FfiConverterMergeOperatorCallbackError) Lower(value *MergeOperatorCallbackError) C.RustBuffer {
	return LowerIntoRustBuffer[*MergeOperatorCallbackError](c, value)
}

func (c FfiConverterMergeOperatorCallbackError) LowerExternal(value *MergeOperatorCallbackError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*MergeOperatorCallbackError](c, value))
}

func (c FfiConverterMergeOperatorCallbackError) Read(reader io.Reader) *MergeOperatorCallbackError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &MergeOperatorCallbackError{&MergeOperatorCallbackErrorFailed{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterMergeOperatorCallbackError.Read()", errorID))
	}
}

func (c FfiConverterMergeOperatorCallbackError) Write(writer io.Writer, value *MergeOperatorCallbackError) {
	switch variantValue := value.err.(type) {
	case *MergeOperatorCallbackErrorFailed:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterMergeOperatorCallbackError.Write", value))
	}
}

type FfiDestroyerMergeOperatorCallbackError struct{}

func (_ FfiDestroyerMergeOperatorCallbackError) Destroy(value *MergeOperatorCallbackError) {
	switch variantValue := value.err.(type) {
	case MergeOperatorCallbackErrorFailed:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerMergeOperatorCallbackError.Destroy", value))
	}
}

// The kind of entry stored in a WAL row.
type RowEntryKind uint

const (
	RowEntryKindValue     RowEntryKind = 1
	RowEntryKindTombstone RowEntryKind = 2
	RowEntryKindMerge     RowEntryKind = 3
)

type FfiConverterRowEntryKind struct{}

var FfiConverterRowEntryKindINSTANCE = FfiConverterRowEntryKind{}

func (c FfiConverterRowEntryKind) Lift(rb RustBufferI) RowEntryKind {
	return LiftFromRustBuffer[RowEntryKind](c, rb)
}

func (c FfiConverterRowEntryKind) Lower(value RowEntryKind) C.RustBuffer {
	return LowerIntoRustBuffer[RowEntryKind](c, value)
}

func (c FfiConverterRowEntryKind) LowerExternal(value RowEntryKind) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[RowEntryKind](c, value))
}
func (FfiConverterRowEntryKind) Read(reader io.Reader) RowEntryKind {
	id := readInt32(reader)
	return RowEntryKind(id)
}

func (FfiConverterRowEntryKind) Write(writer io.Writer, value RowEntryKind) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerRowEntryKind struct{}

func (_ FfiDestroyerRowEntryKind) Destroy(value RowEntryKind) {
}

// Error returned by the SlateDB FFI layer.
//
// The FFI wrapper groups core SlateDB errors into a smaller set of stable
// categories while preserving the original message text.
type SlatedbError struct {
	err error
}

// Convience method to turn *SlatedbError into error
// Avoiding treating nil pointer as non nil error interface
func (err *SlatedbError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err SlatedbError) Error() string {
	return fmt.Sprintf("SlatedbError: %s", err.err.Error())
}

func (err SlatedbError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrSlatedbErrorTransaction = fmt.Errorf("SlatedbErrorTransaction")
var ErrSlatedbErrorClosed = fmt.Errorf("SlatedbErrorClosed")
var ErrSlatedbErrorUnavailable = fmt.Errorf("SlatedbErrorUnavailable")
var ErrSlatedbErrorInvalid = fmt.Errorf("SlatedbErrorInvalid")
var ErrSlatedbErrorData = fmt.Errorf("SlatedbErrorData")
var ErrSlatedbErrorInternal = fmt.Errorf("SlatedbErrorInternal")

// Variant structs
// A transaction failed to commit or otherwise encountered a conflict.
type SlatedbErrorTransaction struct {
	Message string
}

// A transaction failed to commit or otherwise encountered a conflict.
func NewSlatedbErrorTransaction(
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorTransaction{
		Message: message}}
}

func (e SlatedbErrorTransaction) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorTransaction) Error() string {
	return fmt.Sprint("Transaction",
		": ",

		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorTransaction) Is(target error) bool {
	return target == ErrSlatedbErrorTransaction
}

// The database or transaction handle has already been closed.
type SlatedbErrorClosed struct {
	Reason  CloseReason
	Message string
}

// The database or transaction handle has already been closed.
func NewSlatedbErrorClosed(
	reason CloseReason,
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorClosed{
		Reason:  reason,
		Message: message}}
}

func (e SlatedbErrorClosed) destroy() {
	FfiDestroyerCloseReason{}.Destroy(e.Reason)
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorClosed) Error() string {
	return fmt.Sprint("Closed",
		": ",

		"Reason=",
		err.Reason,
		", ",
		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorClosed) Is(target error) bool {
	return target == ErrSlatedbErrorClosed
}

// A required dependency or remote service is temporarily unavailable.
type SlatedbErrorUnavailable struct {
	Message string
}

// A required dependency or remote service is temporarily unavailable.
func NewSlatedbErrorUnavailable(
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorUnavailable{
		Message: message}}
}

func (e SlatedbErrorUnavailable) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorUnavailable) Error() string {
	return fmt.Sprint("Unavailable",
		": ",

		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorUnavailable) Is(target error) bool {
	return target == ErrSlatedbErrorUnavailable
}

// The caller supplied invalid input.
type SlatedbErrorInvalid struct {
	Message string
}

// The caller supplied invalid input.
func NewSlatedbErrorInvalid(
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorInvalid{
		Message: message}}
}

func (e SlatedbErrorInvalid) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorInvalid) Error() string {
	return fmt.Sprint("Invalid",
		": ",

		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorInvalid) Is(target error) bool {
	return target == ErrSlatedbErrorInvalid
}

// Stored data was invalid or could not be decoded.
type SlatedbErrorData struct {
	Message string
}

// Stored data was invalid or could not be decoded.
func NewSlatedbErrorData(
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorData{
		Message: message}}
}

func (e SlatedbErrorData) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorData) Error() string {
	return fmt.Sprint("Data",
		": ",

		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorData) Is(target error) bool {
	return target == ErrSlatedbErrorData
}

// An unexpected internal failure occurred.
type SlatedbErrorInternal struct {
	Message string
}

// An unexpected internal failure occurred.
func NewSlatedbErrorInternal(
	message string,
) *SlatedbError {
	return &SlatedbError{err: &SlatedbErrorInternal{
		Message: message}}
}

func (e SlatedbErrorInternal) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err SlatedbErrorInternal) Error() string {
	return fmt.Sprint("Internal",
		": ",

		"Message=",
		err.Message,
	)
}

func (self SlatedbErrorInternal) Is(target error) bool {
	return target == ErrSlatedbErrorInternal
}

type FfiConverterSlatedbError struct{}

var FfiConverterSlatedbErrorINSTANCE = FfiConverterSlatedbError{}

func (c FfiConverterSlatedbError) Lift(eb RustBufferI) *SlatedbError {
	return LiftFromRustBuffer[*SlatedbError](c, eb)
}

func (c FfiConverterSlatedbError) Lower(value *SlatedbError) C.RustBuffer {
	return LowerIntoRustBuffer[*SlatedbError](c, value)
}

func (c FfiConverterSlatedbError) LowerExternal(value *SlatedbError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*SlatedbError](c, value))
}

func (c FfiConverterSlatedbError) Read(reader io.Reader) *SlatedbError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &SlatedbError{&SlatedbErrorTransaction{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &SlatedbError{&SlatedbErrorClosed{
			Reason:  FfiConverterCloseReasonINSTANCE.Read(reader),
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &SlatedbError{&SlatedbErrorUnavailable{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 4:
		return &SlatedbError{&SlatedbErrorInvalid{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 5:
		return &SlatedbError{&SlatedbErrorData{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 6:
		return &SlatedbError{&SlatedbErrorInternal{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterSlatedbError.Read()", errorID))
	}
}

func (c FfiConverterSlatedbError) Write(writer io.Writer, value *SlatedbError) {
	switch variantValue := value.err.(type) {
	case *SlatedbErrorTransaction:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *SlatedbErrorClosed:
		writeInt32(writer, 2)
		FfiConverterCloseReasonINSTANCE.Write(writer, variantValue.Reason)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *SlatedbErrorUnavailable:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *SlatedbErrorInvalid:
		writeInt32(writer, 4)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *SlatedbErrorData:
		writeInt32(writer, 5)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *SlatedbErrorInternal:
		writeInt32(writer, 6)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterSlatedbError.Write", value))
	}
}

type FfiDestroyerSlatedbError struct{}

func (_ FfiDestroyerSlatedbError) Destroy(value *SlatedbError) {
	switch variantValue := value.err.(type) {
	case SlatedbErrorTransaction:
		variantValue.destroy()
	case SlatedbErrorClosed:
		variantValue.destroy()
	case SlatedbErrorUnavailable:
		variantValue.destroy()
	case SlatedbErrorInvalid:
		variantValue.destroy()
	case SlatedbErrorData:
		variantValue.destroy()
	case SlatedbErrorInternal:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerSlatedbError.Destroy", value))
	}
}

// SST block sizes that can be selected on [`crate::DbBuilder`].
type SstBlockSize uint

const (
	// Use 1 KiB SST blocks.
	SstBlockSizeBlock1Kib SstBlockSize = 1
	// Use 2 KiB SST blocks.
	SstBlockSizeBlock2Kib SstBlockSize = 2
	// Use 4 KiB SST blocks.
	SstBlockSizeBlock4Kib SstBlockSize = 3
	// Use 8 KiB SST blocks.
	SstBlockSizeBlock8Kib SstBlockSize = 4
	// Use 16 KiB SST blocks.
	SstBlockSizeBlock16Kib SstBlockSize = 5
	// Use 32 KiB SST blocks.
	SstBlockSizeBlock32Kib SstBlockSize = 6
	// Use 64 KiB SST blocks.
	SstBlockSizeBlock64Kib SstBlockSize = 7
)

type FfiConverterSstBlockSize struct{}

var FfiConverterSstBlockSizeINSTANCE = FfiConverterSstBlockSize{}

func (c FfiConverterSstBlockSize) Lift(rb RustBufferI) SstBlockSize {
	return LiftFromRustBuffer[SstBlockSize](c, rb)
}

func (c FfiConverterSstBlockSize) Lower(value SstBlockSize) C.RustBuffer {
	return LowerIntoRustBuffer[SstBlockSize](c, value)
}

func (c FfiConverterSstBlockSize) LowerExternal(value SstBlockSize) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SstBlockSize](c, value))
}
func (FfiConverterSstBlockSize) Read(reader io.Reader) SstBlockSize {
	id := readInt32(reader)
	return SstBlockSize(id)
}

func (FfiConverterSstBlockSize) Write(writer io.Writer, value SstBlockSize) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerSstBlockSize struct{}

func (_ FfiDestroyerSstBlockSize) Destroy(value SstBlockSize) {
}

// Time-to-live configuration for put and merge operations.
type Ttl interface {
	Destroy()
}

// Use the database default TTL behavior.
type TtlDefault struct {
}

func (e TtlDefault) Destroy() {
}

// Store the value without an expiry.
type TtlNoExpiry struct {
}

func (e TtlNoExpiry) Destroy() {
}

// Expire the value after the provided number of clock ticks.
type TtlExpireAfterTicks struct {
	Field0 uint64
}

func (e TtlExpireAfterTicks) Destroy() {
	FfiDestroyerUint64{}.Destroy(e.Field0)
}

type FfiConverterTtl struct{}

var FfiConverterTtlINSTANCE = FfiConverterTtl{}

func (c FfiConverterTtl) Lift(rb RustBufferI) Ttl {
	return LiftFromRustBuffer[Ttl](c, rb)
}

func (c FfiConverterTtl) Lower(value Ttl) C.RustBuffer {
	return LowerIntoRustBuffer[Ttl](c, value)
}

func (c FfiConverterTtl) LowerExternal(value Ttl) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Ttl](c, value))
}
func (FfiConverterTtl) Read(reader io.Reader) Ttl {
	id := readInt32(reader)
	switch id {
	case 1:
		return TtlDefault{}
	case 2:
		return TtlNoExpiry{}
	case 3:
		return TtlExpireAfterTicks{
			FfiConverterUint64INSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterTtl.Read()", id))
	}
}

func (FfiConverterTtl) Write(writer io.Writer, value Ttl) {
	switch variant_value := value.(type) {
	case TtlDefault:
		writeInt32(writer, 1)
	case TtlNoExpiry:
		writeInt32(writer, 2)
	case TtlExpireAfterTicks:
		writeInt32(writer, 3)
		FfiConverterUint64INSTANCE.Write(writer, variant_value.Field0)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterTtl.Write", value))
	}
}

type FfiDestroyerTtl struct{}

func (_ FfiDestroyerTtl) Destroy(value Ttl) {
	value.Destroy()
}

// Callback interface for SlateDB merge operators.
//
// Merge operators are configured on [`DbBuilder`] and are used by merge reads
// and writes to combine an existing value with a new operand.
type MergeOperator interface {

	// Merge a new operand into the existing value for a key.
	//
	// ## Arguments
	// - `key`: the key being merged.
	// - `existing_value`: the current value, if one exists.
	// - `operand`: the new merge operand.
	//
	// ## Returns
	// - `Result<Vec<u8>, MergeOperatorCallbackError>`: the merged value that
	// should become visible for the key.
	Merge(key []byte, existingValue *[]byte, operand []byte) ([]byte, error)
}

type FfiConverterCallbackInterfaceMergeOperator struct {
	handleMap *concurrentHandleMap[MergeOperator]
}

var FfiConverterCallbackInterfaceMergeOperatorINSTANCE = FfiConverterCallbackInterfaceMergeOperator{
	handleMap: newConcurrentHandleMap[MergeOperator](),
}

func (c FfiConverterCallbackInterfaceMergeOperator) Lift(handle uint64) MergeOperator {
	val, ok := c.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return val
}

func (c FfiConverterCallbackInterfaceMergeOperator) Read(reader io.Reader) MergeOperator {
	return c.Lift(readUint64(reader))
}

func (c FfiConverterCallbackInterfaceMergeOperator) Lower(value MergeOperator) C.uint64_t {
	return C.uint64_t(c.handleMap.insert(value))
}

func (c FfiConverterCallbackInterfaceMergeOperator) Write(writer io.Writer, value MergeOperator) {
	writeUint64(writer, uint64(c.Lower(value)))
}

type FfiDestroyerCallbackInterfaceMergeOperator struct{}

func (FfiDestroyerCallbackInterfaceMergeOperator) Destroy(value MergeOperator) {}

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

//export slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0
func slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0(uniffiHandle C.uint64_t, key C.RustBuffer, existingValue C.RustBuffer, operand C.RustBuffer, uniffiOutReturn *C.RustBuffer, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterCallbackInterfaceMergeOperatorINSTANCE.handleMap.tryGet(handle)
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
		var actualError *MergeOperatorCallbackError
		if errors.As(err, &actualError) {
			*callStatus = C.RustCallStatus{
				code:     C.int8_t(uniffiCallbackResultError),
				errorBuf: FfiConverterMergeOperatorCallbackErrorINSTANCE.Lower(actualError),
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

var UniffiVTableCallbackInterfaceMergeOperatorINSTANCE = C.UniffiVTableCallbackInterfaceMergeOperator{
	merge: (C.UniffiCallbackInterfaceMergeOperatorMethod0)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0),

	uniffiFree: (C.UniffiCallbackInterfaceFree)(C.slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorFree),
}

//export slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorFree
func slatedb_ffi_cgo_dispatchCallbackInterfaceMergeOperatorFree(handle C.uint64_t) {
	FfiConverterCallbackInterfaceMergeOperatorINSTANCE.handleMap.remove(uint64(handle))
}

func (c FfiConverterCallbackInterfaceMergeOperator) register() {
	C.uniffi_slatedb_ffi_fn_init_callback_vtable_mergeoperator(&UniffiVTableCallbackInterfaceMergeOperatorINSTANCE)
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

type FfiConverterOptionalKeyValue struct{}

var FfiConverterOptionalKeyValueINSTANCE = FfiConverterOptionalKeyValue{}

func (c FfiConverterOptionalKeyValue) Lift(rb RustBufferI) *KeyValue {
	return LiftFromRustBuffer[*KeyValue](c, rb)
}

func (_ FfiConverterOptionalKeyValue) Read(reader io.Reader) *KeyValue {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterKeyValueINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalKeyValue) Lower(value *KeyValue) C.RustBuffer {
	return LowerIntoRustBuffer[*KeyValue](c, value)
}

func (c FfiConverterOptionalKeyValue) LowerExternal(value *KeyValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*KeyValue](c, value))
}

func (_ FfiConverterOptionalKeyValue) Write(writer io.Writer, value *KeyValue) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterKeyValueINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalKeyValue struct{}

func (_ FfiDestroyerOptionalKeyValue) Destroy(value *KeyValue) {
	if value != nil {
		FfiDestroyerKeyValue{}.Destroy(*value)
	}
}

type FfiConverterOptionalRowEntry struct{}

var FfiConverterOptionalRowEntryINSTANCE = FfiConverterOptionalRowEntry{}

func (c FfiConverterOptionalRowEntry) Lift(rb RustBufferI) *RowEntry {
	return LiftFromRustBuffer[*RowEntry](c, rb)
}

func (_ FfiConverterOptionalRowEntry) Read(reader io.Reader) *RowEntry {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterRowEntryINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalRowEntry) Lower(value *RowEntry) C.RustBuffer {
	return LowerIntoRustBuffer[*RowEntry](c, value)
}

func (c FfiConverterOptionalRowEntry) LowerExternal(value *RowEntry) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*RowEntry](c, value))
}

func (_ FfiConverterOptionalRowEntry) Write(writer io.Writer, value *RowEntry) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterRowEntryINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalRowEntry struct{}

func (_ FfiDestroyerOptionalRowEntry) Destroy(value *RowEntry) {
	if value != nil {
		FfiDestroyerRowEntry{}.Destroy(*value)
	}
}

type FfiConverterOptionalWriteHandle struct{}

var FfiConverterOptionalWriteHandleINSTANCE = FfiConverterOptionalWriteHandle{}

func (c FfiConverterOptionalWriteHandle) Lift(rb RustBufferI) *WriteHandle {
	return LiftFromRustBuffer[*WriteHandle](c, rb)
}

func (_ FfiConverterOptionalWriteHandle) Read(reader io.Reader) *WriteHandle {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterWriteHandleINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalWriteHandle) Lower(value *WriteHandle) C.RustBuffer {
	return LowerIntoRustBuffer[*WriteHandle](c, value)
}

func (c FfiConverterOptionalWriteHandle) LowerExternal(value *WriteHandle) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*WriteHandle](c, value))
}

func (_ FfiConverterOptionalWriteHandle) Write(writer io.Writer, value *WriteHandle) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterWriteHandleINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalWriteHandle struct{}

func (_ FfiDestroyerOptionalWriteHandle) Destroy(value *WriteHandle) {
	if value != nil {
		FfiDestroyerWriteHandle{}.Destroy(*value)
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

type FfiConverterSequenceWalFile struct{}

var FfiConverterSequenceWalFileINSTANCE = FfiConverterSequenceWalFile{}

func (c FfiConverterSequenceWalFile) Lift(rb RustBufferI) []*WalFile {
	return LiftFromRustBuffer[[]*WalFile](c, rb)
}

func (c FfiConverterSequenceWalFile) Read(reader io.Reader) []*WalFile {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]*WalFile, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterWalFileINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceWalFile) Lower(value []*WalFile) C.RustBuffer {
	return LowerIntoRustBuffer[[]*WalFile](c, value)
}

func (c FfiConverterSequenceWalFile) LowerExternal(value []*WalFile) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]*WalFile](c, value))
}

func (c FfiConverterSequenceWalFile) Write(writer io.Writer, value []*WalFile) {
	if len(value) > math.MaxInt32 {
		panic("[]*WalFile is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterWalFileINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceWalFile struct{}

func (FfiDestroyerSequenceWalFile) Destroy(sequence []*WalFile) {
	for _, value := range sequence {
		FfiDestroyerWalFile{}.Destroy(value)
	}
}

type FfiConverterSequenceDbWriteOperation struct{}

var FfiConverterSequenceDbWriteOperationINSTANCE = FfiConverterSequenceDbWriteOperation{}

func (c FfiConverterSequenceDbWriteOperation) Lift(rb RustBufferI) []DbWriteOperation {
	return LiftFromRustBuffer[[]DbWriteOperation](c, rb)
}

func (c FfiConverterSequenceDbWriteOperation) Read(reader io.Reader) []DbWriteOperation {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]DbWriteOperation, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterDbWriteOperationINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceDbWriteOperation) Lower(value []DbWriteOperation) C.RustBuffer {
	return LowerIntoRustBuffer[[]DbWriteOperation](c, value)
}

func (c FfiConverterSequenceDbWriteOperation) LowerExternal(value []DbWriteOperation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]DbWriteOperation](c, value))
}

func (c FfiConverterSequenceDbWriteOperation) Write(writer io.Writer, value []DbWriteOperation) {
	if len(value) > math.MaxInt32 {
		panic("[]DbWriteOperation is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterDbWriteOperationINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceDbWriteOperation struct{}

func (FfiDestroyerSequenceDbWriteOperation) Destroy(sequence []DbWriteOperation) {
	for _, value := range sequence {
		FfiDestroyerDbWriteOperation{}.Destroy(value)
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

// Return the default [`slatedb::Settings`] value as JSON.
//
// This is useful for FFI callers that want to start from the Rust default
// configuration, modify selected fields, and pass the full JSON document back
// to [`crate::DbBuilder::with_settings_json`].
//
// ## Returns
// - `Result<String, SlatedbError>`: the default settings encoded as JSON.
func DefaultSettingsJson() (string, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_ffi_fn_func_default_settings_json(_uniffiStatus),
		}
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue string
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterStringINSTANCE.Lift(_uniffiRV), nil
	}
}

// Initialize SlateDB logging at the default `Info` level.
func InitDefaultLogging() error {
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_func_init_default_logging(_uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Initialize SlateDB logging at the requested level.
func InitLogging(level LogLevel) error {
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_func_init_logging(FfiConverterLogLevelINSTANCE.Lower(level), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Resolve an object store from a URL.
//
// ## Arguments
// - `url`: the object-store URL, for example `memory:///` or `s3://bucket/prefix`.
//
// ## Returns
// - `Result<Arc<ObjectStore>, SlatedbError>`: the resolved object-store handle.
//
// ## Errors
// - `SlatedbError`: if the URL cannot be parsed or the object-store backend is unsupported.
func ResolveObjectStore(url string) (*ObjectStore, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_ffi_fn_func_resolve_object_store(FfiConverterStringINSTANCE.Lower(url), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *ObjectStore
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterObjectStoreINSTANCE.Lift(_uniffiRV), nil
	}
}

// Update the process-global SlateDB logging level.
func SetLoggingLevel(level LogLevel) error {
	_, _uniffiErr := rustCallWithError[SlatedbError](FfiConverterSlatedbError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_ffi_fn_func_set_logging_level(FfiConverterLogLevelINSTANCE.Lower(level), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
