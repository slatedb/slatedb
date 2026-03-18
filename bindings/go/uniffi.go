package uniffi

// #include <uniffi.h>
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
		C.ffi_slatedb_uniffi_rustbuffer_free(cb.inner, status)
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
		return C.ffi_slatedb_uniffi_rustbuffer_from_bytes(foreign, status)
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

	FfiConverterLogCallbackINSTANCE.register()
	FfiConverterMergeOperatorINSTANCE.register()
	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 29
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_slatedb_uniffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("uniffi: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_func_init_logging()
		})
		if checksum != 20973 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_func_init_logging: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_begin()
		})
		if checksum != 51275 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_begin: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_delete()
		})
		if checksum != 34129 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_delete_with_options()
		})
		if checksum != 42509 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_delete_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_flush()
		})
		if checksum != 18130 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_flush: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_flush_with_options()
		})
		if checksum != 63293 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_flush_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get()
		})
		if checksum != 50068 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_key_value()
		})
		if checksum != 57684 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options()
		})
		if checksum != 20648 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_with_options()
		})
		if checksum != 20501 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_merge()
		})
		if checksum != 17999 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_merge_with_options()
		})
		if checksum != 61231 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_metrics()
		})
		if checksum != 63278 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_metrics: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_put()
		})
		if checksum != 59996 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_put_with_options()
		})
		if checksum != 58268 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan()
		})
		if checksum != 38146 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix()
		})
		if checksum != 16589 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options()
		})
		if checksum != 37166 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_with_options()
		})
		if checksum != 57778 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_shutdown()
		})
		if checksum != 43377 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_snapshot()
		})
		if checksum != 13313 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_snapshot: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_status()
		})
		if checksum != 55824 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_status: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_write()
		})
		if checksum != 13969 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_write_with_options()
		})
		if checksum != 34167 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_db_write_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_build()
		})
		if checksum != 57780 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled()
		})
		if checksum != 47291 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator()
		})
		if checksum != 26367 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed()
		})
		if checksum != 4525 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings()
		})
		if checksum != 60845 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size()
		})
		if checksum != 9450 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store()
		})
		if checksum != 59224 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbiterator_next()
		})
		if checksum != 49160 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbiterator_seek()
		})
		if checksum != 43547 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbiterator_seek: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_get()
		})
		if checksum != 22886 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options()
		})
		if checksum != 9133 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan()
		})
		if checksum != 19575 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix()
		})
		if checksum != 51732 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options()
		})
		if checksum != 24990 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options()
		})
		if checksum != 33406 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown()
		})
		if checksum != 33391 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build()
		})
		if checksum != 3383 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id()
		})
		if checksum != 20357 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator()
		})
		if checksum != 54971 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options()
		})
		if checksum != 5765 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store()
		})
		if checksum != 15471 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get()
		})
		if checksum != 37663 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value()
		})
		if checksum != 1007 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options()
		})
		if checksum != 20762 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options()
		})
		if checksum != 29177 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan()
		})
		if checksum != 18781 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix()
		})
		if checksum != 43063 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options()
		})
		if checksum != 39827 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options()
		})
		if checksum != 1457 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit()
		})
		if checksum != 17358 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options()
		})
		if checksum != 53495 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete()
		})
		if checksum != 9717 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get()
		})
		if checksum != 27661 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value()
		})
		if checksum != 62855 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options()
		})
		if checksum != 37939 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options()
		})
		if checksum != 53534 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_id()
		})
		if checksum != 16876 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read()
		})
		if checksum != 26788 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge()
		})
		if checksum != 28294 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options()
		})
		if checksum != 63505 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put()
		})
		if checksum != 30341 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options()
		})
		if checksum != 24593 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback()
		})
		if checksum != 23348 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan()
		})
		if checksum != 12571 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix()
		})
		if checksum != 49961 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options()
		})
		if checksum != 33081 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options()
		})
		if checksum != 55349 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum()
		})
		if checksum != 60506 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write()
		})
		if checksum != 15301 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_logcallback_log()
		})
		if checksum != 11398 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_logcallback_log: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge()
		})
		if checksum != 9511 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_settings_set()
		})
		if checksum != 31996 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_settings_set: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_settings_to_json_string()
		})
		if checksum != 62526 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_settings_to_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_id()
		})
		if checksum != 51355 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfile_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_iterator()
		})
		if checksum != 50239 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfile_iterator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_metadata()
		})
		if checksum != 30832 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfile_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_next_file()
		})
		if checksum != 52353 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfile_next_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_next_id()
		})
		if checksum != 60587 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfile_next_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfileiterator_next()
		})
		if checksum != 18233 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walfileiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walreader_get()
		})
		if checksum != 40699 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walreader_list()
		})
		if checksum != 62366 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_walreader_list: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_delete()
		})
		if checksum != 37032 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_writebatch_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_merge()
		})
		if checksum != 51939 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_writebatch_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options()
		})
		if checksum != 30105 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_put()
		})
		if checksum != 35694 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_writebatch_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options()
		})
		if checksum != 23639 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new()
		})
		if checksum != 20774 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new()
		})
		if checksum != 63705 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env()
		})
		if checksum != 31525 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve()
		})
		if checksum != 27737 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_default()
		})
		if checksum != 56170 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env()
		})
		if checksum != 49511 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default()
		})
		if checksum != 6106 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_file()
		})
		if checksum != 40167 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_from_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string()
		})
		if checksum != 43399 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_load()
		})
		if checksum != 3704 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_settings_load: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_walreader_new()
		})
		if checksum != 791 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_walreader_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_writebatch_new()
		})
		if checksum != 25201 {
			// If this happens try cleaning and rebuilding your project
			panic("uniffi: uniffi_slatedb_uniffi_checksum_constructor_writebatch_new: UniFFI API checksum mismatch")
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

// A writable SlateDB handle.
type DbInterface interface {
	// Starts a transaction at the requested isolation level.
	Begin(isolationLevel IsolationLevel) (*DbTransaction, error)
	// Deletes `key` and returns metadata for the write.
	Delete(key []byte) (WriteHandle, error)
	// Deletes `key` using custom write options.
	DeleteWithOptions(key []byte, options WriteOptions) (WriteHandle, error)
	// Flushes the default storage layer.
	Flush() error
	// Flushes according to the provided flush options.
	FlushWithOptions(options FlushOptions) error
	// Reads the current value for `key`.
	Get(key []byte) (*[]byte, error)
	// Reads the current row version for `key`, including metadata.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Reads the current row version for `key` using custom read options.
	GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error)
	// Reads the current value for `key` using custom read options.
	GetWithOptions(key []byte, options ReadOptions) (*[]byte, error)
	// Appends a merge operand for `key` and returns metadata for the write.
	Merge(key []byte, operand []byte) (WriteHandle, error)
	// Appends a merge operand using custom merge and write options.
	MergeWithOptions(key []byte, operand []byte, mergeOptions MergeOptions, writeOptions WriteOptions) (WriteHandle, error)
	// Returns a snapshot of the current integer metrics registry.
	Metrics() (map[string]int64, error)
	// Inserts or overwrites a value and returns metadata for the write.
	//
	// Keys must be non-empty and at most `u16::MAX` bytes. Values must be at
	// most `u32::MAX` bytes.
	Put(key []byte, value []byte) (WriteHandle, error)
	// Inserts or overwrites a value using custom put and write options.
	PutWithOptions(key []byte, value []byte, putOptions PutOptions, writeOptions WriteOptions) (WriteHandle, error)
	// Scans rows inside `range`.
	Scan(varRange KeyRange) (*DbIterator, error)
	// Scans rows whose keys start with `prefix`.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error)
	// Scans rows inside `range` using custom scan options.
	ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error)
	// Flushes outstanding work and closes the database.
	Shutdown() error
	// Creates a read-only snapshot representing a consistent point in time.
	Snapshot() (*DbSnapshot, error)
	// Returns an error if the database is not currently healthy and open.
	Status() error
	// Applies all operations in `batch` atomically.
	//
	// The provided batch is consumed and cannot be reused afterwards.
	Write(batch *WriteBatch) (WriteHandle, error)
	// Applies all operations in `batch` atomically using custom write options.
	//
	// The provided batch is consumed and cannot be reused afterwards.
	WriteWithOptions(batch *WriteBatch, options WriteOptions) (WriteHandle, error)
}

// A writable SlateDB handle.
type Db struct {
	ffiObject FfiObject
}

// Starts a transaction at the requested isolation level.
func (_self *Db) Begin(isolationLevel IsolationLevel) (*DbTransaction, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbTransaction {
			return FfiConverterDbTransactionINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_begin(
			_pointer, FfiConverterIsolationLevelINSTANCE.Lower(isolationLevel)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Deletes `key` and returns metadata for the write.
func (_self *Db) Delete(key []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Deletes `key` using custom write options.
func (_self *Db) DeleteWithOptions(key []byte, options WriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_delete_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Flushes the default storage layer.
func (_self *Db) Flush() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_db_flush(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Flushes according to the provided flush options.
func (_self *Db) FlushWithOptions(options FlushOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_db_flush_with_options(
			_pointer, FfiConverterFlushOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Reads the current value for `key`.
func (_self *Db) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the current row version for `key`, including metadata.
func (_self *Db) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the current row version for `key` using custom read options.
func (_self *Db) GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the current value for `key` using custom read options.
func (_self *Db) GetWithOptions(key []byte, options ReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Appends a merge operand for `key` and returns metadata for the write.
func (_self *Db) Merge(key []byte, operand []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Appends a merge operand using custom merge and write options.
func (_self *Db) MergeWithOptions(key []byte, operand []byte, mergeOptions MergeOptions, writeOptions WriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterMergeOptionsINSTANCE.Lower(mergeOptions), FfiConverterWriteOptionsINSTANCE.Lower(writeOptions)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns a snapshot of the current integer metrics registry.
func (_self *Db) Metrics() (map[string]int64, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_db_metrics(
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

// Inserts or overwrites a value and returns metadata for the write.
//
// Keys must be non-empty and at most `u16::MAX` bytes. Values must be at
// most `u32::MAX` bytes.
func (_self *Db) Put(key []byte, value []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Inserts or overwrites a value using custom put and write options.
func (_self *Db) PutWithOptions(key []byte, value []byte, putOptions PutOptions, writeOptions WriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterPutOptionsINSTANCE.Lower(putOptions), FfiConverterWriteOptionsINSTANCE.Lower(writeOptions)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range`.
func (_self *Db) Scan(varRange KeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix`.
func (_self *Db) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` using custom scan options.
func (_self *Db) ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range` using custom scan options.
func (_self *Db) ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Flushes outstanding work and closes the database.
func (_self *Db) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_db_shutdown(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Creates a read-only snapshot representing a consistent point in time.
func (_self *Db) Snapshot() (*DbSnapshot, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbSnapshot {
			return FfiConverterDbSnapshotINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_snapshot(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns an error if the database is not currently healthy and open.
func (_self *Db) Status() error {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_db_status(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Applies all operations in `batch` atomically.
//
// The provided batch is consumed and cannot be reused afterwards.
func (_self *Db) Write(batch *WriteBatch) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_write(
			_pointer, FfiConverterWriteBatchINSTANCE.Lower(batch)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Applies all operations in `batch` atomically using custom write options.
//
// The provided batch is consumed and cannot be reused afterwards.
func (_self *Db) WriteWithOptions(batch *WriteBatch, options WriteOptions) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WriteHandle {
			return FfiConverterWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_write_with_options(
			_pointer, FfiConverterWriteBatchINSTANCE.Lower(batch), FfiConverterWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_db(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_db(pointer, status)
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

// Builder for opening a writable [`crate::Db`].
//
// Builders are single-use: calling [`DbBuilder::build`] consumes the builder.
type DbBuilderInterface interface {
	// Opens the database and consumes this builder.
	Build() (*Db, error)
	// Disables the SST block and metadata cache.
	WithDbCacheDisabled() error
	// Installs an application-defined merge operator.
	WithMergeOperator(mergeOperator MergeOperator) error
	// Sets the seed used for SlateDB's internal random number generation.
	WithSeed(seed uint64) error
	// Applies a [`crate::Settings`] object to the builder.
	WithSettings(settings *Settings) error
	// Sets the SSTable block size used for newly written tables.
	WithSstBlockSize(sstBlockSize SstBlockSize) error
	// Uses a separate object store for WAL files.
	WithWalObjectStore(walObjectStore *ObjectStore) error
}

// Builder for opening a writable [`crate::Db`].
//
// Builders are single-use: calling [`DbBuilder::build`] consumes the builder.
type DbBuilder struct {
	ffiObject FfiObject
}

// Creates a new database builder for `path` in `object_store`.
func NewDbBuilder(path string, objectStore *ObjectStore) *DbBuilder {
	return FfiConverterDbBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_dbbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Opens the database and consumes this builder.
func (_self *DbBuilder) Build() (*Db, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *Db {
			return FfiConverterDbINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Disables the SST block and metadata cache.
func (_self *DbBuilder) WithDbCacheDisabled() error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_db_cache_disabled(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Installs an application-defined merge operator.
func (_self *DbBuilder) WithMergeOperator(mergeOperator MergeOperator) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_merge_operator(
			_pointer, FfiConverterMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Sets the seed used for SlateDB's internal random number generation.
func (_self *DbBuilder) WithSeed(seed uint64) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_seed(
			_pointer, FfiConverterUint64INSTANCE.Lower(seed), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Applies a [`crate::Settings`] object to the builder.
func (_self *DbBuilder) WithSettings(settings *Settings) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_settings(
			_pointer, FfiConverterSettingsINSTANCE.Lower(settings), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Sets the SSTable block size used for newly written tables.
func (_self *DbBuilder) WithSstBlockSize(sstBlockSize SstBlockSize) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_sst_block_size(
			_pointer, FfiConverterSstBlockSizeINSTANCE.Lower(sstBlockSize), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Uses a separate object store for WAL files.
func (_self *DbBuilder) WithWalObjectStore(walObjectStore *ObjectStore) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_wal_object_store(
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbbuilder(pointer, status)
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

// Async iterator returned by scan APIs.
type DbIteratorInterface interface {
	// Returns the next key/value pair from the iterator.
	Next() (*KeyValue, error)
	// Seeks the iterator to the first entry at or after `key`.
	Seek(key []byte) error
}

// Async iterator returned by scan APIs.
type DbIterator struct {
	ffiObject FfiObject
}

// Returns the next key/value pair from the iterator.
func (_self *DbIterator) Next() (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbiterator_next(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Seeks the iterator to the first entry at or after `key`.
func (_self *DbIterator) Seek(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbIterator")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbiterator_seek(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbiterator(pointer, status)
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

// Read-only database handle opened by [`crate::DbReaderBuilder`].
type DbReaderInterface interface {
	// Reads the current value for `key`.
	Get(key []byte) (*[]byte, error)
	// Reads the current value for `key` using custom read options.
	GetWithOptions(key []byte, options ReadOptions) (*[]byte, error)
	// Scans rows inside `range`.
	Scan(varRange KeyRange) (*DbIterator, error)
	// Scans rows whose keys start with `prefix`.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` using custom scan options.
	ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error)
	// Scans rows inside `range` using custom scan options.
	ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error)
	// Closes the reader.
	Shutdown() error
}

// Read-only database handle opened by [`crate::DbReaderBuilder`].
type DbReader struct {
	ffiObject FfiObject
}

// Reads the current value for `key`.
func (_self *DbReader) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the current value for `key` using custom read options.
func (_self *DbReader) GetWithOptions(key []byte, options ReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range`.
func (_self *DbReader) Scan(varRange KeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix`.
func (_self *DbReader) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` using custom scan options.
func (_self *DbReader) ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range` using custom scan options.
func (_self *DbReader) ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Closes the reader.
func (_self *DbReader) Shutdown() error {
	_pointer := _self.ffiObject.incrementPointer("*DbReader")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbreader_shutdown(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbreader(pointer, status)
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

// Builder for opening a read-only [`crate::DbReader`].
//
// Builders are single-use: calling [`DbReaderBuilder::build`] consumes the builder.
type DbReaderBuilderInterface interface {
	// Opens the reader and consumes this builder.
	Build() (*DbReader, error)
	// Pins the reader to an existing checkpoint UUID string.
	WithCheckpointId(checkpointId string) error
	// Installs an application-defined merge operator used while reading merge rows.
	WithMergeOperator(mergeOperator MergeOperator) error
	// Applies custom reader options.
	WithOptions(options ReaderOptions) error
	// Uses a separate object store for WAL files.
	WithWalObjectStore(walObjectStore *ObjectStore) error
}

// Builder for opening a read-only [`crate::DbReader`].
//
// Builders are single-use: calling [`DbReaderBuilder::build`] consumes the builder.
type DbReaderBuilder struct {
	ffiObject FfiObject
}

// Creates a new reader builder for `path` in `object_store`.
func NewDbReaderBuilder(path string, objectStore *ObjectStore) *DbReaderBuilder {
	return FfiConverterDbReaderBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_dbreaderbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Opens the reader and consumes this builder.
func (_self *DbReaderBuilder) Build() (*DbReader, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbReader {
			return FfiConverterDbReaderINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Pins the reader to an existing checkpoint UUID string.
func (_self *DbReaderBuilder) WithCheckpointId(checkpointId string) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_checkpoint_id(
			_pointer, FfiConverterStringINSTANCE.Lower(checkpointId), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Installs an application-defined merge operator used while reading merge rows.
func (_self *DbReaderBuilder) WithMergeOperator(mergeOperator MergeOperator) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_merge_operator(
			_pointer, FfiConverterMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Applies custom reader options.
func (_self *DbReaderBuilder) WithOptions(options ReaderOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_options(
			_pointer, FfiConverterReaderOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Uses a separate object store for WAL files.
func (_self *DbReaderBuilder) WithWalObjectStore(walObjectStore *ObjectStore) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_wal_object_store(
			_pointer, FfiConverterObjectStoreINSTANCE.Lower(walObjectStore), _uniffiStatus)
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbreaderbuilder(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbreaderbuilder(pointer, status)
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

// Read-only snapshot representing a consistent view of the database.
type DbSnapshotInterface interface {
	// Reads the value visible in this snapshot for `key`.
	Get(key []byte) (*[]byte, error)
	// Reads the row version visible in this snapshot for `key`.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Reads the row version visible in this snapshot for `key` using custom read options.
	GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error)
	// Reads the value visible in this snapshot for `key` using custom read options.
	GetWithOptions(key []byte, options ReadOptions) (*[]byte, error)
	// Scans rows inside `range` as of this snapshot.
	Scan(varRange KeyRange) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` as of this snapshot.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` as of this snapshot using custom options.
	ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error)
	// Scans rows inside `range` as of this snapshot using custom scan options.
	ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error)
}

// Read-only snapshot representing a consistent view of the database.
type DbSnapshot struct {
	ffiObject FfiObject
}

// Reads the value visible in this snapshot for `key`.
func (_self *DbSnapshot) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the row version visible in this snapshot for `key`.
func (_self *DbSnapshot) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the row version visible in this snapshot for `key` using custom read options.
func (_self *DbSnapshot) GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the value visible in this snapshot for `key` using custom read options.
func (_self *DbSnapshot) GetWithOptions(key []byte, options ReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range` as of this snapshot.
func (_self *DbSnapshot) Scan(varRange KeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` as of this snapshot.
func (_self *DbSnapshot) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` as of this snapshot using custom options.
func (_self *DbSnapshot) ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range` as of this snapshot using custom scan options.
func (_self *DbSnapshot) ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbSnapshot")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbsnapshot(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbsnapshot(pointer, status)
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

// Transaction handle returned by [`crate::Db::begin`].
//
// A transaction becomes unusable after `commit`, `commit_with_options`, or
// `rollback`.
type DbTransactionInterface interface {
	// Commits the transaction.
	//
	// Returns `None` when the transaction performed no writes.
	Commit() (*WriteHandle, error)
	// Commits the transaction using custom write options.
	//
	// Returns `None` when the transaction performed no writes.
	CommitWithOptions(options WriteOptions) (*WriteHandle, error)
	// Buffers a delete inside the transaction.
	Delete(key []byte) error
	// Reads the value visible to this transaction for `key`.
	Get(key []byte) (*[]byte, error)
	// Reads the row version visible to this transaction for `key`.
	GetKeyValue(key []byte) (*KeyValue, error)
	// Reads the row version visible to this transaction for `key` using custom options.
	GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error)
	// Reads the value visible to this transaction for `key` using custom read options.
	GetWithOptions(key []byte, options ReadOptions) (*[]byte, error)
	// Returns the transaction identifier as a UUID string.
	Id() string
	// Marks keys as read for conflict detection.
	MarkRead(keys [][]byte) error
	// Buffers a merge operand inside the transaction.
	Merge(key []byte, operand []byte) error
	// Buffers a merge operand inside the transaction using custom merge options.
	MergeWithOptions(key []byte, operand []byte, options MergeOptions) error
	// Buffers a put inside the transaction.
	Put(key []byte, value []byte) error
	// Buffers a put inside the transaction using custom put options.
	PutWithOptions(key []byte, value []byte, options PutOptions) error
	// Rolls back the transaction and marks it completed.
	Rollback() error
	// Scans rows inside `range` as visible to this transaction.
	Scan(varRange KeyRange) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` as visible to this transaction.
	ScanPrefix(prefix []byte) (*DbIterator, error)
	// Scans rows whose keys start with `prefix` as visible to this transaction using custom options.
	ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error)
	// Scans rows inside `range` as visible to this transaction using custom options.
	ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error)
	// Returns the sequence number assigned when the transaction started.
	Seqnum() uint64
	// Excludes written keys from transaction conflict detection.
	UnmarkWrite(keys [][]byte) error
}

// Transaction handle returned by [`crate::Db::begin`].
//
// A transaction becomes unusable after `commit`, `commit_with_options`, or
// `rollback`.
type DbTransaction struct {
	ffiObject FfiObject
}

// Commits the transaction.
//
// Returns `None` when the transaction performed no writes.
func (_self *DbTransaction) Commit() (*WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *WriteHandle {
			return FfiConverterOptionalWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_commit(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Commits the transaction using custom write options.
//
// Returns `None` when the transaction performed no writes.
func (_self *DbTransaction) CommitWithOptions(options WriteOptions) (*WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *WriteHandle {
			return FfiConverterOptionalWriteHandleINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_commit_with_options(
			_pointer, FfiConverterWriteOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Buffers a delete inside the transaction.
func (_self *DbTransaction) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Reads the value visible to this transaction for `key`.
func (_self *DbTransaction) Get(key []byte) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_get(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the row version visible to this transaction for `key`.
func (_self *DbTransaction) GetKeyValue(key []byte) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_get_key_value(
			_pointer, FfiConverterBytesINSTANCE.Lower(key)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the row version visible to this transaction for `key` using custom options.
func (_self *DbTransaction) GetKeyValueWithOptions(key []byte, options ReadOptions) (*KeyValue, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *KeyValue {
			return FfiConverterOptionalKeyValueINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_get_key_value_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads the value visible to this transaction for `key` using custom read options.
func (_self *DbTransaction) GetWithOptions(key []byte, options ReadOptions) (*[]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *[]byte {
			return FfiConverterOptionalBytesINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_get_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterReadOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns the transaction identifier as a UUID string.
func (_self *DbTransaction) Id() string {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_dbtransaction_id(
				_pointer, _uniffiStatus),
		}
	}))
}

// Marks keys as read for conflict detection.
func (_self *DbTransaction) MarkRead(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_mark_read(
			_pointer, FfiConverterSequenceBytesINSTANCE.Lower(keys)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Buffers a merge operand inside the transaction.
func (_self *DbTransaction) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Buffers a merge operand inside the transaction using custom merge options.
func (_self *DbTransaction) MergeWithOptions(key []byte, operand []byte, options MergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterMergeOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Buffers a put inside the transaction.
func (_self *DbTransaction) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Buffers a put inside the transaction using custom put options.
func (_self *DbTransaction) PutWithOptions(key []byte, value []byte, options PutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterPutOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Rolls back the transaction and marks it completed.
func (_self *DbTransaction) Rollback() error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_rollback(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
		},
	)

	if err == nil {
		return nil
	}

	return err
}

// Scans rows inside `range` as visible to this transaction.
func (_self *DbTransaction) Scan(varRange KeyRange) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` as visible to this transaction.
func (_self *DbTransaction) ScanPrefix(prefix []byte) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows whose keys start with `prefix` as visible to this transaction using custom options.
func (_self *DbTransaction) ScanPrefixWithOptions(prefix []byte, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Scans rows inside `range` as visible to this transaction using custom options.
func (_self *DbTransaction) ScanWithOptions(varRange KeyRange, options ScanOptions) (*DbIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns the sequence number assigned when the transaction started.
func (_self *DbTransaction) Seqnum() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_dbtransaction_seqnum(
			_pointer, _uniffiStatus)
	}))
}

// Excludes written keys from transaction conflict detection.
func (_self *DbTransaction) UnmarkWrite(keys [][]byte) error {
	_pointer := _self.ffiObject.incrementPointer("*DbTransaction")
	defer _self.ffiObject.decrementPointer()
	_, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) struct{} {
			C.ffi_slatedb_uniffi_rust_future_complete_void(handle, status)
			return struct{}{}
		},
		// liftFn
		func(_ struct{}) struct{} { return struct{}{} },
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_unmark_write(
			_pointer, FfiConverterSequenceBytesINSTANCE.Lower(keys)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_void(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_void(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_dbtransaction(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbtransaction(pointer, status)
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

// Callback invoked for each emitted log record.
type LogCallback interface {
	// Handles one log record.
	Log(record LogRecord)
}

// Callback invoked for each emitted log record.
type LogCallbackImpl struct {
	ffiObject FfiObject
}

// Handles one log record.
func (_self *LogCallbackImpl) Log(record LogRecord) {
	_pointer := _self.ffiObject.incrementPointer("LogCallback")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_logcallback_log(
			_pointer, FfiConverterLogRecordINSTANCE.Lower(record), _uniffiStatus)
		return false
	})
}
func (object *LogCallbackImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterLogCallback struct {
	handleMap *concurrentHandleMap[LogCallback]
}

var FfiConverterLogCallbackINSTANCE = FfiConverterLogCallback{
	handleMap: newConcurrentHandleMap[LogCallback](),
}

func (c FfiConverterLogCallback) Lift(pointer unsafe.Pointer) LogCallback {
	result := &LogCallbackImpl{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_uniffi_fn_clone_logcallback(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_logcallback(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*LogCallbackImpl).Destroy)
	return result
}

func (c FfiConverterLogCallback) Read(reader io.Reader) LogCallback {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterLogCallback) Lower(value LogCallback) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := unsafe.Pointer(uintptr(c.handleMap.insert(value)))
	return pointer

}

func (c FfiConverterLogCallback) Write(writer io.Writer, value LogCallback) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerLogCallback struct{}

func (_ FfiDestroyerLogCallback) Destroy(value LogCallback) {
	if val, ok := value.(*LogCallbackImpl); ok {
		val.Destroy()
	} else {
		panic("Expected *LogCallbackImpl")
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

//export slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackMethod0
func slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackMethod0(uniffiHandle C.uint64_t, record C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterLogCallbackINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Log(
		FfiConverterLogRecordINSTANCE.Lift(GoRustBuffer{
			inner: record,
		}),
	)

}

var UniffiVTableCallbackInterfaceLogCallbackINSTANCE = C.UniffiVTableCallbackInterfaceLogCallback{
	log: (C.UniffiCallbackInterfaceLogCallbackMethod0)(C.slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackMethod0),

	uniffiFree: (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackFree),
}

//export slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackFree
func slatedb_uniffi_cgo_dispatchCallbackInterfaceLogCallbackFree(handle C.uint64_t) {
	FfiConverterLogCallbackINSTANCE.handleMap.remove(uint64(handle))
}

func (c FfiConverterLogCallback) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_logcallback(&UniffiVTableCallbackInterfaceLogCallbackINSTANCE)
}

// Application-provided merge operator used by merge-enabled databases.
type MergeOperator interface {
	// Combines an existing value and a new merge operand into the next value.
	//
	// `existing_value` is `None` when the key has no visible base value.
	Merge(key []byte, existingValue *[]byte, operand []byte) ([]byte, error)
}

// Application-provided merge operator used by merge-enabled databases.
type MergeOperatorImpl struct {
	ffiObject FfiObject
}

// Combines an existing value and a new merge operand into the next value.
//
// `existing_value` is `None` when the key has no visible base value.
func (_self *MergeOperatorImpl) Merge(key []byte, existingValue *[]byte, operand []byte) ([]byte, error) {
	_pointer := _self.ffiObject.incrementPointer("MergeOperator")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[MergeOperatorCallbackError](FfiConverterMergeOperatorCallbackError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_mergeoperator_merge(
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
func (object *MergeOperatorImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterMergeOperator struct {
	handleMap *concurrentHandleMap[MergeOperator]
}

var FfiConverterMergeOperatorINSTANCE = FfiConverterMergeOperator{
	handleMap: newConcurrentHandleMap[MergeOperator](),
}

func (c FfiConverterMergeOperator) Lift(pointer unsafe.Pointer) MergeOperator {
	result := &MergeOperatorImpl{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_uniffi_fn_clone_mergeoperator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_mergeoperator(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*MergeOperatorImpl).Destroy)
	return result
}

func (c FfiConverterMergeOperator) Read(reader io.Reader) MergeOperator {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterMergeOperator) Lower(value MergeOperator) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := unsafe.Pointer(uintptr(c.handleMap.insert(value)))
	return pointer

}

func (c FfiConverterMergeOperator) Write(writer io.Writer, value MergeOperator) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerMergeOperator struct{}

func (_ FfiDestroyerMergeOperator) Destroy(value MergeOperator) {
	if val, ok := value.(*MergeOperatorImpl); ok {
		val.Destroy()
	} else {
		panic("Expected *MergeOperatorImpl")
	}
}

//export slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0
func slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0(uniffiHandle C.uint64_t, key C.RustBuffer, existingValue C.RustBuffer, operand C.RustBuffer, uniffiOutReturn *C.RustBuffer, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterMergeOperatorINSTANCE.handleMap.tryGet(handle)
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
	merge: (C.UniffiCallbackInterfaceMergeOperatorMethod0)(C.slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorMethod0),

	uniffiFree: (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorFree),
}

//export slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorFree
func slatedb_uniffi_cgo_dispatchCallbackInterfaceMergeOperatorFree(handle C.uint64_t) {
	FfiConverterMergeOperatorINSTANCE.handleMap.remove(uint64(handle))
}

func (c FfiConverterMergeOperator) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_mergeoperator(&UniffiVTableCallbackInterfaceMergeOperatorINSTANCE)
}

// Object store handle used when opening databases, readers, and WAL readers.
type ObjectStoreInterface interface {
}

// Object store handle used when opening databases, readers, and WAL readers.
type ObjectStore struct {
	ffiObject FfiObject
}

// Builds an object store from environment configuration.
//
// When `env_file` is provided, environment variables are loaded from that
// file before constructing the store.
func ObjectStoreFromEnv(envFile *string) (*ObjectStore, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_objectstore_from_env(FfiConverterOptionalStringINSTANCE.Lower(envFile), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *ObjectStore
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterObjectStoreINSTANCE.Lift(_uniffiRV), nil
	}
}

// Resolves an object store from a URL understood by SlateDB.
func ObjectStoreResolve(url string) (*ObjectStore, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_objectstore_resolve(FfiConverterStringINSTANCE.Lower(url), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *ObjectStore
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterObjectStoreINSTANCE.Lift(_uniffiRV), nil
	}
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
				return C.uniffi_slatedb_uniffi_fn_clone_objectstore(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_objectstore(pointer, status)
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

// Mutable database settings object used to configure a [`crate::DbBuilder`].
type SettingsInterface interface {
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
	// Serializes the current settings value to a JSON string.
	ToJsonString() (string, error)
}

// Mutable database settings object used to configure a [`crate::DbBuilder`].
type Settings struct {
	ffiObject FfiObject
}

// Creates a settings object populated with SlateDB defaults.
func SettingsDefault() *Settings {
	return FfiConverterSettingsINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_default(_uniffiStatus)
	}))
}

// Loads settings from environment variables using `prefix`.
func SettingsFromEnv(prefix string) (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_from_env(FfiConverterStringINSTANCE.Lower(prefix), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Settings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

// Loads settings from environment variables, falling back to `default_settings`.
func SettingsFromEnvWithDefault(prefix string, defaultSettings *Settings) (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_from_env_with_default(FfiConverterStringINSTANCE.Lower(prefix), FfiConverterSettingsINSTANCE.Lower(defaultSettings), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Settings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

// Loads settings from a JSON, TOML, or YAML file based on its extension.
func SettingsFromFile(path string) (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_from_file(FfiConverterStringINSTANCE.Lower(path), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Settings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

// Parses settings from a JSON string.
func SettingsFromJsonString(json string) (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_from_json_string(FfiConverterStringINSTANCE.Lower(json), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Settings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterSettingsINSTANCE.Lift(_uniffiRV), nil
	}
}

// Loads settings from SlateDB's default file and environment lookup order.
func SettingsLoad() (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_load(_uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Settings
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterSettingsINSTANCE.Lift(_uniffiRV), nil
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
func (_self *Settings) Set(key string, valueJson string) error {
	_pointer := _self.ffiObject.incrementPointer("*Settings")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_settings_set(
			_pointer, FfiConverterStringINSTANCE.Lower(key), FfiConverterStringINSTANCE.Lower(valueJson), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Serializes the current settings value to a JSON string.
func (_self *Settings) ToJsonString() (string, error) {
	_pointer := _self.ffiObject.incrementPointer("*Settings")
	defer _self.ffiObject.decrementPointer()
	_uniffiRV, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_settings_to_json_string(
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
func (object *Settings) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterSettings struct{}

var FfiConverterSettingsINSTANCE = FfiConverterSettings{}

func (c FfiConverterSettings) Lift(pointer unsafe.Pointer) *Settings {
	result := &Settings{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_slatedb_uniffi_fn_clone_settings(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_settings(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Settings).Destroy)
	return result
}

func (c FfiConverterSettings) Read(reader io.Reader) *Settings {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterSettings) Lower(value *Settings) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*Settings")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterSettings) Write(writer io.Writer, value *Settings) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerSettings struct{}

func (_ FfiDestroyerSettings) Destroy(value *Settings) {
	value.Destroy()
}

// Handle for a single WAL file.
type WalFileInterface interface {
	// Returns the WAL file ID.
	Id() uint64
	// Opens an iterator over raw row entries in this WAL file.
	Iterator() (*WalFileIterator, error)
	// Reads object-store metadata for this WAL file.
	Metadata() (WalFileMetadata, error)
	// Returns a handle for the next WAL file ID without checking existence.
	NextFile() *WalFile
	// Returns the WAL ID immediately after this file.
	NextId() uint64
}

// Handle for a single WAL file.
type WalFile struct {
	ffiObject FfiObject
}

// Returns the WAL file ID.
func (_self *WalFile) Id() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_walfile_id(
			_pointer, _uniffiStatus)
	}))
}

// Opens an iterator over raw row entries in this WAL file.
func (_self *WalFile) Iterator() (*WalFileIterator, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) unsafe.Pointer {
			res := C.ffi_slatedb_uniffi_rust_future_complete_pointer(handle, status)
			return res
		},
		// liftFn
		func(ffi unsafe.Pointer) *WalFileIterator {
			return FfiConverterWalFileIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_walfile_iterator(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_pointer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_pointer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Reads object-store metadata for this WAL file.
func (_self *WalFile) Metadata() (WalFileMetadata, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) WalFileMetadata {
			return FfiConverterWalFileMetadataINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_walfile_metadata(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err
}

// Returns a handle for the next WAL file ID without checking existence.
func (_self *WalFile) NextFile() *WalFile {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_method_walfile_next_file(
			_pointer, _uniffiStatus)
	}))
}

// Returns the WAL ID immediately after this file.
func (_self *WalFile) NextId() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*WalFile")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_walfile_next_id(
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
				return C.uniffi_slatedb_uniffi_fn_clone_walfile(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walfile(pointer, status)
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

// Iterator over raw row entries stored in a WAL file.
type WalFileIteratorInterface interface {
	// Returns the next raw row entry from the WAL file.
	Next() (*RowEntry, error)
}

// Iterator over raw row entries stored in a WAL file.
type WalFileIterator struct {
	ffiObject FfiObject
}

// Returns the next raw row entry from the WAL file.
func (_self *WalFileIterator) Next() (*RowEntry, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalFileIterator")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) *RowEntry {
			return FfiConverterOptionalRowEntryINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_walfileiterator_next(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_walfileiterator(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walfileiterator(pointer, status)
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

// Reader for WAL files stored under a database path.
type WalReaderInterface interface {
	// Returns a handle for the WAL file with the given ID.
	Get(id uint64) *WalFile
	// Lists WAL files in ascending ID order.
	//
	// `start_id` is inclusive and `end_id` is exclusive when provided.
	List(startId *uint64, endId *uint64) ([]*WalFile, error)
}

// Reader for WAL files stored under a database path.
type WalReader struct {
	ffiObject FfiObject
}

// Creates a WAL reader for `path` in `object_store`.
func NewWalReader(path string, objectStore *ObjectStore) *WalReader {
	return FfiConverterWalReaderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_walreader_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Returns a handle for the WAL file with the given ID.
func (_self *WalReader) Get(id uint64) *WalFile {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_method_walreader_get(
			_pointer, FfiConverterUint64INSTANCE.Lower(id), _uniffiStatus)
	}))
}

// Lists WAL files in ascending ID order.
//
// `start_id` is inclusive and `end_id` is exclusive when provided.
func (_self *WalReader) List(startId *uint64, endId *uint64) ([]*WalFile, error) {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer{
				inner: res,
			}
		},
		// liftFn
		func(ffi RustBufferI) []*WalFile {
			return FfiConverterSequenceWalFileINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_walreader_list(
			_pointer, FfiConverterOptionalUint64INSTANCE.Lower(startId), FfiConverterOptionalUint64INSTANCE.Lower(endId)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_rust_buffer(handle)
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
				return C.uniffi_slatedb_uniffi_fn_clone_walreader(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walreader(pointer, status)
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

// Mutable batch of write operations applied atomically by [`crate::Db::write`].
//
// A batch is single-use once submitted to the database.
type WriteBatchInterface interface {
	// Appends a delete operation to the batch.
	Delete(key []byte) error
	// Appends a merge operation to the batch.
	Merge(key []byte, operand []byte) error
	// Appends a merge operation with custom merge options.
	MergeWithOptions(key []byte, operand []byte, options MergeOptions) error
	// Appends a put operation to the batch.
	Put(key []byte, value []byte) error
	// Appends a put operation with custom put options.
	PutWithOptions(key []byte, value []byte, options PutOptions) error
}

// Mutable batch of write operations applied atomically by [`crate::Db::write`].
//
// A batch is single-use once submitted to the database.
type WriteBatch struct {
	ffiObject FfiObject
}

// Creates an empty write batch.
func NewWriteBatch() *WriteBatch {
	return FfiConverterWriteBatchINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_slatedb_uniffi_fn_constructor_writebatch_new(_uniffiStatus)
	}))
}

// Appends a delete operation to the batch.
func (_self *WriteBatch) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_writebatch_delete(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Appends a merge operation to the batch.
func (_self *WriteBatch) Merge(key []byte, operand []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_writebatch_merge(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Appends a merge operation with custom merge options.
func (_self *WriteBatch) MergeWithOptions(key []byte, operand []byte, options MergeOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_writebatch_merge_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(operand), FfiConverterMergeOptionsINSTANCE.Lower(options), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Appends a put operation to the batch.
func (_self *WriteBatch) Put(key []byte, value []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_writebatch_put(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Appends a put operation with custom put options.
func (_self *WriteBatch) PutWithOptions(key []byte, value []byte, options PutOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_writebatch_put_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(key), FfiConverterBytesINSTANCE.Lower(value), FfiConverterPutOptionsINSTANCE.Lower(options), _uniffiStatus)
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
				return C.uniffi_slatedb_uniffi_fn_clone_writebatch(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_writebatch(pointer, status)
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

// Options for an explicit flush request.
type FlushOptions struct {
	// Which storage layer should be flushed.
	FlushType FlushType
}

func (r *FlushOptions) Destroy() {
	FfiDestroyerFlushType{}.Destroy(r.FlushType)
}

type FfiConverterFlushOptions struct{}

var FfiConverterFlushOptionsINSTANCE = FfiConverterFlushOptions{}

func (c FfiConverterFlushOptions) Lift(rb RustBufferI) FlushOptions {
	return LiftFromRustBuffer[FlushOptions](c, rb)
}

func (c FfiConverterFlushOptions) Read(reader io.Reader) FlushOptions {
	return FlushOptions{
		FfiConverterFlushTypeINSTANCE.Read(reader),
	}
}

func (c FfiConverterFlushOptions) Lower(value FlushOptions) C.RustBuffer {
	return LowerIntoRustBuffer[FlushOptions](c, value)
}

func (c FfiConverterFlushOptions) LowerExternal(value FlushOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[FlushOptions](c, value))
}

func (c FfiConverterFlushOptions) Write(writer io.Writer, value FlushOptions) {
	FfiConverterFlushTypeINSTANCE.Write(writer, value.FlushType)
}

type FfiDestroyerFlushOptions struct{}

func (_ FfiDestroyerFlushOptions) Destroy(value FlushOptions) {
	value.Destroy()
}

// A half-open or closed byte-key range used by scan APIs.
type KeyRange struct {
	// Inclusive or exclusive lower bound. `None` means unbounded.
	Start *[]byte
	// Whether `start` is inclusive when present.
	StartInclusive bool
	// Inclusive or exclusive upper bound. `None` means unbounded.
	End *[]byte
	// Whether `end` is inclusive when present.
	EndInclusive bool
}

func (r *KeyRange) Destroy() {
	FfiDestroyerOptionalBytes{}.Destroy(r.Start)
	FfiDestroyerBool{}.Destroy(r.StartInclusive)
	FfiDestroyerOptionalBytes{}.Destroy(r.End)
	FfiDestroyerBool{}.Destroy(r.EndInclusive)
}

type FfiConverterKeyRange struct{}

var FfiConverterKeyRangeINSTANCE = FfiConverterKeyRange{}

func (c FfiConverterKeyRange) Lift(rb RustBufferI) KeyRange {
	return LiftFromRustBuffer[KeyRange](c, rb)
}

func (c FfiConverterKeyRange) Read(reader io.Reader) KeyRange {
	return KeyRange{
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterOptionalBytesINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterKeyRange) Lower(value KeyRange) C.RustBuffer {
	return LowerIntoRustBuffer[KeyRange](c, value)
}

func (c FfiConverterKeyRange) LowerExternal(value KeyRange) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[KeyRange](c, value))
}

func (c FfiConverterKeyRange) Write(writer io.Writer, value KeyRange) {
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.Start)
	FfiConverterBoolINSTANCE.Write(writer, value.StartInclusive)
	FfiConverterOptionalBytesINSTANCE.Write(writer, value.End)
	FfiConverterBoolINSTANCE.Write(writer, value.EndInclusive)
}

type FfiDestroyerKeyRange struct{}

func (_ FfiDestroyerKeyRange) Destroy(value KeyRange) {
	value.Destroy()
}

// A key/value pair together with the row version metadata that produced it.
type KeyValue struct {
	// Row key.
	Key []byte
	// Row value bytes.
	Value []byte
	// Sequence number of the row version.
	Seq uint64
	// Creation timestamp of the row version.
	CreateTs int64
	// Expiration timestamp, if the row has a TTL.
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

// A single log event forwarded to a foreign callback.
type LogRecord struct {
	// Event severity.
	Level LogLevel
	// Logging target or explicit `log.target` field.
	Target string
	// Rendered log message.
	Message string
	// Rust module path, when available.
	ModulePath *string
	// Source file path, when available.
	File *string
	// Source line number, when available.
	Line *uint32
}

func (r *LogRecord) Destroy() {
	FfiDestroyerLogLevel{}.Destroy(r.Level)
	FfiDestroyerString{}.Destroy(r.Target)
	FfiDestroyerString{}.Destroy(r.Message)
	FfiDestroyerOptionalString{}.Destroy(r.ModulePath)
	FfiDestroyerOptionalString{}.Destroy(r.File)
	FfiDestroyerOptionalUint32{}.Destroy(r.Line)
}

type FfiConverterLogRecord struct{}

var FfiConverterLogRecordINSTANCE = FfiConverterLogRecord{}

func (c FfiConverterLogRecord) Lift(rb RustBufferI) LogRecord {
	return LiftFromRustBuffer[LogRecord](c, rb)
}

func (c FfiConverterLogRecord) Read(reader io.Reader) LogRecord {
	return LogRecord{
		FfiConverterLogLevelINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalStringINSTANCE.Read(reader),
		FfiConverterOptionalUint32INSTANCE.Read(reader),
	}
}

func (c FfiConverterLogRecord) Lower(value LogRecord) C.RustBuffer {
	return LowerIntoRustBuffer[LogRecord](c, value)
}

func (c FfiConverterLogRecord) LowerExternal(value LogRecord) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[LogRecord](c, value))
}

func (c FfiConverterLogRecord) Write(writer io.Writer, value LogRecord) {
	FfiConverterLogLevelINSTANCE.Write(writer, value.Level)
	FfiConverterStringINSTANCE.Write(writer, value.Target)
	FfiConverterStringINSTANCE.Write(writer, value.Message)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.ModulePath)
	FfiConverterOptionalStringINSTANCE.Write(writer, value.File)
	FfiConverterOptionalUint32INSTANCE.Write(writer, value.Line)
}

type FfiDestroyerLogRecord struct{}

func (_ FfiDestroyerLogRecord) Destroy(value LogRecord) {
	value.Destroy()
}

// Options applied to a merge operation.
type MergeOptions struct {
	// TTL policy for the inserted merge operand.
	Ttl Ttl
}

func (r *MergeOptions) Destroy() {
	FfiDestroyerTtl{}.Destroy(r.Ttl)
}

type FfiConverterMergeOptions struct{}

var FfiConverterMergeOptionsINSTANCE = FfiConverterMergeOptions{}

func (c FfiConverterMergeOptions) Lift(rb RustBufferI) MergeOptions {
	return LiftFromRustBuffer[MergeOptions](c, rb)
}

func (c FfiConverterMergeOptions) Read(reader io.Reader) MergeOptions {
	return MergeOptions{
		FfiConverterTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterMergeOptions) Lower(value MergeOptions) C.RustBuffer {
	return LowerIntoRustBuffer[MergeOptions](c, value)
}

func (c FfiConverterMergeOptions) LowerExternal(value MergeOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[MergeOptions](c, value))
}

func (c FfiConverterMergeOptions) Write(writer io.Writer, value MergeOptions) {
	FfiConverterTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerMergeOptions struct{}

func (_ FfiDestroyerMergeOptions) Destroy(value MergeOptions) {
	value.Destroy()
}

// Options applied to a put operation.
type PutOptions struct {
	// TTL policy for the inserted value.
	Ttl Ttl
}

func (r *PutOptions) Destroy() {
	FfiDestroyerTtl{}.Destroy(r.Ttl)
}

type FfiConverterPutOptions struct{}

var FfiConverterPutOptionsINSTANCE = FfiConverterPutOptions{}

func (c FfiConverterPutOptions) Lift(rb RustBufferI) PutOptions {
	return LiftFromRustBuffer[PutOptions](c, rb)
}

func (c FfiConverterPutOptions) Read(reader io.Reader) PutOptions {
	return PutOptions{
		FfiConverterTtlINSTANCE.Read(reader),
	}
}

func (c FfiConverterPutOptions) Lower(value PutOptions) C.RustBuffer {
	return LowerIntoRustBuffer[PutOptions](c, value)
}

func (c FfiConverterPutOptions) LowerExternal(value PutOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[PutOptions](c, value))
}

func (c FfiConverterPutOptions) Write(writer io.Writer, value PutOptions) {
	FfiConverterTtlINSTANCE.Write(writer, value.Ttl)
}

type FfiDestroyerPutOptions struct{}

func (_ FfiDestroyerPutOptions) Destroy(value PutOptions) {
	value.Destroy()
}

// Options that control a point read.
type ReadOptions struct {
	// Minimum durability level a returned row must satisfy.
	DurabilityFilter DurabilityLevel
	// Whether uncommitted dirty data may be returned.
	Dirty bool
	// Whether fetched blocks should be inserted into the block cache.
	CacheBlocks bool
}

func (r *ReadOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
}

type FfiConverterReadOptions struct{}

var FfiConverterReadOptionsINSTANCE = FfiConverterReadOptions{}

func (c FfiConverterReadOptions) Lift(rb RustBufferI) ReadOptions {
	return LiftFromRustBuffer[ReadOptions](c, rb)
}

func (c FfiConverterReadOptions) Read(reader io.Reader) ReadOptions {
	return ReadOptions{
		FfiConverterDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterReadOptions) Lower(value ReadOptions) C.RustBuffer {
	return LowerIntoRustBuffer[ReadOptions](c, value)
}

func (c FfiConverterReadOptions) LowerExternal(value ReadOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ReadOptions](c, value))
}

func (c FfiConverterReadOptions) Write(writer io.Writer, value ReadOptions) {
	FfiConverterDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
}

type FfiDestroyerReadOptions struct{}

func (_ FfiDestroyerReadOptions) Destroy(value ReadOptions) {
	value.Destroy()
}

// Options for opening a [`crate::DbReader`].
type ReaderOptions struct {
	// How often the reader polls for new manifests and WAL data, in milliseconds.
	ManifestPollIntervalMs uint64
	// Lifetime of an internally managed checkpoint, in milliseconds.
	CheckpointLifetimeMs uint64
	// Maximum size of one in-memory table used while replaying WAL data.
	MaxMemtableBytes uint64
	// Whether WAL replay should be skipped entirely.
	SkipWalReplay bool
}

func (r *ReaderOptions) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.ManifestPollIntervalMs)
	FfiDestroyerUint64{}.Destroy(r.CheckpointLifetimeMs)
	FfiDestroyerUint64{}.Destroy(r.MaxMemtableBytes)
	FfiDestroyerBool{}.Destroy(r.SkipWalReplay)
}

type FfiConverterReaderOptions struct{}

var FfiConverterReaderOptionsINSTANCE = FfiConverterReaderOptions{}

func (c FfiConverterReaderOptions) Lift(rb RustBufferI) ReaderOptions {
	return LiftFromRustBuffer[ReaderOptions](c, rb)
}

func (c FfiConverterReaderOptions) Read(reader io.Reader) ReaderOptions {
	return ReaderOptions{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterReaderOptions) Lower(value ReaderOptions) C.RustBuffer {
	return LowerIntoRustBuffer[ReaderOptions](c, value)
}

func (c FfiConverterReaderOptions) LowerExternal(value ReaderOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ReaderOptions](c, value))
}

func (c FfiConverterReaderOptions) Write(writer io.Writer, value ReaderOptions) {
	FfiConverterUint64INSTANCE.Write(writer, value.ManifestPollIntervalMs)
	FfiConverterUint64INSTANCE.Write(writer, value.CheckpointLifetimeMs)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxMemtableBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.SkipWalReplay)
}

type FfiDestroyerReaderOptions struct{}

func (_ FfiDestroyerReaderOptions) Destroy(value ReaderOptions) {
	value.Destroy()
}

// A raw row entry returned from WAL inspection.
type RowEntry struct {
	// Encoded row kind.
	Kind RowEntryKind
	// Row key.
	Key []byte
	// Row value for value and merge entries. `None` for tombstones.
	Value *[]byte
	// Sequence number of the entry.
	Seq uint64
	// Creation timestamp if present in the WAL entry.
	CreateTs *int64
	// Expiration timestamp if present in the WAL entry.
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

// Options that control range scans and prefix scans.
type ScanOptions struct {
	// Minimum durability level a returned row must satisfy.
	DurabilityFilter DurabilityLevel
	// Whether uncommitted dirty data may be returned.
	Dirty bool
	// Number of bytes to read ahead while scanning.
	ReadAheadBytes uint64
	// Whether fetched blocks should be inserted into the block cache.
	CacheBlocks bool
	// Maximum number of concurrent fetch tasks used by the scan.
	MaxFetchTasks uint64
}

func (r *ScanOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerUint64{}.Destroy(r.ReadAheadBytes)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
	FfiDestroyerUint64{}.Destroy(r.MaxFetchTasks)
}

type FfiConverterScanOptions struct{}

var FfiConverterScanOptionsINSTANCE = FfiConverterScanOptions{}

func (c FfiConverterScanOptions) Lift(rb RustBufferI) ScanOptions {
	return LiftFromRustBuffer[ScanOptions](c, rb)
}

func (c FfiConverterScanOptions) Read(reader io.Reader) ScanOptions {
	return ScanOptions{
		FfiConverterDurabilityLevelINSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterScanOptions) Lower(value ScanOptions) C.RustBuffer {
	return LowerIntoRustBuffer[ScanOptions](c, value)
}

func (c FfiConverterScanOptions) LowerExternal(value ScanOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ScanOptions](c, value))
}

func (c FfiConverterScanOptions) Write(writer io.Writer, value ScanOptions) {
	FfiConverterDurabilityLevelINSTANCE.Write(writer, value.DurabilityFilter)
	FfiConverterBoolINSTANCE.Write(writer, value.Dirty)
	FfiConverterUint64INSTANCE.Write(writer, value.ReadAheadBytes)
	FfiConverterBoolINSTANCE.Write(writer, value.CacheBlocks)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxFetchTasks)
}

type FfiDestroyerScanOptions struct{}

func (_ FfiDestroyerScanOptions) Destroy(value ScanOptions) {
	value.Destroy()
}

// Metadata describing a WAL file in object storage.
type WalFileMetadata struct {
	// Last-modified timestamp seconds component.
	LastModifiedSeconds int64
	// Last-modified timestamp nanoseconds component.
	LastModifiedNanos uint32
	// File size in bytes.
	SizeBytes uint64
	// Object-store location of the file.
	Location string
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

// Metadata returned by a successful write.
type WriteHandle struct {
	// Sequence number assigned to the write.
	Seqnum uint64
	// Creation timestamp assigned to the write.
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

// Options that control durability behavior for writes and commits.
type WriteOptions struct {
	// Whether the call waits for the write to become durable before returning.
	AwaitDurable bool
}

func (r *WriteOptions) Destroy() {
	FfiDestroyerBool{}.Destroy(r.AwaitDurable)
}

type FfiConverterWriteOptions struct{}

var FfiConverterWriteOptionsINSTANCE = FfiConverterWriteOptions{}

func (c FfiConverterWriteOptions) Lift(rb RustBufferI) WriteOptions {
	return LiftFromRustBuffer[WriteOptions](c, rb)
}

func (c FfiConverterWriteOptions) Read(reader io.Reader) WriteOptions {
	return WriteOptions{
		FfiConverterBoolINSTANCE.Read(reader),
	}
}

func (c FfiConverterWriteOptions) Lower(value WriteOptions) C.RustBuffer {
	return LowerIntoRustBuffer[WriteOptions](c, value)
}

func (c FfiConverterWriteOptions) LowerExternal(value WriteOptions) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[WriteOptions](c, value))
}

func (c FfiConverterWriteOptions) Write(writer io.Writer, value WriteOptions) {
	FfiConverterBoolINSTANCE.Write(writer, value.AwaitDurable)
}

type FfiDestroyerWriteOptions struct{}

func (_ FfiDestroyerWriteOptions) Destroy(value WriteOptions) {
	value.Destroy()
}

// Reason a database or reader reports itself as closed.
type CloseReason uint

const (
	// Closed cleanly by the caller.
	CloseReasonClean CloseReason = 1
	// Closed because another writer fenced this instance.
	CloseReasonFenced CloseReason = 2
	// Closed because of a panic in a background task.
	CloseReasonPanic CloseReason = 3
	// Closed for a reason not modeled explicitly by this binding.
	CloseReasonUnknown CloseReason = 4
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

// Minimum durability level required for data returned by reads and scans.
type DurabilityLevel uint

const (
	// Return only data that has been flushed to remote object storage.
	DurabilityLevelRemote DurabilityLevel = 1
	// Return both remote data and newer in-memory data.
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

// Error type returned by the UniFFI bindings.
type Error struct {
	err error
}

// Convience method to turn *Error into error
// Avoiding treating nil pointer as non nil error interface
func (err *Error) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err Error) Error() string {
	return fmt.Sprintf("Error: %s", err.err.Error())
}

func (err Error) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrErrorTransaction = fmt.Errorf("ErrorTransaction")
var ErrErrorClosed = fmt.Errorf("ErrorClosed")
var ErrErrorUnavailable = fmt.Errorf("ErrorUnavailable")
var ErrErrorInvalid = fmt.Errorf("ErrorInvalid")
var ErrErrorData = fmt.Errorf("ErrorData")
var ErrErrorInternal = fmt.Errorf("ErrorInternal")

// Variant structs
// Transaction-specific failure.
type ErrorTransaction struct {
	Message string
}

// Transaction-specific failure.
func NewErrorTransaction(
	message string,
) *Error {
	return &Error{err: &ErrorTransaction{
		Message: message}}
}

func (e ErrorTransaction) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorTransaction) Error() string {
	return fmt.Sprint("Transaction",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorTransaction) Is(target error) bool {
	return target == ErrErrorTransaction
}

// Operation attempted on a closed handle.
type ErrorClosed struct {
	Reason  CloseReason
	Message string
}

// Operation attempted on a closed handle.
func NewErrorClosed(
	reason CloseReason,
	message string,
) *Error {
	return &Error{err: &ErrorClosed{
		Reason:  reason,
		Message: message}}
}

func (e ErrorClosed) destroy() {
	FfiDestroyerCloseReason{}.Destroy(e.Reason)
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorClosed) Error() string {
	return fmt.Sprint("Closed",
		": ",

		"Reason=",
		err.Reason,
		", ",
		"Message=",
		err.Message,
	)
}

func (self ErrorClosed) Is(target error) bool {
	return target == ErrErrorClosed
}

// Temporary unavailability, such as an unavailable dependency.
type ErrorUnavailable struct {
	Message string
}

// Temporary unavailability, such as an unavailable dependency.
func NewErrorUnavailable(
	message string,
) *Error {
	return &Error{err: &ErrorUnavailable{
		Message: message}}
}

func (e ErrorUnavailable) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorUnavailable) Error() string {
	return fmt.Sprint("Unavailable",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorUnavailable) Is(target error) bool {
	return target == ErrErrorUnavailable
}

// Invalid input or invalid API usage.
type ErrorInvalid struct {
	Message string
}

// Invalid input or invalid API usage.
func NewErrorInvalid(
	message string,
) *Error {
	return &Error{err: &ErrorInvalid{
		Message: message}}
}

func (e ErrorInvalid) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorInvalid) Error() string {
	return fmt.Sprint("Invalid",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorInvalid) Is(target error) bool {
	return target == ErrErrorInvalid
}

// Corrupt, missing, or otherwise invalid data was encountered.
type ErrorData struct {
	Message string
}

// Corrupt, missing, or otherwise invalid data was encountered.
func NewErrorData(
	message string,
) *Error {
	return &Error{err: &ErrorData{
		Message: message}}
}

func (e ErrorData) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorData) Error() string {
	return fmt.Sprint("Data",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorData) Is(target error) bool {
	return target == ErrErrorData
}

// Internal failure inside SlateDB or the binding layer.
type ErrorInternal struct {
	Message string
}

// Internal failure inside SlateDB or the binding layer.
func NewErrorInternal(
	message string,
) *Error {
	return &Error{err: &ErrorInternal{
		Message: message}}
}

func (e ErrorInternal) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorInternal) Error() string {
	return fmt.Sprint("Internal",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorInternal) Is(target error) bool {
	return target == ErrErrorInternal
}

type FfiConverterError struct{}

var FfiConverterErrorINSTANCE = FfiConverterError{}

func (c FfiConverterError) Lift(eb RustBufferI) *Error {
	return LiftFromRustBuffer[*Error](c, eb)
}

func (c FfiConverterError) Lower(value *Error) C.RustBuffer {
	return LowerIntoRustBuffer[*Error](c, value)
}

func (c FfiConverterError) LowerExternal(value *Error) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*Error](c, value))
}

func (c FfiConverterError) Read(reader io.Reader) *Error {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &Error{&ErrorTransaction{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &Error{&ErrorClosed{
			Reason:  FfiConverterCloseReasonINSTANCE.Read(reader),
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &Error{&ErrorUnavailable{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 4:
		return &Error{&ErrorInvalid{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 5:
		return &Error{&ErrorData{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 6:
		return &Error{&ErrorInternal{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterError.Read()", errorID))
	}
}

func (c FfiConverterError) Write(writer io.Writer, value *Error) {
	switch variantValue := value.err.(type) {
	case *ErrorTransaction:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ErrorClosed:
		writeInt32(writer, 2)
		FfiConverterCloseReasonINSTANCE.Write(writer, variantValue.Reason)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ErrorUnavailable:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ErrorInvalid:
		writeInt32(writer, 4)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ErrorData:
		writeInt32(writer, 5)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *ErrorInternal:
		writeInt32(writer, 6)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterError.Write", value))
	}
}

type FfiDestroyerError struct{}

func (_ FfiDestroyerError) Destroy(value *Error) {
	switch variantValue := value.err.(type) {
	case ErrorTransaction:
		variantValue.destroy()
	case ErrorClosed:
		variantValue.destroy()
	case ErrorUnavailable:
		variantValue.destroy()
	case ErrorInvalid:
		variantValue.destroy()
	case ErrorData:
		variantValue.destroy()
	case ErrorInternal:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerError.Destroy", value))
	}
}

// Storage layer targeted by an explicit flush.
type FlushType uint

const (
	// Flush the active memtable and any immutable memtables to object storage.
	FlushTypeMemTable FlushType = 1
	// Flush the active WAL and any immutable WAL segments to object storage.
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
	// Reads see a stable snapshot without full serializable conflict checking.
	IsolationLevelSnapshot IsolationLevel = 1
	// Reads see a stable snapshot with serializable conflict detection.
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

// Log level used by [`init_logging`].
type LogLevel uint

const (
	// Disable logging.
	LogLevelOff LogLevel = 1
	// Error-level logs only.
	LogLevelError LogLevel = 2
	// Warning and error logs.
	LogLevelWarn LogLevel = 3
	// Info, warning, and error logs.
	LogLevelInfo LogLevel = 4
	// Debug, info, warning, and error logs.
	LogLevelDebug LogLevel = 5
	// Trace and all higher-severity logs.
	LogLevelTrace LogLevel = 6
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

// Error returned by a foreign [`crate::MergeOperator`] implementation.
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
// The merge callback failed with an application-defined message.
type MergeOperatorCallbackErrorFailed struct {
	Message string
}

// The merge callback failed with an application-defined message.
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

// Kind of row entry stored in WAL iteration results.
type RowEntryKind uint

const (
	// A regular value row.
	RowEntryKindValue RowEntryKind = 1
	// A delete tombstone.
	RowEntryKindTombstone RowEntryKind = 2
	// A merge operand row.
	RowEntryKindMerge RowEntryKind = 3
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

// Block size used for newly written SSTable blocks.
type SstBlockSize uint

const (
	// 1 KiB blocks.
	SstBlockSizeBlock1Kib SstBlockSize = 1
	// 2 KiB blocks.
	SstBlockSizeBlock2Kib SstBlockSize = 2
	// 4 KiB blocks.
	SstBlockSizeBlock4Kib SstBlockSize = 3
	// 8 KiB blocks.
	SstBlockSizeBlock8Kib SstBlockSize = 4
	// 16 KiB blocks.
	SstBlockSizeBlock16Kib SstBlockSize = 5
	// 32 KiB blocks.
	SstBlockSizeBlock32Kib SstBlockSize = 6
	// 64 KiB blocks.
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

// Time-to-live policy applied to an inserted value or merge operand.
type Ttl interface {
	Destroy()
}

// Use the database default TTL.
type TtlDefault struct {
}

func (e TtlDefault) Destroy() {
}

// Store the value without expiration.
type TtlNoExpiry struct {
}

func (e TtlNoExpiry) Destroy() {
}

// Expire the value after the given number of clock ticks.
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

type FfiConverterOptionalLogCallback struct{}

var FfiConverterOptionalLogCallbackINSTANCE = FfiConverterOptionalLogCallback{}

func (c FfiConverterOptionalLogCallback) Lift(rb RustBufferI) *LogCallback {
	return LiftFromRustBuffer[*LogCallback](c, rb)
}

func (_ FfiConverterOptionalLogCallback) Read(reader io.Reader) *LogCallback {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterLogCallbackINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalLogCallback) Lower(value *LogCallback) C.RustBuffer {
	return LowerIntoRustBuffer[*LogCallback](c, value)
}

func (c FfiConverterOptionalLogCallback) LowerExternal(value *LogCallback) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*LogCallback](c, value))
}

func (_ FfiConverterOptionalLogCallback) Write(writer io.Writer, value *LogCallback) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterLogCallbackINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalLogCallback struct{}

func (_ FfiDestroyerOptionalLogCallback) Destroy(value *LogCallback) {
	if value != nil {
		FfiDestroyerLogCallback{}.Destroy(*value)
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

//export uniffi_uniffiFutureContinuationCallback
func uniffi_uniffiFutureContinuationCallback(data C.uint64_t, pollResult C.int8_t) {
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
			(C.UniffiRustFutureContinuationCallback)(C.uniffi_uniffiFutureContinuationCallback),
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

//export uniffi_uniffiFreeGorutine
func uniffi_uniffiFreeGorutine(data C.uint64_t) {
	handle := cgo.Handle(uintptr(data))
	defer handle.Delete()

	guard := handle.Value().(chan struct{})
	guard <- struct{}{}
}

// Installs SlateDB logging exactly once for the current process.
//
// If `callback` is provided, log records are forwarded to it. Otherwise logs
// are written to standard error using the default tracing formatter.
func InitLogging(level LogLevel, callback *LogCallback) error {
	_, _uniffiErr := rustCallWithError[Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_func_init_logging(FfiConverterLogLevelINSTANCE.Lower(level), FfiConverterOptionalLogCallbackINSTANCE.Lower(callback), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
