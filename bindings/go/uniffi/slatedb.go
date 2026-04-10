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
	"reflect"
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

func rustCallWithError[E any, U any](converter BufReader[E], callback func(*C.RustCallStatus) U) (U, E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[E], status C.RustCallStatus) E {
	switch status.code {
	case 0:
		var zero E
		return zero
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

	FfiConverterCounterINSTANCE.register()
	FfiConverterGaugeINSTANCE.register()
	FfiConverterHistogramINSTANCE.register()
	FfiConverterLogCallbackINSTANCE.register()
	FfiConverterMergeOperatorINSTANCE.register()
	FfiConverterMetricsRecorderINSTANCE.register()
	FfiConverterUpDownCounterINSTANCE.register()
	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 30
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_slatedb_uniffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("slatedb: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_func_init_logging()
		})
		if checksum != 43029 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_func_init_logging: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_build()
		})
		if checksum != 18005 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled()
		})
		if checksum != 17477 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator()
		})
		if checksum != 5839 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_metrics_recorder()
		})
		if checksum != 18128 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_metrics_recorder: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed()
		})
		if checksum != 58796 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings()
		})
		if checksum != 64263 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size()
		})
		if checksum != 40009 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store()
		})
		if checksum != 4790 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build()
		})
		if checksum != 11741 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id()
		})
		if checksum != 41016 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator()
		})
		if checksum != 63455 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_metrics_recorder()
		})
		if checksum != 20032 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_metrics_recorder: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options()
		})
		if checksum != 46155 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store()
		})
		if checksum != 2290 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_begin()
		})
		if checksum != 38869 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_begin: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_delete()
		})
		if checksum != 4063 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_delete_with_options()
		})
		if checksum != 44744 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_delete_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_flush()
		})
		if checksum != 42157 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_flush: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_flush_with_options()
		})
		if checksum != 27835 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_flush_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get()
		})
		if checksum != 39474 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_key_value()
		})
		if checksum != 35423 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options()
		})
		if checksum != 6898 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_get_with_options()
		})
		if checksum != 20708 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_merge()
		})
		if checksum != 28366 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_merge_with_options()
		})
		if checksum != 15865 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_put()
		})
		if checksum != 53275 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_put_with_options()
		})
		if checksum != 37591 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan()
		})
		if checksum != 60557 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix()
		})
		if checksum != 44288 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options()
		})
		if checksum != 34774 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_scan_with_options()
		})
		if checksum != 63326 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_shutdown()
		})
		if checksum != 3032 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_snapshot()
		})
		if checksum != 53137 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_snapshot: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_status()
		})
		if checksum != 26 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_status: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_write()
		})
		if checksum != 29016 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_db_write_with_options()
		})
		if checksum != 13580 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_db_write_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_get()
		})
		if checksum != 53337 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options()
		})
		if checksum != 22247 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan()
		})
		if checksum != 19340 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix()
		})
		if checksum != 2510 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options()
		})
		if checksum != 46251 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options()
		})
		if checksum != 27137 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown()
		})
		if checksum != 34395 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get()
		})
		if checksum != 52436 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value()
		})
		if checksum != 58808 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options()
		})
		if checksum != 21706 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options()
		})
		if checksum != 58422 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan()
		})
		if checksum != 5467 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix()
		})
		if checksum != 57746 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options()
		})
		if checksum != 4221 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options()
		})
		if checksum != 63293 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit()
		})
		if checksum != 56467 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options()
		})
		if checksum != 62589 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete()
		})
		if checksum != 7933 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get()
		})
		if checksum != 4279 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value()
		})
		if checksum != 51665 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options()
		})
		if checksum != 61936 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options()
		})
		if checksum != 30800 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_id()
		})
		if checksum != 33247 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read()
		})
		if checksum != 33456 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge()
		})
		if checksum != 16664 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options()
		})
		if checksum != 17753 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put()
		})
		if checksum != 56350 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options()
		})
		if checksum != 37260 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback()
		})
		if checksum != 25213 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan()
		})
		if checksum != 2520 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix()
		})
		if checksum != 28799 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options()
		})
		if checksum != 31002 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options()
		})
		if checksum != 5569 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum()
		})
		if checksum != 63575 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write()
		})
		if checksum != 16990 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbiterator_next()
		})
		if checksum != 1225 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_dbiterator_seek()
		})
		if checksum != 61052 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_dbiterator_seek: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_logcallback_log()
		})
		if checksum != 36316 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_logcallback_log: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge()
		})
		if checksum != 48409 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_counter_increment()
		})
		if checksum != 45426 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_counter_increment: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_metric_by_name_and_labels()
		})
		if checksum != 45073 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_metric_by_name_and_labels: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_metrics_by_name()
		})
		if checksum != 7602 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_metrics_by_name: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_counter()
		})
		if checksum != 51600 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_counter: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_gauge()
		})
		if checksum != 34281 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_gauge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_histogram()
		})
		if checksum != 4383 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_histogram: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_up_down_counter()
		})
		if checksum != 19270 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_register_up_down_counter: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_snapshot()
		})
		if checksum != 39221 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_defaultmetricsrecorder_snapshot: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_gauge_set()
		})
		if checksum != 19642 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_gauge_set: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_histogram_record()
		})
		if checksum != 17863 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_histogram_record: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_counter()
		})
		if checksum != 40366 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_counter: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_gauge()
		})
		if checksum != 30425 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_gauge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_up_down_counter()
		})
		if checksum != 64639 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_up_down_counter: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_histogram()
		})
		if checksum != 26503 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_metricsrecorder_register_histogram: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_updowncounter_increment()
		})
		if checksum != 34440 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_updowncounter_increment: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_settings_set()
		})
		if checksum != 34344 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_settings_set: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_settings_to_json_string()
		})
		if checksum != 8458 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_settings_to_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_id()
		})
		if checksum != 62512 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfile_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_iterator()
		})
		if checksum != 46880 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfile_iterator: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_metadata()
		})
		if checksum != 32912 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfile_metadata: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_next_file()
		})
		if checksum != 56800 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfile_next_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfile_next_id()
		})
		if checksum != 48353 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfile_next_id: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walfileiterator_next()
		})
		if checksum != 51490 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walfileiterator_next: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walreader_get()
		})
		if checksum != 11510 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walreader_get: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_walreader_list()
		})
		if checksum != 43661 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_walreader_list: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_delete()
		})
		if checksum != 58549 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_writebatch_delete: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_merge()
		})
		if checksum != 62067 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_writebatch_merge: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options()
		})
		if checksum != 24696 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_put()
		})
		if checksum != 48246 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_writebatch_put: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options()
		})
		if checksum != 31177 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new()
		})
		if checksum != 60260 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new()
		})
		if checksum != 20397 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_defaultmetricsrecorder_new()
		})
		if checksum != 31165 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_defaultmetricsrecorder_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env()
		})
		if checksum != 61956 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve()
		})
		if checksum != 17196 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_default()
		})
		if checksum != 64704 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env()
		})
		if checksum != 28170 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_from_env: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default()
		})
		if checksum != 3189 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_file()
		})
		if checksum != 38462 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_from_file: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string()
		})
		if checksum != 38360 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_settings_load()
		})
		if checksum != 7949 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_settings_load: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_walreader_new()
		})
		if checksum != 30537 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_walreader_new: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_slatedb_uniffi_checksum_constructor_writebatch_new()
		})
		if checksum != 2056 {
			// If this happens try cleaning and rebuilding your project
			panic("slatedb: uniffi_slatedb_uniffi_checksum_constructor_writebatch_new: UniFFI API checksum mismatch")
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

type FfiConverterFloat64 struct{}

var FfiConverterFloat64INSTANCE = FfiConverterFloat64{}

func (FfiConverterFloat64) Lower(value float64) C.double {
	return C.double(value)
}

func (FfiConverterFloat64) Write(writer io.Writer, value float64) {
	writeFloat64(writer, value)
}

func (FfiConverterFloat64) Lift(value C.double) float64 {
	return float64(value)
}

func (FfiConverterFloat64) Read(reader io.Reader) float64 {
	return readFloat64(reader)
}

type FfiDestroyerFloat64 struct{}

func (FfiDestroyerFloat64) Destroy(_ float64) {}

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
	handle        C.uint64_t
	callCounter   atomic.Int64
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t
	freeFunction  func(C.uint64_t, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	handle C.uint64_t,
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t,
	freeFunction func(C.uint64_t, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		handle:        handle,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) C.uint64_t {
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

	return rustCall(func(status *C.RustCallStatus) C.uint64_t {
		return ffiObject.cloneFunction(ffiObject.handle, status)
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
	if ffiObject.handle == 0 {
		return
	}
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.handle, status)
		return 0
	})
}

// Handle for a monotonic counter metric.
type Counter interface {
	// Adds `value` to the counter.
	Increment(value uint64)
}

// Handle for a monotonic counter metric.
type CounterImpl struct {
	ffiObject FfiObject
}

// Adds `value` to the counter.
func (_self *CounterImpl) Increment(value uint64) {
	_pointer := _self.ffiObject.incrementPointer("Counter")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_counter_increment(
			_pointer, FfiConverterUint64INSTANCE.Lower(value), _uniffiStatus)
		return false
	})
}
func (object *CounterImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterCounter struct {
	handleMap *concurrentHandleMap[Counter]
}

var FfiConverterCounterINSTANCE = FfiConverterCounter{
	handleMap: newConcurrentHandleMap[Counter](),
}

func (c FfiConverterCounter) Lift(handle C.uint64_t) Counter {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &CounterImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_counter(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_counter(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*CounterImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterCounter) Read(reader io.Reader) Counter {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterCounter) Lower(value Counter) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*CounterImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("Counter")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterCounter) Write(writer io.Writer, value Counter) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalCounter(handle uint64) Counter {
	return FfiConverterCounterINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalCounter(value Counter) uint64 {
	return uint64(FfiConverterCounterINSTANCE.Lower(value))
}

type FfiDestroyerCounter struct{}

func (_ FfiDestroyerCounter) Destroy(value Counter) {
	if val, ok := value.(*CounterImpl); ok {
		val.Destroy()
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
		handles:       map[uint64]T{},
		currentHandle: 1,
	}
}

func (cm *concurrentHandleMap[T]) insert(obj T) uint64 {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	handle := cm.currentHandle
	cm.currentHandle = cm.currentHandle + 2
	cm.handles[handle] = obj
	return handle
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

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterMethod0
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterMethod0(uniffiHandle C.uint64_t, value C.uint64_t, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterCounterINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Increment(
		FfiConverterUint64INSTANCE.Lift(value),
	)

}

var UniffiVTableCallbackInterfaceCounterINSTANCE = C.UniffiVTableCallbackInterfaceCounter{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterClone),
	increment:   (C.UniffiCallbackInterfaceCounterMethod0)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterMethod0),
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterFree
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterFree(handle C.uint64_t) {
	FfiConverterCounterINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterClone
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceCounterClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterCounterINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterCounterINSTANCE.handleMap.insert(val))
}

func (c FfiConverterCounter) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_counter(&UniffiVTableCallbackInterfaceCounterINSTANCE)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbTransaction {
			return FfiConverterDbTransactionINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_begin(
			_pointer, FfiConverterIsolationLevelINSTANCE.Lower(isolationLevel)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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

// Inserts or overwrites a value and returns metadata for the write.
//
// Keys must be non-empty and at most `u16::MAX` bytes. Values must be at
// most `u32::MAX` bytes.
func (_self *Db) Put(key []byte, value []byte) (WriteHandle, error) {
	_pointer := _self.ffiObject.incrementPointer("*Db")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbSnapshot {
			return FfiConverterDbSnapshotINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_db_snapshot(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterDb) Lift(handle C.uint64_t) *Db {
	result := &Db{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_db(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_db(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Db).Destroy)
	return result
}

func (c FfiConverterDb) Read(reader io.Reader) *Db {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDb) Lower(value *Db) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Db")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDb) Write(writer io.Writer, value *Db) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDb(handle uint64) *Db {
	return FfiConverterDbINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDb(value *Db) uint64 {
	return uint64(FfiConverterDbINSTANCE.Lower(value))
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
	// Installs an application-defined metrics recorder.
	WithMetricsRecorder(metricsRecorder MetricsRecorder) error
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
	return FfiConverterDbBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_dbbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Opens the database and consumes this builder.
func (_self *DbBuilder) Build() (*Db, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *Db {
			return FfiConverterDbINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_merge_operator(
			_pointer, FfiConverterMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Installs an application-defined metrics recorder.
func (_self *DbBuilder) WithMetricsRecorder(metricsRecorder MetricsRecorder) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbbuilder_with_metrics_recorder(
			_pointer, FfiConverterMetricsRecorderINSTANCE.Lower(metricsRecorder), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Sets the seed used for SlateDB's internal random number generation.
func (_self *DbBuilder) WithSeed(seed uint64) error {
	_pointer := _self.ffiObject.incrementPointer("*DbBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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

func (c FfiConverterDbBuilder) Lift(handle C.uint64_t) *DbBuilder {
	result := &DbBuilder{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbbuilder(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbbuilder(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbBuilder).Destroy)
	return result
}

func (c FfiConverterDbBuilder) Read(reader io.Reader) *DbBuilder {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbBuilder) Lower(value *DbBuilder) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbBuilder")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbBuilder) Write(writer io.Writer, value *DbBuilder) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbBuilder(handle uint64) *DbBuilder {
	return FfiConverterDbBuilderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbBuilder(value *DbBuilder) uint64 {
	return uint64(FfiConverterDbBuilderINSTANCE.Lower(value))
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
	res, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterDbIterator) Lift(handle C.uint64_t) *DbIterator {
	result := &DbIterator{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbiterator(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbiterator(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbIterator).Destroy)
	return result
}

func (c FfiConverterDbIterator) Read(reader io.Reader) *DbIterator {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbIterator) Lower(value *DbIterator) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbIterator")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbIterator) Write(writer io.Writer, value *DbIterator) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbIterator(handle uint64) *DbIterator {
	return FfiConverterDbIteratorINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbIterator(value *DbIterator) uint64 {
	return uint64(FfiConverterDbIteratorINSTANCE.Lower(value))
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreader_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterDbReader) Lift(handle C.uint64_t) *DbReader {
	result := &DbReader{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbreader(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbreader(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbReader).Destroy)
	return result
}

func (c FfiConverterDbReader) Read(reader io.Reader) *DbReader {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbReader) Lower(value *DbReader) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbReader")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbReader) Write(writer io.Writer, value *DbReader) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbReader(handle uint64) *DbReader {
	return FfiConverterDbReaderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbReader(value *DbReader) uint64 {
	return uint64(FfiConverterDbReaderINSTANCE.Lower(value))
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
	// Installs an application-defined metrics recorder.
	WithMetricsRecorder(metricsRecorder MetricsRecorder) error
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
	return FfiConverterDbReaderBuilderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_dbreaderbuilder_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Opens the reader and consumes this builder.
func (_self *DbReaderBuilder) Build() (*DbReader, error) {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbReader {
			return FfiConverterDbReaderINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_build(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_merge_operator(
			_pointer, FfiConverterMergeOperatorINSTANCE.Lower(mergeOperator), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Installs an application-defined metrics recorder.
func (_self *DbReaderBuilder) WithMetricsRecorder(metricsRecorder MetricsRecorder) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_metrics_recorder(
			_pointer, FfiConverterMetricsRecorderINSTANCE.Lower(metricsRecorder), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

// Applies custom reader options.
func (_self *DbReaderBuilder) WithOptions(options ReaderOptions) error {
	_pointer := _self.ffiObject.incrementPointer("*DbReaderBuilder")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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

func (c FfiConverterDbReaderBuilder) Lift(handle C.uint64_t) *DbReaderBuilder {
	result := &DbReaderBuilder{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbreaderbuilder(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbreaderbuilder(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbReaderBuilder).Destroy)
	return result
}

func (c FfiConverterDbReaderBuilder) Read(reader io.Reader) *DbReaderBuilder {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbReaderBuilder) Lower(value *DbReaderBuilder) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbReaderBuilder")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbReaderBuilder) Write(writer io.Writer, value *DbReaderBuilder) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbReaderBuilder(handle uint64) *DbReaderBuilder {
	return FfiConverterDbReaderBuilderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbReaderBuilder(value *DbReaderBuilder) uint64 {
	return uint64(FfiConverterDbReaderBuilderINSTANCE.Lower(value))
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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

func (c FfiConverterDbSnapshot) Lift(handle C.uint64_t) *DbSnapshot {
	result := &DbSnapshot{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbsnapshot(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbsnapshot(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbSnapshot).Destroy)
	return result
}

func (c FfiConverterDbSnapshot) Read(reader io.Reader) *DbSnapshot {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbSnapshot) Lower(value *DbSnapshot) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbSnapshot")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbSnapshot) Write(writer io.Writer, value *DbSnapshot) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbSnapshot(handle uint64) *DbSnapshot {
	return FfiConverterDbSnapshotINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbSnapshot(value *DbSnapshot) uint64 {
	return uint64(FfiConverterDbSnapshotINSTANCE.Lower(value))
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	_, err := uniffiRustCallAsync[*Error](
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_prefix(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_prefix_with_options(
			_pointer, FfiConverterBytesINSTANCE.Lower(prefix), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *DbIterator {
			return FfiConverterDbIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_dbtransaction_scan_with_options(
			_pointer, FfiConverterKeyRangeINSTANCE.Lower(varRange), FfiConverterScanOptionsINSTANCE.Lower(options)),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	_, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterDbTransaction) Lift(handle C.uint64_t) *DbTransaction {
	result := &DbTransaction{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_dbtransaction(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_dbtransaction(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DbTransaction).Destroy)
	return result
}

func (c FfiConverterDbTransaction) Read(reader io.Reader) *DbTransaction {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDbTransaction) Lower(value *DbTransaction) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DbTransaction")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDbTransaction) Write(writer io.Writer, value *DbTransaction) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDbTransaction(handle uint64) *DbTransaction {
	return FfiConverterDbTransactionINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDbTransaction(value *DbTransaction) uint64 {
	return uint64(FfiConverterDbTransactionINSTANCE.Lower(value))
}

type FfiDestroyerDbTransaction struct{}

func (_ FfiDestroyerDbTransaction) Destroy(value *DbTransaction) {
	value.Destroy()
}

// Built-in atomic-backed metrics recorder with snapshot access.
type DefaultMetricsRecorderInterface interface {
	// Returns the metric matching `name` and the exact label set, if present.
	MetricByNameAndLabels(name string, labels []MetricLabel) *Metric
	// Returns every metric with the requested name.
	MetricsByName(name string) []Metric
	RegisterCounter(name string, description string, labels []MetricLabel) Counter
	RegisterGauge(name string, description string, labels []MetricLabel) Gauge
	RegisterHistogram(name string, description string, labels []MetricLabel, boundaries []float64) Histogram
	RegisterUpDownCounter(name string, description string, labels []MetricLabel) UpDownCounter
	// Returns a point-in-time snapshot of every registered metric.
	Snapshot() []Metric
}

// Built-in atomic-backed metrics recorder with snapshot access.
type DefaultMetricsRecorder struct {
	ffiObject FfiObject
}

// Creates an empty default metrics recorder.
func NewDefaultMetricsRecorder() *DefaultMetricsRecorder {
	return FfiConverterDefaultMetricsRecorderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_defaultmetricsrecorder_new(_uniffiStatus)
	}))
}

// Returns the metric matching `name` and the exact label set, if present.
func (_self *DefaultMetricsRecorder) MetricByNameAndLabels(name string, labels []MetricLabel) *Metric {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterOptionalMetricINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_metric_by_name_and_labels(
				_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus),
		}
	}))
}

// Returns every metric with the requested name.
func (_self *DefaultMetricsRecorder) MetricsByName(name string) []Metric {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSequenceMetricINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_metrics_by_name(
				_pointer, FfiConverterStringINSTANCE.Lower(name), _uniffiStatus),
		}
	}))
}

func (_self *DefaultMetricsRecorder) RegisterCounter(name string, description string, labels []MetricLabel) Counter {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterCounterINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_register_counter(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

func (_self *DefaultMetricsRecorder) RegisterGauge(name string, description string, labels []MetricLabel) Gauge {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterGaugeINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_register_gauge(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

func (_self *DefaultMetricsRecorder) RegisterHistogram(name string, description string, labels []MetricLabel, boundaries []float64) Histogram {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterHistogramINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_register_histogram(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), FfiConverterSequenceFloat64INSTANCE.Lower(boundaries), _uniffiStatus)
	}))
}

func (_self *DefaultMetricsRecorder) RegisterUpDownCounter(name string, description string, labels []MetricLabel) UpDownCounter {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUpDownCounterINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_register_up_down_counter(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

// Returns a point-in-time snapshot of every registered metric.
func (_self *DefaultMetricsRecorder) Snapshot() []Metric {
	_pointer := _self.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterSequenceMetricINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer{
			inner: C.uniffi_slatedb_uniffi_fn_method_defaultmetricsrecorder_snapshot(
				_pointer, _uniffiStatus),
		}
	}))
}
func (object *DefaultMetricsRecorder) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterDefaultMetricsRecorder struct{}

var FfiConverterDefaultMetricsRecorderINSTANCE = FfiConverterDefaultMetricsRecorder{}

func (c FfiConverterDefaultMetricsRecorder) Lift(handle C.uint64_t) *DefaultMetricsRecorder {
	result := &DefaultMetricsRecorder{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_defaultmetricsrecorder(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_defaultmetricsrecorder(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*DefaultMetricsRecorder).Destroy)
	return result
}

func (c FfiConverterDefaultMetricsRecorder) Read(reader io.Reader) *DefaultMetricsRecorder {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterDefaultMetricsRecorder) Lower(value *DefaultMetricsRecorder) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*DefaultMetricsRecorder")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterDefaultMetricsRecorder) Write(writer io.Writer, value *DefaultMetricsRecorder) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalDefaultMetricsRecorder(handle uint64) *DefaultMetricsRecorder {
	return FfiConverterDefaultMetricsRecorderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalDefaultMetricsRecorder(value *DefaultMetricsRecorder) uint64 {
	return uint64(FfiConverterDefaultMetricsRecorderINSTANCE.Lower(value))
}

type FfiDestroyerDefaultMetricsRecorder struct{}

func (_ FfiDestroyerDefaultMetricsRecorder) Destroy(value *DefaultMetricsRecorder) {
	value.Destroy()
}

// Handle for a gauge metric.
type Gauge interface {
	// Sets the gauge to `value`.
	Set(value int64)
}

// Handle for a gauge metric.
type GaugeImpl struct {
	ffiObject FfiObject
}

// Sets the gauge to `value`.
func (_self *GaugeImpl) Set(value int64) {
	_pointer := _self.ffiObject.incrementPointer("Gauge")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_gauge_set(
			_pointer, FfiConverterInt64INSTANCE.Lower(value), _uniffiStatus)
		return false
	})
}
func (object *GaugeImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterGauge struct {
	handleMap *concurrentHandleMap[Gauge]
}

var FfiConverterGaugeINSTANCE = FfiConverterGauge{
	handleMap: newConcurrentHandleMap[Gauge](),
}

func (c FfiConverterGauge) Lift(handle C.uint64_t) Gauge {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &GaugeImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_gauge(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_gauge(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*GaugeImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterGauge) Read(reader io.Reader) Gauge {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterGauge) Lower(value Gauge) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*GaugeImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("Gauge")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterGauge) Write(writer io.Writer, value Gauge) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalGauge(handle uint64) Gauge {
	return FfiConverterGaugeINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalGauge(value Gauge) uint64 {
	return uint64(FfiConverterGaugeINSTANCE.Lower(value))
}

type FfiDestroyerGauge struct{}

func (_ FfiDestroyerGauge) Destroy(value Gauge) {
	if val, ok := value.(*GaugeImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeMethod0
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeMethod0(uniffiHandle C.uint64_t, value C.int64_t, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterGaugeINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Set(
		FfiConverterInt64INSTANCE.Lift(value),
	)

}

var UniffiVTableCallbackInterfaceGaugeINSTANCE = C.UniffiVTableCallbackInterfaceGauge{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeClone),
	set:         (C.UniffiCallbackInterfaceGaugeMethod0)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeMethod0),
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeFree
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeFree(handle C.uint64_t) {
	FfiConverterGaugeINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeClone
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceGaugeClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterGaugeINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterGaugeINSTANCE.handleMap.insert(val))
}

func (c FfiConverterGauge) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_gauge(&UniffiVTableCallbackInterfaceGaugeINSTANCE)
}

// Handle for a histogram metric.
type Histogram interface {
	// Records `value` in the histogram.
	Record(value float64)
}

// Handle for a histogram metric.
type HistogramImpl struct {
	ffiObject FfiObject
}

// Records `value` in the histogram.
func (_self *HistogramImpl) Record(value float64) {
	_pointer := _self.ffiObject.incrementPointer("Histogram")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_histogram_record(
			_pointer, FfiConverterFloat64INSTANCE.Lower(value), _uniffiStatus)
		return false
	})
}
func (object *HistogramImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterHistogram struct {
	handleMap *concurrentHandleMap[Histogram]
}

var FfiConverterHistogramINSTANCE = FfiConverterHistogram{
	handleMap: newConcurrentHandleMap[Histogram](),
}

func (c FfiConverterHistogram) Lift(handle C.uint64_t) Histogram {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &HistogramImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_histogram(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_histogram(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*HistogramImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterHistogram) Read(reader io.Reader) Histogram {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterHistogram) Lower(value Histogram) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*HistogramImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("Histogram")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterHistogram) Write(writer io.Writer, value Histogram) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalHistogram(handle uint64) Histogram {
	return FfiConverterHistogramINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalHistogram(value Histogram) uint64 {
	return uint64(FfiConverterHistogramINSTANCE.Lower(value))
}

type FfiDestroyerHistogram struct{}

func (_ FfiDestroyerHistogram) Destroy(value Histogram) {
	if val, ok := value.(*HistogramImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramMethod0
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramMethod0(uniffiHandle C.uint64_t, value C.double, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterHistogramINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Record(
		FfiConverterFloat64INSTANCE.Lift(value),
	)

}

var UniffiVTableCallbackInterfaceHistogramINSTANCE = C.UniffiVTableCallbackInterfaceHistogram{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramClone),
	record:      (C.UniffiCallbackInterfaceHistogramMethod0)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramMethod0),
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramFree
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramFree(handle C.uint64_t) {
	FfiConverterHistogramINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramClone
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceHistogramClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterHistogramINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterHistogramINSTANCE.handleMap.insert(val))
}

func (c FfiConverterHistogram) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_histogram(&UniffiVTableCallbackInterfaceHistogramINSTANCE)
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

func (c FfiConverterLogCallback) Lift(handle C.uint64_t) LogCallback {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &LogCallbackImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_logcallback(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_logcallback(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*LogCallbackImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterLogCallback) Read(reader io.Reader) LogCallback {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterLogCallback) Lower(value LogCallback) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*LogCallbackImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("LogCallback")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterLogCallback) Write(writer io.Writer, value LogCallback) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalLogCallback(handle uint64) LogCallback {
	return FfiConverterLogCallbackINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalLogCallback(value LogCallback) uint64 {
	return uint64(FfiConverterLogCallbackINSTANCE.Lower(value))
}

type FfiDestroyerLogCallback struct{}

func (_ FfiDestroyerLogCallback) Destroy(value LogCallback) {
	if val, ok := value.(*LogCallbackImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackMethod0
func slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackMethod0(uniffiHandle C.uint64_t, record C.RustBuffer, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
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
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackClone),
	log:         (C.UniffiCallbackInterfaceLogCallbackMethod0)(C.slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackMethod0),
}

//export slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackFree
func slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackFree(handle C.uint64_t) {
	FfiConverterLogCallbackINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackClone
func slatedb_uniffi_logging_cgo_dispatchCallbackInterfaceLogCallbackClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterLogCallbackINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterLogCallbackINSTANCE.handleMap.insert(val))
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
	_uniffiRV, _uniffiErr := rustCallWithError[*MergeOperatorCallbackError](FfiConverterMergeOperatorCallbackError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
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

func (c FfiConverterMergeOperator) Lift(handle C.uint64_t) MergeOperator {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &MergeOperatorImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_mergeoperator(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_mergeoperator(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*MergeOperatorImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterMergeOperator) Read(reader io.Reader) MergeOperator {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterMergeOperator) Lower(value MergeOperator) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*MergeOperatorImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("MergeOperator")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterMergeOperator) Write(writer io.Writer, value MergeOperator) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalMergeOperator(handle uint64) MergeOperator {
	return FfiConverterMergeOperatorINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalMergeOperator(value MergeOperator) uint64 {
	return uint64(FfiConverterMergeOperatorINSTANCE.Lower(value))
}

type FfiDestroyerMergeOperator struct{}

func (_ FfiDestroyerMergeOperator) Destroy(value MergeOperator) {
	if val, ok := value.(*MergeOperatorImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorMethod0
func slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorMethod0(uniffiHandle C.uint64_t, key C.RustBuffer, existingValue C.RustBuffer, operand C.RustBuffer, uniffiOutReturn *C.RustBuffer, callStatus *C.RustCallStatus) {
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
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorClone),
	merge:       (C.UniffiCallbackInterfaceMergeOperatorMethod0)(C.slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorMethod0),
}

//export slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorFree
func slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorFree(handle C.uint64_t) {
	FfiConverterMergeOperatorINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorClone
func slatedb_uniffi_merge_operator_cgo_dispatchCallbackInterfaceMergeOperatorClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterMergeOperatorINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterMergeOperatorINSTANCE.handleMap.insert(val))
}

func (c FfiConverterMergeOperator) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_mergeoperator(&UniffiVTableCallbackInterfaceMergeOperatorINSTANCE)
}

// Application-defined metrics recorder used to publish SlateDB metrics.
type MetricsRecorder interface {
	// Registers a monotonically increasing counter.
	RegisterCounter(name string, description string, labels []MetricLabel) Counter
	// Registers a gauge.
	RegisterGauge(name string, description string, labels []MetricLabel) Gauge
	// Registers an up/down counter.
	RegisterUpDownCounter(name string, description string, labels []MetricLabel) UpDownCounter
	// Registers a histogram with explicit bucket boundaries.
	RegisterHistogram(name string, description string, labels []MetricLabel, boundaries []float64) Histogram
}

// Application-defined metrics recorder used to publish SlateDB metrics.
type MetricsRecorderImpl struct {
	ffiObject FfiObject
}

// Registers a monotonically increasing counter.
func (_self *MetricsRecorderImpl) RegisterCounter(name string, description string, labels []MetricLabel) Counter {
	_pointer := _self.ffiObject.incrementPointer("MetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterCounterINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_metricsrecorder_register_counter(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

// Registers a gauge.
func (_self *MetricsRecorderImpl) RegisterGauge(name string, description string, labels []MetricLabel) Gauge {
	_pointer := _self.ffiObject.incrementPointer("MetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterGaugeINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_metricsrecorder_register_gauge(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

// Registers an up/down counter.
func (_self *MetricsRecorderImpl) RegisterUpDownCounter(name string, description string, labels []MetricLabel) UpDownCounter {
	_pointer := _self.ffiObject.incrementPointer("MetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUpDownCounterINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_metricsrecorder_register_up_down_counter(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), _uniffiStatus)
	}))
}

// Registers a histogram with explicit bucket boundaries.
func (_self *MetricsRecorderImpl) RegisterHistogram(name string, description string, labels []MetricLabel, boundaries []float64) Histogram {
	_pointer := _self.ffiObject.incrementPointer("MetricsRecorder")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterHistogramINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_method_metricsrecorder_register_histogram(
			_pointer, FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(description), FfiConverterSequenceMetricLabelINSTANCE.Lower(labels), FfiConverterSequenceFloat64INSTANCE.Lower(boundaries), _uniffiStatus)
	}))
}
func (object *MetricsRecorderImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterMetricsRecorder struct {
	handleMap *concurrentHandleMap[MetricsRecorder]
}

var FfiConverterMetricsRecorderINSTANCE = FfiConverterMetricsRecorder{
	handleMap: newConcurrentHandleMap[MetricsRecorder](),
}

func (c FfiConverterMetricsRecorder) Lift(handle C.uint64_t) MetricsRecorder {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &MetricsRecorderImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_metricsrecorder(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_metricsrecorder(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*MetricsRecorderImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterMetricsRecorder) Read(reader io.Reader) MetricsRecorder {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterMetricsRecorder) Lower(value MetricsRecorder) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*MetricsRecorderImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("MetricsRecorder")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterMetricsRecorder) Write(writer io.Writer, value MetricsRecorder) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalMetricsRecorder(handle uint64) MetricsRecorder {
	return FfiConverterMetricsRecorderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalMetricsRecorder(value MetricsRecorder) uint64 {
	return uint64(FfiConverterMetricsRecorderINSTANCE.Lower(value))
}

type FfiDestroyerMetricsRecorder struct{}

func (_ FfiDestroyerMetricsRecorder) Destroy(value MetricsRecorder) {
	if val, ok := value.(*MetricsRecorderImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod0
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod0(uniffiHandle C.uint64_t, name C.RustBuffer, description C.RustBuffer, labels C.RustBuffer, uniffiOutReturn *C.uint64_t, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterMetricsRecorderINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	res :=
		uniffiObj.RegisterCounter(
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: name,
			}),
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: description,
			}),
			FfiConverterSequenceMetricLabelINSTANCE.Lift(GoRustBuffer{
				inner: labels,
			}),
		)

	*uniffiOutReturn = FfiConverterCounterINSTANCE.Lower(res)
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod1
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod1(uniffiHandle C.uint64_t, name C.RustBuffer, description C.RustBuffer, labels C.RustBuffer, uniffiOutReturn *C.uint64_t, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterMetricsRecorderINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	res :=
		uniffiObj.RegisterGauge(
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: name,
			}),
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: description,
			}),
			FfiConverterSequenceMetricLabelINSTANCE.Lift(GoRustBuffer{
				inner: labels,
			}),
		)

	*uniffiOutReturn = FfiConverterGaugeINSTANCE.Lower(res)
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod2
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod2(uniffiHandle C.uint64_t, name C.RustBuffer, description C.RustBuffer, labels C.RustBuffer, uniffiOutReturn *C.uint64_t, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterMetricsRecorderINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	res :=
		uniffiObj.RegisterUpDownCounter(
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: name,
			}),
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: description,
			}),
			FfiConverterSequenceMetricLabelINSTANCE.Lift(GoRustBuffer{
				inner: labels,
			}),
		)

	*uniffiOutReturn = FfiConverterUpDownCounterINSTANCE.Lower(res)
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod3
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod3(uniffiHandle C.uint64_t, name C.RustBuffer, description C.RustBuffer, labels C.RustBuffer, boundaries C.RustBuffer, uniffiOutReturn *C.uint64_t, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterMetricsRecorderINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	res :=
		uniffiObj.RegisterHistogram(
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: name,
			}),
			FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: description,
			}),
			FfiConverterSequenceMetricLabelINSTANCE.Lift(GoRustBuffer{
				inner: labels,
			}),
			FfiConverterSequenceFloat64INSTANCE.Lift(GoRustBuffer{
				inner: boundaries,
			}),
		)

	*uniffiOutReturn = FfiConverterHistogramINSTANCE.Lower(res)
}

var UniffiVTableCallbackInterfaceMetricsRecorderINSTANCE = C.UniffiVTableCallbackInterfaceMetricsRecorder{
	uniffiFree:            (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderFree),
	uniffiClone:           (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderClone),
	registerCounter:       (C.UniffiCallbackInterfaceMetricsRecorderMethod0)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod0),
	registerGauge:         (C.UniffiCallbackInterfaceMetricsRecorderMethod1)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod1),
	registerUpDownCounter: (C.UniffiCallbackInterfaceMetricsRecorderMethod2)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod2),
	registerHistogram:     (C.UniffiCallbackInterfaceMetricsRecorderMethod3)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderMethod3),
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderFree
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderFree(handle C.uint64_t) {
	FfiConverterMetricsRecorderINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderClone
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceMetricsRecorderClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterMetricsRecorderINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterMetricsRecorderINSTANCE.handleMap.insert(val))
}

func (c FfiConverterMetricsRecorder) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_metricsrecorder(&UniffiVTableCallbackInterfaceMetricsRecorderINSTANCE)
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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

func (c FfiConverterObjectStore) Lift(handle C.uint64_t) *ObjectStore {
	result := &ObjectStore{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_objectstore(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_objectstore(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ObjectStore).Destroy)
	return result
}

func (c FfiConverterObjectStore) Read(reader io.Reader) *ObjectStore {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterObjectStore) Lower(value *ObjectStore) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*ObjectStore")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterObjectStore) Write(writer io.Writer, value *ObjectStore) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalObjectStore(handle uint64) *ObjectStore {
	return FfiConverterObjectStoreINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalObjectStore(value *ObjectStore) uint64 {
	return uint64(FfiConverterObjectStoreINSTANCE.Lower(value))
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
	return FfiConverterSettingsINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_settings_default(_uniffiStatus)
	}))
}

// Loads settings from environment variables using `prefix`.
func SettingsFromEnv(prefix string) (*Settings, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_uniffiRV, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) RustBufferI {
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

func (c FfiConverterSettings) Lift(handle C.uint64_t) *Settings {
	result := &Settings{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_settings(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_settings(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Settings).Destroy)
	return result
}

func (c FfiConverterSettings) Read(reader io.Reader) *Settings {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterSettings) Lower(value *Settings) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Settings")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterSettings) Write(writer io.Writer, value *Settings) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalSettings(handle uint64) *Settings {
	return FfiConverterSettingsINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalSettings(value *Settings) uint64 {
	return uint64(FfiConverterSettingsINSTANCE.Lower(value))
}

type FfiDestroyerSettings struct{}

func (_ FfiDestroyerSettings) Destroy(value *Settings) {
	value.Destroy()
}

// Handle for an up/down counter metric.
type UpDownCounter interface {
	// Adds `value` to the counter.
	Increment(value int64)
}

// Handle for an up/down counter metric.
type UpDownCounterImpl struct {
	ffiObject FfiObject
}

// Adds `value` to the counter.
func (_self *UpDownCounterImpl) Increment(value int64) {
	_pointer := _self.ffiObject.incrementPointer("UpDownCounter")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_method_updowncounter_increment(
			_pointer, FfiConverterInt64INSTANCE.Lower(value), _uniffiStatus)
		return false
	})
}
func (object *UpDownCounterImpl) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterUpDownCounter struct {
	handleMap *concurrentHandleMap[UpDownCounter]
}

var FfiConverterUpDownCounterINSTANCE = FfiConverterUpDownCounter{
	handleMap: newConcurrentHandleMap[UpDownCounter](),
}

func (c FfiConverterUpDownCounter) Lift(handle C.uint64_t) UpDownCounter {
	if uint64(handle)&1 == 0 {
		// Rust-generated handle (even), construct a new object wrapping the handle
		result := &UpDownCounterImpl{
			newFfiObject(
				handle,
				func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
					return C.uniffi_slatedb_uniffi_fn_clone_updowncounter(handle, status)
				},
				func(handle C.uint64_t, status *C.RustCallStatus) {
					C.uniffi_slatedb_uniffi_fn_free_updowncounter(handle, status)
				},
			),
		}
		runtime.SetFinalizer(result, (*UpDownCounterImpl).Destroy)
		return result
	} else {
		// Go-generated handle (odd), retrieve from the handle map
		val, ok := c.handleMap.tryGet(uint64(handle))
		if !ok {
			panic(fmt.Errorf("no callback in handle map: %d", handle))
		}
		c.handleMap.remove(uint64(handle))
		return val
	}
}

func (c FfiConverterUpDownCounter) Read(reader io.Reader) UpDownCounter {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterUpDownCounter) Lower(value UpDownCounter) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	if val, ok := value.(*UpDownCounterImpl); ok {
		// Rust-backed object, clone the handle
		handle := val.ffiObject.incrementPointer("UpDownCounter")
		defer val.ffiObject.decrementPointer()
		return handle
	} else {
		// Go-backed object, insert into handle map
		return C.uint64_t(c.handleMap.insert(value))
	}
}

func (c FfiConverterUpDownCounter) Write(writer io.Writer, value UpDownCounter) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalUpDownCounter(handle uint64) UpDownCounter {
	return FfiConverterUpDownCounterINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalUpDownCounter(value UpDownCounter) uint64 {
	return uint64(FfiConverterUpDownCounterINSTANCE.Lower(value))
}

type FfiDestroyerUpDownCounter struct{}

func (_ FfiDestroyerUpDownCounter) Destroy(value UpDownCounter) {
	if val, ok := value.(*UpDownCounterImpl); ok {
		val.Destroy()
	}
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterMethod0
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterMethod0(uniffiHandle C.uint64_t, value C.int64_t, uniffiOutReturn *C.void, callStatus *C.RustCallStatus) {
	handle := uint64(uniffiHandle)
	uniffiObj, ok := FfiConverterUpDownCounterINSTANCE.handleMap.tryGet(handle)
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}

	uniffiObj.Increment(
		FfiConverterInt64INSTANCE.Lift(value),
	)

}

var UniffiVTableCallbackInterfaceUpDownCounterINSTANCE = C.UniffiVTableCallbackInterfaceUpDownCounter{
	uniffiFree:  (C.UniffiCallbackInterfaceFree)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterFree),
	uniffiClone: (C.UniffiCallbackInterfaceClone)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterClone),
	increment:   (C.UniffiCallbackInterfaceUpDownCounterMethod0)(C.slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterMethod0),
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterFree
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterFree(handle C.uint64_t) {
	FfiConverterUpDownCounterINSTANCE.handleMap.remove(uint64(handle))
}

//export slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterClone
func slatedb_uniffi_metrics_cgo_dispatchCallbackInterfaceUpDownCounterClone(handle C.uint64_t) C.uint64_t {
	val, ok := FfiConverterUpDownCounterINSTANCE.handleMap.tryGet(uint64(handle))
	if !ok {
		panic(fmt.Errorf("no callback in handle map: %d", handle))
	}
	return C.uint64_t(FfiConverterUpDownCounterINSTANCE.handleMap.insert(val))
}

func (c FfiConverterUpDownCounter) register() {
	C.uniffi_slatedb_uniffi_fn_init_callback_vtable_updowncounter(&UniffiVTableCallbackInterfaceUpDownCounterINSTANCE)
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
	res, err := uniffiRustCallAsync[*Error](
		FfiConverterErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_slatedb_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *WalFileIterator {
			return FfiConverterWalFileIteratorINSTANCE.Lift(ffi)
		},
		C.uniffi_slatedb_uniffi_fn_method_walfile_iterator(
			_pointer),
		// pollFn
		func(handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func(handle C.uint64_t) {
			C.ffi_slatedb_uniffi_rust_future_free_u64(handle)
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
	res, err := uniffiRustCallAsync[*Error](
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
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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

func (c FfiConverterWalFile) Lift(handle C.uint64_t) *WalFile {
	result := &WalFile{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_walfile(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walfile(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalFile).Destroy)
	return result
}

func (c FfiConverterWalFile) Read(reader io.Reader) *WalFile {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterWalFile) Lower(value *WalFile) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*WalFile")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterWalFile) Write(writer io.Writer, value *WalFile) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalWalFile(handle uint64) *WalFile {
	return FfiConverterWalFileINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalWalFile(value *WalFile) uint64 {
	return uint64(FfiConverterWalFileINSTANCE.Lower(value))
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
	res, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterWalFileIterator) Lift(handle C.uint64_t) *WalFileIterator {
	result := &WalFileIterator{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_walfileiterator(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walfileiterator(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalFileIterator).Destroy)
	return result
}

func (c FfiConverterWalFileIterator) Read(reader io.Reader) *WalFileIterator {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterWalFileIterator) Lower(value *WalFileIterator) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*WalFileIterator")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterWalFileIterator) Write(writer io.Writer, value *WalFileIterator) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalWalFileIterator(handle uint64) *WalFileIterator {
	return FfiConverterWalFileIteratorINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalWalFileIterator(value *WalFileIterator) uint64 {
	return uint64(FfiConverterWalFileIteratorINSTANCE.Lower(value))
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
	return FfiConverterWalReaderINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_walreader_new(FfiConverterStringINSTANCE.Lower(path), FfiConverterObjectStoreINSTANCE.Lower(objectStore), _uniffiStatus)
	}))
}

// Returns a handle for the WAL file with the given ID.
func (_self *WalReader) Get(id uint64) *WalFile {
	_pointer := _self.ffiObject.incrementPointer("*WalReader")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterWalFileINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
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
	res, err := uniffiRustCallAsync[*Error](
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

func (c FfiConverterWalReader) Lift(handle C.uint64_t) *WalReader {
	result := &WalReader{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_walreader(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_walreader(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WalReader).Destroy)
	return result
}

func (c FfiConverterWalReader) Read(reader io.Reader) *WalReader {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterWalReader) Lower(value *WalReader) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*WalReader")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterWalReader) Write(writer io.Writer, value *WalReader) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalWalReader(handle uint64) *WalReader {
	return FfiConverterWalReaderINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalWalReader(value *WalReader) uint64 {
	return uint64(FfiConverterWalReaderINSTANCE.Lower(value))
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
	return FfiConverterWriteBatchINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_slatedb_uniffi_fn_constructor_writebatch_new(_uniffiStatus)
	}))
}

// Appends a delete operation to the batch.
func (_self *WriteBatch) Delete(key []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*WriteBatch")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
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

func (c FfiConverterWriteBatch) Lift(handle C.uint64_t) *WriteBatch {
	result := &WriteBatch{
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_slatedb_uniffi_fn_clone_writebatch(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_slatedb_uniffi_fn_free_writebatch(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*WriteBatch).Destroy)
	return result
}

func (c FfiConverterWriteBatch) Read(reader io.Reader) *WriteBatch {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterWriteBatch) Lower(value *WriteBatch) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*WriteBatch")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterWriteBatch) Write(writer io.Writer, value *WriteBatch) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalWriteBatch(handle uint64) *WriteBatch {
	return FfiConverterWriteBatchINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalWriteBatch(value *WriteBatch) uint64 {
	return uint64(FfiConverterWriteBatchINSTANCE.Lower(value))
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

// Histogram payload captured in a metric snapshot.
type HistogramMetricValue struct {
	// Total number of recorded observations.
	Count uint64
	// Sum of all observed values.
	Sum float64
	// Minimum observed value.
	Min float64
	// Maximum observed value.
	Max float64
	// Histogram bucket boundaries.
	Boundaries []float64
	// Number of observations in each bucket.
	BucketCounts []uint64
}

func (r *HistogramMetricValue) Destroy() {
	FfiDestroyerUint64{}.Destroy(r.Count)
	FfiDestroyerFloat64{}.Destroy(r.Sum)
	FfiDestroyerFloat64{}.Destroy(r.Min)
	FfiDestroyerFloat64{}.Destroy(r.Max)
	FfiDestroyerSequenceFloat64{}.Destroy(r.Boundaries)
	FfiDestroyerSequenceUint64{}.Destroy(r.BucketCounts)
}

type FfiConverterHistogramMetricValue struct{}

var FfiConverterHistogramMetricValueINSTANCE = FfiConverterHistogramMetricValue{}

func (c FfiConverterHistogramMetricValue) Lift(rb RustBufferI) HistogramMetricValue {
	return LiftFromRustBuffer[HistogramMetricValue](c, rb)
}

func (c FfiConverterHistogramMetricValue) Read(reader io.Reader) HistogramMetricValue {
	return HistogramMetricValue{
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterFloat64INSTANCE.Read(reader),
		FfiConverterFloat64INSTANCE.Read(reader),
		FfiConverterFloat64INSTANCE.Read(reader),
		FfiConverterSequenceFloat64INSTANCE.Read(reader),
		FfiConverterSequenceUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterHistogramMetricValue) Lower(value HistogramMetricValue) C.RustBuffer {
	return LowerIntoRustBuffer[HistogramMetricValue](c, value)
}

func (c FfiConverterHistogramMetricValue) LowerExternal(value HistogramMetricValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[HistogramMetricValue](c, value))
}

func (c FfiConverterHistogramMetricValue) Write(writer io.Writer, value HistogramMetricValue) {
	FfiConverterUint64INSTANCE.Write(writer, value.Count)
	FfiConverterFloat64INSTANCE.Write(writer, value.Sum)
	FfiConverterFloat64INSTANCE.Write(writer, value.Min)
	FfiConverterFloat64INSTANCE.Write(writer, value.Max)
	FfiConverterSequenceFloat64INSTANCE.Write(writer, value.Boundaries)
	FfiConverterSequenceUint64INSTANCE.Write(writer, value.BucketCounts)
}

type FfiDestroyerHistogramMetricValue struct{}

func (_ FfiDestroyerHistogramMetricValue) Destroy(value HistogramMetricValue) {
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

// One metric from a [`DefaultMetricsRecorder`] snapshot.
type Metric struct {
	// Dotted metric name.
	Name string
	// Canonical label set for the metric instance.
	Labels []MetricLabel
	// Human-readable description.
	Description string
	// Current metric value.
	Value MetricValue
}

func (r *Metric) Destroy() {
	FfiDestroyerString{}.Destroy(r.Name)
	FfiDestroyerSequenceMetricLabel{}.Destroy(r.Labels)
	FfiDestroyerString{}.Destroy(r.Description)
	FfiDestroyerMetricValue{}.Destroy(r.Value)
}

type FfiConverterMetric struct{}

var FfiConverterMetricINSTANCE = FfiConverterMetric{}

func (c FfiConverterMetric) Lift(rb RustBufferI) Metric {
	return LiftFromRustBuffer[Metric](c, rb)
}

func (c FfiConverterMetric) Read(reader io.Reader) Metric {
	return Metric{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterSequenceMetricLabelINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterMetricValueINSTANCE.Read(reader),
	}
}

func (c FfiConverterMetric) Lower(value Metric) C.RustBuffer {
	return LowerIntoRustBuffer[Metric](c, value)
}

func (c FfiConverterMetric) LowerExternal(value Metric) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Metric](c, value))
}

func (c FfiConverterMetric) Write(writer io.Writer, value Metric) {
	FfiConverterStringINSTANCE.Write(writer, value.Name)
	FfiConverterSequenceMetricLabelINSTANCE.Write(writer, value.Labels)
	FfiConverterStringINSTANCE.Write(writer, value.Description)
	FfiConverterMetricValueINSTANCE.Write(writer, value.Value)
}

type FfiDestroyerMetric struct{}

func (_ FfiDestroyerMetric) Destroy(value Metric) {
	value.Destroy()
}

// Key-value label attached to a metric.
type MetricLabel struct {
	// Label key.
	Key string
	// Label value.
	Value string
}

func (r *MetricLabel) Destroy() {
	FfiDestroyerString{}.Destroy(r.Key)
	FfiDestroyerString{}.Destroy(r.Value)
}

type FfiConverterMetricLabel struct{}

var FfiConverterMetricLabelINSTANCE = FfiConverterMetricLabel{}

func (c FfiConverterMetricLabel) Lift(rb RustBufferI) MetricLabel {
	return LiftFromRustBuffer[MetricLabel](c, rb)
}

func (c FfiConverterMetricLabel) Read(reader io.Reader) MetricLabel {
	return MetricLabel{
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterMetricLabel) Lower(value MetricLabel) C.RustBuffer {
	return LowerIntoRustBuffer[MetricLabel](c, value)
}

func (c FfiConverterMetricLabel) LowerExternal(value MetricLabel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[MetricLabel](c, value))
}

func (c FfiConverterMetricLabel) Write(writer io.Writer, value MetricLabel) {
	FfiConverterStringINSTANCE.Write(writer, value.Key)
	FfiConverterStringINSTANCE.Write(writer, value.Value)
}

type FfiDestroyerMetricLabel struct{}

func (_ FfiDestroyerMetricLabel) Destroy(value MetricLabel) {
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
	// Timeout in milliseconds for the read. Defaults to no timeout.
	TimeoutMs *uint64
}

func (r *ReadOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
	FfiDestroyerOptionalUint64{}.Destroy(r.TimeoutMs)
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
		FfiConverterOptionalUint64INSTANCE.Read(reader),
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
	FfiConverterOptionalUint64INSTANCE.Write(writer, value.TimeoutMs)
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
	// The iteration order for the scan. Defaults to ascending when not set.
	Order *IterationOrder
}

func (r *ScanOptions) Destroy() {
	FfiDestroyerDurabilityLevel{}.Destroy(r.DurabilityFilter)
	FfiDestroyerBool{}.Destroy(r.Dirty)
	FfiDestroyerUint64{}.Destroy(r.ReadAheadBytes)
	FfiDestroyerBool{}.Destroy(r.CacheBlocks)
	FfiDestroyerUint64{}.Destroy(r.MaxFetchTasks)
	FfiDestroyerOptionalIterationOrder{}.Destroy(r.Order)
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
		FfiConverterOptionalIterationOrderINSTANCE.Read(reader),
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
	FfiConverterOptionalIterationOrderINSTANCE.Write(writer, value.Order)
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
	// Timeout in milliseconds for the write. Defaults to no timeout.
	TimeoutMs *uint64
}

func (r *WriteOptions) Destroy() {
	FfiDestroyerBool{}.Destroy(r.AwaitDurable)
	FfiDestroyerOptionalUint64{}.Destroy(r.TimeoutMs)
}

type FfiConverterWriteOptions struct{}

var FfiConverterWriteOptionsINSTANCE = FfiConverterWriteOptions{}

func (c FfiConverterWriteOptions) Lift(rb RustBufferI) WriteOptions {
	return LiftFromRustBuffer[WriteOptions](c, rb)
}

func (c FfiConverterWriteOptions) Read(reader io.Reader) WriteOptions {
	return WriteOptions{
		FfiConverterBoolINSTANCE.Read(reader),
		FfiConverterOptionalUint64INSTANCE.Read(reader),
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
	FfiConverterOptionalUint64INSTANCE.Write(writer, value.TimeoutMs)
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
var ErrErrorTimeout = fmt.Errorf("ErrorTimeout")

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

// The operation exceeded the configured timeout.
type ErrorTimeout struct {
	Message string
}

// The operation exceeded the configured timeout.
func NewErrorTimeout(
	message string,
) *Error {
	return &Error{err: &ErrorTimeout{
		Message: message}}
}

func (e ErrorTimeout) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err ErrorTimeout) Error() string {
	return fmt.Sprint("Timeout",
		": ",

		"Message=",
		err.Message,
	)
}

func (self ErrorTimeout) Is(target error) bool {
	return target == ErrErrorTimeout
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
	case 7:
		return &Error{&ErrorTimeout{
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
	case *ErrorTimeout:
		writeInt32(writer, 7)
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
	case ErrorTimeout:
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

// The iteration order for a scan.
type IterationOrder uint

const (
	IterationOrderAscending  IterationOrder = 1
	IterationOrderDescending IterationOrder = 2
)

type FfiConverterIterationOrder struct{}

var FfiConverterIterationOrderINSTANCE = FfiConverterIterationOrder{}

func (c FfiConverterIterationOrder) Lift(rb RustBufferI) IterationOrder {
	return LiftFromRustBuffer[IterationOrder](c, rb)
}

func (c FfiConverterIterationOrder) Lower(value IterationOrder) C.RustBuffer {
	return LowerIntoRustBuffer[IterationOrder](c, value)
}

func (c FfiConverterIterationOrder) LowerExternal(value IterationOrder) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[IterationOrder](c, value))
}
func (FfiConverterIterationOrder) Read(reader io.Reader) IterationOrder {
	id := readInt32(reader)
	return IterationOrder(id)
}

func (FfiConverterIterationOrder) Write(writer io.Writer, value IterationOrder) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerIterationOrder struct{}

func (_ FfiDestroyerIterationOrder) Destroy(value IterationOrder) {
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

// Value stored in a metric snapshot.
type MetricValue interface {
	Destroy()
}

// Monotonic counter value.
type MetricValueCounter struct {
	Field0 uint64
}

func (e MetricValueCounter) Destroy() {
	FfiDestroyerUint64{}.Destroy(e.Field0)
}

// Gauge value.
type MetricValueGauge struct {
	Field0 int64
}

func (e MetricValueGauge) Destroy() {
	FfiDestroyerInt64{}.Destroy(e.Field0)
}

// Up/down counter value.
type MetricValueUpDownCounter struct {
	Field0 int64
}

func (e MetricValueUpDownCounter) Destroy() {
	FfiDestroyerInt64{}.Destroy(e.Field0)
}

// Histogram summary and buckets.
type MetricValueHistogram struct {
	Field0 HistogramMetricValue
}

func (e MetricValueHistogram) Destroy() {
	FfiDestroyerHistogramMetricValue{}.Destroy(e.Field0)
}

type FfiConverterMetricValue struct{}

var FfiConverterMetricValueINSTANCE = FfiConverterMetricValue{}

func (c FfiConverterMetricValue) Lift(rb RustBufferI) MetricValue {
	return LiftFromRustBuffer[MetricValue](c, rb)
}

func (c FfiConverterMetricValue) Lower(value MetricValue) C.RustBuffer {
	return LowerIntoRustBuffer[MetricValue](c, value)
}

func (c FfiConverterMetricValue) LowerExternal(value MetricValue) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[MetricValue](c, value))
}
func (FfiConverterMetricValue) Read(reader io.Reader) MetricValue {
	id := readInt32(reader)
	switch id {
	case 1:
		return MetricValueCounter{
			FfiConverterUint64INSTANCE.Read(reader),
		}
	case 2:
		return MetricValueGauge{
			FfiConverterInt64INSTANCE.Read(reader),
		}
	case 3:
		return MetricValueUpDownCounter{
			FfiConverterInt64INSTANCE.Read(reader),
		}
	case 4:
		return MetricValueHistogram{
			FfiConverterHistogramMetricValueINSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterMetricValue.Read()", id))
	}
}

func (FfiConverterMetricValue) Write(writer io.Writer, value MetricValue) {
	switch variant_value := value.(type) {
	case MetricValueCounter:
		writeInt32(writer, 1)
		FfiConverterUint64INSTANCE.Write(writer, variant_value.Field0)
	case MetricValueGauge:
		writeInt32(writer, 2)
		FfiConverterInt64INSTANCE.Write(writer, variant_value.Field0)
	case MetricValueUpDownCounter:
		writeInt32(writer, 3)
		FfiConverterInt64INSTANCE.Write(writer, variant_value.Field0)
	case MetricValueHistogram:
		writeInt32(writer, 4)
		FfiConverterHistogramMetricValueINSTANCE.Write(writer, variant_value.Field0)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterMetricValue.Write", value))
	}
}

type FfiDestroyerMetricValue struct{}

func (_ FfiDestroyerMetricValue) Destroy(value MetricValue) {
	value.Destroy()
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

// Expire the value at the given absolute timestamp (clock ticks).
type TtlExpireAt struct {
	Field0 int64
}

func (e TtlExpireAt) Destroy() {
	FfiDestroyerInt64{}.Destroy(e.Field0)
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
	case 4:
		return TtlExpireAt{
			FfiConverterInt64INSTANCE.Read(reader),
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
	case TtlExpireAt:
		writeInt32(writer, 4)
		FfiConverterInt64INSTANCE.Write(writer, variant_value.Field0)
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

type FfiConverterOptionalMetric struct{}

var FfiConverterOptionalMetricINSTANCE = FfiConverterOptionalMetric{}

func (c FfiConverterOptionalMetric) Lift(rb RustBufferI) *Metric {
	return LiftFromRustBuffer[*Metric](c, rb)
}

func (_ FfiConverterOptionalMetric) Read(reader io.Reader) *Metric {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterMetricINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalMetric) Lower(value *Metric) C.RustBuffer {
	return LowerIntoRustBuffer[*Metric](c, value)
}

func (c FfiConverterOptionalMetric) LowerExternal(value *Metric) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*Metric](c, value))
}

func (_ FfiConverterOptionalMetric) Write(writer io.Writer, value *Metric) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterMetricINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalMetric struct{}

func (_ FfiDestroyerOptionalMetric) Destroy(value *Metric) {
	if value != nil {
		FfiDestroyerMetric{}.Destroy(*value)
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

type FfiConverterOptionalIterationOrder struct{}

var FfiConverterOptionalIterationOrderINSTANCE = FfiConverterOptionalIterationOrder{}

func (c FfiConverterOptionalIterationOrder) Lift(rb RustBufferI) *IterationOrder {
	return LiftFromRustBuffer[*IterationOrder](c, rb)
}

func (_ FfiConverterOptionalIterationOrder) Read(reader io.Reader) *IterationOrder {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterIterationOrderINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalIterationOrder) Lower(value *IterationOrder) C.RustBuffer {
	return LowerIntoRustBuffer[*IterationOrder](c, value)
}

func (c FfiConverterOptionalIterationOrder) LowerExternal(value *IterationOrder) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*IterationOrder](c, value))
}

func (_ FfiConverterOptionalIterationOrder) Write(writer io.Writer, value *IterationOrder) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterIterationOrderINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalIterationOrder struct{}

func (_ FfiDestroyerOptionalIterationOrder) Destroy(value *IterationOrder) {
	if value != nil {
		FfiDestroyerIterationOrder{}.Destroy(*value)
	}
}

type FfiConverterSequenceUint64 struct{}

var FfiConverterSequenceUint64INSTANCE = FfiConverterSequenceUint64{}

func (c FfiConverterSequenceUint64) Lift(rb RustBufferI) []uint64 {
	return LiftFromRustBuffer[[]uint64](c, rb)
}

func (c FfiConverterSequenceUint64) Read(reader io.Reader) []uint64 {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]uint64, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterUint64INSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceUint64) Lower(value []uint64) C.RustBuffer {
	return LowerIntoRustBuffer[[]uint64](c, value)
}

func (c FfiConverterSequenceUint64) LowerExternal(value []uint64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]uint64](c, value))
}

func (c FfiConverterSequenceUint64) Write(writer io.Writer, value []uint64) {
	if len(value) > math.MaxInt32 {
		panic("[]uint64 is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterUint64INSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceUint64 struct{}

func (FfiDestroyerSequenceUint64) Destroy(sequence []uint64) {
	for _, value := range sequence {
		FfiDestroyerUint64{}.Destroy(value)
	}
}

type FfiConverterSequenceFloat64 struct{}

var FfiConverterSequenceFloat64INSTANCE = FfiConverterSequenceFloat64{}

func (c FfiConverterSequenceFloat64) Lift(rb RustBufferI) []float64 {
	return LiftFromRustBuffer[[]float64](c, rb)
}

func (c FfiConverterSequenceFloat64) Read(reader io.Reader) []float64 {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]float64, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterFloat64INSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceFloat64) Lower(value []float64) C.RustBuffer {
	return LowerIntoRustBuffer[[]float64](c, value)
}

func (c FfiConverterSequenceFloat64) LowerExternal(value []float64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]float64](c, value))
}

func (c FfiConverterSequenceFloat64) Write(writer io.Writer, value []float64) {
	if len(value) > math.MaxInt32 {
		panic("[]float64 is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterFloat64INSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceFloat64 struct{}

func (FfiDestroyerSequenceFloat64) Destroy(sequence []float64) {
	for _, value := range sequence {
		FfiDestroyerFloat64{}.Destroy(value)
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

type FfiConverterSequenceMetric struct{}

var FfiConverterSequenceMetricINSTANCE = FfiConverterSequenceMetric{}

func (c FfiConverterSequenceMetric) Lift(rb RustBufferI) []Metric {
	return LiftFromRustBuffer[[]Metric](c, rb)
}

func (c FfiConverterSequenceMetric) Read(reader io.Reader) []Metric {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Metric, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterMetricINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceMetric) Lower(value []Metric) C.RustBuffer {
	return LowerIntoRustBuffer[[]Metric](c, value)
}

func (c FfiConverterSequenceMetric) LowerExternal(value []Metric) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Metric](c, value))
}

func (c FfiConverterSequenceMetric) Write(writer io.Writer, value []Metric) {
	if len(value) > math.MaxInt32 {
		panic("[]Metric is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterMetricINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceMetric struct{}

func (FfiDestroyerSequenceMetric) Destroy(sequence []Metric) {
	for _, value := range sequence {
		FfiDestroyerMetric{}.Destroy(value)
	}
}

type FfiConverterSequenceMetricLabel struct{}

var FfiConverterSequenceMetricLabelINSTANCE = FfiConverterSequenceMetricLabel{}

func (c FfiConverterSequenceMetricLabel) Lift(rb RustBufferI) []MetricLabel {
	return LiftFromRustBuffer[[]MetricLabel](c, rb)
}

func (c FfiConverterSequenceMetricLabel) Read(reader io.Reader) []MetricLabel {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]MetricLabel, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterMetricLabelINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceMetricLabel) Lower(value []MetricLabel) C.RustBuffer {
	return LowerIntoRustBuffer[[]MetricLabel](c, value)
}

func (c FfiConverterSequenceMetricLabel) LowerExternal(value []MetricLabel) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]MetricLabel](c, value))
}

func (c FfiConverterSequenceMetricLabel) Write(writer io.Writer, value []MetricLabel) {
	if len(value) > math.MaxInt32 {
		panic("[]MetricLabel is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterMetricLabelINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceMetricLabel struct{}

func (FfiDestroyerSequenceMetricLabel) Destroy(sequence []MetricLabel) {
	for _, value := range sequence {
		FfiDestroyerMetricLabel{}.Destroy(value)
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
	errConverter BufReader[E],
	completeFunc rustFutureCompleteFunc[F],
	liftFunc func(F) T,
	rustFuture C.uint64_t,
	pollFunc rustFuturePollFunc,
	freeFunc rustFutureFreeFunc,
) (T, E) {
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
	ffiValue, err := rustCallWithError(errConverter, func(status *C.RustCallStatus) F {
		return completeFunc(rustFuture, status)
	})
	if value := reflect.ValueOf(err); value.IsValid() && !value.IsZero() {
		return goValue, err
	}
	return liftFunc(ffiValue), err
}

//export slatedb_uniffiFreeGorutine
func slatedb_uniffiFreeGorutine(data C.uint64_t) {
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
	_, _uniffiErr := rustCallWithError[*Error](FfiConverterError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_slatedb_uniffi_fn_func_init_logging(FfiConverterLogLevelINSTANCE.Lower(level), FfiConverterOptionalLogCallbackINSTANCE.Lower(callback), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
