package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>

static slatedb_wal_file_t* slatedb_wal_file_at_index(slatedb_wal_file_t **files, size_t i) {
	return files[i];
}
*/
import "C"
import (
	"errors"
	"io"
	"time"
	"unsafe"
)

// RowEntryKind describes the type of a row entry.
type RowEntryKind uint8

const (
	RowEntryKindValue     RowEntryKind = 0
	RowEntryKindTombstone RowEntryKind = 1
	RowEntryKindMerge     RowEntryKind = 2
)

// RowEntry represents a single row entry in a WAL file.
type RowEntry struct {
	Kind     RowEntryKind
	Key      []byte
	Value    []byte // nil for tombstones
	Seq      uint64
	CreateTs *int64
	ExpireTs *int64
}

// WalFileMetadata holds metadata about a WAL file.
type WalFileMetadata struct {
	LastModified time.Time
	SizeBytes    uint64
	Location     string
}

// WalFileIterator iterates over entries in a single WAL file.
type WalFileIterator struct {
	ptr    *C.slatedb_wal_file_iterator_t
	closed bool
}

// Next returns the next row entry from the iterator.
//
// Returns io.EOF when all entries have been read.
//
// Typical usage:
//
//	for {
//	    entry, err := iter.Next()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    process(entry)
//	}
func (it *WalFileIterator) Next() (*RowEntry, error) {
	if it.closed {
		return nil, errors.New("wal file iterator is closed")
	}
	if it.ptr == nil {
		return nil, errors.New("invalid wal file iterator")
	}

	var present C.bool
	var cEntry C.slatedb_row_entry_t

	result := C.slatedb_wal_file_iterator_next(it.ptr, &present, &cEntry)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}

	if present == C.bool(false) {
		return nil, io.EOF
	}
	defer C.slatedb_row_entry_free(&cEntry)

	entry := &RowEntry{
		Kind: RowEntryKind(cEntry.kind),
		Key:  C.GoBytes(unsafe.Pointer(cEntry.key), C.int(cEntry.key_len)),
		Seq:  uint64(cEntry.seq),
	}

	if cEntry.value != nil {
		entry.Value = C.GoBytes(unsafe.Pointer(cEntry.value), C.int(cEntry.value_len))
	}

	if cEntry.create_ts_present != C.bool(false) {
		ts := int64(cEntry.create_ts)
		entry.CreateTs = &ts
	}

	if cEntry.expire_ts_present != C.bool(false) {
		ts := int64(cEntry.expire_ts)
		entry.ExpireTs = &ts
	}

	return entry, nil
}

// Close releases iterator resources.
//
// Close the iterator before closing the owning WalFile.
func (it *WalFileIterator) Close() error {
	if it.closed {
		return nil
	}
	if it.ptr == nil {
		return errors.New("invalid wal file iterator")
	}
	result := C.slatedb_wal_file_iterator_close(it.ptr)
	it.closed = true
	it.ptr = nil
	return resultToErrorAndFree(result)
}

// WalFile represents a single WAL file.
type WalFile struct {
	ptr    *C.slatedb_wal_file_t
	closed bool
}

// ID returns the ID of this WAL file.
func (f *WalFile) ID() (uint64, error) {
	if f.closed || f.ptr == nil {
		return 0, errors.New("wal file is closed")
	}
	var id C.uint64_t
	result := C.slatedb_wal_file_id(f.ptr, &id)
	if err := resultToErrorAndFree(result); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

// NextID returns the WAL ID immediately following this file's ID (id + 1).
func (f *WalFile) NextID() (uint64, error) {
	if f.closed || f.ptr == nil {
		return 0, errors.New("wal file is closed")
	}
	var id C.uint64_t
	result := C.slatedb_wal_file_next_id(f.ptr, &id)
	if err := resultToErrorAndFree(result); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

// NextFile returns a handle for the WAL file immediately following this one.
//
// The handle is returned without verifying that the file exists in object storage.
// The caller must close the returned WalFile when done.
func (f *WalFile) NextFile() (*WalFile, error) {
	if f.closed || f.ptr == nil {
		return nil, errors.New("wal file is closed")
	}
	var nextPtr *C.slatedb_wal_file_t
	result := C.slatedb_wal_file_next_file(f.ptr, &nextPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	return &WalFile{ptr: nextPtr}, nil
}

// Metadata fetches metadata for this WAL file from object storage.
func (f *WalFile) Metadata() (*WalFileMetadata, error) {
	if f.closed || f.ptr == nil {
		return nil, errors.New("wal file is closed")
	}
	var cMeta C.slatedb_wal_file_metadata_t
	result := C.slatedb_wal_file_metadata(f.ptr, &cMeta)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer C.slatedb_wal_file_metadata_free(&cMeta)

	location := string(C.GoBytes(unsafe.Pointer(cMeta.location), C.int(cMeta.location_len)))
	return &WalFileMetadata{
		LastModified: time.Unix(int64(cMeta.last_modified_secs), int64(cMeta.last_modified_nanos)).UTC(),
		SizeBytes:    uint64(cMeta.size_bytes),
		Location:     location,
	}, nil
}

// Iterator returns an iterator over entries in this WAL file.
//
// The caller must close the returned WalFileIterator when done.
func (f *WalFile) Iterator() (*WalFileIterator, error) {
	if f.closed || f.ptr == nil {
		return nil, errors.New("wal file is closed")
	}
	var iterPtr *C.slatedb_wal_file_iterator_t
	result := C.slatedb_wal_file_iterator(f.ptr, &iterPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	return &WalFileIterator{ptr: iterPtr}, nil
}

// Close releases WAL file resources.
//
// Close all iterators obtained from this file before closing the file.
func (f *WalFile) Close() error {
	if f.closed {
		return nil
	}
	if f.ptr == nil {
		return errors.New("invalid wal file")
	}
	result := C.slatedb_wal_file_close(f.ptr)
	f.closed = true
	f.ptr = nil
	return resultToErrorAndFree(result)
}

// WalReader provides read access to WAL files in a SlateDB database.
type WalReader struct {
	ptr *C.slatedb_wal_reader_t
}

// NewWalReader opens a WAL reader for the database at path.
//
// Parameters:
//   - path: local database path
//   - WithUrl[WalReaderConfig]: object-store URL
//   - WithEnvFile[WalReaderConfig]: optional .env file for URL/provider resolution
//
// Example:
//
//	reader, err := slatedb.NewWalReader(
//	    "/tmp/mydb",
//	    slatedb.WithEnvFile[slatedb.WalReaderConfig](".env"),
//	)
func NewWalReader(path string, opts ...Option[WalReaderConfig]) (*WalReader, error) {
	cfg := &WalReaderConfig{}
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

	var readerPtr *C.slatedb_wal_reader_t
	result := C.slatedb_wal_reader_new(cPath, objectStore, &readerPtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	return &WalReader{ptr: readerPtr}, nil
}

// List returns WAL files in ascending ID order within the given range.
//
// A nil start means unbounded (start from the first available file).
// A non-nil start means the range begins at that ID, inclusive.
// A nil end means unbounded (include all files to the end).
// A non-nil end means the range ends before that ID, exclusive.
//
// The caller must close each returned WalFile when done.
func (r *WalReader) List(start, end *uint64) ([]*WalFile, error) {
	if r.ptr == nil {
		return nil, errors.New("wal reader is closed")
	}

	var startVal, endVal C.uint64_t
	rangeValue := C.slatedb_range_t{
		start: C.slatedb_bound_t{kind: C.uint8_t(C.SLATEDB_BOUND_KIND_UNBOUNDED)},
		end:   C.slatedb_bound_t{kind: C.uint8_t(C.SLATEDB_BOUND_KIND_UNBOUNDED)},
	}
	if start != nil {
		startVal = C.uint64_t(*start)
		rangeValue.start.kind = C.uint8_t(C.SLATEDB_BOUND_KIND_INCLUDED)
		rangeValue.start.data = unsafe.Pointer(&startVal)
	}
	if end != nil {
		endVal = C.uint64_t(*end)
		rangeValue.end.kind = C.uint8_t(C.SLATEDB_BOUND_KIND_EXCLUDED)
		rangeValue.end.data = unsafe.Pointer(&endVal)
	}

	var filesPtr **C.slatedb_wal_file_t
	var count C.uintptr_t
	result := C.slatedb_wal_reader_list(r.ptr, rangeValue, &filesPtr, &count)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}

	if filesPtr == nil || count == 0 {
		return nil, nil
	}
	defer C.slatedb_wal_files_free(filesPtr, count)

	files := make([]*WalFile, int(count))
	for i := range files {
		files[i] = &WalFile{ptr: C.slatedb_wal_file_at_index(filesPtr, C.size_t(i))}
	}
	return files, nil
}

// Get returns a WalFile handle for the given WAL ID.
//
// The file handle is returned without verifying that the file exists in object
// storage. The caller must close the returned WalFile when done.
func (r *WalReader) Get(id uint64) (*WalFile, error) {
	if r.ptr == nil {
		return nil, errors.New("wal reader is closed")
	}
	var filePtr *C.slatedb_wal_file_t
	result := C.slatedb_wal_reader_get(r.ptr, C.uint64_t(id), &filePtr)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	return &WalFile{ptr: filePtr}, nil
}

// Close releases WAL reader resources.
func (r *WalReader) Close() error {
	if r.ptr == nil {
		return nil
	}
	result := C.slatedb_wal_reader_close(r.ptr)
	r.ptr = nil
	return resultToErrorAndFree(result)
}
