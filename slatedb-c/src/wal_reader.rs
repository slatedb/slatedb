//! WAL reader APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions backed by `slatedb::WalReader`,
//! `slatedb::WalFile`, and `slatedb::WalFileIterator`.

use std::os::raw::c_char;
use std::sync::Arc;

use crate::ffi::{
    alloc_bytes, create_runtime, cstr_to_string, error_from_slate_error, require_handle,
    require_out_ptr, slatedb_object_store_t, slatedb_result_t, slatedb_row_entry_t,
    slatedb_wal_file_iterator_t, slatedb_wal_file_metadata_t, slatedb_wal_file_t,
    slatedb_wal_reader_t, success_result, u64_range_from_c, SLATEDB_ROW_ENTRY_KIND_MERGE,
    SLATEDB_ROW_ENTRY_KIND_TOMBSTONE, SLATEDB_ROW_ENTRY_KIND_VALUE,
};
use crate::slatedb_range_t;
use slatedb::{ValueDeletable, WalReader};

/// Creates a new WAL reader for the database at the given path.
///
/// ## Arguments
/// - `path`: Database path as a null-terminated UTF-8 string. If the database
///   was configured with a separate WAL object store, pass that store in
///   `object_store`.
/// - `object_store`: Opaque object store handle.
/// - `out_reader`: Output pointer populated with a `slatedb_wal_reader_t*` on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers or UTF-8 errors.
/// - `SLATEDB_ERROR_KIND_INTERNAL` if the Tokio runtime cannot be created.
///
/// ## Safety
/// - `path` must be a valid null-terminated C string.
/// - `object_store` must be a valid object store handle.
/// - `out_reader` must be a valid non-null writable pointer.
/// - The returned handle must be freed with `slatedb_wal_reader_close`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_reader_new(
    path: *const c_char,
    object_store: *const slatedb_object_store_t,
    out_reader: *mut *mut slatedb_wal_reader_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_reader, "out_reader") {
        return err;
    }
    if let Err(err) = require_handle(object_store, "object_store") {
        return err;
    }
    *out_reader = std::ptr::null_mut();

    let path = match cstr_to_string(path, "path") {
        Ok(p) => p,
        Err(err) => return err,
    };

    let runtime = match create_runtime() {
        Ok(rt) => rt,
        Err(err) => return err,
    };

    let object_store = (&*object_store).object_store.clone();
    let reader = WalReader::new(path, object_store);
    *out_reader = Box::into_raw(Box::new(slatedb_wal_reader_t { runtime, reader }));
    success_result()
}

/// Lists WAL files in ascending order by ID within the specified range.
///
/// ## Arguments
/// - `reader`: WAL reader handle.
/// - `range`: Range bounds to scan.
/// - `out_files`: On success, set to an array of `slatedb_wal_file_t*` (null when
///   no files exist). Free each element with `slatedb_wal_file_close`, then free
///   the outer array with `slatedb_wal_files_free`.
/// - `out_count`: Set to the number of elements in `out_files`.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers or malformed range bounds.
/// - Mapped SlateDB errors for object-storage failures.
///
/// ## Safety
/// - `reader`, `out_files`, and `out_count` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_reader_list(
    reader: *mut slatedb_wal_reader_t,
    range: slatedb_range_t,
    out_files: *mut *mut *mut slatedb_wal_file_t,
    out_count: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "wal_reader") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_files, "out_files") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_count, "out_count") {
        return err;
    }
    *out_files = std::ptr::null_mut();
    *out_count = 0;

    let range = match u64_range_from_c(range) {
        Ok(range) => range,
        Err(err) => return err,
    };

    let handle = &*reader;
    let wal_files = match handle.runtime.block_on(handle.reader.list(range)) {
        Ok(files) => files,
        Err(err) => return error_from_slate_error(&err),
    };

    let mut file_ptrs: Vec<*mut slatedb_wal_file_t> = wal_files
        .into_iter()
        .map(|file| {
            Box::into_raw(Box::new(slatedb_wal_file_t {
                runtime: Arc::clone(&handle.runtime),
                file,
            }))
        })
        .collect();

    let count = file_ptrs.len();
    if count > 0 {
        file_ptrs.shrink_to_fit();
        let ptr = file_ptrs.as_mut_ptr();
        std::mem::forget(file_ptrs);
        *out_files = ptr;
        *out_count = count;
    }
    success_result()
}

/// Frees the outer array returned by `slatedb_wal_reader_list`.
///
/// Does **not** free the individual `slatedb_wal_file_t*` elements — those must
/// be freed with `slatedb_wal_file_close` before calling this function.
///
/// ## Arguments
/// - `files`: Array pointer written by `slatedb_wal_reader_list`. No-op if null.
/// - `count`: Element count written by `slatedb_wal_reader_list`.
///
/// ## Safety
/// - `files`/`count` must exactly match what `slatedb_wal_reader_list` wrote.
/// - All elements must have already been freed with `slatedb_wal_file_close`.
/// - Do not call more than once per array.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_files_free(files: *mut *mut slatedb_wal_file_t, count: usize) {
    if !files.is_null() && count > 0 {
        let _ = Vec::from_raw_parts(files, count, count);
    }
}

/// Returns a `slatedb_wal_file_t` handle for a specific WAL ID without checking
/// whether that file exists in object storage.
///
/// ## Arguments
/// - `reader`: WAL reader handle.
/// - `id`: WAL file ID to retrieve.
/// - `out_file`: Output pointer populated with a `slatedb_wal_file_t*` on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
///
/// ## Safety
/// - `reader` and `out_file` must be valid non-null pointers.
/// - The returned handle must be freed with `slatedb_wal_file_close`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_reader_get(
    reader: *mut slatedb_wal_reader_t,
    id: u64,
    out_file: *mut *mut slatedb_wal_file_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "wal_reader") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_file, "out_file") {
        return err;
    }
    *out_file = std::ptr::null_mut();

    let handle = &*reader;
    let file = handle.reader.get(id);
    *out_file = Box::into_raw(Box::new(slatedb_wal_file_t {
        runtime: Arc::clone(&handle.runtime),
        file,
    }));
    success_result()
}

/// Closes and frees a WAL reader handle.
///
/// ## Arguments
/// - `reader`: WAL reader handle to close.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` if `reader` is null.
///
/// ## Safety
/// - `reader` must be a valid non-null handle. Do not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_reader_close(
    reader: *mut slatedb_wal_reader_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "wal_reader") {
        return err;
    }
    let _ = Box::from_raw(reader);
    success_result()
}

/// Returns the ID of this WAL file.
///
/// ## Arguments
/// - `file`: WAL file handle.
/// - `out_id`: Output pointer populated with the file ID on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
///
/// ## Safety
/// - `file` and `out_id` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_id(
    file: *const slatedb_wal_file_t,
    out_id: *mut u64,
) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_id, "out_id") {
        return err;
    }
    *out_id = (&*file).file.id;
    success_result()
}

/// Returns the WAL ID immediately following this file's ID (`id + 1`).
///
/// ## Arguments
/// - `file`: WAL file handle.
/// - `out_id`: Output pointer populated with the next file ID on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
///
/// ## Safety
/// - `file` and `out_id` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_next_id(
    file: *const slatedb_wal_file_t,
    out_id: *mut u64,
) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_id, "out_id") {
        return err;
    }
    *out_id = (&*file).file.next_id();
    success_result()
}

/// Returns a handle for the WAL file immediately following this one without
/// checking whether it exists in object storage.
///
/// ## Arguments
/// - `file`: WAL file handle.
/// - `out_next_file`: Output pointer populated with a `slatedb_wal_file_t*` on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
///
/// ## Safety
/// - `file` and `out_next_file` must be valid non-null pointers.
/// - The returned handle must be freed with `slatedb_wal_file_close`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_next_file(
    file: *const slatedb_wal_file_t,
    out_next_file: *mut *mut slatedb_wal_file_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_next_file, "out_next_file") {
        return err;
    }
    *out_next_file = std::ptr::null_mut();
    let handle = &*file;
    *out_next_file = Box::into_raw(Box::new(slatedb_wal_file_t {
        runtime: Arc::clone(&handle.runtime),
        file: handle.file.next_file(),
    }));
    success_result()
}

/// Returns metadata for this WAL file.
///
/// Populates `out_metadata.location` with a Rust-allocated UTF-8 byte buffer.
/// Call `slatedb_wal_file_metadata_free` to release it.
///
/// ## Arguments
/// - `file`: WAL file handle.
/// - `out_metadata`: Output struct populated with metadata on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
/// - `SLATEDB_ERROR_KIND_DATA` if the file is missing from object storage.
///
/// ## Safety
/// - `file` and `out_metadata` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_metadata(
    file: *const slatedb_wal_file_t,
    out_metadata: *mut slatedb_wal_file_metadata_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_metadata, "out_metadata") {
        return err;
    }
    let handle = &*file;
    let metadata = match handle.runtime.block_on(handle.file.metadata()) {
        Ok(m) => m,
        Err(err) => return error_from_slate_error(&err),
    };
    let location_str = metadata.location.to_string();
    let (location, location_len) = alloc_bytes(location_str.as_bytes());
    *out_metadata = slatedb_wal_file_metadata_t {
        last_modified_secs: metadata.last_modified_dt.timestamp(),
        last_modified_nanos: metadata.last_modified_dt.timestamp_subsec_nanos(),
        size_bytes: metadata.size_bytes,
        location,
        location_len,
    };
    success_result()
}

/// Frees the `location` buffer in a `slatedb_wal_file_metadata_t`.
///
/// ## Arguments
/// - `metadata`: Pointer to the struct whose `location` buffer should be freed.
///   No-op if null.
///
/// ## Safety
/// - `metadata.location`/`metadata.location_len` must match values from
///   `slatedb_wal_file_metadata`. Do not call more than once.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_metadata_free(
    metadata: *mut slatedb_wal_file_metadata_t,
) {
    if metadata.is_null() {
        return;
    }
    let m = &mut *metadata;
    crate::memory::slatedb_bytes_free(m.location, m.location_len);
    m.location = std::ptr::null_mut();
    m.location_len = 0;
}

/// Returns an iterator over entries in this WAL file, preserving tombstones and
/// merge operands exactly as written.
///
/// ## Arguments
/// - `file`: WAL file handle.
/// - `out_iter`: Output pointer populated with a `slatedb_wal_file_iterator_t*` on success.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
/// - `SLATEDB_ERROR_KIND_DATA` if the file is missing from object storage.
///
/// ## Safety
/// - `file` and `out_iter` must be valid non-null pointers.
/// - The returned handle must be freed with `slatedb_wal_file_iterator_close`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_iterator(
    file: *const slatedb_wal_file_t,
    out_iter: *mut *mut slatedb_wal_file_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_iter, "out_iter") {
        return err;
    }
    *out_iter = std::ptr::null_mut();
    let handle = &*file;
    let iter = match handle.runtime.block_on(handle.file.iterator()) {
        Ok(i) => i,
        Err(err) => return error_from_slate_error(&err),
    };
    *out_iter = Box::into_raw(Box::new(slatedb_wal_file_iterator_t {
        runtime: Arc::clone(&handle.runtime),
        iter,
    }));
    success_result()
}

/// Closes and frees a WAL file handle.
///
/// ## Arguments
/// - `file`: WAL file handle to close.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` if `file` is null.
///
/// ## Safety
/// - `file` must be a valid non-null handle. Do not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_close(file: *mut slatedb_wal_file_t) -> slatedb_result_t {
    if let Err(err) = require_handle(file, "wal_file") {
        return err;
    }
    let _ = Box::from_raw(file);
    success_result()
}

/// Retrieves the next entry from a WAL file iterator.
///
/// Sets `*out_present` to `true` and populates `*out_entry` when an entry is
/// available; sets `*out_present` to `false` at end of file.
///
/// Call `slatedb_row_entry_free` to release `out_entry.key` / `out_entry.value`
/// when `*out_present` is true.
///
/// ## Arguments
/// - `iter`: WAL file iterator handle.
/// - `out_present`: Set to `true` if an entry was read, `false` at end of file.
/// - `out_entry`: Output struct populated with the entry when `*out_present` is true.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
/// - Mapped SlateDB errors for decode or object-storage failures.
///
/// ## Safety
/// - `iter`, `out_present`, and `out_entry` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_iterator_next(
    iter: *mut slatedb_wal_file_iterator_t,
    out_present: *mut bool,
    out_entry: *mut slatedb_row_entry_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(iter, "wal_file_iterator") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_present, "out_present") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_entry, "out_entry") {
        return err;
    }
    *out_present = false;

    let handle = &mut *iter;
    match handle.runtime.block_on(handle.iter.next_entry()) {
        Ok(Some(entry)) => {
            let (key, key_len) = alloc_bytes(entry.key.as_ref());
            let (kind, value, value_len) = match &entry.value {
                ValueDeletable::Value(v) => {
                    let (val, val_len) = alloc_bytes(v.as_ref());
                    (SLATEDB_ROW_ENTRY_KIND_VALUE, val, val_len)
                }
                ValueDeletable::Merge(v) => {
                    let (val, val_len) = alloc_bytes(v.as_ref());
                    (SLATEDB_ROW_ENTRY_KIND_MERGE, val, val_len)
                }
                ValueDeletable::Tombstone => {
                    (SLATEDB_ROW_ENTRY_KIND_TOMBSTONE, std::ptr::null_mut(), 0)
                }
            };
            *out_present = true;
            *out_entry = slatedb_row_entry_t {
                kind,
                key,
                key_len,
                value,
                value_len,
                seq: entry.seq,
                create_ts_present: entry.create_ts.is_some(),
                create_ts: entry.create_ts.unwrap_or(0),
                expire_ts_present: entry.expire_ts.is_some(),
                expire_ts: entry.expire_ts.unwrap_or(0),
            };
            success_result()
        }
        Ok(None) => success_result(),
        Err(err) => error_from_slate_error(&err),
    }
}

/// Frees the `key` and `value` buffers in a `slatedb_row_entry_t`.
///
/// ## Arguments
/// - `entry`: Pointer to the entry whose `key`/`value` buffers should be freed.
///   No-op if null.
///
/// ## Safety
/// - Buffers must match those written by `slatedb_wal_file_iterator_next`.
/// - Do not call more than once per entry.
#[no_mangle]
pub unsafe extern "C" fn slatedb_row_entry_free(entry: *mut slatedb_row_entry_t) {
    if entry.is_null() {
        return;
    }
    let e = &mut *entry;
    crate::memory::slatedb_bytes_free(e.key, e.key_len);
    e.key = std::ptr::null_mut();
    e.key_len = 0;
    if !e.value.is_null() {
        crate::memory::slatedb_bytes_free(e.value, e.value_len);
        e.value = std::ptr::null_mut();
        e.value_len = 0;
    }
}

/// Closes and frees a WAL file iterator handle.
///
/// ## Arguments
/// - `iter`: WAL file iterator handle to close.
///
/// ## Returns
/// `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - `SLATEDB_ERROR_KIND_INVALID` if `iter` is null.
///
/// ## Safety
/// - `iter` must be a valid non-null handle. Do not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn slatedb_wal_file_iterator_close(
    iter: *mut slatedb_wal_file_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(iter, "wal_file_iterator") {
        return err;
    }
    let _ = Box::from_raw(iter);
    success_result()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{slatedb_error_kind_t, slatedb_object_store_t, SLATEDB_BOUND_KIND_UNBOUNDED};
    use crate::slatedb_bound_t;
    use std::ffi::CString;
    use std::ptr::null;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::runtime::Builder as RuntimeBuilder;

    struct AppendMergeOperator;
    impl slatedb::MergeOperator for AppendMergeOperator {
        fn merge(
            &self,
            _key: &slatedb::bytes::Bytes,
            existing: Option<slatedb::bytes::Bytes>,
            operand: slatedb::bytes::Bytes,
        ) -> Result<slatedb::bytes::Bytes, slatedb::MergeOperatorError> {
            let merged = match existing {
                Some(e) => [e.as_ref(), b",", operand.as_ref()].concat(),
                None => operand.to_vec(),
            };
            Ok(slatedb::bytes::Bytes::from(merged))
        }
    }

    fn next_test_path() -> String {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        format!(
            "slatedb_c_wal_test_{}",
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn assert_result_kind(result: slatedb_result_t, expected: slatedb_error_kind_t) {
        let kind = result.kind;
        let msg = if result.message.is_null() {
            String::new()
        } else {
            unsafe {
                std::ffi::CStr::from_ptr(result.message)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        crate::memory::slatedb_result_free(result);
        assert_eq!(kind, expected, "unexpected kind, message: {msg}");
    }

    fn assert_ok(result: slatedb_result_t) {
        assert_result_kind(result, slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE);
    }

    fn seed_db(path: &str, object_store: Arc<dyn slatedb::object_store::ObjectStore>) {
        let rt = RuntimeBuilder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let db = slatedb::Db::open(path.to_owned(), object_store)
                .await
                .unwrap();
            db.put(b"k1", b"v1").await.unwrap();
            db.put(b"k2", b"v2").await.unwrap();
            db.flush().await.unwrap();
            db.close().await.unwrap();
        });
    }

    fn take_bytes(ptr: *mut u8, len: usize) -> Vec<u8> {
        let v = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }.to_vec();
        crate::memory::slatedb_bytes_free(ptr, len);
        v
    }

    #[test]
    fn test_list_and_iterate() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();
        seed_db(&path, Arc::clone(&object_store));

        let os = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(path).unwrap();

        let mut reader: *mut slatedb_wal_reader_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_reader_new(path_c.as_ptr(), os, &mut reader) });

        let mut files: *mut *mut slatedb_wal_file_t = std::ptr::null_mut();
        let mut count = 0usize;
        let range = slatedb_range_t {
            start: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
        };
        assert_ok(unsafe { slatedb_wal_reader_list(reader, range, &mut files, &mut count) });
        assert!(count > 0 && !files.is_null());

        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let file = unsafe { *files.add(i) };
            let mut iter: *mut slatedb_wal_file_iterator_t = std::ptr::null_mut();
            assert_ok(unsafe { slatedb_wal_file_iterator(file, &mut iter) });

            loop {
                let mut present = false;
                let mut entry = std::mem::MaybeUninit::<slatedb_row_entry_t>::uninit();
                assert_ok(unsafe {
                    slatedb_wal_file_iterator_next(iter, &mut present, entry.as_mut_ptr())
                });
                if !present {
                    break;
                }
                let e = unsafe { entry.assume_init_mut() };
                let key = take_bytes(e.key, e.key_len);
                e.key = std::ptr::null_mut();
                e.key_len = 0;
                if !e.value.is_null() {
                    crate::memory::slatedb_bytes_free(e.value, e.value_len);
                    e.value = std::ptr::null_mut();
                    e.value_len = 0;
                }
                all_keys.push(key);
            }

            assert_ok(unsafe { slatedb_wal_file_iterator_close(iter) });
            assert_ok(unsafe { slatedb_wal_file_close(file) });
        }
        unsafe { slatedb_wal_files_free(files, count) };

        assert_eq!(all_keys.len(), 2);
        assert!(all_keys.iter().any(|k| k == b"k1"));
        assert!(all_keys.iter().any(|k| k == b"k2"));

        assert_ok(unsafe { slatedb_wal_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(os) });
    }

    #[test]
    fn test_get_and_next_file() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let os = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(next_test_path()).unwrap();

        let mut reader: *mut slatedb_wal_reader_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_reader_new(path_c.as_ptr(), os, &mut reader) });

        let mut file: *mut slatedb_wal_file_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_reader_get(reader, 42, &mut file) });
        assert!(!file.is_null());

        let mut id = 0u64;
        assert_ok(unsafe { slatedb_wal_file_id(file, &mut id) });
        assert_eq!(id, 42);

        let mut next_id = 0u64;
        assert_ok(unsafe { slatedb_wal_file_next_id(file, &mut next_id) });
        assert_eq!(next_id, 43);

        let mut next_file: *mut slatedb_wal_file_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_file_next_file(file, &mut next_file) });
        assert!(!next_file.is_null());

        let mut nf_id = 0u64;
        assert_ok(unsafe { slatedb_wal_file_id(next_file, &mut nf_id) });
        assert_eq!(nf_id, 43);

        assert_ok(unsafe { slatedb_wal_file_close(next_file) });
        assert_ok(unsafe { slatedb_wal_file_close(file) });
        assert_ok(unsafe { slatedb_wal_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(os) });
    }

    #[test]
    fn test_metadata() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();
        seed_db(&path, Arc::clone(&object_store));

        let os = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(path).unwrap();

        let mut reader: *mut slatedb_wal_reader_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_reader_new(path_c.as_ptr(), os, &mut reader) });

        let mut files: *mut *mut slatedb_wal_file_t = std::ptr::null_mut();
        let mut count = 0usize;
        let range = slatedb_range_t {
            start: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
        };
        assert_ok(unsafe { slatedb_wal_reader_list(reader, range, &mut files, &mut count) });
        assert!(count > 0);

        let file = unsafe { *files };
        let mut meta = std::mem::MaybeUninit::<slatedb_wal_file_metadata_t>::uninit();
        assert_ok(unsafe { slatedb_wal_file_metadata(file, meta.as_mut_ptr()) });
        let meta = unsafe { meta.assume_init_mut() };

        assert!(meta.size_bytes > 0);
        assert!(meta.last_modified_secs > 0);
        assert!(!meta.location.is_null() && meta.location_len > 0);

        unsafe { slatedb_wal_file_metadata_free(meta as *mut _) };
        assert!(meta.location.is_null());

        assert_ok(unsafe { slatedb_wal_file_close(file) });
        unsafe { slatedb_wal_files_free(files, count) };
        assert_ok(unsafe { slatedb_wal_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(os) });
    }

    #[test]
    fn test_entry_kinds() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();

        let rt = Arc::new(
            RuntimeBuilder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        rt.block_on(async {
            let db = slatedb::Db::builder(path.clone(), Arc::clone(&object_store))
                .with_merge_operator(Arc::new(AppendMergeOperator))
                .build()
                .await
                .unwrap();
            db.put(b"k_val", b"hello").await.unwrap();
            db.delete(b"k_tomb").await.unwrap();
            db.merge(b"k_merge", b"operand").await.unwrap();
            db.flush().await.unwrap();
            db.close().await.unwrap();
        });

        let os = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(path).unwrap();

        let mut reader: *mut slatedb_wal_reader_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_wal_reader_new(path_c.as_ptr(), os, &mut reader) });

        let mut files: *mut *mut slatedb_wal_file_t = std::ptr::null_mut();
        let mut count = 0usize;
        let range = slatedb_range_t {
            start: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: null(),
                len: 0,
            },
        };
        assert_ok(unsafe { slatedb_wal_reader_list(reader, range, &mut files, &mut count) });
        assert!(count > 0);

        let mut entries: Vec<(Vec<u8>, u8, Vec<u8>)> = Vec::new();
        for i in 0..count {
            let file = unsafe { *files.add(i) };
            let mut iter: *mut slatedb_wal_file_iterator_t = std::ptr::null_mut();
            assert_ok(unsafe { slatedb_wal_file_iterator(file, &mut iter) });

            loop {
                let mut present = false;
                let mut entry = std::mem::MaybeUninit::<slatedb_row_entry_t>::uninit();
                assert_ok(unsafe {
                    slatedb_wal_file_iterator_next(iter, &mut present, entry.as_mut_ptr())
                });
                if !present {
                    break;
                }
                let e = unsafe { entry.assume_init_mut() };
                let key = take_bytes(e.key, e.key_len);
                e.key = std::ptr::null_mut();
                e.key_len = 0;
                let val = if e.value.is_null() {
                    Vec::new()
                } else {
                    take_bytes(e.value, e.value_len)
                };
                e.value = std::ptr::null_mut();
                e.value_len = 0;
                entries.push((key, e.kind, val));
            }

            assert_ok(unsafe { slatedb_wal_file_iterator_close(iter) });
            assert_ok(unsafe { slatedb_wal_file_close(file) });
        }
        unsafe { slatedb_wal_files_free(files, count) };

        let find = |key: &[u8]| entries.iter().find(|(k, _, _)| k == key).cloned();

        let (_, kind, val) = find(b"k_val").expect("k_val missing");
        assert_eq!(kind, SLATEDB_ROW_ENTRY_KIND_VALUE);
        assert_eq!(val, b"hello");

        let (_, kind, val) = find(b"k_tomb").expect("k_tomb missing");
        assert_eq!(kind, SLATEDB_ROW_ENTRY_KIND_TOMBSTONE);
        assert!(val.is_empty());

        let (_, kind, val) = find(b"k_merge").expect("k_merge missing");
        assert_eq!(kind, SLATEDB_ROW_ENTRY_KIND_MERGE);
        assert_eq!(val, b"operand");

        assert_ok(unsafe { slatedb_wal_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(os) });
    }

    #[test]
    fn test_null_path_rejected() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let os = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));

        let mut reader: *mut slatedb_wal_reader_t = std::ptr::null_mut();
        assert_result_kind(
            unsafe { slatedb_wal_reader_new(std::ptr::null(), os, &mut reader) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert!(reader.is_null());

        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(os) });
    }
}
