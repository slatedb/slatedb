//! Read-only database reader APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions backed by `slatedb::DbReader`.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, create_runtime, cstr_to_string, db_reader_options_from_ptr,
    error_from_slate_error, error_result, range_from_c, read_options_from_ptr, require_handle,
    require_out_ptr, scan_options_from_ptr, slatedb_db_reader_options_t, slatedb_db_reader_t,
    slatedb_error_kind_t, slatedb_iterator_t, slatedb_object_store_t, slatedb_range_t,
    slatedb_read_options_t, slatedb_result_t, slatedb_scan_options_t, success_result,
};
use slatedb::DbReader;
use uuid::Uuid;

/// Opens a read-only database reader using a pre-resolved object store handle.
///
/// ## Arguments
/// - `path`: Database path as a null-terminated UTF-8 string.
/// - `object_store`: Opaque object store handle.
/// - `checkpoint_id`: Optional checkpoint UUID string. Null means latest state.
/// - `reader_options`: Optional reader options pointer. Null uses defaults.
/// - `out_reader`: Output pointer populated with a `slatedb_db_reader_t*` on success.
///
/// ## Returns
/// - `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers, UTF-8,
///   or malformed checkpoint UUID.
/// - Returns mapped SlateDB errors for open failures.
///
/// ## Safety
/// - `path` must be a valid null-terminated C string.
/// - `object_store` must be a valid object store handle.
/// - `checkpoint_id`, when non-null, must be a valid null-terminated C string.
/// - `reader_options`, when non-null, must point to a valid
///   `slatedb_db_reader_options_t`.
/// - `out_reader` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_open(
    path: *const std::os::raw::c_char,
    object_store: *const slatedb_object_store_t,
    checkpoint_id: *const std::os::raw::c_char,
    reader_options: *const slatedb_db_reader_options_t,
    out_reader: *mut *mut slatedb_db_reader_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_reader, "out_reader") {
        return err;
    }
    if let Err(err) = require_handle(object_store, "object_store") {
        return err;
    }
    *out_reader = std::ptr::null_mut();

    let path = match cstr_to_string(path, "path") {
        Ok(path) => path,
        Err(err) => return err,
    };

    let checkpoint_id = if checkpoint_id.is_null() {
        None
    } else {
        let checkpoint_id = match cstr_to_string(checkpoint_id, "checkpoint_id") {
            Ok(checkpoint_id) => checkpoint_id,
            Err(err) => return err,
        };
        if checkpoint_id.is_empty() {
            None
        } else {
            match Uuid::parse_str(&checkpoint_id) {
                Ok(checkpoint_id) => Some(checkpoint_id),
                Err(err) => {
                    return error_result(
                        slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                        &format!("invalid checkpoint_id UUID: {err}"),
                    );
                }
            }
        }
    };

    let reader_options = db_reader_options_from_ptr(reader_options);

    let runtime = match create_runtime() {
        Ok(runtime) => runtime,
        Err(err) => return err,
    };

    let object_store = (&*object_store).object_store.clone();
    match runtime.block_on(DbReader::open(
        path,
        object_store,
        checkpoint_id,
        reader_options,
    )) {
        Ok(reader) => {
            let handle = Box::new(slatedb_db_reader_t { runtime, reader });
            *out_reader = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Reads a single key using default read options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `out_present`: Set to `true` when a value is found.
/// - `out_val`: Output pointer to Rust-allocated value bytes.
/// - `out_val_len`: Output length for `out_val`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles.
/// - Returns mapped SlateDB errors for read failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as required.
/// - `out_val` must be freed with `slatedb_bytes_free` when `*out_present` is true.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_get(
    reader: *mut slatedb_db_reader_t,
    key: *const u8,
    key_len: usize,
    out_present: *mut bool,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    slatedb_db_reader_get_with_options(
        reader,
        key,
        key_len,
        std::ptr::null(),
        out_present,
        out_val,
        out_val_len,
    )
}

/// Reads a single key using explicit read options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `read_options`: Optional read options pointer (null uses defaults).
/// - `out_present`: Set to `true` when a value is found.
/// - `out_val`: Output pointer to Rust-allocated value bytes.
/// - `out_val_len`: Output length for `out_val`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles/options.
/// - Returns mapped SlateDB errors for read failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as required.
/// - `out_val` must be freed with `slatedb_bytes_free` when `*out_present` is true.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_get_with_options(
    reader: *mut slatedb_db_reader_t,
    key: *const u8,
    key_len: usize,
    read_options: *const slatedb_read_options_t,
    out_present: *mut bool,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "db_reader") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_val, "out_val") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_val_len, "out_val_len") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_present, "out_present") {
        return err;
    }
    *out_present = false;
    *out_val = std::ptr::null_mut();
    *out_val_len = 0;

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };

    let read_options = match read_options_from_ptr(read_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let handle = &mut *reader;
    match handle
        .runtime
        .block_on(handle.reader.get_with_options(key, &read_options))
    {
        Ok(Some(value)) => {
            let (val, val_len) = alloc_bytes(value.as_ref());
            *out_val = val;
            *out_val_len = val_len;
            *out_present = true;
            success_result()
        }
        Ok(None) => {
            *out_present = false;
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Scans a key range using default scan options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `range`: Range bounds to scan.
/// - `out_iterator`: Output pointer populated with `slatedb_iterator_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles/range.
/// - Returns mapped SlateDB errors for scan failures.
///
/// ## Safety
/// - `reader` and `out_iterator` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_scan(
    reader: *mut slatedb_db_reader_t,
    range: slatedb_range_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_reader_scan_with_options(reader, range, std::ptr::null(), out_iterator)
}

/// Scans a key range with explicit scan options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `range`: Range bounds to scan.
/// - `scan_options`: Optional scan options pointer.
/// - `out_iterator`: Output pointer populated with `slatedb_iterator_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles/options/range.
/// - Returns mapped SlateDB errors for scan failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as required.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_scan_with_options(
    reader: *mut slatedb_db_reader_t,
    range: slatedb_range_t,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "db_reader") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_iterator, "out_iterator") {
        return err;
    }
    *out_iterator = std::ptr::null_mut();

    let range = match range_from_c(range) {
        Ok(range) => range,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let reader_handle = &mut *reader;
    match reader_handle
        .runtime
        .block_on(reader_handle.reader.scan_with_options(range, &scan_options))
    {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: reader_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Scans keys matching a prefix using default scan options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `prefix`: Prefix bytes.
/// - `prefix_len`: Length of `prefix`.
/// - `out_iterator`: Output pointer populated with `slatedb_iterator_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles/sizes.
/// - Returns mapped SlateDB errors for scan failures.
///
/// ## Safety
/// - `prefix` must reference at least `prefix_len` readable bytes.
/// - `reader` and `out_iterator` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_scan_prefix(
    reader: *mut slatedb_db_reader_t,
    prefix: *const u8,
    prefix_len: usize,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_reader_scan_prefix_with_options(
        reader,
        prefix,
        prefix_len,
        std::ptr::null(),
        out_iterator,
    )
}

/// Scans keys matching a prefix with explicit scan options.
///
/// ## Arguments
/// - `reader`: Reader handle.
/// - `prefix`: Prefix bytes.
/// - `prefix_len`: Length of `prefix`.
/// - `scan_options`: Optional scan options pointer.
/// - `out_iterator`: Output pointer populated with `slatedb_iterator_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles/options/sizes.
/// - Returns mapped SlateDB errors for scan failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as required.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_scan_prefix_with_options(
    reader: *mut slatedb_db_reader_t,
    prefix: *const u8,
    prefix_len: usize,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "db_reader") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_iterator, "out_iterator") {
        return err;
    }
    *out_iterator = std::ptr::null_mut();

    let prefix = match bytes_from_ptr(prefix, prefix_len, "prefix") {
        Ok(prefix) => prefix,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let reader_handle = &mut *reader;
    match reader_handle.runtime.block_on(
        reader_handle
            .reader
            .scan_prefix_with_options(prefix, &scan_options),
    ) {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: reader_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Closes and frees a database reader handle.
///
/// ## Arguments
/// - `reader`: Reader handle to close.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles.
/// - Returns mapped SlateDB errors for close failures.
///
/// ## Safety
/// - `reader` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_reader_close(
    reader: *mut slatedb_db_reader_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(reader, "db_reader") {
        return err;
    }

    let handle = Box::from_raw(reader);
    match handle.runtime.block_on(handle.reader.close()) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{
        slatedb_bound_t, slatedb_error_kind_t, SLATEDB_BOUND_KIND_EXCLUDED,
        SLATEDB_BOUND_KIND_INCLUDED,
    };
    use std::ffi::{CStr, CString};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

    fn next_test_path() -> String {
        static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);
        format!(
            "slatedb_c_db_reader_test_{}",
            NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn assert_result_kind(result: slatedb_result_t, expected: slatedb_error_kind_t) {
        let kind = result.kind;
        let message = if result.message.is_null() {
            String::new()
        } else {
            unsafe {
                CStr::from_ptr(result.message)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        crate::memory::slatedb_result_free(result);
        assert_eq!(
            kind, expected,
            "unexpected result kind with message: {message}"
        );
    }

    fn assert_ok(result: slatedb_result_t) {
        assert_result_kind(result, slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE);
    }

    fn seed_db(path: &str, object_store: Arc<dyn slatedb::object_store::ObjectStore>) {
        let runtime: Arc<Runtime> = Arc::new(
            RuntimeBuilder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build runtime"),
        );
        runtime.block_on(async {
            let db = slatedb::Db::open(path.to_owned(), object_store)
                .await
                .expect("failed to open test db");
            db.put(b"pref:a", b"value-a").await.expect("put failed");
            db.put(b"pref:b", b"value-b").await.expect("put failed");
            db.put(b"other", b"value-c").await.expect("put failed");
            db.flush().await.expect("flush failed");
            db.close().await.expect("close failed");
        });
    }

    fn read_value(ptr: *mut u8, len: usize) -> Vec<u8> {
        let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }.to_vec();
        crate::memory::slatedb_bytes_free(ptr, len);
        bytes
    }

    #[test]
    fn test_db_reader_open_get_and_close() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();
        seed_db(&path, Arc::clone(&object_store));

        let object_store_handle = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(path).expect("CString failed");

        let mut reader: *mut slatedb_db_reader_t = std::ptr::null_mut();
        assert_ok(unsafe {
            slatedb_db_reader_open(
                path_c.as_ptr(),
                object_store_handle as *const slatedb_object_store_t,
                std::ptr::null(),
                std::ptr::null(),
                &mut reader,
            )
        });
        assert!(!reader.is_null());

        let mut present = false;
        let mut value: *mut u8 = std::ptr::null_mut();
        let mut value_len = 0usize;
        assert_ok(unsafe {
            slatedb_db_reader_get(
                reader,
                b"pref:a".as_ptr(),
                b"pref:a".len(),
                &mut present,
                &mut value,
                &mut value_len,
            )
        });
        assert!(present);
        assert_eq!(read_value(value, value_len), b"value-a");

        assert_ok(unsafe { slatedb_db_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(object_store_handle) });
    }

    #[test]
    fn test_db_reader_scan_prefix() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();
        seed_db(&path, Arc::clone(&object_store));

        let object_store_handle = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));
        let path_c = CString::new(path).expect("CString failed");

        let mut reader: *mut slatedb_db_reader_t = std::ptr::null_mut();
        assert_ok(unsafe {
            slatedb_db_reader_open(
                path_c.as_ptr(),
                object_store_handle as *const slatedb_object_store_t,
                std::ptr::null(),
                std::ptr::null(),
                &mut reader,
            )
        });

        let start = b"pref:";
        let end = b"pref;";
        let range = slatedb_range_t {
            start: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_INCLUDED,
                data: start.as_ptr(),
                len: start.len(),
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_EXCLUDED,
                data: end.as_ptr(),
                len: end.len(),
            },
        };

        let mut iterator: *mut slatedb_iterator_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_db_reader_scan(reader, range, &mut iterator) });
        assert!(!iterator.is_null());

        let mut keys = Vec::new();
        loop {
            let mut present = false;
            let mut key_ptr: *mut u8 = std::ptr::null_mut();
            let mut key_len = 0usize;
            let mut val_ptr: *mut u8 = std::ptr::null_mut();
            let mut val_len = 0usize;

            assert_ok(unsafe {
                crate::iterator::slatedb_iterator_next(
                    iterator,
                    &mut present,
                    &mut key_ptr,
                    &mut key_len,
                    &mut val_ptr,
                    &mut val_len,
                )
            });
            if !present {
                break;
            }

            let key = read_value(key_ptr, key_len);
            let _ = read_value(val_ptr, val_len);
            keys.push(key);
        }

        assert_eq!(keys, vec![b"pref:a".to_vec(), b"pref:b".to_vec()]);
        assert_ok(unsafe { crate::iterator::slatedb_iterator_close(iterator) });
        assert_ok(unsafe { slatedb_db_reader_close(reader) });
        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(object_store_handle) });
    }

    #[test]
    fn test_db_reader_open_rejects_invalid_checkpoint_id() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let object_store_handle = Box::into_raw(Box::new(slatedb_object_store_t { object_store }));

        let path = CString::new(next_test_path()).expect("CString failed");
        let bad_checkpoint = CString::new("not-a-uuid").expect("CString failed");
        let mut reader: *mut slatedb_db_reader_t = std::ptr::null_mut();

        assert_result_kind(
            unsafe {
                slatedb_db_reader_open(
                    path.as_ptr(),
                    object_store_handle as *const slatedb_object_store_t,
                    bad_checkpoint.as_ptr(),
                    std::ptr::null(),
                    &mut reader,
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert!(reader.is_null());

        assert_ok(unsafe { crate::object_store::slatedb_object_store_close(object_store_handle) });
    }
}
