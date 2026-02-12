//! Database APIs for `slatedb-c`.
//!
//! This module exposes the primary C ABI surface for opening databases and
//! executing read/write operations.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, create_runtime, cstr_to_string, error_from_slate_error,
    flush_options_from_ptr, merge_options_from_ptr, put_options_from_ptr, range_from_c,
    read_options_from_ptr, require_handle, require_out_ptr, scan_options_from_ptr, slatedb_db_t,
    slatedb_flush_options_t, slatedb_iterator_t, slatedb_merge_options_t, slatedb_object_store_t,
    slatedb_put_options_t, slatedb_range_t, slatedb_read_options_t, slatedb_result_t,
    slatedb_scan_options_t, slatedb_write_batch_t, slatedb_write_options_t, success_result,
    take_write_batch, validate_write_key, validate_write_key_value, write_options_from_ptr,
};
use slatedb::Db;

/// Opens a database using a pre-resolved object store handle.
///
/// ## Arguments
/// - `path`: Database path as a null-terminated UTF-8 string.
/// - `object_store`: Opaque object store handle.
/// - `out_db`: Output pointer populated with a `slatedb_db_t*` on success.
///
/// ## Returns
/// - `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
/// - Returns mapped SlateDB errors for open failures.
///
/// ## Safety
/// - `path` must be a valid null-terminated C string.
/// - `object_store` must be a valid object store handle.
/// - `out_db` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_open(
    path: *const std::os::raw::c_char,
    object_store: *const slatedb_object_store_t,
    out_db: *mut *mut slatedb_db_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_db, "out_db") {
        return err;
    }
    if let Err(err) = require_handle(object_store, "object_store") {
        return err;
    }

    let path = match cstr_to_string(path, "path") {
        Ok(path) => path,
        Err(err) => return err,
    };

    let runtime = match create_runtime() {
        Ok(runtime) => runtime,
        Err(err) => return err,
    };

    let object_store = (&*object_store).object_store.clone();
    match runtime.block_on(Db::open(path, object_store)) {
        Ok(db) => {
            let handle = Box::new(slatedb_db_t { runtime, db });
            *out_db = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("db open failed: {err}")),
    }
}

/// Returns current database status without performing I/O.
///
/// ## Arguments
/// - `db`: Database handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating open/closed/error state.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null handle.
/// - Returns mapped SlateDB status errors (including close reason).
///
/// ## Safety
/// - `db` must be a valid database handle.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_status(db: *const slatedb_db_t) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let handle = &*db;
    match handle.db.status() {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db status failed: {err}")),
    }
}

/// Reads a single key using default read options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `out_found`: Set to `true` when a value is found.
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
/// - All pointer arguments must be valid for reads/writes as appropriate.
/// - `out_val` must be freed with `slatedb_bytes_free` when `*out_found` is true.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_get(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    out_found: *mut bool,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    slatedb_db_get_with_options(
        db,
        key,
        key_len,
        std::ptr::null(),
        out_found,
        out_val,
        out_val_len,
    )
}

/// Reads a single key using explicit read options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `read_options`: Optional read options pointer (null uses defaults).
/// - `out_found`: Set to `true` when a value is found.
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
/// - `out_val` must be freed with `slatedb_bytes_free` when `*out_found` is true.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_get_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    read_options: *const slatedb_read_options_t,
    out_found: *mut bool,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_val, "out_val") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_val_len, "out_val_len") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_found, "out_found") {
        return err;
    }
    *out_found = false;
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

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.get_with_options(key, &read_options))
    {
        Ok(Some(value)) => {
            let (val, val_len) = alloc_bytes(value.as_ref());
            *out_val = val;
            *out_val_len = val_len;
            *out_found = true;
            success_result()
        }
        Ok(None) => {
            *out_found = false;
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("db get failed: {err}")),
    }
}

/// Writes a key/value pair using default put/write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Value bytes.
/// - `value_len`: Length of `value`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/sizes.
/// - Returns mapped SlateDB errors for write failures.
///
/// ## Safety
/// - `key`/`value` must reference at least `key_len`/`value_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_put(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.put(key, value)) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db put failed: {err}")),
    }
}

/// Writes a key/value pair with explicit put and write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Value bytes.
/// - `value_len`: Length of `value`.
/// - `put_options`: Optional put options pointer.
/// - `write_options`: Optional write options pointer.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/options/sizes.
/// - Returns mapped SlateDB errors for write failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as required.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_put_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    put_options: *const slatedb_put_options_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let put_options = match put_options_from_ptr(put_options) {
        Ok(options) => options,
        Err(err) => return err,
    };
    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.put_with_options(
        key,
        value,
        &put_options,
        &write_options,
    )) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db put_with_options failed: {err}")),
    }
}

/// Deletes a key using default write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/sizes.
/// - Returns mapped SlateDB errors for delete failures.
///
/// ## Safety
/// - `key` must reference at least `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_delete(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.delete(key)) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db delete failed: {err}")),
    }
}

/// Deletes a key with explicit write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `write_options`: Optional write options pointer.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/options.
/// - Returns mapped SlateDB errors for delete failures.
///
/// ## Safety
/// - `key` must reference at least `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_delete_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.delete_with_options(key, &write_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db delete_with_options failed: {err}")),
    }
}

/// Merges a value into a key using default merge/write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Merge operand bytes.
/// - `value_len`: Length of `value`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/sizes.
/// - Returns mapped SlateDB errors for merge failures.
///
/// ## Safety
/// - `key`/`value` must reference readable memory for their lengths.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_merge(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.merge(key, value)) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db merge failed: {err}")),
    }
}

/// Merges a value into a key with explicit merge and write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Merge operand bytes.
/// - `value_len`: Length of `value`.
/// - `merge_options`: Optional merge options pointer.
/// - `write_options`: Optional write options pointer.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/options/sizes.
/// - Returns mapped SlateDB errors for merge failures.
///
/// ## Safety
/// - Pointer arguments must be valid for reads as required.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_merge_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    merge_options: *const slatedb_merge_options_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let merge_options = match merge_options_from_ptr(merge_options) {
        Ok(options) => options,
        Err(err) => return err,
    };
    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.merge_with_options(
        key,
        value,
        &merge_options,
        &write_options,
    )) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db merge_with_options failed: {err}")),
    }
}

/// Applies a write batch with default write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `write_batch`: Mutable write batch handle, consumed by this call regardless
///   of write outcome.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles or already-consumed
///   batches.
/// - Returns mapped SlateDB errors for write failures.
///
/// ## Safety
/// - `db` and `write_batch` must be valid non-null handles.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_write(
    db: *mut slatedb_db_t,
    write_batch: *mut slatedb_write_batch_t,
) -> slatedb_result_t {
    slatedb_db_write_with_options(db, write_batch, std::ptr::null())
}

/// Applies a write batch with explicit write options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `write_batch`: Mutable write batch handle, consumed by this call regardless
///   of write outcome.
/// - `write_options`: Optional write options pointer (null uses defaults).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles or already-consumed
///   batches.
/// - Returns mapped SlateDB errors for write failures.
///
/// ## Safety
/// - `db` and `write_batch` must be valid non-null handles.
/// - `write_options`, when non-null, must point to a valid
///   `slatedb_write_options_t`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_write_with_options(
    db: *mut slatedb_db_t,
    write_batch: *mut slatedb_write_batch_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let write_options = write_options_from_ptr(write_options);
    let batch = match take_write_batch(write_batch) {
        Ok(batch) => batch,
        Err(err) => return err,
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.write_with_options(batch, &write_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db write_with_options failed: {err}")),
    }
}

/// Scans a key range using default scan options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `range`: Range bounds to scan.
/// - `out_iterator`: Output pointer populated with `slatedb_iterator_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers/range.
/// - Returns mapped SlateDB errors for scan failures.
///
/// ## Safety
/// - `db` and `out_iterator` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan(
    db: *mut slatedb_db_t,
    range: slatedb_range_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_scan_with_options(db, range, std::ptr::null(), out_iterator)
}

/// Scans a key range with explicit scan options.
///
/// ## Arguments
/// - `db`: Database handle.
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
pub unsafe extern "C" fn slatedb_db_scan_with_options(
    db: *mut slatedb_db_t,
    range: slatedb_range_t,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_iterator, "out_iterator") {
        return err;
    }

    let range = match range_from_c(range) {
        Ok(range) => range,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.scan_with_options(range, &scan_options))
    {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: db_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("db scan failed: {err}")),
    }
}

/// Scans keys matching a prefix using default scan options.
///
/// ## Arguments
/// - `db`: Database handle.
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
/// - `db` and `out_iterator` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan_prefix(
    db: *mut slatedb_db_t,
    prefix: *const u8,
    prefix_len: usize,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_scan_prefix_with_options(db, prefix, prefix_len, std::ptr::null(), out_iterator)
}

/// Scans keys matching a prefix with explicit scan options.
///
/// ## Arguments
/// - `db`: Database handle.
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
pub unsafe extern "C" fn slatedb_db_scan_prefix_with_options(
    db: *mut slatedb_db_t,
    prefix: *const u8,
    prefix_len: usize,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_iterator, "out_iterator") {
        return err;
    }

    let prefix = match bytes_from_ptr(prefix, prefix_len, "prefix") {
        Ok(prefix) => prefix,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.scan_prefix_with_options(prefix, &scan_options))
    {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: db_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("db scan_prefix failed: {err}")),
    }
}

/// Flushes the database using default flush behavior.
///
/// ## Arguments
/// - `db`: Database handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles.
/// - Returns mapped SlateDB errors for flush failures.
///
/// ## Safety
/// - `db` must be a valid non-null handle.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_flush(db: *mut slatedb_db_t) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.flush()) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db flush failed: {err}")),
    }
}

/// Flushes the database with explicit flush options.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `flush_options`: Optional flush options pointer.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/options.
/// - Returns mapped SlateDB errors for flush failures.
///
/// ## Safety
/// - `db` must be a valid non-null handle.
/// - `flush_options`, when non-null, must point to a valid `slatedb_flush_options_t`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_flush_with_options(
    db: *mut slatedb_db_t,
    flush_options: *const slatedb_flush_options_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let flush_options = match flush_options_from_ptr(flush_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.flush_with_options(flush_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db flush_with_options failed: {err}")),
    }
}

/// Closes and frees a database handle.
///
/// ## Arguments
/// - `db`: Database handle to close.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles.
/// - Returns mapped SlateDB errors for close failures (including close reason).
///
/// ## Safety
/// - `db` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_close(db: *mut slatedb_db_t) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }

    let handle = Box::from_raw(db);
    match handle.runtime.block_on(handle.db.close()) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("db close failed: {err}")),
    }
}
