//! Database APIs for `slatedb-c`.
//!
//! This module exposes the primary C ABI surface for opening databases and
//! executing read/write operations.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, create_runtime, cstr_to_string, error_from_slate_error,
    error_result, flush_options_from_ptr, merge_options_from_ptr, put_options_from_ptr,
    range_from_c, read_options_from_ptr, require_handle, require_out_ptr, scan_options_from_ptr,
    slatedb_db_t, slatedb_error_kind_t, slatedb_flush_options_t, slatedb_iterator_t,
    slatedb_merge_options_t, slatedb_object_store_t, slatedb_put_options_t, slatedb_range_t,
    slatedb_read_options_t, slatedb_result_t, slatedb_scan_options_t, slatedb_write_batch_t,
    slatedb_write_options_t, success_result, take_write_batch, validate_write_key,
    validate_write_key_value, write_options_from_ptr,
};
use serde_json::{Map, Value};
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

/// Returns a JSON snapshot of current metrics for the database.
///
/// The payload is a UTF-8 JSON object mapping metric name to metric value:
/// `{ "db/get_requests": 42, ... }`.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `out_json`: Output pointer to Rust-allocated UTF-8 bytes.
/// - `out_json_len`: Output length for `out_json`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles.
/// - Returns `SLATEDB_ERROR_KIND_INTERNAL` if JSON serialization fails.
///
/// ## Safety
/// - `db`, `out_json`, and `out_json_len` must be valid non-null pointers.
/// - `out_json` must be freed with `slatedb_bytes_free`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_metrics(
    db: *const slatedb_db_t,
    out_json: *mut *mut u8,
    out_json_len: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_json, "out_json") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_json_len, "out_json_len") {
        return err;
    }

    *out_json = std::ptr::null_mut();
    *out_json_len = 0;

    let handle = &*db;
    let registry = handle.db.metrics();
    let mut snapshot = Map::new();

    for name in registry.names() {
        if let Some(stat) = registry.lookup(name) {
            snapshot.insert(name.to_owned(), Value::from(stat.get()));
        }
    }

    match serde_json::to_vec(&Value::Object(snapshot)) {
        Ok(encoded) => {
            let (json_data, json_len) = alloc_bytes(&encoded);
            *out_json = json_data;
            *out_json_len = json_len;
            success_result()
        }
        Err(err) => error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
            &format!("db metrics serialization failed: {err}"),
        ),
    }
}

/// Reads a single metric value by name.
///
/// ## Arguments
/// - `db`: Database handle.
/// - `name`: Null-terminated UTF-8 metric name (for example `db/get_requests`).
/// - `out_present`: Set to `true` when the metric exists.
/// - `out_value`: Metric value when `out_present` is true.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles or
///   invalid UTF-8 metric names.
///
/// ## Safety
/// - `db`, `name`, `out_present`, and `out_value` must be valid non-null
///   pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_metric_get(
    db: *const slatedb_db_t,
    name: *const std::os::raw::c_char,
    out_present: *mut bool,
    out_value: *mut i64,
) -> slatedb_result_t {
    if let Err(err) = require_handle(db, "db") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_present, "out_present") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_value, "out_value") {
        return err;
    }

    *out_present = false;
    *out_value = 0;

    let metric_name = match cstr_to_string(name, "name") {
        Ok(name) => name,
        Err(err) => return err,
    };

    let handle = &*db;
    let registry = handle.db.metrics();
    for registered_name in registry.names() {
        if registered_name == metric_name {
            if let Some(stat) = registry.lookup(registered_name) {
                *out_present = true;
                *out_value = stat.get();
            }
            break;
        }
    }

    success_result()
}

/// Reads a single key using default read options.
///
/// ## Arguments
/// - `db`: Database handle.
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
/// - All pointer arguments must be valid for reads/writes as appropriate.
/// - `out_val` must be freed with `slatedb_bytes_free` when `*out_present` is true.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_get(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    out_present: *mut bool,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    slatedb_db_get_with_options(
        db,
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
/// - `db`: Database handle.
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
pub unsafe extern "C" fn slatedb_db_get_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    read_options: *const slatedb_read_options_t,
    out_present: *mut bool,
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

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.get_with_options(key, &read_options))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::slatedb_error_kind_t;
    use serde_json::Value;
    use std::ffi::{CStr, CString};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

    fn next_test_path() -> String {
        static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);
        format!(
            "slatedb_c_metrics_test_{}",
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

    fn open_test_db() -> *mut slatedb_db_t {
        let runtime: Arc<Runtime> = Arc::new(
            RuntimeBuilder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build runtime"),
        );
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = next_test_path();
        let db = runtime
            .block_on(Db::open(path, object_store))
            .expect("failed to open test db");
        Box::into_raw(Box::new(slatedb_db_t { runtime, db }))
    }

    fn close_test_db(db: *mut slatedb_db_t) {
        assert_ok(unsafe { slatedb_db_close(db) });
    }

    fn fetch_metrics_snapshot(db: *const slatedb_db_t) -> Value {
        let mut json_data: *mut u8 = std::ptr::null_mut();
        let mut json_len: usize = 0;
        assert_ok(unsafe { slatedb_db_metrics(db, &mut json_data, &mut json_len) });
        assert!(!json_data.is_null());
        assert!(json_len > 0);

        let payload = unsafe { std::slice::from_raw_parts(json_data as *const u8, json_len) };
        let snapshot: Value = serde_json::from_slice(payload).expect("invalid metrics JSON");
        crate::memory::slatedb_bytes_free(json_data, json_len);
        snapshot
    }

    fn get_metric(db: *const slatedb_db_t, name: &CStr) -> (bool, i64) {
        let mut present = false;
        let mut value = 0i64;
        assert_ok(unsafe { slatedb_db_metric_get(db, name.as_ptr(), &mut present, &mut value) });
        (present, value)
    }

    fn perform_get(db: *mut slatedb_db_t, key: &[u8]) {
        let mut value_present = false;
        let mut value_data: *mut u8 = std::ptr::null_mut();
        let mut value_len = 0usize;
        assert_ok(unsafe {
            slatedb_db_get(
                db,
                key.as_ptr(),
                key.len(),
                &mut value_present,
                &mut value_data,
                &mut value_len,
            )
        });
        assert!(!value_present);
        assert!(value_data.is_null());
        assert_eq!(value_len, 0);
    }

    #[test]
    fn test_db_metrics_returns_json_snapshot() {
        let db = open_test_db();
        let snapshot = fetch_metrics_snapshot(db as *const slatedb_db_t);
        let object = snapshot
            .as_object()
            .expect("metrics snapshot should be a JSON object");
        assert!(
            object.contains_key("db/get_requests"),
            "expected db/get_requests in metrics snapshot"
        );
        close_test_db(db);
    }

    #[test]
    fn test_db_metric_get_for_known_and_unknown_metric_names() {
        let db = open_test_db();

        let known_name = CString::new("db/get_requests").expect("CString failed");
        let (present, before) = get_metric(db as *const slatedb_db_t, known_name.as_c_str());
        assert!(present);

        perform_get(db, b"missing-key");

        let (after_present, after) = get_metric(db as *const slatedb_db_t, known_name.as_c_str());
        assert!(after_present);
        assert!(after > before);

        let unknown_name = CString::new("db/not_a_metric").expect("CString failed");
        let (unknown_present, unknown_value) =
            get_metric(db as *const slatedb_db_t, unknown_name.as_c_str());
        assert!(!unknown_present);
        assert_eq!(unknown_value, 0);

        close_test_db(db);
    }

    #[test]
    fn test_db_metrics_and_metric_get_validate_required_pointers() {
        let db = open_test_db();
        let known_name = CString::new("db/get_requests").expect("CString failed");

        let mut json_len = 0usize;
        assert_result_kind(
            unsafe {
                slatedb_db_metrics(
                    db as *const slatedb_db_t,
                    std::ptr::null_mut(),
                    &mut json_len,
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );

        let mut present = false;
        assert_result_kind(
            unsafe {
                slatedb_db_metric_get(
                    db as *const slatedb_db_t,
                    known_name.as_ptr(),
                    &mut present,
                    std::ptr::null_mut(),
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );

        close_test_db(db);
    }
}
