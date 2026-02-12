//! Write-batch APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions to create, mutate, and destroy
//! `WriteBatch` handles used by `slatedb_db_write*`.

use crate::ffi::{
    bytes_from_ptr, error_result, merge_options_from_ptr, put_options_from_ptr,
    slatedb_error_kind_t, slatedb_merge_options_t, slatedb_put_options_t, slatedb_result_t,
    slatedb_write_batch_t, success_result, validate_write_key, validate_write_key_value,
};
use slatedb::WriteBatch;

/// Allocates a new empty write batch.
///
/// ## Arguments
/// - `out_write_batch`: Output pointer that receives the new batch handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` if `out_write_batch` is null.
///
/// ## Safety
/// - `out_write_batch` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_new(
    out_write_batch: *mut *mut slatedb_write_batch_t,
) -> slatedb_result_t {
    if out_write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_write_batch pointer is null",
        );
    }

    let handle = Box::new(slatedb_write_batch_t {
        batch: Some(WriteBatch::new()),
    });
    *out_write_batch = Box::into_raw(handle);
    success_result()
}

/// Appends a `put` operation to a write batch.
///
/// ## Arguments
/// - `write_batch`: Write batch handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Value bytes.
/// - `value_len`: Length of `value`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, consumed batches,
///   null pointers, or invalid key/value sizes.
///
/// ## Safety
/// - `write_batch` must be a valid batch handle.
/// - `key`/`value` must reference at least `key_len`/`value_len` readable bytes
///   when lengths are non-zero.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_put(
    write_batch: *mut slatedb_write_batch_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
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

    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        );
    };

    batch.put(key, value);
    success_result()
}

/// Appends a `put` operation with explicit put options.
///
/// ## Arguments
/// - `write_batch`: Write batch handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Value bytes.
/// - `value_len`: Length of `value`.
/// - `put_options`: Optional put options pointer (null uses defaults).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, consumed batches,
///   null pointers, invalid options, or invalid key/value sizes.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as appropriate.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_put_with_options(
    write_batch: *mut slatedb_write_batch_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    put_options: *const slatedb_put_options_t,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
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

    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        );
    };

    batch.put_with_options(key, value, &put_options);
    success_result()
}

/// Appends a `merge` operation to a write batch.
///
/// ## Arguments
/// - `write_batch`: Write batch handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Merge operand bytes.
/// - `value_len`: Length of `value`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, consumed batches,
///   null pointers, or invalid key/value sizes.
///
/// ## Safety
/// - Pointer arguments must be valid for reads as required.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_merge(
    write_batch: *mut slatedb_write_batch_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
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

    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        );
    };

    batch.merge(key, value);
    success_result()
}

/// Appends a `merge` operation with explicit merge options.
///
/// ## Arguments
/// - `write_batch`: Write batch handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
/// - `value`: Merge operand bytes.
/// - `value_len`: Length of `value`.
/// - `merge_options`: Optional merge options pointer (null uses defaults).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, consumed batches,
///   null pointers, invalid options, or invalid key/value sizes.
///
/// ## Safety
/// - Pointer arguments must be valid for reads/writes as appropriate.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_merge_with_options(
    write_batch: *mut slatedb_write_batch_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    merge_options: *const slatedb_merge_options_t,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
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

    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        );
    };

    batch.merge_with_options(key, value, &merge_options);
    success_result()
}

/// Appends a `delete` operation to a write batch.
///
/// ## Arguments
/// - `write_batch`: Write batch handle.
/// - `key`: Key bytes.
/// - `key_len`: Length of `key`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, consumed batches,
///   null pointers, or invalid key size.
///
/// ## Safety
/// - `key` must reference at least `key_len` readable bytes when `key_len > 0`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_delete(
    write_batch: *mut slatedb_write_batch_t,
    key: *const u8,
    key_len: usize,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        );
    };

    batch.delete(key);
    success_result()
}

/// Closes and frees a write batch handle.
///
/// ## Arguments
/// - `write_batch`: Batch handle to destroy.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` when `write_batch` is null.
#[no_mangle]
pub extern "C" fn slatedb_write_batch_close(
    write_batch: *mut slatedb_write_batch_t,
) -> slatedb_result_t {
    if write_batch.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid write batch handle",
        );
    }

    unsafe {
        let _ = Box::from_raw(write_batch);
    }

    success_result()
}
