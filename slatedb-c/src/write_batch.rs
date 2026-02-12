//! Write-batch APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions to create, mutate, and destroy
//! `WriteBatch` handles used by `slatedb_db_write*`.

use crate::ffi::{
    bytes_from_ptr, merge_options_from_ptr, put_options_from_ptr, require_handle, require_out_ptr,
    slatedb_merge_options_t, slatedb_put_options_t, slatedb_result_t, slatedb_write_batch_t,
    success_result, validate_write_key, validate_write_key_value, with_write_batch_mut,
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
    if let Err(err) = require_out_ptr(out_write_batch, "out_write_batch") {
        return err;
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

    if let Err(err) = with_write_batch_mut(write_batch, |batch| batch.put(key, value)) {
        return err;
    }
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

    if let Err(err) = with_write_batch_mut(write_batch, |batch| {
        batch.put_with_options(key, value, &put_options)
    }) {
        return err;
    }
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

    if let Err(err) = with_write_batch_mut(write_batch, |batch| batch.merge(key, value)) {
        return err;
    }
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

    if let Err(err) = with_write_batch_mut(write_batch, |batch| {
        batch.merge_with_options(key, value, &merge_options)
    }) {
        return err;
    }
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
    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    if let Err(err) = with_write_batch_mut(write_batch, |batch| batch.delete(key)) {
        return err;
    }
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
///
/// ## Safety
/// - `write_batch` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_close(
    write_batch: *mut slatedb_write_batch_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(write_batch, "write batch") {
        return err;
    }

    let _ = Box::from_raw(write_batch);

    success_result()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::slatedb_error_kind_t;
    use std::ffi::CStr;

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

    fn new_batch() -> *mut slatedb_write_batch_t {
        let mut write_batch: *mut slatedb_write_batch_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_write_batch_new(&mut write_batch) });
        assert!(!write_batch.is_null());
        write_batch
    }

    #[test]
    fn test_write_batch_new_rejects_null_out_pointer() {
        assert_result_kind(
            unsafe { slatedb_write_batch_new(std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }

    #[test]
    fn test_write_batch_put_rejects_empty_key() {
        let write_batch = new_batch();
        assert_result_kind(
            unsafe {
                slatedb_write_batch_put(
                    write_batch,
                    std::ptr::null(),
                    0,
                    b"value".as_ptr(),
                    b"value".len(),
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert_ok(unsafe { slatedb_write_batch_close(write_batch) });
    }

    #[test]
    fn test_write_batch_put_merge_delete_success() {
        let write_batch = new_batch();
        assert_ok(unsafe {
            slatedb_write_batch_put(
                write_batch,
                b"key".as_ptr(),
                b"key".len(),
                b"value".as_ptr(),
                b"value".len(),
            )
        });
        assert_ok(unsafe {
            slatedb_write_batch_merge(
                write_batch,
                b"key".as_ptr(),
                b"key".len(),
                b"operand".as_ptr(),
                b"operand".len(),
            )
        });
        assert_ok(unsafe {
            slatedb_write_batch_delete(write_batch, b"key".as_ptr(), b"key".len())
        });
        assert_ok(unsafe { slatedb_write_batch_close(write_batch) });
    }

    #[test]
    fn test_write_batch_mutation_rejects_consumed_batch() {
        let write_batch = new_batch();
        unsafe {
            (*write_batch).batch = None;
        }
        assert_result_kind(
            unsafe { slatedb_write_batch_delete(write_batch, b"key".as_ptr(), b"key".len()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert_ok(unsafe { slatedb_write_batch_close(write_batch) });
    }

    #[test]
    fn test_write_batch_close_rejects_null_handle() {
        assert_result_kind(
            unsafe { slatedb_write_batch_close(std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }
}
