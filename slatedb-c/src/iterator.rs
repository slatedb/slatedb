//! Iterator APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for consuming database scan iterators.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, error_from_slate_error, error_result, slatedb_error_kind_t,
    slatedb_iterator_t, slatedb_result_t, success_result,
};

/// Retrieves the next key/value pair from an iterator.
///
/// ## Arguments
/// - `iterator`: Iterator handle created by scan APIs.
/// - `out_has_item`: Set to `true` when a row is returned.
/// - `out_key`: Output key buffer pointer (allocated by Rust).
/// - `out_key_len`: Output key length.
/// - `out_val`: Output value buffer pointer (allocated by Rust).
/// - `out_val_len`: Output value length.
///
/// ## Returns
/// - `slatedb_result_t` with `kind == SLATEDB_ERROR_KIND_NONE` on success.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid handles.
/// - Returns mapped SlateDB error kinds if iteration fails.
///
/// ## Safety
/// - All output pointers must be valid, non-null writable pointers.
/// - Buffers returned in `out_key`/`out_val` must be freed with
///   `slatedb_bytes_free`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next(
    iterator: *mut slatedb_iterator_t,
    out_has_item: *mut bool,
    out_key: *mut *mut u8,
    out_key_len: *mut usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid iterator handle",
        );
    }
    if out_key.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_key pointer is null",
        );
    }
    if out_key_len.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_key_len pointer is null",
        );
    }
    if out_val.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_val pointer is null",
        );
    }
    if out_val_len.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_val_len pointer is null",
        );
    }
    if out_has_item.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_has_item pointer is null",
        );
    }

    *out_has_item = false;
    *out_key = std::ptr::null_mut();
    *out_key_len = 0;
    *out_val = std::ptr::null_mut();
    *out_val_len = 0;

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next()) {
        Ok(Some(kv)) => {
            let (key, key_len) = alloc_bytes(kv.key.as_ref());
            let (val, val_len) = alloc_bytes(kv.value.as_ref());
            *out_key = key;
            *out_key_len = key_len;
            *out_val = val;
            *out_val_len = val_len;
            *out_has_item = true;
            success_result()
        }
        Ok(None) => {
            *out_has_item = false;
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("iterator next failed: {err}")),
    }
}

/// Seeks the iterator to the first key greater than or equal to `key`.
///
/// ## Arguments
/// - `iterator`: Iterator handle.
/// - `key`: Seek target key bytes.
/// - `key_len`: Length of `key`.
///
/// ## Returns
/// - `slatedb_result_t` describing success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles/pointers.
/// - Returns mapped SlateDB error kinds for seek failures.
///
/// ## Safety
/// - `iterator` must be a valid iterator handle.
/// - `key` must reference at least `key_len` readable bytes when `key_len > 0`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_seek(
    iterator: *mut slatedb_iterator_t,
    key: *const u8,
    key_len: usize,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid iterator handle",
        );
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.seek(key)) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err, &format!("iterator seek failed: {err}")),
    }
}

/// Closes and frees an iterator handle.
///
/// ## Arguments
/// - `iterator`: Iterator handle previously returned from scan APIs.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` when `iterator` is null.
///
/// ## Safety
/// - `iterator` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_close(
    iterator: *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid iterator handle",
        );
    }

    let _ = Box::from_raw(iterator);

    success_result()
}
