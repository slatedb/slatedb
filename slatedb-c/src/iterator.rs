//! Iterator APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for consuming database scan iterators.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, error_from_slate_error, error_result, require_handle,
    require_out_ptr, slatedb_error_kind_t, slatedb_iterator_t, slatedb_result_t,
    slatedb_row_entry_t, success_result, SLATEDB_ROW_ENTRY_KIND_MERGE,
    SLATEDB_ROW_ENTRY_KIND_TOMBSTONE, SLATEDB_ROW_ENTRY_KIND_VALUE,
};

/// Retrieves the next key/value pair from an iterator.
///
/// ## Arguments
/// - `iterator`: Iterator handle created by scan APIs.
/// - `out_present`: Set to `true` when a row is returned.
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
    out_present: *mut bool,
    out_key: *mut *mut u8,
    out_key_len: *mut usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(iterator, "iterator") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_key, "out_key") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_key_len, "out_key_len") {
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

/// ## Safety
/// - `iterator` must be a valid iterator handle.
/// - `out_present` must be a valid pointer to a `bool`.
/// - `out_row` must be a valid pointer to a `*mut slatedb_row_entry_t`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next_row(
    iterator: *mut slatedb_iterator_t,
    out_present: *mut bool,
    out_row: *mut *mut slatedb_row_entry_t,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID, "iterator");
    }
    // out_present and out_row are checked by the C compiler/runtime if strict,
    // but good to check null here too if we want robustness.
    if out_present.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_present",
        );
    }
    if out_row.is_null() {
        return error_result(slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID, "out_row");
    }

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next_row()) {
        Ok(Some(row)) => {
            let (key, key_len) = alloc_bytes(row.key.as_ref());
            let (kind, value, value_len) = match &row.value {
                slatedb::ValueDeletable::Value(v) => {
                    let (val, val_len) = alloc_bytes(v.as_ref());
                    (SLATEDB_ROW_ENTRY_KIND_VALUE, val, val_len)
                }
                slatedb::ValueDeletable::Merge(v) => {
                    let (val, val_len) = alloc_bytes(v.as_ref());
                    (SLATEDB_ROW_ENTRY_KIND_MERGE, val, val_len)
                }
                slatedb::ValueDeletable::Tombstone => {
                    (SLATEDB_ROW_ENTRY_KIND_TOMBSTONE, std::ptr::null_mut(), 0)
                }
            };

            let c_row = Box::new(slatedb_row_entry_t {
                kind,
                key,
                key_len,
                value,
                value_len,
                seq: row.seq,
                create_ts: row.create_ts.unwrap_or(0),
                create_ts_present: row.create_ts.is_some(),
                expire_ts: row.expire_ts.unwrap_or(0),
                expire_ts_present: row.expire_ts.is_some(),
            });

            *out_row = Box::into_raw(c_row);
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
    if let Err(err) = require_handle(iterator, "iterator") {
        return err;
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.seek(key)) {
        Ok(()) => success_result(),
        Err(err) => error_from_slate_error(&err),
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
    if let Err(err) = require_handle(iterator, "iterator") {
        return err;
    }

    let _ = Box::from_raw(iterator);

    success_result()
}
