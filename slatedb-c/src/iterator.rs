//! Iterator APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for consuming database scan iterators.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, error_from_slate_error, require_handle, require_out_ptr,
    slatedb_iterator_t, slatedb_result_t, success_result,
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

/// Retrieves up to `max_count` key/value pairs from an iterator in a single call.
///
/// Results are packed into a single buffer with the following layout per entry:
/// ```text
/// [key_len: 8 bytes LE u64][val_len: 8 bytes LE u64][key_bytes][val_bytes]
/// ```
///
/// ## Arguments
/// - `iterator`: Iterator handle created by scan APIs.
/// - `max_count`: Maximum number of key/value pairs to return.
/// - `out_data`: Output buffer pointer (allocated by Rust, single allocation).
/// - `out_data_len`: Output total buffer length in bytes.
/// - `out_count`: Output number of key/value pairs in the buffer.
///
/// ## Returns
/// - `slatedb_result_t` with `kind == SLATEDB_ERROR_KIND_NONE` on success.
/// - `out_count == 0` means the iterator is exhausted.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid handles.
/// - Returns mapped SlateDB error kinds if iteration fails.
///
/// ## Safety
/// - All output pointers must be valid, non-null writable pointers.
/// - The buffer returned in `out_data` must be freed with `slatedb_bytes_free`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next_batch(
    iterator: *mut slatedb_iterator_t,
    max_count: usize,
    out_data: *mut *mut u8,
    out_data_len: *mut usize,
    out_count: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(iterator, "iterator") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_data, "out_data") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_data_len, "out_data_len") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_count, "out_count") {
        return err;
    }

    *out_data = std::ptr::null_mut();
    *out_data_len = 0;
    *out_count = 0;

    let handle = &mut *iterator;
    let mut buf = Vec::new();
    let mut count: usize = 0;

    for _ in 0..max_count {
        match handle.runtime.block_on(handle.iter.next()) {
            Ok(Some(kv)) => {
                let key = kv.key.as_ref();
                let val = kv.value.as_ref();
                buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
                buf.extend_from_slice(&(val.len() as u64).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(val);
                count += 1;
            }
            Ok(None) => {
                break;
            }
            Err(err) => {
                return error_from_slate_error(&err);
            }
        }
    }

    if count > 0 {
        let (data, data_len) = alloc_bytes(&buf);
        *out_data = data;
        *out_data_len = data_len;
    }
    *out_count = count;

    success_result()
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
