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

/// Retrieves up to `max_count` key/value pairs (or up to `max_bytes` of packed
/// data) from an iterator in a single call — whichever limit is reached first.
///
/// Results are packed into a single buffer with the following layout per entry:
/// ```text
/// [key_len: 8 bytes LE u64][val_len: 8 bytes LE u64][key_bytes][val_bytes]
/// ```
///
/// ## Arguments
/// - `iterator`: Iterator handle created by scan APIs.
/// - `max_count`: Maximum number of key/value pairs to return. Pass `0` for no
///   count limit.
/// - `max_bytes`: Maximum packed buffer size in bytes. The last entry is allowed
///   to push the buffer over this limit so that at least one entry is always
///   returned when the iterator is not exhausted. Pass `0` for no byte limit.
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
    max_bytes: usize,
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

    loop {
        if max_count > 0 && count >= max_count {
            break;
        }
        if max_bytes > 0 && buf.len() >= max_bytes {
            break;
        }

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

#[cfg(test)]
mod tests {
    use crate::db::{slatedb_db_close, slatedb_db_put, slatedb_db_scan};
    use crate::ffi::{
        slatedb_bound_t, slatedb_db_t, slatedb_error_kind_t, slatedb_iterator_t, slatedb_range_t,
        slatedb_result_t, SLATEDB_BOUND_KIND_UNBOUNDED,
    };
    use crate::memory::slatedb_bytes_free;
    use slatedb::Db;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

    use super::*;

    fn next_test_path() -> String {
        static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);
        format!(
            "slatedb_c_iterator_test_{}",
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

    fn put_kv(db: *mut slatedb_db_t, key: &[u8], val: &[u8]) {
        assert_ok(unsafe { slatedb_db_put(db, key.as_ptr(), key.len(), val.as_ptr(), val.len()) });
    }

    fn unbounded_range() -> slatedb_range_t {
        slatedb_range_t {
            start: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: std::ptr::null(),
                len: 0,
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: std::ptr::null(),
                len: 0,
            },
        }
    }

    fn scan_all(db: *mut slatedb_db_t) -> *mut slatedb_iterator_t {
        let mut iter: *mut slatedb_iterator_t = std::ptr::null_mut();
        assert_ok(unsafe { slatedb_db_scan(db, unbounded_range(), &mut iter) });
        assert!(!iter.is_null());
        iter
    }

    /// Parses the packed batch buffer into key/value pairs.
    /// Format per entry: [key_len: u64 LE][val_len: u64 LE][key_bytes][val_bytes]
    fn parse_batch(data: *const u8, data_len: usize, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let buf = unsafe { std::slice::from_raw_parts(data, data_len) };
        let mut offset = 0;
        let mut pairs = Vec::with_capacity(count);
        for _ in 0..count {
            let key_len = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            let val_len = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            let key = buf[offset..offset + key_len].to_vec();
            offset += key_len;
            let val = buf[offset..offset + val_len].to_vec();
            offset += val_len;
            pairs.push((key, val));
        }
        assert_eq!(offset, data_len, "buffer not fully consumed");
        pairs
    }

    #[test]
    fn test_next_batch_returns_all_entries() {
        let db = open_test_db();
        for i in 0..5 {
            put_kv(
                db,
                format!("key_{i:02}").as_bytes(),
                format!("val_{i:02}").as_bytes(),
            );
        }

        let iter = scan_all(db);
        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;

        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                10,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });

        assert_eq!(out_count, 5);
        assert!(!out_data.is_null());

        let pairs = parse_batch(out_data, out_data_len, out_count);
        for (i, (key, val)) in pairs.iter().enumerate().take(5) {
            assert_eq!(key, &format!("key_{i:02}").as_bytes());
            assert_eq!(val, &format!("val_{i:02}").as_bytes());
        }

        slatedb_bytes_free(out_data, out_data_len);
        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }

    #[test]
    fn test_next_batch_respects_max_count() {
        let db = open_test_db();
        for i in 0..5 {
            put_kv(
                db,
                format!("key_{i:02}").as_bytes(),
                format!("val_{i:02}").as_bytes(),
            );
        }

        let iter = scan_all(db);
        let mut all_pairs = Vec::new();

        // First batch: expect 2
        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                2,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });
        assert_eq!(out_count, 2);
        all_pairs.extend(parse_batch(out_data, out_data_len, out_count));
        slatedb_bytes_free(out_data, out_data_len);

        // Second batch: expect 2
        out_data = std::ptr::null_mut();
        out_data_len = 0;
        out_count = 0;
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                2,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });
        assert_eq!(out_count, 2);
        all_pairs.extend(parse_batch(out_data, out_data_len, out_count));
        slatedb_bytes_free(out_data, out_data_len);

        // Third batch: expect 1 (remaining)
        out_data = std::ptr::null_mut();
        out_data_len = 0;
        out_count = 0;
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                2,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });
        assert_eq!(out_count, 1);
        all_pairs.extend(parse_batch(out_data, out_data_len, out_count));
        slatedb_bytes_free(out_data, out_data_len);

        // Verify ordering across batches
        for (i, (key, val)) in all_pairs.iter().enumerate().take(5) {
            assert_eq!(key, &format!("key_{i:02}").as_bytes());
            assert_eq!(val, &format!("val_{i:02}").as_bytes());
        }

        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }

    #[test]
    fn test_next_batch_empty_iterator() {
        let db = open_test_db();
        let iter = scan_all(db);

        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;

        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                10,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });

        assert_eq!(out_count, 0);
        assert!(out_data.is_null());
        assert_eq!(out_data_len, 0);

        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }

    #[test]
    fn test_next_batch_after_seek() {
        let db = open_test_db();
        for i in 0..5 {
            put_kv(
                db,
                format!("key_{i:02}").as_bytes(),
                format!("val_{i:02}").as_bytes(),
            );
        }

        let iter = scan_all(db);
        let seek_key = b"key_02";
        assert_ok(unsafe { slatedb_iterator_seek(iter, seek_key.as_ptr(), seek_key.len()) });

        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;

        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                10,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });

        assert_eq!(out_count, 3);
        let pairs = parse_batch(out_data, out_data_len, out_count);
        assert_eq!(pairs[0].0, b"key_02");
        assert_eq!(pairs[1].0, b"key_03");
        assert_eq!(pairs[2].0, b"key_04");

        slatedb_bytes_free(out_data, out_data_len);
        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }

    #[test]
    fn test_next_batch_respects_max_bytes() {
        let db = open_test_db();
        // Each entry: 8 (key_len) + 8 (val_len) + key_bytes + val_bytes
        // "k0" + "v0" = 2 + 2 = 4 bytes payload, 16 bytes header = 20 bytes per entry
        put_kv(db, b"k0", b"v0");
        put_kv(db, b"k1", b"v1");
        put_kv(db, b"k2", b"v2");
        put_kv(db, b"k3", b"v3");
        put_kv(db, b"k4", b"v4");

        let iter = scan_all(db);
        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;

        // Set max_bytes to 40, which fits exactly 2 entries (20 bytes each).
        // The third entry would push buf.len() to 60 which is >= 40, so the
        // loop breaks after checking count > 0 && buf.len() >= max_bytes.
        // But note: the check happens BEFORE reading the next entry, so with
        // max_bytes=40, after 2 entries buf.len()=40 >= 40, loop breaks.
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                0,
                40,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });

        assert_eq!(out_count, 2);
        let pairs = parse_batch(out_data, out_data_len, out_count);
        assert_eq!(pairs[0].0, b"k0");
        assert_eq!(pairs[1].0, b"k1");
        slatedb_bytes_free(out_data, out_data_len);

        // Read remaining entries
        out_data = std::ptr::null_mut();
        out_data_len = 0;
        out_count = 0;
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                0,
                40,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });
        assert_eq!(out_count, 2);
        let pairs = parse_batch(out_data, out_data_len, out_count);
        assert_eq!(pairs[0].0, b"k2");
        assert_eq!(pairs[1].0, b"k3");
        slatedb_bytes_free(out_data, out_data_len);

        // Last entry
        out_data = std::ptr::null_mut();
        out_data_len = 0;
        out_count = 0;
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                0,
                40,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });
        assert_eq!(out_count, 1);
        let pairs = parse_batch(out_data, out_data_len, out_count);
        assert_eq!(pairs[0].0, b"k4");
        slatedb_bytes_free(out_data, out_data_len);

        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }

    #[test]
    fn test_next_batch_zero_limits_means_unlimited() {
        let db = open_test_db();
        for i in 0..5 {
            put_kv(
                db,
                format!("key_{i:02}").as_bytes(),
                format!("val_{i:02}").as_bytes(),
            );
        }

        let iter = scan_all(db);
        let mut out_data: *mut u8 = std::ptr::null_mut();
        let mut out_data_len: usize = 0;
        let mut out_count: usize = 0;

        // Both limits 0 = unlimited, should return all entries
        assert_ok(unsafe {
            slatedb_iterator_next_batch(
                iter,
                0,
                0,
                &mut out_data,
                &mut out_data_len,
                &mut out_count,
            )
        });

        assert_eq!(out_count, 5);
        let pairs = parse_batch(out_data, out_data_len, out_count);
        for (i, (key, val)) in pairs.iter().enumerate().take(5) {
            assert_eq!(key, &format!("key_{i:02}").as_bytes());
            assert_eq!(val, &format!("val_{i:02}").as_bytes());
        }

        slatedb_bytes_free(out_data, out_data_len);
        assert_ok(unsafe { slatedb_iterator_close(iter) });
        close_test_db(db);
    }
}
