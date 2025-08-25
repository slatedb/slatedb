use slatedb::admin::load_object_store_from_env;
use slatedb::Db;
use std::os::raw::c_char;
use tokio::runtime::Builder;

use crate::config::{
    convert_put_options, convert_range_bounds, convert_read_options, convert_scan_options,
    convert_write_options, create_object_store, parse_db_options, parse_store_config,
};
use crate::error::{
    create_error_result, create_success_result, safe_str_from_ptr, slate_error_to_code, CSdbError,
    CSdbResult,
};
use crate::types::{
    CSdbHandle, CSdbIterator, CSdbPutOptions, CSdbReadOptions, CSdbScanOptions, CSdbValue,
    CSdbWriteOptions, SlateDbFFI,
};

// ============================================================================
// Database Functions
// ============================================================================

#[no_mangle]
pub extern "C" fn slatedb_open(
    path: *const c_char,
    store_config_json: *const c_char,
    options_json: *const c_char,
) -> CSdbHandle {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
        Err(_) => return CSdbHandle::null(),
    };

    // Parse options JSON
    let options_str = if options_json.is_null() {
        None
    } else {
        match safe_str_from_ptr(options_json) {
            Ok(s) => Some(s),
            Err(_) => return CSdbHandle::null(),
        }
    };

    let db_options = match parse_db_options(options_str) {
        Ok(opts) => opts,
        Err(_) => return CSdbHandle::null(),
    };

    // Create a dedicated runtime for this DB instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return CSdbHandle::null(),
    };

    // Create object store: try config first, fall back to environment on any failure
    let object_store = {
        // Try to parse store config from JSON
        let store_result = safe_str_from_ptr(store_config_json)
            .and_then(|json_str| {
                parse_store_config(json_str).map_err(|_| CSdbError::InvalidArgument)
            })
            .and_then(|config| {
                rt.block_on(create_object_store(&config))
                    .map_err(|_| CSdbError::InvalidArgument)
            });

        match store_result {
            Ok(store) => store,
            Err(_) => {
                // Config failed, try environment fallback
                match load_object_store_from_env(None) {
                    Ok(store) => store,
                    Err(_) => return CSdbHandle::null(),
                }
            }
        }
    };

    match rt.block_on(async {
        let mut builder = Db::builder(path_str, object_store).with_settings(db_options.settings);

        if let Some(block_size) = db_options.sst_block_size {
            builder = builder.with_sst_block_size(block_size);
        }

        builder.build().await
    }) {
        Ok(db) => {
            let ffi = Box::new(SlateDbFFI { rt, db });
            CSdbHandle(Box::into_raw(ffi))
        }
        Err(_) => CSdbHandle::null(),
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `value` must point to valid memory of at least `value_len` bytes
/// - `put_options` must be a valid pointer to CSdbPutOptions or null
/// - `write_options` must be a valid pointer to CSdbWriteOptions or null
#[no_mangle]
pub unsafe extern "C" fn slatedb_put_with_options(
    mut handle: CSdbHandle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    put_options: *const CSdbPutOptions,
    write_options: *const CSdbWriteOptions,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    if key.is_null() || value.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key or value is null");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };
    let value_slice = unsafe { std::slice::from_raw_parts(value, value_len) };

    // Convert C options to Rust options using centralized functions
    let rust_put_opts = convert_put_options(put_options);
    let rust_write_opts = convert_write_options(write_options);

    let inner = handle.as_inner();
    match inner.block_on(inner.db.put_with_options(
        key_slice,
        value_slice,
        &rust_put_opts,
        &rust_write_opts,
    )) {
        Ok(_) => create_success_result(),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(
                error_code,
                &format!("Put with options operation failed: {}", e),
            )
        }
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `write_options` must be a valid pointer to CSdbWriteOptions or null
#[no_mangle]
pub unsafe extern "C" fn slatedb_delete_with_options(
    mut handle: CSdbHandle,
    key: *const u8,
    key_len: usize,
    write_options: *const CSdbWriteOptions,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    if key.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key is null");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };

    // Convert C write options to Rust WriteOptions
    let rust_write_opts = convert_write_options(write_options);

    let inner = handle.as_inner();
    match inner.block_on(inner.db.delete_with_options(key_slice, &rust_write_opts)) {
        Ok(_) => create_success_result(),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(
                error_code,
                &format!("Delete with options operation failed: {}", e),
            )
        }
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `read_options` must be a valid pointer to CSdbReadOptions or null
/// - `value_out` must be a valid pointer to a location where a value can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_get_with_options(
    mut handle: CSdbHandle,
    key: *const u8,
    key_len: usize,
    read_options: *const CSdbReadOptions,
    value_out: *mut CSdbValue,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    if key.is_null() || value_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key or value_out is null");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };

    // Convert C read options to Rust ReadOptions
    let rust_read_opts = convert_read_options(read_options);

    let inner = handle.as_inner();
    match inner.block_on(inner.db.get_with_options(key_slice, &rust_read_opts)) {
        Ok(Some(value)) => {
            let value_vec = value.to_vec();
            let len = value_vec.len();

            // Convert to boxed slice first, then get pointer
            let boxed_slice = value_vec.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut u8;

            unsafe {
                (*value_out).data = ptr;
                (*value_out).len = len;
            }

            create_success_result()
        }
        Ok(None) => create_error_result(CSdbError::NotFound, "Key not found"),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(
                error_code,
                &format!("Get with options operation failed: {}", e),
            )
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_flush(mut handle: CSdbHandle) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    let inner = unsafe { handle.as_inner() };
    match inner.block_on(inner.db.flush()) {
        Ok(_) => create_success_result(),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Flush operation failed: {}", e))
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_close(handle: CSdbHandle) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    unsafe {
        let inner = Box::from_raw(handle.0);
        match inner.block_on(inner.db.close()) {
            Ok(_) => create_success_result(),
            Err(e) => {
                let error_code = slate_error_to_code(&e);
                create_error_result(error_code, &format!("Close operation failed: {}", e))
            }
        }
        // inner (SlateDbFFI) is automatically dropped here along with its runtime and DB
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `start` must point to valid memory of at least `start_len` bytes (if not null)
/// - `end` must point to valid memory of at least `end_len` bytes (if not null)
/// - `options` must be a valid pointer to CSdbScanOptions or null
/// - `iter_out` must be a valid pointer to a location where an iterator pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_scan_with_options(
    mut handle: CSdbHandle,
    start: *const u8,
    start_len: usize,
    end: *const u8,
    end_len: usize,
    options: *const CSdbScanOptions,
    iter_out: *mut *mut CSdbIterator,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::NullPointer, "Database handle is null");
    }

    if iter_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator output pointer is null");
    }

    // Extract raw pointer before borrowing handle
    let handle_ptr = handle.0;
    let db_ffi = handle.as_inner();

    // Convert range bounds
    let (start_bound, end_bound) = convert_range_bounds(start, start_len, end, end_len);

    // Convert scan options
    let scan_opts = convert_scan_options(options);

    // Create the iterator using the bounds
    match db_ffi.block_on(
        db_ffi
            .db
            .scan_with_options((start_bound, end_bound), &scan_opts),
    ) {
        Ok(iter) => {
            // Create FFI wrapper
            let iter_ffi = CSdbIterator::new(handle_ptr, iter);
            unsafe {
                *iter_out = Box::into_raw(iter_ffi);
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Scan operation failed: {}", e))
        }
    }
}
