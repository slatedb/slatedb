use serde_json;
use slatedb::admin::load_object_store_from_env;
use slatedb::Db;
use std::os::raw::c_char;
use tokio::runtime::Builder;

use crate::config::{
    convert_put_options, convert_range_bounds, convert_read_options, convert_scan_options,
    convert_write_options, create_object_store, parse_store_config,
};
use crate::error::{
    create_error_result, create_success_result, safe_str_from_ptr, slate_error_to_code, CSdbError,
    CSdbResult,
};
use crate::types::{
    CSdbHandle, CSdbIterator, CSdbPutOptions, CSdbReadOptions, CSdbScanOptions, CSdbValue,
    CSdbWriteOptions, SlateDbFFI,
};
use slatedb::config::{Settings, SstBlockSize};

// ============================================================================
// Database Functions
// ============================================================================

#[no_mangle]
pub extern "C" fn slatedb_open(
    path: *const c_char,
    store_config_json: *const c_char,
) -> CSdbHandle {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
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
                create_object_store(&config).map_err(|_| CSdbError::InvalidArgument)
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
        // Use all defaults - custom configuration should use the Builder API
        Db::builder(path_str, object_store).build().await
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
/// - `start_key` must point to valid memory of at least `start_key_len` bytes (if not null)
/// - `end_key` must point to valid memory of at least `end_key_len` bytes (if not null)
/// - `scan_options` must be a valid pointer to CSdbScanOptions or null
/// - `iterator_ptr` must be a valid pointer to a location where an iterator pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_scan_with_options(
    mut handle: CSdbHandle,
    start_key: *const u8,
    start_key_len: usize,
    end_key: *const u8,
    end_key_len: usize,
    scan_options: *const CSdbScanOptions,
    iterator_ptr: *mut *mut CSdbIterator,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::NullPointer, "Database handle is null");
    }

    if iterator_ptr.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator output pointer is null");
    }

    // Extract raw pointer before borrowing handle
    let handle_ptr = handle.0;
    let db_ffi = handle.as_inner();

    // Convert range bounds
    let (start_bound, end_bound) =
        convert_range_bounds(start_key, start_key_len, end_key, end_key_len);

    // Convert scan options
    let scan_opts = convert_scan_options(scan_options);

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
                *iterator_ptr = Box::into_raw(iter_ffi);
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Scan operation failed: {}", e))
        }
    }
}

// ============================================================================
// Database Builder Functions
// ============================================================================

/// Create a new DbBuilder
#[no_mangle]
pub extern "C" fn slatedb_builder_new(
    path: *const c_char,
    store_config_json: *const c_char,
) -> *mut slatedb::DbBuilder<String> {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };

    // Create object store from config, fall back to environment on failure
    let object_store = {
        // Try to parse store config from JSON and create object store
        let store_result = safe_str_from_ptr(store_config_json)
            .and_then(|json_str| {
                parse_store_config(json_str).map_err(|_| CSdbError::InvalidArgument)
            })
            .and_then(|config| {
                create_object_store(&config).map_err(|_| CSdbError::InvalidArgument)
            });

        match store_result {
            Ok(store) => store,
            Err(_) => {
                // Config failed, try environment fallback
                match load_object_store_from_env(None) {
                    Ok(store) => store,
                    Err(_) => return std::ptr::null_mut(),
                }
            }
        }
    };

    let builder = Db::builder(path_str, object_store);
    Box::into_raw(Box::new(builder))
}

/// Set settings on DbBuilder from JSON
///
/// # Safety
///
/// - `builder` must be a valid pointer to a DbBuilder
/// - `settings_json` must be a valid C string pointer
#[no_mangle]
pub unsafe extern "C" fn slatedb_builder_with_settings(
    builder: *mut slatedb::DbBuilder<String>,
    settings_json: *const c_char,
) -> bool {
    if builder.is_null() || settings_json.is_null() {
        return false;
    }

    let settings_str = match safe_str_from_ptr(settings_json) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let settings: Settings = match serde_json::from_str(settings_str) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let builder_ref = &mut *builder;
    // We need to replace the builder since DbBuilder consumes self
    let old_builder = std::ptr::read(builder_ref);
    let new_builder = old_builder.with_settings(settings);
    std::ptr::write(builder_ref, new_builder);

    true
}

/// Set SST block size on DbBuilder
///
/// # Safety
///
/// - `builder` must be a valid pointer to a DbBuilder
#[no_mangle]
pub unsafe extern "C" fn slatedb_builder_with_sst_block_size(
    builder: *mut slatedb::DbBuilder<String>,
    size: u8,
) -> bool {
    if builder.is_null() {
        return false;
    }

    let block_size = match size {
        1 => SstBlockSize::Block1Kib,
        2 => SstBlockSize::Block2Kib,
        3 => SstBlockSize::Block4Kib,
        4 => SstBlockSize::Block8Kib,
        5 => SstBlockSize::Block16Kib,
        6 => SstBlockSize::Block32Kib,
        7 => SstBlockSize::Block64Kib,
        _ => return false,
    };

    let builder_ref = &mut *builder;
    // We need to replace the builder since DbBuilder consumes self
    let old_builder = std::ptr::read(builder_ref);
    let new_builder = old_builder.with_sst_block_size(block_size);
    std::ptr::write(builder_ref, new_builder);

    true
}

/// Build the database from DbBuilder
///
/// # Safety
///
/// - `builder` must be a valid pointer to a DbBuilder that was previously allocated
#[no_mangle]
pub unsafe extern "C" fn slatedb_builder_build(
    builder: *mut slatedb::DbBuilder<String>,
) -> CSdbHandle {
    if builder.is_null() {
        return CSdbHandle::null();
    }

    let builder_owned = Box::from_raw(builder);

    // Create a dedicated runtime for this DB instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return CSdbHandle::null(),
    };

    match rt.block_on(builder_owned.build()) {
        Ok(db) => {
            let ffi = SlateDbFFI { rt, db };
            CSdbHandle(Box::into_raw(Box::new(ffi)))
        }
        Err(_) => CSdbHandle::null(),
    }
}

/// Free DbBuilder
///
/// # Safety
///
/// - `builder` must be a valid pointer to a DbBuilder that was previously allocated
#[no_mangle]
pub unsafe extern "C" fn slatedb_builder_free(builder: *mut slatedb::DbBuilder<String>) {
    if !builder.is_null() {
        let _ = Box::from_raw(builder);
    }
}
