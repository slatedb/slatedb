use serde_json;
use slatedb::Db;
use std::collections::HashMap;
use std::os::raw::c_char;
use tokio::runtime::Builder;

use crate::config::{
    convert_put_options, convert_range_bounds, convert_read_options, convert_scan_options,
    convert_write_options,
};
use crate::error::{
    create_error_result, create_handle_error_result, create_handle_success_result,
    create_none_result, create_success_result, message_to_cstring, safe_str_from_ptr,
    slate_error_to_code, CSdbBuilderResult, CSdbError, CSdbHandleResult, CSdbResult,
};
use crate::object_store::create_object_store;
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
    url: *const c_char,
    env_file: *const c_char,
) -> CSdbHandleResult {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
        Err(err) => return create_handle_error_result(err, "Invalid path"),
    };

    // Create a dedicated runtime for this DB instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(err) => return create_handle_error_result(CSdbError::InternalError, &err.to_string()),
    };

    let url_str: Option<&str> = if url.is_null() {
        None
    } else {
        match safe_str_from_ptr(url) {
            Ok(s) => Some(s),
            Err(err) => return create_handle_error_result(err, "Invalid pointer for url"),
        }
    };
    let env_file_str = if env_file.is_null() {
        None
    } else {
        match safe_str_from_ptr(env_file) {
            Ok(s) => Some(s.to_string()),
            Err(err) => return create_handle_error_result(err, "Invalid pointer for env file"),
        }
    };
    let object_store = match create_object_store(url_str, env_file_str) {
        Ok(store) => store,
        Err(err) => {
            return CSdbHandleResult {
                handle: CSdbHandle::null(),
                result: err,
            }
        }
    };

    match rt.block_on(async {
        // Use all defaults - custom configuration should use the Builder API
        Db::builder(path_str, object_store).build().await
    }) {
        Ok(db) => {
            let ffi = Box::new(SlateDbFFI { rt, db });
            create_handle_success_result(CSdbHandle(Box::into_raw(ffi)))
        }
        Err(err) => create_handle_error_result(CSdbError::InternalError, &err.to_string()),
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
        Ok(None) => create_none_result(),
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
            let iter_ffi = CSdbIterator::new_db(handle_ptr, iter);
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

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `prefix` must point to valid memory of at least `prefix_len` bytes (unless prefix_len is 0)
/// - `scan_options` must be a valid pointer to CSdbScanOptions or null
/// - `iterator_ptr` must be a valid pointer to a location where an iterator pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_scan_prefix_with_options(
    mut handle: CSdbHandle,
    prefix: *const u8,
    prefix_len: usize,
    scan_options: *const CSdbScanOptions,
    iterator_ptr: *mut *mut CSdbIterator,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::NullPointer, "Database handle is null");
    }

    if iterator_ptr.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator output pointer is null");
    }

    if prefix.is_null() && prefix_len > 0 {
        return create_error_result(CSdbError::NullPointer, "Prefix pointer is null");
    }

    let prefix_slice = if prefix_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(prefix, prefix_len) }
    };

    // Extract raw pointer before borrowing handle
    let handle_ptr = handle.0;
    let db_ffi = handle.as_inner();

    let scan_opts = convert_scan_options(scan_options);

    match db_ffi.block_on(db_ffi.db.scan_prefix_with_options(prefix_slice, &scan_opts)) {
        Ok(iter) => {
            let iter_ffi = CSdbIterator::new_db(handle_ptr, iter);
            unsafe {
                *iterator_ptr = Box::into_raw(iter_ffi);
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Scan prefix operation failed: {}", e))
        }
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `value_out` must be a valid pointer to a location where a value can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_metrics(
    mut handle: CSdbHandle,
    value_out: *mut CSdbValue,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }
    if value_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "value_out is null");
    }
    let inner = handle.as_inner();
    let metrics = inner.db.metrics();
    let mut metrics_map: HashMap<String, i64> = HashMap::new();
    for name in metrics.names() {
        if let Some(value) = metrics.lookup(name) {
            metrics_map.insert(name.to_string(), value.get());
        }
    }
    match serde_json::to_vec(&metrics_map) {
        Ok(json_vec) => {
            let len = json_vec.len();
            let boxed_slice = json_vec.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut u8;
            unsafe {
                (*value_out).data = ptr;
                (*value_out).len = len;
            }
            create_success_result()
        }
        Err(e) => create_error_result(
            CSdbError::InternalError,
            &format!("Metrics serialization failed: {}", e),
        ),
    }
}

// ============================================================================
// Database Builder Functions
// ============================================================================

/// Create a new DbBuilder
#[no_mangle]
pub extern "C" fn slatedb_builder_new(
    path: *const c_char,
    url: *const c_char,
    env_file: *const c_char,
) -> CSdbBuilderResult {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s.to_string(),
        Err(err) => {
            return CSdbBuilderResult {
                builder: std::ptr::null_mut(),
                result: CSdbResult {
                    error: err,
                    none: false,
                    message: message_to_cstring("Invalid path").into_raw(),
                },
            }
        }
    };

    let url_str: Option<&str> = if url.is_null() {
        None
    } else {
        match safe_str_from_ptr(url) {
            Ok(s) => Some(s),
            Err(err) => {
                return CSdbBuilderResult {
                    builder: std::ptr::null_mut(),
                    result: CSdbResult {
                        error: err,
                        none: false,
                        message: message_to_cstring("Invalid URL").into_raw(),
                    },
                }
            }
        }
    };
    let env_file_str = if env_file.is_null() {
        None
    } else {
        match safe_str_from_ptr(env_file) {
            Ok(s) => Some(s.to_string()),
            Err(err) => {
                return CSdbBuilderResult {
                    builder: std::ptr::null_mut(),
                    result: CSdbResult {
                        error: err,
                        none: false,
                        message: message_to_cstring("Invalid env file path").into_raw(),
                    },
                }
            }
        }
    };
    let object_store = match create_object_store(url_str, env_file_str) {
        Ok(store) => store,
        Err(err) => {
            return CSdbBuilderResult {
                builder: std::ptr::null_mut(),
                result: err,
            }
        }
    };

    let builder = Db::builder(path_str, object_store);
    CSdbBuilderResult {
        builder: Box::into_raw(Box::new(builder)),
        result: create_success_result(),
    }
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
) -> CSdbResult {
    if builder.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid pointer to builder");
    }
    if settings_json.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid pointer to settings json");
    }

    let settings_str = match safe_str_from_ptr(settings_json) {
        Ok(s) => s,
        Err(err) => return create_error_result(err, "Invalid settings json"),
    };

    let settings: Settings = match serde_json::from_str(settings_str) {
        Ok(s) => s,
        Err(err) => {
            return create_error_result(
                CSdbError::InvalidArgument,
                &format!("Invalid settings json: {}", err),
            )
        }
    };

    let builder_ref = &mut *builder;
    // We need to replace the builder since DbBuilder consumes self
    let old_builder = std::ptr::read(builder_ref);
    let new_builder = old_builder.with_settings(settings);
    std::ptr::write(builder_ref, new_builder);

    create_success_result()
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
) -> CSdbResult {
    if builder.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid pointer to builder");
    }

    let block_size = match size {
        1 => SstBlockSize::Block1Kib,
        2 => SstBlockSize::Block2Kib,
        3 => SstBlockSize::Block4Kib,
        4 => SstBlockSize::Block8Kib,
        5 => SstBlockSize::Block16Kib,
        6 => SstBlockSize::Block32Kib,
        7 => SstBlockSize::Block64Kib,
        _ => {
            return create_error_result(
                CSdbError::InvalidArgument,
                &format!("Invalid block size: {}", size),
            )
        }
    };

    let builder_ref = &mut *builder;
    // We need to replace the builder since DbBuilder consumes self
    let old_builder = std::ptr::read(builder_ref);
    let new_builder = old_builder.with_sst_block_size(block_size);
    std::ptr::write(builder_ref, new_builder);

    create_success_result()
}

/// Build the database from DbBuilder
///
/// # Safety
///
/// - `builder` must be a valid pointer to a DbBuilder that was previously allocated
#[no_mangle]
pub unsafe extern "C" fn slatedb_builder_build(
    builder: *mut slatedb::DbBuilder<String>,
) -> CSdbHandleResult {
    if builder.is_null() {
        return create_handle_error_result(CSdbError::InvalidHandle, "Invalid builder handle");
    }

    let builder_owned = Box::from_raw(builder);

    // Create a dedicated runtime for this DB instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(err) => return create_handle_error_result(CSdbError::InternalError, &err.to_string()),
    };

    match rt.block_on(builder_owned.build()) {
        Ok(db) => {
            let ffi = Box::new(SlateDbFFI { rt, db });
            create_handle_success_result(CSdbHandle(Box::into_raw(ffi)))
        }
        Err(err) => create_handle_error_result(CSdbError::InternalError, &err.to_string()),
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
