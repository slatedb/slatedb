use std::os::raw::c_char;
use std::ptr;

use slatedb::admin::load_object_store_from_env;
use slatedb::config::DbReaderOptions;
use slatedb::DbReader;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

// Import our shared modules
use crate::config::{
    convert_range_bounds, convert_read_options, convert_reader_options, convert_scan_options,
    create_object_store, parse_store_config,
};
use crate::error::{
    create_error_result, create_success_result, safe_str_from_ptr, slate_error_to_code, CSdbError,
    CSdbResult,
};
use crate::types::{CSdbIterator, CSdbReadOptions, CSdbScanOptions, CSdbValue, SlateDbFFI};

/// Internal struct that owns a Tokio runtime and a SlateDB DbReader instance.
/// Similar to SlateDbFFI but for read-only operations.
pub struct DbReaderFFI {
    pub rt: Runtime,
    pub reader: DbReader,
}

impl DbReaderFFI {
    /// Convenience helper to run an async block on the internal runtime.
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }
}

/// Type-safe wrapper around a pointer to DbReaderFFI.
/// This provides better type safety than raw pointers.
#[repr(C)]
pub struct CSdbReaderHandle(pub *mut DbReaderFFI);

impl CSdbReaderHandle {
    pub fn null() -> Self {
        CSdbReaderHandle(ptr::null_mut())
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// # Safety
    ///
    /// Caller must ensure the pointer is valid and properly aligned.
    /// The returned mutable reference must not outlive the pointer's validity.
    pub unsafe fn as_inner(&mut self) -> &mut DbReaderFFI {
        &mut *self.0
    }
}

/// DbReader options for FFI
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbReaderOptions {
    /// How often to poll for manifest updates (in milliseconds)
    pub manifest_poll_interval_ms: u64,
    /// How long checkpoints should live (in milliseconds)  
    pub checkpoint_lifetime_ms: u64,
    /// Max size of in-memory table for WAL buffering
    pub max_memtable_bytes: u64,
}

impl Default for CSdbReaderOptions {
    fn default() -> Self {
        let defaults = DbReaderOptions::default();
        Self {
            manifest_poll_interval_ms: defaults.manifest_poll_interval.as_millis() as u64,
            checkpoint_lifetime_ms: defaults.checkpoint_lifetime.as_millis() as u64,
            max_memtable_bytes: defaults.max_memtable_bytes,
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_reader_open(
    path: *const c_char,
    store_config_json: *const c_char,
    checkpoint_id: *const c_char, // Nullable - use null for latest
    reader_options: *const CSdbReaderOptions,
) -> CSdbReaderHandle {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
        Err(e) => {
            log::error!("slatedb_reader_open: Invalid path parameter: {:?}", e);
            return CSdbReaderHandle::null();
        }
    };

    // Parse checkpoint ID if provided
    let checkpoint_uuid = if checkpoint_id.is_null() {
        None
    } else {
        match safe_str_from_ptr(checkpoint_id) {
            Ok(id_str) => match Uuid::parse_str(id_str) {
                Ok(uuid) => Some(uuid),
                Err(e) => {
                    log::error!(
                        "slatedb_reader_open: Invalid checkpoint ID format '{}': {}",
                        id_str,
                        e
                    );
                    return CSdbReaderHandle::null();
                }
            },
            Err(e) => {
                log::error!(
                    "slatedb_reader_open: Invalid checkpoint_id parameter: {:?}",
                    e
                );
                return CSdbReaderHandle::null();
            }
        }
    };

    // Parse reader options
    let opts = convert_reader_options(reader_options);

    // Create a dedicated runtime for this DbReader instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            log::error!("slatedb_reader_open: Failed to create tokio runtime: {}", e);
            return CSdbReaderHandle::null();
        }
    };

    // Create object store: try config first, fall back to environment on any failure
    let object_store = {
        let store_result = safe_str_from_ptr(store_config_json)
            .and_then(|json_str| {
                parse_store_config(json_str).map_err(|_| CSdbError::InvalidArgument)
            })
            .and_then(|config| {
                create_object_store(&config).map_err(|_| CSdbError::InvalidArgument)
            });

        match store_result {
            Ok(store) => store,
            Err(config_error) => {
                log::warn!("slatedb_reader_open: Store config failed, trying environment fallback. Config error: {:?}", config_error);
                // Config failed, try environment fallback
                match load_object_store_from_env(None) {
                    Ok(store) => {
                        log::info!("slatedb_reader_open: Successfully created object store from environment");
                        store
                    }
                    Err(env_error) => {
                        log::error!("slatedb_reader_open: Both store config and environment fallback failed. Config error: {:?}, Env error: {}", config_error, env_error);
                        return CSdbReaderHandle::null();
                    }
                }
            }
        }
    };

    // Open DbReader
    match rt.block_on(async { DbReader::open(path_str, object_store, checkpoint_uuid, opts).await })
    {
        Ok(reader) => {
            log::info!(
                "slatedb_reader_open: Successfully opened DbReader for path '{}'",
                path_str
            );
            let ffi = Box::new(DbReaderFFI { rt, reader });
            CSdbReaderHandle(Box::into_raw(ffi))
        }
        Err(e) => {
            log::error!(
                "slatedb_reader_open: Failed to open DbReader for path '{}': {}",
                path_str,
                e
            );
            CSdbReaderHandle::null()
        }
    }
}

/// # Safety
///
/// - `handle` must contain a valid reader handle pointer
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `read_options` must be a valid pointer to CSdbReadOptions or null
/// - `result` must be a valid pointer to a location where a value can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_reader_get_with_options(
    mut handle: CSdbReaderHandle,
    key: *const u8,
    key_len: usize,
    read_options: *const CSdbReadOptions,
    result: *mut CSdbValue,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid reader handle");
    }

    if key.is_null() || result.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key or result is null");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };
    let rust_read_opts = convert_read_options(read_options);

    let inner = handle.as_inner();
    match inner.block_on(inner.reader.get_with_options(key_slice, &rust_read_opts)) {
        Ok(Some(bytes)) => {
            let data = bytes.as_ptr() as *mut u8;
            let len = bytes.len();

            // Leak the bytes so they remain valid for C caller
            std::mem::forget(bytes);

            unsafe {
                *result = CSdbValue { data, len };
            }
            create_success_result()
        }
        Ok(None) => {
            unsafe {
                *result = CSdbValue {
                    data: ptr::null_mut(),
                    len: 0,
                };
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Get operation failed: {}", e))
        }
    }
}

/// # Safety
///
/// - `handle` must contain a valid reader handle pointer
/// - `start_key` must point to valid memory of at least `start_key_len` bytes (if not null)
/// - `end_key` must point to valid memory of at least `end_key_len` bytes (if not null)
/// - `scan_options` must be a valid pointer to CSdbScanOptions or null
/// - `iterator_ptr` must be a valid pointer to a location where an iterator pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_reader_scan_with_options(
    mut handle: CSdbReaderHandle,
    start_key: *const u8,
    start_key_len: usize,
    end_key: *const u8,
    end_key_len: usize,
    scan_options: *const CSdbScanOptions,
    iterator_ptr: *mut *mut CSdbIterator,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid reader handle");
    }

    if iterator_ptr.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator pointer is null");
    }

    // Convert range bounds
    let range = convert_range_bounds(start_key, start_key_len, end_key, end_key_len);

    let rust_scan_opts = convert_scan_options(scan_options);

    // Extract raw pointer before borrowing handle
    let handle_ptr = handle.0 as *mut SlateDbFFI;
    let inner = handle.as_inner();
    match inner.block_on(inner.reader.scan_with_options(range, &rust_scan_opts)) {
        Ok(iter) => {
            // Create iterator FFI wrapper - we'll use a dummy SlateDbFFI pointer since
            // CSdbIterator was designed for DB, but DbReader iterators work similarly
            let iter_box = CSdbIterator::new(handle_ptr, iter);
            unsafe {
                *iterator_ptr = Box::into_raw(iter_box);
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Scan operation failed: {}", e))
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_reader_close(handle: CSdbReaderHandle) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid reader handle");
    }

    let inner = unsafe { Box::from_raw(handle.0) };

    // Close the reader
    match inner.block_on(inner.reader.close()) {
        Ok(_) => create_success_result(),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Close operation failed: {}", e))
        }
    }
}
