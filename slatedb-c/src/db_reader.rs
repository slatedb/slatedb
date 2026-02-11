use std::os::raw::c_char;
use std::ptr;

use slatedb::config::DbReaderOptions;
use slatedb::DbReader;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

// Import our shared modules
use crate::config::{
    convert_range_bounds, convert_read_options, convert_reader_options, convert_scan_options,
};
use crate::error::{
    create_error_result, create_none_result, create_reader_handle_error_result,
    create_reader_handle_success_result, create_success_result, safe_str_from_ptr,
    slate_error_to_code, CSdbError, CSdbReaderHandleResult, CSdbResult,
};
use crate::object_store::create_object_store;
use crate::types::{CSdbIterator, CSdbReadOptions, CSdbScanOptions, CSdbValue};

/// Internal struct that owns a Tokio runtime and a SlateDB DbReader instance.
/// Similar to SlateDbFFI but for read-only operations.
pub struct SlateDbReaderFFI {
    pub rt: Runtime,
    pub reader: DbReader,
}

impl SlateDbReaderFFI {
    /// Convenience helper to run an async block on the internal runtime.
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }
}

/// Type-safe wrapper around a pointer to DbReaderFFI.
/// This provides better type safety than raw pointers.
#[repr(C)]
pub struct CSdbReaderHandle(pub *mut SlateDbReaderFFI);

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
    pub unsafe fn as_inner(&mut self) -> &mut SlateDbReaderFFI {
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
    /// When true, skip WAL replay entirely (only see compacted data)
    pub skip_wal_replay: bool,
}

impl Default for CSdbReaderOptions {
    fn default() -> Self {
        let defaults = DbReaderOptions::default();
        Self {
            manifest_poll_interval_ms: defaults.manifest_poll_interval.as_millis() as u64,
            checkpoint_lifetime_ms: defaults.checkpoint_lifetime.as_millis() as u64,
            max_memtable_bytes: defaults.max_memtable_bytes,
            skip_wal_replay: defaults.skip_wal_replay,
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_reader_open(
    path: *const c_char,
    url: *const c_char,
    env_file: *const c_char,
    checkpoint_id: *const c_char, // Nullable - use null for latest
    reader_options: *const CSdbReaderOptions,
) -> CSdbReaderHandleResult {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
        Err(err) => return create_reader_handle_error_result(err, "Invalid path"),
    };

    // Parse checkpoint ID if provided
    let checkpoint_uuid = if checkpoint_id.is_null() {
        None
    } else {
        match safe_str_from_ptr(checkpoint_id) {
            Ok(id_str) => match Uuid::parse_str(id_str) {
                Ok(uuid) => Some(uuid),
                Err(err) => {
                    return create_reader_handle_error_result(
                        CSdbError::InvalidArgument,
                        &format!("Invalid checkpoint_id format '{id_str}': {err}"),
                    )
                }
            },
            Err(err) => return create_reader_handle_error_result(err, "Invalid checkpoint_id"),
        }
    };

    // Parse reader options
    let opts = convert_reader_options(reader_options);

    // Create a dedicated runtime for this DbReader instance
    let rt = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(err) => {
            return create_reader_handle_error_result(CSdbError::InternalError, &err.to_string())
        }
    };

    let url_str: Option<&str> = if url.is_null() {
        None
    } else {
        match safe_str_from_ptr(url) {
            Ok(s) => Some(s),
            Err(err) => return create_reader_handle_error_result(err, "Invalid pointer for url"),
        }
    };
    let env_file_str = if env_file.is_null() {
        None
    } else {
        match safe_str_from_ptr(env_file) {
            Ok(s) => Some(s.to_string()),
            Err(err) => {
                return create_reader_handle_error_result(err, "Invalid pointer for env file")
            }
        }
    };
    let object_store = match create_object_store(url_str, env_file_str) {
        Ok(store) => store,
        Err(err) => {
            return CSdbReaderHandleResult {
                handle: CSdbReaderHandle::null(),
                result: err,
            }
        }
    };

    // Open DbReader
    match rt.block_on(async { DbReader::open(path_str, object_store, checkpoint_uuid, opts).await })
    {
        Ok(reader) => {
            let ffi = Box::new(SlateDbReaderFFI { rt, reader });
            create_reader_handle_success_result(CSdbReaderHandle(Box::into_raw(ffi)))
        }
        Err(err) => create_reader_handle_error_result(CSdbError::InternalError, &err.to_string()),
    }
}

/// # Safety
///
/// - `handle` must contain a valid reader handle pointer
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `read_options` must be a valid pointer to CSdbReadOptions or null
/// - `value_out` must be a valid pointer to a location where a value can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_reader_get_with_options(
    mut handle: CSdbReaderHandle,
    key: *const u8,
    key_len: usize,
    read_options: *const CSdbReadOptions,
    value_out: *mut CSdbValue,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid reader handle");
    }

    if key.is_null() || value_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key or value_out is null");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };

    // Convert C read options to Rust ReadOptions
    let rust_read_opts = convert_read_options(read_options);

    let inner = handle.as_inner();
    match inner.block_on(inner.reader.get_with_options(key_slice, &rust_read_opts)) {
        Ok(Some(bytes)) => {
            let value_vec = bytes.to_vec();
            let len = value_vec.len();

            let boxed_slice = value_vec.into_boxed_slice();
            let data = Box::into_raw(boxed_slice) as *mut u8;

            unsafe {
                *value_out = CSdbValue { data, len };
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
    let handle_ptr = handle.0;
    let inner = handle.as_inner();
    match inner.block_on(inner.reader.scan_with_options(range, &rust_scan_opts)) {
        Ok(iter) => {
            // Create iterator FFI wrapper tied to reader lifecycle
            let iter_box = CSdbIterator::new_reader(handle_ptr, iter);
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

/// # Safety
///
/// - `handle` must contain a valid reader handle pointer
/// - `prefix` must point to valid memory of at least `prefix_len` bytes (unless prefix_len is 0)
/// - `scan_options` must be a valid pointer to CSdbScanOptions or null
/// - `iterator_ptr` must be a valid pointer to a location where an iterator pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_reader_scan_prefix_with_options(
    mut handle: CSdbReaderHandle,
    prefix: *const u8,
    prefix_len: usize,
    scan_options: *const CSdbScanOptions,
    iterator_ptr: *mut *mut CSdbIterator,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid reader handle");
    }

    if iterator_ptr.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator pointer is null");
    }

    if prefix.is_null() && prefix_len > 0 {
        return create_error_result(CSdbError::NullPointer, "Prefix pointer is null");
    }

    let prefix_slice = if prefix_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(prefix, prefix_len) }
    };

    let rust_scan_opts = convert_scan_options(scan_options);

    // Extract raw pointer before borrowing handle
    let handle_ptr = handle.0;
    let inner = handle.as_inner();
    match inner.block_on(
        inner
            .reader
            .scan_prefix_with_options(prefix_slice, &rust_scan_opts),
    ) {
        Ok(iter) => {
            let iter_box = CSdbIterator::new_reader(handle_ptr, iter);
            unsafe {
                *iterator_ptr = Box::into_raw(iter_box);
            }
            create_success_result()
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Scan prefix operation failed: {}", e))
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
