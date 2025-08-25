use crate::config::{convert_put_options, convert_write_options};
use crate::error::{
    create_error_result, create_success_result, slate_error_to_code, CSdbError, CSdbResult,
};
use crate::types::{CSdbHandle, CSdbPutOptions, CSdbWriteBatch, CSdbWriteOptions};
use slatedb::WriteBatch;

// ============================================================================
// WriteBatch Functions
// ============================================================================

/// # Safety
///
/// - `batch_out` must be a valid pointer to a location where a batch pointer can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_new(
    batch_out: *mut *mut CSdbWriteBatch,
) -> CSdbResult {
    if batch_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "Output pointer is null");
    }

    let batch = CSdbWriteBatch {
        batch: Some(WriteBatch::new()),
    };

    unsafe {
        *batch_out = Box::into_raw(Box::new(batch));
    }

    create_success_result()
}

/// # Safety
///
/// - `batch` must be a valid pointer to a WriteBatch
/// - `key` must point to valid memory of at least `key_len` bytes
/// - `value` must point to valid memory of at least `value_len` bytes
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_put(
    batch: *mut CSdbWriteBatch,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> CSdbResult {
    if batch.is_null() {
        return create_error_result(CSdbError::NullPointer, "WriteBatch pointer is null");
    }
    if key.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key pointer is null");
    }
    if value.is_null() {
        return create_error_result(CSdbError::NullPointer, "Value pointer is null");
    }

    let batch_ref = unsafe { &mut *batch };

    if let Some(ref mut wb) = batch_ref.batch {
        let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };
        let value_slice = unsafe { std::slice::from_raw_parts(value, value_len) };

        // Validate key/value sizes (match Rust core validation)
        if key_slice.is_empty() {
            return create_error_result(CSdbError::InvalidArgument, "Key cannot be empty");
        }
        if key_len > u16::MAX as usize {
            return create_error_result(CSdbError::InvalidArgument, "Key size must be <= u16::MAX");
        }
        if value_len > u32::MAX as usize {
            return create_error_result(
                CSdbError::InvalidArgument,
                "Value size must be <= u32::MAX",
            );
        }

        wb.put(key_slice, value_slice);
        create_success_result()
    } else {
        create_error_result(CSdbError::InvalidArgument, "WriteBatch already consumed")
    }
}

/// # Safety
///
/// - `batch` must be a valid pointer to a WriteBatch
/// - `key` must point to valid memory of at least `key_len` bytes  
/// - `value` must point to valid memory of at least `value_len` bytes
/// - `options` must be a valid pointer to CSdbPutOptions or null
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_put_with_options(
    batch: *mut CSdbWriteBatch,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    options: *const CSdbPutOptions,
) -> CSdbResult {
    if batch.is_null() {
        return create_error_result(CSdbError::NullPointer, "WriteBatch pointer is null");
    }
    if key.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key pointer is null");
    }
    if value.is_null() {
        return create_error_result(CSdbError::NullPointer, "Value pointer is null");
    }

    let batch_ref = unsafe { &mut *batch };

    if let Some(ref mut wb) = batch_ref.batch {
        let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };
        let value_slice = unsafe { std::slice::from_raw_parts(value, value_len) };

        // Validate key/value sizes (match Rust core validation)
        if key_slice.is_empty() {
            return create_error_result(CSdbError::InvalidArgument, "Key cannot be empty");
        }
        if key_len > u16::MAX as usize {
            return create_error_result(CSdbError::InvalidArgument, "Key size must be <= u16::MAX");
        }
        if value_len > u32::MAX as usize {
            return create_error_result(
                CSdbError::InvalidArgument,
                "Value size must be <= u32::MAX",
            );
        }

        // Convert options
        let put_options = convert_put_options(options);
        wb.put_with_options(key_slice, value_slice, &put_options);

        create_success_result()
    } else {
        create_error_result(CSdbError::InvalidArgument, "WriteBatch already consumed")
    }
}

/// # Safety
///
/// - `batch` must be a valid pointer to a WriteBatch
/// - `key` must point to valid memory of at least `key_len` bytes
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_delete(
    batch: *mut CSdbWriteBatch,
    key: *const u8,
    key_len: usize,
) -> CSdbResult {
    if batch.is_null() {
        return create_error_result(CSdbError::NullPointer, "WriteBatch pointer is null");
    }
    if key.is_null() {
        return create_error_result(CSdbError::NullPointer, "Key pointer is null");
    }

    let batch_ref = unsafe { &mut *batch };

    if let Some(ref mut wb) = batch_ref.batch {
        let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };

        // Validate key size (match Rust core validation)
        if key_slice.is_empty() {
            return create_error_result(CSdbError::InvalidArgument, "Key cannot be empty");
        }
        if key_len > u16::MAX as usize {
            return create_error_result(CSdbError::InvalidArgument, "Key size must be <= u16::MAX");
        }

        wb.delete(key_slice);
        create_success_result()
    } else {
        create_error_result(CSdbError::InvalidArgument, "WriteBatch already consumed")
    }
}

/// # Safety
///
/// - `handle` must contain a valid database handle pointer
/// - `batch` must be a valid pointer to a WriteBatch
/// - `options` must be a valid pointer to CSdbWriteOptions or null
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_write(
    mut handle: CSdbHandle,
    batch: *mut CSdbWriteBatch,
    options: *const CSdbWriteOptions,
) -> CSdbResult {
    if handle.is_null() {
        return create_error_result(CSdbError::NullPointer, "Database handle is null");
    }
    if batch.is_null() {
        return create_error_result(CSdbError::NullPointer, "WriteBatch pointer is null");
    }

    let db_ref = handle.as_inner();
    let batch_ref = unsafe { &mut *batch };

    // Take the batch out - this "consumes" it
    let wb = match batch_ref.batch.take() {
        Some(b) => b,
        None => {
            return create_error_result(CSdbError::InvalidArgument, "WriteBatch already consumed");
        }
    };

    // Convert write options
    let write_options = convert_write_options(options);

    // Execute the write
    match db_ref.block_on(async { db_ref.db.write_with_options(wb, &write_options).await }) {
        Ok(_) => create_success_result(),
        Err(e) => create_error_result(
            slate_error_to_code(&e),
            &format!("Failed to write batch: {}", e),
        ),
    }
}

/// # Safety
///
/// - `batch` must be a valid pointer to a WriteBatch that was previously allocated
#[no_mangle]
pub unsafe extern "C" fn slatedb_write_batch_close(batch: *mut CSdbWriteBatch) -> CSdbResult {
    if batch.is_null() {
        return create_error_result(CSdbError::NullPointer, "WriteBatch pointer is null");
    }

    // Simply drop the WriteBatch - this frees all resources
    unsafe {
        let _ = Box::from_raw(batch); // Explicitly drop the Box
    }

    create_success_result()
}
