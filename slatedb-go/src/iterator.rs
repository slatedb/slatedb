use crate::error::{
    create_error_result, create_success_result, slate_error_to_code, CSdbError, CSdbResult,
};
use crate::types::{CSdbIterator, CSdbKeyValue};

// ============================================================================
// Iterator Functions
// ============================================================================

/// # Safety
///
/// - `iter` must be a valid pointer to a CSdbIterator
/// - `kv_out` must be a valid pointer to a location where a key-value pair can be stored
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next(
    iter: *mut CSdbIterator,
    kv_out: *mut CSdbKeyValue,
) -> CSdbResult {
    if iter.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator pointer is null");
    }

    if kv_out.is_null() {
        return create_error_result(CSdbError::NullPointer, "Output pointer is null");
    }

    let iter_ffi = unsafe { &mut *iter };

    // Validate DB pointer is still alive (basic check)
    if iter_ffi.db_ptr.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    let db_ffi = unsafe { &*iter_ffi.db_ptr };

    match db_ffi.block_on(iter_ffi.iter.next()) {
        Ok(Some(kv)) => {
            // Allocate memory for key and value using Box
            let key_len = kv.key.len();
            let value_len = kv.value.len();

            // Create boxed byte arrays and leak them to get raw pointers
            let key_bytes = kv.key.to_vec().into_boxed_slice();
            let key_ptr = Box::into_raw(key_bytes) as *mut u8;

            let value_bytes = kv.value.to_vec().into_boxed_slice();
            let value_ptr = Box::into_raw(value_bytes) as *mut u8;

            unsafe {
                (*kv_out).key.data = key_ptr;
                (*kv_out).key.len = key_len;
                (*kv_out).value.data = value_ptr;
                (*kv_out).value.len = value_len;
            }

            create_success_result()
        }
        Ok(None) => {
            // End of iteration - return NotFound to indicate end
            create_error_result(CSdbError::NotFound, "End of iteration")
        }
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Iterator next failed: {}", e))
        }
    }
}

/// # Safety
///
/// - `iter` must be a valid pointer to a CSdbIterator
/// - `key` must point to valid memory of at least `key_len` bytes
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_seek(
    iter: *mut CSdbIterator,
    key: *const u8,
    key_len: usize,
) -> CSdbResult {
    if iter.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator pointer is null");
    }

    if key.is_null() || key_len == 0 {
        return create_error_result(
            CSdbError::InvalidArgument,
            "Seek key cannot be null or empty",
        );
    }

    let iter_ffi = unsafe { &mut *iter };

    // Validate DB pointer is still alive (basic check)
    if iter_ffi.db_ptr.is_null() {
        return create_error_result(CSdbError::InvalidHandle, "Invalid database handle");
    }

    let key_slice = unsafe { std::slice::from_raw_parts(key, key_len) };
    let db_ffi = unsafe { &*iter_ffi.db_ptr };

    match db_ffi.block_on(iter_ffi.iter.seek(key_slice)) {
        Ok(_) => create_success_result(),
        Err(e) => {
            let error_code = slate_error_to_code(&e);
            create_error_result(error_code, &format!("Iterator seek failed: {}", e))
        }
    }
}

/// # Safety
///
/// - `iter` must be a valid pointer to a CSdbIterator that was previously allocated
#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_close(iter: *mut CSdbIterator) -> CSdbResult {
    if iter.is_null() {
        return create_error_result(CSdbError::NullPointer, "Iterator pointer is null");
    }

    // Simply drop the iterator - this frees all resources
    unsafe {
        let _ = Box::from_raw(iter); // Explicitly drop the Box
    }

    create_success_result()
}
