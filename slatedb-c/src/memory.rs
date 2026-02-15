//! Memory-management helper APIs for `slatedb-c`.
//!
//! These functions free Rust-allocated buffers returned by FFI calls.

use crate::ffi::slatedb_result_t;
use std::ffi::CString;

/// Frees heap memory referenced by `slatedb_result_t.message`.
///
/// ## Arguments
/// - `result`: Result value returned by a SlateDB C API function.
///
/// ## Returns
/// - No return value.
///
/// ## Safety
/// - `result.message` must be either null or a pointer returned by this crate.
#[no_mangle]
pub extern "C" fn slatedb_result_free(result: slatedb_result_t) {
    if !result.message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.message);
        }
    }
}

/// Frees a byte buffer allocated by SlateDB C APIs.
///
/// ## Arguments
/// - `data`: Byte pointer returned by this crate.
/// - `len`: Buffer length associated with `data`.
///
/// ## Returns
/// - No return value.
///
/// ## Safety
/// - `data`/`len` must exactly match a buffer allocated by this crate.
/// - Do not call more than once for the same allocation.
#[no_mangle]
pub extern "C" fn slatedb_bytes_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(data, len));
        }
    }
}
