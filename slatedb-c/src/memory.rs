use crate::ffi::slatedb_result_t;
use std::ffi::CString;

#[no_mangle]
pub extern "C" fn slatedb_result_free(result: slatedb_result_t) {
    if !result.message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.message);
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_bytes_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(data, len));
        }
    }
}
