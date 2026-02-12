use crate::ffi::{slatedb_key_value_t, slatedb_result_t, slatedb_value_t};
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
pub extern "C" fn slatedb_value_free(value: slatedb_value_t) {
    if !value.data.is_null() && value.len > 0 {
        unsafe {
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(value.data, value.len));
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_key_value_free(key_value: slatedb_key_value_t) {
    slatedb_value_free(key_value.key);
    slatedb_value_free(key_value.value);
}
