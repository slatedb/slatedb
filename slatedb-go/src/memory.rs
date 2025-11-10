use std::ffi::CString;

use crate::error::CSdbResult;
use crate::types::{CSdbScanResult, CSdbValue};

// Memory management functions
#[no_mangle]
pub extern "C" fn slatedb_free_result(result: CSdbResult) {
    if !result.message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.message);
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_free_value(value: CSdbValue) {
    if !value.data.is_null() && value.len > 0 {
        unsafe {
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(value.data, value.len));
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_free_scan_result(result: CSdbScanResult) {
    if !result.items.is_null() && result.count > 0 {
        unsafe {
            // Convert back to Box to free properly
            let items_boxed = Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                result.items,
                result.count,
            ));

            // Free each individual key/value
            for item in items_boxed.iter() {
                slatedb_free_value(item.key);
                slatedb_free_value(item.value);
            }
            // items_boxed is automatically dropped here
        }
    }

    // Free the next_key if it's set
    if !result.next_key.data.is_null() && result.next_key.len > 0 {
        slatedb_free_value(result.next_key);
    }
}
