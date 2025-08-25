use slatedb::Error as SlateError;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// Error codes that will be exposed to C/Go
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CSdbError {
    Success = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    IOError = 4,
    InternalError = 5,
    NullPointer = 6,
    InvalidHandle = 7,
}

// Result type for returning both error codes and messages
#[repr(C)]
pub struct CSdbResult {
    pub error: CSdbError,
    pub message: *mut c_char,
}

// Helper functions for error handling
pub fn create_error_result(error: CSdbError, message: &str) -> CSdbResult {
    let c_message =
        CString::new(message).unwrap_or_else(|_| CString::new("Invalid UTF-8").unwrap());
    CSdbResult {
        error,
        message: c_message.into_raw(),
    }
}

pub fn create_success_result() -> CSdbResult {
    CSdbResult {
        error: CSdbError::Success,
        message: std::ptr::null_mut(),
    }
}

pub fn safe_str_from_ptr(ptr: *const c_char) -> Result<&'static str, CSdbError> {
    if ptr.is_null() {
        return Err(CSdbError::NullPointer);
    }

    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map_err(|_| CSdbError::InvalidArgument)
    }
}

pub fn slate_error_to_code(error: &SlateError) -> CSdbError {
    // Use string matching since we can't pattern match on the exact variants
    let error_str = format!("{:?}", error);
    if error_str.contains("NotFound") {
        CSdbError::NotFound
    } else if error_str.contains("AlreadyExists") {
        CSdbError::AlreadyExists
    } else if error_str.contains("InvalidRequest") {
        CSdbError::InvalidArgument
    } else if error_str.contains("IO") {
        CSdbError::IOError
    } else {
        CSdbError::InternalError
    }
}
