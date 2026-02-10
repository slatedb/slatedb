use crate::types::CSdbHandle;
use crate::CSdbReaderHandle;
use slatedb::{Error as SlateError, ErrorKind};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// Error codes that will be exposed to C, they must match slate ErrorKind enum
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CSdbError {
    Success = 0,
    Transaction = 1,
    Closed = 2,
    Unavailable = 3,
    Invalid = 4,
    Data = 5,
    Internal = 6,
}

// Result type for returning both error codes and messages
#[repr(C)]
pub struct CSdbResult {
    pub error: CSdbError,
    pub none: bool,
    pub message: *mut c_char,
}

impl std::fmt::Display for CSdbResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {:?}", self.error, self.message)?;
        Ok(())
    }
}

#[repr(C)]
pub struct CSdbHandleResult {
    pub handle: CSdbHandle,
    pub result: CSdbResult,
}

#[repr(C)]
pub struct CSdbBuilderResult {
    pub builder: *mut slatedb::DbBuilder<String>,
    pub result: CSdbResult,
}

#[repr(C)]
pub struct CSdbReaderHandleResult {
    pub handle: CSdbReaderHandle,
    pub result: CSdbResult,
}

pub(crate) fn message_to_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("Invalid UTF-8").unwrap())
}

pub(crate) fn create_handle_error_result(error: CSdbError, message: &str) -> CSdbHandleResult {
    CSdbHandleResult {
        handle: CSdbHandle::null(),
        result: CSdbResult {
            error,
            none: false,
            message: message_to_cstring(message).into_raw(),
        },
    }
}

pub(crate) fn create_handle_error_result_from_slate_error(error: &SlateError) -> CSdbHandleResult {
    CSdbHandleResult {
        handle: CSdbHandle::null(),
        result: slate_error_to_error_result(error),
    }
}

pub(crate) fn create_handle_success_result(handler: CSdbHandle) -> CSdbHandleResult {
    CSdbHandleResult {
        handle: handler,
        result: create_success_result(),
    }
}

pub(crate) fn create_reader_handle_error_result(
    error: CSdbError,
    message: &str,
) -> CSdbReaderHandleResult {
    CSdbReaderHandleResult {
        handle: CSdbReaderHandle::null(),
        result: CSdbResult {
            error,
            none: false,
            message: message_to_cstring(message).into_raw(),
        },
    }
}

pub(crate) fn create_reader_handle_error_result_from_slate_error(
    error: &SlateError,
) -> CSdbReaderHandleResult {
    CSdbReaderHandleResult {
        handle: CSdbReaderHandle::null(),
        result: slate_error_to_error_result(error),
    }
}

pub(crate) fn create_reader_handle_success_result(
    handler: CSdbReaderHandle,
) -> CSdbReaderHandleResult {
    CSdbReaderHandleResult {
        handle: handler,
        result: create_success_result(),
    }
}

// Helper functions for error handling
pub(crate) fn create_error_result(error: CSdbError, message: &str) -> CSdbResult {
    CSdbResult {
        error,
        none: false,
        message: message_to_cstring(message).into_raw(),
    }
}

pub(crate) fn create_success_result() -> CSdbResult {
    CSdbResult {
        error: CSdbError::Success,
        none: false,
        message: std::ptr::null_mut(),
    }
}

pub(crate) fn create_none_result() -> CSdbResult {
    CSdbResult {
        error: CSdbError::Success,
        none: true,
        message: std::ptr::null_mut(),
    }
}

pub(crate) fn safe_str_from_ptr(ptr: *const c_char) -> Result<&'static str, CSdbError> {
    if ptr.is_null() {
        return Err(CSdbError::Invalid);
    }

    unsafe { CStr::from_ptr(ptr).to_str().map_err(|_| CSdbError::Invalid) }
}

pub(crate) fn slate_error_to_code(error: &SlateError) -> CSdbError {
    match error.kind() {
        ErrorKind::Transaction => CSdbError::Transaction,
        ErrorKind::Closed(_) => CSdbError::Closed,
        ErrorKind::Unavailable => CSdbError::Unavailable,
        ErrorKind::Invalid => CSdbError::Invalid,
        ErrorKind::Data => CSdbError::Data,
        _ => CSdbError::Internal,
    }
}

pub(crate) fn slate_error_to_error_result(error: &SlateError) -> CSdbResult {
    CSdbResult {
        error: slate_error_to_code(error),
        message: message_to_cstring(error.to_string().as_str()).into_raw(),
    }
}
