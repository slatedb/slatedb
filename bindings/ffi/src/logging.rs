use std::ffi::CStr;

use crate::error::{FfiCloseReason, FfiSlatedbError};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiLogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl FfiLogLevel {
    fn into_c(self) -> slatedb_c::slatedb_log_level_t {
        match self {
            Self::Trace => slatedb_c::SLATEDB_LOG_LEVEL_TRACE,
            Self::Debug => slatedb_c::SLATEDB_LOG_LEVEL_DEBUG,
            Self::Info => slatedb_c::SLATEDB_LOG_LEVEL_INFO,
            Self::Warn => slatedb_c::SLATEDB_LOG_LEVEL_WARN,
            Self::Error => slatedb_c::SLATEDB_LOG_LEVEL_ERROR,
        }
    }
}

fn map_c_close_reason(reason: slatedb_c::slatedb_close_reason_t) -> FfiCloseReason {
    match reason {
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE => FfiCloseReason::None,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_CLEAN => FfiCloseReason::Clean,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_FENCED => FfiCloseReason::Fenced,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_PANIC => FfiCloseReason::Panic,
        _ => FfiCloseReason::Unknown,
    }
}

fn take_c_message(result: &slatedb_c::slatedb_result_t) -> String {
    if result.message.is_null() {
        String::new()
    } else {
        // SAFETY: `slatedb_result_t.message` is either null or a valid
        // NUL-terminated string allocated by `slatedb-c`.
        unsafe { CStr::from_ptr(result.message) }
            .to_string_lossy()
            .into_owned()
    }
}

fn result_from_slatedb_c(result: slatedb_c::slatedb_result_t) -> Result<(), FfiSlatedbError> {
    let mapped = match result.kind {
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE => Ok(()),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_TRANSACTION => {
            Err(FfiSlatedbError::Transaction {
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_CLOSED => {
            Err(FfiSlatedbError::Closed {
                reason: map_c_close_reason(result.close_reason),
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNAVAILABLE => {
            Err(FfiSlatedbError::Unavailable {
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID => {
            Err(FfiSlatedbError::Invalid {
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_DATA => Err(FfiSlatedbError::Data {
            message: take_c_message(&result),
        }),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL
        | slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNKNOWN => {
            Err(FfiSlatedbError::Internal {
                message: take_c_message(&result),
            })
        }
    };

    slatedb_c::slatedb_result_free(result);

    mapped
}

#[uniffi::export]
pub fn ffi_init_logging(level: FfiLogLevel) -> Result<(), FfiSlatedbError> {
    result_from_slatedb_c(slatedb_c::slatedb_logging_init(level.into_c()))
}

#[uniffi::export]
pub fn ffi_set_logging_level(level: FfiLogLevel) -> Result<(), FfiSlatedbError> {
    result_from_slatedb_c(slatedb_c::slatedb_logging_set_level(level.into_c()))
}

#[uniffi::export]
pub fn ffi_init_default_logging() -> Result<(), FfiSlatedbError> {
    ffi_init_logging(FfiLogLevel::Info)
}
