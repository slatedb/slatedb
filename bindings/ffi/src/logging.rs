//! Logging helpers delegated to `slatedb-c`.

use std::ffi::CStr;

use crate::error::{CloseReason, SlatedbError};

/// The available logging levels.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl LogLevel {
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

fn map_c_close_reason(reason: slatedb_c::slatedb_close_reason_t) -> CloseReason {
    match reason {
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE => CloseReason::None,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_CLEAN => CloseReason::Clean,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_FENCED => CloseReason::Fenced,
        slatedb_c::slatedb_close_reason_t::SLATEDB_CLOSE_REASON_PANIC => CloseReason::Panic,
        _ => CloseReason::Unknown,
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

fn result_from_slatedb_c(result: slatedb_c::slatedb_result_t) -> Result<(), SlatedbError> {
    let mapped = match result.kind {
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE => Ok(()),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_TRANSACTION => {
            Err(SlatedbError::Transaction {
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_CLOSED => Err(SlatedbError::Closed {
            reason: map_c_close_reason(result.close_reason),
            message: take_c_message(&result),
        }),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNAVAILABLE => {
            Err(SlatedbError::Unavailable {
                message: take_c_message(&result),
            })
        }
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID => Err(SlatedbError::Invalid {
            message: take_c_message(&result),
        }),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_DATA => Err(SlatedbError::Data {
            message: take_c_message(&result),
        }),
        slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL
        | slatedb_c::slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNKNOWN => {
            Err(SlatedbError::Internal {
                message: take_c_message(&result),
            })
        }
    };

    slatedb_c::slatedb_result_free(result);

    mapped
}

/// Initialize SlateDB logging at the requested level.
#[uniffi::export]
pub fn init_logging(level: LogLevel) -> Result<(), SlatedbError> {
    result_from_slatedb_c(slatedb_c::slatedb_logging_init(level.into_c()))
}

/// Update the process-global SlateDB logging level.
#[uniffi::export]
pub fn set_logging_level(level: LogLevel) -> Result<(), SlatedbError> {
    result_from_slatedb_c(slatedb_c::slatedb_logging_set_level(level.into_c()))
}

/// Initialize SlateDB logging at the default `Info` level.
#[uniffi::export]
pub fn init_default_logging() -> Result<(), SlatedbError> {
    init_logging(LogLevel::Info)
}
