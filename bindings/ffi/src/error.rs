//! Error types returned by the SlateDB FFI wrapper.

use thiserror::Error;

/// The reason a database handle was closed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum CloseReason {
    /// No close reason is available.
    #[default]
    None,
    /// The database was closed cleanly.
    Clean,
    /// The database instance was fenced by another writer.
    Fenced,
    /// A background task panicked.
    Panic,
    /// The close reason was not recognized by this binding version.
    Unknown,
}

/// Error returned by the SlateDB FFI layer.
///
/// The FFI wrapper groups core SlateDB errors into a smaller set of stable
/// categories while preserving the original message text.
#[derive(Debug, Error, uniffi::Error)]
pub enum SlatedbError {
    /// A transaction failed to commit or otherwise encountered a conflict.
    #[error("{message}")]
    Transaction {
        /// The original error message.
        message: String,
    },

    /// The database or transaction handle has already been closed.
    #[error("{message}")]
    Closed {
        /// The reason the handle was closed.
        reason: CloseReason,
        /// The original error message.
        message: String,
    },

    /// A required dependency or remote service is temporarily unavailable.
    #[error("{message}")]
    Unavailable {
        /// The original error message.
        message: String,
    },

    /// The caller supplied invalid input.
    #[error("{message}")]
    Invalid {
        /// The original error message.
        message: String,
    },

    /// Stored data was invalid or could not be decoded.
    #[error("{message}")]
    Data {
        /// The original error message.
        message: String,
    },

    /// An unexpected internal failure occurred.
    #[error("{message}")]
    Internal {
        /// The original error message.
        message: String,
    },
}

/// Error returned by foreign merge operator callbacks.
#[derive(Debug, Error, uniffi::Error)]
pub enum MergeOperatorCallbackError {
    /// The merge operator rejected the input or could not produce a merged value.
    #[error("{message}")]
    Failed {
        /// The original error message.
        message: String,
    },
}

impl From<slatedb::Error> for SlatedbError {
    fn from(error: slatedb::Error) -> Self {
        let message = error.to_string();
        match error.kind() {
            slatedb::ErrorKind::Transaction => Self::Transaction { message },
            slatedb::ErrorKind::Closed(reason) => Self::Closed {
                reason: reason.into(),
                message,
            },
            slatedb::ErrorKind::Unavailable => Self::Unavailable { message },
            slatedb::ErrorKind::Invalid => Self::Invalid { message },
            slatedb::ErrorKind::Data => Self::Data { message },
            slatedb::ErrorKind::Internal => Self::Internal { message },
            _ => Self::Internal { message },
        }
    }
}

impl From<serde_json::Error> for SlatedbError {
    fn from(error: serde_json::Error) -> Self {
        Self::Invalid {
            message: error.to_string(),
        }
    }
}

impl From<slatedb::CloseReason> for CloseReason {
    fn from(reason: slatedb::CloseReason) -> Self {
        match reason {
            slatedb::CloseReason::Clean => Self::Clean,
            slatedb::CloseReason::Fenced => Self::Fenced,
            slatedb::CloseReason::Panic => Self::Panic,
            _ => Self::Unknown,
        }
    }
}
