use std::error::Error as StdError;

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum SlateDbError {
    #[error("key cannot be empty")]
    EmptyKey,

    #[error("key size must be <= u16::MAX")]
    KeyTooLarge,

    #[error("value size must be <= u32::MAX")]
    ValueTooLarge,

    #[error("{field} is too large")]
    ValueTooLargeForUsize { field: &'static str },

    #[error("builder has already been consumed")]
    BuilderConsumed,

    #[error("transaction has already been completed")]
    TransactionCompleted,

    #[error("write batch has already been consumed")]
    WriteBatchConsumed,

    #[error("invalid checkpoint_id UUID: {source}")]
    InvalidCheckpointId { source: uuid::Error },

    #[error("range start cannot be empty")]
    EmptyRangeStart,

    #[error("range end cannot be empty")]
    EmptyRangeEnd,

    #[error("range start must not be greater than range end")]
    RangeStartGreaterThanEnd,

    #[error("range must be non-empty")]
    EmptyRange,

    #[error("{source}")]
    SettingsJsonParse { source: serde_json::Error },

    #[error("settings serialization failed: {source}")]
    SettingsSerialization { source: serde_json::Error },

    #[error("settings JSON root was not an object")]
    SettingsJsonRootNotObject,

    #[error("value_json is not valid JSON: {source}")]
    InvalidValueJson { source: serde_json::Error },

    #[error("key invalid: {message}")]
    InvalidSettingsKey { message: String },

    #[error("settings update produced invalid settings: {source}")]
    InvalidSettingsUpdate { source: serde_json::Error },

    #[error("object store creation failed: {source}")]
    ObjectStoreCreationError {
        #[from]
        source: Box<dyn StdError>,
    },
}

/// Error returned by a foreign [`crate::MergeOperator`] implementation.
#[derive(Debug, Error, uniffi::Error)]
pub enum MergeOperatorCallbackError {
    /// The merge callback failed with an application-defined message.
    #[error("{message}")]
    Failed { message: String },
}

/// Reason a database or reader reports itself as closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CloseReason {
    /// Closed cleanly by the caller.
    Clean,
    /// Closed because another writer fenced this instance.
    Fenced,
    /// Closed because of a panic in a background task.
    Panic,
    /// Closed for a reason not modeled explicitly by this binding.
    Unknown,
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

/// Error type returned by the UniFFI bindings.
#[derive(Debug, Error, uniffi::Error)]
pub enum Error {
    /// Transaction-specific failure.
    #[error("{message}")]
    Transaction { message: String },

    /// Operation attempted on a closed handle.
    #[error("{message}")]
    Closed {
        /// Reported reason the handle is closed.
        reason: CloseReason,
        /// Human-readable error message.
        message: String,
    },

    /// Temporary unavailability, such as an unavailable dependency.
    #[error("{message}")]
    Unavailable { message: String },

    /// Invalid input or invalid API usage.
    #[error("{message}")]
    Invalid { message: String },

    /// Corrupt, missing, or otherwise invalid data was encountered.
    #[error("{message}")]
    Data { message: String },

    /// Internal failure inside SlateDB or the binding layer.
    #[error("{message}")]
    Internal { message: String },

    /// The operation exceeded the configured timeout.
    #[error("{message}")]
    Timeout { message: String },
}

impl From<SlateDbError> for Error {
    fn from(error: SlateDbError) -> Self {
        let message = error.to_string();
        Self::Invalid { message }
    }
}

impl From<slatedb::Error> for Error {
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
