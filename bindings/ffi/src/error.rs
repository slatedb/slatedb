use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum FfiSlateDbError {
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

    #[error("seek key cannot be empty")]
    EmptySeekKey,

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
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FfiMergeOperatorCallbackError {
    #[error("{message}")]
    Failed { message: String },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiCloseReason {
    #[default]
    None,
    Clean,
    Fenced,
    Panic,
    Unknown,
}

impl From<slatedb::CloseReason> for FfiCloseReason {
    fn from(reason: slatedb::CloseReason) -> Self {
        match reason {
            slatedb::CloseReason::Clean => Self::Clean,
            slatedb::CloseReason::Fenced => Self::Fenced,
            slatedb::CloseReason::Panic => Self::Panic,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FfiError {
    #[error("{message}")]
    Transaction { message: String },

    #[error("{message}")]
    Closed {
        reason: FfiCloseReason,
        message: String,
    },

    #[error("{message}")]
    Unavailable { message: String },

    #[error("{message}")]
    Invalid { message: String },

    #[error("{message}")]
    Data { message: String },

    #[error("{message}")]
    Internal { message: String },
}

impl From<FfiSlateDbError> for FfiError {
    fn from(error: FfiSlateDbError) -> Self {
        let message = error.to_string();
        match error {
            _ => Self::Invalid { message },
        }
    }
}

impl From<slatedb::Error> for FfiError {
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
