use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum FfiCloseReason {
    #[default]
    None,
    Clean,
    Fenced,
    Panic,
    Unknown,
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FfiSlatedbError {
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

#[derive(Debug, Error, uniffi::Error)]
pub enum FfiMergeOperatorCallbackError {
    #[error("{message}")]
    Failed { message: String },
}

impl From<slatedb::Error> for FfiSlatedbError {
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

impl From<serde_json::Error> for FfiSlatedbError {
    fn from(error: serde_json::Error) -> Self {
        Self::Invalid {
            message: error.to_string(),
        }
    }
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
