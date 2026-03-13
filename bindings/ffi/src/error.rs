use thiserror::Error;

#[derive(Debug, Error, uniffi::Error)]
pub enum SlatedbError {
    #[error("{message}")]
    Transaction { message: String },

    #[error("{message}")]
    Closed { message: String },

    #[error("{message}")]
    Unavailable { message: String },

    #[error("{message}")]
    Invalid { message: String },

    #[error("{message}")]
    Data { message: String },

    #[error("{message}")]
    Internal { message: String },
}

impl From<slatedb::Error> for SlatedbError {
    fn from(error: slatedb::Error) -> Self {
        let message = error.to_string();
        match error.kind() {
            slatedb::ErrorKind::Transaction => Self::Transaction { message },
            slatedb::ErrorKind::Closed(_) => Self::Closed { message },
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
