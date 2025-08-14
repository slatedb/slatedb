use slatedb::Error;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub(crate) enum DstError {
    #[error("failed to perform an operation on the SQLite state database")]
    SQLiteStateError(#[from] rusqlite::Error),
}

impl From<DstError> for Error {
    fn from(err: DstError) -> Self {
        let msg = err.to_string();
        match err {
            DstError::SQLiteStateError(e) => Error::operation(msg).with_source(Box::new(e)),
        }
    }
}
