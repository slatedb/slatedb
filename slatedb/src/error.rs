use object_store::path::Path;
use std::any::Any;
use std::ops::Bound;
use std::sync::Mutex;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error as ThisError;
use uuid::Uuid;

use crate::bytes_range::BytesRange;
use crate::merge_operator::MergeOperatorError;

#[non_exhaustive]
#[derive(Clone, Debug, ThisError)]
pub(crate) enum SlateDBError {
    #[error("io error")]
    IoError(#[from] Arc<std::io::Error>),

    #[error("checksum mismatch")]
    ChecksumMismatch,

    #[error("empty SSTable")]
    EmptySSTable,

    #[error("empty block metadata")]
    EmptyBlockMeta,

    #[error("empty block")]
    EmptyBlock,

    #[error("empty manifest")]
    EmptyManifest,

    #[error("object store error")]
    ObjectStoreError(#[from] Arc<object_store::Error>),

    #[error("file already exists")]
    FileVersionExists,

    #[error("manifest file already exists")]
    ManifestVersionExists,

    #[error("failed to find manifest with id. id=`{0}`")]
    ManifestMissing(u64),

    #[error("failed to find latest manifest")]
    LatestManifestMissing,

    #[error("invalid deletion")]
    InvalidDeletion,

    #[error("invalid sst error")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("invalid DB state error")]
    InvalidDBState,

    #[error("unsupported operation. operation=`{0}`")]
    Unsupported(String),

    #[error("invalid compaction")]
    InvalidCompaction,

    #[error("compaction executor failed")]
    CompactionExecutorFailed,

    #[error(
        "invalid clock tick, must be monotonic. last_tick=`{last_tick}`, next_tick=`{next_tick}`"
    )]
    InvalidClockTick { last_tick: i64, next_tick: i64 },

    #[error("detected newer DB client")]
    Fenced,

    #[error("invalid cache part size bytes, it must be multiple of 1024 and greater than 0")]
    InvalidCachePartSize,

    #[error("invalid compression codec")]
    InvalidCompressionCodec,

    #[cfg(any(
        feature = "snappy",
        feature = "zlib",
        feature = "lz4",
        feature = "zstd"
    ))]
    #[error("error decompressing block")]
    BlockDecompressionError,

    #[cfg(any(feature = "snappy", feature = "zlib", feature = "zstd"))]
    #[error("error compressing block")]
    BlockCompressionError,

    #[error("Invalid RowFlags. #{message}. encoded_bits=`{encoded_bits:#b}`, known_bits=`{known_bits:#b}`"
    )]
    InvalidRowFlags {
        encoded_bits: u8,
        known_bits: u8,
        message: String,
    },

    #[error("read channel error")]
    ReadChannelError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("iterator invalidated after unexpected error")]
    InvalidatedIterator(#[from] Box<SlateDBError>),

    #[error("background task panic'd")]
    // we need to wrap the panic args in an Arc so SlateDbError is Clone
    // we need to wrap the panic args in a mutex so that SlateDbError is Sync
    BackgroundTaskPanic(Arc<Mutex<Box<dyn Any + Send>>>),

    #[error("db is closed")]
    Closed,

    #[error("merge operator error")]
    MergeOperatorError(#[from] MergeOperatorError),

    #[error("checkpoint missing. checkpoint_id=`{0}`")]
    CheckpointMissing(Uuid),

    #[error(
        "byte format version mismatch. expected_version=`{expected_version}`, actual_version=`{actual_version}`"
    )]
    InvalidVersion {
        expected_version: u16,
        actual_version: u16,
    },

    #[error("foyer cache reading error")]
    #[cfg(feature = "foyer")]
    FoyerCacheReadingError(#[from] Arc<anyhow::Error>),

    #[error("operation timed out. operation=`{operation}`")]
    Timeout { operation: &'static str },

    #[error("wal buffer already started")]
    WalBufferAlreadyStarted,

    #[error("cannot seek to a key outside the iterator range. key=`{key:?}`, range=`{range:?}`")]
    SeekKeyOutOfRange { key: Vec<u8>, range: BytesRange },

    #[error("cannot seek to a key less than the last returned key")]
    SeekKeyLessThanLastReturnedKey,

    #[error(
        "parent path must be different from the clone's path. parent_path=`{0}`, clone_path=`{0}`"
    )]
    IdenticalClonePaths(Path),

    #[error("invalid checkpoint lifetime. lifetime=`{0:?}`")]
    InvalidCheckpointLifetime(Duration),

    #[error("invalid manifest poll interval. interval=`{0:?}`")]
    InvalidManifestPollInterval(Duration),

    #[error("checkpoint lifetime must be at least double the manifest poll interval. lifetime=`{lifetime:?}`, interval=`{interval:?}`")]
    CheckpointLifetimeTooShort {
        lifetime: Duration,
        interval: Duration,
    },

    #[error("invalid sst batch size. size=`{0}`")]
    InvalidSSTBatchSize(usize),

    #[error("cannot seek to a key outside the iterator range. key=`{key:?}`, start_key=`{start_key:?}`, end_key=`{end_key:?}`")]
    SeekKeyOutOfKeyRange {
        key: Vec<u8>,
        start_key: Bound<Vec<u8>>,
        end_key: Bound<Vec<u8>>,
    },

    #[error("the cloned database is not attached to any external database")]
    CloneExternalDbMissing,

    #[error("the cloned database is not attached to external database with a valid checkpoint. path=`{path}`, checkpoint_id=`{checkpoint_id:?}`")]
    CloneIncorrectExternalDbCheckpoint {
        path: String,
        checkpoint_id: Option<Uuid>,
    },

    #[error("the final checkpoint for the cloned database no longer exists in the manifest. path=`{path}`, checkpoint_id=`{checkpoint_id}`")]
    CloneIncorrectFinalCheckpoint { path: String, checkpoint_id: Uuid },

    #[error("unknown configuration file format. path=`{0}`")]
    UnknownConfigurationFormat(PathBuf),

    #[error("invalid configuration format")]
    InvalidConfigurationFormat(#[from] Box<figment::Error>),

    #[error("attempted a WAL operation when the WAL is disabled")]
    WalDisabled,

    #[error("invalid object store URL. url=`{0}`")]
    InvalidObjectStoreURL(String, #[source] url::ParseError),

    #[error("transaction conflict")]
    TransactionConflict,
}

impl From<std::io::Error> for SlateDBError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(Arc::new(value))
    }
}

impl From<object_store::Error> for SlateDBError {
    fn from(value: object_store::Error) -> Self {
        Self::ObjectStoreError(Arc::new(value))
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Represents the kind of public errors that can be returned to the user.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    /// The current instance has been fenced. The instance is no longer usable.
    Fenced,

    /// A transaction conflict occurred. The transaction must be retried or dropped.
    Transaction,

    /// The database has been shutdown cleanly. The instance is no longer usable.
    Closed,

    /// An I/O error occurred. The operation must be retried or dropped.
    Io,

    /// The user supplied invalid configuration. The user must correct the configuration
    /// and create a new instance.
    Configuration,

    /// The user supplied an illegal argument in a method call. The user must correct the argument
    /// and retry, or drop the operation.
    Argument,

    /// An internal error occurred.
    Internal,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Fenced => write!(f, "Fencing error"),
            ErrorKind::Transaction => write!(f, "Transaction error"),
            ErrorKind::Closed => write!(f, "Closed error"),
            ErrorKind::Io => write!(f, "I/O error"),
            ErrorKind::Configuration => write!(f, "Configuration error"),
            ErrorKind::Argument => write!(f, "Argument error"),
            ErrorKind::Internal => write!(f, "Internal error"),
        }
    }
}

#[non_exhaustive]
/// Represents a public error that can be returned to the user.
#[derive(Debug)]
pub struct Error {
    msg: String,
    kind: ErrorKind,
    source: Option<BoxError>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.msg)?;
        if let Some(source) = self.source.as_ref() {
            write!(f, " ({source})")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl Error {
    /// Creates a new fencing error.
    pub fn fenced(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Fenced,
            source: None,
        }
    }

    /// Creates a new fencing error.
    pub fn transaction(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Transaction,
            source: None,
        }
    }

    /// Creates a new fencing error.
    pub fn closed(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Closed,
            source: None,
        }
    }

    /// Creates a new I/O error.
    pub fn io(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Io,
            source: None,
        }
    }

    /// Creates a new configuration error.
    pub fn configuration(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Configuration,
            source: None,
        }
    }

    /// Creates a new argument error.
    pub fn argument(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Argument,
            source: None,
        }
    }

    /// Creates a new internal error.
    pub fn internal(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Internal,
            source: None,
        }
    }

    /// Adds a source to the error.
    pub fn with_source(mut self, source: BoxError) -> Self {
        self.source = Some(source);
        self
    }

    /// Returns the error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<SlateDBError> for Error {
    fn from(err: SlateDBError) -> Self {
        let msg = err.to_string();
        match err {
            // Fenced, transaction, and closed errors
            SlateDBError::Fenced => Error::fenced(msg),
            SlateDBError::TransactionConflict => Error::transaction(msg),

            // Closed
            SlateDBError::Closed => Error::closed(msg),
            SlateDBError::ReadChannelError(err) => Error::closed(msg).with_source(Box::new(err)),

            // Io errors
            SlateDBError::IoError(err) => Error::io(msg).with_source(Box::new(err)),
            SlateDBError::ObjectStoreError(err) => Error::io(msg).with_source(Box::new(err)),
            #[cfg(feature = "foyer")]
            SlateDBError::FoyerCacheReadingError(err) => {
                Error::io(msg).with_source(Box::new(AnyhowError(err)))
            }

            // Configuration errors
            SlateDBError::InvalidCachePartSize => Error::configuration(msg),
            SlateDBError::InvalidCompressionCodec => Error::configuration(msg),
            SlateDBError::Unsupported(_) => Error::configuration(msg),
            SlateDBError::InvalidConfigurationFormat(err) => {
                Error::configuration(msg).with_source(Box::new(err))
            }
            SlateDBError::InvalidObjectStoreURL(_, err) => {
                Error::configuration(msg).with_source(Box::new(err))
            }
            SlateDBError::UnknownConfigurationFormat(_) => Error::configuration(msg),
            SlateDBError::InvalidSSTBatchSize(_) => Error::configuration(msg),
            SlateDBError::InvalidCheckpointLifetime(_) => Error::configuration(msg),
            SlateDBError::InvalidManifestPollInterval(_) => Error::configuration(msg),
            SlateDBError::CheckpointLifetimeTooShort { .. } => Error::configuration(msg),

            // Argument errors
            SlateDBError::InvalidatedIterator(err) => {
                Error::argument(msg).with_source(Box::new(err))
            }
            SlateDBError::SeekKeyOutOfRange { .. } => Error::argument(msg),
            SlateDBError::SeekKeyLessThanLastReturnedKey => Error::argument(msg),
            SlateDBError::IdenticalClonePaths { .. } => Error::argument(msg),
            SlateDBError::WalDisabled => Error::argument(msg),

            // Internal errors
            SlateDBError::Timeout { .. } => Error::internal(msg),
            SlateDBError::InvalidFlatbuffer(err) => Error::internal(msg).with_source(Box::new(err)),
            SlateDBError::InvalidDeletion => Error::internal(msg),
            SlateDBError::InvalidDBState => Error::internal(msg),
            SlateDBError::InvalidCompaction => Error::internal(msg),
            SlateDBError::CompactionExecutorFailed => Error::internal(msg),
            SlateDBError::InvalidClockTick { .. } => Error::internal(msg),
            #[cfg(any(
                feature = "snappy",
                feature = "zlib",
                feature = "lz4",
                feature = "zstd"
            ))]
            SlateDBError::BlockDecompressionError => Error::internal(msg),
            #[cfg(any(feature = "snappy", feature = "zlib", feature = "zstd"))]
            SlateDBError::BlockCompressionError => Error::internal(msg),
            SlateDBError::InvalidRowFlags { .. } => Error::internal(msg),
            SlateDBError::BackgroundTaskPanic(err) => {
                Error::internal(msg).with_source(Box::new(PanicError(err)))
            }
            SlateDBError::MergeOperatorError(err) => {
                Error::internal(msg).with_source(Box::new(err))
            }
            SlateDBError::SeekKeyOutOfKeyRange { .. } => Error::argument(msg),
            SlateDBError::CheckpointMissing(_) => Error::internal(msg),
            SlateDBError::InvalidVersion { .. } => Error::internal(msg),
            SlateDBError::WalBufferAlreadyStarted => Error::internal(msg),
            SlateDBError::ManifestMissing(_) => Error::internal(msg),
            SlateDBError::LatestManifestMissing => Error::internal(msg),
            SlateDBError::ManifestVersionExists => Error::internal(msg),
            SlateDBError::EmptyManifest => Error::internal(msg),
            SlateDBError::EmptyBlock => Error::internal(msg),
            SlateDBError::EmptyBlockMeta => Error::internal(msg),
            SlateDBError::EmptySSTable => Error::internal(msg),
            SlateDBError::ChecksumMismatch => Error::internal(msg),
            SlateDBError::CloneExternalDbMissing => Error::internal(msg),
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. } => Error::internal(msg),
            SlateDBError::CloneIncorrectFinalCheckpoint { .. } => Error::internal(msg),
            SlateDBError::FileVersionExists => Error::internal(msg),
        }
    }
}

#[derive(Debug)]
struct PanicError(Arc<Mutex<Box<dyn Any + Send>>>);
impl std::error::Error for PanicError {}
impl std::fmt::Display for PanicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.0.lock().expect("Failed to lock panic error");
        if let Some(err) = guard.downcast_ref::<SlateDBError>() {
            write!(f, "{err}")?;
        } else if let Some(err) = guard.downcast_ref::<Box<dyn std::error::Error>>() {
            write!(f, "{err}")?;
        } else if let Some(err) = guard.downcast_ref::<String>() {
            write!(f, "{err}")?;
        } else {
            write!(f, "irrecoverable panic")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
#[cfg(feature = "foyer")]
struct AnyhowError(Arc<anyhow::Error>);
#[cfg(feature = "foyer")]
impl std::error::Error for AnyhowError {}
#[cfg(feature = "foyer")]
impl std::fmt::Display for AnyhowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
