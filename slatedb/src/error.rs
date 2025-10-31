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

    #[allow(dead_code)]
    #[error("manifest file already exists")]
    ManifestVersionExists,

    #[error("failed to find manifest with id. id=`{0}`")]
    ManifestMissing(u64),

    #[error("failed to find latest record")]
    LatestRecordMissing,

    #[allow(dead_code)]
    #[error("failed to find latest manifest")]
    LatestManifestMissing,

    #[error("invalid deletion")]
    InvalidDeletion,

    #[error("invalid sst error")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("invalid DB state error")]
    InvalidDBState,

    #[error("wal store reconfiguration unsupported")]
    WalStoreReconfigurationError,

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

    #[error("Invalid RowFlags. #{message}. encoded_bits=`{encoded_bits:#b}`, known_bits=`{known_bits:#b}`")]
    InvalidRowFlags {
        encoded_bits: u8,
        known_bits: u8,
        message: String,
    },

    #[error("read channel error")]
    ReadChannelError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("background task panicked. name=`{0}`")]
    // we need to wrap the panic args in an Arc so SlateDbError is Clone
    // we need to wrap the panic args in a mutex so that SlateDbError is Sync
    BackgroundTaskPanic(String, Arc<Mutex<Box<dyn Any + Send>>>),

    #[error("background task exists. name=`{0}`")]
    BackgroundTaskExists(String),

    #[error("background task cancelled. name=`{0}`")]
    BackgroundTaskCancelled(String),

    #[error("background task executor already started")]
    BackgroundTaskExecutorStarted,

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

    #[error("foyer error")]
    #[cfg(feature = "foyer")]
    FoyerError(#[from] Arc<foyer::Error>),

    #[error("manifest update timeout after {timeout:?}")]
    ManifestUpdateTimeout { timeout: Duration },

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

    #[error("iterator not initialized")]
    IteratorNotInitialized,
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

#[cfg(feature = "foyer")]
impl From<foyer::Error> for SlateDBError {
    fn from(value: foyer::Error) -> Self {
        Self::FoyerError(Arc::new(value))
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Represents the reason that a database instance has been closed.
#[derive(Debug, Clone, Copy)]
pub enum CloseReason {
    /// The database has been shutdown cleanly.
    Clean,

    /// The current instance has been fenced and is no longer usable.
    Fenced,

    /// One or more background tasks panicked.
    Panic,
}

/// Represents the kind of public errors that can be returned to the user.
///
/// These are less specific and more prescriptive. Application developers or operators must
/// decide how to proceed. Adding new [ErrorKind]s requires an RFC, and should happen very
/// infrequently.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    /// A transaction conflict occurred. The transaction must be retried or dropped.
    Transaction,

    /// The database has been shutdown. The instance is no longer usable. The user must
    /// create a new instance to continue using the database.
    Closed(CloseReason),

    /// A storage or network service is unavailable. The user must retry or drop the
    /// operation.
    Unavailable,

    /// User attempted an invalid request. This might be:
    ///
    /// - An invalid configuration on initialization
    /// - An invalid argument to a method
    /// - An invalid method call
    /// - A user-supplied plugin such as the compaction schedule supplier or logical clock
    ///   failed.
    ///
    /// The user must correct the code, configuration, or argument and retry the operation.
    Invalid,

    /// Persisted data is in an unexpected state. This could be caused by:
    ///
    /// - Temporary or permanent machine or object storage corruption
    /// - Incompatible file format versions between clients
    /// - An eventual consistency issue in object storage
    ///
    /// The user must fix the data, use a compatible client version, retry the operation,
    /// or drop the operation.
    Data,

    /// An unexpected internal error occurred. Users should not expect to see this error.
    /// Please [open a Github issue](https://github.com/slatedb/slatedb/issues/new?template=bug_report.md&title=Internal+error+returned)
    /// if you receive this error.
    Internal,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Transaction => write!(f, "Transaction error"),
            ErrorKind::Closed(_) => write!(f, "Closed error"),
            ErrorKind::Unavailable => write!(f, "Unavailable error"),
            ErrorKind::Invalid => write!(f, "Invalid error"),
            ErrorKind::Data => write!(f, "Data error"),
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
    pub fn transaction(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Transaction,
            source: None,
        }
    }

    /// Creates a new fencing error.
    pub fn closed(msg: String, reason: CloseReason) -> Self {
        Self {
            msg,
            kind: ErrorKind::Closed(reason),
            source: None,
        }
    }

    /// Creates a new I/O error.
    pub fn unavailable(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Unavailable,
            source: None,
        }
    }

    /// Creates a new configuration error.
    pub fn invalid(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Invalid,
            source: None,
        }
    }

    /// Creates a new data error.
    pub fn data(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Data,
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
            // Transaction errors
            SlateDBError::TransactionConflict => Error::transaction(msg),

            // Closed
            SlateDBError::Closed => Error::closed(msg, CloseReason::Clean),
            SlateDBError::Fenced => Error::closed(msg, CloseReason::Fenced),

            // Unavailable errors
            SlateDBError::IoError(err) => Error::unavailable(msg).with_source(Box::new(err)),
            SlateDBError::ObjectStoreError(err) => {
                Error::unavailable(msg).with_source(Box::new(err))
            }
            #[cfg(feature = "foyer")]
            SlateDBError::FoyerError(err) => Error::unavailable(msg).with_source(Box::new(err)),
            SlateDBError::ManifestUpdateTimeout { .. } => Error::unavailable(msg),

            // Invalid errors
            SlateDBError::InvalidCachePartSize => Error::invalid(msg),
            SlateDBError::InvalidCompressionCodec => Error::invalid(msg),
            SlateDBError::WalStoreReconfigurationError => Error::invalid(msg),
            SlateDBError::InvalidConfigurationFormat(err) => {
                Error::invalid(msg).with_source(Box::new(err))
            }
            SlateDBError::InvalidObjectStoreURL(_, err) => {
                Error::invalid(msg).with_source(Box::new(err))
            }
            SlateDBError::UnknownConfigurationFormat(_) => Error::invalid(msg),
            SlateDBError::InvalidSSTBatchSize(_) => Error::invalid(msg),
            SlateDBError::InvalidCheckpointLifetime(_) => Error::invalid(msg),
            SlateDBError::InvalidManifestPollInterval(_) => Error::invalid(msg),
            SlateDBError::CheckpointLifetimeTooShort { .. } => Error::invalid(msg),
            SlateDBError::SeekKeyOutOfRange { .. } => Error::invalid(msg),
            SlateDBError::SeekKeyLessThanLastReturnedKey => Error::invalid(msg),
            SlateDBError::IdenticalClonePaths { .. } => Error::invalid(msg),
            SlateDBError::WalDisabled => Error::invalid(msg),
            SlateDBError::InvalidCompaction => Error::invalid(msg),
            SlateDBError::InvalidClockTick { .. } => Error::invalid(msg),
            SlateDBError::InvalidDeletion => Error::invalid(msg),
            SlateDBError::MergeOperatorError(err) => Error::invalid(msg).with_source(Box::new(err)),
            SlateDBError::IteratorNotInitialized => Error::invalid(msg),

            // Data errors
            SlateDBError::InvalidFlatbuffer(err) => Error::data(msg).with_source(Box::new(err)),
            SlateDBError::InvalidDBState => Error::data(msg),
            #[cfg(any(
                feature = "snappy",
                feature = "zlib",
                feature = "lz4",
                feature = "zstd"
            ))]
            SlateDBError::BlockDecompressionError => Error::data(msg),
            #[cfg(any(feature = "snappy", feature = "zlib", feature = "zstd"))]
            SlateDBError::BlockCompressionError => Error::data(msg),
            SlateDBError::InvalidRowFlags { .. } => Error::data(msg),
            SlateDBError::CheckpointMissing(_) => Error::data(msg),
            SlateDBError::InvalidVersion { .. } => Error::data(msg),
            SlateDBError::ManifestMissing(_) => Error::data(msg),
            SlateDBError::LatestManifestMissing => Error::data(msg),
            SlateDBError::ManifestVersionExists => Error::data(msg),
            SlateDBError::EmptyManifest => Error::data(msg),
            SlateDBError::EmptyBlock => Error::data(msg),
            SlateDBError::EmptyBlockMeta => Error::data(msg),
            SlateDBError::EmptySSTable => Error::data(msg),
            SlateDBError::ChecksumMismatch => Error::data(msg),
            SlateDBError::CloneExternalDbMissing => Error::data(msg),
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. } => Error::data(msg),
            SlateDBError::CloneIncorrectFinalCheckpoint { .. } => Error::data(msg),
            SlateDBError::FileVersionExists => Error::data(msg),
            SlateDBError::LatestRecordMissing => Error::data(msg),

            // Internal errors
            SlateDBError::CompactionExecutorFailed => Error::internal(msg),
            SlateDBError::BackgroundTaskPanic(_, err) => {
                Error::internal(msg).with_source(Box::new(PanicError(err)))
            }
            SlateDBError::SeekKeyOutOfKeyRange { .. } => Error::internal(msg),
            SlateDBError::ReadChannelError(err) => Error::internal(msg).with_source(Box::new(err)),
            SlateDBError::BackgroundTaskExists(_) => Error::internal(msg),
            SlateDBError::BackgroundTaskCancelled(_) => Error::internal(msg),
            SlateDBError::BackgroundTaskExecutorStarted => Error::internal(msg),
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
