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

    #[error("background task shutdown")]
    BackgroundTaskShutdown,

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

    #[error("operation timed out. operation=`{op}`")]
    Timeout { op: &'static str, backoff: Duration },

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
    TranscationConflict,
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
#[derive(Debug)]
pub enum ErrorKind {
    /// The database attempted to use an invalid configuration.
    Configuration,

    /// The database attempted an invalid operation or an operation with an
    /// invalid parameter (including misconfiguration).
    Operation,

    /// Unexpected internal error. This error is fatal (i.e. the database must be closed).
    System,

    /// Invalid persistent state (e.g. corrupted data files). The state must
    /// be repaired before the database can be restarted.
    PersistentState,

    /// Failed access database resources (e.g. remote storage) due to some
    /// kind of authentication or authorization error. The operation can be
    /// retried after the permission issue is resolved.
    Permission,

    /// The operation failed due to a transient error (such as IO unavailability).
    /// The operation can be retried after backing off.
    Transient(Duration),
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Configuration => write!(f, "Configuration error"),
            ErrorKind::Operation => write!(f, "Operation error"),
            ErrorKind::System => write!(f, "System error"),
            ErrorKind::PersistentState => write!(f, "Persistent state error"),
            ErrorKind::Permission => write!(f, "Permission error"),
            ErrorKind::Transient(backoff) => {
                write!(f, "Transient error (retry after: {backoff:?})")
            }
        }
    }
}

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
    /// Creates a new public configuration error.
    pub fn configuration(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Configuration,
            source: None,
        }
    }

    /// Creates a new public operation error.
    pub fn operation(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Operation,
            source: None,
        }
    }

    /// Creates a new public system error.
    pub fn system(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::System,
            source: None,
        }
    }

    /// Creates a new public persistent state error.
    pub fn persistent_state(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::PersistentState,
            source: None,
        }
    }

    /// Creates a new public permission error.
    pub fn permission(msg: String) -> Self {
        Self {
            msg,
            kind: ErrorKind::Permission,
            source: None,
        }
    }

    /// Creates a new public transient error.
    pub fn transient(msg: String, backoff: Duration) -> Self {
        Self {
            msg,
            kind: ErrorKind::Transient(backoff),
            source: None,
        }
    }

    /// Adds a source to the error.
    pub fn with_source(mut self, source: BoxError) -> Self {
        self.source = Some(source);
        self
    }
}

impl From<SlateDBError> for Error {
    fn from(err: SlateDBError) -> Self {
        let msg = err.to_string();
        match err {
            SlateDBError::IoError(err) => Error::system(msg).with_source(Box::new(err)),
            SlateDBError::ObjectStoreError(err) => Error::operation(msg).with_source(Box::new(err)),
            SlateDBError::InvalidFlatbuffer(err) => {
                Error::persistent_state(msg).with_source(Box::new(err))
            }
            SlateDBError::InvalidDeletion => Error::operation(msg),
            SlateDBError::InvalidDBState => Error::persistent_state(msg),
            SlateDBError::InvalidCompaction => Error::operation(msg),
            SlateDBError::CompactionExecutorFailed => Error::system(msg),
            SlateDBError::InvalidClockTick { .. } => Error::operation(msg),
            SlateDBError::Fenced => Error::permission(msg),
            SlateDBError::InvalidCachePartSize => Error::operation(msg),
            SlateDBError::InvalidCompressionCodec => Error::operation(msg),
            #[cfg(any(
                feature = "snappy",
                feature = "zlib",
                feature = "lz4",
                feature = "zstd"
            ))]
            SlateDBError::BlockDecompressionError => Error::operation(msg),
            #[cfg(any(feature = "snappy", feature = "zlib", feature = "zstd"))]
            SlateDBError::BlockCompressionError => Error::operation(msg),
            SlateDBError::InvalidRowFlags { .. } => Error::operation(msg),
            SlateDBError::ReadChannelError(err) => Error::system(msg).with_source(Box::new(err)),
            SlateDBError::InvalidatedIterator(err) => {
                Error::operation(msg).with_source(Box::new(err))
            }
            SlateDBError::BackgroundTaskPanic(err) => {
                Error::system(msg).with_source(Box::new(PanicError(err)))
            }
            SlateDBError::BackgroundTaskShutdown => Error::system(msg),
            SlateDBError::MergeOperatorError(err) => {
                Error::operation(msg).with_source(Box::new(err))
            }
            SlateDBError::CheckpointMissing(_) => Error::persistent_state(msg),
            SlateDBError::InvalidVersion { .. } => Error::persistent_state(msg),
            #[cfg(feature = "foyer")]
            SlateDBError::FoyerCacheReadingError(err) => {
                Error::persistent_state(msg).with_source(Box::new(AnyhowError(err)))
            }
            SlateDBError::Timeout { backoff, .. } => Error::transient(msg, backoff),
            SlateDBError::WalBufferAlreadyStarted => Error::system(msg),
            SlateDBError::ManifestMissing(_) => Error::persistent_state(msg),
            SlateDBError::LatestManifestMissing => Error::persistent_state(msg),
            SlateDBError::ManifestVersionExists => Error::persistent_state(msg),
            SlateDBError::EmptyManifest => Error::persistent_state(msg),
            SlateDBError::EmptyBlock => Error::persistent_state(msg),
            SlateDBError::EmptyBlockMeta => Error::persistent_state(msg),
            SlateDBError::EmptySSTable => Error::persistent_state(msg),
            SlateDBError::Unsupported(_) => Error::operation(msg),
            SlateDBError::ChecksumMismatch => Error::persistent_state(msg),
            SlateDBError::SeekKeyOutOfRange { .. } => Error::operation(msg),
            SlateDBError::SeekKeyLessThanLastReturnedKey => Error::operation(msg),
            SlateDBError::IdenticalClonePaths { .. } => Error::operation(msg),
            SlateDBError::InvalidCheckpointLifetime(_) => Error::operation(msg),
            SlateDBError::InvalidManifestPollInterval(_) => Error::operation(msg),
            SlateDBError::CheckpointLifetimeTooShort { .. } => Error::operation(msg),
            SlateDBError::InvalidSSTBatchSize(_) => Error::operation(msg),
            SlateDBError::SeekKeyOutOfKeyRange { .. } => Error::operation(msg),
            SlateDBError::CloneExternalDbMissing => Error::persistent_state(msg),
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. } => Error::persistent_state(msg),
            SlateDBError::CloneIncorrectFinalCheckpoint { .. } => Error::persistent_state(msg),
            SlateDBError::UnknownConfigurationFormat(_) => Error::configuration(msg),
            SlateDBError::InvalidConfigurationFormat(err) => {
                Error::configuration(msg).with_source(Box::new(err))
            }
            SlateDBError::WalDisabled => Error::operation(msg),
            SlateDBError::InvalidObjectStoreURL(_, err) => {
                Error::configuration(msg).with_source(Box::new(err))
            }
            SlateDBError::TranscationConflict => Error::operation(msg),
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
