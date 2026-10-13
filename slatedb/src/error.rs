use bytes::Bytes;
use object_store::path::Path;
use std::ops::Bound;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error as ThisError;
use uuid::Uuid;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError::{
    LatestTransactionalObjectVersionMissing, TransactionalObjectVersionExists,
};
use crate::merge_operator::MergeOperatorError;
use slatedb_mirror::error::MIRROR_STORE_NAME;
use slatedb_mirror::MirrorError;
use slatedb_txn_obj::TransactionalObjectError;

#[non_exhaustive]
#[derive(Clone, Debug, ThisError)]
pub(crate) enum SlateDBError {
    #[error("io error")]
    IoError(#[source] Arc<std::io::Error>),

    #[error("checksum mismatch{}", .path.as_ref().map(|p| format!(" in {p}")).unwrap_or_default())]
    ChecksumMismatch { path: Option<Path> },

    #[error("empty SSTable")]
    EmptySSTable,

    #[error("empty block metadata")]
    EmptyBlockMeta,

    #[error("invalid composite filter block")]
    InvalidFilterBlock,

    #[error("empty block")]
    EmptyBlock,

    #[error("empty RowEntry key")]
    EmptyKey,

    #[error("empty write batch not allowed")]
    EmptyBatch,

    #[error("empty manifest")]
    EmptyManifest,

    #[error("object store error")]
    ObjectStoreError(#[source] Arc<object_store::Error>),

    /// An error raised by an `ObjectStoreMirror`. `From` picks this over
    /// `IoError` and `ObjectStoreError` when the source chain holds a
    /// `MirrorError`.
    #[error("object store mirror error")]
    MirrorError(#[source] Arc<object_store::Error>),

    #[error("failed to find manifest with id. id=`{0}`")]
    ManifestMissing(u64),

    #[error("transactional object (e.g. manifest) version already exists")]
    TransactionalObjectVersionExists,

    #[error("failed to find latest transactional object (e.g. manifest) version")]
    LatestTransactionalObjectVersionMissing,

    #[error("generic transactional object (e.g. manifest) error {0:?}")]
    TransactionalObjectError(#[from] Arc<TransactionalObjectError>),

    #[error("transactional object (e.g. manifest) op timeout after {timeout:?}")]
    TransactionalObjectTimeout { timeout: Duration },

    #[error("transactional object (e.g. manifest) is in an invalid state")]
    InvalidTransactionalObjectState,

    #[error("invalid deletion")]
    #[allow(unused)]
    InvalidDeletion,

    #[error("invalid sst error")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("invalid DB state error")]
    InvalidDBState,

    #[error("wal store reconfiguration unsupported")]
    WalStoreReconfigurationError,

    #[error("wal truncated at wal file `{0}`")]
    WalTruncated(u64),

    #[error("wal unavailable")]
    WalUnavailable(Arc<dyn std::error::Error + Sync + Send + 'static>),

    #[error("wal internal error")]
    WalInternalError(Arc<dyn std::error::Error + Sync + Send + 'static>),

    #[error("wal data error")]
    WalDataError(Arc<dyn std::error::Error + Sync + Send + 'static>),

    #[error("invalid compaction")]
    InvalidCompaction,

    #[error("segment prefix {prefix:?} would nest with existing segment {conflict:?}")]
    InvalidSegmentPrefix { prefix: Bytes, conflict: Bytes },

    #[error("recency scan prefix spans multiple segments, which is unsupported")]
    RecencyScanPrefixSpansMultipleSegments,

    #[error(
        "segment extractor configuration mismatch (persisted: {persisted:?}, \
         configured: {configured:?})"
    )]
    SegmentExtractorMismatch {
        persisted: Option<String>,
        configured: Option<String>,
    },

    #[error(
        "segment prefix {prefix:?} is not recognized by the configured extractor `{extractor}`"
    )]
    SegmentPrefixNotRecognized { prefix: Bytes, extractor: String },

    #[error("segment extractor produced an empty prefix for key {key:?}")]
    EmptySegmentPrefix { key: Bytes },

    #[error("compaction executor failed")]
    CompactorExecutorFailed,

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

    #[error("error transforming block")]
    BlockTransformError,

    #[error("Invalid RowFlags. #{message}. encoded_bits=`{encoded_bits:#b}`, known_bits=`{known_bits:#b}`")]
    InvalidRowFlags {
        encoded_bits: u8,
        known_bits: u8,
        message: String,
    },

    #[error("read channel error")]
    ReadChannelError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("background task panicked. name=`{0}`")]
    BackgroundTaskPanic(String),

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

    #[error("merge operator missing. A merge operator is required to read merge operands")]
    MergeOperatorMissing,

    #[error(
        "only one merge TTL per-key allowed in a batch. key={key:?}, previous=`{previous_expire_ts:?}`, current=`{current_expire_ts:?}`"
    )]
    IncompatibleMergeTtls {
        key: Bytes,
        previous_expire_ts: Option<i64>,
        current_expire_ts: Option<i64>,
    },

    #[error("checkpoint missing. checkpoint_id=`{0}`")]
    CheckpointMissing(Uuid),

    #[error(
        "unsupported {format_name} format version. supported_versions=`{supported_versions:?}`, actual_version=`{actual_version}`"
    )]
    InvalidVersion {
        format_name: &'static str,
        supported_versions: Vec<u16>,
        actual_version: u16,
    },

    #[error("foyer error")]
    #[cfg(feature = "foyer")]
    FoyerError(#[from] Arc<foyer::Error>),

    #[error("cannot seek to a key outside the iterator range. key=`{key:?}`, range=`{range:?}`")]
    SeekKeyOutOfRange { key: Vec<u8>, range: BytesRange },

    #[error("cannot seek to a key less than the last returned key")]
    SeekKeyLessThanLastReturnedKey,

    #[error("seek is not supported for descending scans")]
    SeekNotSupportedForDescendingScan,

    #[error(
        "parent path must be different from the clone's path. parent_path=`{0}`, clone_path=`{0}`"
    )]
    IdenticalClonePaths(Path),

    #[error("clone source paths must be unique, found duplicate: `{0}`")]
    DuplicatedCloneSourcePath(Path),

    #[error("Projection and/or union with WAL is not supported, sources with WAL: `{paths:?}`")]
    InvalidCloneSourceWithWal { paths: Vec<Path> },

    #[error("Source manifest set must not be empty")]
    InvalidUnionSetEmpty(),

    #[error("invalid union: {0}")]
    InvalidUnion(String),

    #[error("invalid clone projection for segment prefix {prefix:?}: {reason}")]
    InvalidProjection { prefix: Bytes, reason: String },

    #[error("invalid checkpoint lifetime. lifetime=`{0:?}`")]
    InvalidCheckpointLifetime(Duration),

    #[error("invalid manifest poll interval. interval=`{0:?}`")]
    InvalidManifestPollInterval(Duration),

    #[error("checkpoint lifetime must be at least double the manifest poll interval. lifetime=`{lifetime:?}`, interval=`{interval:?}`")]
    CheckpointLifetimeTooShort {
        lifetime: Duration,
        interval: Duration,
    },

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

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

    #[error("invalid object store path. provide path to builder instead. path=`{0}`")]
    InvalidObjectStorePath(String),

    #[error("transaction conflict")]
    TransactionConflict,

    #[error("iterator not initialized")]
    IteratorNotInitialized,

    #[cfg(feature = "compaction_filters")]
    #[error("compaction filter error: {0}")]
    CompactionFilterError(Arc<crate::compaction_filter::CompactionFilterError>),

    #[error("invalid sequence number ordering during merge. expected sequence numbers in descending order, but found {current_seq} followed by {next_seq}")]
    InvalidSequenceOrder { current_seq: u64, next_seq: u64 },

    #[error(
        "invalid environment variable {key} value `{}`",
        .value.as_deref().unwrap_or("null")
    )]
    InvalidEnvironmentVariable { key: String, value: Option<String> },

    #[error("unexpected tombstone encountered where a value was expected")]
    UnexpectedTombstone,

    #[error(
        "invalid sequence number, must be greater than the current max. provided=`{provided}`, current=`{current}`"
    )]
    InvalidSequenceNumber { provided: u64, current: u64 },
}

impl SlateDBError {
    pub(crate) fn with_path(self, path: &Path) -> Self {
        match self {
            SlateDBError::ChecksumMismatch { path: None } => SlateDBError::ChecksumMismatch {
                path: Some(path.clone()),
            },
            other => other,
        }
    }

    /// Returns true if this error or any of its sources is an object-store NotFound.
    pub(crate) fn has_object_store_not_found(&self) -> bool {
        fn is_object_store_not_found(err: &(dyn std::error::Error + 'static)) -> bool {
            err.downcast_ref::<object_store::Error>()
                .is_some_and(|err| matches!(err, object_store::Error::NotFound { .. }))
                || err
                    .downcast_ref::<Arc<object_store::Error>>()
                    .is_some_and(|err| matches!(err.as_ref(), object_store::Error::NotFound { .. }))
        }

        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(self);
        while let Some(err) = current {
            if is_object_store_not_found(err) {
                return true;
            }
            current = err.source();
        }
        false
    }

    /// Returns true if this error means a sequenced write should refresh and retry.
    pub(crate) fn is_sequenced_write_conflict(&self) -> bool {
        matches!(self, Self::TransactionalObjectVersionExists)
    }

    /// Classifies this error as a recoverable SST validation failure to reissue
    /// the read with a [`RetryReason`], or `None` if it is not recoverable.
    ///
    /// This doesn't include transient errors like I/O or object store errors.
    /// It includes errors that indicate the SST is corrupt or invalid, and the
    /// read should be retried with a different strategy.
    pub(crate) fn maybe_validation_retry_reason(&self) -> Option<RetryReason> {
        match self {
            SlateDBError::ChecksumMismatch { .. } => Some(RetryReason::CrcMismatch),
            #[cfg(any(
                feature = "snappy",
                feature = "zlib",
                feature = "lz4",
                feature = "zstd"
            ))]
            SlateDBError::BlockDecompressionError => Some(RetryReason::DecompressionError),
            SlateDBError::InvalidFlatbuffer(_)
            | SlateDBError::EmptyBlock
            | SlateDBError::EmptyBlockMeta
            | SlateDBError::InvalidFilterBlock
            | SlateDBError::BlockTransformError => Some(RetryReason::BlockDecodeError),
            _ => None,
        }
    }
}

impl From<TransactionalObjectError> for SlateDBError {
    fn from(error: TransactionalObjectError) -> Self {
        match error {
            TransactionalObjectError::IoError(e) => SlateDBError::from(e),
            TransactionalObjectError::ObjectStoreError(e) => SlateDBError::from(e),
            TransactionalObjectError::LatestRecordMissing => {
                LatestTransactionalObjectVersionMissing
            }
            TransactionalObjectError::ObjectVersionExists => TransactionalObjectVersionExists,
            TransactionalObjectError::Fenced => SlateDBError::Fenced,
            TransactionalObjectError::CallbackError(err) => match err.downcast::<SlateDBError>() {
                Err(err) => SlateDBError::TransactionalObjectError(Arc::new(
                    TransactionalObjectError::CallbackError(err),
                )),
                Ok(err) => *err,
            },
            TransactionalObjectError::ObjectUpdateTimeout { timeout } => {
                SlateDBError::TransactionalObjectTimeout { timeout }
            }
            // returned when the persisted state is invalid (e.g. malformed name, missing file)
            TransactionalObjectError::InvalidObjectState => {
                SlateDBError::InvalidTransactionalObjectState
            }
            other => SlateDBError::TransactionalObjectError(Arc::new(other)),
        }
    }
}

impl From<std::io::Error> for SlateDBError {
    fn from(value: std::io::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<std::io::Error>> for SlateDBError {
    fn from(value: Arc<std::io::Error>) -> Self {
        if MirrorError::find(value.as_ref()).is_none() {
            return Self::IoError(value);
        }
        // `BufWriter` wraps object-store errors from multipart uploads in
        // `io::Error`. Take the original error out when we can. Otherwise keep
        // the `io::Error` as the source so the mirror error is still found.
        let source: Box<dyn std::error::Error + Send + Sync> = match Arc::try_unwrap(value) {
            Ok(err)
                if err
                    .get_ref()
                    .is_some_and(|inner| inner.is::<object_store::Error>()) =>
            {
                let inner = err
                    .into_inner()
                    .and_then(|inner| inner.downcast::<object_store::Error>().ok())
                    .expect("checked that io::Error wraps an object_store::Error");
                return Self::MirrorError(Arc::new(*inner));
            }
            Ok(err) => Box::new(err),
            Err(value) => Box::new(value),
        };
        Self::MirrorError(Arc::new(object_store::Error::Generic {
            store: MIRROR_STORE_NAME,
            source,
        }))
    }
}

impl From<object_store::Error> for SlateDBError {
    fn from(value: object_store::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<object_store::Error>> for SlateDBError {
    fn from(value: Arc<object_store::Error>) -> Self {
        if MirrorError::find(value.as_ref()).is_some() {
            Self::MirrorError(value)
        } else {
            Self::ObjectStoreError(value)
        }
    }
}

#[cfg(feature = "foyer")]
impl From<foyer::Error> for SlateDBError {
    fn from(value: foyer::Error) -> Self {
        Self::FoyerError(Arc::new(value))
    }
}

#[cfg(feature = "compaction_filters")]
impl From<crate::compaction_filter::CompactionFilterError> for SlateDBError {
    fn from(value: crate::compaction_filter::CompactionFilterError) -> Self {
        Self::CompactionFilterError(Arc::new(value))
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Represents the reason that a database instance has been closed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl From<ErrorKind> for CloseReason {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Closed(reason) => reason,
            _ => CloseReason::Panic,
        }
    }
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

/// Why a recoverable SST read is being reissued (the reason it failed validation
/// the first time).
///
/// Carried on the reissued read's
/// [`ObjectStoreCallTag`](crate::object_store_tag::ObjectStoreCallTag) so a
/// caching wrapper can drop its local copy and refetch instead of serving the
/// same bytes again.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryReason {
    /// The read bytes failed a checksum validation.
    CrcMismatch,
    /// The read bytes could not be decoded as a block.
    BlockDecodeError,
    /// The read bytes could not be decompressed.
    #[cfg(any(
        feature = "snappy",
        feature = "zlib",
        feature = "lz4",
        feature = "zstd"
    ))]
    DecompressionError,
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
            SlateDBError::BackgroundTaskPanic(_) => Error::closed(msg, CloseReason::Panic),

            // Unavailable errors
            SlateDBError::IoError(err) => Error::unavailable(msg).with_source(Box::new(err)),
            SlateDBError::ObjectStoreError(err) => {
                let error = if matches!(err.as_ref(), object_store::Error::NotFound { .. }) {
                    Error::data(msg)
                } else {
                    Error::unavailable(msg)
                };
                error.with_source(Box::new(err))
            }
            SlateDBError::MirrorError(err) => {
                let error = match MirrorError::find(err.as_ref()) {
                    // The policy routed the read to the local copy and it
                    // wasn't there. Like a remote `NotFound`, retrying the
                    // read won't help.
                    Some(MirrorError::NotLocal { .. }) => Error::data(msg),
                    // The object is durable remotely but the mirror's
                    // follow-up work didn't finish. `Db` and the compactor
                    // close with this error, and reopening runs `observe`
                    // again (RFC 0034).
                    Some(MirrorError::WriteCommitted { .. }) => Error::data(msg),
                    Some(
                        MirrorError::Policy { .. }
                        | MirrorError::Unsupported { .. }
                        | MirrorError::InvalidConfig { .. },
                    ) => Error::invalid(msg),
                    _ => Error::unavailable(msg),
                };
                error.with_source(Box::new(err))
            }
            #[cfg(feature = "foyer")]
            SlateDBError::FoyerError(err) => Error::unavailable(msg).with_source(Box::new(err)),
            SlateDBError::TransactionalObjectTimeout { .. } => Error::unavailable(msg),
            SlateDBError::WalUnavailable(src) => Error::unavailable(msg).with_source(Box::new(src)),

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
            SlateDBError::InvalidObjectStorePath(_) => Error::invalid(msg),
            SlateDBError::UnknownConfigurationFormat(_) => Error::invalid(msg),
            SlateDBError::InvalidConfiguration(_) => Error::invalid(msg),
            SlateDBError::InvalidCheckpointLifetime(_) => Error::invalid(msg),
            SlateDBError::InvalidManifestPollInterval(_) => Error::invalid(msg),
            SlateDBError::CheckpointLifetimeTooShort { .. } => Error::invalid(msg),
            SlateDBError::SeekKeyOutOfRange { .. } => Error::invalid(msg),
            SlateDBError::SeekKeyLessThanLastReturnedKey => Error::invalid(msg),
            SlateDBError::SeekNotSupportedForDescendingScan => Error::invalid(msg),
            SlateDBError::IdenticalClonePaths { .. } => Error::invalid(msg),
            SlateDBError::DuplicatedCloneSourcePath(_) => Error::invalid(msg),
            SlateDBError::InvalidCloneSourceWithWal { .. } => Error::invalid(msg),
            SlateDBError::InvalidUnionSetEmpty() => Error::invalid(msg),
            SlateDBError::InvalidUnion(_) => Error::invalid(msg),
            SlateDBError::InvalidProjection { .. } => Error::invalid(msg),
            SlateDBError::WalDisabled => Error::invalid(msg),
            SlateDBError::InvalidCompaction => Error::invalid(msg),
            SlateDBError::InvalidSegmentPrefix { .. } => Error::invalid(msg),
            SlateDBError::RecencyScanPrefixSpansMultipleSegments => Error::invalid(msg),
            SlateDBError::SegmentExtractorMismatch { .. } => Error::invalid(msg),
            SlateDBError::SegmentPrefixNotRecognized { .. } => Error::invalid(msg),
            SlateDBError::EmptySegmentPrefix { .. } => Error::invalid(msg),
            SlateDBError::InvalidClockTick { .. } => Error::invalid(msg),
            SlateDBError::InvalidDeletion => Error::invalid(msg),
            SlateDBError::MergeOperatorError(err) => Error::invalid(msg).with_source(Box::new(err)),
            SlateDBError::MergeOperatorMissing => Error::invalid(msg),
            SlateDBError::IncompatibleMergeTtls { .. } => Error::invalid(msg),
            SlateDBError::IteratorNotInitialized => Error::invalid(msg),
            SlateDBError::InvalidSequenceOrder { .. } => Error::invalid(msg),
            SlateDBError::InvalidEnvironmentVariable { .. } => Error::invalid(msg),
            SlateDBError::InvalidSequenceNumber { .. } => Error::invalid(msg),
            SlateDBError::EmptyBatch => Error::invalid(msg),

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
            SlateDBError::BlockTransformError => Error::data(msg),
            SlateDBError::InvalidRowFlags { .. } => Error::data(msg),
            SlateDBError::CheckpointMissing(_) => Error::data(msg),
            SlateDBError::InvalidVersion { .. } => Error::data(msg),
            SlateDBError::ManifestMissing(_) => Error::data(msg),
            LatestTransactionalObjectVersionMissing => Error::data(msg),
            TransactionalObjectVersionExists => Error::data(msg),
            SlateDBError::InvalidTransactionalObjectState => Error::data(msg),
            SlateDBError::EmptyManifest => Error::data(msg),
            SlateDBError::EmptyBlock => Error::data(msg),
            SlateDBError::EmptyKey => Error::data(msg),
            SlateDBError::EmptyBlockMeta => Error::data(msg),
            SlateDBError::InvalidFilterBlock => Error::data(msg),
            SlateDBError::EmptySSTable => Error::data(msg),
            SlateDBError::ChecksumMismatch { .. } => Error::data(msg),
            SlateDBError::CloneExternalDbMissing => Error::data(msg),
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. } => Error::data(msg),
            SlateDBError::CloneIncorrectFinalCheckpoint { .. } => Error::data(msg),
            SlateDBError::WalTruncated(_) => Error::data(msg),
            SlateDBError::WalDataError(src) => Error::data(msg).with_source(Box::new(src)),

            // Internal errors
            SlateDBError::CompactorExecutorFailed => Error::internal(msg),
            #[cfg(feature = "compaction_filters")]
            SlateDBError::CompactionFilterError(_) => Error::internal(msg),
            SlateDBError::SeekKeyOutOfKeyRange { .. } => Error::internal(msg),
            SlateDBError::ReadChannelError(err) => Error::internal(msg).with_source(Box::new(err)),
            SlateDBError::BackgroundTaskExists(_) => Error::internal(msg),
            SlateDBError::BackgroundTaskCancelled(_) => Error::internal(msg),
            SlateDBError::BackgroundTaskExecutorStarted => Error::internal(msg),
            SlateDBError::UnexpectedTombstone => Error::internal(msg),
            SlateDBError::TransactionalObjectError(err) => {
                Error::internal(msg).with_source(Box::new(err))
            }
            SlateDBError::WalInternalError(src) => Error::internal(msg).with_source(Box::new(src)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_error_not_found_maps_to_data() {
        let err = SlateDBError::from(object_store::Error::NotFound {
            path: "test/path".to_string(),
            source: Box::new(std::io::Error::other("not found")),
        });
        let public_err = Error::from(err);

        assert_eq!(public_err.kind(), ErrorKind::Data);
    }

    #[test]
    fn has_object_store_not_found_detects_wrapped_source() {
        let err = SlateDBError::from(object_store::Error::NotFound {
            path: "test/path".to_string(),
            source: Box::new(std::io::Error::other("not found")),
        });

        assert!(err.has_object_store_not_found());
    }

    #[test]
    fn object_store_error_non_not_found_maps_to_unavailable() {
        let err = SlateDBError::from(object_store::Error::NotImplemented {
            operation: "test".to_string(),
            implementer: "test".to_string(),
        });
        let public_err = Error::from(err);

        assert_eq!(public_err.kind(), ErrorKind::Unavailable);
    }

    fn mirror_error_kind(err: MirrorError) -> ErrorKind {
        let err = SlateDBError::from(object_store::Error::from(err));
        assert!(matches!(err, SlateDBError::MirrorError(_)));
        Error::from(err).kind()
    }

    #[test]
    fn mirror_errors_map_to_public_kinds() {
        let path = Path::from("compacted/01K.sst");
        assert_eq!(
            mirror_error_kind(MirrorError::NotLocal { path: path.clone() }),
            ErrorKind::Data
        );
        assert_eq!(
            mirror_error_kind(MirrorError::WriteCommitted {
                path,
                source: Box::new(std::io::Error::other("disk full")),
            }),
            ErrorKind::Data
        );
        assert_eq!(
            mirror_error_kind(MirrorError::Local {
                source: std::io::Error::other("disk full"),
            }),
            ErrorKind::Unavailable
        );
        assert_eq!(
            mirror_error_kind(MirrorError::Policy {
                source: Box::new(std::io::Error::other("wrong root")),
            }),
            ErrorKind::Invalid
        );
        assert_eq!(
            mirror_error_kind(MirrorError::Unsupported { operation: "copy" }),
            ErrorKind::Invalid
        );
        assert_eq!(
            mirror_error_kind(MirrorError::InvalidConfig {
                message: "download_concurrency must be greater than zero".to_string(),
            }),
            ErrorKind::Invalid
        );
    }

    #[test]
    fn mirror_write_committed_through_io_error_maps_to_data() {
        // `BufWriter` wraps object-store errors from multipart uploads in
        // `io::Error`.
        let err = SlateDBError::from(std::io::Error::other(object_store::Error::from(
            MirrorError::WriteCommitted {
                path: Path::from("compacted/01K.sst"),
                source: Box::new(std::io::Error::other("rename failed")),
            },
        )));

        let SlateDBError::MirrorError(inner) = &err else {
            panic!("expected SlateDBError::MirrorError, got {err:?}");
        };
        assert!(matches!(
            MirrorError::find(inner.as_ref()),
            Some(MirrorError::WriteCommitted { .. })
        ));
        assert_eq!(Error::from(err).kind(), ErrorKind::Data);
    }

    #[test]
    fn mirror_error_behind_shared_io_error_maps_to_mirror_variant() {
        // A shared `Arc` can't be unwrapped, so the `io::Error` stays in the
        // source chain.
        let io_err = Arc::new(std::io::Error::other(object_store::Error::from(
            MirrorError::NotLocal {
                path: Path::from("compacted/01K.sst"),
            },
        )));
        let err = SlateDBError::from(Arc::clone(&io_err));

        assert!(matches!(err, SlateDBError::MirrorError(_)));
        assert_eq!(Error::from(err).kind(), ErrorKind::Data);
    }

    #[test]
    fn non_mirror_errors_keep_their_variants() {
        let err = SlateDBError::from(std::io::Error::other("disk full"));
        assert!(matches!(err, SlateDBError::IoError(_)));

        let err = SlateDBError::from(object_store::Error::NotFound {
            path: "a".to_string(),
            source: Box::new(std::io::Error::other("missing")),
        });
        assert!(matches!(err, SlateDBError::ObjectStoreError(_)));
    }
}
