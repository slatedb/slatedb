use std::any::Any;
use std::sync::Mutex;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;

use crate::merge_operator::MergeOperatorError;

#[derive(Clone, Debug, Error)]
pub enum SlateDBError {
    #[error("IO error: {0}")]
    IoError(#[from] Arc<std::io::Error>),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Empty SSTable")]
    EmptySSTable,

    #[error("Empty block metadata")]
    EmptyBlockMeta,

    #[error("Empty block")]
    EmptyBlock,

    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] Arc<object_store::Error>),

    #[error("Manifest file already exists")]
    ManifestVersionExists,

    #[error("Failed to find manifest with id {0}")]
    ManifestMissing(u64),

    #[error("Failed to find latest manifest")]
    LatestManifestMissing,

    #[error("Invalid deletion")]
    InvalidDeletion,

    #[error("Invalid sst error: {0}")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("Invalid DB state error")]
    InvalidDBState,

    #[error("Invalid Compaction")]
    InvalidCompaction,

    #[error(
        "Invalid clock tick, most be monotonic. Last tick: {}, Next tick: {}",
        last_tick,
        next_tick
    )]
    InvalidClockTick { last_tick: i64, next_tick: i64 },

    #[error("Detected newer DB client")]
    Fenced,

    #[error("Invalid cache part size bytes, it must be multiple of 1024 and greater than 0")]
    InvalidCachePartSize,

    #[error("Invalid Compression Codec")]
    InvalidCompressionCodec,

    #[error("Error Decompressing Block")]
    BlockDecompressionError,

    #[error("Error Compressing Block")]
    BlockCompressionError,

    #[error("Invalid RowFlags (encoded_bits: {encoded_bits:#b}, known_bits: {known_bits:#b}): {message}")]
    InvalidRowFlags {
        encoded_bits: u8,
        known_bits: u8,
        message: String,
    },

    #[error("Error flushing immutable wals: channel closed")]
    WalFlushChannelError,

    #[error("Error flushing memtables: channel closed")]
    MemtableFlushChannelError,

    #[error("Read channel error: {0}")]
    ReadChannelError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Iterator invalidated after unexpected error {0}")]
    InvalidatedIterator(#[from] Box<SlateDBError>),

    #[error("Invalid Argument")]
    InvalidArgument { msg: String },

    #[error("background task panic'd")]
    // we need to wrap the panic args in an Arc so SlateDbError is Clone
    // we need to wrap the panic args in a mutex so that SlateDbError is Sync
    BackgroundTaskPanic(Arc<Mutex<Box<dyn Any + Send>>>),

    #[error("background task shutdown")]
    BackgroundTaskShutdown,

    #[error("Merge Operator error: {0}")]
    MergeOperatorError(#[from] MergeOperatorError),
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

/// Represents errors that can occur during the database configuration.
///
/// This enum encapsulates various error conditions that may arise
/// when parsing or processing database configuration options.
#[derive(Error, Debug)]
pub enum DbOptionsError {
    #[error("Unknown configuration file format: {0}")]
    UnknownFormat(PathBuf),

    #[error("Invalid configuration format: {0}")]
    InvalidFormat(#[from] figment::Error),
}
