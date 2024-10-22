use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum SlateDBError {
    #[error("IO error")]
    IoError(#[from] std::io::Error),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Empty SSTable")]
    EmptySSTable,

    #[error("Empty block metadata")]
    EmptyBlockMeta,

    #[error("Empty block")]
    EmptyBlock,

    #[error("Object store error")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("Manifest file already exists")]
    ManifestVersionExists,

    #[error("Manifest missing")]
    ManifestMissing,

    #[error("Invalid deletion")]
    InvalidDeletion,

    #[error("Invalid sst error")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("Invalid DB state error")]
    InvalidDBState,

    #[error("Invalid Compaction")]
    InvalidCompaction,

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

    #[error("Unknown RowFlags -- this may be caused by reading data encoded with a newer codec")]
    InvalidRowFlags,
}

/// Represents errors that can occur during the database configuration.
///
/// This enum encapsulates various error conditions that may arise
/// when parsing or processing database configuration options.
#[derive(thiserror::Error, Debug)]
pub enum DbOptionsError {
    #[error("Unknown configuration file format: {0}")]
    UnknownFormat(PathBuf),

    #[error("Invalid configuration format")]
    InvalidFormat(#[from] figment::Error),
}
