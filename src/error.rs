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

    #[error("Invalid sst error")]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),
}
