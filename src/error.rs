#[derive(thiserror::Error, Debug)]
pub enum SlateDBError {
    #[error("IO error")]
    IoError(#[from] std::io::Error),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Empty block meta")]
    EmptyBlockMeta,

    #[error("Empty block")]
    EmptyBlock,

    #[error("Object store error")]
    ObjectStoreError(#[from] object_store::Error),
}
