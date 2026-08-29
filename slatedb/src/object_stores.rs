/// Whether the object store holds the main data path or the WAL.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ObjectStoreType {
    /// The primary object store (SSTs, manifests, compacted data).
    Main,
    /// The dedicated WAL object store, when configured separately.
    Wal,
}

impl ObjectStoreType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Wal => "wal",
        }
    }
}
