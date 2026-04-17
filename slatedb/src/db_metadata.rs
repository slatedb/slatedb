use crate::db_status::DbStatus;
use crate::manifest::VersionedManifest;

/// Trait for database metadata operations.
///
/// This trait provides access to database status and manifest information,
/// implemented by [`Db`](crate::Db) and [`DbReader`](crate::DbReader) to
/// provide a unified interface for metadata access.
///
/// The trait is object-safe, allowing for dynamic dispatch when needed.
pub trait DbMetadataOps {
    /// Get the current manifest state.
    ///
    /// Returns the in-memory manifest snapshot currently held by this handle,
    /// paired with its manifest version ID.
    fn manifest(&self) -> VersionedManifest;

    /// Subscribe to database status changes.
    ///
    /// Returns a [`tokio::sync::watch::Receiver<DbStatus>`] that always
    /// reflects the latest database status.
    fn subscribe(&self) -> tokio::sync::watch::Receiver<DbStatus>;

    /// Returns the latest database status.
    ///
    /// This is a snapshot of the current state and will not update automatically.
    /// Use [`subscribe`](DbMetadataOps::subscribe) to receive real-time updates.
    fn status(&self) -> DbStatus;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time check: the trait is object-safe.
    fn _assert_object_safe(_: &dyn DbMetadataOps) {}
}
