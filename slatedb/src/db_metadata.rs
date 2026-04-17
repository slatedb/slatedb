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

    /// Subscribe to database state changes.
    ///
    /// Returns a [`tokio::sync::watch::Receiver<DbStatus>`] that always
    /// reflects the latest database status. The status includes the latest
    /// durable sequence number and the current in-memory manifest snapshot
    /// observed by this handle. For example, you can wait for a specific
    /// sequence number to become durable:
    ///
    /// ```ignore
    /// let seq = 42; // sequence number from a write operation
    /// let mut rx = db.subscribe();
    /// rx.wait_for(|s| s.durable_seq >= seq).await.expect("db dropped");
    /// ```
    ///
    /// # Deadlock risk
    ///
    /// The returned receiver holds a read lock on the current value while
    /// borrowed (via [`borrow`](tokio::sync::watch::Receiver::borrow),
    /// [`borrow_and_update`](tokio::sync::watch::Receiver::borrow_and_update),
    /// or the guard returned by [`wait_for`](tokio::sync::watch::Receiver::wait_for)).
    /// The database must acquire a write lock to publish new status updates.
    /// Holding the read guard for an extended period will block all database
    /// status updates and may cause a deadlock. See the [deadlock warning in
    /// `Receiver::borrow`](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html#method.borrow)
    /// for details. Always clone or copy the data you need:
    ///
    /// ```ignore
    /// // Good: clone the status and release the lock immediately.
    /// let status = rx.borrow().clone();
    /// some_async_fn(status.durable_seq).await;
    /// some_other_async_fn(status.current_manifest.clone()).await;
    ///
    /// // Good: copy the durable seq and release the lock immediately.
    /// let durable_seq = rx.borrow().durable_seq; // uses Copy trait
    /// some_async_fn(durable_seq).await;
    ///
    /// // Bad: holding the status across an await blocks all senders.
    /// let status = rx.borrow();
    /// some_async_fn(status.durable_seq).await; // deadlock!
    /// ```
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
