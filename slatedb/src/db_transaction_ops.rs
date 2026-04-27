use uuid::Uuid;

use crate::config::{MergeOptions, PutOptions, WriteOptions};
use crate::db::WriteHandle;
use crate::DbReadOps;

/// Trait for transactional database operations.
///
/// This trait defines the synchronous write API and lifecycle operations
/// exposed by [`DbTransaction`](crate::DbTransaction), and extends
/// [`DbReadOps`] so consumers can write generic code or test doubles over
/// the full transaction surface without depending on the concrete
/// `DbTransaction` type.
#[async_trait::async_trait]
pub trait DbTransactionOps: DbReadOps {
    /// Put a key-value pair into the transaction with default `PutOptions`.
    /// The write is buffered in the transaction's write batch until commit.
    fn put<K, V>(&self, key: K, value: V) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the transaction with custom `PutOptions`.
    /// The write is buffered in the transaction's write batch until commit.
    fn put_with_options<K, V>(
        &self,
        key: K,
        value: V,
        options: &PutOptions,
    ) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    /// Delete a key from the transaction. The delete is buffered in the
    /// transaction's write batch until commit.
    fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), crate::Error>;

    /// Merge a key-value pair into the transaction with default
    /// `MergeOptions`.
    ///
    /// ## Errors
    /// - `Error`: if no merge operator is configured for the database.
    fn merge<K, V>(&self, key: K, value: V) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(key, value, &MergeOptions::default())
    }

    /// Merge a key-value pair into the transaction with custom `MergeOptions`.
    ///
    /// ## Errors
    /// - `Error`: if no merge operator is configured for the database.
    fn merge_with_options<K, V>(
        &self,
        key: K,
        value: V,
        options: &MergeOptions,
    ) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    /// Mark keys as read for conflict detection.
    ///
    /// When keys are marked as read, the transaction will detect conflicts
    /// if another transaction modifies any of those keys after this
    /// transaction started, regardless of the isolation level.
    fn mark_read<K, I>(&self, keys: I) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>;

    /// Mark written keys as untracked for conflict detection.
    ///
    /// Keys marked with this method are still written atomically with the
    /// rest of the transaction, but are excluded from transaction conflict
    /// detection on commit for both this transaction and other transactions.
    fn unmark_write<K, I>(&self, keys: I) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>;

    /// Get the sequence number this transaction was started at.
    fn seqnum(&self) -> u64;

    /// Get the unique transaction ID assigned by the transaction manager.
    fn id(&self) -> Uuid;

    /// Commit the transaction with default `WriteOptions`.
    ///
    /// ## Returns
    /// - `Ok(Some(WriteHandle))` if the commit is successful and there are
    ///   writes in the batch.
    /// - `Ok(None)` if the commit is successful but the write batch is empty.
    ///
    /// ## Errors
    /// - `Error`: if the commit operation fails (I/O errors or conflict
    ///   detection).
    async fn commit(self) -> Result<Option<WriteHandle>, crate::Error>
    where
        Self: Sized + Send,
    {
        self.commit_with_options(&WriteOptions::default()).await
    }

    /// Commit the transaction with custom `WriteOptions`.
    async fn commit_with_options(
        self,
        options: &WriteOptions,
    ) -> Result<Option<WriteHandle>, crate::Error>
    where
        Self: Sized + Send;

    /// Rollback the transaction by discarding all buffered operations.
    fn rollback(self)
    where
        Self: Sized;
}
