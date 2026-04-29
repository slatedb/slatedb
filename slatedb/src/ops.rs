use bytes::Bytes;
use std::ops::RangeBounds;
use uuid::Uuid;

use crate::batch::WriteBatch;
use crate::bytes_range::BytesRange;
use crate::config::{
    FlushOptions, MergeOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions,
};
use crate::db::WriteHandle;
use crate::db_cache_manager::CacheTarget;
use crate::db_state::SsTableId;
use crate::db_status::DbStatus;
use crate::manifest::VersionedManifest;
use crate::transaction_manager::IsolationLevel;
use crate::types::KeyValue;
use crate::DbIterator;

/// Trait for read-only database operations.
///
/// This trait defines the interface for reading data from SlateDB,
/// and can be implemented by `Db`, `DbReader` and `DbSnapshot`
/// to provide a unified interface for read-only operations.
#[async_trait::async_trait]
pub trait DbReadOps {
    /// Get a value from the database with default read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the database with custom read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///   - `Some(Bytes)`: the value if it exists
    ///   - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error>;

    /// Get a key-value pair from the database with default read options.
    ///
    /// Returns the key along with its value and metadata (sequence number,
    /// creation timestamp, expiration timestamp). Unlike [`get`](Self::get),
    /// which returns only the value bytes, this method returns a [`KeyValue`]
    /// that includes row metadata.
    ///
    /// ## Arguments
    /// - `key`: the key to look up
    ///
    /// ## Returns
    /// - `Ok(Some(KeyValue))`: if the key exists and is not deleted/expired
    /// - `Ok(None)`: if the key does not exist or is deleted/expired
    ///
    /// ## Errors
    /// - `Error`: if there was an error reading from the database
    async fn get_key_value<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<KeyValue>, crate::Error> {
        self.get_key_value_with_options(key, &ReadOptions::default())
            .await
    }

    /// Get a key-value pair from the database with custom read options.
    ///
    /// Returns the key along with its value and metadata (sequence number,
    /// creation timestamp, expiration timestamp). Unlike
    /// [`get_with_options`](Self::get_with_options), which returns only the
    /// value bytes, this method returns a [`KeyValue`] that includes row
    /// metadata.
    ///
    /// ## Arguments
    /// - `key`: the key to look up
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Ok(Some(KeyValue))`: if the key exists and is not deleted/expired
    /// - `Ok(None)`: if the key does not exist or is deleted/expired
    ///
    /// ## Errors
    /// - `Error`: if there was an error reading from the database
    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, crate::Error>;

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys with the provided options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the scan options to use
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send;

    /// Scan all keys that share the provided prefix using the default scan options.
    ///
    /// ## Arguments
    /// - `prefix`: the key prefix to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan_prefix<P>(&self, prefix: P) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        self.scan_prefix_with_options(prefix, &ScanOptions::default())
            .await
    }

    /// Scan all keys that share the provided prefix with custom options.
    ///
    /// ## Arguments
    /// - `prefix`: the key prefix to scan
    /// - `options`: the scan options to use
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan_prefix_with_options<P>(
        &self,
        prefix: P,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        let range = BytesRange::from_prefix(prefix.as_ref());
        self.scan_with_options(range, options).await
    }
}

/// Trait for write-side database operations.
///
/// This trait defines the asynchronous write API exposed by [`Db`](crate::Db),
/// allowing consumers to write generic code or test doubles over the writer
/// surface without depending on the concrete `Db` type.
#[async_trait::async_trait]
pub trait DbWriteOps {
    /// The transaction type returned by [`Self::begin`]. Stub
    /// implementations supply their own [`DbTransactionOps`] type, while
    /// the real `Db` returns a `DbTransaction`.
    type Transaction: DbTransactionOps + Send;

    /// Write a value into the database with default `PutOptions` and
    /// `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to write
    /// - `value`: the value to write
    ///
    /// ## Errors
    /// - `Error`: if there was an error writing the value.
    async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send,
    {
        self.put_with_options(key, value, &PutOptions::default(), &WriteOptions::default())
            .await
    }

    /// Write a value into the database with custom `PutOptions` and
    /// `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to write
    /// - `value`: the value to write
    /// - `put_opts`: the put options to use
    /// - `write_opts`: the write options to use
    ///
    /// ## Errors
    /// - `Error`: if there was an error writing the value.
    async fn put_with_options<K, V>(
        &self,
        key: K,
        value: V,
        put_opts: &PutOptions,
        write_opts: &WriteOptions,
    ) -> Result<WriteHandle, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send;

    /// Delete a key from the database with default `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    ///
    /// ## Errors
    /// - `Error`: if there was an error deleting the key.
    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<WriteHandle, crate::Error> {
        self.delete_with_options(key, &WriteOptions::default())
            .await
    }

    /// Delete a key from the database with custom `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    /// - `options`: the write options to use
    ///
    /// ## Errors
    /// - `Error`: if there was an error deleting the key.
    async fn delete_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &WriteOptions,
    ) -> Result<WriteHandle, crate::Error>;

    /// Merge a value into the database with default `MergeOptions` and
    /// `WriteOptions`.
    ///
    /// Merge operations allow applications to bypass the traditional
    /// read/modify/write cycle by expressing partial updates using an
    /// associative operator. The merge operator must be configured when
    /// opening the database.
    ///
    /// ## Arguments
    /// - `key`: the key to merge into
    /// - `value`: the merge operand to apply
    ///
    /// ## Errors
    /// - `Error`: if there was an error merging the value, or if no merge
    ///   operator is configured.
    async fn merge<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send,
    {
        self.merge_with_options(
            key,
            value,
            &MergeOptions::default(),
            &WriteOptions::default(),
        )
        .await
    }

    /// Merge a value into the database with custom `MergeOptions` and
    /// `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to merge into
    /// - `value`: the merge operand to apply
    /// - `merge_opts`: the merge options to use
    /// - `write_opts`: the write options to use
    ///
    /// ## Errors
    /// - `Error`: if there was an error merging the value, or if no merge
    ///   operator is configured.
    async fn merge_with_options<K, V>(
        &self,
        key: K,
        value: V,
        merge_opts: &MergeOptions,
        write_opts: &WriteOptions,
    ) -> Result<WriteHandle, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send;

    /// Write a batch of put/delete operations atomically to the database.
    ///
    /// ## Arguments
    /// - `batch`: the batch of operations to write
    ///
    /// ## Errors
    /// - `Error`: if there was an error writing the batch.
    async fn write(&self, batch: WriteBatch) -> Result<WriteHandle, crate::Error> {
        self.write_with_options(batch, &WriteOptions::default())
            .await
    }

    /// Write a batch of put/delete operations atomically to the database with
    /// custom `WriteOptions`.
    ///
    /// ## Arguments
    /// - `batch`: the batch of operations to write
    /// - `options`: the write options to use
    ///
    /// ## Errors
    /// - `Error`: if there was an error writing the batch.
    async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<WriteHandle, crate::Error>;

    /// Flush in-memory writes to disk. This function blocks until the
    /// in-memory data has been durably written to object storage.
    ///
    /// ## Errors
    /// - `Error`: if there was an error flushing the database.
    async fn flush(&self) -> Result<(), crate::Error>;

    /// Flush in-memory writes to disk with custom options.
    ///
    /// An error will be returned if `options.flush_type` is `FlushType::Wal`
    /// and the WAL is disabled.
    ///
    /// ## Arguments
    /// - `options`: the flush options
    ///
    /// ## Errors
    /// - `Error`: if there was an error flushing the database.
    async fn flush_with_options(&self, options: FlushOptions) -> Result<(), crate::Error>;

    /// Begin a new transaction with the specified isolation level.
    ///
    /// ## Arguments
    /// - `isolation_level`: the isolation level for the transaction
    ///
    /// ## Returns
    /// - `Result<Self::Transaction, Error>`: the transaction handle
    async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Self::Transaction, crate::Error>;
}

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
    /// Returns the current manifest snapshot known to this handle, paired
    /// with its manifest version ID.
    fn manifest(&self) -> VersionedManifest;

    /// Subscribe to database state changes.
    ///
    /// Returns a [`tokio::sync::watch::Receiver<DbStatus>`] that always
    /// reflects the latest database status. The status includes the latest durable
    /// sequence number and the current manifest snapshot observed by this
    /// handle. For [`Db`](crate::Db) is is the current in-memory snapshot and
    /// for [`DbReader`](crate::DbReader) it is the latest manifest polled from object storage.
    /// For example, you can wait for a specific sequence number to
    /// become durable:
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
    /// Holding the read guard for an extended period will block all database status
    /// updates and may cause a deadlock. See the [deadlock warning in
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

/// Trait for block-cache warming and eviction operations.
#[async_trait::async_trait]
pub trait DbCacheManagerOps {
    /// Warms selected cache content for one SST.
    ///
    /// Callers fan out over SSTs themselves (for example with
    /// `FuturesUnordered`) to get the concurrency they want. Per-target
    /// outcomes are reflected in cache-manager metrics, not the return value.
    ///
    /// Returns `Err` on the first failing target. If no block cache is
    /// configured, or if the SST is not reachable from the current manifest,
    /// the call is a no-op that returns `Ok(())`.
    async fn warm_sst(
        &self,
        sst_id: SsTableId,
        targets: &[CacheTarget],
    ) -> Result<(), crate::Error>;

    /// Best-effort eviction of block-cache entries for one SST.
    ///
    /// If no block cache is configured, logs a warning and returns `Ok(())`.
    /// Does not check whether the SST is still live in the current manifest —
    /// callers own that policy.
    async fn evict_cached_sst(&self, sst_id: SsTableId) -> Result<(), crate::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time check: the trait is object-safe.
    fn _assert_object_safe(_: &dyn DbMetadataOps) {}
    fn _assert_cache_manager_ops_object_safe(_: &dyn DbCacheManagerOps) {}
}
