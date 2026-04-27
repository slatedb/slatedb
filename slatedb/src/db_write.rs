use crate::{
    batch::WriteBatch,
    config::{FlushOptions, MergeOptions, PutOptions, WriteOptions},
    db::WriteHandle,
    transaction_manager::IsolationLevel,
    DbTransactionOps,
};

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
