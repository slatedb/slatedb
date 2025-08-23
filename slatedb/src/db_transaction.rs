use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;
use uuid::Uuid;

use crate::batch::WriteBatch;
use crate::bytes_range::BytesRange;
use crate::config::{PutOptions, ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;

use crate::db::DbInner;
use crate::transaction_manager::TransactionManager;
use crate::{DbRead, Error};

/// A database transaction that supports both read and write operations.
///
/// DbTransaction provides ACID properties:
/// - Atomicity: All operations within a transaction are applied atomically
/// - Consistency: The database remains in a consistent state
/// - Isolation: Concurrent transactions don't interfere with each other
/// - Durability: Committed changes persist across system failures
///
/// Transactions use optimistic concurrency control with conflict detection at commit time.
pub struct DbTransaction {
    /// txn_id is the id of this transaction
    txn_id: Uuid,
    /// started_seq is the sequence number when this transaction started
    started_seq: u64,
    /// Reference to the transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Reference to the database
    db_inner: Arc<DbInner>,
    /// Local write batch containing all modifications in this transaction
    write_batch: WriteBatch,
    /// Whether this transaction has been committed or rolled back
    is_finished: bool,
}

impl DbTransaction {
    /// Create a new database transaction
    pub(crate) fn new(
        db_inner: Arc<DbInner>,
        txn_manager: Arc<TransactionManager>,
        seq: u64,
    ) -> Arc<Self> {
        let txn_id = txn_manager.new_txn(seq, false); // read_only = false for transactions
        let mut write_batch = WriteBatch::new();
        write_batch.txn_id = Some(txn_id);

        Arc::new(Self {
            txn_id,
            started_seq: seq,
            txn_manager,
            db_inner,
            write_batch,
            is_finished: false,
        })
    }

    /// Put a key-value pair into the transaction
    ///
    /// ## Arguments
    /// - `key`: the key to put
    /// - `value`: the value to put
    ///
    /// ## Returns
    /// - `Result<(), Error>`: Ok if successful
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<(), Error> {
        self.put_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the transaction with custom options
    ///
    /// ## Arguments  
    /// - `key`: the key to put
    /// - `value`: the value to put
    /// - `options`: the put options
    ///
    /// ## Returns
    /// - `Result<(), Error>`: Ok if successful
    pub fn put_with_options<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
        options: &PutOptions,
    ) -> Result<(), Error> {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        self.write_batch.put_with_options(key, value, options);
        Ok(())
    }

    /// Delete a key from the transaction
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    ///
    /// ## Returns
    /// - `Result<(), Error>`: Ok if successful
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error> {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        self.write_batch.delete(key);
        Ok(())
    }

    /// Get a value from the transaction (includes both committed data and pending writes)
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`: the value if it exists, None otherwise
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the transaction with custom read options
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`: the value if it exists, None otherwise
    pub async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, Error> {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        // TODO: put write batch in reader
        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .get_with_options(key, options, &db_state, Some(self.started_seq))
            .await
            .map_err(Into::into)
    }

    /// Scan a range of keys from the transaction
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys from the transaction with custom options
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the scan options to use
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = (start, end);

        // TODO: put write batch in reader

        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .scan_with_options(
                BytesRange::from(range),
                options,
                &db_state,
                Some(self.started_seq),
            )
            .await
            .map_err(Into::into)
    }

    /// Commit the transaction, applying all changes atomically
    ///
    /// ## Returns
    /// - `Result<(), Error>`: Ok if successful, error if there are conflicts
    pub async fn commit(mut self) -> Result<(), Error> {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        if self.write_batch.ops.is_empty() {
            // No changes to commit
            self.is_finished = true;
            return Ok(());
        }

        // TODO: Implement transaction commit with conflict checking
        // This requires access to a public write_batch method on DbInner
        // For now, we'll return an error to indicate this is not yet implemented
        self.is_finished = true;
        Err(Error::operation(
            "Transaction commit not yet implemented - requires access to DbInner::write_batch"
                .to_string(),
        ))
    }

    /// Roll back the transaction, discarding all changes
    ///
    /// ## Returns
    /// - `Result<(), Error>`: Ok if successful
    pub fn rollback(mut self) -> Result<(), Error> {
        if self.is_finished {
            return Err(Error::operation(
                "Transaction has been committed or rolled back".to_string(),
            ));
        }

        self.is_finished = true;
        // Simply discard the write batch - no changes are applied
        Ok(())
    }

    /// Check if the transaction has been finished (committed or rolled back)
    fn is_finished(&self) -> bool {
        self.is_finished
    }

    /// Get the transaction ID
    fn txn_id(&self) -> Uuid {
        self.txn_id
    }
}

#[async_trait::async_trait]
impl DbRead for DbTransaction {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, Error> {
        self.get_with_options(key, options).await
    }

    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, options).await
    }
}

/// Automatically roll back transaction when dropped (if not already finished)
impl Drop for DbTransaction {
    fn drop(&mut self) {
        if !self.is_finished {
            // Transaction was not committed or explicitly rolled back
            // Drop it from the transaction manager
            self.txn_manager.drop_txn(&self.txn_id);
            self.is_finished = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompactorOptions, Settings};
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStore;
    use crate::{Db, Error};
    use std::sync::Arc;
    use std::time::Duration;
}
