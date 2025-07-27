use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::{Arc, Weak};

use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;
use crate::error::SlateDBError;
use crate::transaction_manager::{TransactionManager, TransactionState};
use crate::Db;

pub struct DbSnapshot {
    txn_state: Arc<TransactionState>,
    /// Unique ID assigned by the transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Reference to the database
    db: Arc<Db>,
}

impl DbSnapshot {
    pub(crate) fn new(db: Arc<Db>, txn_manager: Arc<TransactionManager>, seq: u64) -> Arc<Self> {
        let txn_state = txn_manager.new_txn(seq);

        Arc::new(Self {
            txn_state: txn_state,
            txn_manager,
            db,
        })
    }

    /// Get a value from the snapshot with default read options.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`: the value if it exists, None otherwise
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, SlateDBError> {
        todo!()
    }

    /// Get a value from the snapshot with custom read options.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`: the value if it exists, None otherwise
    pub async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        todo!()
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        todo!()
    }

    /// Scan a range of keys with the provided options.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the scan options to use
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        todo!()
    }
}

impl Drop for DbSnapshot {
    fn drop(&mut self) {
        // Unregister from transaction manager when dropped
        self.txn_manager.remove_txn(self.transaction_state.as_ref());
    }
}
