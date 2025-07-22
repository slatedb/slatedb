use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::{Arc, Weak};

use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;
use crate::error::SlateDBError;
use crate::transaction_manager::TransactionManager;
use crate::Db;

pub struct DbSnapshot {
    seq: u64,
    /// Unique ID assigned by the transaction manager
    id: u64,
    /// Reference to the transaction manager for cleanup
    transaction_manager: Arc<TransactionManager>,
    /// Reference to the database
    db: Arc<Db>,
}

impl DbSnapshot {
    pub(crate) fn new(db: Arc<Db>, transaction_manager: Arc<TransactionManager>, seq: u64) -> Arc<Self> {
        // Get the ID first, then create the snapshot
        let id = transaction_manager.next_snapshot_id();
        
        let snapshot = Arc::new(Self {
            seq,
            id,
            transaction_manager: transaction_manager.clone(),
            db,
        });
        
        // Register the snapshot with the transaction manager
        transaction_manager.register_snapshot(id, Arc::downgrade(&snapshot));
        
        snapshot
    }

    /// Get the snapshot's sequence number
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Get the snapshot's unique ID
    pub fn id(&self) -> u64 {
        self.id
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
        self.transaction_manager.unregister_snapshot(self.id);
    }
}
