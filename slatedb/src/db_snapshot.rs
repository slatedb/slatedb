use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;

use crate::db::DbInner;
use crate::transaction_manager::{TransactionManager, TransactionState};

pub struct DbSnapshot {
    /// txn_state holds the seq number of the transaction that created this snapshot
    txn_state: Arc<TransactionState>,
    /// Unique ID assigned by the transaction manager
    txn_manager: Arc<TransactionManager>,
    /// Reference to the database
    db_inner: Arc<DbInner>,
}

impl DbSnapshot {
    pub(crate) fn new(
        db_inner: Arc<DbInner>,
        txn_manager: Arc<TransactionManager>,
        seq: u64,
    ) -> Arc<Self> {
        let txn_state = txn_manager.new_txn(seq);

        Arc::new(Self {
            txn_state: txn_state,
            txn_manager,
            db_inner,
        })
    }

    /// Get a value from the snapshot with default read options.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`: the value if it exists, None otherwise
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
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
    ) -> Result<Option<Bytes>, crate::Error> {
        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .get_with_options(key, options, &db_state, Some(self.txn_state.seq))
            .await
            .map_err(Into::into)
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default())
            .await
            .map_err(Into::into)
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
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        // TODO: this range conversion logic can be extract to an util
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = (start, end);
        self.db_inner.check_error()?;
        let db_state = self.db_inner.state.read().view();
        self.db_inner
            .reader
            .scan_with_options(
                BytesRange::from(range),
                options,
                &db_state,
                Some(self.txn_state.seq),
            )
            .await
            .map_err(Into::into)
    }
}

impl Drop for DbSnapshot {
    fn drop(&mut self) {
        // Unregister from transaction manager when dropped
        self.txn_manager.remove_txn(self.txn_state.as_ref());
    }
}
