use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;
use uuid::Uuid;

use crate::bytes_range::BytesRange;
use crate::config::{ReadOptions, ScanOptions};
use crate::db_iter::DbIterator;

use crate::db::DbInner;
use crate::transaction_manager::TransactionManager;
use crate::{DbRead, WriteBatch};

pub struct DBTransaction {
    /// txn_id is the id of the transaction
    txn_id: Uuid,
    /// txn_seq is the sequence number of the transaction
    started_seq: u64,
    /// Reference to the transaction manager that created this snapshot
    txn_manager: Arc<TransactionManager>,
    /// The write batch of the transaction, which contains the uncommitted writes.
    /// Users can read data from the write batch during the transaction,
    /// thus providing an MVCC view of the database.
    write_batch: WriteBatch,
    /// Reference to the database
    db_inner: Arc<DbInner>,
}
