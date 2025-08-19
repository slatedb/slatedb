use crate::rand::DbRand;
use crate::utils::IdGenerator;
use crate::WriteBatch;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct TransactionState {
    /// id is used to track the lifecycle of a transaction. when a snapshot/transaction
    /// ends, we can remove the transaction state from the transaction manager by this
    /// id. we can not use seq as the txn id, because it's possible to start multiple
    /// transactions with the same seq number.
    id: Uuid,
    pub(crate) read_only: bool,
    /// seq is the sequence number when the transaction started. this is used to establish
    /// a snapshot of this transaction. we should ensure the compactor cannot recycle
    /// the row versions that are below any seq number of active transactions.
    pub(crate) started_seq: u64,
    /// the sequence number when the transaction committed. this field is only set AFTER
    /// a transaction is committed. this is used to check conflicts with recent committed
    /// transactions.
    pub(crate) committed_seq: Option<u64>,
    /// the write keys of the transaction.
    pub(crate) write_keys: HashSet<Bytes>,
}

impl TransactionState {
    fn track_write_keys(&mut self, keys: impl IntoIterator<Item = Bytes>) {
        todo!()
    }

    fn mark_as_committed(&mut self, seq: u64) {
        self.committed_seq = Some(seq);
    }
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    inner: Arc<RwLock<TransactionManagerInner>>,
    // random number generator for generating transaction IDs
    db_rand: Arc<DbRand>,
}

struct TransactionManagerInner {
    /// Map of transaction state ID to weak reference.
    active_txns: HashMap<Uuid, Arc<TransactionState>>,
    /// Tracks recently committed transaction states for conflict checks at commit.
    ///
    /// An entry can be garbage collected when *all* active transactions (excluding snapshots,
    /// since snapshots are read-only so it's impossible to have any conflict) have `started_seq`
    /// strictly greater than the entry's `committed_seq`.
    ///
    /// Notes:
    /// - Snapshots are treated as read-only transactions.
    /// - Non-transactional writes are modeled as single-op transactions with `started_seq ==
    ///   committed_seq` and follow the same GC rule.
    /// - If there are no active non-readonly transactions, this deque can be fully drained.
    recent_committed_txns: VecDeque<Arc<TransactionState>>,
}

impl TransactionManager {
    pub fn new(db_rand: Arc<DbRand>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransactionManagerInner {
                active_txns: HashMap::new(),
                recent_committed_txns: VecDeque::new(),
            })),
            db_rand,
        }
    }

    /// Register a transaction state with a specific ID
    pub fn new_txn(&self, seq: u64, read_only: bool) -> Arc<TransactionState> {
        let id = self.db_rand.rng().gen_uuid();
        let txn_state = Arc::new(TransactionState {
            id,
            started_seq: seq,
            committed_seq: None,
            read_only,
            write_keys: HashSet::new(),
        });
        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(id, txn_state.clone());
        }

        txn_state
    }

    /// Remove a transaction state when it's dropped. The dropped txn is considered
    /// as rolled back, no side effect is ever produced.
    pub fn drop_txn(&self, txn_state: &TransactionState) {
        {
            let mut inner = self.inner.write();
            inner.active_txns.remove(&txn_state.id);
        }
    }

    /// Mark the txn as committed, and record it in recent_committed_txns.
    pub fn mark_committed(&self, txn_state: &TransactionState, seq: u64) {
        todo!();
    }

    /// The min started_seq of all active transactions, including snapshots. This value
    /// is useful to inform the compactor about the min seq of data still needed to be
    /// retained for active transactions, so that the compactor can avoid deleting the
    /// data that is still needed.
    ///
    /// min_active_seq will be persisted to the `recent_snapshot_min_seq` in the manifest
    /// when a new L0 is flushed.
    pub fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner
            .active_txns
            .values()
            .map(|state| state.started_seq)
            .min()
    }

    /// The min started_seq of all non-readonly transactions, this seq is useful to garbage
    /// collect the entries in the `recent_committed_txns` deque.
    pub fn min_conflict_check_seq(&self) -> Option<u64> {
        todo!();
    }

    pub fn check_conflict(&self, txn_state: &TransactionState) -> bool {
        todo!();
    }
}
