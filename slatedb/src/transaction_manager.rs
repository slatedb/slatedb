use crate::rand::DbRand;
use crate::utils::IdGenerator;
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
    /// seq is the sequence number when the transaction started. this is used to establish
    /// a snapshot of this transaction. we should ensure the compactor cannot recycle
    /// the row versions that are below any seq number of active transactions.
    pub(crate) started_seq: u64,
    /// the sequence number when the transaction committed.
    pub(crate) committed_seq: Option<u64>,
    /// the write keys of the transaction.
    pub(crate) write_keys: HashSet<Bytes>,
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
    // Tracks recently committed transaction states used for conflict checks at commit time.
    // We can safely garbage collect an entry when ALL active transactions and snapshots
    // have started_seq strictly greater than this entry's committed_seq.
    // Notes:
    // - Snapshots are treated as read-only transactions and included in the active set.
    // - Non-transactional writes are modeled as single-op transactions with
    //   started_seq == committed_seq and follow the same GC rule.
    // - If there are no active transactions/snapshots, this deque can be drained.
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
    pub fn new_txn(&self, seq: u64) -> Arc<TransactionState> {
        let id = self.db_rand.rng().gen_uuid();
        let txn_state = Arc::new(TransactionState {
            id,
            started_seq: seq,
            committed_seq: None,
            write_keys: HashSet::new(),
        });
        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(id, txn_state.clone());
        }

        txn_state
    }

    /// Remove a transaction state when it's dropped
    pub fn remove_txn(&self, txn_state: &TransactionState) {
        {
            let mut inner = self.inner.write();
            inner.active_txns.remove(&txn_state.id);
        }
    }

    pub fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner
            .active_txns
            .values()
            .map(|state| state.started_seq)
            .min()
    }
}
