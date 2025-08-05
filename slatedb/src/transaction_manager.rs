use crate::db_state::DbState;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use uuid::Uuid;

pub(crate) struct TransactionState {
    id: Uuid,
    pub(crate) seq: u64,
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    inner: Arc<RwLock<TransactionManagerInner>>,
    // reference to the db state for updating recent_snapshot_min_seq
    db_state: Arc<RwLock<DbState>>,
}

struct TransactionManagerInner {
    /// Map of transaction state ID to weak reference
    active_txns: HashMap<Uuid, Weak<TransactionState>>,
}

impl TransactionManager {
    pub fn new(db_state: Arc<RwLock<DbState>>) -> Self {
        Self {
            db_state,
            inner: Arc::new(RwLock::new(TransactionManagerInner {
                active_txns: HashMap::new(),
            })),
        }
    }

    /// Register a transaction state with a specific ID
    pub fn new_txn(&self, seq: u64) -> Arc<TransactionState> {
        let id = Uuid::new_v4();
        let txn_state = Arc::new(TransactionState { id, seq });
        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(id, Arc::downgrade(&txn_state));
        }
        txn_state
    }

    /// Remove a transaction state when it's dropped
    pub fn remove_txn(&self, txn_state: &TransactionState) {
        {
            let mut inner = self.inner.write();
            inner.active_txns.remove(&txn_state.id);
        }

        self.save_recent_snapshot_min_seq();
    }

    fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner
            .active_txns
            .values()
            .filter_map(|state| state.upgrade().map(|state| state.seq))
            .min()
    }

    fn save_recent_snapshot_min_seq(&self) {
        let min_seq = self.min_active_seq();

        // update recent_snapshot_min_seq in the db state. the editted db state will be persisted
        // to the manifest store when memtable is flushed.
        let mut guard = self.db_state.write();
        guard.modify(|state| {
            state.state.manifest.core.recent_snapshot_min_seq = min_seq;
        });
    }
}
