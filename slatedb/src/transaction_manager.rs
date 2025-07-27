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
    /// Map of transaction state ID to weak reference
    inner: Arc<RwLock<TransactionManagerInner>>,
}

struct TransactionManagerInner {
    active_txns: HashMap<Uuid, Weak<TransactionState>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
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
        let mut inner = self.inner.write();
        inner.active_txns.remove(&txn_state.id);
    }

    pub fn min_retention_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner.active_txns.values().map(|state| state.seq).min()
    }
}
