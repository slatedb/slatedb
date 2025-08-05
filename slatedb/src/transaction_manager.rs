use crate::db::DbInner;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) struct TransactionState {
    id: Uuid,
    pub(crate) seq: u64,
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    /// Map of transaction state ID to weak reference
    inner: Arc<RwLock<TransactionManagerInner>>,
    /// Reference to the db inner for updating manifest retention information
    db_inner: Arc<DbInner>,
}

struct TransactionManagerInner {
    active_txns: HashMap<Uuid, Weak<TransactionState>>,
    /// The last min retention seq that has been synced to the object store.
    last_sync_manifest_time: Option<Instant>,
}

impl TransactionManager {
    pub fn new(db_inner: Arc<DbInner>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransactionManagerInner {
                active_txns: HashMap::new(),
                last_sync_manifest_time: None,
            })),
            db_inner,
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
        let mut state_guard = self.db_inner.state.write();
        state_guard.modify(|state| {
            state.state.manifest.core.recent_snapshot_min_seq = min_seq;
        });
    }
}
