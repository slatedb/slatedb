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
    transaction_states: Arc<RwLock<HashMap<Uuid, Weak<TransactionState>>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transaction_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a transaction state with a specific ID
    pub fn new_transaction_state(&self, seq: u64) -> Arc<TransactionState> {
        let id = Uuid::new_v4();
        let transaction_state = Arc::new(TransactionState { id, seq });
        {
            let mut transaction_states = self.transaction_states.write();
            transaction_states.insert(id, Arc::downgrade(&transaction_state));
        }
        transaction_state
    }

    /// Remove a transaction state when it's dropped
    pub fn remove(&self, transaction_state: &TransactionState) {
        let mut transaction_states = self.transaction_states.write();
        transaction_states.remove(&transaction_state.id);
    }
}
