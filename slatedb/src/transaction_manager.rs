use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Weak;

pub(crate) struct TransactionState {
    seq: u64,
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    /// Map of transaction state ID to weak reference
    transaction_states: Arc<RwLock<HashMap<u64, Weak<TransactionState>>>>,
    /// Next transaction state ID to assign
    next_id: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transaction_states: Arc::new(RwLock::new(HashMap::new())),
            next_id: AtomicU64::new(1),
        }
    }

    /// Get the next available transaction state ID
    pub fn next_transaction_state_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Register a transaction state with a specific ID
    pub fn register_transaction_state(&self, id: u64, transaction_state: Weak<TransactionState>) {
        {
            let mut transaction_states = self.transaction_states.write();
            transaction_states.insert(id, transaction_state);
        }
    }

    /// Unregister a transaction state when it's dropped
    pub fn unregister_transaction_state(&self, id: u64) {
        let mut transaction_states = self.transaction_states.write();
        transaction_states.remove(&id);
    }

    /// Clean up any transaction states that have been dropped but not properly unregistered
    pub fn cleanup_dropped_transaction_states(&self) {
        let mut transaction_states = self.transaction_states.write();

        transaction_states.retain(|_, weak_ref| weak_ref.strong_count() > 0);
    }

    /// Get the number of currently active transaction states
    pub fn active_transaction_state_count(&self) -> usize {
        let transaction_states = self.transaction_states.read();
        transaction_states.len()
    }

    /// Get all currently active transaction state IDs
    pub fn get_active_transaction_state_ids(&self) -> Vec<u64> {
        let transaction_states = self.transaction_states.read();
        transaction_states.keys().copied().collect()
    }

    /// Get transaction state information for debugging/monitoring
    pub fn get_transaction_state_info(&self) -> HashMap<u64, bool> {
        let transaction_states = self.transaction_states.read();
        transaction_states
            .iter()
            .map(|(id, weak_ref)| (*id, weak_ref.strong_count() > 0))
            .collect()
    }
}
