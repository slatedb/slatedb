use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Weak;

use crate::db_snapshot::DbSnapshot;
use crate::stats::{Counter, Gauge, StatRegistry};

/// Manages the lifecycle of DbSnapshot objects, tracking all living snapshots
pub struct TransactionManager {
    /// Map of snapshot ID to weak reference
    snapshots: Arc<RwLock<HashMap<u64, Weak<DbSnapshot>>>>,
    /// Next snapshot ID to assign
    next_id: AtomicU64,
}

impl TransactionManager {
    pub fn new(stat_registry: &StatRegistry) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            next_id: AtomicU64::new(1),
        }
    }

    /// Get the next available snapshot ID
    pub fn next_snapshot_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Register a snapshot with a specific ID
    pub fn register_snapshot(&self, id: u64, snapshot: Weak<DbSnapshot>) {
        {
            let mut snapshots = self.snapshots.write();
            snapshots.insert(id, snapshot);
        }
    }

    /// Unregister a snapshot when it's dropped
    pub fn unregister_snapshot(&self, id: u64) {
        let mut snapshots = self.snapshots.write();
        snapshots.remove(&id);
    }

    /// Clean up any snapshots that have been dropped but not properly unregistered
    pub fn cleanup_dropped_snapshots(&self) {
        let mut snapshots = self.snapshots.write();
        let initial_count = snapshots.len();

        snapshots.retain(|_, weak_ref| weak_ref.strong_count() > 0);
    }

    /// Get the number of currently active snapshots
    pub fn active_snapshot_count(&self) -> usize {
        let snapshots = self.snapshots.read();
        snapshots.len()
    }

    /// Get all currently active snapshot IDs
    pub fn get_active_snapshot_ids(&self) -> Vec<u64> {
        let snapshots = self.snapshots.read();
        snapshots.keys().copied().collect()
    }

    /// Get snapshot information for debugging/monitoring
    pub fn get_snapshot_info(&self) -> HashMap<u64, bool> {
        let snapshots = self.snapshots.read();
        snapshots
            .iter()
            .map(|(id, weak_ref)| (*id, weak_ref.strong_count() > 0))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::StatRegistry;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_transaction_manager_lifecycle() {
        let registry = StatRegistry::new();
        let manager = TransactionManager::new(&registry);

        // Initially no snapshots
        assert_eq!(manager.active_snapshot_count(), 0);
        assert_eq!(manager.stats.active_snapshots.get(), 0);
        assert_eq!(manager.stats.total_snapshots_created.get(), 0);

        // Create a snapshot (we'll need to create a mock for testing)
        // This test will be completed once DbSnapshot is fully implemented
    }

    #[tokio::test]
    async fn test_cleanup_dropped_snapshots() {
        let registry = StatRegistry::new();
        let manager = TransactionManager::new(&registry);

        // Test cleanup functionality
        manager.cleanup_dropped_snapshots();
        assert_eq!(manager.active_snapshot_count(), 0);
    }
}
