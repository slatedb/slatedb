use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::oracle::{DbOracle, Oracle};

/// Tracks active snapshot registrations by sequence number.
///
/// `DbSnapshot` instances register here so the flusher/compactor knows the
/// oldest seq that must be retained. The BTreeMap provides an efficient min
/// lookup via `first_key_value()`.
pub(crate) struct SnapshotManager {
    inner: RwLock<SnapshotManagerInner>,
}

struct SnapshotManagerInner {
    /// Map of sequence number to reference count.
    active_snapshots: BTreeMap<u64, u32>,
    /// The oracle for tracking the last committed sequence number.
    oracle: Arc<DbOracle>,
}

impl SnapshotManager {
    pub(crate) fn new(oracle: Arc<DbOracle>) -> Self {
        Self {
            inner: RwLock::new(SnapshotManagerInner {
                active_snapshots: BTreeMap::new(),
                oracle,
            }),
        }
    }

    pub(crate) fn register(&self, seq: Option<u64>) -> u64 {
        let mut inner = self.inner.write();
        let seq = seq.unwrap_or_else(|| inner.oracle.last_committed_seq());
        *inner.active_snapshots.entry(seq).or_insert(0) += 1;
        seq
    }

    pub(crate) fn unregister(&self, seq: u64) {
        let mut inner = self.inner.write();
        if let Some(count) = inner.active_snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                inner.active_snapshots.remove(&seq);
            }
        } else {
            unreachable!("unregister called on seq that is not tracked")
        }
    }

    /// The min started_seq of all active snapshots. This value
    /// is useful to inform the compactor about the min seq of data still needed to be
    /// retained for active snapshots, so that the compactor can avoid deleting the
    /// data that is still needed.
    ///
    /// min_active_seq will be persisted to the `recent_snapshot_min_seq` in the
    /// manifest when a new L0 is flushed.
    pub(crate) fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner.active_snapshots.first_key_value().map(|(&k, _)| k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_status::DbStatusReporter;

    #[test]
    fn test_resgister_uses_oracle_seq() {
        let status_reporter = DbStatusReporter::new(123);
        let oracle = Arc::new(DbOracle::new(123, 123, 123, status_reporter));
        let mgr = SnapshotManager::new(oracle);

        let seq = mgr.register(None);
        assert_eq!(seq, 123);
    }

    #[test]
    fn test_register_and_min_seq() {
        let mgr = SnapshotManager::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        assert_eq!(mgr.min_active_seq(), None);

        mgr.register(Some(10));
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.register(Some(5));
        assert_eq!(mgr.min_active_seq(), Some(5));

        mgr.register(Some(20));
        assert_eq!(mgr.min_active_seq(), Some(5));
    }

    #[test]
    fn test_unregister_removes_entry() {
        let mgr = SnapshotManager::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        mgr.register(Some(10));
        mgr.register(Some(20));
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_active_seq(), Some(20));

        mgr.unregister(20);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    fn test_multiple_snapshots_with_same_seq() {
        let mgr = SnapshotManager::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        mgr.register(Some(10));
        mgr.register(Some(10));
        mgr.register(Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    #[should_panic]
    fn test_unregister_nonexistent_panics() {
        let mgr = SnapshotManager::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        mgr.unregister(999);
    }
}
