use parking_lot::RwLock;
use std::collections::BTreeMap;

/// Tracks active snapshot registrations by sequence number.
///
/// Both `DbSnapshot` and `DbTransaction` (via its internal snapshot) register
/// here so the flusher/compactor knows the oldest seq that must be retained.
/// The BTreeMap provides O(1) min lookup via `first_key_value()`.
pub(crate) struct SnapshotManager {
    inner: RwLock<BTreeMap<u64, u32>>,
}

impl SnapshotManager {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub(crate) fn register(&self, seq: u64) {
        let mut map = self.inner.write();
        *map.entry(seq).or_insert(0) += 1;
    }

    pub(crate) fn unregister(&self, seq: u64) {
        let mut map = self.inner.write();
        if let Some(count) = map.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                map.remove(&seq);
            }
        }
    }

    pub(crate) fn min_seq(&self) -> Option<u64> {
        self.inner.read().first_key_value().map(|(&k, _)| k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_min_seq() {
        let mgr = SnapshotManager::new();
        assert_eq!(mgr.min_seq(), None);

        mgr.register(10);
        assert_eq!(mgr.min_seq(), Some(10));

        mgr.register(5);
        assert_eq!(mgr.min_seq(), Some(5));

        mgr.register(20);
        assert_eq!(mgr.min_seq(), Some(5));
    }

    #[test]
    fn test_unregister_removes_entry() {
        let mgr = SnapshotManager::new();
        mgr.register(10);
        mgr.register(20);
        assert_eq!(mgr.min_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_seq(), Some(20));

        mgr.unregister(20);
        assert_eq!(mgr.min_seq(), None);
    }

    #[test]
    fn test_refcount_for_same_seq() {
        let mgr = SnapshotManager::new();
        mgr.register(10);
        mgr.register(10);
        mgr.register(10);

        mgr.unregister(10);
        assert_eq!(mgr.min_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_seq(), Some(10));

        mgr.unregister(10);
        assert_eq!(mgr.min_seq(), None);
    }

    #[test]
    fn test_unregister_nonexistent_is_noop() {
        let mgr = SnapshotManager::new();
        mgr.unregister(999);
        assert_eq!(mgr.min_seq(), None);
    }
}
