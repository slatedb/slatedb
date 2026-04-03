use std::collections::BTreeMap;
use std::sync::Arc;

use crate::oracle::{DbOracle, Oracle};

/// Tracks active sequence registrations by sequence number.
///
/// Read-only snapshots and transactions both register here so the flusher and
/// compactor know the oldest seq that must be retained. The BTreeMap provides
/// an efficient min lookup via `first_key_value()`.
pub(crate) struct ActiveSeqTracker {
    /// Map of sequence number to reference count.
    pub(crate) active_seqs: BTreeMap<u64, u32>,
    /// The oracle for tracking the last committed sequence number.
    pub(crate) oracle: Arc<DbOracle>,
}

impl ActiveSeqTracker {
    pub(crate) fn new(oracle: Arc<DbOracle>) -> Self {
        Self {
            active_seqs: BTreeMap::new(),
            oracle,
        }
    }

    pub(crate) fn register(&mut self, seq: Option<u64>) -> u64 {
        let seq = seq.unwrap_or_else(|| self.oracle.last_committed_seq());
        *self.active_seqs.entry(seq).or_insert(0) += 1;
        seq
    }

    #[allow(clippy::panic)]
    pub(crate) fn unregister(&mut self, seq: u64) {
        if let Some(count) = self.active_seqs.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                self.active_seqs.remove(&seq);
            }
        } else {
            panic!("unregister called on seq that is not tracked")
        }
    }

    /// The minimum seq of all active readers that still require GC protection.
    /// This value is useful to inform the compactor about the minimum seq of data
    /// still needed to be retained so the compactor can avoid deleting needed data.
    ///
    /// This value is persisted to `recent_snapshot_min_seq` in the manifest when
    /// a new L0 is flushed.
    pub(crate) fn min_seq(&self) -> Option<u64> {
        self.active_seqs.first_key_value().map(|(&k, _)| k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_status::DbStatusReporter;

    #[test]
    fn test_register_uses_oracle_seq() {
        let status_reporter = DbStatusReporter::new(123);
        let oracle = Arc::new(DbOracle::new(123, 123, 123, status_reporter));
        let mut tracker = ActiveSeqTracker::new(oracle);

        let seq = tracker.register(None);
        assert_eq!(seq, 123);
    }

    #[test]
    fn test_register_and_min_seq() {
        let mut tracker =
            ActiveSeqTracker::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        assert_eq!(tracker.min_seq(), None);

        tracker.register(Some(10));
        assert_eq!(tracker.min_seq(), Some(10));

        tracker.register(Some(5));
        assert_eq!(tracker.min_seq(), Some(5));

        tracker.register(Some(20));
        assert_eq!(tracker.min_seq(), Some(5));
    }

    #[test]
    fn test_unregister_removes_entry() {
        let mut tracker =
            ActiveSeqTracker::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        let seq10 = tracker.register(Some(10));
        let seq20 = tracker.register(Some(20));
        assert_eq!(tracker.min_seq(), Some(10));

        tracker.unregister(seq10);
        assert_eq!(tracker.min_seq(), Some(20));

        tracker.unregister(seq20);
        assert_eq!(tracker.min_seq(), None);
    }

    #[test]
    fn test_refcount_for_same_seq() {
        let mut tracker =
            ActiveSeqTracker::new(Arc::new(DbOracle::new(0, 0, 0, DbStatusReporter::new(0))));
        let seq1 = tracker.register(Some(10));
        let seq2 = tracker.register(Some(10));
        let seq3 = tracker.register(Some(10));

        tracker.unregister(seq1);
        assert_eq!(tracker.min_seq(), Some(10));

        tracker.unregister(seq2);
        assert_eq!(tracker.min_seq(), Some(10));

        tracker.unregister(seq3);
        assert_eq!(tracker.min_seq(), None);
    }
}
