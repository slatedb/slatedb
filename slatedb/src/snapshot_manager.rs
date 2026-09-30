use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::oracle::{DbOracle, Oracle};
use crate::utils::IdGenerator;
use slatedb_common::DbRand;

/// Tracks active snapshot registrations by sequence numbers.
///
/// `DbSnapshot` instances register here so the flusher/compactor knows the
/// oldest seq that must be retained.
pub(crate) struct SnapshotManager {
    inner: RwLock<SnapshotManagerInner>,
    db_rand: Arc<DbRand>,
}

struct SnapshotManagerInner {
    /// Map of snapshot id to the minimum sequence number visible to the snapshot.
    active_snapshots: HashMap<Uuid, u64>,
    /// The oracle for tracking the last committed and last remote persisted sequence numbers.
    oracle: Arc<DbOracle>,
}

impl SnapshotManager {
    pub(crate) fn new(oracle: Arc<DbOracle>, db_rand: Arc<DbRand>) -> Self {
        Self {
            inner: RwLock::new(SnapshotManagerInner {
                active_snapshots: HashMap::new(),
                oracle,
            }),
            db_rand,
        }
    }

    pub(crate) fn new_snapshot(
        &self,
        seqs: Option<SnapshotSeqs>,
    ) -> Result<(Uuid, SnapshotSeqs), crate::Error> {
        let snapshot_id = self.db_rand.rng().gen_uuid();
        let mut inner = self.inner.write();

        let seqs = match seqs {
            Some(seqs) => seqs,
            None => {
                let memory_seq = inner.oracle.last_committed_seq();
                // A write batch is appended to the WAL and memtable before its commit seq is
                // advanced, so an async WAL flush in that short window can briefly cause
                // the remote sequence number to be greater than the memory sequence number.
                // To account for that, we cap the remote sequence number at the memory sequence number.
                let remote_seq = inner.oracle.last_remote_persisted_seq().min(memory_seq);
                SnapshotSeqs::new(memory_seq, remote_seq)?
            }
        };
        inner.active_snapshots.insert(snapshot_id, seqs.remote_seq);
        Ok((snapshot_id, seqs))
    }

    pub(crate) fn drop_snapshot(&self, snapshot_id: &Uuid) {
        let mut inner = self.inner.write();
        let removed = inner.active_snapshots.remove(snapshot_id);
        assert!(
            removed.is_some(),
            "drop_snapshot called on snapshot_id that is not tracked"
        );
    }

    /// The minimum visible sequence number across all active snapshots. This value
    /// is useful to inform the compactor about the min seq of data still needed to be
    /// retained for active snapshots, so that the compactor can avoid deleting the
    /// data that is still needed.
    ///
    /// min_active_seq will be persisted to the `recent_snapshot_min_seq` in the
    /// manifest when a new L0 is flushed.
    pub(crate) fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner.active_snapshots.values().copied().min()
    }
}

/// Sequence numbers for a snapshot.
///
/// Both memory (committed) and remote (persisted) sequence numbers are tracked to allow
/// consistent reads at different durability levels.
///
/// The remote sequence number must always be less than or equal to the memory sequence number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotSeqs {
    memory_seq: u64,
    remote_seq: u64,
}

impl SnapshotSeqs {
    pub(crate) fn new(memory_seq: u64, remote_seq: u64) -> Result<Self, crate::Error> {
        if remote_seq > memory_seq {
            return Err(crate::Error::internal(format!(
                "snapshot remote sequence {remote_seq} exceeds memory sequence {memory_seq}"
            )));
        }

        Ok(Self {
            memory_seq,
            remote_seq,
        })
    }

    pub(crate) fn memory_seq(&self) -> u64 {
        self.memory_seq
    }

    pub(crate) fn remote_seq(&self) -> u64 {
        self.remote_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_status::DbStatusManager;

    fn new_snapshot_manager(memory_seq: u64, remote_seq: u64) -> SnapshotManager {
        SnapshotManager::new(
            Arc::new(DbOracle::new(
                memory_seq,
                memory_seq,
                remote_seq,
                Arc::new(DbStatusManager::new(remote_seq)),
            )),
            Arc::new(DbRand::new(0)),
        )
    }

    #[test]
    fn test_new_snapshot_uses_oracle_seqs() {
        let mgr = new_snapshot_manager(123, 100);

        let (_, seqs) = mgr.new_snapshot(None).unwrap();
        assert_eq!(seqs.memory_seq, 123);
        assert_eq!(seqs.remote_seq, 100);
        assert_eq!(mgr.min_active_seq(), Some(100));
    }

    #[test]
    fn test_new_snapshot_caps_remote_seq_at_memory_seq() {
        let mgr = new_snapshot_manager(100, 123);

        let (_, seqs) = mgr.new_snapshot(None).unwrap();
        assert_eq!(seqs.memory_seq, 100);
        assert_eq!(seqs.remote_seq, 100);
        assert_eq!(mgr.min_active_seq(), Some(100));
    }

    #[test]
    fn test_snapshot_seqs_rejects_remote_seq_above_memory_seq() {
        let err = SnapshotSeqs::new(100, 123).unwrap_err();

        assert_eq!(err.kind(), crate::ErrorKind::Internal);
    }

    #[test]
    fn test_new_snapshot_and_min_seq() {
        let mgr = new_snapshot_manager(0, 0);
        assert_eq!(mgr.min_active_seq(), None);

        mgr.new_snapshot(Some(SnapshotSeqs::new(10, 10).unwrap()))
            .unwrap();
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.new_snapshot(Some(SnapshotSeqs::new(5, 5).unwrap()))
            .unwrap();
        assert_eq!(mgr.min_active_seq(), Some(5));

        mgr.new_snapshot(Some(SnapshotSeqs::new(20, 20).unwrap()))
            .unwrap();
        assert_eq!(mgr.min_active_seq(), Some(5));
    }

    #[test]
    fn test_drop_snapshot_removes_entry() {
        let mgr = new_snapshot_manager(0, 0);
        let (snapshot_10, _) = mgr
            .new_snapshot(Some(SnapshotSeqs::new(10, 10).unwrap()))
            .unwrap();
        let (snapshot_20, _) = mgr
            .new_snapshot(Some(SnapshotSeqs::new(20, 20).unwrap()))
            .unwrap();
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_10);
        assert_eq!(mgr.min_active_seq(), Some(20));

        mgr.drop_snapshot(&snapshot_20);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    fn test_multiple_snapshots_with_same_seq() {
        let mgr = new_snapshot_manager(0, 0);
        let (snapshot_1, _) = mgr
            .new_snapshot(Some(SnapshotSeqs::new(10, 10).unwrap()))
            .unwrap();
        let (snapshot_2, _) = mgr
            .new_snapshot(Some(SnapshotSeqs::new(10, 10).unwrap()))
            .unwrap();
        let (snapshot_3, _) = mgr
            .new_snapshot(Some(SnapshotSeqs::new(10, 10).unwrap()))
            .unwrap();

        mgr.drop_snapshot(&snapshot_1);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_2);
        assert_eq!(mgr.min_active_seq(), Some(10));

        mgr.drop_snapshot(&snapshot_3);
        assert_eq!(mgr.min_active_seq(), None);
    }

    #[test]
    #[should_panic]
    fn test_drop_snapshot_nonexistent_panics() {
        let mgr = new_snapshot_manager(0, 0);
        mgr.drop_snapshot(&Uuid::nil());
    }
}
