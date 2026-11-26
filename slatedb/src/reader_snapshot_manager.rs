use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use crate::utils::IdGenerator;
use slatedb_common::DbRand;

/// Tracks the live `DbReaderSnapshot`s of a reader and the checkpoint each one
/// reads.
///
/// A `ManagedCheckpoint` reader asks it before it replaces its checkpoint. If a
/// snapshot reads the checkpoint, the reader keeps the checkpoint, refreshes it
/// on each poll while a snapshot reads it, and deletes it once none does.
pub(crate) struct ReaderSnapshotManager {
    inner: Mutex<ReaderSnapshotManagerInner>,
    db_rand: Arc<DbRand>,
}

#[derive(Default)]
struct ReaderSnapshotManagerInner {
    /// Map of snapshot id to the checkpoint that its state reads, if any.
    active_snapshots: HashMap<Uuid, Option<Uuid>>,
    /// Checkpoints that the reader replaced while a snapshot read them.
    retained_checkpoints: HashSet<Uuid>,
}

impl ReaderSnapshotManager {
    pub(crate) fn new(db_rand: Arc<DbRand>) -> Self {
        Self {
            inner: Mutex::new(ReaderSnapshotManagerInner::default()),
            db_rand,
        }
    }

    pub(crate) fn new_snapshot(&self, checkpoint_id: Option<Uuid>) -> Uuid {
        let snapshot_id = self.db_rand.rng().gen_uuid();
        self.inner
            .lock()
            .active_snapshots
            .insert(snapshot_id, checkpoint_id);
        snapshot_id
    }

    pub(crate) fn drop_snapshot(&self, snapshot_id: &Uuid) {
        let removed = self.inner.lock().active_snapshots.remove(snapshot_id);
        assert!(
            removed.is_some(),
            "drop_snapshot called on snapshot_id that is not tracked"
        );
    }

    /// Whether a live snapshot reads `checkpoint_id`.
    pub(crate) fn is_read(&self, checkpoint_id: Uuid) -> bool {
        self.inner
            .lock()
            .active_snapshots
            .values()
            .any(|id| *id == Some(checkpoint_id))
    }

    /// Keeps `checkpoint_id`, which the reader replaced while a snapshot read it.
    pub(crate) fn retain(&self, checkpoint_id: Uuid) {
        self.inner.lock().retained_checkpoints.insert(checkpoint_id);
    }

    /// Stops keeping `checkpoint_id`, for example because it no longer exists.
    pub(crate) fn forget(&self, checkpoint_id: &Uuid) {
        self.inner.lock().retained_checkpoints.remove(checkpoint_id);
    }

    /// Splits the retained checkpoints into the ones that a live snapshot still
    /// reads and the ones that no snapshot reads. The second set is no longer
    /// retained.
    pub(crate) fn split_retained(&self) -> (Vec<Uuid>, Vec<Uuid>) {
        let mut inner = self.inner.lock();
        let read: HashSet<Uuid> = inner.active_snapshots.values().flatten().copied().collect();
        let (still_read, released): (Vec<Uuid>, Vec<Uuid>) = inner
            .retained_checkpoints
            .iter()
            .partition(|id| read.contains(id));
        for id in &released {
            inner.retained_checkpoints.remove(id);
        }
        (still_read, released)
    }

    /// Removes and returns every retained checkpoint.
    pub(crate) fn take_retained(&self) -> Vec<Uuid> {
        self.inner.lock().retained_checkpoints.drain().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> ReaderSnapshotManager {
        ReaderSnapshotManager::new(Arc::new(DbRand::new(0)))
    }

    #[test]
    fn test_is_read_follows_live_snapshots() {
        let mgr = manager();
        let checkpoint = Uuid::new_v4();
        assert!(!mgr.is_read(checkpoint));

        let first = mgr.new_snapshot(Some(checkpoint));
        let second = mgr.new_snapshot(Some(checkpoint));
        let unmanaged = mgr.new_snapshot(None);
        assert!(mgr.is_read(checkpoint));

        mgr.drop_snapshot(&first);
        assert!(mgr.is_read(checkpoint));
        mgr.drop_snapshot(&second);
        assert!(!mgr.is_read(checkpoint));
        mgr.drop_snapshot(&unmanaged);
    }

    #[test]
    fn test_split_retained_releases_checkpoints_no_snapshot_reads() {
        let mgr = manager();
        let (read, released) = (Uuid::new_v4(), Uuid::new_v4());
        let snapshot = mgr.new_snapshot(Some(read));
        mgr.retain(read);
        mgr.retain(released);

        assert_eq!(mgr.split_retained(), (vec![read], vec![released]));
        assert_eq!(mgr.split_retained(), (vec![read], vec![]));

        mgr.drop_snapshot(&snapshot);
        assert_eq!(mgr.split_retained(), (vec![], vec![read]));
        assert_eq!(mgr.split_retained(), (vec![], vec![]));
    }

    #[test]
    fn test_forget_and_take_retained() {
        let mgr = manager();
        let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
        mgr.retain(first);
        mgr.retain(second);
        mgr.forget(&first);
        assert_eq!(mgr.take_retained(), vec![second]);
        assert!(mgr.take_retained().is_empty());
    }

    #[test]
    #[should_panic]
    fn test_drop_snapshot_nonexistent_panics() {
        manager().drop_snapshot(&Uuid::new_v4());
    }
}
