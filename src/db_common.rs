use crate::db::DbInner;
use crate::db::DbState;
use crate::mem_table::{ImmutableMemtable, WritableKVTable};
use crate::mem_table_flush::MemtableFlushThreadMsg::FlushImmutableMemtables;
use parking_lot::RwLockWriteGuard;
use std::sync::Arc;

impl DbInner {
    pub(crate) fn maybe_freeze_memtable(&self, guard: &mut RwLockWriteGuard<DbState>, wal_id: u64) {
        if guard.memtable.size() < self.options.l0_sst_size_bytes {
            return;
        }
        self.freeze_memtable(guard, wal_id);
    }

    fn freeze_memtable(&self, guard: &mut RwLockWriteGuard<DbState>, wal_id: u64) {
        let old_memtable = std::mem::replace(&mut guard.memtable, WritableKVTable::new());
        let mut compacted_snapshot = guard.compacted.as_ref().clone();
        compacted_snapshot
            .imm_memtable
            .push_front(Arc::new(ImmutableMemtable::new(old_memtable, wal_id)));
        guard.compacted = Arc::new(compacted_snapshot);
        self.memtable_flush_notifier
            .send(FlushImmutableMemtables)
            .expect("failed to send memtable flush msg");
    }
}
