use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::mem_table_flush::MemtableFlushThreadMsg::FlushImmutableMemtables;

impl DbInner {
    pub(crate) fn maybe_freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) {
        let (total_size_bytes, num_keys) = {
            let memtable = guard.memtable();
            (memtable.total_size_bytes(), memtable.num_entries())
        };
        let estimated_sst_size = self
            .sst_format
            .estimate_sst_size(num_keys, total_size_bytes);
        if estimated_sst_size < self.options.l0_sst_size_bytes {
            return;
        }
        guard.freeze_memtable(wal_id);
        self.memtable_flush_notifier
            .send(FlushImmutableMemtables(None))
            .expect("failed to send memtable flush msg");
    }
}
