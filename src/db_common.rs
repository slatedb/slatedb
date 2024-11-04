use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::mem_table_flush::MemtableFlushThreadMsg;
impl DbInner {
    pub(crate) fn maybe_freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) -> Result<(), SlateDBError> {
        if guard.memtable().size() < self.options.l0_sst_size_bytes {
            return Ok(());
        }
        guard.freeze_memtable(wal_id);
        self.memtable_flush_notifier
            .send(MemtableFlushThreadMsg::FlushImmutableMemtables(None))?;
        Ok(())
    }
}
