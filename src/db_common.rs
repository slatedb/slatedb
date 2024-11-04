use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::flush::FlushThreadMsg;

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
            .send(FlushThreadMsg::Flush(None))?;
        Ok(())
    }

    pub(crate) fn maybe_freeze_wal(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
    ) -> Result<(), SlateDBError> {
        if guard.wal().size() < 64 * 1024 * 1024 {
            return Ok(());
        }
        guard.freeze_wal();
        self.wal_flush_notifier.send(FlushThreadMsg::Flush(None))?;
        Ok(())
    }
}
