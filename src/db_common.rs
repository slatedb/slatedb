use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::flush::WalFlushThreadMsg;
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
            .send(MemtableFlushThreadMsg::FlushImmutableMemtables(None))
            .map_err(|_| SlateDBError::MemtableFlushChannelError)?;
        Ok(())
    }

    pub(crate) fn maybe_freeze_wal(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
    ) -> Result<(), SlateDBError> {
        // Use L0 SST size as the threshold for freezing a WAL table because
        // a single WAL table gets added to a single L0 SST. If the WAL table
        // were allowed to grow larger than the L0 SST threshold, the L0 SST
        // size would be greater than the threshold.
        if guard.wal().size() < self.options.l0_sst_size_bytes {
            return Ok(());
        }
        guard.freeze_wal();
        self.wal_flush_notifier
            .send(WalFlushThreadMsg::FlushImmutableWals(None))
            .map_err(|_| SlateDBError::WalFlushChannelError)?;
        Ok(())
    }
}
