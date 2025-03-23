use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::mem_table_flush::MemtableFlushMsg;
use crate::wal_replay::ReplayedMemtable;

impl DbInner {
    pub(crate) fn maybe_freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) -> Result<(), SlateDBError> {
        if guard.memtable().size() < self.options.l0_sst_size_bytes {
            Ok(())
        } else {
            self.freeze_memtable(guard, wal_id)
        }
    }

    pub(crate) fn freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) -> Result<(), SlateDBError> {
        if guard.memtable().is_empty() {
            return Ok(());
        }

        guard.freeze_memtable(wal_id)?;
        self.memtable_flush_notifier
            .send(MemtableFlushMsg::FlushImmutableMemtables { sender: None })
            .map_err(|_| SlateDBError::MemtableFlushChannelError)?;
        Ok(())
    }

    pub(crate) fn replay_memtable(
        &self,
        replayed_memtable: ReplayedMemtable,
    ) -> Result<(), SlateDBError> {
        let mut guard = self.state.write();
        let last_wal_id = guard.last_written_wal_id();
        self.freeze_memtable(&mut guard, last_wal_id)?;

        let last_wal_id = replayed_memtable.last_wal_id;
        guard.set_next_wal_id(last_wal_id + 1);
        guard.update_last_seq(replayed_memtable.last_seq);
        self.mono_clock.set_last_tick(replayed_memtable.last_tick)?;
        guard.replace_memtable(replayed_memtable.table)
    }
}
