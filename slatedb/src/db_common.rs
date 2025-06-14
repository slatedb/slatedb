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
        let meta = guard.memtable().metadata();
        if self
            .table_store
            .estimate_encoded_size(meta.entry_num, meta.entries_size_in_bytes)
            < self.settings.l0_sst_size_bytes
        {
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

        // a WAL might contain the data across multiple memtables. we can only consider
        // last_wal_id - 1 as the recent persisted wal id when the memtable is reconstructed.
        // or when we need to replay again, we might risks to lose some WAL entries.
        let recent_flushed_wal_id = if replayed_memtable.last_wal_id > 0 {
            replayed_memtable.last_wal_id - 1
        } else {
            0
        };
        self.freeze_memtable(&mut guard, recent_flushed_wal_id)?;

        let last_wal = replayed_memtable.last_wal_id;
        guard.set_next_wal_id(last_wal + 1);

        // update seqs and clock
        self.oracle.last_seq.store(replayed_memtable.last_seq);
        self.oracle
            .last_committed_seq
            .store(replayed_memtable.last_seq);
        self.mono_clock.set_last_tick(replayed_memtable.last_tick)?;

        // replace the memtable
        guard.replace_memtable(replayed_memtable.table)
    }
}
