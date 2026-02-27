use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::mem_table_flush::MemtableFlushMsg;
use crate::oracle::Oracle;
use crate::utils::SendSafely;
use crate::wal_replay::ReplayedMemtable;

pub(crate) const MAX_WAL_FLUSHES_BEFORE_L0_FLUSH: u64 = 4096;

impl DbInner {
    pub(crate) fn maybe_freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) -> Result<(), SlateDBError> {
        let meta = guard.memtable().metadata();

        let last_freeze_wal_id = guard
            .state()
            .imm_memtable
            .front()
            .map(|imm| imm.recent_flushed_wal_id())
            .unwrap_or(guard.state().core().replay_after_wal_id);

        let l0_sst_size_est = self
            .table_store
            .estimate_encoded_size_compacted(meta.entry_num, meta.entries_size_in_bytes);

        if (wal_id - last_freeze_wal_id) < MAX_WAL_FLUSHES_BEFORE_L0_FLUSH
            && l0_sst_size_est < self.settings.l0_sst_size_bytes
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
        self.memtable_flush_notifier.send_safely(
            guard.closed_result_reader(),
            MemtableFlushMsg::FlushImmutableMemtables { sender: None },
        )?;
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
        guard.modify(|modifier| modifier.state.manifest.value.core.next_wal_sst_id = last_wal + 1);

        // update seqs and clock
        // we know these won't move backwards (even though the replayed wal files might contain some
        // older rows) because the wal replay iterator ignores any entries with seq num lower than
        // l0_last_seq from the manifest
        assert!(self.oracle.last_seq() <= replayed_memtable.last_seq);
        self.oracle.advance_last_seq(replayed_memtable.last_seq);
        assert!(self.oracle.last_committed_seq() <= replayed_memtable.last_seq);
        self.oracle
            .advance_committed_seq(replayed_memtable.last_seq);
        self.mono_clock.set_last_tick(replayed_memtable.last_tick)?;

        // replace the memtable
        guard.replace_memtable(replayed_memtable.table)
    }
}
