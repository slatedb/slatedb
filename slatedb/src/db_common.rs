use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use log::{debug, warn};
use parking_lot::RwLockWriteGuard;

use crate::db::DbInner;
use crate::db_state::DbState;
use crate::error::SlateDBError;
use crate::manifest::{ManifestCore, SsTableHandle, SsTableId};
use crate::mem_table_flush::MemtableFlushMsg;
use crate::utils::{IdGenerator, SendSafely};
use crate::wal_replay::{ReplayedMemtable, WalReplayIterator, WalReplayOptions};

pub(crate) struct WalToL0Result {
    pub(crate) sst_handles: VecDeque<SsTableHandle>,
    pub(crate) last_tick: Option<i64>,
    pub(crate) last_seq: Option<u64>,
}

impl DbInner {
    pub(crate) fn maybe_freeze_memtable(
        &self,
        guard: &mut RwLockWriteGuard<'_, DbState>,
        wal_id: u64,
    ) -> Result<(), SlateDBError> {
        let meta = guard.memtable().metadata();
        if self
            .table_store
            .estimate_encoded_size_compacted(meta.entry_num, meta.entries_size_in_bytes)
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
        assert!(self.oracle.last_seq.load() <= replayed_memtable.last_seq);
        self.oracle.last_seq.store(replayed_memtable.last_seq);
        assert!(self.oracle.last_committed_seq.load() <= replayed_memtable.last_seq);
        self.oracle
            .last_committed_seq
            .store(replayed_memtable.last_seq);
        self.mono_clock.set_last_tick(replayed_memtable.last_tick)?;

        // replace the memtable
        guard.replace_memtable(replayed_memtable.table)
    }

    // Replays WALs in the id range into SSTs and flushes them to l0.
    // Returns the handles of the newly generated and flushed SSTs,
    // the latest sequence number and latest creation timestamp of the entries
    pub(crate) async fn replay_wal_to_l0(
        &self,
        wal_id_range: Range<u64>,
        core: &ManifestCore,
    ) -> Result<WalToL0Result, SlateDBError> {
        let mut result = WalToL0Result {
            sst_handles: VecDeque::new(),
            last_tick: None,
            last_seq: None,
        };

        if wal_id_range.is_empty() {
            return Ok(result);
        }

        debug!("replaying wal_id_range {:?} to l0", &wal_id_range);

        let wal_replay_options = WalReplayOptions {
            min_memtable_bytes: self.settings.l0_sst_size_bytes,
            ..WalReplayOptions::default()
        };

        let mut replay_iter = WalReplayIterator::range(
            wal_id_range,
            core,
            wal_replay_options,
            Arc::clone(&self.table_store),
        )
        .await?;

        while let Some(replayed_table) = replay_iter.next().await? {
            if replayed_table.table.is_empty() {
                continue;
            }

            let id = SsTableId::Compacted(self.rand.rng().gen_ulid(self.system_clock.as_ref()));
            let l0_len = self.state.read().state().core().l0.len();
            if l0_len + 1 >= self.settings.l0_max_ssts {
                warn!(
                    "too many l0 files [l0_len={}, l0_max_ssts={}]. flushing of replayed_table to l0 sst (id={}) will still run.",
                    l0_len,
                    self.settings.l0_max_ssts,
                    id.unwrap_compacted_id(),
                );
            }
            let iter = replayed_table.table.table().iter();
            let handle = self.flush_table_iter(&id, iter, false).await?;
            result.sst_handles.push_front(handle);

            result.last_seq = Some(std::cmp::max(
                result.last_seq.unwrap_or(replayed_table.last_seq),
                replayed_table.last_seq,
            ));
            result.last_tick = Some(std::cmp::max(
                result.last_tick.unwrap_or(replayed_table.last_tick),
                replayed_table.last_tick,
            ));
        }

        debug!("replayed WALs to new L0 SSTs: {:?}", result.sst_handles);

        Ok(result)
    }
}
