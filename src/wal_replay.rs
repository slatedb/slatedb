use crate::db_state::{CoreDbState, SsTableId};
use crate::iter::KeyValueIterator;
use crate::mem_table::WritableKVTable;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::SlateDBError;
use std::cmp;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct WalReplayOptions {
    pub(crate) sst_batch_size: usize,
    pub(crate) min_memtable_bytes: usize,
    pub(crate) sst_iter_options: SstIteratorOptions,
}

pub(crate) struct ReplayedMemtable {
    pub(crate) table: WritableKVTable,
    pub(crate) last_tick: i64,
    pub(crate) last_seq: u64,
    pub(crate) last_wal_id: u64,
}

pub(crate) struct WalReplayIterator<'a> {
    options: WalReplayOptions,
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    next_iters: VecDeque<SstIterator<'a>>,
    last_tick: i64,
    last_seq: u64,
    next_wal_id: u64,
}

impl<'a> WalReplayIterator<'a> {

    pub(crate) async fn new(
        db_state: &CoreDbState,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError>  {
        let wal_id_start = db_state.last_compacted_wal_sst_id + 1;
        let wal_id_end = table_store.last_seen_wal_id().await?;
        let sst_batch_size = options.sst_batch_size;

        // load the last seq number from manifest, and use it as the starting seq number.
        // there might have bigger seq number in the WALs, we'd update the last seq number
        // to the max seq number while iterating over the WALs.
        let last_seq = db_state.last_l0_seq;
        let last_tick = db_state.last_l0_clock_tick;

        let mut replay_iter = WalReplayIterator {
            options,
            wal_id_range: wal_id_start..(wal_id_end + 1),
            table_store: Arc::clone(&table_store),
            next_iters: VecDeque::new(),
            last_tick,
            last_seq,
            next_wal_id: wal_id_start
        };

        for _ in 0..sst_batch_size {
            if !replay_iter.load_next_sst_iter().await? {
                break;
            }
        }

        Ok(replay_iter)
    }

    async fn load_next_sst_iter(&mut self) -> Result<bool, SlateDBError> {
        if !self.wal_id_range.contains(&self.next_wal_id) {
            return Ok(false)
        }

        let next_wal_id = self.next_wal_id;
        self.next_wal_id += 1;

        let sst = self.table_store
            .open_sst(&SsTableId::Wal(next_wal_id))
            .await?;
        let sst_iter = SstIterator::new_owned(
            ..,
            sst,
            Arc::clone(&self.table_store),
            self.options.sst_iter_options.clone(),
        ).await?;
        self.next_iters.push_back(sst_iter);
        Ok(true)
    }

    pub(crate) async fn next(&mut self) -> Result<Option<ReplayedMemtable>, SlateDBError> {
        if self.next_iters.is_empty() {
            return Ok(None)
        }

        let mut table = WritableKVTable::new();
        let mut last_tick = self.last_tick;
        let mut last_seq = self.last_seq;
        let mut last_wal_id = 0;

        while let Some(mut sst_iter) = self.next_iters.pop_front() {
            last_wal_id = sst_iter.table_id().unwrap_wal_id();

            while let Some(row_entry) = sst_iter.next_entry().await? {
                if let Some(ts) = row_entry.create_ts {
                    last_tick = cmp::max(last_tick, ts);
                }
                last_seq = last_seq.max(row_entry.seq);
                table.put(row_entry);
            }

            self.load_next_sst_iter().await?;

            if table.size() > self.options.min_memtable_bytes {
                break;
            }
        }

        self.last_tick = last_tick;
        self.last_seq = last_seq;
        Ok(Some(ReplayedMemtable {
            table,
            last_tick,
            last_seq,
            last_wal_id
        }))
    }

}

