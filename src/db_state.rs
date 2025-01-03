use crate::checkpoint::Checkpoint;
use bytes::Bytes;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use tracing::debug;
use ulid::Ulid;
use SsTableId::{Compacted, Wal};

use crate::bytes_range::BytesRange;
use crate::config::CompressionCodec;
use crate::error::SlateDBError;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, WritableKVTable};
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};

#[derive(Clone, PartialEq, Serialize)]
pub(crate) struct SsTableHandle {
    pub id: SsTableId,
    pub info: SsTableInfo,
}

impl Debug for SsTableHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("SsTableHandle({:?})", self.id))
    }
}

impl SsTableHandle {
    pub(crate) fn new(id: SsTableId, info: SsTableInfo) -> Self {
        SsTableHandle { id, info }
    }

    pub(crate) fn range_covers_key(&self, key: &[u8]) -> bool {
        if let Some(first_key) = self.info.first_key.as_ref() {
            return key >= first_key;
        }
        false
    }

    pub(crate) fn intersects_range(
        &self,
        end_bound_key: Option<Bytes>,
        range: &BytesRange,
    ) -> bool {
        let start_bound = match &self.info.first_key {
            None => Unbounded,
            Some(key) => Included(key),
        }
        .cloned();

        let end_bound = match end_bound_key {
            None => Unbounded,
            Some(end_bound_key) => Excluded(end_bound_key),
        };

        let this_range = BytesRange::new(start_bound, end_bound);
        let intersection = this_range.intersection(range);
        intersection.non_empty()
    }

    pub(crate) fn estimate_size(&self) -> u64 {
        // this is a hacky estimate of the sst size since we don't have it stored anywhere
        // right now. Just use the index's offset and add the index length. Since the index
        // is the last thing we put in the SST before the info footer, this should be a good
        // estimate for now.
        self.info.index_offset + self.info.index_len
    }
}

impl AsRef<SsTableHandle> for SsTableHandle {
    fn as_ref(&self) -> &SsTableHandle {
        self
    }
}

#[derive(Clone, PartialEq, Debug, Hash, Eq, Copy, Serialize)]
pub(crate) enum SsTableId {
    Wal(u64),
    Compacted(Ulid),
}

impl SsTableId {
    #[allow(clippy::panic)]
    pub(crate) fn unwrap_wal_id(&self) -> u64 {
        match self {
            Wal(wal_id) => *wal_id,
            Compacted(_) => panic!("found compacted id when unwrapping WAL ID"),
        }
    }

    #[allow(clippy::panic)]
    pub(crate) fn unwrap_compacted_id(&self) -> Ulid {
        match self {
            Wal(_) => panic!("found WAL id when unwrapping compacted ID"),
            Compacted(ulid) => *ulid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SsTableInfo {
    pub(crate) first_key: Option<Bytes>,
    pub(crate) index_offset: u64,
    pub(crate) index_len: u64,
    pub(crate) filter_offset: u64,
    pub(crate) filter_len: u64,
    pub(crate) compression_codec: Option<CompressionCodec>,
}

pub(crate) trait SsTableInfoCodec: Send + Sync {
    fn encode(&self, manifest: &SsTableInfo) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<SsTableInfo, SlateDBError>;

    fn clone_box(&self) -> Box<dyn SsTableInfoCodec>;
}

/// Implement Clone for Box<dyn SsTableInfoCodec> by delegating to the clone_box method.
/// This is the idiomatic way to clone trait objects in Rust. It is also the only way to
/// clone trait objects without knowing the concrete type.
impl Clone for Box<dyn SsTableInfoCodec> {
    fn clone(&self) -> Self {
        self.as_ref().clone_box()
    }
}

#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct SortedRun {
    pub(crate) id: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
}

impl SortedRun {
    pub(crate) fn estimate_size(&self) -> u64 {
        self.ssts.iter().map(|sst| sst.estimate_size()).sum()
    }

    pub(crate) fn find_sst_with_range_covering_key_idx(&self, key: &[u8]) -> Option<usize> {
        // returns the sst after the one whose range includes the key
        let first_sst = self.ssts.partition_point(|sst| {
            sst.info
                .first_key
                .as_ref()
                .expect("sst must have first key")
                <= key
        });
        if first_sst > 0 {
            return Some(first_sst - 1);
        }
        // all ssts have a range greater than the key
        None
    }

    pub(crate) fn find_sst_with_range_covering_key(&self, key: &[u8]) -> Option<&SsTableHandle> {
        self.find_sst_with_range_covering_key_idx(key)
            .map(|idx| &self.ssts[idx])
    }

    pub(crate) fn tables_covering_range(&self, range: &BytesRange) -> VecDeque<&SsTableHandle> {
        let mut covering_ssts = VecDeque::new();
        for idx in 0..self.ssts.len() {
            let current_sst = &self.ssts[idx];

            let upper_bound_key = if idx + 1 < self.ssts.len() {
                let next_sst = &self.ssts[idx + 1];
                next_sst.info.first_key.clone()
            } else {
                None
            };

            if current_sst.intersects_range(upper_bound_key, range) {
                covering_ssts.push_back(&self.ssts[idx]);
            }
        }
        covering_ssts
    }
}

pub(crate) struct DbState {
    memtable: WritableKVTable,
    wal: WritableKVTable,
    state: Arc<COWDbState>,
    error: WatchableOnceCell<SlateDBError>,
}

// represents the state that is mutated by creating a new copy with the mutations
#[derive(Clone)]
pub(crate) struct COWDbState {
    pub(crate) imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    pub(crate) imm_wal: VecDeque<Arc<ImmutableWal>>,
    pub(crate) core: CoreDbState,
}

// represents the core db state that we persist in the manifest
#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct CoreDbState {
    pub(crate) initialized: bool,
    pub(crate) l0_last_compacted: Option<Ulid>,
    pub(crate) l0: VecDeque<SsTableHandle>,
    pub(crate) compacted: Vec<SortedRun>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
    pub(crate) last_clock_tick: i64,
    pub(crate) last_seq: u64,
    pub(crate) checkpoints: Vec<Checkpoint>,
}

impl CoreDbState {
    pub(crate) fn new() -> Self {
        Self {
            initialized: true,
            l0_last_compacted: None,
            l0: VecDeque::new(),
            compacted: vec![],
            next_wal_sst_id: 1,
            last_compacted_wal_sst_id: 0,
            last_clock_tick: i64::MIN,
            last_seq: 0,
            checkpoints: vec![],
        }
    }

    pub(crate) fn log_db_runs(&self) {
        let l0s: Vec<_> = self.l0.iter().map(|l0| l0.estimate_size()).collect();
        let compacted: Vec<_> = self
            .compacted
            .iter()
            .map(|sr| (sr.id, sr.estimate_size()))
            .collect();
        debug!("DB Levels:");
        debug!("-----------------");
        debug!("{:?}", l0s);
        debug!("{:?}", compacted);
        debug!("-----------------");
    }
}

// represents a read-snapshot of the current db state
#[derive(Clone)]
pub(crate) struct DbStateSnapshot {
    pub(crate) memtable: Arc<KVTable>,
    pub(crate) wal: Arc<KVTable>,
    pub(crate) state: Arc<COWDbState>,
}

impl DbState {
    pub fn new(core_db_state: CoreDbState) -> Self {
        Self {
            memtable: WritableKVTable::new(),
            wal: WritableKVTable::new(),
            state: Arc::new(COWDbState {
                imm_memtable: VecDeque::new(),
                imm_wal: VecDeque::new(),
                core: core_db_state,
            }),
            error: WatchableOnceCell::new(),
        }
    }

    pub fn state(&self) -> Arc<COWDbState> {
        self.state.clone()
    }

    pub fn snapshot(&self) -> DbStateSnapshot {
        DbStateSnapshot {
            memtable: self.memtable.table().clone(),
            wal: self.wal.table().clone(),
            state: self.state.clone(),
        }
    }

    pub fn last_written_wal_id(&self) -> u64 {
        assert!(self.state.core.next_wal_sst_id > 0);
        self.state.core.next_wal_sst_id - 1
    }

    pub fn error_reader(&self) -> WatchableOnceCellReader<SlateDBError> {
        self.error.reader()
    }

    // mutations
    pub fn record_fatal_error(&mut self, error: SlateDBError) {
        self.error.write(error);
    }

    pub fn wal(&mut self) -> &mut WritableKVTable {
        &mut self.wal
    }

    pub fn memtable(&mut self) -> &mut WritableKVTable {
        &mut self.memtable
    }

    fn state_copy(&self) -> COWDbState {
        self.state.as_ref().clone()
    }

    fn update_state(&mut self, state: COWDbState) {
        self.state = Arc::new(state);
    }

    pub fn freeze_memtable(&mut self, wal_id: u64) -> Result<(), SlateDBError> {
        if let Some(err) = self.error.reader().read() {
            return Err(err.clone());
        }
        let old_memtable = std::mem::replace(&mut self.memtable, WritableKVTable::new());
        let mut state = self.state_copy();
        state
            .imm_memtable
            .push_front(Arc::new(ImmutableMemtable::new(old_memtable, wal_id)));
        self.update_state(state);
        Ok(())
    }

    pub fn freeze_wal(&mut self) -> Result<Option<u64>, SlateDBError> {
        if let Some(err) = self.error.reader().read() {
            return Err(err.clone());
        }
        if self.wal.table().is_empty() {
            return Ok(None);
        }
        let old_wal = std::mem::replace(&mut self.wal, WritableKVTable::new());
        let mut state = self.state_copy();
        let imm_wal = Arc::new(ImmutableWal::new(state.core.next_wal_sst_id, old_wal));
        let id = imm_wal.id();
        state.imm_wal.push_front(imm_wal);
        state.core.next_wal_sst_id += 1;
        self.update_state(state);
        Ok(Some(id))
    }

    pub fn pop_imm_wal(&mut self) {
        let mut state = self.state_copy();
        state.imm_wal.pop_back();
        self.update_state(state);
    }

    pub fn move_imm_memtable_to_l0(
        &mut self,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
    ) {
        let mut state = self.state_copy();
        let popped = state
            .imm_memtable
            .pop_back()
            .expect("expected imm memtable");
        assert!(Arc::ptr_eq(&popped, &imm_memtable));
        state.core.l0.push_front(sst_handle);
        state.core.last_compacted_wal_sst_id = imm_memtable.last_wal_id();
        self.update_state(state);
    }

    pub fn increment_next_wal_id(&mut self) {
        let mut state = self.state_copy();
        state.core.next_wal_sst_id += 1;
        self.update_state(state);
    }

    pub fn update_clock_tick(&mut self, tick: i64) -> Result<i64, SlateDBError> {
        if self.state.core.last_clock_tick > tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: self.state.core.last_clock_tick,
                next_tick: tick,
            });
        }

        let mut state = self.state_copy();
        state.core.last_clock_tick = tick;
        self.update_state(state);
        Ok(tick)
    }

    pub fn increment_seq(&mut self) -> u64 {
        let mut state = self.state_copy();
        let last_seq = state.core.last_seq;
        state.core.last_seq += 1;
        self.update_state(state);
        last_seq + 1
    }

    pub fn update_last_seq(&mut self, seq: u64) {
        let mut state = self.state_copy();
        state.core.last_seq = seq;
        self.update_state(state);
    }

    pub fn refresh_db_state(&mut self, compactor_state: &CoreDbState) {
        // copy over L0 up to l0_last_compacted
        let l0_last_compacted = &compactor_state.l0_last_compacted;
        let mut new_l0 = VecDeque::new();
        for sst in self.state.core.l0.iter() {
            if let Some(l0_last_compacted) = l0_last_compacted {
                if sst.id.unwrap_compacted_id() == *l0_last_compacted {
                    break;
                }
            }
            new_l0.push_back(sst.clone());
        }
        let compacted = compactor_state.compacted.clone();
        let mut state = self.state_copy();
        state.core.l0_last_compacted.clone_from(l0_last_compacted);
        state.core.l0 = new_l0;
        state.core.compacted = compacted;
        self.update_state(state);
    }
}

#[cfg(test)]
mod tests {
    use crate::db_state::{CoreDbState, DbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::proptest_util::arbitrary;
    use bytes::Bytes;
    use proptest::collection::vec;
    use proptest::proptest;
    use std::collections::BTreeSet;
    use std::collections::Bound::Included;
    use std::ops::RangeBounds;
    use ulid::Ulid;

    #[test]
    fn test_should_refresh_db_state_with_l0s_up_to_last_compacted() {
        // given:
        let mut db_state = DbState::new(CoreDbState::new());
        add_l0s_to_dbstate(&mut db_state, 4);
        // mimic the compactor popping off l0s
        let mut compactor_state = db_state.state.core.clone();
        let last_compacted = compactor_state.l0.pop_back().unwrap();
        compactor_state.l0_last_compacted = Some(last_compacted.id.unwrap_compacted_id());

        // when:
        db_state.refresh_db_state(&compactor_state);

        // then:
        let expected: Vec<SsTableId> = compactor_state.l0.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core.l0.iter().map(|l0| l0.id).collect();
        assert_eq!(expected, merged);
    }

    #[test]
    fn test_should_refresh_db_state_with_all_l0s_if_none_compacted() {
        // given:
        let mut db_state = DbState::new(CoreDbState::new());
        add_l0s_to_dbstate(&mut db_state, 4);
        let l0s = db_state.state.core.l0.clone();

        // when:
        db_state.refresh_db_state(&CoreDbState::new());

        // then:
        let expected: Vec<SsTableId> = l0s.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core.l0.iter().map(|l0| l0.id).collect();
        assert_eq!(expected, merged);
    }

    fn add_l0s_to_dbstate(db_state: &mut DbState, n: u32) {
        let dummy_info = create_sst_info(None);
        for i in 0..n {
            db_state
                .freeze_memtable(i as u64)
                .expect("db in error state");
            let imm = db_state.state.imm_memtable.back().unwrap().clone();
            let handle = SsTableHandle::new(SsTableId::Compacted(Ulid::new()), dummy_info.clone());
            db_state.move_imm_memtable_to_l0(imm, handle);
        }
    }

    #[test]
    fn test_sorted_run_collect_tables_in_range() {
        let max_bytes_len = 5;
        proptest!(|(
            table_first_keys in vec(arbitrary::nonempty_bytes(max_bytes_len), 1..10),
            range in arbitrary::nonempty_range(max_bytes_len),
        )| {
            let sorted_first_keys: BTreeSet<Bytes> = table_first_keys.into_iter().collect();
            let sorted_run = create_sorted_run(0, &sorted_first_keys);
            let covering_tables = sorted_run.tables_covering_range(&range);
            let first_key = sorted_first_keys.first().unwrap().clone();

            if covering_tables.is_empty() {
                let end_bound =  range.end_bound_opt().unwrap();
                assert!(end_bound <= first_key);
            } else {
                let range_start_key = range.start_bound_opt().unwrap_or(Bytes::new());
                let covering_first_key = covering_tables.front()
                .and_then(|t| t.info.first_key.clone())
                .unwrap();

                if range_start_key < covering_first_key {
                    assert_eq!(covering_first_key, first_key)
                }

                let range_end_key: Bytes = range.end_bound_opt()
                .unwrap_or(vec![u8::MAX; max_bytes_len + 1].into());

                let covering_last_key = covering_tables.iter().last()
                .and_then(|t| t.info.first_key.clone())
                .unwrap();
                if covering_last_key == range_end_key {
                    assert_eq!(Included(range_end_key), range.end_bound().cloned());
                } else {
                    assert!(covering_last_key < range_end_key);
                }
            }
        });
    }

    fn create_sorted_run(id: u32, first_keys: &BTreeSet<Bytes>) -> SortedRun {
        let mut ssts = Vec::new();
        for first_key in first_keys {
            ssts.push(create_compacted_sst_handle(Some(first_key.clone())));
        }
        SortedRun { id, ssts }
    }

    fn create_compacted_sst_handle(first_key: Option<Bytes>) -> SsTableHandle {
        let sst_info = create_sst_info(first_key);
        let sst_id = SsTableId::Compacted(Ulid::new());
        SsTableHandle::new(sst_id, sst_info.clone())
    }

    fn create_sst_info(first_key: Option<Bytes>) -> SsTableInfo {
        SsTableInfo {
            first_key,
            index_offset: 0,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
        }
    }
}
