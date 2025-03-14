use crate::bytes_range;
use crate::checkpoint::Checkpoint;
use crate::config::CompressionCodec;
use crate::error::SlateDBError;
use crate::manifest::store::DirtyManifest;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, WritableKVTable};
use crate::reader::ReadSnapshot;
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};
use bytes::Bytes;
use serde::Serialize;
use std::cmp;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, Range};
use std::sync::Arc;
use tracing::debug;
use ulid::Ulid;
use uuid::Uuid;
use SsTableId::{Compacted, Wal};

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

    // Compacted (non-WAL) SSTs are never empty. They are created by compaction or
    // memtable flushes, which should never produce empty SSTs.
    pub(crate) fn compacted_first_key(&self) -> &Bytes {
        assert!(matches!(self.id, Compacted(_)));
        match &self.info.first_key {
            Some(k) => k,
            None => unreachable!("Compacted SSTs must be non-empty."),
        }
    }

    pub(crate) fn range_covers_key(&self, key: &[u8]) -> bool {
        if let Some(first_key) = self.info.first_key.as_ref() {
            return key >= first_key;
        }
        // If there is no first key, it means the SST is empty so it doesn't cover the key.
        false
    }

    pub(crate) fn intersects_range(
        &self,
        end_bound: Bound<&[u8]>,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> bool {
        let start_bound = match &self.info.first_key {
            Some(key) => Included(key.as_ref()),
            None => {
                // If there is no first key, it means the SST is empty so there is no intersection.
                return false;
            }
        };
        bytes_range::has_nonempty_intersection(range, (start_bound, end_bound))
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
        let first_sst = self
            .ssts
            .partition_point(|sst| sst.compacted_first_key() <= key);
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

    fn table_idx_covering_range(&self, range: (Bound<&[u8]>, Bound<&[u8]>)) -> Range<usize> {
        let mut min_idx = None;
        let mut max_idx = 0;

        for idx in 0..self.ssts.len() {
            let current_sst = &self.ssts[idx];

            let upper_bound_key = if idx + 1 < self.ssts.len() {
                let next_sst = &self.ssts[idx + 1];
                Excluded(next_sst.compacted_first_key().as_ref())
            } else {
                Unbounded
            };

            if current_sst.intersects_range(upper_bound_key, range) {
                if min_idx.is_none() {
                    min_idx = Some(idx);
                }

                max_idx = idx;
            }
        }

        match min_idx {
            Some(min_idx) => min_idx..(max_idx + 1),
            None => 0..0,
        }
    }

    pub(crate) fn tables_covering_range(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> VecDeque<&SsTableHandle> {
        let matching_range = self.table_idx_covering_range(range);
        self.ssts[matching_range].iter().collect()
    }

    pub(crate) fn into_tables_covering_range(
        mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> VecDeque<SsTableHandle> {
        let matching_range = self.table_idx_covering_range(range);
        self.ssts.drain(matching_range).collect()
    }
}

pub(crate) struct DbState {
    memtable: WritableKVTable,
    wal: WritableKVTable,
    state: Arc<COWDbState>,
    last_seq: u64,
    error: WatchableOnceCell<SlateDBError>,
}

// represents the state that is mutated by creating a new copy with the mutations
#[derive(Clone)]
pub(crate) struct COWDbState {
    pub(crate) imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    pub(crate) imm_wal: VecDeque<Arc<ImmutableWal>>,
    pub(crate) manifest: DirtyManifest,
}

impl COWDbState {
    pub(crate) fn core(&self) -> &CoreDbState {
        &self.manifest.core
    }
}

/// represent the in-memory state of the manifest
#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct CoreDbState {
    pub(crate) initialized: bool,
    pub(crate) l0_last_compacted: Option<Ulid>,
    pub(crate) l0: VecDeque<SsTableHandle>,
    pub(crate) compacted: Vec<SortedRun>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
    /// the `last_l0_clock_tick` includes all data in L0 and below --
    /// WAL entries will have their latest ticks recovered on replay
    /// into the in-memory state
    pub(crate) last_l0_clock_tick: i64,
    /// it's persisted in the manifest, and only updated when a new L0
    /// SST is created in the manifest.
    pub(crate) last_l0_seq: u64,
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
            last_l0_clock_tick: i64::MIN,
            last_l0_seq: 0,
            checkpoints: vec![],
        }
    }

    pub(crate) fn init_clone_db(&self) -> CoreDbState {
        let mut clone = self.clone();
        clone.initialized = false;
        clone.checkpoints.clear();
        clone
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

    pub(crate) fn find_checkpoint(&self, checkpoint_id: Uuid) -> Option<&Checkpoint> {
        self.checkpoints.iter().find(|c| c.id == checkpoint_id)
    }
}

// represents a read-snapshot of the current db state
#[derive(Clone)]
pub(crate) struct DbStateSnapshot {
    pub(crate) memtable: Arc<KVTable>,
    pub(crate) wal: Arc<KVTable>,
    pub(crate) state: Arc<COWDbState>,
}

impl ReadSnapshot for DbStateSnapshot {
    fn memtable(&self) -> Arc<KVTable> {
        Arc::clone(&self.memtable)
    }

    fn wal(&self) -> Arc<KVTable> {
        Arc::clone(&self.wal)
    }

    fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>> {
        &self.state.imm_memtable
    }

    fn imm_wal(&self) -> &VecDeque<Arc<ImmutableWal>> {
        &self.state.imm_wal
    }

    fn core(&self) -> &CoreDbState {
        self.state.core()
    }
}

impl DbState {
    pub fn new(manifest: DirtyManifest) -> Self {
        let last_l0_seq = manifest.core.last_l0_seq;
        Self {
            memtable: WritableKVTable::new(),
            wal: WritableKVTable::new(),
            state: Arc::new(COWDbState {
                imm_memtable: VecDeque::new(),
                imm_wal: VecDeque::new(),
                manifest,
            }),
            error: WatchableOnceCell::new(),
            last_seq: last_l0_seq,
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
        assert!(self.state.core().next_wal_sst_id > 0);
        self.state.core().next_wal_sst_id - 1
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

    pub(crate) fn replace_memtable(
        &mut self,
        memtable: WritableKVTable,
    ) -> Result<(), SlateDBError> {
        if let Some(err) = self.error.reader().read() {
            return Err(err.clone());
        }
        assert!(self.memtable.is_empty());
        let _ = std::mem::replace(&mut self.memtable, memtable);
        Ok(())
    }

    pub fn freeze_wal(&mut self) -> Result<(), SlateDBError> {
        if let Some(err) = self.error.reader().read() {
            return Err(err.clone());
        }
        if self.wal.table().is_empty() {
            return Ok(());
        }
        let old_wal = std::mem::replace(&mut self.wal, WritableKVTable::new());
        let mut state = self.state_copy();
        let imm_wal = Arc::new(ImmutableWal::new(old_wal));
        state.imm_wal.push_front(imm_wal);
        self.update_state(state);
        Ok(())
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
    ) -> Result<(), SlateDBError> {
        let mut state = self.state_copy();
        let popped = state
            .imm_memtable
            .pop_back()
            .expect("expected imm memtable");
        assert!(Arc::ptr_eq(&popped, &imm_memtable));
        state.manifest.core.l0.push_front(sst_handle);
        state.manifest.core.last_compacted_wal_sst_id = imm_memtable.last_wal_id();

        // ensure the persisted manifest tick never goes backwards in time
        let memtable_tick = imm_memtable.table().last_tick();
        state.manifest.core.last_l0_clock_tick =
            cmp::max(state.manifest.core.last_l0_clock_tick, memtable_tick);
        if state.manifest.core.last_l0_clock_tick != memtable_tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: state.manifest.core.last_l0_clock_tick,
                next_tick: memtable_tick,
            });
        }
        // update the persisted manifest last_l0_seq as the latest seq in the imm.
        if let Some(seq) = imm_memtable.table().last_seq() {
            state.manifest.core.last_l0_seq = seq;
        }

        self.update_state(state);
        Ok(())
    }

    pub fn increment_next_wal_id(&mut self) {
        let mut state = self.state_copy();
        state.manifest.core.next_wal_sst_id += 1;
        self.update_state(state);
    }

    pub fn set_next_wal_id(&mut self, next_wal_id: u64) {
        let mut state = self.state_copy();
        state.manifest.core.next_wal_sst_id = next_wal_id;
        self.update_state(state);
    }

    /// increment_seq is called whenever a new write is performed.
    pub fn increment_seq(&mut self) -> u64 {
        self.last_seq += 1;
        self.last_seq
    }

    /// update_last_seq is called when we replay the WALs to recover the
    /// latest sequence number.
    pub fn update_last_seq(&mut self, seq: u64) {
        self.last_seq = seq;
    }

    pub fn merge_remote_manifest(&mut self, mut remote_manifest: DirtyManifest) {
        // The compactor removes tables from l0_last_compacted, so we
        // only want to keep the tables up to there.
        let l0_last_compacted = &remote_manifest.core.l0_last_compacted;
        let new_l0 = if let Some(l0_last_compacted) = l0_last_compacted {
            self.state
                .manifest
                .core
                .l0
                .iter()
                .cloned()
                .take_while(|sst| sst.id.unwrap_compacted_id() != *l0_last_compacted)
                .collect()
        } else {
            self.state.manifest.core.l0.iter().cloned().collect()
        };

        let mut state = self.state_copy();
        let my_db_state = state.core();
        remote_manifest.core = CoreDbState {
            initialized: my_db_state.initialized,
            l0_last_compacted: remote_manifest.core.l0_last_compacted,
            l0: new_l0,
            compacted: remote_manifest.core.compacted,
            next_wal_sst_id: my_db_state.next_wal_sst_id,
            last_compacted_wal_sst_id: my_db_state.last_compacted_wal_sst_id,
            last_l0_clock_tick: my_db_state.last_l0_clock_tick,
            last_l0_seq: my_db_state.last_l0_seq,
            checkpoints: remote_manifest.core.checkpoints,
        };
        state.manifest = remote_manifest;
        self.update_state(state);
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::Checkpoint;
    use crate::db_state::{DbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::proptest_util::arbitrary;
    use crate::test_utils;
    use bytes::Bytes;
    use proptest::collection::vec;
    use proptest::proptest;
    use std::collections::BTreeSet;
    use std::collections::Bound::Included;
    use std::ops::RangeBounds;
    use std::time::SystemTime;
    use ulid::Ulid;
    use uuid::Uuid;

    #[test]
    fn test_should_merge_db_state_with_new_checkpoints() {
        // given:
        let mut db_state = DbState::new(new_dirty_manifest());
        // mimic an externally added checkpoint
        let mut updated_state = new_dirty_manifest();
        updated_state.core = db_state.state.core().clone();
        let checkpoint = Checkpoint {
            id: Uuid::new_v4(),
            manifest_id: 1,
            expire_time: None,
            create_time: SystemTime::now(),
        };
        updated_state.core.checkpoints.push(checkpoint.clone());

        // when:
        db_state.merge_remote_manifest(updated_state);

        // then:
        assert_eq!(vec![checkpoint], db_state.state.core().checkpoints);
    }

    #[test]
    fn test_should_merge_db_state_with_l0s_up_to_last_compacted() {
        // given:
        let mut db_state = DbState::new(new_dirty_manifest());
        add_l0s_to_dbstate(&mut db_state, 4);
        // mimic the compactor popping off l0s
        let mut compactor_state = new_dirty_manifest();
        compactor_state.core = db_state.state.core().clone();
        let last_compacted = compactor_state.core.l0.pop_back().unwrap();
        compactor_state.core.l0_last_compacted = Some(last_compacted.id.unwrap_compacted_id());

        // when:
        db_state.merge_remote_manifest(compactor_state.clone());

        // then:
        let expected: Vec<SsTableId> = compactor_state.core.l0.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core().l0.iter().map(|l0| l0.id).collect();
        assert_eq!(expected, merged);
    }

    #[test]
    fn test_should_merge_db_state_with_all_l0s_if_none_compacted() {
        // given:
        let mut db_state = DbState::new(new_dirty_manifest());
        add_l0s_to_dbstate(&mut db_state, 4);
        let l0s = db_state.state.core().l0.clone();

        // when:
        db_state.merge_remote_manifest(new_dirty_manifest());

        // then:
        let expected: Vec<SsTableId> = l0s.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core().l0.iter().map(|l0| l0.id).collect();
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
            db_state.move_imm_memtable_to_l0(imm, handle).unwrap();
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
            let covering_tables = sorted_run.tables_covering_range(range.as_ref());
            let first_key = sorted_first_keys.first().unwrap().clone();

            let range_start_key = test_utils::bound_as_option(range.start_bound())
            .cloned()
            .unwrap_or_default();
            let range_end_key = test_utils::bound_as_option(range.end_bound())
            .cloned()
            .unwrap_or(vec![u8::MAX; max_bytes_len + 1].into());

            if covering_tables.is_empty() {
                assert!(range_end_key <= first_key);
            } else {
                let covering_first_key = covering_tables.front()
                .map(|t| t.compacted_first_key().clone())
                .unwrap();

                if range_start_key < covering_first_key {
                    assert_eq!(covering_first_key, first_key)
                }

                let covering_last_key = covering_tables.iter().last()
                .map(|t| t.compacted_first_key().clone())
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
