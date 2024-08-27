use crate::flatbuffer_types::SsTableInfoOwned;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, WritableKVTable};
use std::collections::VecDeque;
use std::sync::Arc;
use ulid::Ulid;
use SsTableId::Compacted;

#[derive(Clone, PartialEq)]
pub struct SSTableHandle {
    pub id: SsTableId,
    pub info: SsTableInfoOwned,
}

impl SSTableHandle {
    pub(crate) fn new(id: SsTableId, info: SsTableInfoOwned) -> Self {
        SSTableHandle { id, info }
    }

    pub(crate) fn range_covers_key(&self, key: &[u8]) -> bool {
        if let Some(first_key) = self.info.borrow().first_key() {
            return key >= first_key.bytes();
        }
        false
    }
}

#[derive(Clone, PartialEq, Debug, Hash, Eq)]
pub enum SsTableId {
    Wal(u64),
    Compacted(Ulid),
}

impl SsTableId {
    #[allow(clippy::panic)]
    pub(crate) fn unwrap_compacted_id(&self) -> Ulid {
        match self {
            SsTableId::Wal(_) => panic!("found WAL id when unwrapping compacted ID"),
            Compacted(ulid) => *ulid,
        }
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct SortedRun {
    pub(crate) id: u32,
    pub(crate) ssts: Vec<SSTableHandle>,
}

impl SortedRun {
    pub(crate) fn find_sst_with_range_covering_key_idx(&self, key: &[u8]) -> Option<usize> {
        // returns the sst after the one whose range includes the key
        let first_sst = self.ssts.partition_point(|sst| {
            sst.info
                .borrow()
                .first_key()
                .expect("sst must have first key")
                .bytes()
                <= key
        });
        if first_sst > 0 {
            return Some(first_sst - 1);
        }
        // all ssts have a range greater than the key
        None
    }

    pub(crate) fn find_sst_with_range_covering_key(&self, key: &[u8]) -> Option<&SSTableHandle> {
        self.find_sst_with_range_covering_key_idx(key)
            .map(|idx| &self.ssts[idx])
    }
}

pub(crate) struct DbState {
    memtable: WritableKVTable,
    wal: WritableKVTable,
    state: Arc<COWDbState>,
}

// represents the state that is mutated by creating a new copy with the mutations
#[derive(Clone)]
pub(crate) struct COWDbState {
    pub(crate) imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    pub(crate) imm_wal: VecDeque<Arc<ImmutableWal>>,
    pub(crate) core: CoreDbState,
}
// represents the core db state that we persist in the manifest
#[derive(Clone, PartialEq)]
pub(crate) struct CoreDbState {
    pub(crate) l0_last_compacted: Option<Ulid>,
    pub(crate) l0: VecDeque<SSTableHandle>,
    pub(crate) compacted: Vec<SortedRun>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
}

impl CoreDbState {
    pub(crate) fn new() -> Self {
        Self {
            l0_last_compacted: None,
            l0: VecDeque::new(),
            compacted: vec![],
            next_wal_sst_id: 1,
            last_compacted_wal_sst_id: 0,
        }
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

    // mutations

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

    pub fn freeze_memtable(&mut self, wal_id: u64) {
        let old_memtable = std::mem::replace(&mut self.memtable, WritableKVTable::new());
        let mut state = self.state_copy();
        state
            .imm_memtable
            .push_front(Arc::new(ImmutableMemtable::new(old_memtable, wal_id)));
        self.update_state(state);
    }

    pub fn freeze_wal(&mut self) -> Option<u64> {
        if self.wal.table().is_empty() {
            return None;
        }
        let old_wal = std::mem::replace(&mut self.wal, WritableKVTable::new());
        let mut state = self.state_copy();
        let imm_wal = Arc::new(ImmutableWal::new(state.core.next_wal_sst_id, old_wal));
        let id = imm_wal.id();
        state.imm_wal.push_front(imm_wal);
        state.core.next_wal_sst_id += 1;
        self.update_state(state);
        Some(id)
    }

    pub fn pop_imm_wal(&mut self) {
        let mut state = self.state_copy();
        state.imm_wal.pop_back();
        self.update_state(state);
    }

    pub fn move_imm_memtable_to_l0(
        &mut self,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SSTableHandle,
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
    use crate::db_state::{CoreDbState, DbState, SSTableHandle, SsTableId};
    use crate::flatbuffer_types::{SsTableInfo, SsTableInfoArgs, SsTableInfoOwned};
    use bytes::Bytes;
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
        let expected: Vec<SsTableId> = compactor_state.l0.iter().map(|l0| l0.id.clone()).collect();
        let merged: Vec<SsTableId> = db_state
            .state
            .core
            .l0
            .iter()
            .map(|l0| l0.id.clone())
            .collect();
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
        let expected: Vec<SsTableId> = l0s.iter().map(|l0| l0.id.clone()).collect();
        let merged: Vec<SsTableId> = db_state
            .state
            .core
            .l0
            .iter()
            .map(|l0| l0.id.clone())
            .collect();
        assert_eq!(expected, merged);
    }

    fn add_l0s_to_dbstate(db_state: &mut DbState, n: u32) {
        let dummy_info = create_sst_info();
        for i in 0..n {
            db_state.freeze_memtable(i as u64);
            let imm = db_state.state.imm_memtable.back().unwrap().clone();
            let handle = SSTableHandle::new(SsTableId::Compacted(Ulid::new()), dummy_info.clone());
            db_state.move_imm_memtable_to_l0(imm, handle);
        }
    }

    fn create_sst_info() -> SsTableInfoOwned {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let wip = SsTableInfo::create(
            &mut builder,
            &SsTableInfoArgs {
                first_key: None,
                index_offset: 0,
                index_len: 0,
                filter_offset: 0,
                filter_len: 0,
                compression_format: None.into(),
            },
        );
        builder.finish(wip, None);
        SsTableInfoOwned::new(Bytes::copy_from_slice(builder.finished_data())).unwrap()
    }
}
