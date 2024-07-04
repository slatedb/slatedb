use crate::flatbuffer_types::ManifestV1Owned;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, WritableKVTable};
use crate::tablestore::SSTableHandle;
use std::collections::VecDeque;
use std::sync::Arc;

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
#[derive(Clone)]
pub(crate) struct CoreDbState {
    pub(crate) l0: VecDeque<SSTableHandle>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
}

// represents a read-snapshot of the current db state
#[derive(Clone)]
pub(crate) struct DbStateSnapshot {
    pub(crate) memtable: Arc<KVTable>,
    pub(crate) wal: Arc<KVTable>,
    pub(crate) state: Arc<COWDbState>,
}

impl CoreDbState {
    pub fn load(manifest: &ManifestV1Owned) -> CoreDbState {
        let manifest = manifest.borrow();
        CoreDbState {
            l0: VecDeque::new(),
            next_wal_sst_id: manifest.wal_id_last_compacted() + 1,
            last_compacted_wal_sst_id: manifest.wal_id_last_compacted(),
        }
    }
}

impl DbState {
    pub fn load(manifest: &ManifestV1Owned) -> Self {
        Self {
            memtable: WritableKVTable::new(),
            wal: WritableKVTable::new(),
            state: Arc::new(COWDbState {
                imm_memtable: VecDeque::new(),
                imm_wal: VecDeque::new(),
                core: CoreDbState::load(manifest),
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
}
