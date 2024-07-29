use crate::compactor_state::CompactionStatus::Submitted;
use crate::db_state::{CoreDbState, SortedRun};
use crate::error::SlateDBError;
use crate::flatbuffer_types::{ManifestV1Owned, SsTableInfoOwned};
use crate::tablestore::SsTableId::Compacted;
use crate::tablestore::{SSTableHandle, TableStore};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SourceId {
    SortedRun(u32),
    Sst(Ulid),
}

impl SourceId {
    #[allow(dead_code)]
    pub(crate) fn unwrap_sorted_run(&self) -> u32 {
        self.maybe_unwrap_sorted_run()
            .expect("tried to unwrap Sst as Sorted Run")
    }

    pub(crate) fn maybe_unwrap_sorted_run(&self) -> Option<u32> {
        match self {
            SourceId::SortedRun(id) => Some(*id),
            SourceId::Sst(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn unwrap_sst(&self) -> Ulid {
        self.maybe_unwrap_sst()
            .expect("tried to unwrap Sst as Sorted Run")
    }

    pub(crate) fn maybe_unwrap_sst(&self) -> Option<Ulid> {
        match self {
            SourceId::SortedRun(_) => None,
            SourceId::Sst(ulid) => Some(*ulid),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CompactionStatus {
    Submitted,
    #[allow(dead_code)]
    InProgress,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Compaction {
    pub(crate) status: CompactionStatus,
    pub(crate) sources: Vec<SourceId>,
    pub(crate) destination: u32,
}

impl Compaction {
    pub(crate) fn new(sources: Vec<SourceId>, destination: u32) -> Self {
        Self {
            status: Submitted,
            sources,
            destination,
        }
    }
}

pub(crate) struct CompactorState {
    db_state: CoreDbState,
    // TODO: fully specify the manifest in CoreDbState, and then we can drop this and just convert
    //       back and forth from ManifestSynchronizer. This is also blocked on lazy-loading the
    //       filters, since its currently too expensive to load the manifest from CoreDbState.
    manifest: ManifestV1Owned,
    compactions: HashMap<u32, Compaction>,
}

impl CompactorState {
    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.db_state
    }

    pub(crate) fn manifest(&self) -> &ManifestV1Owned {
        &self.manifest
    }

    #[allow(dead_code)]
    pub(crate) fn compactions(&self) -> Vec<Compaction> {
        self.compactions.values().cloned().collect()
    }

    pub(crate) fn new(
        manifest: ManifestV1Owned,
        table_store: &TableStore,
        tokio_handle: &Handle,
    ) -> Result<Self, SlateDBError> {
        let db_state = tokio_handle.block_on(CoreDbState::load(&manifest, table_store))?;
        Ok(Self {
            db_state,
            manifest,
            compactions: HashMap::<u32, Compaction>::new(),
        })
    }

    pub(crate) fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        // todo: validate the compaction here
        //       https://github.com/slatedb/slatedb/issues/96
        if self.compactions.contains_key(&compaction.destination) {
            return Err(SlateDBError::InvalidCompaction);
        }
        self.compactions.insert(compaction.destination, compaction);
        Ok(())
    }

    pub(crate) fn refresh_db_state(
        &mut self,
        writer_manifest_owned: ManifestV1Owned,
        table_store: Arc<TableStore>,
        tokio_handle: &Handle,
    ) -> Result<(), SlateDBError> {
        let writer_manifest = writer_manifest_owned.borrow();
        // the writer may have added more l0 SSTs. Add these to our l0 list.
        let last_compacted_l0 = self.db_state.l0_last_compacted;
        let mut merged_l0s = VecDeque::new();
        let our_l0s_by_id: HashMap<Ulid, &SSTableHandle> = self
            .db_state
            .l0
            .iter()
            .map(|h| (h.id.unwrap_compacted_id(), h))
            .collect();
        let writer_l0 = writer_manifest.l0();
        let mut i = 0;
        while i < writer_l0.len() {
            // todo: move to some utility method
            let writer_l0_sst = writer_l0.get(i);
            let writer_l0_id = writer_l0_sst.id().expect("l0 sst must have id").ulid();
            // todo: this is brittle. we are relying on the l0 list always being updated in
            //       an expected order. We should instead encode the ordering in the l0 SST IDs
            //       and assert that it follows the order
            if match &last_compacted_l0 {
                None => true,
                Some(last_compacted_l0_id) => writer_l0_id != *last_compacted_l0_id,
            } {
                if let Some(&handle_ref) = our_l0s_by_id.get(&writer_l0_id) {
                    merged_l0s.push_back(handle_ref.clone())
                } else {
                    let writer_l0_info = writer_l0_sst.info().expect("l0 sst must have info");
                    let handle = tokio_handle.block_on(table_store.open_compacted_sst(
                        Compacted(writer_l0_id),
                        SsTableInfoOwned::create_copy(&writer_l0_info),
                    ))?;
                    merged_l0s.push_back(handle);
                }
            } else {
                break;
            }
            i += 1;
        }

        // write out the merged core db state and manifest
        let mut merged = self.db_state.clone();
        merged.l0 = merged_l0s;
        merged.last_compacted_wal_sst_id = writer_manifest.wal_id_last_compacted();
        merged.next_wal_sst_id = writer_manifest.wal_id_last_seen() + 1;
        self.db_state = merged;
        self.manifest = self.manifest.create_updated_manifest(&self.db_state);

        Ok(())
    }

    pub(crate) fn finish_compaction(&mut self, output_sr: SortedRun) {
        if let Some(compaction) = self.compactions.get(&output_sr.id) {
            // reconstruct l0
            let compaction_l0s: HashSet<Ulid> = compaction
                .sources
                .iter()
                .filter_map(|id| id.maybe_unwrap_sst())
                .collect();
            let compaction_srs: HashSet<u32> = compaction
                .sources
                .iter()
                .chain(std::iter::once(&SourceId::SortedRun(
                    compaction.destination,
                )))
                .filter_map(|id| id.maybe_unwrap_sorted_run())
                .collect();
            let mut db_state = self.db_state.clone();
            let new_l0: VecDeque<SSTableHandle> = db_state
                .l0
                .iter()
                .filter_map(|l0| {
                    let l0_id = l0.id.unwrap_compacted_id();
                    if compaction_l0s.contains(&l0_id) {
                        return None;
                    }
                    Some(l0.clone())
                })
                .collect();
            let mut new_compacted = Vec::new();
            let mut inserted = false;
            for compacted in db_state.compacted.iter() {
                if !inserted && output_sr.id >= compacted.id {
                    new_compacted.push(output_sr.clone());
                    inserted = true;
                }
                if !compaction_srs.contains(&compacted.id) {
                    new_compacted.push(compacted.clone());
                }
            }
            if !inserted {
                new_compacted.push(output_sr.clone());
            }
            let first_source = compaction
                .sources
                .first()
                .expect("illegal: empty compaction");
            if let Some(compacted_l0) = first_source.maybe_unwrap_sst() {
                // if there are l0s, the newest must be the first entry in sources
                // TODO: validate that this is the case
                db_state.l0_last_compacted = Some(compacted_l0)
            }
            db_state.l0 = new_l0;
            db_state.compacted = new_compacted;
            self.db_state = db_state;
            self.manifest = self.manifest.create_updated_manifest(&self.db_state);
            self.compactions.remove(&output_sr.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::CompactionStatus::Submitted;
    use crate::compactor_state::SourceId::Sst;
    use crate::db::{Db, DbOptions};
    use crate::sst::SsTableFormat;
    use crate::tablestore::SsTableId;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use tokio::runtime::Runtime;

    const PATH: &str = "/test/db";
    const DEFAULT_OPTIONS: DbOptions = DbOptions {
        flush_ms: 100,
        min_filter_keys: 0,
        l0_sst_size_bytes: 128,
        compactor_options: None,
    };

    #[test]
    fn test_should_register_compaction_as_submitted() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());

        // when:
        state
            .submit_compaction(build_l0_compaction(&state.db_state().l0, 0))
            .unwrap();

        // then:
        assert_eq!(state.compactions().len(), 1);
        assert_eq!(state.compactions().first().unwrap().status, Submitted);
    }

    #[test]
    fn test_should_update_dbstate_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction = build_l0_compaction(&before_compaction.l0, 0);
        state.submit_compaction(compaction).unwrap();

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(sr.clone());

        // then:
        assert_eq!(
            state.db_state().l0_last_compacted,
            Some(
                before_compaction
                    .l0
                    .front()
                    .unwrap()
                    .id
                    .unwrap_compacted_id()
            )
        );
        assert_eq!(state.db_state().l0.len(), 0);
        assert_eq!(state.db_state().compacted.len(), 1);
        assert_eq!(state.db_state().compacted.first().unwrap().id, sr.id);
        let expected_ids: Vec<SsTableId> = sr.ssts.iter().map(|h| h.id.clone()).collect();
        let found_ids: Vec<SsTableId> = state
            .db_state()
            .compacted
            .first()
            .unwrap()
            .ssts
            .iter()
            .map(|h| h.id.clone())
            .collect();
        assert_eq!(expected_ids, found_ids);
    }

    #[test]
    fn test_should_remove_compaction_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction = build_l0_compaction(&before_compaction.l0, 0);
        state.submit_compaction(compaction).unwrap();

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(sr.clone());

        // then:
        assert_eq!(state.compactions().len(), 0)
    }

    #[test]
    fn test_should_refresh_db_state_correctly_when_never_compacted() {
        // given:
        let rt = build_runtime();
        let (os, table_store, mut state) = build_test_state(rt.handle());
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48]));
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48]));
        let writer_manifest =
            wait_for_manifest_with_l0_len(&table_store, rt.handle(), state.db_state().l0.len() + 1);

        // when:
        state
            .refresh_db_state(writer_manifest.clone(), table_store, rt.handle())
            .unwrap();

        // then:
        assert!(state.db_state().l0_last_compacted.is_none());
        let expected_merged_l0s: Vec<Ulid> = writer_manifest
            .borrow()
            .l0()
            .iter()
            .map(|t| t.id().unwrap().ulid())
            .collect();
        let merged_l0s: Vec<Ulid> = state
            .db_state()
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(expected_merged_l0s, merged_l0s);
    }

    #[test]
    fn test_should_refresh_db_state_correctly() {
        // given:
        let rt = build_runtime();
        let (os, table_store, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        state
            .submit_compaction(Compaction::new(
                vec![Sst(original_l0s.back().unwrap().id.unwrap_compacted_id())],
                0,
            ))
            .unwrap();
        state.finish_compaction(SortedRun {
            id: 0,
            ssts: vec![original_l0s.back().unwrap().clone()],
        });
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48]));
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48]));
        let writer_manifest =
            wait_for_manifest_with_l0_len(&table_store, rt.handle(), original_l0s.len() + 1);
        let db_state_before_merge = state.db_state().clone();

        // when:
        state
            .refresh_db_state(writer_manifest.clone(), table_store, rt.handle())
            .unwrap();

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s: VecDeque<Ulid> = original_l0s
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        expected_merged_l0s.pop_back();
        let new_l0 = writer_manifest.borrow().l0().get(0).id().unwrap();
        let new_l0 = Ulid::from((new_l0.high(), new_l0.low()));
        expected_merged_l0s.push_front(new_l0);
        let merged_l0: VecDeque<Ulid> = db_state
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(merged_l0, expected_merged_l0s);
        assert_eq!(
            compacted_to_description(&db_state.compacted),
            compacted_to_description(&db_state_before_merge.compacted)
        );
        assert_eq!(
            db_state.last_compacted_wal_sst_id,
            writer_manifest.borrow().wal_id_last_compacted()
        );
        assert_eq!(
            db_state.next_wal_sst_id,
            writer_manifest.borrow().wal_id_last_seen() + 1
        );
        let manifest = state.manifest();
        assert_eq!(
            manifest.borrow(),
            writer_manifest.create_updated_manifest(db_state).borrow()
        )
    }

    #[test]
    fn test_should_refresh_db_state_correctly_when_all_l0_compacted() {
        // given:
        let rt = build_runtime();
        let (os, table_store, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        state
            .submit_compaction(Compaction::new(
                original_l0s
                    .iter()
                    .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
                    .collect(),
                0,
            ))
            .unwrap();
        state.finish_compaction(SortedRun {
            id: 0,
            ssts: original_l0s.clone().into(),
        });
        assert_eq!(state.db_state().l0.len(), 0);
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48]));
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48]));
        let writer_manifest =
            wait_for_manifest_with_l0_len(&table_store, rt.handle(), original_l0s.len() + 1);

        // when:
        state
            .refresh_db_state(writer_manifest.clone(), table_store, rt.handle())
            .unwrap();

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s = VecDeque::new();
        let new_l0 = writer_manifest.borrow().l0().get(0).id().unwrap();
        let new_l0 = Ulid::from((new_l0.high(), new_l0.low()));
        expected_merged_l0s.push_front(new_l0);
        let merged_l0: VecDeque<Ulid> = db_state
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(merged_l0, expected_merged_l0s);
    }

    // test helpers

    fn run_for<T, F>(duration: Duration, mut f: F) -> Option<T>
    where
        F: FnMut() -> Option<T>,
    {
        let now = SystemTime::now();
        while now.elapsed().unwrap() < duration {
            let maybe_result = f();
            if maybe_result.is_some() {
                return maybe_result;
            }
            sleep(Duration::from_millis(100));
        }
        None
    }

    #[derive(PartialEq, Debug)]
    struct SortedRunDescription {
        id: u32,
        ssts: Vec<SsTableId>,
    }

    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn compacted_to_description(compacted: &[SortedRun]) -> Vec<SortedRunDescription> {
        compacted.iter().map(sorted_run_to_description).collect()
    }

    fn sorted_run_to_description(sr: &SortedRun) -> SortedRunDescription {
        SortedRunDescription {
            id: sr.id,
            ssts: sr.ssts.iter().map(|h| h.id.clone()).collect(),
        }
    }

    fn wait_for_manifest_with_l0_len(
        table_store: &Arc<TableStore>,
        tokio_handle: &Handle,
        len: usize,
    ) -> ManifestV1Owned {
        run_for(Duration::from_secs(30), || {
            let maybe_manifest = tokio_handle
                .block_on(table_store.open_latest_manifest())
                .unwrap()
                .unwrap();
            if maybe_manifest.borrow().l0().len() == len {
                return Some(maybe_manifest);
            }
            None
        })
        .expect("no manifest found with l0 len")
    }

    fn build_l0_compaction(ssts: &VecDeque<SSTableHandle>, dst: u32) -> Compaction {
        let sources = ssts
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        Compaction::new(sources, dst)
    }

    fn build_db(os: Arc<dyn ObjectStore>, tokio_handle: &Handle) -> Db {
        tokio_handle
            .block_on(Db::open(Path::from(PATH), DEFAULT_OPTIONS, os))
            .unwrap()
    }

    fn build_test_state(
        tokio_handle: &Handle,
    ) -> (Arc<dyn ObjectStore>, Arc<TableStore>, CompactorState) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = build_db(os.clone(), tokio_handle);
        let l0_count: u64 = 5;
        for i in 0..l0_count {
            tokio_handle.block_on(db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]));
            tokio_handle.block_on(db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]));
        }
        tokio_handle.block_on(db.close()).unwrap();
        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = Arc::new(TableStore::new(os.clone(), sst_format, Path::from(PATH)));
        let manifest = tokio_handle
            .block_on(table_store.open_latest_manifest())
            .unwrap()
            .unwrap();
        let state = CompactorState::new(manifest, table_store.as_ref(), tokio_handle).unwrap();
        (os, table_store, state)
    }
}
