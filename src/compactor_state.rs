use crate::compactor_state::CompactionStatus::Submitted;
use crate::db_state::{CoreDbState, SSTableHandle, SortedRun};
use crate::error::SlateDBError;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use tracing::info;
use ulid::Ulid;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum SourceId {
    SortedRun(u32),
    Sst(Ulid),
}

impl Display for SourceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SourceId::SortedRun(id) => {
                    format!("{}", *id)
                }
                SourceId::Sst(_) => String::from("l0"),
            }
        )
    }
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
pub struct Compaction {
    pub(crate) status: CompactionStatus,
    pub(crate) sources: Vec<SourceId>,
    pub(crate) destination: u32,
}

impl Display for Compaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let displayed_sources: Vec<_> = self.sources.iter().map(|s| format!("{}", s)).collect();
        write!(
            f,
            "{:?} -> {}: {:?}",
            displayed_sources, self.destination, self.status
        )
    }
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

pub struct CompactorState {
    db_state: CoreDbState,
    compactions: HashMap<u32, Compaction>,
}

impl CompactorState {
    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.db_state
    }

    pub(crate) fn num_compactions(&self) -> usize {
        self.compactions.len()
    }

    pub(crate) fn compactions(&self) -> Vec<Compaction> {
        self.compactions.values().cloned().collect()
    }

    pub(crate) fn new(db_state: CoreDbState) -> Self {
        Self {
            db_state,
            compactions: HashMap::<u32, Compaction>::new(),
        }
    }

    pub(crate) fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        // todo: validate the compaction here
        //       https://github.com/slatedb/slatedb/issues/96
        if self.compactions.contains_key(&compaction.destination) {
            // we already have an ongoing compaction for this destination
            return Err(SlateDBError::InvalidCompaction);
        }
        if self
            .db_state
            .compacted
            .iter()
            .any(|sr| sr.id == compaction.destination)
        {
            // the compaction overwrites an existing sr but doesn't include the sr
            if !compaction.sources.iter().any(|src| match src {
                SourceId::SortedRun(sr) => *sr == compaction.destination,
                SourceId::Sst(_) => false,
            }) {
                return Err(SlateDBError::InvalidCompaction);
            }
        }
        info!("accepted submitted compaction: {:?}", compaction);
        self.compactions.insert(compaction.destination, compaction);
        Ok(())
    }

    pub(crate) fn refresh_db_state(&mut self, writer_state: &CoreDbState) {
        // the writer may have added more l0 SSTs. Add these to our l0 list.
        let last_compacted_l0 = self.db_state.l0_last_compacted;
        let mut merged_l0s = VecDeque::new();
        let writer_l0 = &writer_state.l0;
        for writer_l0_sst in writer_l0 {
            let writer_l0_id = writer_l0_sst.id.unwrap_compacted_id();
            // todo: this is brittle. we are relying on the l0 list always being updated in
            //       an expected order. We should instead encode the ordering in the l0 SST IDs
            //       and assert that it follows the order
            if match &last_compacted_l0 {
                None => true,
                Some(last_compacted_l0_id) => writer_l0_id != *last_compacted_l0_id,
            } {
                merged_l0s.push_back(writer_l0_sst.clone());
            } else {
                break;
            }
        }

        // write out the merged core db state and manifest
        let mut merged = self.db_state.clone();
        merged.l0 = merged_l0s;
        merged.last_compacted_wal_sst_id = writer_state.last_compacted_wal_sst_id;
        merged.next_wal_sst_id = writer_state.next_wal_sst_id;
        self.db_state = merged;
    }

    pub(crate) fn finish_compaction(&mut self, output_sr: SortedRun) {
        if let Some(compaction) = self.compactions.get(&output_sr.id) {
            info!("finished compaction: {:?}", compaction);
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
            Self::assert_compacted_srs_in_id_order(&new_compacted);
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
            self.compactions.remove(&output_sr.id);
        }
    }

    fn assert_compacted_srs_in_id_order(compacted: &[SortedRun]) {
        let mut last_sr_id = u32::MAX;
        for sr in compacted.iter() {
            assert!(sr.id < last_sr_id);
            last_sr_id = sr.id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::CompactionStatus::Submitted;
    use crate::compactor_state::SourceId::Sst;
    use crate::config::{CompactorOptions, DbOptions};
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::manifest_store::{ManifestStore, StoredManifest};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use tokio::runtime::{Handle, Runtime};

    const PATH: &str = "/test/db";

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
        let (os, mut sm, mut state) = build_test_state(rt.handle());
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48]));
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48]));
        let writer_db_state =
            wait_for_manifest_with_l0_len(&mut sm, rt.handle(), state.db_state().l0.len() + 1);

        // when:
        state.refresh_db_state(&writer_db_state);

        // then:
        assert!(state.db_state().l0_last_compacted.is_none());
        let expected_merged_l0s: Vec<Ulid> = writer_db_state
            .l0
            .iter()
            .map(|t| t.id.unwrap_compacted_id())
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
        let (os, mut sm, mut state) = build_test_state(rt.handle());
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
        let writer_db_state =
            wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);
        let db_state_before_merge = state.db_state().clone();

        // when:
        state.refresh_db_state(&writer_db_state);

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s: VecDeque<Ulid> = original_l0s
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        expected_merged_l0s.pop_back();
        let new_l0 = writer_db_state.l0.front().unwrap().id.unwrap_compacted_id();
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
            writer_db_state.last_compacted_wal_sst_id
        );
        assert_eq!(db_state.next_wal_sst_id, writer_db_state.next_wal_sst_id);
    }

    #[test]
    fn test_should_refresh_db_state_correctly_when_all_l0_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        state
            .submit_compaction(Compaction::new(
                original_l0s
                    .iter()
                    .map(|h| Sst(h.id.unwrap_compacted_id()))
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
        let writer_db_state =
            wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);

        // when:
        state.refresh_db_state(&writer_db_state);

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s = VecDeque::new();
        let new_l0 = writer_db_state.l0.front().unwrap().id.unwrap_compacted_id();
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
        stored_manifest: &mut StoredManifest,
        tokio_handle: &Handle,
        len: usize,
    ) -> CoreDbState {
        run_for(Duration::from_secs(30), || {
            let db_state = tokio_handle.block_on(stored_manifest.refresh()).unwrap();
            if db_state.l0.len() == len {
                return Some(db_state.clone());
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
        let opts = DbOptions {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_secs(1),
            min_filter_keys: 1000,
            l0_sst_size_bytes: 128,
            max_unflushed_memtable: 2,
            l0_max_ssts: 8,
            compactor_options: Some(CompactorOptions::default()),
            compression_codec: None,
        };
        tokio_handle
            .block_on(Db::open_with_opts(Path::from(PATH), opts, os))
            .unwrap()
    }

    fn build_test_state(
        tokio_handle: &Handle,
    ) -> (Arc<dyn ObjectStore>, StoredManifest, CompactorState) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = build_db(os.clone(), tokio_handle);
        let l0_count: u64 = 5;
        for i in 0..l0_count {
            tokio_handle.block_on(db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]));
            tokio_handle.block_on(db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]));
        }
        tokio_handle.block_on(db.close()).unwrap();
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let stored_manifest = tokio_handle
            .block_on(StoredManifest::load(manifest_store))
            .unwrap()
            .unwrap();
        let state = CompactorState::new(stored_manifest.db_state().clone());
        (os, stored_manifest, state)
    }
}
