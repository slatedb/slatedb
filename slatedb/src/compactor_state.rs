use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};

use log::{info, warn};
use ulid::Ulid;
use uuid::Uuid;

use crate::compactor_state::CompactionStatus::Submitted;
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest::store::DirtyManifest;

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
    manifest: DirtyManifest,
    compactions: HashMap<Uuid, Compaction>,
}

impl CompactorState {
    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    pub(crate) fn manifest(&self) -> &DirtyManifest {
        &self.manifest
    }

    pub(crate) fn num_compactions(&self) -> usize {
        self.compactions.len()
    }

    pub(crate) fn compactions(&self) -> Vec<Compaction> {
        self.compactions.values().cloned().collect()
    }

    pub(crate) fn new(manifest: DirtyManifest) -> Self {
        Self {
            manifest,
            compactions: HashMap::<Uuid, Compaction>::new(),
        }
    }

    pub(crate) fn submit_compaction(
        &mut self,
        id: Uuid,
        compaction: Compaction,
    ) -> Result<Uuid, SlateDBError> {
        self.validate_compaction(&compaction)?;
        if self
            .compactions
            .values()
            .any(|c| c.destination == compaction.destination)
        {
            // we already have an ongoing compaction for this destination
            return Err(SlateDBError::InvalidCompaction);
        }
        if self
            .db_state()
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
        info!("accepted submitted compaction [compaction={}]", compaction);
        self.compactions.insert(id, compaction);
        Ok(id)
    }

    pub(crate) fn merge_remote_manifest(&mut self, mut remote_manifest: DirtyManifest) {
        // the writer may have added more l0 SSTs. Add these to our l0 list.
        let my_db_state = self.db_state();
        let last_compacted_l0 = my_db_state.l0_last_compacted;
        let mut merged_l0s = VecDeque::new();
        let writer_l0 = &remote_manifest.core.l0;
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
        let merged = CoreDbState {
            initialized: remote_manifest.core.initialized,
            l0_last_compacted: my_db_state.l0_last_compacted,
            l0: merged_l0s,
            compacted: my_db_state.compacted.clone(),
            next_wal_sst_id: remote_manifest.core.next_wal_sst_id,
            replay_after_wal_id: remote_manifest.core.replay_after_wal_id,
            last_l0_clock_tick: remote_manifest.core.last_l0_clock_tick,
            last_l0_seq: remote_manifest.core.last_l0_seq,
            checkpoints: remote_manifest.core.checkpoints.clone(),
            wal_object_store_uri: my_db_state.wal_object_store_uri.clone(),
            recent_snapshot_min_seq: my_db_state.recent_snapshot_min_seq,
        };
        remote_manifest.core = merged;
        self.manifest = remote_manifest;
    }

    pub(crate) fn finish_failed_compaction(&mut self, id: Uuid) {
        self.compactions.remove(&id);
    }

    pub(crate) fn finish_compaction(&mut self, id: Uuid, output_sr: SortedRun) {
        if let Some(compaction) = self.compactions.get(&id) {
            info!("finished compaction [compaction={}]", compaction);
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
            let mut db_state = self.db_state().clone();
            let new_l0: VecDeque<SsTableHandle> = db_state
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
            self.manifest.core = db_state;
            self.compactions.remove(&id);
        }
    }

    fn assert_compacted_srs_in_id_order(compacted: &[SortedRun]) {
        let mut last_sr_id = u32::MAX;
        for sr in compacted.iter() {
            assert!(sr.id < last_sr_id);
            last_sr_id = sr.id;
        }
    }

    fn validate_compaction(&mut self, compaction: &Compaction) -> Result<(), SlateDBError> {
        // Logical order of sources: [L0 (newest → oldest), then SRs (highest id → 0)]
        let sources_logical_order: Vec<SourceId> = self
            .db_state()
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .chain(
                self.db_state()
                    .compacted
                    .iter()
                    .map(|sr| SourceId::SortedRun(sr.id)),
            )
            .collect();

        // Validate compaction sources exist
        if compaction.sources.is_empty() {
            warn!("submitted compaction is empty: {:?}", compaction.sources);
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate if the compaction sources are strictly consecutive elements in the db_state sources
        if !sources_logical_order
            .windows(compaction.sources.len())
            .any(|w| w == compaction.sources.as_slice())
        {
            warn!("submitted compaction is not a consecutive series of sources from db state: {:?} {:?}",
            compaction.sources, sources_logical_order);
            return Err(SlateDBError::InvalidCompaction);
        }

        let has_sr = compaction
            .sources
            .iter()
            .any(|s| matches!(s, SourceId::SortedRun(_)));

        if has_sr {
            // Must merge into the lowest-id SR among sources
            let min_sr = compaction
                .sources
                .iter()
                .filter_map(|s| s.maybe_unwrap_sorted_run())
                .min()
                .expect("at least one SR in sources");
            if compaction.destination != min_sr {
                warn!(
                    "destination does not match lowest-id SR among sources: {:?} {:?}",
                    compaction.destination, min_sr
                );
                return Err(SlateDBError::InvalidCompaction);
            }
        } else {
            // L0-only: must create new SR with id > highest_existing
            let highest_id = self.db_state().compacted.first().map_or(0, |sr| sr.id + 1);
            if compaction.destination < highest_id {
                warn!(
                    "compaction destination is lesser than the expected L0-only highest_id: {:?} {:?}",
                    compaction.destination, highest_id
                );
                return Err(SlateDBError::InvalidCompaction);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::clock::{DefaultSystemClock, SystemClock};
    use crate::compactor_state::CompactionStatus::Submitted;
    use crate::compactor_state::SourceId::Sst;
    use crate::config::Settings;
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use tokio::runtime::{Handle, Runtime};

    const PATH: &str = "/test/db";

    #[test]
    fn test_should_register_compaction_as_submitted() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());

        // when:
        state
            .submit_compaction(
                uuid::Uuid::new_v4(),
                build_l0_compaction(&state.db_state().l0, 0),
            )
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
        let id = state
            .submit_compaction(uuid::Uuid::new_v4(), compaction)
            .unwrap();

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(id, sr.clone());

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
        let expected_ids: Vec<SsTableId> = sr.ssts.iter().map(|h| h.id).collect();
        let found_ids: Vec<SsTableId> = state
            .db_state()
            .compacted
            .first()
            .unwrap()
            .ssts
            .iter()
            .map(|h| h.id)
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
        let id = state
            .submit_compaction(uuid::Uuid::new_v4(), compaction)
            .unwrap();

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(id, sr.clone());

        // then:
        assert_eq!(state.compactions().len(), 0)
    }

    #[test]
    fn test_should_merge_db_state_correctly_when_never_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state) = build_test_state(rt.handle());
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), state.db_state().l0.len() + 1);

        // when:
        state.merge_remote_manifest(sm.prepare_dirty());

        // then:
        assert!(state.db_state().l0_last_compacted.is_none());
        let expected_merged_l0s: Vec<Ulid> = sm
            .manifest()
            .core
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
    fn test_should_merge_db_state_correctly() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let id = state
            .submit_compaction(
                uuid::Uuid::new_v4(),
                Compaction::new(
                    vec![Sst(original_l0s.back().unwrap().id.unwrap_compacted_id())],
                    0,
                ),
            )
            .unwrap();
        state.finish_compaction(
            id,
            SortedRun {
                id: 0,
                ssts: vec![original_l0s.back().unwrap().clone()],
            },
        );
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);
        let db_state_before_merge = state.db_state().clone();

        // when:
        state.merge_remote_manifest(sm.prepare_dirty());

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s: VecDeque<Ulid> = original_l0s
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        expected_merged_l0s.pop_back();
        let new_l0 = sm
            .manifest()
            .core
            .l0
            .front()
            .unwrap()
            .id
            .unwrap_compacted_id();
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
            db_state.replay_after_wal_id,
            sm.manifest().core.replay_after_wal_id
        );
        assert_eq!(db_state.next_wal_sst_id, sm.manifest().core.next_wal_sst_id);
    }

    #[test]
    fn test_should_merge_db_state_correctly_when_all_l0_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let id = state
            .submit_compaction(
                uuid::Uuid::new_v4(),
                Compaction::new(
                    original_l0s
                        .iter()
                        .map(|h| Sst(h.id.unwrap_compacted_id()))
                        .collect(),
                    0,
                ),
            )
            .unwrap();
        state.finish_compaction(
            id,
            SortedRun {
                id: 0,
                ssts: original_l0s.clone().into(),
            },
        );
        assert_eq!(state.db_state().l0.len(), 0);
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);

        // when:
        state.merge_remote_manifest(sm.prepare_dirty());

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s = VecDeque::new();
        let new_l0 = sm
            .manifest()
            .core
            .l0
            .front()
            .unwrap()
            .id
            .unwrap_compacted_id();
        expected_merged_l0s.push_front(new_l0);
        let merged_l0: VecDeque<Ulid> = db_state
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(merged_l0, expected_merged_l0s);
    }

    #[test]
    fn test_should_merge_db_state_with_new_checkpoints() {
        // given:
        let mut state = CompactorState::new(new_dirty_manifest());
        // mimic an externally added checkpoint
        let mut dirty = new_dirty_manifest();
        let checkpoint = Checkpoint {
            id: uuid::Uuid::new_v4(),
            manifest_id: 1,
            expire_time: None,
            create_time: DefaultSystemClock::default().now(),
        };
        dirty.core.checkpoints.push(checkpoint.clone());

        // when:
        state.merge_remote_manifest(dirty);

        // then:
        assert_eq!(vec![checkpoint], state.db_state().checkpoints);
    }

    #[test]
    fn test_should_submit_invalid_compaction_wrong_order() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());

        let l0_sst = &mut state.db_state().l0.clone();
        let last_sst = l0_sst.pop_back();
        l0_sst.push_front(last_sst.unwrap());
        // when:
        let compaction = build_l0_compaction(l0_sst, 0);
        let result = state.submit_compaction(uuid::Uuid::new_v4(), compaction);

        // then:
        assert!(matches!(result, Err(SlateDBError::InvalidCompaction)));
    }

    #[test]
    fn test_should_submit_invalid_compaction_skipped_sst() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());

        let l0_sst = &mut state.db_state().l0.clone();
        let last_sst = l0_sst.pop_back().unwrap();
        l0_sst.push_front(last_sst);
        l0_sst.pop_back();
        // when:
        let compaction = build_l0_compaction(l0_sst, 0);
        let result = state.submit_compaction(uuid::Uuid::new_v4(), compaction);

        // then:
        assert!(matches!(result, Err(SlateDBError::InvalidCompaction)));
    }

    #[test]
    fn test_should_submit_invalid_compaction_with_sr() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state) = build_test_state(rt.handle());

        let mut compaction = build_l0_compaction(&state.db_state().l0, 0);
        compaction.sources.push(SourceId::SortedRun(5));
        // when:
        let result = state.submit_compaction(uuid::Uuid::new_v4(), compaction);

        // then:
        assert!(matches!(result, Err(SlateDBError::InvalidCompaction)));
    }

    #[test]
    fn test_should_submit_correct_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let result = state.submit_compaction(
            uuid::Uuid::new_v4(),
            Compaction::new(
                original_l0s
                    .iter()
                    .enumerate()
                    .filter(|(i, _e)| i > &2usize)
                    .map(|(_i, x)| Sst(x.id.unwrap_compacted_id()))
                    .collect::<Vec<SourceId>>(),
                0,
            ),
        );

        // then:
        assert!(result.is_ok());
    }

    #[test]
    fn test_source_boundary_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state) = build_test_state(rt.handle());
        let original_l0s = &state.db_state().clone().l0;
        let original_srs = &state.db_state().clone().compacted;
        // L0: from 4th onward (index > 2)
        let l0_sources = original_l0s
            .iter()
            .skip(3)
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()));

        // SRs: first 3 (index < 3)
        let sr_sources = original_srs
            .iter()
            .take(3)
            .map(|sr| SourceId::SortedRun(sr.id));

        // If you need both:
        let sources: Vec<SourceId> = l0_sources.chain(sr_sources).collect();
        let result = state.submit_compaction(uuid::Uuid::new_v4(), Compaction::new(sources, 0));

        // or simply:
        assert!(result.is_ok());
    }

    // test helpers

    fn run_for<T, F>(duration: Duration, mut f: F) -> Option<T>
    where
        F: FnMut() -> Option<T>,
    {
        let clock = DefaultSystemClock::default();
        let start = clock.now();
        while clock
            .now()
            .signed_duration_since(start)
            .to_std()
            .expect("duration < 0 not allowed")
            < duration
        {
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
            ssts: sr.ssts.iter().map(|h| h.id).collect(),
        }
    }

    fn wait_for_manifest_with_l0_len(
        stored_manifest: &mut StoredManifest,
        tokio_handle: &Handle,
        len: usize,
    ) {
        run_for(Duration::from_secs(30), || {
            let manifest = tokio_handle.block_on(stored_manifest.refresh()).unwrap();
            if manifest.core.l0.len() == len {
                return Some(manifest.core.clone());
            }
            None
        })
        .expect("no manifest found with l0 len");
    }

    fn build_l0_compaction(ssts: &VecDeque<SsTableHandle>, dst: u32) -> Compaction {
        let sources = ssts
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        Compaction::new(sources, dst)
    }

    fn build_db(os: Arc<dyn ObjectStore>, tokio_handle: &Handle) -> Db {
        let opts = Settings {
            l0_sst_size_bytes: 256,
            // make sure to run with the compactor disabled. The tests will explicitly
            // manage compaction execution and assert the associated state mutations.
            compactor_options: None,
            ..Default::default()
        };
        tokio_handle
            .block_on(Db::builder(PATH, os.clone()).with_settings(opts).build())
            .unwrap()
    }

    fn build_test_state(
        tokio_handle: &Handle,
    ) -> (Arc<dyn ObjectStore>, StoredManifest, CompactorState) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = build_db(os.clone(), tokio_handle);
        let l0_count: u64 = 5;
        for i in 0..l0_count {
            tokio_handle
                .block_on(db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]))
                .unwrap();
            tokio_handle
                .block_on(db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]))
                .unwrap();
        }
        tokio_handle.block_on(db.close()).unwrap();
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(PATH),
            os.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let stored_manifest = tokio_handle
            .block_on(StoredManifest::load(manifest_store))
            .unwrap();
        let state = CompactorState::new(stored_manifest.prepare_dirty());
        (os, stored_manifest, state)
    }
}
