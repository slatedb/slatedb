use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};

use log::{error, info};
use ulid::Ulid;

use crate::db_state::{CoreDbState, SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest::store::DirtyManifest;

/// Identifier for a compaction input source.
///
/// A `SourceId` distinguishes between two kinds of inputs a compaction can read:
/// an existing compacted sorted run (identified by its run id), or an L0 SSTable
/// (identified by its ULID).
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum SourceId {
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
    /// Unwraps the source as a Sorted Run id, panicking if it is an L0 SST.
    ///
    /// ## Returns
    /// - The sorted run id.
    ///
    /// ## Panics
    /// - If called on `SourceId::Sst`.
    pub(crate) fn unwrap_sorted_run(&self) -> u32 {
        self.maybe_unwrap_sorted_run()
            .expect("tried to unwrap Sst as Sorted Run")
    }

    /// Returns the sorted run id if this source is a `SortedRun`, otherwise `None`.
    pub(crate) fn maybe_unwrap_sorted_run(&self) -> Option<u32> {
        match self {
            SourceId::SortedRun(id) => Some(*id),
            SourceId::Sst(_) => None,
        }
    }

    /// Returns the SST ULID if this source is an `Sst`, otherwise `None`.
    pub(crate) fn maybe_unwrap_sst(&self) -> Option<Ulid> {
        match self {
            SourceId::SortedRun(_) => None,
            SourceId::Sst(ulid) => Some(*ulid),
        }
    }
}

/// Immutable specification that describes a compaction. Currently, this only holds the
/// input sources and destination SR id for a compaction.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionSpec {
    /// Input sources for the compaction.
    sources: Vec<SourceId>,
    /// Destination sorted run id for the compaction.
    destination: u32,
}

impl CompactionSpec {
    /// Creates a new job spec describing which sources to compact and the destination SR id.
    ///
    /// ## Arguments
    /// - `sources`: Ordered list of sources (L0 SST ULIDs and/or existing SR ids).
    /// - `destination`: Sorted Run id for the compaction output.
    pub fn new(sources: Vec<SourceId>, destination: u32) -> Self {
        Self {
            sources,
            destination,
        }
    }

    /// The sources (input SSTs and sorted runs) for this compaction.
    pub fn sources(&self) -> &Vec<SourceId> {
        &self.sources
    }

    /// The destination sorted run id that will be produced by this compaction.
    pub fn destination(&self) -> u32 {
        self.destination
    }
}

impl Display for CompactionSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let displayed_sources: Vec<String> =
            self.sources().iter().map(|s| format!("{}", s)).collect();
        write!(f, "{:?} -> {}", displayed_sources, self.destination())
    }
}

/// Canonical, internal record of a compaction.
///
/// A compaction is the unit tracked by the compactor: it has a stable `id` (ULID) and a `spec`
/// (what to compact and where).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Compaction {
    /// Stable compaction id (ULID) used to track this compaction across messages and attempts.
    id: Ulid,
    /// What to compact (sources) and where to write (destination).
    spec: CompactionSpec,
}

impl Compaction {
    pub(crate) fn new(id: Ulid, spec: CompactionSpec) -> Self {
        Self { id, spec }
    }

    /// Returns all sorted run sources for this compaction.
    ///
    /// ## Arguments
    /// - `db_state`: The current core DB state from the manifest.
    pub(crate) fn get_sorted_runs(&self, db_state: &CoreDbState) -> Vec<SortedRun> {
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();

        self.spec
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .filter_map(|id| srs_by_id.get(&id).map(|t| (*t).clone()))
            .collect()
    }

    /// Returns all L0 SSTable sources for this compaction.
    ///
    /// ## Arguments
    /// - `db_state`: The current core DB state from the manifest.
    pub(crate) fn get_ssts(&self, db_state: &CoreDbState) -> Vec<SsTableHandle> {
        let ssts_by_id: HashMap<Ulid, &SsTableHandle> = db_state
            .l0
            .iter()
            .map(|sst| (sst.id.unwrap_compacted_id(), sst))
            .collect();

        self.spec
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sst())
            .filter_map(|ulid| ssts_by_id.get(&ulid).map(|t| (*t).clone()))
            .collect()
    }

    /// The stable job id (ULID) used to track this compaction across messages and attempts.
    pub(crate) fn id(&self) -> Ulid {
        self.id
    }

    /// Returns the immutable job spec describing inputs and destination.
    pub(crate) fn spec(&self) -> &CompactionSpec {
        &self.spec
    }
}

impl Display for Compaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let displayed_sources: Vec<_> = self
            .spec
            .sources()
            .iter()
            .map(|s| format!("{}", s))
            .collect();
        write!(f, "{:?} -> {}", displayed_sources, self.spec.destination())
    }
}

/// Process-local runtime state owned by the compactor.
///
/// This is the in-memory view that a single compactor task uses to:
/// - keep a fresh `DirtyManifest` (view of `CoreDbState`),
/// - track in-flight jobs by job id (ULID).
pub struct CompactorState {
    manifest: DirtyManifest,
    jobs: BTreeMap<Ulid, Compaction>,
}

impl CompactorState {
    /// Creates a new compactor state seeded with the provided dirty manifest. Jobs are
    /// initialized empty.
    pub(crate) fn new(manifest: DirtyManifest) -> Self {
        Self {
            manifest,
            jobs: BTreeMap::new(),
        }
    }

    /// Returns the current in-memory core DB state derived from the manifest.
    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    /// Returns the local dirty manifest that will be written back after compactions.
    pub(crate) fn manifest(&self) -> &DirtyManifest {
        &self.manifest
    }

    /// Returns an iterator over all in-flight jobs.
    pub(crate) fn jobs(&self) -> impl Iterator<Item = &Compaction> {
        self.jobs.values()
    }

    /// Merges the remote (writer) manifest view into the compactor's local state.
    ///
    /// This preserves local knowledge about compactions already applied (e.g., L0 last
    /// compacted marker, existing compacted runs) while pulling in newly created L0 SSTs
    /// and other writer-updated fields.
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
            sequence_tracker: remote_manifest.core.sequence_tracker,
        };
        remote_manifest.core = merged;
        self.manifest = remote_manifest;
    }

    /// Validates and registers a newly submitted job with this compactor.
    ///
    /// ## Returns
    /// - `Ok(())` if accepted, or [`SlateDBError::InvalidCompaction`] if the job conflicts
    ///   with an existing destination or violates destination overwrite rules.
    pub(crate) fn add_job(&mut self, job: Compaction) -> Result<(), SlateDBError> {
        let spec = job.spec();
        if self
            .jobs
            .values()
            .map(|c| c.spec())
            .any(|c| c.destination() == spec.destination())
        {
            // we already have an ongoing job for this destination
            return Err(SlateDBError::InvalidCompaction);
        }
        if self
            .db_state()
            .compacted
            .iter()
            .any(|sr| sr.id == spec.destination())
            && !spec.sources().iter().any(|src| match src {
                SourceId::SortedRun(sr) => *sr == spec.destination(),
                SourceId::Sst(_) => false,
            })
        {
            // the job overwrites an existing sr but doesn't include the sr
            return Err(SlateDBError::InvalidCompaction);
        }
        info!("accepted submitted compaction [compaction={}]", job);

        self.jobs.insert(job.id(), job);
        Ok(())
    }

    /// Removes a job from the in-flight map (called after completion or failure).
    pub(crate) fn remove_job(&mut self, job_id: &Ulid) {
        self.jobs.remove(job_id);
    }

    /// Applies the effects of a finished compaction to the in-memory manifest.
    ///
    /// This removes compacted L0 SSTs and source SRs, inserts the output SR in id-descending
    /// order, updates `l0_last_compacted`, and removes the job from the in-flight map.
    pub(crate) fn finish_job(&mut self, job_id: Ulid, output_sr: SortedRun) {
        if let Some(job) = self.jobs.get(&job_id) {
            let spec = job.spec();
            info!("finished compaction [spec={}]", spec);
            // reconstruct l0
            let compaction_l0s: HashSet<Ulid> = spec
                .sources()
                .iter()
                .filter_map(|id| id.maybe_unwrap_sst())
                .collect();
            let compaction_srs: HashSet<u32> = spec
                .sources()
                .iter()
                .chain(std::iter::once(&SourceId::SortedRun(spec.destination())))
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
            let first_source = spec
                .sources()
                .first()
                .expect("illegal: empty compaction spec");
            if let Some(compacted_l0) = first_source.maybe_unwrap_sst() {
                // if there are l0s, the newest must be the first entry in sources
                // TODO: validate that this is the case
                db_state.l0_last_compacted = Some(compacted_l0)
            }
            db_state.l0 = new_l0;
            db_state.compacted = new_compacted;
            self.manifest.core = db_state;
            if self.jobs.remove(&job_id).is_none() {
                error!("scheduled compactor job not found [job_id={}]", job_id);
            }
        } else {
            error!("compactor job not found [job_id={}]", job_id);
        }
    }

    /// Debug assertion that compacted sorted runs are kept in strictly descending id order.
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
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::clock::{DefaultSystemClock, SystemClock};
    use crate::compactor_state::SourceId::Sst;
    use crate::config::Settings;
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::utils::IdGenerator;
    use crate::DbRand;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use tokio::runtime::{Handle, Runtime};

    const PATH: &str = "/test/db";

    #[test]
    fn test_should_register_job() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());

        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&state.db_state().l0, 0);
        // when:
        let compaction = Compaction::new(job_id, spec.clone());
        state
            .add_job(compaction.clone())
            .expect("failed to add job");

        // then:
        let mut jobs = state.jobs();
        let expected = Compaction::new(job_id, spec.clone());
        assert_eq!(jobs.next().expect("job not found"), &expected);
        assert!(jobs.next().is_none());
    }

    #[test]
    fn test_should_update_dbstate_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&before_compaction.l0, 0);
        let compaction = Compaction::new(job_id, spec);
        state
            .add_job(compaction.clone())
            .expect("failed to add job");

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_job(job_id, sr.clone());

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
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&before_compaction.l0, 0);
        let compaction = Compaction::new(job_id, spec);
        state
            .add_job(compaction.clone())
            .expect("failed to add job");

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_job(job_id, sr.clone());

        // then:
        assert_eq!(state.jobs().count(), 0)
    }

    #[test]
    fn test_should_merge_db_state_correctly_when_never_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state, _, _) = build_test_state(rt.handle());
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
        let (os, mut sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(
            vec![Sst(original_l0s.back().unwrap().id.unwrap_compacted_id())],
            0,
        );
        let compaction = Compaction::new(job_id, spec);
        state
            .add_job(compaction.clone())
            .expect("failed to add job");
        state.finish_job(
            job_id,
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
        let (os, mut sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let job_id = rand.rng().gen_ulid(system_clock.as_ref());

        let spec = CompactionSpec::new(
            original_l0s
                .iter()
                .map(|h| Sst(h.id.unwrap_compacted_id()))
                .collect(),
            0,
        );
        let compactor_job = Compaction::new(job_id, spec);
        state
            .add_job(compactor_job.clone())
            .expect("failed to add job");
        state.finish_job(
            job_id,
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
    fn test_should_submit_correct_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(
            original_l0s
                .iter()
                .enumerate()
                .filter(|(i, _e)| i > &2usize)
                .map(|(_i, x)| Sst(x.id.unwrap_compacted_id()))
                .collect::<Vec<SourceId>>(),
            0,
        );
        let compaction = Compaction::new(job_id, spec);
        let result = state.add_job(compaction.clone());

        // then:
        assert!(result.is_ok());
    }

    #[test]
    fn test_source_boundary_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state, system_clock, rand) = build_test_state(rt.handle());
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

        let job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(sources, 0);
        let compaction = Compaction::new(job_id, spec);
        let result = state.add_job(compaction.clone());

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

    fn build_l0_compaction(ssts: &VecDeque<SsTableHandle>, dst: u32) -> CompactionSpec {
        let sources = ssts
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        CompactionSpec::new(sources, dst)
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

    #[allow(clippy::type_complexity)]
    fn build_test_state(
        tokio_handle: &Handle,
    ) -> (
        Arc<dyn ObjectStore>,
        StoredManifest,
        CompactorState,
        Arc<dyn SystemClock>,
        Arc<DbRand>,
    ) {
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
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand: Arc<DbRand> = Arc::new(DbRand::default());

        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(PATH),
            os.clone(),
            system_clock.clone(),
        ));
        let stored_manifest = tokio_handle
            .block_on(StoredManifest::load(manifest_store))
            .unwrap();
        let state = CompactorState::new(stored_manifest.prepare_dirty());
        (os, stored_manifest, state, system_clock, rand)
    }
}
