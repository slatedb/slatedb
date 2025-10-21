use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};

use log::{error, info};
use ulid::Ulid;

use crate::compactor_executor::CompactionJob;
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
    Pending,
    #[allow(dead_code)]
    InProgress,
    #[allow(dead_code)]
    Completed,
    #[allow(dead_code)]
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CompactionType {
    Internal,
    #[allow(dead_code)]
    External,
}

#[derive(Clone, Debug, PartialEq)]
/// In-memory description of the inputs to a compaction.
///
/// A `CompactionSpec` represents the concrete set of input SSTs and/or Sorted
/// Runs that a particular compaction algorithm (e.g., size-tiered, leveled,
/// etc.) has selected to produce a destination Sorted Run.
///
/// The scheduler constructs a `CompactionSpec` for a planned compaction and
/// passes it, together with the selected `sources`, to `Compaction::new`.
/// Keeping the spec inside the in-memory `Compaction` avoids recomputing or
/// cloning inputs at job creation time and enables encode/decode roundtrips in
/// tests via FlatBuffers.
pub(crate) enum CompactionSpec {
    /// Compact a combination of L0 SSTs and/or existing sorted runs into a
    /// new sorted run with id `destination` carried by the surrounding
    /// `Compaction`.
    SortedRunCompaction {
        /// L0 SSTs (by handle) that will be compacted.
        ssts: Vec<SsTableHandle>,
        /// Existing sorted runs that participate in this compaction.
        sorted_runs: Vec<SortedRun>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Compaction {
    pub(crate) status: CompactionStatus,
    pub(crate) sources: Vec<SourceId>,
    pub(crate) destination: u32,
    pub(crate) job_attempts: Vec<CompactionJob>,
    pub(crate) spec: CompactionSpec,
}

#[derive(Clone)]
/// Canonical, internal record of a compaction plan.
///
/// A plan bundles a stable `id` (ULID), a `compaction_type` (internal vs. external)
/// and the client-facing `Compaction` payload. The compactor tracks plans in two
/// maps: a canonical history keyed by plan id and a runtime view keyed by job id.
///
/// Note: `CompactionPlan` is internal-only; `Compaction` remains the public type
/// that clients observe and does not expose the plan id.
pub(crate) struct CompactionPlan {
    id: Ulid,
    compaction_type: CompactionType,
    pub(crate) compaction: Compaction,
}

impl CompactionPlan {
    pub(crate) fn new(id: Ulid, compaction_type: CompactionType, compaction: Compaction) -> Self {
        Self {
            id,
            compaction_type,
            compaction,
        }
    }

    pub(crate) fn id(&self) -> Ulid {
        self.id
    }

    pub(crate) fn compaction_type(&self) -> &CompactionType {
        &self.compaction_type
    }

    pub(crate) fn compaction(&self) -> &Compaction {
        &self.compaction
    }
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
    pub(crate) fn new(sources: Vec<SourceId>, spec: CompactionSpec, destination: u32) -> Self {
        Self {
            status: Submitted,
            sources,
            destination,
            job_attempts: Vec::new(),
            spec,
        }
    }

    pub(crate) fn get_sorted_runs(db_state: &CoreDbState, sources: &[SourceId]) -> Vec<SortedRun> {
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();

        sources
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .filter_map(|id| srs_by_id.get(&id).map(|t| (*t).clone()))
            .collect()
    }

    pub(crate) fn get_ssts(db_state: &CoreDbState, sources: &[SourceId]) -> Vec<SsTableHandle> {
        let ssts_by_id: HashMap<Ulid, &SsTableHandle> = db_state
            .l0
            .iter()
            .map(|sst| (sst.id.unwrap_compacted_id(), sst))
            .collect();

        sources
            .iter()
            .filter_map(|s| s.maybe_unwrap_sst())
            .filter_map(|ulid| ssts_by_id.get(&ulid).map(|t| (*t).clone()))
            .collect()
    }
}

/// Compaction state persisted in the object store (future work).
///
/// This struct will hold the durable snapshot of compaction plans (queued,
/// in-progress, and completed) keyed by the canonical plan id. Until
/// persistence is implemented, this is used in tests and as a placeholder for
/// wiring to the on-disk format.
#[derive(Clone)]
pub(crate) struct CompactionState {
    #[allow(dead_code)]
    pub(crate) compactor_epoch: u64,
    // active_compaction plan queued, in-progress and completed mapped by compaction id in object store
    #[allow(dead_code)]
    pub(crate) compaction_plans: HashMap<Ulid, CompactionPlan>,
}

impl CompactionState {
    #[allow(dead_code)]
    pub(crate) fn initial() -> Self {
        Self {
            compactor_epoch: 0,
            compaction_plans: HashMap::<Ulid, CompactionPlan>::new(),
        }
    }
}

/// Process-local runtime state owned by the compactor.
///
/// This is the in-memory controller view that a single compactor task uses to:
/// - keep a fresh `DirtyManifest` (view of `CoreDbState`),
/// - track canonical `compaction_plans` by plan id (ULID), and
/// - track `scheduled_compactions` by job id for executions owned by this process.
///
/// It validates submissions, records lifecycle transitions (Submitted → Pending →
/// InProgress → Completed/Failed) and mutates the in-memory manifest when jobs
/// finish.
///
/// Difference vs `CompactionState`:
/// - `CompactorState` is transient, process-local runtime state; it is not
///   persisted and is rebuilt when the process starts.
/// - `CompactionState` is the durable, object-store representation (future work)
///   of compaction plans and their statuses across processes, used for
///   recovery/GC and history.
pub struct CompactorState {
    manifest: DirtyManifest,
    // TODO: Add compaction_state during compaction state persistence implementation
    //compaction_state: DirtyRecord<CompactionState>,
    compaction_plans: HashMap<Ulid, CompactionPlan>,

    // In-memory record of compaction plans scheduled by compactor process mapped by compaction job id
    scheduled_compactions: HashMap<Ulid, CompactionPlan>,
}

impl CompactorState {
    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    pub(crate) fn manifest(&self) -> &DirtyManifest {
        &self.manifest
    }

    pub(crate) fn num_compactions(&self) -> usize {
        self.scheduled_compactions.len()
    }

    pub(crate) fn compactions(&self) -> Vec<Compaction> {
        self.scheduled_compactions
            .values()
            .map(|plan| plan.compaction())
            .cloned()
            .collect()
    }

    pub(crate) fn new(manifest: DirtyManifest) -> Self {
        Self {
            manifest,
            scheduled_compactions: HashMap::new(),
            compaction_plans: HashMap::new(),
        }
    }

    pub(crate) fn submit_compaction(
        &mut self,
        id: Ulid,
        compaction_plan: CompactionPlan,
    ) -> Result<Ulid, SlateDBError> {
        let compaction = compaction_plan.compaction().clone();
        if self
            .scheduled_compactions
            .values()
            .map(|c| c.compaction())
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
        // TODO: Add compactionJob attempt to the object store
        let compaction_id = compaction_plan.id();
        if let Some(plan) = self.compaction_plans.get_mut(&compaction_id) {
            plan.compaction.status = CompactionStatus::Pending;
            self.scheduled_compactions.insert(id, plan.clone());
        } else {
            error!(
                "compaction plan not found [compaction_id={}]",
                compaction_id
            );
            return Err(SlateDBError::InvalidCompaction);
        }
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
            sequence_tracker: remote_manifest.core.sequence_tracker,
        };
        remote_manifest.core = merged;
        self.manifest = remote_manifest;
    }

    pub(crate) fn compaction_submitted(&mut self, compaction_plan: CompactionPlan) {
        self.compaction_plans
            .insert(compaction_plan.id(), compaction_plan);
    }

    pub(crate) fn remove(&mut self, id: &Ulid) {
        self.compaction_plans.remove(id);
    }

    pub(crate) fn compaction_started(&mut self, id: Ulid) {
        if let Some(sched) = self.scheduled_compactions.get(&id) {
            let plan_id = sched.id();
            if let Some(plan) = self.compaction_plans.get_mut(&plan_id) {
                plan.compaction.status = CompactionStatus::InProgress; // update canonical entry
            } else {
                error!("compaction plan not found [compaction_id={}]", plan_id);
            }
        } else {
            error!("scheduled compaction job not found [job_id={}]", id);
        }
    }

    pub(crate) fn finish_failed_compaction(&mut self, id: Ulid) {
        // TODO: Add compaction plan to object store with Failed status
        if let Some(sched) = self.scheduled_compactions.get(&id) {
            let plan_id = sched.id();
            if let Some(plan) = self.compaction_plans.get_mut(&plan_id) {
                plan.compaction.status = CompactionStatus::Failed; // update canonical entry
            } else {
                error!("compaction plan not found [compaction_id={}]", plan_id);
            }
            self.scheduled_compactions.remove(&id);
            // This should be removed once persistence is implemented since removal be take care by garbage collection
            self.compaction_plans.remove(&plan_id);
        } else {
            error!("scheduled compaction job not found [job_id={}]", id);
        }
    }

    pub(crate) fn finish_compaction(&mut self, id: Ulid, output_sr: SortedRun) {
        if let Some(compaction_plan) = self.scheduled_compactions.get(&id) {
            let compaction = compaction_plan.compaction();
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
            // TODO: Add compaction plan to object store with Completed status
            if let Some(sched) = self.scheduled_compactions.get(&id) {
                let plan_id = sched.id();
                if let Some(plan) = self.compaction_plans.get_mut(&plan_id) {
                    plan.compaction.status = CompactionStatus::Completed; // update canonical entry
                } else {
                    error!("compaction plan not found [compaction_id={}]", plan_id);
                }
                self.scheduled_compactions.remove(&id);
                // This should be removed once persistence is implemented since removal be take care by garbage collection
                self.compaction_plans.remove(&plan_id);
            } else {
                error!("scheduled compaction job not found [job_id={}]", id);
            }
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
    fn test_should_register_compaction_as_submitted() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());

        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = build_l0_compaction(&state.db_state().l0, 0);

        // when:
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let id = state
            .submit_compaction(compaction_job_id, compaction_plan)
            .unwrap();

        // then:
        assert_eq!(id, compaction_job_id);
        assert_eq!(state.compactions().len(), 1);
        assert_eq!(
            state.compactions().first().unwrap().status,
            CompactionStatus::Pending
        );
    }

    #[test]
    fn test_should_update_dbstate_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = build_l0_compaction(&before_compaction.l0, 0);
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let id = state
            .submit_compaction(compaction_job_id, compaction_plan)
            .unwrap();

        assert_eq!(id, compaction_job_id);

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
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = build_l0_compaction(&before_compaction.l0, 0);
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let id = state
            .submit_compaction(compaction_job_id, compaction_plan)
            .unwrap();

        assert_eq!(id, compaction_job_id);

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
        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: original_l0s.clone().into(),
            sorted_runs: vec![],
        };
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = Compaction::new(
            vec![Sst(original_l0s.back().unwrap().id.unwrap_compacted_id())],
            spec,
            0,
        );
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let id = state
            .submit_compaction(compaction_job_id, compaction_plan)
            .unwrap();

        assert_eq!(compaction_job_id, id);

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
        let (os, mut sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: original_l0s.clone().into(),
            sorted_runs: vec![],
        };
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());

        let compaction = Compaction::new(
            original_l0s
                .iter()
                .map(|h| Sst(h.id.unwrap_compacted_id()))
                .collect(),
            spec,
            0,
        );
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let id = state
            .submit_compaction(compaction_job_id, compaction_plan)
            .unwrap();

        assert_eq!(compaction_job_id, id);

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
    fn test_should_submit_correct_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: original_l0s.clone().into(),
            sorted_runs: vec![],
        };
        let id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = Compaction::new(
            original_l0s
                .iter()
                .enumerate()
                .filter(|(i, _e)| i > &2usize)
                .map(|(_i, x)| Sst(x.id.unwrap_compacted_id()))
                .collect::<Vec<SourceId>>(),
            spec,
            0,
        );
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let result = state.submit_compaction(id, compaction_plan);
        // then:
        assert!(result.is_ok());
        let result_id = result.unwrap();
        assert_eq!(result_id, id);
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
        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: original_l0s.clone().into(),
            sorted_runs: original_srs.clone(),
        };

        let id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = Compaction::new(sources, spec, 0);
        let compaction_plan =
            CompactionPlan::new(compaction_id, CompactionType::Internal, compaction);
        state.compaction_submitted(compaction_plan.clone());
        let result = state.submit_compaction(id, compaction_plan);

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
        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: ssts.clone().into(),
            sorted_runs: vec![],
        };
        Compaction::new(sources, spec, dst)
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
