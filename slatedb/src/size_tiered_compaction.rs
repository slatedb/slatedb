use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::Peekable;
use std::slice::Iter;

use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{CompactorJobRequest, CompactorState, SourceId};
use crate::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use crate::db_state::CoreDbState;

use crate::error::Error;
use log::warn;

const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 4;

#[derive(Clone, Debug)]
struct CompactionSource {
    source: SourceId,
    size: u64,
}

/// Checks a candidate compaction to make sure that it does not conflict
/// with any other ongoing compactions. A compaction conflict if it uses a
/// source (SST or SR) that is already in use by a running compaction.
pub(crate) struct ConflictChecker {
    sources_used: HashSet<SourceId>,
}

impl ConflictChecker {
    fn new(compactions: &[CompactorJobRequest]) -> Self {
        let mut checker = Self {
            sources_used: HashSet::new(),
        };
        for compaction in compactions.iter() {
            checker.add_compaction(compaction);
        }
        checker
    }

    fn check_compaction(&self, sources: &VecDeque<CompactionSource>, dst: u32) -> bool {
        for source in sources.iter() {
            if self.sources_used.contains(&source.source) {
                return false;
            }
        }
        let dst = SourceId::SortedRun(dst);
        if self.sources_used.contains(&dst) {
            return false;
        }
        true
    }

    fn add_compaction(&mut self, compaction: &CompactorJobRequest) {
        for source in compaction.sources().iter() {
            self.sources_used.insert(source.clone());
        }
        self.sources_used
            .insert(SourceId::SortedRun(compaction.destination()));
    }
}

/// Checks a candidate compaction to ensure that the db is not compacting smaller
/// ssts too quickly. First, the checker estimates the size of the output of a candidate compaction.
/// Then, it sees whether there are already lots of sorted runs that are sized similarly to the
/// output of the candidate compaction. If there are, then it rejects the compaction. To estimate
/// the size of the output, the checker simply sums up the sizes of the sources. To look for
/// similarly sized runs, the db uses the same include_size_threshold as the compaction scheduler.
/// It rejects a compaction if there are more than max_compaction_sources that are <= output
/// size * include_size_threshold.
struct BackpressureChecker {
    include_size_threshold: f32,
    max_compaction_sources: usize,
    longest_compactable_runs_by_sr: HashMap<u32, VecDeque<CompactionSource>>,
}

impl BackpressureChecker {
    fn new(
        include_size_threshold: f32,
        max_compaction_sources: usize,
        srs: &[CompactionSource],
    ) -> Self {
        let mut longest_compactable_runs_by_sr = HashMap::new();
        let mut srs_iter = srs.iter().peekable();
        while let Some(sr) = srs_iter.peek() {
            let sr = sr.source.unwrap_sorted_run();
            let compactable_run = SizeTieredCompactionScheduler::build_compactable_run(
                include_size_threshold,
                srs_iter.clone(),
                None,
            );
            longest_compactable_runs_by_sr.insert(sr, compactable_run);
            srs_iter.next();
        }
        Self {
            include_size_threshold,
            max_compaction_sources,
            longest_compactable_runs_by_sr,
        }
    }

    fn check_compaction(
        &self,
        sources: &VecDeque<CompactionSource>,
        next_sr: Option<&CompactionSource>,
    ) -> bool {
        // checks that we are not creating a run that itself needs to be compacted
        // with lots of runs. If we are, we should wait till those runs are compacted
        let estimated_result_size: u64 = sources.iter().map(|src| src.size).sum();
        if let Some(next_sr) = next_sr {
            let estimated_result_sz = estimated_result_size;
            if next_sr.size <= ((estimated_result_sz as f32) * self.include_size_threshold) as u64 {
                // the result of this compaction can be compacted with the next sr. Check to see
                // if there's already a max sized list of compactable runs there
                if self
                    .longest_compactable_runs_by_sr
                    .get(&next_sr.source.unwrap_sorted_run())
                    .map(|r| r.len())
                    .unwrap_or(0)
                    >= self.max_compaction_sources
                {
                    return false;
                }
            }
        }
        true
    }
}

struct CompactionChecker {
    conflict_checker: ConflictChecker,
    backpressure_checker: BackpressureChecker,
}

impl CompactionChecker {
    fn new(conflict_checker: ConflictChecker, backpressure_checker: BackpressureChecker) -> Self {
        Self {
            conflict_checker,
            backpressure_checker,
        }
    }

    fn check_compaction(
        &self,
        sources: &VecDeque<CompactionSource>,
        dst: u32,
        next_sr: Option<&CompactionSource>,
    ) -> bool {
        if !self.conflict_checker.check_compaction(sources, dst) {
            return false;
        }
        if !self.backpressure_checker.check_compaction(sources, next_sr) {
            return false;
        }
        true
    }
}

/// Implements a size-tiered compaction scheduler. The scheduler works by scheduling compaction for
/// one of:
/// - options.min_compaction_sources L0 SSTs
/// - A series of at least options.min_compaction_sources sorted runs where the size of the largest
///   run <= options.include_size_threshold * size of the smallest run.
///
/// The scheduler ensures that a compaction has at most options.max_compaction_sources. The scheduler
/// rejects compactions that violate one of the compaction checkers defined above.
pub(crate) struct SizeTieredCompactionScheduler {
    options: SizeTieredCompactionSchedulerOptions,
    max_concurrent_compactions: usize,
}

impl Default for SizeTieredCompactionScheduler {
    fn default() -> Self {
        Self::new(
            SizeTieredCompactionSchedulerOptions::default(),
            DEFAULT_MAX_CONCURRENT_COMPACTIONS,
        )
    }
}

impl CompactionScheduler for SizeTieredCompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<CompactorJobRequest> {
        let mut compactions = Vec::new();
        let db_state = state.db_state();

        let (l0, srs) = self.compaction_sources(db_state);

        let conflict_checker = ConflictChecker::new(&state.requests());
        let backpressure_checker = BackpressureChecker::new(
            self.options.include_size_threshold,
            self.options.max_compaction_sources,
            &srs,
        );
        let mut checker = CompactionChecker::new(conflict_checker, backpressure_checker);

        while state.requests().len() + compactions.len() < self.max_concurrent_compactions {
            let Some(compaction) = self.pick_next_compaction(&l0, &srs, &checker) else {
                break;
            };
            checker.conflict_checker.add_compaction(&compaction);
            compactions.push(compaction);
        }

        compactions
    }

    fn validate_compaction(
        &self,
        state: &CompactorState,
        compaction: &CompactorJobRequest,
    ) -> Result<(), crate::error::Error> {
        // Logical order of sources: [L0 (newest → oldest), then SRs (highest id → 0)]
        let sources_logical_order: Vec<SourceId> = state
            .db_state()
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .chain(
                state
                    .db_state()
                    .compacted
                    .iter()
                    .map(|sr| SourceId::SortedRun(sr.id)),
            )
            .collect();

        // Validate if the compaction sources are strictly consecutive elements in the db_state sources
        if !sources_logical_order
            .windows(compaction.sources().len())
            .any(|w| w == compaction.sources().as_slice())
        {
            warn!("submitted compaction is not a consecutive series of sources from db state: {:?} {:?}",
            compaction.sources(), sources_logical_order);
            return Err(Error::invalid(
                "non-consecutive compaction sources".to_string(),
            ));
        }

        let has_sr = compaction
            .sources()
            .iter()
            .any(|s| matches!(s, SourceId::SortedRun(_)));

        if has_sr {
            // Must merge into the lowest-id SR among sources
            let min_sr = compaction
                .sources()
                .iter()
                .filter_map(|s| s.maybe_unwrap_sorted_run())
                .min()
                .expect("at least one SR in sources");
            if compaction.destination() != min_sr {
                warn!(
                    "destination does not match lowest-id SR among sources: {:?} {:?}",
                    compaction.destination(),
                    min_sr
                );
                return Err(Error::invalid(
                    "destination not the lowest-id SR among sources".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl SizeTieredCompactionScheduler {
    pub(crate) fn new(
        options: SizeTieredCompactionSchedulerOptions,
        max_concurrent_compactions: usize,
    ) -> Self {
        Self {
            options,
            max_concurrent_compactions,
        }
    }

    fn pick_next_compaction(
        &self,
        l0: &[CompactionSource],
        srs: &[CompactionSource],
        checker: &CompactionChecker,
    ) -> Option<CompactorJobRequest> {
        // compact l0s if required
        let l0_candidates: VecDeque<_> = l0.iter().cloned().collect();
        if let Some(mut l0_candidates) = self.clamp_min(l0_candidates) {
            l0_candidates = self.clamp_max(l0_candidates);
            let dst = srs
                .first()
                .map_or(0, |sr| sr.source.unwrap_sorted_run() + 1);
            let next_sr = srs.first();
            if checker.check_compaction(&l0_candidates, dst, next_sr) {
                return Some(self.create_compaction(l0_candidates, dst));
            }
        }

        // try to compact the lower levels
        let mut srs_iter = srs.iter().peekable();
        while srs_iter.peek().is_some() {
            let compactable_run = Self::build_compactable_run(
                self.options.include_size_threshold,
                srs_iter.clone(),
                Some(checker),
            );
            let compactable_run = self.clamp_min(compactable_run);
            if let Some(mut compactable_run) = compactable_run {
                compactable_run = self.clamp_max(compactable_run);
                let dst = compactable_run
                    .back()
                    .expect("expected non-empty compactable run")
                    .source
                    .unwrap_sorted_run();
                return Some(self.create_compaction(compactable_run, dst));
            }
            srs_iter.next();
        }
        None
    }

    fn clamp_min(&self, sources: VecDeque<CompactionSource>) -> Option<VecDeque<CompactionSource>> {
        if sources.len() < self.options.min_compaction_sources {
            return None;
        }
        Some(sources)
    }

    fn clamp_max(&self, mut sources: VecDeque<CompactionSource>) -> VecDeque<CompactionSource> {
        while sources.len() > self.options.max_compaction_sources {
            sources.pop_front();
        }
        sources
    }

    fn create_compaction(
        &self,
        sources: VecDeque<CompactionSource>,
        dst: u32,
    ) -> CompactorJobRequest {
        let sources: Vec<SourceId> = sources.iter().map(|src| src.source.clone()).collect();
        CompactorJobRequest::new(sources, dst)
    }

    // looks for a series of sorted runs with similar sizes and assemble to a vecdequeue,
    // optionally validating the resulting series
    fn build_compactable_run(
        size_threshold: f32,
        mut sources: Peekable<Iter<CompactionSource>>,
        checker: Option<&CompactionChecker>,
    ) -> VecDeque<CompactionSource> {
        let mut compactable_runs = VecDeque::new();
        let mut maybe_min_sz = None;
        while let Some(src) = sources.next() {
            if let Some(min_sz) = maybe_min_sz {
                if src.size > ((min_sz as f32) * size_threshold) as u64 {
                    break;
                }
                maybe_min_sz = Some(min(min_sz, src.size));
            } else {
                maybe_min_sz = Some(src.size);
            }
            compactable_runs.push_back(src.clone());
            if let Some(checker) = checker {
                let dst = src.source.unwrap_sorted_run();
                let next_sr = sources.peek().cloned().cloned();
                if !checker.check_compaction(&compactable_runs, dst, next_sr.as_ref()) {
                    compactable_runs.pop_back();
                    break;
                }
            }
        }
        compactable_runs
    }

    fn compaction_sources(
        &self,
        db_state: &CoreDbState,
    ) -> (Vec<CompactionSource>, Vec<CompactionSource>) {
        (
            db_state
                .l0
                .iter()
                .map(|l0| CompactionSource {
                    source: SourceId::Sst(l0.id.unwrap_compacted_id()),
                    size: l0.estimate_size(),
                })
                .collect(),
            db_state
                .compacted
                .iter()
                .map(|sr| CompactionSource {
                    source: SourceId::SortedRun(sr.id),
                    size: sr.estimate_size(),
                })
                .collect(),
        )
    }
}

#[derive(Default)]
pub struct SizeTieredCompactionSchedulerSupplier {
    options: SizeTieredCompactionSchedulerOptions,
}

impl SizeTieredCompactionSchedulerSupplier {
    pub const fn new(options: SizeTieredCompactionSchedulerOptions) -> Self {
        Self { options }
    }
}

impl CompactionSchedulerSupplier for SizeTieredCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        compactor_options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(SizeTieredCompactionScheduler::new(
            self.options.clone(),
            compactor_options.max_concurrent_compactions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::compactor::CompactionScheduler;

    use crate::clock::DefaultSystemClock;
    use crate::compactor_state::{CompactorJob, CompactorJobRequest, CompactorState, SourceId};
    use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::seq_tracker::SequenceTracker;
    use crate::size_tiered_compaction::SizeTieredCompactionScheduler;
    use crate::utils::IdGenerator;
    use crate::DbRand;
    use std::sync::Arc;

    #[test]
    fn test_should_compact_l0s_to_first_sr() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let state =
            create_compactor_state(create_db_state(l0.iter().cloned().collect(), Vec::new()));

        // when:
        let requests: Vec<CompactorJobRequest> = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(requests.len(), 1);
        let request = requests.first().unwrap();
        let expected_sources: Vec<SourceId> = l0
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        assert_eq!(request.sources(), &expected_sources);
        assert_eq!(request.destination(), 0);
    }

    #[test]
    fn test_should_compact_l0s_to_new_sr() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let state = create_compactor_state(create_db_state(
            l0.iter().cloned().collect(),
            vec![create_sr2(10, 2), create_sr2(0, 2)],
        ));

        // when:
        let requests: Vec<CompactorJobRequest> = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(requests.len(), 1);
        let request = requests.first().unwrap();
        assert_eq!(request.destination(), 11);
    }

    #[test]
    fn test_should_not_compact_l0s_if_fewer_than_min_threshold() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = [create_sst(1), create_sst(1), create_sst(1)];
        let state = create_compactor_state(create_db_state(l0.iter().cloned().collect(), vec![]));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 0);
    }

    #[test]
    fn test_should_compact_srs_if_enough_with_similar_size() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr2(0, 2),
            ],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 1);
        let compaction = compactions.first().unwrap();
        let expected_compaction = create_sr_compaction(vec![4, 3, 2, 1, 0]);

        assert_eq!(compaction.clone(), expected_compaction,)
    }

    #[test]
    fn test_should_only_include_srs_if_with_similar_size() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr2(0, 10),
            ],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 1);

        let compaction = compactions.first().unwrap();
        let expected_compaction = create_sr_compaction(vec![4, 3, 2, 1]);
        assert_eq!(compaction.clone(), expected_compaction)
    }

    #[test]
    fn test_should_not_schedule_compaction_for_source_that_is_already_compacting() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let mut state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr2(0, 2),
            ],
        ));
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let request = create_sr_compaction(vec![3, 2, 1, 0]);
        let compactor_job = CompactorJob::new(compaction_id, request);

        state.submit_job(compactor_job.clone());

        let id = state
            .submit_compaction(compaction_job_id, compactor_job)
            .unwrap();

        assert_eq!(id, compaction_job_id);

        // when:
        let requests = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(requests.len(), 0);
    }

    #[test]
    fn test_should_not_compact_srs_if_fewer_than_min_threshold() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![create_sr2(2, 2), create_sr2(1, 2), create_sr4(0, 2)],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert!(compactions.is_empty());
    }

    #[test]
    fn test_should_clamp_compaction_size() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![
                create_sr2(11, 2),
                create_sr2(10, 2),
                create_sr2(9, 2),
                create_sr2(8, 2),
                create_sr2(7, 2),
                create_sr2(6, 2),
                create_sr2(5, 2),
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr4(0, 2),
            ],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);
        let expected_compaction = create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0]);

        // then:
        assert_eq!(compactions.len(), 1);
        assert_eq!(compactions.first().unwrap(), &expected_compaction,);
    }

    #[test]
    fn test_should_apply_backpressure() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let mut state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![
                create_sr2(11, 2),
                create_sr2(10, 2),
                create_sr2(9, 2),
                create_sr2(8, 2),
                create_sr2(7, 2),
                create_sr2(6, 2),
                create_sr2(5, 2),
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr4(0, 2),
            ],
        ));
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let request = create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0]);
        let compactor_job = CompactorJob::new(compaction_id, request);
        state.submit_job(compactor_job.clone());
        let id = state
            .submit_compaction(compaction_job_id, compactor_job)
            .unwrap();

        assert_eq!(id, compaction_job_id);

        // when:
        let requests = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert!(requests.is_empty());
    }

    #[test]
    fn test_should_apply_backpressure_for_l0s() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let mut state = create_compactor_state(create_db_state(
            l0.iter().cloned().collect(),
            vec![
                create_sr2(7, 2),
                create_sr2(6, 2),
                create_sr2(5, 2),
                create_sr2(4, 2),
                create_sr2(3, 2),
                create_sr2(2, 2),
                create_sr2(1, 2),
                create_sr4(0, 2),
            ],
        ));
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let compaction_job_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compactor_job = CompactorJob::new(
            compaction_id,
            create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0]),
        );
        state.submit_job(compactor_job.clone());
        let id = state
            .submit_compaction(compaction_job_id, compactor_job)
            .unwrap();

        assert_eq!(id, compaction_job_id);

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert!(compactions.is_empty());
    }

    #[test]
    fn test_should_return_multiple_compactions() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let state = create_compactor_state(create_db_state(
            l0.iter().cloned().collect(),
            vec![
                create_sr2(10, 2),
                create_sr2(9, 2),
                create_sr2(8, 2),
                create_sr2(7, 2),
                create_sr4(3, 16),
                create_sr4(2, 16),
                create_sr4(1, 16),
                create_sr4(0, 16),
            ],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 3);
        let compaction = compactions.first().unwrap();
        let expected_l0_compaction = create_l0_compaction(&l0, 11);
        assert_eq!(compaction.clone(), expected_l0_compaction);
        let compaction = compactions.get(1).unwrap();
        let expected_sr0_compaction = create_sr_compaction(vec![10, 9, 8, 7]);
        assert_eq!(compaction.clone(), expected_sr0_compaction);
        let compaction = compactions.get(2).unwrap();
        let expected_sr1_compaction = create_sr_compaction(vec![3, 2, 1, 0]);
        assert_eq!(compaction.clone(), expected_sr1_compaction);
    }

    #[test]
    fn test_should_submit_invalid_compaction_wrong_order() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = VecDeque::from(vec![create_sst(1), create_sst(1), create_sst(1)]);
        let state = create_compactor_state(create_db_state(l0.clone(), Vec::new()));

        let mut l0_sst = l0.clone();
        let last_sst = l0_sst.pop_back();
        l0_sst.push_front(last_sst.unwrap());
        // when:
        let compaction = create_l0_compaction(l0_sst.make_contiguous(), 0);
        let result = scheduler.validate_compaction(&state, &compaction);

        // then:
        assert!(result.is_err());
    }

    #[test]
    fn test_should_submit_invalid_compaction_skipped_sst() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let l0 = VecDeque::from(vec![create_sst(1), create_sst(1), create_sst(1)]);
        let state = create_compactor_state(create_db_state(l0.clone(), Vec::new()));

        let mut l0_sst = l0.clone();
        let last_sst = l0_sst.pop_back().unwrap();
        l0_sst.push_front(last_sst);
        l0_sst.pop_back();
        // when:
        let compaction = create_l0_compaction(l0_sst.make_contiguous(), 0);
        let result = scheduler.validate_compaction(&state, &compaction);

        // then:
        assert!(result.is_err());
    }

    #[test]
    fn test_should_submit_invalid_compaction_with_sr() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state =
            create_compactor_state(create_db_state(VecDeque::new(), vec![create_sr2(0, 2)]));

        let mut l0 = state.db_state().l0.clone();
        let request = create_l0_compaction(l0.make_contiguous(), 0);
        let mut new_sources: Vec<SourceId> = request.sources().clone();
        new_sources.push(SourceId::SortedRun(5));
        let new_request = CompactorJobRequest::new(new_sources, request.destination());
        // when:
        let result = scheduler.validate_compaction(&state, &new_request);

        // then:
        assert!(result.is_err());
    }

    #[test]
    fn test_should_submit_valid_compaction_with_srs() {
        // given:
        let scheduler = SizeTieredCompactionScheduler::default();
        let state = create_compactor_state(create_db_state(
            VecDeque::new(),
            vec![create_sr2(0, 2), create_sr2(1, 2)],
        ));

        let srs = state.db_state().compacted.clone();
        let compaction = create_sr_compaction(srs.iter().map(|sr| sr.id).collect());
        // when:
        let result = scheduler.validate_compaction(&state, &compaction);

        // then:
        assert!(result.is_err());
    }

    fn create_sst(size: u64) -> SsTableHandle {
        let info = SsTableInfo {
            first_key: None,
            index_offset: size,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
        };
        SsTableHandle::new(SsTableId::Compacted(ulid::Ulid::new()), info)
    }

    fn create_sr2(id: u32, size: u64) -> SortedRun {
        create_sr(id, size / 2, 2)
    }

    fn create_sr4(id: u32, size: u64) -> SortedRun {
        create_sr(id, size / 4, 4)
    }

    fn create_sr(id: u32, sst_size: u64, num_ssts: usize) -> SortedRun {
        let ssts: Vec<SsTableHandle> = (0..num_ssts).map(|_| create_sst(sst_size)).collect();
        SortedRun { id, ssts }
    }

    fn create_db_state(l0: VecDeque<SsTableHandle>, srs: Vec<SortedRun>) -> CoreDbState {
        CoreDbState {
            initialized: true,
            l0_last_compacted: None,
            l0,
            compacted: srs,
            next_wal_sst_id: 0,
            replay_after_wal_id: 0,
            last_l0_seq: 0,
            last_l0_clock_tick: 0,
            checkpoints: vec![],
            wal_object_store_uri: None,
            recent_snapshot_min_seq: 0,
            sequence_tracker: SequenceTracker::new(),
        }
    }

    fn create_compactor_state(db_state: CoreDbState) -> CompactorState {
        let mut dirty = new_dirty_manifest();
        dirty.core = db_state;
        CompactorState::new(dirty)
    }

    fn create_l0_compaction(l0: &[SsTableHandle], dst: u32) -> CompactorJobRequest {
        let sources: Vec<SourceId> = l0
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();

        CompactorJobRequest::new(sources, dst)
    }

    fn create_sr_compaction(srs: Vec<u32>) -> CompactorJobRequest {
        let sources: Vec<SourceId> = srs.iter().map(|sr| SourceId::SortedRun(*sr)).collect();
        CompactorJobRequest::new(sources, *srs.last().unwrap())
    }
}
