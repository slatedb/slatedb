use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::Peekable;
use std::slice::Iter;

use crate::compactor::CompactionScheduler;
use crate::compactor_state::{Compaction, CompactorState, SourceId};
use crate::config::{CompactionSchedulerSupplier, SizeTieredCompactionSchedulerOptions};
use crate::db_state::CoreDbState;

const MAX_IN_FLIGHT_COMPACTIONS: usize = 4;

#[derive(Clone)]
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
    fn new(compactions: &[Compaction]) -> Self {
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

    fn add_compaction(&mut self, compaction: &Compaction) {
        for source in compaction.sources.iter() {
            self.sources_used.insert(source.clone());
        }
        self.sources_used
            .insert(SourceId::SortedRun(compaction.destination));
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
}

impl CompactionScheduler for SizeTieredCompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction> {
        let mut compactions = Vec::new();

        let (l0, srs) = self.compaction_sources(state.db_state());

        let conflict_checker = ConflictChecker::new(&state.compactions());
        let backpressure_checker = BackpressureChecker::new(
            self.options.include_size_threshold,
            self.options.max_compaction_sources,
            &srs,
        );
        let mut checker = CompactionChecker::new(conflict_checker, backpressure_checker);

        while state.compactions().len() + compactions.len() < MAX_IN_FLIGHT_COMPACTIONS {
            let Some(compaction) = self.pick_next_compaction(&l0, &srs, &checker) else {
                break;
            };
            checker.conflict_checker.add_compaction(&compaction);
            compactions.push(compaction);
        }

        compactions
    }
}

impl SizeTieredCompactionScheduler {
    pub(crate) fn new(options: SizeTieredCompactionSchedulerOptions) -> Self {
        Self { options }
    }

    fn pick_next_compaction(
        &self,
        l0: &[CompactionSource],
        srs: &[CompactionSource],
        checker: &CompactionChecker,
    ) -> Option<Compaction> {
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

    fn create_compaction(&self, sources: VecDeque<CompactionSource>, dst: u32) -> Compaction {
        let sources: Vec<SourceId> = sources.iter().map(|src| src.source.clone()).collect();
        Compaction::new(sources, dst)
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

pub struct SizeTieredCompactionSchedulerSupplier {
    options: SizeTieredCompactionSchedulerOptions,
}

impl SizeTieredCompactionSchedulerSupplier {
    pub const fn new(options: SizeTieredCompactionSchedulerOptions) -> Self {
        Self { options }
    }
}

impl CompactionSchedulerSupplier for SizeTieredCompactionSchedulerSupplier {
    fn compaction_scheduler(&self) -> Box<dyn CompactionScheduler> {
        Box::new(SizeTieredCompactionScheduler::new(self.options.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::compactor::CompactionScheduler;
    use crate::compactor_state::{Compaction, CompactorState, SourceId};
    use crate::config::SizeTieredCompactionSchedulerOptions;
    use crate::db_state::{
        CoreDbState, RowAttribute, SortedRun, SsTableHandle, SsTableId, SsTableInfo,
    };
    use crate::size_tiered_compaction::SizeTieredCompactionScheduler;

    #[test]
    fn test_should_compact_l0s_to_first_sr() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let state =
            create_compactor_state(create_db_state(l0.iter().cloned().collect(), Vec::new()));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 1);
        let compaction = compactions.first().unwrap();
        let expected_sources: Vec<SourceId> = l0
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        assert_eq!(compaction.sources, expected_sources);
        assert_eq!(compaction.destination, 0);
    }

    #[test]
    fn test_should_compact_l0s_to_new_sr() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
        let l0 = vec![create_sst(1), create_sst(1), create_sst(1), create_sst(1)];
        let state = create_compactor_state(create_db_state(
            l0.iter().cloned().collect(),
            vec![create_sr2(10, 2), create_sr2(0, 2)],
        ));

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 1);
        let compaction = compactions.first().unwrap();
        assert_eq!(compaction.destination, 11);
    }

    #[test]
    fn test_should_not_compact_l0s_if_fewer_than_min_threshold() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        assert_eq!(
            compaction.clone(),
            create_sr_compaction(vec![4, 3, 2, 1, 0])
        )
    }

    #[test]
    fn test_should_only_include_srs_if_with_similar_size() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        assert_eq!(compaction.clone(), create_sr_compaction(vec![4, 3, 2, 1]))
    }

    #[test]
    fn test_should_not_schedule_compaction_for_source_that_is_already_compacting() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        state
            .submit_compaction(create_sr_compaction(vec![3, 2, 1, 0]))
            .unwrap();

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert_eq!(compactions.len(), 0);
    }

    #[test]
    fn test_should_not_compact_srs_if_fewer_than_min_threshold() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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

        // then:
        assert_eq!(compactions.len(), 1);
        assert_eq!(
            compactions.first().unwrap(),
            &create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0])
        );
    }

    #[test]
    fn test_should_apply_backpressure() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        state
            .submit_compaction(create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0]))
            .unwrap();

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert!(compactions.is_empty());
    }

    #[test]
    fn test_should_apply_backpressure_for_l0s() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        state
            .submit_compaction(create_sr_compaction(vec![7, 6, 5, 4, 3, 2, 1, 0]))
            .unwrap();

        // when:
        let compactions = scheduler.maybe_schedule_compaction(&state);

        // then:
        assert!(compactions.is_empty());
    }

    #[test]
    fn test_should_return_multiple_compactions() {
        // given:
        let scheduler =
            SizeTieredCompactionScheduler::new(SizeTieredCompactionSchedulerOptions::default());
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
        assert_eq!(compaction, &create_l0_compaction(&l0, 11));
        let compaction = compactions.get(1).unwrap();
        assert_eq!(compaction, &create_sr_compaction(vec![10, 9, 8, 7]));
        let compaction = compactions.get(2).unwrap();
        assert_eq!(compaction, &create_sr_compaction(vec![3, 2, 1, 0]));
    }

    fn create_sst(size: u64) -> SsTableHandle {
        let info = SsTableInfo {
            first_key: None,
            index_offset: size,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
            row_attributes: vec![RowAttribute::Flags],
        };
        SsTableHandle {
            id: SsTableId::Compacted(ulid::Ulid::new()),
            info,
        }
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
            l0_last_compacted: None,
            l0,
            compacted: srs,
            next_wal_sst_id: 0,
            last_compacted_wal_sst_id: 0,
        }
    }

    fn create_compactor_state(db_state: CoreDbState) -> CompactorState {
        CompactorState::new(db_state)
    }

    fn create_l0_compaction(l0: &[SsTableHandle], dst: u32) -> Compaction {
        Compaction::new(
            l0.iter()
                .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
                .collect(),
            dst,
        )
    }

    fn create_sr_compaction(srs: Vec<u32>) -> Compaction {
        Compaction::new(
            srs.iter().map(|sr| SourceId::SortedRun(*sr)).collect(),
            *srs.last().unwrap(),
        )
    }
}
