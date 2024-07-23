use crate::compactor::CompactionScheduler;
use crate::compactor_state::SourceId::Sst;
use crate::compactor_state::{Compaction, CompactorState, SourceId};

pub(crate) struct SizeTieredCompactionScheduler {}

impl CompactionScheduler for SizeTieredCompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction> {
        let db_state = state.db_state();
        // for now, just compact l0 down to a new sorted run each time
        let mut compactions = Vec::new();
        if db_state.l0.len() >= 4 {
            let next_sr = db_state.compacted.first().map_or(0, |r| r.id);
            let sources: Vec<SourceId> = db_state
                .l0
                .iter()
                .map(|h| Sst(h.id.unwrap_compacted_id()))
                .collect();
            compactions.push(Compaction::new(sources, next_sr));
        }
        compactions
    }
}
