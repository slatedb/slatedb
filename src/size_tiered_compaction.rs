use crate::compactor::CompactionSchedulerFactory;
use crate::compactor_state::SourceId::Sst;
use crate::compactor_state::{
    CompactionWriter, CompactorStateListener, CompactorStateReader, SourceId,
};
use std::rc::Rc;

pub(crate) struct SizeTieredCompactionScheduler {
    state: Rc<dyn CompactorStateReader>,
    compaction_writer: Rc<dyn CompactionWriter>,
}

impl CompactorStateListener for SizeTieredCompactionScheduler {
    fn on_writer_db_state_update(&self) {
        self.maybe_schedule_compaction();
    }

    fn on_compactor_db_state_update(&self) {
        self.maybe_schedule_compaction();
    }
}

impl SizeTieredCompactionScheduler {
    fn maybe_schedule_compaction(&self) {
        let db_state = self.state.db_state();
        // for now, just compact l0 down to a new sorted run each time
        if db_state.l0.len() >= 4 {
            let next_sr = db_state.compacted.first().map_or(0, |r| r.id);
            let sources: Vec<SourceId> = db_state
                .l0
                .iter()
                .map(|h| Sst(h.id.unwrap_compacted_id()))
                .collect();
            let result = self.compaction_writer.submit_compaction(sources, next_sr);
            if result.is_err() {
                // todo: log me
            }
        }
    }
}

pub(crate) struct SizeTieredCompactionSchedulerFactory {}

impl CompactionSchedulerFactory for SizeTieredCompactionSchedulerFactory {
    fn create(
        &self,
        state: Rc<dyn CompactorStateReader>,
        compaction_writer: Rc<dyn CompactionWriter>,
    ) -> Rc<dyn CompactorStateListener> {
        Rc::new(SizeTieredCompactionScheduler {
            state,
            compaction_writer,
        })
    }
}
