use crate::stats::{Counter, StatRegistry};
use std::sync::Arc;

macro_rules! db_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("db", $suffix)
    };
}

pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const FILTER_FALSE_POSITIVES: &str = db_stat_name!("filter_false_positives");
pub const FILTER_POSITIVES: &str = db_stat_name!("filter_positives");
pub const FILTER_NEGATIVES: &str = db_stat_name!("filter_negatives");


#[non_exhaustive]
#[derive(Clone, Debug)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<Counter>,
    pub(crate) filter_false_positives: Arc<Counter>,
    pub(crate) filter_positives: Arc<Counter>,
    pub(crate) filter_negatives: Arc<Counter>,
}

impl DbStats {
    pub(crate) fn new(registry: &StatRegistry) -> DbStats {
        let stats = Self {
            immutable_memtable_flushes: Arc::new(Counter::default()),
            filter_false_positives: Arc::new(Counter::default()),
            filter_positives: Arc::new(Counter::default()),
            filter_negatives: Arc::new(Counter::default()),
        };
        registry.register(IMMUTABLE_MEMTABLE_FLUSHES, stats.immutable_memtable_flushes.clone());
        registry.register(FILTER_FALSE_POSITIVES, stats.filter_false_positives.clone());
        registry.register(FILTER_POSITIVES, stats.filter_positives.clone());
        registry.register(FILTER_NEGATIVES, stats.filter_negatives.clone());
        stats
    }
}
