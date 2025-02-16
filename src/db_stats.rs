use crate::stats::{Counter, StatRegistry};
use std::sync::Arc;

macro_rules! db_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("db", $suffix)
    };
}

pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");

#[non_exhaustive]
#[derive(Clone, Debug)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<Counter>,
}

impl DbStats {
    pub(crate) fn new(registry: &StatRegistry) -> DbStats {
        let stats = Self {
            immutable_memtable_flushes: Arc::new(Counter::default()),
        };
        registry.register(
            IMMUTABLE_MEMTABLE_FLUSHES,
            stats.immutable_memtable_flushes.clone(),
        );
        stats
    }
}
