use crate::stats::{Counter, StatRegistry};
use std::sync::Arc;

macro_rules! gc_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("gc", $suffix)
    };
}

pub const GC_MANIFEST_COUNT: &str = gc_stat_name!("manifest_count");
pub const GC_WAL_COUNT: &str = gc_stat_name!("wal_count");
pub const GC_COMPACTED_COUNT: &str = gc_stat_name!("compacted_count");
pub const GC_COUNT: &str = gc_stat_name!("count");

/// Stats for the garbage collector.
///
/// This struct is used to collect stats for the garbage collector.
pub struct GcStats {
    pub gc_manifest_count: Arc<Counter>,
    pub gc_wal_count: Arc<Counter>,
    pub gc_compacted_count: Arc<Counter>,
    pub gc_count: Arc<Counter>,
}

impl GcStats {
    /// Create a new GcStats instance.
    ///
    /// This function creates a new GcStats instance and registers the stats with the given registry.
    ///
    /// ## Arguments
    ///
    /// * `registry`: The `StatRegistry` to register the stats with.
    ///
    /// ## Returns
    ///
    /// * `Self`: The new GcStats instance.
    pub fn new(registry: Arc<StatRegistry>) -> Self {
        let stats = Self {
            gc_manifest_count: Arc::new(Counter::default()),
            gc_wal_count: Arc::new(Counter::default()),
            gc_compacted_count: Arc::new(Counter::default()),
            gc_count: Arc::new(Counter::default()),
        };
        registry.register(GC_MANIFEST_COUNT, stats.gc_manifest_count.clone());
        registry.register(GC_WAL_COUNT, stats.gc_wal_count.clone());
        registry.register(GC_COMPACTED_COUNT, stats.gc_compacted_count.clone());
        registry.register(GC_COUNT, stats.gc_count.clone());
        stats
    }
}
