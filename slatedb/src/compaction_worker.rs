use std::sync::Arc;

use crate::config::CompactionWorkerOptions;

/// The compactor worker is responsible for taking groups of sorted runs (this doc uses the term
/// sorted run to refer to both sorted runs and l0 ssts) and compacting them together to
/// reduce space amplification (by removing old versions of rows that have been updated/deleted)
/// and read amplification (by reducing the number of sorted runs that need to be searched on
/// a read). This class is a single worker process that claims jobs according to the protocols
/// specified in the distributed compaction RFC (RFC 25)
#[derive(Clone)]
pub struct CompactionWorker {
    options: Arc<CompactionWorkerOptions>,
}

impl CompactionWorker {
    #[cfg_attr(not(feature = "compaction_filters"), allow(clippy::too_many_arguments))]
    pub(crate) fn new(options: CompactionWorkerOptions) -> Self {
        Self {
            options: Arc::new(options),
        }
    }

    pub fn run() -> Self {
        unimplemented!()
    }
}
