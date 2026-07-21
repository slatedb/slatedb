//! Block cache policy for controlling which SST components are cached.

/// A decoded SST component that can be inserted into the block cache after an
/// SST write.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CacheComponent {
    Data,
    Filters,
    Index,
    Stats,
}

/// Controls block-cache insertion for memtable flush and compaction output.
// TODO: add setters to configure the flush and compaction-output components.
// TODO: add control over when reads go through the block cache, e.g. for
// probing during compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlockCachePolicy {
    flush_components: Vec<CacheComponent>,
    compaction_output_components: Vec<CacheComponent>,
}

impl BlockCachePolicy {
    pub(crate) fn flush_components(&self) -> &[CacheComponent] {
        &self.flush_components
    }

    pub(crate) fn compaction_output_components(&self) -> &[CacheComponent] {
        &self.compaction_output_components
    }
}

/// The default policy inserts data, index, and filter blocks after a memtable
/// flush, and inserts index and filter blocks as compaction output is written.
impl Default for BlockCachePolicy {
    fn default() -> Self {
        Self {
            flush_components: vec![
                CacheComponent::Data,
                CacheComponent::Index,
                CacheComponent::Filters,
            ],
            compaction_output_components: vec![CacheComponent::Index, CacheComponent::Filters],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_caches_on_flush_and_metadata_on_compaction_output() {
        let policy = BlockCachePolicy::default();

        assert_eq!(
            policy.flush_components(),
            &[
                CacheComponent::Data,
                CacheComponent::Index,
                CacheComponent::Filters
            ]
        );
        assert_eq!(
            policy.compaction_output_components(),
            &[CacheComponent::Index, CacheComponent::Filters]
        );
    }
}
