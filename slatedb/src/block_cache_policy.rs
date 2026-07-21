//! Block cache policy for controlling which SST components are cached.

/// A decoded SST component that can be inserted into the block cache after an
/// SST write.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CacheComponent {
    Data,
    Filters,
    Index,
    Stats,
}

/// Controls block-cache insertion for memtable flush and compaction output.
///
// TODO: add control over when reads go through the block cache, e.g. for
// probing during compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockCachePolicy {
    flush_components: Vec<CacheComponent>,
    compaction_output_components: Vec<CacheComponent>,
}

impl BlockCachePolicy {
    /// Sets the components requested for insertion after a memtable flush.
    /// An empty slice disables insertion.
    pub fn with_flush_components(mut self, components: &[CacheComponent]) -> Self {
        self.flush_components = components.to_vec();
        self
    }

    /// Sets the components requested for insertion as compaction output is
    /// written. An empty slice disables insertion.
    pub fn with_compaction_output_components(mut self, components: &[CacheComponent]) -> Self {
        self.compaction_output_components = components.to_vec();
        self
    }

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

    #[test]
    fn setters_replace_components() {
        let policy = BlockCachePolicy::default()
            .with_flush_components(&[CacheComponent::Stats])
            .with_compaction_output_components(&[CacheComponent::Index, CacheComponent::Filters]);

        assert_eq!(policy.flush_components, &[CacheComponent::Stats]);
        assert_eq!(
            policy.compaction_output_components,
            &[CacheComponent::Index, CacheComponent::Filters]
        );
    }
}
