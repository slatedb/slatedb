//! Block cache policy for controlling which SST components are cached.

use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use crate::bytes_range::BytesRange;

/// A decoded SST component that can be inserted into the block cache after an
/// SST write.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CacheComponent {
    /// Data blocks whose key span overlaps the supplied key range.
    Data((Bound<Bytes>, Bound<Bytes>)),
    Filters,
    Index,
    Stats,
}

impl CacheComponent {
    /// Convenience constructor for [`CacheComponent::Data`] that accepts any
    /// [`RangeBounds`], mirroring the `Db::scan` signature. Pass `..` to
    /// select all data blocks.
    pub fn data<K, T>(range: T) -> Self
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        CacheComponent::Data((start, end))
    }
}

/// Whether any [`CacheComponent::Data`] range in `components` overlaps a data
/// block whose first and last key are `key_span`.
pub(crate) fn should_cache_data_block(
    components: &[CacheComponent],
    key_span: &(Bytes, Bytes),
) -> bool {
    components.iter().any(|component| {
        let CacheComponent::Data(range) = component else {
            return false;
        };
        let (first_key, last_key) = key_span;
        let Some(range) = BytesRange::try_new(range.0.clone(), range.1.clone()) else {
            return false;
        };
        let span = BytesRange::new(
            Bound::Included(first_key.clone()),
            Bound::Included(last_key.clone()),
        );
        range.intersect(&span).is_some()
    })
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
                CacheComponent::data::<&[u8], _>(..),
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
                CacheComponent::data::<&[u8], _>(..),
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
    fn should_cache_data_block_by_key_span_overlap() {
        let components = [CacheComponent::data(b"c".as_slice()..b"f".as_slice())];
        let span = |first: &[u8], last: &[u8]| {
            (Bytes::copy_from_slice(first), Bytes::copy_from_slice(last))
        };

        assert!(should_cache_data_block(&components, &span(b"a", b"c")));
        assert!(should_cache_data_block(&components, &span(b"d", b"e")));
        assert!(should_cache_data_block(&components, &span(b"e", b"z")));
        // end bound is exclusive
        assert!(!should_cache_data_block(&components, &span(b"f", b"z")));
        assert!(!should_cache_data_block(&components, &span(b"a", b"b")));

        assert!(!should_cache_data_block(
            &[CacheComponent::Index],
            &span(b"d", b"e")
        ));
        assert!(should_cache_data_block(
            &[CacheComponent::data::<&[u8], _>(..)],
            &span(b"a", b"b")
        ));
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
