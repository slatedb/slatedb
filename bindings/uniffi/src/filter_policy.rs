use std::sync::Arc;

use crate::error::{Error, SlateDbError};

/// Expected length of the `FilterContext::Inline` payload.
pub(crate) const FILTER_CONTEXT_INLINE_LEN: usize = 64;

/// Opaque caller-supplied context forwarded to custom filter policies at
/// query time.
///
/// Custom filter policies read this to parametrize their evaluation; built-in
/// policies (including the bloom filter) ignore it.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum FilterContext {
    /// Inline payload. The byte vector must be exactly 64 bytes long. Smaller
    /// or larger payloads are rejected when the context is converted for use.
    Inline { payload: Vec<u8> },
}

impl TryFrom<FilterContext> for slatedb::FilterContext {
    type Error = Error;

    fn try_from(value: FilterContext) -> Result<Self, Self::Error> {
        match value {
            FilterContext::Inline { payload } => {
                let bytes: [u8; FILTER_CONTEXT_INLINE_LEN] =
                    payload.as_slice().try_into().map_err(|_| {
                        Error::from(SlateDbError::InvalidFilterContextPayload {
                            expected: FILTER_CONTEXT_INLINE_LEN,
                            actual: payload.len(),
                        })
                    })?;
                Ok(slatedb::FilterContext::Inline(bytes))
            }
        }
    }
}

/// Identifies the target of a [`PrefixExtractor::prefix_len`] query.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum PrefixTarget {
    /// A complete key, supplied either during SST construction or a point lookup.
    Point { key: Vec<u8> },
    /// A scan prefix supplied during a prefix scan.
    Prefix { prefix: Vec<u8> },
}

impl From<&slatedb::PrefixTarget> for PrefixTarget {
    fn from(value: &slatedb::PrefixTarget) -> Self {
        match value {
            slatedb::PrefixTarget::Point(k) => PrefixTarget::Point { key: k.to_vec() },
            slatedb::PrefixTarget::Prefix(p) => PrefixTarget::Prefix { prefix: p.to_vec() },
        }
    }
}

/// Application-provided prefix extractor used to configure prefix-based
/// bloom filters.
#[uniffi::export(with_foreign)]
pub trait PrefixExtractor: Send + Sync {
    /// Stable identifier for this extractor's configuration. Included in the
    /// bloom filter policy name so filters built with different extractors
    /// are never mismatched.
    fn name(&self) -> String;

    /// Returns the prefix length to use for `target`, or `None` when no
    /// prefix is extractable.
    fn prefix_len(&self, target: PrefixTarget) -> Option<u64>;
}

struct PrefixExtractorAdapter {
    inner: Arc<dyn PrefixExtractor>,
    name: String,
}

impl PrefixExtractorAdapter {
    fn new(inner: Arc<dyn PrefixExtractor>) -> Self {
        let name = inner.name();
        Self { inner, name }
    }
}

impl slatedb::PrefixExtractor for PrefixExtractorAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
        let target = PrefixTarget::from(target);
        self.inner
            .prefix_len(target)
            .map(|n| usize::try_from(n).expect("prefix_len value must fit in usize"))
    }
}

fn adapt_prefix_extractor(
    extractor: Arc<dyn PrefixExtractor>,
) -> Arc<dyn slatedb::PrefixExtractor> {
    Arc::new(PrefixExtractorAdapter::new(extractor))
}

/// A filter policy used to build and read SST filters.
///
/// Construct one with [`FilterPolicy::bloom`] or
/// [`FilterPolicy::bloom_with_options`] for the built-in bloom filter.
#[derive(uniffi::Object)]
pub struct FilterPolicy {
    pub(crate) inner: Arc<dyn slatedb::FilterPolicy>,
}

/// Options controlling how a bloom filter policy is constructed.
///
/// Pass an optional prefix extractor as a separate constructor parameter; it
/// is kept out of this record because uniffi cannot marshal a trait object
/// inside a record across every target language.
#[derive(Clone, Debug, uniffi::Record)]
pub struct BloomFilterOptions {
    /// Average bits stored per inserted key. Higher values lower the false
    /// positive rate at the cost of filter size.
    pub bits_per_key: u32,
    /// When `true`, hashes the full key into the filter so point lookups can
    /// probe it. Defaults to `true`.
    #[uniffi(default = true)]
    pub whole_key_filtering: bool,
}

impl Default for BloomFilterOptions {
    fn default() -> Self {
        Self {
            bits_per_key: 10,
            whole_key_filtering: true,
        }
    }
}

#[uniffi::export]
impl FilterPolicy {
    /// Constructs a bloom filter policy with the given bits per key,
    /// whole-key filtering enabled, and no prefix extractor.
    #[uniffi::constructor]
    pub fn bloom(bits_per_key: u32) -> Arc<FilterPolicy> {
        Self::build_bloom(
            BloomFilterOptions {
                bits_per_key,
                ..BloomFilterOptions::default()
            },
            None,
        )
    }

    /// Constructs a bloom filter policy from the supplied options, with an
    /// optional prefix extractor enabling prefix-based bloom filtering.
    #[uniffi::constructor]
    pub fn bloom_with_options(
        options: BloomFilterOptions,
        prefix_extractor: Option<Arc<dyn PrefixExtractor>>,
    ) -> Arc<FilterPolicy> {
        Self::build_bloom(options, prefix_extractor)
    }

    /// Returns the policy name encoded into SSTs that use this policy.
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }
}

impl FilterPolicy {
    fn build_bloom(
        options: BloomFilterOptions,
        prefix_extractor: Option<Arc<dyn PrefixExtractor>>,
    ) -> Arc<FilterPolicy> {
        let BloomFilterOptions {
            bits_per_key,
            whole_key_filtering,
        } = options;
        let mut policy = slatedb::BloomFilterPolicy::new(bits_per_key)
            .with_whole_key_filtering(whole_key_filtering);
        if let Some(extractor) = prefix_extractor {
            policy = policy.with_prefix_extractor(adapt_prefix_extractor(extractor));
        }
        Arc::new(FilterPolicy {
            inner: Arc::new(policy),
        })
    }
}

pub(crate) fn collect_filter_policies(
    policies: Vec<Arc<FilterPolicy>>,
) -> Vec<Arc<dyn slatedb::FilterPolicy>> {
    policies.into_iter().map(|p| p.inner.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_context_inline_round_trips_64_bytes() {
        let payload = (0u8..64).collect::<Vec<_>>();
        let ctx = FilterContext::Inline {
            payload: payload.clone(),
        };
        let core: slatedb::FilterContext = ctx.try_into().expect("64-byte payload must convert");
        match core {
            slatedb::FilterContext::Inline(bytes) => {
                assert_eq!(bytes.as_slice(), payload.as_slice())
            }
            _ => panic!("expected Inline variant"),
        }
    }

    #[test]
    fn filter_context_inline_rejects_wrong_size() {
        let short = FilterContext::Inline {
            payload: vec![0u8; 32],
        };
        let err = slatedb::FilterContext::try_from(short).expect_err("short payload must fail");
        assert!(matches!(err, Error::Invalid { .. }));

        let long = FilterContext::Inline {
            payload: vec![0u8; 128],
        };
        let err = slatedb::FilterContext::try_from(long).expect_err("long payload must fail");
        assert!(matches!(err, Error::Invalid { .. }));
    }

    #[test]
    fn bloom_policy_name_uses_default_when_no_extractor() {
        let policy = FilterPolicy::bloom(10);
        assert_eq!(policy.name(), slatedb::BloomFilterPolicy::NAME);
    }

    #[test]
    fn bloom_policy_with_options_includes_extractor_name() {
        struct ThreeByteExtractor;
        impl PrefixExtractor for ThreeByteExtractor {
            fn name(&self) -> String {
                "fixed3".to_string()
            }
            fn prefix_len(&self, target: PrefixTarget) -> Option<u64> {
                let input = match target {
                    PrefixTarget::Point { key } => key,
                    PrefixTarget::Prefix { prefix } => prefix,
                };
                (input.len() >= 3).then_some(3)
            }
        }

        let policy = FilterPolicy::bloom_with_options(
            BloomFilterOptions {
                bits_per_key: 10,
                whole_key_filtering: true,
            },
            Some(Arc::new(ThreeByteExtractor)),
        );
        assert_eq!(policy.name(), "_bf:p=fixed3");
    }
}
