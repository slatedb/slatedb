use std::sync::Arc;

use bytes::{BufMut, Bytes};

use crate::filter::{BloomFilter, BloomFilterBuilder};
use crate::types::RowEntry;

/// A named, configurable filter policy.
///
/// Each policy produces a filter per SST during construction and can decode
/// previously written filters during reads. The engine stores the policy's
/// [`name`](FilterPolicy::name) alongside filter data so readers can match
/// stored filters to the correct policy.
pub trait FilterPolicy: Send + Sync {
    /// An identifier for this policy. Stored per-SST so the reader knows
    /// whether it can decode and use the filter block.
    ///
    /// The name should encode anything that affects **compatibility** —
    /// i.e., anything that would make a filter unreadable or produce wrong
    /// results if mismatched. For the built-in bloom filter:
    /// - `bits_per_key` is not included — changing it affects quality (FP
    ///   rate) but not the encoding format, so any reader can decode any
    ///   bloom filter regardless of bits_per_key.
    /// - `prefix_extractor` name is included — it changes which hashes are
    ///   stored, so querying with a different extractor produces false
    ///   negatives.
    fn name(&self) -> &str;

    /// Creates a new builder for constructing a filter.
    fn builder(&self) -> Box<dyn FilterBuilder>;

    /// Decodes a previously encoded filter.
    ///
    /// The engine matches the policy name stored in the composite filter
    /// block against `self.name()` before calling this to ensure the policy
    /// can deserialize the data.
    fn decode(&self, data: &[u8]) -> Arc<dyn Filter>;

    /// Estimates the encoded size in bytes for a filter with `num_keys` keys.
    ///
    /// This is a hint used by the SST builder to reserve buffer space before
    /// the filter is built. It does not need to be exact.
    fn estimate_size(&self, num_keys: usize) -> usize;
}

/// Accumulator for entries during SST construction that produces a [`Filter`].
pub trait FilterBuilder: Send {
    /// Feeds an SST entry to the filter being built.
    ///
    /// The builder receives the full `RowEntry`so that filter implementations
    /// are not limited to key-only hashing. A bloom filter will typically hash
    /// only the key (or a prefix of it), but other filters (e.g. a min/max
    /// timestamp filter) can inspect whichever fields they need.
    fn add_entry(&mut self, entry: &RowEntry);

    /// Finalizes and returns the completed filter.
    fn build(&mut self) -> Arc<dyn Filter>;
}

/// A read-only filter that answers membership queries.
pub trait Filter: Send + Sync {
    /// Returns `true` if the filter cannot rule out the query.
    /// A return value of `false` guarantees no matching key exists.
    ///
    /// Filters that cannot answer a particular query kind should return
    /// `true` to avoid false negatives.
    fn might_match(&self, query: &FilterQuery) -> bool;

    /// Serializes the filter into the provided buffer.
    fn encode(&self, writer: &mut dyn BufMut);

    /// Returns the size of the filter's data in bytes.
    fn size(&self) -> usize;

    /// Returns a copy with over-allocated memory reclaimed.
    ///
    /// Used by the block cache to reduce memory waste from `Bytes` slices
    /// that reference larger buffers.
    fn clamp_allocated_size(&self) -> Arc<dyn Filter>;
}

/// A membership query passed to [`Filter::might_match`].
pub struct FilterQuery {
    /// The target of this query (a specific key or a prefix).
    pub target: FilterTarget,
}

impl FilterQuery {
    /// Creates a point-lookup query for the given key.
    pub fn point(key: Bytes) -> Self {
        Self {
            target: FilterTarget::Point(key),
        }
    }

    /// Creates a prefix-scan query for the given prefix.
    pub fn prefix(prefix: Bytes) -> Self {
        Self {
            target: FilterTarget::Prefix(prefix),
        }
    }
}

/// The target of a filter query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterTarget {
    /// Used to test whether a specific key might exist in the SST.
    Point(Bytes),
    /// Used to test whether any key with the given prefix might exist in the SST.
    Prefix(Bytes),
}

// ---------------------------------------------------------------------------
// NamedFilter — a filter paired with its policy name
// ---------------------------------------------------------------------------

/// A filter paired with the name of the policy that produced it.
#[derive(Clone)]
pub(crate) struct NamedFilter {
    pub(crate) name: String,
    pub(crate) filter: Arc<dyn Filter>,
}

impl std::fmt::Debug for NamedFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NamedFilter({})", self.name)
    }
}

// ---------------------------------------------------------------------------
// Built-in implementation: BloomFilterPolicy
// ---------------------------------------------------------------------------

/// Extractor for a prefix from a key for use in prefix-based bloom filtering.
///
/// This trait is specific to `BloomFilterPolicy` — it is not part of the core
/// `FilterPolicy`/`FilterBuilder`/`Filter` traits. Custom filter policies that
/// need prefix-based filtering can implement their own logic directly in
/// `add_entry`/`might_match`.
///
/// Used on the write path to hash prefixes into the bloom filter during SST
/// construction.
pub trait PrefixExtractor: Send + Sync {
    /// A unique name identifying this extractor's configuration.
    ///
    /// Changing the extractor changes which hashes are stored in the
    /// filter, so existing filters become invalid. `BloomFilterPolicy`
    /// includes this name in the policy name it writes to SST metadata.
    fn name(&self) -> &str;

    /// Returns whether `prefix` is a valid output of [`prefix_len`]
    ///
    /// Used on the read path to decide whether a user-provided scan prefix
    /// can be answered by the filter. If this returns `false`, the filter
    /// must NOT be consulted; doing so can produce false negatives.
    ///
    /// **Consistency with `prefix_len`:** implementors must ensure that
    /// `in_domain` agrees with what `prefix_len` would produce. In
    /// particular, if `prefix_len(key)` returns `None` for every key that
    /// begins with some byte string `p`, then `in_domain(p)` must return
    /// `false`, otherwise the read path will consult the filter for a
    /// prefix that was never hashed into it and silently miss real data.
    fn in_domain(&self, prefix: &[u8]) -> bool;

    /// Returns the length of the prefix this extractor produces from `key`,
    /// or `None` if `key` has no extractable prefix. The caller interprets
    /// the returned length as `&key[..len]`.
    ///
    /// For example, an extractor that selectively indexes keys matching
    /// `"user:"` (so that `scan_prefix("user:")` can use the filter) but
    /// ignores all other keys would return:
    /// - `prefix_len(b"user:alice")` → `Some(5)` (hash `"user:"` into the filter)
    /// - `prefix_len(b"user:bob")`   → `Some(5)` (same)
    /// - `prefix_len(b"post:42")`    → `None`    (don't index this key's prefix)
    /// - `prefix_len(b"session:x")`  → `None`    (don't index this key's prefix)
    ///
    /// When `None` is returned, the caller skips adding a prefix for this
    /// key into the filter. A prefix scan over this key's prefix cannot
    /// use the filter and must fall back to scanning the SST directly
    /// which requires that [`in_domain`] also return `false` for such prefixes
    /// (see the note on `in_domain`).
    ///
    /// # Panics
    ///
    /// The returned length must be ≤ `key.len()`. Returning a longer value
    /// is a contract violation and will panic when the length is used to
    /// slice the key.
    fn prefix_len(&self, key: &[u8]) -> Option<usize>;
}

/// A filter policy backed by the existing bloom filter implementation.
///
/// Supports full-key filtering (point lookups) and optional prefix filtering
/// when a [`PrefixExtractor`] is configured.
pub struct BloomFilterPolicy {
    bits_per_key: u32,
    whole_key_filtering: bool,
    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,
    name: String,
}

// Manual Debug impl because `dyn PrefixExtractor` doesn't implement Debug,
// so #[derive(Debug)] won't compile. We surface the extractor's name instead.
impl std::fmt::Debug for BloomFilterPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilterPolicy")
            .field("bits_per_key", &self.bits_per_key)
            .field("whole_key_filtering", &self.whole_key_filtering)
            .field("name", &self.name)
            .field(
                "prefix_extractor",
                &self.prefix_extractor.as_ref().map(|e| e.name()),
            )
            .finish()
    }
}

impl BloomFilterPolicy {
    /// The canonical name for bloom filter policies.
    pub const NAME: &'static str = "_bf";

    /// Creates a new bloom filter policy with the given bits per key.
    ///
    /// By default, whole-key filtering is enabled and no prefix extractor
    /// is configured.
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            whole_key_filtering: true,
            prefix_extractor: None,
            name: Self::NAME.to_string(),
        }
    }

    /// Configures a prefix extractor for prefix-based bloom filtering.
    ///
    /// When set, the bloom filter hashes both extracted prefixes and (if
    /// `whole_key_filtering` is enabled) full keys. Prefix scans can then
    /// probe the filter to skip SSTs that contain no matching prefixes.
    ///
    /// The extractor's name is included in the policy name to ensure that
    /// filters built with different extractors are not mismatched.
    pub fn with_prefix_extractor(mut self, extractor: Arc<dyn PrefixExtractor>) -> Self {
        self.name = format!("{}:prefix={}", Self::NAME, extractor.name());
        self.prefix_extractor = Some(extractor);
        self
    }

    /// Controls whether full keys are hashed into the bloom filter.
    ///
    /// When `true` (the default), point lookups can use the filter. Set
    /// to `false` if only prefix scans are needed and you want to reduce
    /// filter size.
    pub fn with_whole_key_filtering(mut self, enabled: bool) -> Self {
        self.whole_key_filtering = enabled;
        self
    }

    /// Returns the bits per key setting.
    pub fn bits_per_key(&self) -> u32 {
        self.bits_per_key
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str {
        &self.name
    }

    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(BloomFilterBuilder::new(
            self.bits_per_key,
            self.whole_key_filtering,
            self.prefix_extractor.clone(),
        ))
    }

    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        Arc::new(BloomFilter::decode(
            data,
            self.whole_key_filtering,
            self.prefix_extractor.clone(),
        ))
    }

    fn estimate_size(&self, num_keys: usize) -> usize {
        let num_keys = u32::try_from(num_keys).expect("num_keys should fit in u32");
        BloomFilter::estimate_encoded_size(num_keys, self.bits_per_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueDeletable;

    fn make_entry(key: &[u8]) -> RowEntry {
        RowEntry::new(
            Bytes::copy_from_slice(key),
            ValueDeletable::Value(Bytes::from_static(b"val")),
            0,
            None,
            None,
        )
    }

    #[test]
    fn test_bloom_filter_policy_round_trip() {
        let policy = BloomFilterPolicy::new(10);
        assert_eq!(policy.name(), BloomFilterPolicy::NAME);

        // Build a filter via the policy
        let mut builder = policy.builder();
        for i in 0u32..1000 {
            builder.add_entry(&make_entry(&i.to_be_bytes()));
        }
        let filter = builder.build();

        // Encode and decode
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = policy.decode(&buf);

        // Point queries: all inserted keys should match
        for i in 0u32..1000 {
            let query = FilterQuery::point(Bytes::copy_from_slice(&i.to_be_bytes()));
            assert!(decoded.might_match(&query), "false negative for key {}", i);
        }

        // Prefix queries: should return true (inapplicable, no prefix extractor)
        let prefix_query = FilterQuery::prefix(Bytes::from_static(b"anything"));
        assert!(decoded.might_match(&prefix_query));
    }

    #[test]
    fn test_bloom_filter_policy_estimate_size() {
        let policy = BloomFilterPolicy::new(10);
        let size = policy.estimate_size(1000);
        assert!(size > 0);
    }

    #[test]
    fn test_bloom_filter_policy_false_positives() {
        let policy = BloomFilterPolicy::new(10);
        let mut builder = policy.builder();
        let num_keys = 100_000u32;
        for i in 0..num_keys {
            builder.add_entry(&make_entry(&i.to_be_bytes()));
        }
        let filter = builder.build();

        // Check false positive rate on keys NOT in the filter
        let mut fp = 0u32;
        for i in num_keys..2 * num_keys {
            let query = FilterQuery::point(Bytes::copy_from_slice(&i.to_be_bytes()));
            if filter.might_match(&query) {
                fp += 1;
            }
        }
        let fpr = fp as f64 / num_keys as f64;
        assert!(fpr < 0.01, "false positive rate too high: {}", fpr);
    }

    #[test]
    fn test_clamp_allocated_size() {
        let policy = BloomFilterPolicy::new(10);
        let mut builder = policy.builder();
        for i in 0u32..100 {
            builder.add_entry(&make_entry(&i.to_be_bytes()));
        }
        let filter = builder.build();
        let clamped = filter.clamp_allocated_size();
        assert_eq!(filter.size(), clamped.size());
    }

    /// A fixed-length prefix extractor for testing.
    struct FixedPrefixExtractor {
        len: usize,
        name: String,
    }

    impl FixedPrefixExtractor {
        fn new(len: usize) -> Self {
            Self {
                len,
                name: format!("fixed{}", len),
            }
        }
    }

    impl PrefixExtractor for FixedPrefixExtractor {
        fn name(&self) -> &str {
            &self.name
        }

        fn in_domain(&self, prefix: &[u8]) -> bool {
            prefix.len() == self.len
        }

        fn prefix_len(&self, key: &[u8]) -> Option<usize> {
            if key.len() >= self.len {
                Some(self.len)
            } else {
                None
            }
        }
    }

    #[test]
    fn test_prefix_bloom_filter_policy_name() {
        let extractor = Arc::new(FixedPrefixExtractor::new(3));
        let policy = BloomFilterPolicy::new(10).with_prefix_extractor(extractor);
        assert_eq!(policy.name(), "_bf:prefix=fixed3");
    }

    #[test]
    fn test_prefix_bloom_filter_round_trip() {
        let extractor = Arc::new(FixedPrefixExtractor::new(3));
        let policy = BloomFilterPolicy::new(10).with_prefix_extractor(extractor);

        // Build a filter with keys sharing prefixes "aaa" and "bbb"
        let mut builder = policy.builder();
        for i in 0u32..100 {
            let key = format!("aaa{:04}", i);
            builder.add_entry(&make_entry(key.as_bytes()));
        }
        for i in 0u32..100 {
            let key = format!("bbb{:04}", i);
            builder.add_entry(&make_entry(key.as_bytes()));
        }
        let filter = builder.build();

        // Encode and decode
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = policy.decode(&buf);

        // Point queries should still work
        let query = FilterQuery::point(Bytes::from_static(b"aaa0050"));
        assert!(decoded.might_match(&query));

        // Prefix query for "aaa" should match
        let query = FilterQuery::prefix(Bytes::from_static(b"aaa"));
        assert!(decoded.might_match(&query));

        // Prefix query for "bbb" should match
        let query = FilterQuery::prefix(Bytes::from_static(b"bbb"));
        assert!(decoded.might_match(&query));

        // Prefix query for "ccc" (not inserted) should have low FP rate
        // (can't guarantee false for a single query, but it shouldn't be true
        // for ALL non-existent prefixes)
        let mut fp = 0;
        for c in b'c'..=b'z' {
            let prefix = vec![c, c, c];
            let query = FilterQuery::prefix(Bytes::from(prefix));
            if decoded.might_match(&query) {
                fp += 1;
            }
        }
        // With 10 bits per key and only ~2 prefix hashes, FP rate should be very low
        assert!(
            fp < 10,
            "too many false positives for prefix queries: {}",
            fp
        );
    }

    #[test]
    fn test_prefix_bloom_out_of_domain_returns_true() {
        let extractor = Arc::new(FixedPrefixExtractor::new(3));
        let policy = BloomFilterPolicy::new(10).with_prefix_extractor(extractor);

        let mut builder = policy.builder();
        builder.add_entry(&make_entry(b"aaa0001"));
        let filter = builder.build();

        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = policy.decode(&buf);

        // Prefix shorter than extractor length is out-of-domain, must return true
        let query = FilterQuery::prefix(Bytes::from_static(b"aa"));
        assert!(decoded.might_match(&query));
    }

    #[test]
    fn test_no_prefix_extractor_returns_true_for_prefix_queries() {
        let policy = BloomFilterPolicy::new(10);

        let mut builder = policy.builder();
        builder.add_entry(&make_entry(b"aaa0001"));
        let filter = builder.build();

        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = policy.decode(&buf);

        // Without a prefix extractor, prefix queries should always return true
        let query = FilterQuery::prefix(Bytes::from_static(b"aaa"));
        assert!(decoded.might_match(&query));
    }

    #[test]
    fn test_whole_key_filtering_disabled() {
        let extractor = Arc::new(FixedPrefixExtractor::new(3));
        let policy = BloomFilterPolicy::new(10)
            .with_prefix_extractor(extractor)
            .with_whole_key_filtering(false);

        let mut builder = policy.builder();
        for i in 0u32..1000 {
            let key = format!("aaa{:04}", i);
            builder.add_entry(&make_entry(key.as_bytes()));
        }
        let filter = builder.build();

        // Encode and decode so the decoded filter goes through BloomFilterPolicy::decode,
        // which is where the whole_key_filtering flag must be propagated correctly.
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        let decoded = policy.decode(&buf);

        // Prefix query should still work
        let query = FilterQuery::prefix(Bytes::from_static(b"aaa"));
        assert!(decoded.might_match(&query));

        // Point queries must return true (inapplicable). No full-key hashes were stored,
        // so probing would produce false negatives for keys that actually exist.
        let query = FilterQuery::point(Bytes::from_static(b"aaa0001"));
        assert!(
            decoded.might_match(&query),
            "point query must return true when whole_key_filtering=false to avoid false negatives"
        );
    }
}
