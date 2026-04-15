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
    fn build(&self) -> Arc<dyn Filter>;
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
///
/// Internally, a `NamedFilter` is either **decoded** (holding an `Arc<dyn Filter>`)
/// or **raw** (holding opaque bytes). Raw filters arise from disk-cache
/// deserialization where the policy list is not available; they are resolved to
/// decoded filters on first access by [`crate::tablestore::TableStore`].
#[derive(Clone)]
pub(crate) struct NamedFilter {
    name: String,
    inner: NamedFilterInner,
}

impl std::fmt::Debug for NamedFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &self.inner {
            NamedFilterInner::Decoded(_) => "decoded",
            NamedFilterInner::Raw(b) => {
                return write!(f, "NamedFilter({}, raw {} bytes)", self.name, b.len())
            }
        };
        write!(f, "NamedFilter({}, {})", self.name, state)
    }
}

#[derive(Clone)]
enum NamedFilterInner {
    /// Fully decoded, ready for evaluation.
    Decoded(Arc<dyn Filter>),
    /// Raw encoded bytes from disk-cache deserialization, awaiting policy-based decode.
    Raw(Bytes),
}

impl NamedFilter {
    /// Creates a decoded `NamedFilter`.
    pub(crate) fn new(name: String, filter: Arc<dyn Filter>) -> Self {
        Self {
            name,
            inner: NamedFilterInner::Decoded(filter),
        }
    }

    /// Creates a raw (not yet decoded) `NamedFilter`.
    pub(crate) fn raw(name: String, data: Bytes) -> Self {
        Self {
            name,
            inner: NamedFilterInner::Raw(data),
        }
    }

    /// Returns the policy name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this filter has been decoded.
    pub(crate) fn is_decoded(&self) -> bool {
        matches!(self.inner, NamedFilterInner::Decoded(_))
    }

    /// Returns a reference to the inner decoded filter.
    ///
    /// # Panics
    ///
    /// Panics if this `NamedFilter` is still in raw form. Callers that hold
    /// a `NamedFilter` not in the cache layer are guaranteed a decoded entry:
    /// raw entries only exist inside the cache between disk-cache
    /// deserialization and the next `read_filters` call, which
    /// resolves them. A raw entry here would indicate the invariant was
    /// bypassed.
    #[allow(clippy::panic)]
    pub(crate) fn unwrap_filter(&self) -> &Arc<dyn Filter> {
        match &self.inner {
            NamedFilterInner::Decoded(f) => f,
            NamedFilterInner::Raw(_) => {
                panic!("found raw NamedFilter when unwrapping decoded filter")
            }
        }
    }

    /// Returns the decoded filter, decoding against `policies` if raw.
    ///
    /// If the filter is already decoded, returns the inner `Arc<dyn Filter>`.
    /// If raw, looks up the policy by name and decodes the stored bytes.
    /// Returns `None` if no configured policy matches.
    pub(crate) fn resolve(&self, policies: &[Arc<dyn FilterPolicy>]) -> Option<Arc<dyn Filter>> {
        match &self.inner {
            NamedFilterInner::Decoded(f) => Some(f.clone()),
            NamedFilterInner::Raw(bytes) => match policies.iter().find(|p| p.name() == self.name) {
                Some(p) => Some(p.decode(bytes)),
                None => {
                    log::warn!(
                        "unknown filter policy '{}' in cached filter, skipping",
                        self.name
                    );
                    None
                }
            },
        }
    }

    /// Decodes raw filter bytes against the matching policy in `policies`,
    /// returning a fully-decoded `NamedFilter`.
    ///
    /// Returns `None` if no configured policy matches `name`.
    pub(crate) fn decode_raw(
        name: &str,
        data: &[u8],
        policies: &[Arc<dyn FilterPolicy>],
    ) -> Option<Self> {
        match policies.iter().find(|p| p.name() == name) {
            Some(policy) => Some(Self::new(policy.name().to_string(), policy.decode(data))),
            None => {
                log::warn!("unknown filter policy '{}' in SST, skipping", name);
                None
            }
        }
    }

    /// Returns the size of the filter data in bytes.
    pub(crate) fn size(&self) -> usize {
        match &self.inner {
            NamedFilterInner::Decoded(f) => f.size(),
            NamedFilterInner::Raw(b) => b.len(),
        }
    }

    /// Encodes the filter into a `Bytes` buffer.
    ///
    /// For decoded filters, calls `filter.encode()`. For raw filters,
    /// returns the stored bytes directly.
    pub(crate) fn encode_data(&self) -> Bytes {
        match &self.inner {
            NamedFilterInner::Decoded(f) => {
                let mut buf = Vec::new();
                f.encode(&mut buf);
                Bytes::from(buf)
            }
            NamedFilterInner::Raw(b) => b.clone(),
        }
    }

    /// Returns a copy with over-allocated memory reclaimed.
    pub(crate) fn clamp_allocated_size(&self) -> Self {
        match &self.inner {
            NamedFilterInner::Decoded(f) => Self {
                name: self.name.clone(),
                inner: NamedFilterInner::Decoded(f.clamp_allocated_size()),
            },
            NamedFilterInner::Raw(b) => Self {
                name: self.name.clone(),
                inner: NamedFilterInner::Raw(crate::utils::clamp_allocated_size_bytes(b)),
            },
        }
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

    /// Returns whether the given prefix is a valid output of `extract()`.
    ///
    /// This is used on the read path to verify that a scan prefix provided
    /// by the user matches the prefix format indexed in the filter. If this
    /// returns `false`, the filter must NOT be consulted; doing so can
    /// produce false negatives.
    fn in_domain(&self, prefix: &[u8]) -> bool;

    /// Extracts the prefix from a key. Returns `None` if the key does not
    /// contain a recognizable prefix.
    fn extract<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]>;
}

/// A filter policy backed by the existing bloom filter implementation.
///
/// Supports full-key filtering (point lookups). Prefix filtering requires
/// a `PrefixExtractor` (added in a later phase).
#[derive(Debug)]
pub struct BloomFilterPolicy {
    bits_per_key: u32,
    name: &'static str,
}

impl BloomFilterPolicy {
    /// The canonical name for bloom filter policies.
    pub const NAME: &'static str = "_bf";

    /// Creates a new bloom filter policy with the given bits per key.
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            name: Self::NAME,
        }
    }

    /// Returns the bits per key setting.
    pub fn bits_per_key(&self) -> u32 {
        self.bits_per_key
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &str {
        self.name
    }

    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(BloomFilterBuilder::new(self.bits_per_key))
    }

    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        Arc::new(BloomFilter::decode(data))
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
}
