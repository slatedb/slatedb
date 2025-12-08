use bytes::Bytes;
use serde::Serialize;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

use crate::comparable_range::{ComparableRange, EndBound, StartBound};

/// Concrete struct representing a range of Bytes. Gets around much of
/// the cumbersome work associated with the generic trait RangeBounds<Bytes>
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct BytesRange {
    inner: ComparableRange<Bytes>,
}

impl Serialize for BytesRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

#[cfg(test)]
// Checks for the annoying case when we have ("prefix", "prefix\0").
// When both bounds are excluded, the range is empty even though
// "prefix" < "prefix\0".
pub(crate) fn is_prefix_increment(prefix: &[u8], b: &[u8]) -> bool {
    if !b.starts_with(prefix.as_ref()) {
        return false;
    }

    b[prefix.len()..] == [u8::MIN]
}

impl RangeBounds<Bytes> for BytesRange {
    fn start_bound(&self) -> Bound<&Bytes> {
        self.inner.start_bound()
    }

    fn end_bound(&self) -> Bound<&Bytes> {
        self.inner.end_bound()
    }
}

fn is_bound_non_empty(bound: &Bound<Bytes>) -> bool {
    match bound {
        Included(b) | Excluded(b) => !b.is_empty(),
        Unbounded => true,
    }
}

impl BytesRange {
    pub(crate) fn new(start_bound: Bound<Bytes>, end_bound: Bound<Bytes>) -> Self {
        assert!(
            is_bound_non_empty(&start_bound),
            "Start bound must be non-empty"
        );
        assert!(
            is_bound_non_empty(&end_bound),
            "End bound must be non-empty"
        );
        let inner = ComparableRange::new(start_bound, end_bound);
        assert!(inner.non_empty(), "Range must be non-empty");
        Self { inner }
    }

    pub(crate) fn new_empty() -> Self {
        Self {
            inner: ComparableRange::new(
                Excluded(Bytes::copy_from_slice(&[0_u8])),
                Excluded(Bytes::copy_from_slice(&[0_u8])),
            ),
        }
    }

    pub(crate) fn from<T: RangeBounds<Bytes>>(range: T) -> Self {
        Self::new(range.start_bound().cloned(), range.end_bound().cloned())
    }

    pub(crate) fn from_slice<'a, T>(range: T) -> Self
    where
        T: RangeBounds<&'a [u8]>,
    {
        Self::new(
            range.start_bound().map(|b| Bytes::copy_from_slice(b)),
            range.end_bound().map(|b| Bytes::copy_from_slice(b)),
        )
    }

    /// Build a half-open range `[prefix, prefix+1)` covering all keys that start with
    /// `prefix`.
    ///
    /// - Empty prefix returns `(..)` so callers can scan the entire keyspace.
    /// - If the prefix is all `0xff`, the end bound is unbounded rather than overflowing.
    pub(crate) fn from_prefix(prefix: &[u8]) -> Self {
        if prefix.is_empty() {
            return Self::new(Unbounded, Unbounded);
        }

        let start = Bytes::copy_from_slice(prefix);
        let end = Self::increment_prefix(prefix)
            .map(Excluded)
            .unwrap_or(Unbounded);
        Self::new(Included(start), end)
    }

    /// Compute the smallest byte string that is lexicographically greater than any key
    /// starting with `prefix`. Returns `None` when `prefix` is all `0xff`.
    fn increment_prefix(prefix: &[u8]) -> Option<Bytes> {
        let mut upper_bound = prefix.to_vec();

        for i in (0..upper_bound.len()).rev() {
            if upper_bound[i] != u8::MAX {
                upper_bound[i] += 1;
                upper_bound.truncate(i + 1);
                return Some(Bytes::from(upper_bound));
            }
        }

        None
    }

    #[cfg(test)]
    pub(crate) fn from_ref<K, T>(range: T) -> Self
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
        Self::new(start, end)
    }

    pub(crate) fn intersect(&self, other: &Self) -> Option<Self> {
        self.inner
            .intersect(&other.inner)
            .map(|inner| Self { inner })
    }

    pub(crate) fn is_start_bound_included_or_unbounded(&self) -> bool {
        !matches!(self.start_bound(), Excluded(_))
    }

    #[cfg(test)]
    pub(crate) fn non_empty(&self) -> bool {
        self.inner.non_empty()
    }

    #[cfg(test)]
    pub(crate) fn empty(&self) -> bool {
        !self.inner.non_empty()
    }

    pub(crate) fn comparable_start_bound(&self) -> StartBound<&Bytes> {
        self.inner.comparable_start_bound()
    }

    pub(crate) fn comparable_end_bound(&self) -> EndBound<&Bytes> {
        self.inner.comparable_end_bound()
    }

    pub(crate) fn as_point(&self) -> Option<&Bytes> {
        match (self.start_bound(), self.end_bound()) {
            (Bound::Included(start), Bound::Included(end)) if start == end => Some(start),
            _ => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::bytes_range::BytesRange;
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;

    use bytes::Bytes;
    use proptest::{prop_assert, proptest};
    use std::ops::Bound;
    use std::ops::Bound::Unbounded;
    use std::ops::RangeBounds;

    #[test]
    fn test_arbitrary_range() {
        proptest!(|(range in arbitrary::nonempty_range(10))| {
            assert!(range.non_empty());
        });
    }

    #[should_panic(expected = "Range must be non-empty")]
    #[test]
    fn test_arbitrary_empty_range() {
        proptest!(|(range in arbitrary::empty_range(10))| {
            assert!(range.empty());
        });
    }

    #[test]
    fn test_from_prefix_builds_half_open_range() {
        let range = BytesRange::from_prefix(b"ab");
        let start = Bytes::from("ab");
        let end = Bytes::from("ac");
        assert_eq!(range.start_bound(), Bound::Included(&start));
        assert_eq!(range.end_bound(), Bound::Excluded(&end));
    }

    #[test]
    fn test_from_prefix_handles_all_ff_prefix() {
        let prefix = vec![0xff, 0xff];
        let range = BytesRange::from_prefix(&prefix);
        let start = Bytes::from(prefix);
        assert_eq!(range.start_bound(), Bound::Included(&start));
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_from_prefix_allows_empty_prefix() {
        let range = BytesRange::from_prefix(b"");
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_from_prefix_contains_all_prefixed_keys() {
        proptest!(|(
            prefix in arbitrary::nonempty_bytes(8),
            suffix in arbitrary::bytes(8)
        )| {
            let range = BytesRange::from_prefix(&prefix);
            let mut combined = prefix.clone().to_vec();
            combined.extend_from_slice(&suffix);
            let key = Bytes::from(combined);
            prop_assert!(range.contains(&key));
        });
    }

    #[test]
    fn test_from_prefix_unbounded_for_empty_prefix() {
        proptest!(|(key in arbitrary::bytes(12))| {
            let range = BytesRange::from_prefix(b"");
            prop_assert!(range.contains(&key));
        });
    }

    #[test]
    fn test_from_prefix_filters_keys_by_prefix() {
        proptest!(|(
            prefix in arbitrary::nonempty_bytes(8),
            suffix in arbitrary::bytes(8)
        )| {
            let range = BytesRange::from_prefix(&prefix);

            // Any key starting with the prefix should be contained.
            let mut prefixed = prefix.clone().to_vec();
            prefixed.extend_from_slice(&suffix);
            let prefixed = Bytes::from(prefixed);
            prop_assert!(range.contains(&prefixed));

            // A key with a different first byte should be rejected.
            let mut non_prefixed = prefix.clone().to_vec();
            non_prefixed[0] = non_prefixed[0].wrapping_add(1);
            let non_prefixed = Bytes::from(non_prefixed);
            prop_assert!(!range.contains(&non_prefixed));
        });
    }

    #[test]
    fn test_intersection_of_empty_range_and_nonempty_range_is_empty() {
        proptest!(|(
            non_empty_range in arbitrary::nonempty_range(10),
        )| {
            assert!(BytesRange::new_empty().intersect(&non_empty_range).is_none())
        });
    }

    #[test]
    fn test_intersection_of_non_empty_and_unbounded_range_is_nonempty() {
        proptest!(|(non_empty_range in arbitrary::nonempty_range(10))| {
            let unbounded_range = BytesRange::new(Unbounded, Unbounded);
            assert!(non_empty_range.intersect(&unbounded_range).is_some());
        });
    }

    #[test]
    fn test_contains_with_value_in_range() {
        proptest!(|(range in arbitrary::nonempty_range(10), mut rng in arbitrary::rng())| {
            let sample = sample::bytes_in_range(&mut rng, &range);
            assert!(range.contains(&sample), "Expected value {sample:?} is not in range {range:?}");
        });
    }

    #[test]
    #[should_panic(expected = "Range must be non-empty")]
    fn test_contains_with_empty_range() {
        proptest!(|(range in arbitrary::empty_range(10), sample in arbitrary::nonempty_bytes(10))| {
            assert!(!range.contains(&sample), "Expected value {sample:?} to not be in empty range {range:?}");
        });
    }

    #[test]
    fn test_nonempty_intersection_of_intersecting_ranges() {
        proptest!(|(
            (non_empty_1, non_empty_2) in arbitrary::nonempty_intersecting_ranges(10),
        )| {
            assert!(non_empty_1.intersect(&non_empty_2).is_some())
        });
    }

    #[test]
    fn test_new_with_unbounded_range_is_valid() {
        BytesRange::new(Unbounded, Unbounded);
    }

    #[test]
    #[should_panic(expected = "Range must be non-empty")]
    fn test_new_with_start_larger_than_end_panics() {
        let start = Bound::Included(Bytes::from("z"));
        let end = Bound::Included(Bytes::from("a"));
        BytesRange::new(start, end);
    }
}
