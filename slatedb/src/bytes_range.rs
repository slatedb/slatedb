use bytes::Bytes;
use serde::Serialize;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

use crate::comparable_range::ComparableRange;

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
        Self {
            inner: ComparableRange::new(start_bound, end_bound),
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
    pub(crate) fn is_empty(&self) -> bool {
        !self.inner.non_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::bytes_range::BytesRange;
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;

    use proptest::proptest;
    use std::ops::Bound::Unbounded;
    use std::ops::RangeBounds;

    #[test]
    fn test_arbitrary_range() {
        proptest!(|(range in arbitrary::nonempty_range(10))| {
            assert!(range.non_empty());
        });

        proptest!(|(range in arbitrary::empty_range(10))| {
            assert!(range.is_empty());
        });
    }

    #[test]
    fn test_intersection_of_empty_range_and_nonempty_range_is_empty() {
        proptest!(|(
            empty_range in arbitrary::empty_range(10),
            non_empty_range in arbitrary::nonempty_range(10),
        )| {
            assert!(empty_range.intersect(&non_empty_range).is_none())
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
}
