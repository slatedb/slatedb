use bytes::Bytes;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BytesRefRange<'a> {
    pub(crate) start_bound: Bound<&'a [u8]>,
    pub(crate) end_bound: Bound<&'a [u8]>,
}

impl<'a> BytesRefRange<'a> {
    pub(crate) fn new<T: RangeBounds<&'a [u8]>>(range: T) -> Self {
        Self {
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
        }
    }

    pub(crate) fn key_exceeds(&self, key: &[u8]) -> bool {
        match self.end_bound {
            Included(end) => key > end,
            Excluded(end) => key >= end,
            Unbounded => false,
        }
    }

    pub(crate) fn has_nonempty_intersection(&self, other: BytesRefRange<'a>) -> bool {
        let start_bound = max_start_bound(self.start_bound, other.start_bound);
        let end_bound = min_end_bound(self.end_bound, other.end_bound);
        !Self {
            start_bound,
            end_bound,
        }
        .is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self.end_bound {
            Unbounded => false,
            Included(end) => match self.start_bound {
                Unbounded => false,
                Included(start) => start > end,
                Excluded(start) => start >= end,
            },
            Excluded(end) => match self.start_bound {
                Unbounded => end.len() == 0,
                Included(start_bytes) => start_bytes >= end,
                Excluded(start) if start >= end => true,
                Excluded(start) => is_prefix_increment(start, end),
            },
        }
    }
}

impl<'a> RangeBounds<&'a [u8]> for BytesRefRange<'a> {
    fn start_bound(&self) -> Bound<&&'a [u8]> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> Bound<&&'a [u8]> {
        self.end_bound.as_ref()
    }
}

impl BytesRefRange<'_> {
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        <(Bound<&[u8]>, Bound<&[u8]>) as RangeBounds<[u8]>>::contains::<[u8]>(
            &(self.start_bound, self.end_bound),
            key,
        )
    }
}

// Checks for the annoying case when we have ("prefix", "prefix\0").
// When both bounds are excluded, the range is empty even though
// "prefix" < "prefix\0".
pub(crate) fn is_prefix_increment(prefix: &[u8], b: &[u8]) -> bool {
    if !b.starts_with(prefix.as_ref()) {
        return false;
    }

    b[prefix.len()..] == [u8::MIN]
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BytesRange {
    start_bound: Bound<Bytes>,
    end_bound: Bound<Bytes>,
}

impl RangeBounds<Bytes> for BytesRange {
    fn start_bound(&self) -> Bound<&Bytes> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> Bound<&Bytes> {
        self.end_bound.as_ref()
    }
}

impl BytesRange {
    pub(crate) fn new(start_bound: Bound<Bytes>, end_bound: Bound<Bytes>) -> Self {
        Self {
            start_bound,
            end_bound,
        }
    }

    pub(crate) fn from<T: RangeBounds<Bytes>>(range: T) -> Self {
        Self::new(range.start_bound().cloned(), range.end_bound().cloned())
    }

    #[cfg(test)]
    pub(crate) fn as_ref(&self) -> BytesRefRange {
        BytesRefRange {
            start_bound: self.start_bound.as_ref().map(|b| b.as_ref()),
            end_bound: self.end_bound.as_ref().map(|b| b.as_ref()),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn has_nonempty_intersection(&self, other: BytesRange) -> bool {
        self.as_ref().has_nonempty_intersection(other.as_ref())
    }
}

/// Helper to get the larger or small bound depending on a
/// comparison function (which is assumed to just be `<` or `>`).
fn clamp_bound<'a>(
    a: Bound<&'a [u8]>,
    b: Bound<&'a [u8]>,
    cmp: fn(&[u8], &[u8]) -> bool,
) -> Bound<&'a [u8]> {
    match (a, b) {
        (Unbounded, _) => b,
        (_, Unbounded) => a,
        (Included(a_bytes), Included(b_bytes)) if a_bytes == b_bytes => a,
        (Included(a_bytes) | Excluded(a_bytes), Included(b_bytes) | Excluded(b_bytes)) => {
            if a_bytes == b_bytes {
                Excluded(a_bytes)
            } else if cmp(a_bytes, b_bytes) {
                a
            } else {
                b
            }
        }
    }
}

fn min_end_bound<'a>(a: Bound<&'a [u8]>, b: Bound<&'a [u8]>) -> Bound<&'a [u8]> {
    clamp_bound(a, b, |a, b| a < b)
}

fn max_start_bound<'a>(a: Bound<&'a [u8]>, b: Bound<&'a [u8]>) -> Bound<&'a [u8]> {
    clamp_bound(a, b, |a, b| a > b)
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
            assert!(!range.is_empty());
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
            assert!(!empty_range.has_nonempty_intersection(non_empty_range))
        });
    }

    #[test]
    fn test_intersection_of_non_empty_and_unbounded_range_is_nonempty() {
        proptest!(|(non_empty_range in arbitrary::nonempty_range(10))| {
            let unbounded_range = BytesRange::new(Unbounded, Unbounded);
            assert!(non_empty_range.has_nonempty_intersection(unbounded_range))
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
        proptest!(|(range in arbitrary::empty_range(10), sample in arbitrary::bytes(10))| {
            assert!(!range.contains(&sample), "Expected value {sample:?} to not be in empty range {range:?}");
        });
    }

    #[test]
    fn test_nonempty_intersection_of_intersecting_ranges() {
        proptest!(|(
            (non_empty_1, non_empty_2) in arbitrary::nonempty_intersecting_ranges(10),
        )| {
            assert!(non_empty_1.has_nonempty_intersection(non_empty_2))
        });
    }
}
