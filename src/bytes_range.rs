use bytes::Bytes;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

/// Concrete struct representing a range of Bytes. Gets around much of
/// the cumbersome work associated with the generic trait RangeBounds<Bytes>
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BytesRange {
    start_bound: Bound<Bytes>,
    end_bound: Bound<Bytes>,
}

// Checks for the annoying case when we have ("prefix", "prefix\0").
// When both bounds are excluded, the range is empty even though
// "prefix" < "prefix\0".
pub(crate) fn is_prefix_increment(prefix: &Bytes, b: &Bytes) -> bool {
    if b.len() != prefix.len() + 1 {
        return false;
    }

    for i in 0..prefix.len() {
        if b[i] != prefix[i] {
            return false;
        }
    }

    b[b.len() - 1] == u8::MIN
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

    pub(crate) fn with_start_key(start_key: Option<Bytes>) -> Self {
        match start_key {
            None => Self::from(..),
            Some(k) => Self::from(k..),
        }
    }

    pub(crate) fn contains(&self, value: &Bytes) -> bool {
        let above_start_bound = match &self.start_bound {
            Unbounded => true,
            Included(b) => value >= b,
            Excluded(b) => value > b,
        };

        let below_end_bound = match &self.end_bound {
            Unbounded => true,
            Included(b) => value <= b,
            Excluded(b) => value < b,
        };

        above_start_bound && below_end_bound
    }

    pub(crate) fn non_empty(&self) -> bool {
        !self.is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self.end_bound() {
            Unbounded => false,
            Included(end) => match self.start_bound() {
                Unbounded => false,
                Included(start) => start > end,
                Excluded(start) => start >= end,
            },
            Excluded(end) => match self.start_bound() {
                Unbounded => end.len() == 0,
                Included(start_bytes) => start_bytes >= end,
                Excluded(start) if start >= end => true,
                Excluded(start) => is_prefix_increment(start, end),
            },
        }
    }

    pub(crate) fn start_bound(&self) -> Bound<&Bytes> {
        self.start_bound.as_ref()
    }

    pub(crate) fn start_bound_opt(&self) -> Option<Bytes> {
        as_option(self.start_bound()).cloned()
    }

    pub(crate) fn end_bound(&self) -> Bound<&Bytes> {
        self.end_bound.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn end_bound_opt(&self) -> Option<Bytes> {
        as_option(self.end_bound()).cloned()
    }

    fn compare_bound<'a>(
        a: Bound<&'a Bytes>,
        b: Bound<&'a Bytes>,
        cmp: fn(&Bytes, &Bytes) -> bool,
    ) -> Bound<&'a Bytes> {
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

    fn min_end_bound<'a>(a: Bound<&'a Bytes>, b: Bound<&'a Bytes>) -> Bound<&'a Bytes> {
        Self::compare_bound(a, b, |a, b| a < b)
    }

    fn max_start_bound<'a>(a: Bound<&'a Bytes>, b: Bound<&'a Bytes>) -> Bound<&'a Bytes> {
        Self::compare_bound(a, b, |a, b| a > b)
    }

    pub(crate) fn intersection(&self, other: &BytesRange) -> BytesRange {
        let start_bound = Self::max_start_bound(self.start_bound(), other.start_bound());
        let end_bound = Self::min_end_bound(self.end_bound(), other.end_bound());

        BytesRange {
            start_bound: start_bound.cloned(),
            end_bound: end_bound.cloned(),
        }
    }
}

fn as_option<T>(bound: Bound<&T>) -> Option<&T>
where
    T: ?Sized,
{
    match bound {
        Included(b) | Excluded(b) => Some(b),
        Unbounded => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::bytes_range::BytesRange;
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;

    use proptest::proptest;
    use std::ops::Bound::Unbounded;

    #[test]
    fn test_intersection_of_empty_range_is_empty() {
        proptest!(|(
            empty_range in arbitrary::empty_range(10),
            non_empty_range in arbitrary::nonempty_range(10),
        )| {
            let intersection = empty_range.intersection(&non_empty_range);
            assert!(intersection.is_empty());

            let intersection = non_empty_range.intersection(&empty_range);
            assert!(intersection.is_empty());
        });
    }

    #[test]
    fn test_intersection_of_non_empty_range_and_unbounded_range_equals_non_empty_range() {
        proptest!(|(non_empty_range in arbitrary::nonempty_range(10))| {
            let unbounded_range = BytesRange::new(Unbounded, Unbounded);
            let intersection = non_empty_range.intersection(&unbounded_range);
            assert_eq!(non_empty_range, intersection);
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
    fn test_both_ranges_contain_values_chosen_from_intersection() {
        proptest!(|(non_empty_1 in arbitrary::nonempty_range(10), non_empty_2 in arbitrary::nonempty_range(10), mut rng in arbitrary::rng())| {
            let intersection = non_empty_1.intersection(&non_empty_2);
            if intersection.non_empty() {
                let bytes = sample::bytes_in_range(&mut rng, &intersection);
                assert!(non_empty_1.contains(&bytes));
                assert!(non_empty_2.contains(&bytes));
            };
        });
    }
}
