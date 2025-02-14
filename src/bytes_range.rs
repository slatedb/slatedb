use bytes::Bytes;
use serde::Serialize;
use std::cmp::{max, min, Ordering};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

/// Concrete struct representing a range of Bytes. Gets around much of
/// the cumbersome work associated with the generic trait RangeBounds<Bytes>
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
pub(crate) struct BytesRange {
    start_bound: Bound<Bytes>,
    end_bound: Bound<Bytes>,
}

impl Ord for BytesRange {
    fn cmp(&self, other: &Self) -> Ordering {
        let (this_start, other_start) = (
            ComparableBound::from(self.start_bound.clone()),
            ComparableBound::from(other.start_bound.clone()),
        );
        let (this_end, other_end) = (
            ComparableBound::from(self.end_bound.clone()),
            ComparableBound::from(other.end_bound.clone()),
        );
        this_start.cmp(&other_start).then(this_end.cmp(&other_end))
    }
}

impl PartialOrd for BytesRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
        Self {
            start_bound: start,
            end_bound: end,
        }
    }

    pub(crate) fn as_ref(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            self.start_bound().map(|b| b.as_ref()),
            self.end_bound().map(|b| b.as_ref()),
        )
    }

    #[cfg(test)]
    pub(crate) fn non_empty(&self) -> bool {
        !self.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        let bounds = self.as_ref();
        is_empty(bounds.0, bounds.1)
    }
}

fn is_empty(start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>) -> bool {
    match end_bound {
        Unbounded => false,
        Included(end) => match start_bound {
            Unbounded => false,
            Included(start) => start > end,
            Excluded(start) => start >= end,
        },
        Excluded(end) => match start_bound {
            Unbounded => end.len() == 0,
            Included(start_bytes) => start_bytes >= end,
            Excluded(start) if start >= end => true,
            Excluded(start) => is_prefix_increment(start, end),
        },
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

pub(crate) fn intersect(r1: &BytesRange, r2: &BytesRange) -> Option<BytesRange> {
    // &BytesRange::from_ref(..="f"),
    // &BytesRange::from_ref("d".."e")
    let max_start_bound = max(
        ComparableBound::from(r1.start_bound()),
        ComparableBound::from(r2.start_bound()),
    );
    let min_end_bound = min(
        ComparableBound::from(r1.end_bound()),
        ComparableBound::from(r2.end_bound()),
    );
    // Included(b"d") Excluded(b"e")
    println!("{:?} {:?}", max_start_bound.inner, min_end_bound.inner);
    if max_start_bound <= min_end_bound {
        Some(BytesRange {
            start_bound: max_start_bound.inner.cloned(),
            end_bound: min_end_bound.inner.cloned(),
        })
    } else {
        None
    }
}

#[derive(Debug, Eq)]
struct ComparableBound<T: Ord> {
    inner: Bound<T>,
}

impl<T: Ord> From<Bound<T>> for ComparableBound<T> {
    fn from(bound: Bound<T>) -> Self {
        Self { inner: bound }
    }
}

impl<T: Ord> From<ComparableBound<T>> for Bound<T> {
    fn from(bound: ComparableBound<T>) -> Self {
        bound.inner
    }
}

impl<T: Ord> PartialEq for ComparableBound<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: Ord> Ord for ComparableBound<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.inner, &other.inner) {
            (Bound::Included(a), Bound::Included(b)) => a.cmp(b),
            (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
            (Bound::Included(a), Bound::Excluded(b)) => match a.cmp(b) {
                Ordering::Equal => Ordering::Greater,
                other => other,
            },
            (Bound::Excluded(a), Bound::Included(b)) => match a.cmp(b) {
                Ordering::Equal => Ordering::Less,
                other => other,
            },
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Less,
            (_, Bound::Unbounded) => Ordering::Greater,
        }
    }
}

impl<T: Ord> PartialOrd for ComparableBound<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn merge(r1: &BytesRange, r2: &BytesRange) -> Option<BytesRange> {
    let (first_range, second_range) = if r1 < r2 { (r1, r2) } else { (r2, r1) };
    let first_end_bound = ComparableBound::from(first_range.end_bound.clone());
    let second_start_bound = ComparableBound::from(second_range.start_bound.clone());
    let second_end_bound = ComparableBound::from(second_range.end_bound.clone());
    println!(
        "Merge: {:?} {:?}",
        first_end_bound.inner, second_start_bound.inner
    );
    if first_end_bound >= second_start_bound
        || are_contiguous(&first_end_bound, &second_start_bound)
    {
        Some(BytesRange {
            start_bound: first_range.start_bound.clone(),
            end_bound: max(first_end_bound, second_end_bound).into(),
        })
    } else {
        None
    }
}

fn are_contiguous(first: &ComparableBound<Bytes>, second: &ComparableBound<Bytes>) -> bool {
    if first == second {
        return true;
    }
    match (&first.inner, &second.inner) {
        (Bound::Included(a), Bound::Excluded(b)) => a == b,
        (Bound::Excluded(a), Bound::Included(b)) => a == b,
        _ => false,
    }
}

pub(crate) fn has_nonempty_intersection(
    r1: (Bound<&[u8]>, Bound<&[u8]>),
    r2: (Bound<&[u8]>, Bound<&[u8]>),
) -> bool {
    let start_bound = max_start_bound(r1.0, r2.0);
    let end_bound = min_end_bound(r1.1, r2.1);
    !is_empty(start_bound, end_bound)
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::bytes_range::{
        has_nonempty_intersection, intersect, merge, BytesRange, ComparableBound,
    };
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;

    use proptest::proptest;
    use std::ops::Bound::{self, Unbounded};
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
            assert!(!has_nonempty_intersection(
                empty_range.as_ref(),
                non_empty_range.as_ref(),
            ))
        });
    }

    #[test]
    fn test_intersection_of_non_empty_and_unbounded_range_is_nonempty() {
        proptest!(|(non_empty_range in arbitrary::nonempty_range(10))| {
            let unbounded_range = BytesRange::new(Unbounded, Unbounded);
            assert!(has_nonempty_intersection(
                non_empty_range.as_ref(),
                unbounded_range.as_ref()
            ))
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
            assert!(has_nonempty_intersection(
                non_empty_1.as_ref(),
                non_empty_2.as_ref(),
            ));
        });
    }

    #[test]
    fn test_bytes_intersection() {
        assert_eq!(
            intersect(
                &BytesRange::from_ref("a".."d"),
                &BytesRange::from_ref("d".."e")
            ),
            None
        );
        assert_eq!(
            intersect(
                &BytesRange::from_ref("a"..="d"),
                &BytesRange::from_ref("d".."e")
            ),
            Some(BytesRange::from_ref("d"..="d"))
        );
        assert_eq!(
            intersect(
                &BytesRange::from_ref(..="f"),
                &BytesRange::from_ref("d".."e")
            ),
            Some(BytesRange::from_ref("d".."e"))
        );
    }

    #[test]
    fn test_bytes_merge() {
        assert_eq!(
            merge(
                &BytesRange::from_ref("a".."d"),
                &BytesRange::from_ref("d".."e")
            ),
            Some(BytesRange::from_ref("a".."e"))
        );
        assert_eq!(
            merge(
                &BytesRange::from_ref("a".."d"),
                &BytesRange::from_ref("e".."f")
            ),
            None
        );
        assert_eq!(
            merge(
                &BytesRange::from_ref("a"..="d"),
                &BytesRange::from_ref("d".."e")
            ),
            Some(BytesRange::from_ref("a".."e"))
        );
        assert_eq!(
            merge(
                &BytesRange::from_ref(..="f"),
                &BytesRange::from_ref("d".."e")
            ),
            Some(BytesRange::from_ref(..="f"))
        );
        assert_eq!(
            merge(
                &BytesRange::from_ref("f".."m"),
                &BytesRange::from_ref("a".."g")
            ),
            Some(BytesRange::from_ref("a".."m"))
        );
    }

    #[test]
    fn test_comparable_bound() {
        assert!(
            ComparableBound::from(Bound::Included(1)) == ComparableBound::from(Bound::Included(1))
        );
        assert!(
            ComparableBound::from(Bound::Excluded(1)) == ComparableBound::from(Bound::Excluded(1))
        );
        assert!(
            ComparableBound::from(Bound::Included(1)) > ComparableBound::from(Bound::Excluded(1))
        );
        assert!(
            ComparableBound::from(Bound::Excluded(1)) < ComparableBound::from(Bound::Included(1))
        );
        assert!(
            ComparableBound::from(Bound::Excluded("d"))
                < ComparableBound::from(Bound::Included("e"))
        );
        assert!(
            ComparableBound::from(Bound::Included("d"))
                > ComparableBound::from(Bound::Excluded("d"))
        );
    }
}
