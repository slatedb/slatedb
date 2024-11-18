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
fn is_prefix_increment(prefix: &Bytes, b: &Bytes) -> bool {
    if b.len() != prefix.len() + 1 {
        return false;
    }

    for i in 0..prefix.len() {
        if i >= b.len() || b[i] != prefix[i] {
            return false;
        }
    }

    b[b.len() - 1] == u8::MIN
}

impl BytesRange {
    pub(crate) fn new(start_bound: Bound<Bytes>, end_bound: Bound<Bytes>) -> Self {
        Self {
            start_bound,
            end_bound,
        }
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

impl<T: RangeBounds<Bytes>> From<T> for BytesRange {
    fn from(value: T) -> Self {
        BytesRange {
            start_bound: value.start_bound().cloned(),
            end_bound: value.end_bound().cloned(),
        }
    }
}

fn as_option<T>(bound: Bound<&T>) -> Option<&T>
where
    T: ?Sized,
{
    match bound {
        Included(b) => Some(b),
        Excluded(b) => Some(b),
        Unbounded => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::config::DbRecord;
    use crate::range_util::{is_prefix_increment, BytesRange};
    use bytes::{BufMut, Bytes, BytesMut};
    use proptest::collection::vec;
    use proptest::prelude::{any, Just, Strategy};
    use proptest::{prop_oneof, proptest};
    use rand::random;
    use std::cmp::max;
    use std::ops::Bound;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    pub(crate) fn arbitrary_bytes(size: usize) -> impl Strategy<Value = Bytes> {
        vec(any::<u8>(), 0..size).prop_map(|v| Bytes::from(v))
    }

    pub(crate) fn arbitrary_records(size_upto: usize) -> impl Strategy<Value = DbRecord> {
        (
            arbitrary_nonempty_bytes(size_upto),
            arbitrary_bytes(size_upto),
        )
            .prop_map(|(key, value)| DbRecord::new(key, value))
    }

    pub(crate) fn arbitrary_nonempty_bytes(size: usize) -> impl Strategy<Value = Bytes> {
        vec(any::<u8>(), 1..size).prop_map(|v| Bytes::from(v))
    }

    fn arbitrary_nonempty_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        (arbitrary_bytes(size), arbitrary_bytes(size))
            .prop_filter_map("Filter non-empty ranges", nonempty_range_filter)
            .prop_flat_map(|(start, end)| {
                prop_oneof![
                    Just(BytesRange::new(
                        Included(start.clone()),
                        Included(end.clone())
                    )),
                    Just(BytesRange::new(
                        Excluded(start.clone()),
                        Excluded(end.clone())
                    )),
                    Just(BytesRange::new(
                        Included(start.clone()),
                        Excluded(end.clone())
                    )),
                    Just(BytesRange::new(
                        Excluded(start.clone()),
                        Included(end.clone())
                    )),
                ]
            })
    }

    fn arbitrary_nonempty_partial_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        arbitrary_nonempty_bytes(size).prop_flat_map(|a| {
            prop_oneof![
                Just(BytesRange::new(Unbounded, Included(a.clone()))),
                Just(BytesRange::new(Unbounded, Excluded(a.clone()))),
                Just(BytesRange::new(Included(a.clone()), Unbounded)),
                Just(BytesRange::new(Excluded(a.clone()), Unbounded)),
            ]
        })
    }

    fn extend_prefix(a: &Bytes) -> Bytes {
        let mut b = BytesMut::from(a.as_ref());
        b.put_u8(u8::MIN);
        b.freeze()
    }

    fn arbitrary_single_point_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        arbitrary_bytes(size).prop_flat_map(|a| {
            let a_extended = extend_prefix(&a);
            prop_oneof![
                Just(BytesRange::new(Included(a.clone()), Included(a.clone()))),
                Just(BytesRange::new(
                    Included(a.clone()),
                    Excluded(a_extended.clone())
                )),
                Just(BytesRange::new(
                    Excluded(a.clone()),
                    Included(a_extended.clone())
                )),
            ]
        })
    }

    pub(crate) fn arbitrary_empty_range(size: usize) -> impl Strategy<Value = BytesRange> {
        prop_oneof![
            Just(BytesRange::new(Unbounded, Excluded(Bytes::new()))),
            arbitrary_bytes(size).prop_flat_map(|a| {
                prop_oneof![
                    Just(BytesRange::new(Excluded(a.clone()), Excluded(a.clone()))),
                    Just(BytesRange::new(Included(a.clone()), Excluded(a.clone()))),
                    Just(BytesRange::new(Excluded(a.clone()), Included(a.clone()))),
                ]
            })
        ]
    }

    /// Valid ranges which just have the single empty byte array.
    /// Handled as a special case because an exclusive upper bounded
    /// range with empty bytes (i.e. `..Bytes::new()`) is empty.
    fn nonempty_range_edge_cases() -> impl Strategy<Value = BytesRange> {
        let empty_bytes = Bytes::new();
        prop_oneof![
            Just(BytesRange::new(Unbounded, Included(empty_bytes.clone()))),
            Just(BytesRange::new(Included(empty_bytes.clone()), Unbounded)),
            Just(BytesRange::new(Excluded(empty_bytes.clone()), Unbounded)),
        ]
    }

    fn min_and_max(a: Bytes, b: Bytes) -> (Bytes, Bytes) {
        if a < b {
            (a, b)
        } else {
            (b, a)
        }
    }

    fn nonempty_range_filter(tuple: (Bytes, Bytes)) -> Option<(Bytes, Bytes)> {
        let (a, b) = tuple;
        if a == b {
            None
        } else {
            let (start, end) = min_and_max(a, b);
            if is_prefix_increment(&start, &end) {
                None
            } else {
                Some((start, end))
            }
        }
    }

    pub(crate) fn arbitrary_nonempty_range(size: usize) -> impl Strategy<Value = BytesRange> {
        prop_oneof![
            Just(BytesRange::new(Unbounded, Unbounded)),
            arbitrary_single_point_bounded_range(size),
            arbitrary_nonempty_partial_bounded_range(size),
            arbitrary_nonempty_bounded_range(size),
            nonempty_range_edge_cases()
        ]
    }

    fn increment_lex(b: &Bytes) -> Bytes {
        let mut res = BytesMut::from(b.as_ref());
        for i in (0..res.len()).rev() {
            if res[i] < u8::MAX {
                res[i] += 1;
                return res.freeze();
            } else {
                res[i] = u8::MIN;
            }
        }
        panic!("Overflow when incrementing bytes {b:?}")
    }

    fn decrement_lex(b: &Bytes) -> Bytes {
        let mut res = BytesMut::from(b.as_ref());
        for i in (0..res.len()).rev() {
            if res[i] > u8::MIN {
                res[i] -= 1;
                return res.freeze();
            } else {
                res[i] = u8::MAX;
            }
        }
        panic!("Overflow when decrementing bytes {b:?}")
    }

    fn arbitrary_bytes_between(start: &Bytes, end: &Bytes) -> Bytes {
        assert_eq!(start.len(), end.len());
        let mut res = BytesMut::new();
        let mut start_bound_satisfied = false;
        let mut end_bound_satisfied = false;

        for i in 0..start.len() {
            let next_val_lb = if !start_bound_satisfied {
                start[i]
            } else {
                u8::MIN
            };
            let next_val_ub = if !end_bound_satisfied {
                end[i]
            } else {
                u8::MAX
            };

            let next_val = if next_val_lb == next_val_ub {
                next_val_lb
            } else {
                let next_bound_range_diff = next_val_ub - next_val_lb;
                next_val_lb + (random::<u8>() % next_bound_range_diff)
            };
            res.put_u8(next_val);

            if next_val > next_val_lb {
                start_bound_satisfied = true;
            }

            if next_val < next_val_ub {
                end_bound_satisfied = true;
            }

            let coin_flip: bool = random();
            if start_bound_satisfied && end_bound_satisfied && coin_flip {
                break;
            }
        }

        let res = res.freeze();
        assert!(res >= start && res <= end);
        res
    }

    fn bound_len(bound: Bound<&Bytes>) -> usize {
        match bound {
            Unbounded => 0,
            Included(b) | Excluded(b) => b.len(),
        }
    }

    fn padded_bytes(b: &Bytes, value: u8, len: usize) -> Bytes {
        let mut padded = BytesMut::from(b.as_ref());
        while padded.len() < len {
            padded.put_u8(value);
        }
        padded.freeze()
    }

    fn inclusive_end_bound(range: &BytesRange, len: usize) -> Bytes {
        match range.end_bound() {
            Unbounded => padded_bytes(&Bytes::new(), u8::MAX, len),
            Included(b) | Excluded(b) => {
                let b = padded_bytes(&b, u8::MIN, len);
                decrement_lex(&b)
            }
        }
    }

    fn inclusive_start_bound(range: &BytesRange, len: usize) -> Bytes {
        match range.start_bound() {
            Unbounded => padded_bytes(&Bytes::new(), u8::MIN, len),
            Included(b) => padded_bytes(&b, u8::MIN, len),
            Excluded(b) => {
                let b = padded_bytes(&b, u8::MIN, len);
                increment_lex(&b)
            }
        }
    }

    fn can_decrement_without_truncation(bytes: &Bytes) -> bool {
        bytes.iter().any(|b| *b > u8::MIN)
    }

    fn arbitrary_minvalue_bytes(min_bound: Bound<usize>, max_bound: Bound<usize>) -> Bytes {
        let min_len = match min_bound {
            Unbounded => 0,
            Included(len) => len,
            Excluded(len) => len + 1,
        };

        let max_len = match max_bound {
            Unbounded => min_len + 10,
            Included(len) => len + 1,
            Excluded(len) => len,
        };

        let len = if min_len == max_len {
            min_len
        } else {
            min_len + (random::<usize>() % (max_len - min_len))
        };

        Bytes::from(vec![u8::MIN; len])
    }

    pub(crate) fn arbitrary_bytes_in_range(range: &BytesRange) -> Bytes {
        assert!(
            range.non_empty(),
            "Cannot choose arbitrary value from an empty range"
        );

        if let Some(end) = range.end_bound_opt() {
            if !can_decrement_without_truncation(&end) {
                let min_len = range.start_bound().map(|b| b.len());
                let max_len = range.end_bound().map(|b| b.len());
                return arbitrary_minvalue_bytes(min_len, max_len);
            } else if range.start_bound() == Included(&end) {
                return end.clone();
            } else {
                let start = range.start_bound_opt().unwrap_or_else(|| Bytes::new());
                if is_prefix_increment(&start, &end) {
                    return if range.start_bound() == Included(&start) {
                        start.clone()
                    } else {
                        end.clone()
                    };
                }
            }
        }

        let len = max(bound_len(range.start_bound()), bound_len(range.end_bound())) + 3;

        let start_bound = inclusive_start_bound(&range, len);
        let end_bound = inclusive_end_bound(&range, len);
        arbitrary_bytes_between(&start_bound, &end_bound)
    }

    proptest! {
        #[test]
        fn test_exclusive_incremented_range_is_empty(start in arbitrary_bytes(10)) {
            let end = extend_prefix(&start);
            let range = BytesRange::new(Excluded(start), Excluded(end));
            assert!(range.is_empty());
        }

        #[test]
        fn test_non_empty_ranges_are_non_empty(range in arbitrary_nonempty_range(10)) {
            assert!(range.non_empty());
            assert!(!range.is_empty());
        }

        #[test]
        fn test_empty_ranges_are_empty(range in arbitrary_empty_range(10)) {
            assert!(range.is_empty());
            assert!(!range.non_empty());
        }

        #[test]
        fn test_intersection_of_empty_range_is_empty(
            empty_range in arbitrary_empty_range(10),
            non_empty_range in arbitrary_nonempty_range(10)
        ) {
            let intersection = empty_range.intersection(&non_empty_range);
            assert!(intersection.is_empty());

            let intersection = non_empty_range.intersection(&empty_range);
            assert!(intersection.is_empty());
        }

        #[test]
        fn test_intersection_of_non_empty_range_and_unbounded_range_equals_non_empty_range(
            non_empty_range in arbitrary_nonempty_range(10)
        ) {
            let unbounded_range = BytesRange::new(Unbounded, Unbounded);
            let intersection = non_empty_range.intersection(&unbounded_range);
            assert_eq!(non_empty_range, intersection);
        }

        #[test]
        fn test_contains_with_value_in_range(
            non_empty_range in arbitrary_nonempty_range(10)
        ) {
            let bytes = arbitrary_bytes_in_range(&non_empty_range);
            assert!(non_empty_range.contains(&bytes), "Expected {bytes:?} are not in range {non_empty_range:?}");
        }

        #[test]
        fn test_both_ranges_contain_values_chosen_from_intersection(
            non_empty_1 in arbitrary_nonempty_range(10),
            non_empty_2 in arbitrary_nonempty_range(10)
        ) {
            let intersection = non_empty_1.intersection(&non_empty_2);
            if intersection.non_empty() {
                let bytes = arbitrary_bytes_in_range(&intersection);
                assert!(non_empty_1.contains(&bytes));
                assert!(non_empty_2.contains(&bytes));
            }
        }
    }
}
