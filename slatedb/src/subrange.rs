use std::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

/// Bounds over a subrange of key suffixes, as accepted by
/// [`Db::scan_prefix`](crate::Db::scan_prefix) and
/// [`Db::scan_prefix_with_options`](crate::Db::scan_prefix_with_options).
///
/// This mirrors [`std::ops::RangeBounds`] over byte strings, but is
/// monomorphic in the bound type it yields. With a generic
/// `RangeBounds<K>` parameter the unbounded subrange `..` fails to infer
/// `K` (E0283) and every such call would need a turbofish; with this
/// trait `..` resolves to the dedicated [`RangeFull`] impl, while the
/// other standard range types accept any `AsRef<[u8]>` bound, so
/// `b"a"..b"b"`, `vec_start..`, `..=bytes_end`, explicit
/// [`Bound`] pairs, and `..` all work unannotated.
pub trait SubrangeBounds {
    /// The start bound of the subrange, as a byte-slice bound.
    fn start_bound(&self) -> Bound<&[u8]>;
    /// The end bound of the subrange, as a byte-slice bound.
    fn end_bound(&self) -> Bound<&[u8]>;
}

impl SubrangeBounds for RangeFull {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for Range<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start.as_ref())
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Excluded(self.end.as_ref())
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for RangeInclusive<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start().as_ref())
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.end().as_ref())
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for RangeFrom<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.start.as_ref())
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for RangeTo<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Excluded(self.end.as_ref())
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for RangeToInclusive<K> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.end.as_ref())
    }
}

impl<K: AsRef<[u8]>> SubrangeBounds for (Bound<K>, Bound<K>) {
    fn start_bound(&self) -> Bound<&[u8]> {
        self.0.as_ref().map(|k| k.as_ref())
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        self.1.as_ref().map(|k| k.as_ref())
    }
}

impl<T: SubrangeBounds + ?Sized> SubrangeBounds for &T {
    fn start_bound(&self) -> Bound<&[u8]> {
        (**self).start_bound()
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        (**self).end_bound()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn bounds(subrange: impl SubrangeBounds) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (
            subrange.start_bound().map(|b| b.to_vec()),
            subrange.end_bound().map(|b| b.to_vec()),
        )
    }

    #[test]
    fn test_range_full() {
        assert_eq!(bounds(..), (Bound::Unbounded, Bound::Unbounded));
    }

    #[test]
    fn test_range_over_byte_string_literals() {
        // `b"a"` is `&[u8; 1]`; the whole point of the AsRef impls is
        // that array references work without `.as_slice()`.
        assert_eq!(
            bounds(b"a"..b"z"),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec())
            )
        );
        assert_eq!(
            bounds(b"a"..=b"z"),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Included(b"z".to_vec())
            )
        );
    }

    #[test]
    fn test_half_bounded_ranges() {
        assert_eq!(
            bounds(b"m"..),
            (Bound::Included(b"m".to_vec()), Bound::Unbounded)
        );
        assert_eq!(
            bounds(..b"m"),
            (Bound::Unbounded, Bound::Excluded(b"m".to_vec()))
        );
        assert_eq!(
            bounds(..=b"m"),
            (Bound::Unbounded, Bound::Included(b"m".to_vec()))
        );
    }

    #[test]
    fn test_owned_bound_types() {
        assert_eq!(
            bounds(b"a".to_vec()..b"z".to_vec()),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec())
            )
        );
        assert_eq!(
            bounds(Bytes::from_static(b"a")..Bytes::from_static(b"z")),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec())
            )
        );
    }

    #[test]
    fn test_bound_pair() {
        assert_eq!(
            bounds((Bound::Excluded(b"a"), Bound::Included(b"z"))),
            (
                Bound::Excluded(b"a".to_vec()),
                Bound::Included(b"z".to_vec())
            )
        );
        let unbounded: (Bound<&[u8]>, Bound<&[u8]>) = (Bound::Unbounded, Bound::Unbounded);
        assert_eq!(bounds(unbounded), (Bound::Unbounded, Bound::Unbounded));
    }

    #[test]
    fn test_reference_to_subrange() {
        assert_eq!(
            bounds(&(b"a"..b"z")),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec())
            )
        );
    }
}
