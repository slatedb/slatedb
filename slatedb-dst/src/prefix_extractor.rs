//! Deterministic prefix extractors for DST scenarios.

use slatedb::{PrefixExtractor, PrefixTarget};

/// Deterministic prefix extractor that segments keys at the first `/`
/// delimiter (inclusive). It matches the actor-namespaced key shape used
/// by the bundled DST workload and bank actors, where each actor or
/// account namespace writes under `{name}/...`.
///
/// The returned prefix length always points *past* the delimiter, so the
/// extracted prefix retains the trailing `/` (for example
/// `b"workload-1/foo"` extracts to prefix `b"workload-1/"`).
///
/// For [`PrefixTarget::Prefix`], the extractor returns `Some(n)` only when
/// the input already contains a `/`; otherwise extensions of the prefix
/// could place the first delimiter at different positions and probing
/// would be unsafe. This satisfies the trait's `Prefix` invariant.
#[derive(Debug)]
pub struct FirstDelimiterPrefixExtractor;

impl PrefixExtractor for FirstDelimiterPrefixExtractor {
    fn name(&self) -> &str {
        "first-delim-/"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes: &[u8] = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b.as_ref(),
        };
        bytes.iter().position(|&b| b == b'/').map(|idx| idx + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn point(s: &'static str) -> PrefixTarget {
        PrefixTarget::Point(Bytes::from_static(s.as_bytes()))
    }

    fn prefix(s: &'static str) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn point_returns_position_after_first_delimiter() {
        let extractor = FirstDelimiterPrefixExtractor;
        assert_eq!(extractor.prefix_len(&point("workload-1/0")), Some(11));
        assert_eq!(extractor.prefix_len(&point("a/b/c")), Some(2));
        assert_eq!(extractor.prefix_len(&point("/x")), Some(1));
    }

    #[test]
    fn point_returns_none_without_delimiter() {
        let extractor = FirstDelimiterPrefixExtractor;
        assert_eq!(extractor.prefix_len(&point("nodelim")), None);
        assert_eq!(extractor.prefix_len(&point("")), None);
    }

    #[test]
    fn prefix_returns_none_until_delimiter_is_present() {
        let extractor = FirstDelimiterPrefixExtractor;
        // Without a `/`, an extension could place the first delimiter at
        // any later position, so we cannot safely return a length.
        assert_eq!(extractor.prefix_len(&prefix("workload-")), None);
        // Once the delimiter is in the prefix, every extension keeps it
        // anchored at the same position.
        assert_eq!(extractor.prefix_len(&prefix("workload-1/")), Some(11));
        assert_eq!(extractor.prefix_len(&prefix("workload-1/foo")), Some(11));
    }
}
