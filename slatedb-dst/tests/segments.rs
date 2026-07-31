//! Verifies deterministic scenario-test behavior for RFC-0024 segmented
//! SlateDB databases under DST configuration.
//!
//! Mirrors the unsegmented determinism scenario but configures a
//! [`FirstDelimiterPrefixExtractor`] so the workload's `{name}/N` keys
//! route to a distinct segment per actor. The harness shape exercises:
//!
//! - per-segment L0 grouping at memtable flush (one L0 per touched
//!   segment per flush)
//! - per-segment L0 → SR compaction by the standalone compactor
//! - per-segment backpressure via the standalone compactor's interaction
//!   with `l0_max_ssts`
//! - reopen-time reconciliation of the persisted `segment_extractor_name`
//!   (the standalone compactor opens its own `Db` view of the same
//!   manifest)
//! - prefix scans that target a single segment by the workload's
//!   `scan_prefix({name}/)`
//!
//! Determinism is asserted the same way as the unsegmented scenario: the
//! next root RNG value and current mock-clock time must match across
//! independent runs of the same seed.
#![cfg(dst)]

use std::sync::Arc;

use rstest::rstest;
use slatedb::PrefixExtractor;
use slatedb_dst::{DeterministicScenario, FirstDelimiterPrefixExtractor};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

#[rstest]
#[cfg_attr(not(slow), case::regular(4, 200))]
#[cfg_attr(slow, case::slow(2, 2_400_000))]
fn test_dst_segments_is_deterministic(
    #[case] simulations: u32,
    #[case] shutdown_at_ms: i64,
) -> TestResult<()> {
    let extractor: Arc<dyn PrefixExtractor> = Arc::new(FirstDelimiterPrefixExtractor);
    DeterministicScenario {
        name: "segments",
        simulations,
        shutdown_at_ms,
        segment_extractor: Some(extractor),
    }
    .run()
}
