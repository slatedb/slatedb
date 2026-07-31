//! Verifies deterministic scenario-test behavior for SlateDB under DST
//! configuration.
//!
//! Each simulation run:
//! - starts from a fresh harness root [`DbRand`] initialized with the same seed
//! - starts from a fresh shared [`MockSystemClock`]
//! - opens a real [`Db`] using randomized deterministic settings from
//!   [`build_settings`]
//! - runs looping workload, flusher, and compactor actors against
//!   deterministic local filesystem-backed object stores until a shutdown actor
//!   cancels the shared token at a fixed mock-clock deadline
//! - compares the next random `u64` and current clock time after the run
//!
//! If either the harness or SlateDB consumes randomness differently, advances
//! the clock differently, or executes different deterministic branches for the
//! same seed, one of those post-run checks will diverge.
#![cfg(dst)]

use rstest::rstest;
use slatedb_dst::DeterministicScenario;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

#[rstest]
#[cfg_attr(not(slow), case::regular(4, 200))]
#[cfg_attr(slow, case::slow(2, 4_000_000))]
fn test_dst_is_deterministic(
    #[case] simulations: u32,
    #[case] shutdown_at_ms: i64,
) -> TestResult<()> {
    DeterministicScenario {
        name: "determinism",
        simulations,
        shutdown_at_ms,
        segment_extractor: None,
    }
    .run()
}
