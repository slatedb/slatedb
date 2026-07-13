//! Verifies that RFC-0004 projection and union preserve database contents.
//!
//! Each simulation run:
//! - runs four prefix-scoped workload actors against one root database
//! - quiesces the root and records its complete key/value state
//! - projects the root at `workload-3/` into two adjacent child databases
//! - checks that each child contains exactly its half of the root state
//! - runs the two children concurrently in independent
//!   [`slatedb_dst::Harness`] instances, with workload actors 1-2 assigned to
//!   the left child and 3-4 to the right
//! - quiesces both children and unions them into one database
//! - checks that the union contains exactly the post-workload state of both
//!   children, then runs all four workload actors against the merged database
//!
//! Actor prefixes keep every point operation, write batch, and prefix scan
//! inside one child. The child harnesses use separate seeded runtimes, random
//! number generators, clocks, and fault controllers while sharing the same
//! underlying deterministic object stores. Garbage collection remains active,
//! except for detach GC, so neither child mutates their shared parent's
//! checkpoint state. WALs are disabled because manifest union does not merge
//! WAL state.
//!
#![cfg(dst)]

use rstest::rstest;
use slatedb_dst::RescalingScenario;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

#[rstest]
#[cfg_attr(not(slow), case::regular(200))]
#[cfg_attr(slow, case::slow(2_400_000))]
fn test_dst_rescaling_preserves_data(#[case] shutdown_at_ms: i64) -> TestResult<()> {
    RescalingScenario {
        name: "rescaling",
        shutdown_at_ms,
    }
    .run()
}
