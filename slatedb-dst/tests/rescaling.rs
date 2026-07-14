//! Verifies that RFC-0004 projection and union preserve database contents under
//! DST workloads.
//!
//! Each simulation run:
//! - opens a root database on a deterministic filesystem-backed object store
//! - runs four prefix-scoped workload actors with randomized SlateDB settings,
//!   object-store faults, flushing, compaction, and garbage collection
//! - quiesces the root and records its complete ordered key/value state
//! - projects the root at `workload-3/` into adjacent left and right databases
//! - checks that each projection exactly matches its range in the root snapshot
//! - runs the children concurrently in separate [`slatedb_dst::Harness`]
//!   instances on one seeded runtime, assigning actors 1-2 to the left child
//!   and actors 3-4 to the right
//! - quiesces both children and records their post-workload state
//! - unions the children and checks that the merged database exactly matches
//!   the two child snapshots
//! - runs all four workload actors against the merged database to confirm that
//!   it remains readable and writable after the union
//!
//! Workload actor names are also key prefixes. The split boundary therefore
//! keeps every point operation, write batch, and prefix scan inside one child.
//! The child harnesses share the underlying object store and seeded runtime but
//! have independent clocks and fault controllers. The single-threaded runtime
//! makes their interleaved object-store operations reproducible from the seed.
//!
//! Garbage collection remains enabled during harness runs. Detach GC is
//! disabled because both children retain checkpoints in the root manifest.
//! WALs are disabled because manifest union rejects sources with live WAL data.
//! Projection and union happen only after their source harnesses have stopped.
//!
//! A snapshot mismatch means projection dropped or misplaced a root row, or
//! union failed to preserve the complete logical state of both children.
#![cfg(dst)]

use rstest::rstest;
use slatedb_dst::RescalingScenario;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

#[rstest]
#[cfg_attr(not(slow), case::regular(200))]
// Four physical harness clocks (root, left, right, and merged) make this a
// 4.8M ms aggregate mock-clock budget per seed.
#[cfg_attr(slow, case::slow(1_200_000))]
fn test_dst_rescaling_preserves_data(#[case] shutdown_at_ms: i64) -> TestResult<()> {
    RescalingScenario {
        name: "rescaling",
        shutdown_at_ms,
    }
    .run()
}
