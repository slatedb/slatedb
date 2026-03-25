//! Parallel L0 memtable flusher pipeline.
//!
//! Coordinates the flush of immutable memtables to L0 SSTs in object storage.
//! The pipeline consists of three components:
//! - **flusher**: control plane for flush requests, dispatch, and waiter tracking
//! - **uploader**: parallel worker pool for SST build and upload
//! - **manifest_writer**: ordered manifest retirement and checkpoint creation

/// Monotonic ordering token assigned by the parallel L0 memtable flusher.
///
/// Workers carry this through upload completion so the manifest writer can restore
/// the original immutable-memtable retirement order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FlushEpoch(pub(crate) u64);

mod flusher;
mod manifest_writer;
mod uploader;

pub(crate) use flusher::{FlushTarget, MemtableFlusher};
pub(crate) use manifest_writer::FlushResult;
