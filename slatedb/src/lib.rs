#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]
#![allow(clippy::result_large_err, clippy::too_many_arguments)]
// Disallow non-approved non-deterministic types and functions in production code
#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods
    )
)]

/// Re-export the bytes crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// without having to depend on the bytes crate directly.
pub use bytes;

/// Re-export the fail-parallel crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// with failpoints in their tests without having to depend on the
/// fail-parallel crate directly.
pub use fail_parallel;

/// Re-export the object store crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// without having to depend on the object store crate directly.
pub use object_store;

pub use batch::WriteBatch;
pub use cached_object_store::stats as cached_object_store_stats;
pub use checkpoint::{Checkpoint, CheckpointCreateResult};
pub use compactor::CompactorBuilder;
pub use config::{Settings, SstBlockSize};
pub use db::{Db, DbBuilder};
pub use db_cache::stats as db_cache_stats;
pub use db_iter::DbIterator;
pub use db_read::DbRead;
pub use db_reader::DbReader;
pub use db_snapshot::DbSnapshot;
pub use db_transaction::DbTransaction;
pub use error::{CloseReason, Error, ErrorKind};
pub use garbage_collector::stats as garbage_collector_stats;
pub use garbage_collector::GarbageCollectorBuilder;
pub use merge_operator::{MergeOperator, MergeOperatorError};
pub use rand::DbRand;
pub use sst::BlockTransformer;
pub use transaction_manager::IsolationLevel;
pub use types::KeyValue;

pub mod admin;
pub mod cached_object_store;
pub mod clock;
#[cfg(feature = "bencher")]
pub mod compaction_execute_bench;
pub mod compactor;
pub mod config;
pub mod db_cache;
pub mod db_stats;
pub mod manifest;
pub mod seq_tracker;
pub mod size_tiered_compaction;
pub mod stats;

mod batch;
mod batch_write;
mod blob;
mod block;
mod block_iterator;
#[cfg(any(test, feature = "bencher"))]
mod bytes_generator;
mod bytes_range;
mod checkpoint;
mod clone;
mod compactions_store;
mod compactor_executor;
mod compactor_state;
mod compactor_state_protocols;
#[allow(dead_code)]
mod comparable_range;
mod db;
mod db_common;
mod db_iter;
mod db_read;
mod db_reader;
mod db_snapshot;
mod db_state;
mod db_transaction;
mod dispatcher;
mod error;
mod filter;
mod filter_iterator;
mod flatbuffer_types;
mod flush;
mod garbage_collector;
mod iter;
mod map_iter;
mod mem_table;
mod mem_table_flush;
mod merge_iterator;
mod merge_operator;
mod object_stores;
mod oracle;
mod partitioned_keyspace;
mod paths;
mod peeking_iterator;
#[cfg(test)]
mod proptest_util;
mod rand;
mod reader;
mod retention_iterator;
mod retrying_object_store;
mod row_codec;
mod sorted_run_iterator;
mod sst;
mod sst_iter;
mod store_provider;
mod tablestore;
#[cfg(test)]
mod test_utils;

// Initialize test infrastructure (deadlock detector, tracing) for all tests.
// This ctor runs at crate load time, ensuring these are set up even for tests
// that don't explicitly use test_utils.
#[cfg(test)]
mod test_init {
    use backtrace::Backtrace;
    use std::sync::Once;
    use std::thread;
    use std::time::Duration;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::EnvFilter;

    static INIT_LOGGING: Once = Once::new();
    static INIT_DEADLOCK_DETECTOR: Once = Once::new();

    /// Extract library name from a file path
    /// e.g., "/Users/.../slatedb/src/db.rs" -> "slatedb"
    /// e.g., "/.cargo/registry/.../parking_lot-0.12.1/src/mutex.rs" -> "parking_lot"
    /// e.g., "/rustc/.../library/std/src/sync/mutex.rs" -> "std"
    fn extract_library(path: &str) -> String {
        // Check for .cargo/registry paths (external crates)
        if let Some(idx) = path.find(".cargo/registry/") {
            let after_registry = &path[idx..];
            // Format: .cargo/registry/src/<hash>/<crate-name>-<version>/...
            // Split: [".cargo", "registry", "src", "<hash>", "<crate-name-version>", ...]
            if let Some(crate_start) = after_registry.split('/').nth(4) {
                // Remove version suffix (e.g., "parking_lot-0.12.1" -> "parking_lot")
                if let Some(dash_idx) = crate_start.rfind('-') {
                    // Check if what follows the dash looks like a version
                    let after_dash = &crate_start[dash_idx + 1..];
                    if after_dash
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_digit())
                    {
                        return crate_start[..dash_idx].to_string();
                    }
                }
                return crate_start.to_string();
            }
        }

        // Check for rustc paths (std library)
        if path.contains("/rustc/") {
            if let Some(idx) = path.find("/library/") {
                let after_library = &path[idx + 9..];
                if let Some(lib_name) = after_library.split('/').next() {
                    return lib_name.to_string();
                }
            }
        }

        // Check for local workspace crates (look for /src/ and take the directory before it)
        if let Some(src_idx) = path.rfind("/src/") {
            let before_src = &path[..src_idx];
            if let Some(last_slash) = before_src.rfind('/') {
                return before_src[last_slash + 1..].to_string();
            }
        }

        "-".to_string()
    }

    /// Format a backtrace as a table, showing all frames
    fn format_backtrace(bt: &Backtrace) -> String {
        // Collect frame info: (function_name, library, filename, line)
        let mut frames: Vec<(String, String, String, String)> = Vec::new();

        for frame in bt.frames() {
            for symbol in frame.symbols() {
                let name = symbol
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string());

                let (library, filename, line) =
                    if let (Some(f), Some(l)) = (symbol.filename(), symbol.lineno()) {
                        let path_str = f.to_string_lossy();
                        let lib = extract_library(&path_str);
                        let file = f
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| "-".to_string());
                        (lib, file, l.to_string())
                    } else {
                        ("-".to_string(), "-".to_string(), "-".to_string())
                    };

                frames.push((name, library, filename, line));
            }
        }

        if frames.is_empty() {
            return "  (no relevant frames found - try RUST_BACKTRACE=1)\n".to_string();
        }

        // Calculate column widths
        let func_width = frames.iter().map(|(f, _, _, _)| f.len()).max().unwrap_or(0);
        let lib_width = frames
            .iter()
            .map(|(_, l, _, _)| l.len())
            .max()
            .unwrap_or(0)
            .max(7); // min width for "Library"
        let file_width = frames
            .iter()
            .map(|(_, _, f, _)| f.len())
            .max()
            .unwrap_or(0)
            .max(4); // min width for "File"
        let line_width = frames
            .iter()
            .map(|(_, _, _, l)| l.len())
            .max()
            .unwrap_or(0)
            .max(4); // min width for "Line"

        let mut output = String::new();

        // Header
        output.push_str(&format!(
            "  {:<func_width$} | {:<lib_width$} | {:<file_width$} | {:>line_width$}\n",
            "Function", "Library", "File", "Line"
        ));
        output.push_str(&format!(
            "  {:-<func_width$}-+-{:-<lib_width$}-+-{:-<file_width$}-+-{:->line_width$}\n",
            "", "", "", ""
        ));

        // Rows
        for (func, library, filename, line) in &frames {
            output.push_str(&format!(
                "  {:<func_width$} | {:<lib_width$} | {:<file_width$} | {:>line_width$}\n",
                func, library, filename, line
            ));
        }

        output
    }

    #[ctor::ctor]
    fn init_test_infrastructure() {
        // Initialize tracing/logging
        INIT_LOGGING.call_once(|| {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_test_writer()
                .init();
        });

        // Initialize deadlock detector
        INIT_DEADLOCK_DETECTOR.call_once(|| {
            thread::spawn(|| loop {
                thread::sleep(Duration::from_secs(10));
                let deadlocks = parking_lot::deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                let separator = "=".repeat(60);
                eprintln!("\n{separator}");
                eprintln!("DEADLOCK DETECTED: {} deadlock(s) found", deadlocks.len());
                eprintln!("{separator}\n");

                for (i, threads) in deadlocks.iter().enumerate() {
                    eprintln!("--- Deadlock #{} ({} threads) ---\n", i + 1, threads.len());
                    for (j, t) in threads.iter().enumerate() {
                        eprintln!("Thread {} (id: {:?}):", j + 1, t.thread_id());
                        eprintln!("{}", format_backtrace(t.backtrace()));
                    }
                }

                eprintln!("{separator}\n");
            });
        });
    }
}
mod transaction_manager;
mod types;
mod utils;

mod wal;
mod wal_buffer;
mod wal_id;
mod wal_replay;
