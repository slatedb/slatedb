use std::collections::HashMap;
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use chrono::TimeDelta;
use futures::future::{join, join_all};
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::compactor::CompactorMessage;
use crate::compactor::CompactorMessage::CompactionJobFinished;
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::merge_iterator::MergeIterator;
use crate::merge_operator::{
    MergeOperatorIterator, MergeOperatorRequiredIterator, MergeOperatorType,
};
use crate::peeking_iterator::PeekingIterator;
use crate::rand::DbRand;
use crate::retention_iterator::RetentionIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use slatedb_common::clock::SystemClock;

use crate::compactor::stats::CompactionStats;
use crate::utils::{
    build_concurrent, compute_max_parallel, last_written_key_and_seq, spawn_bg_task, IdGenerator,
};
use log::{debug, error};
use tracing::instrument;
use ulid::Ulid;

/// Arguments for starting a compaction job.
///
/// - `id` is the job id (ULID) and uniquely identifies a single job. This is
///   used as the runtime key in `scheduled_compactions`.
/// - `job_id` is the canonical plan id (ULID) that ties this job back to its
///   `Compaction`
///
/// Jobs carry fully materialized inputs (L0 `ssts` and `sorted_runs`) along with execution-time
/// metadata for progress reporting, retention, and resume logic.
#[derive(Clone, PartialEq)]
pub(crate) struct StartCompactionJobArgs {
    /// Job id. Unique per job.
    pub(crate) id: Ulid,
    /// Canonical compaction job id this job belongs to.
    pub(crate) compaction_id: Ulid,
    /// Destination sorted run id to be produced by this job.
    pub(crate) destination: u32,
    /// Input L0 SSTs for this job.
    pub(crate) ssts: Vec<SsTableHandle>,
    /// Input existing sorted runs for this job.
    pub(crate) sorted_runs: Vec<SortedRun>,
    /// Output SSTs already written for this compaction when resuming.
    pub(crate) output_ssts: Vec<SsTableHandle>,
    /// The logical clock tick representing the logical time the compaction occurs. This is used
    /// to make decisions about retention of expiring records.
    pub(crate) compaction_logical_clock_tick: i64,
    /// Whether the destination sorted run is the last (newest) run after compaction.
    pub(crate) is_dest_last_run: bool,
    /// Optional minimum sequence to retain; lower sequences may be dropped by retention.
    pub(crate) retention_min_seq: Option<u64>,
    /// Estimated total source bytes for this compaction.
    pub(crate) estimated_source_bytes: u64,
}

impl std::fmt::Debug for StartCompactionJobArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartCompactionJobArgs")
            .field("id", &self.id)
            .field("job_id", &self.compaction_id)
            .field("destination", &self.destination)
            .field("ssts", &self.ssts)
            .field("sorted_runs", &self.sorted_runs)
            .field("output_ssts", &self.output_ssts)
            .field(
                "compaction_logical_clock_tick",
                &self.compaction_logical_clock_tick,
            )
            .field("is_dest_last_run", &self.is_dest_last_run)
            .field("estimated_source_bytes", &self.estimated_source_bytes)
            .field("retention_min_seq", &self.retention_min_seq)
            .finish()
    }
}

/// Executes compaction jobs produced by the compactor.
pub(crate) trait CompactionExecutor {
    /// Starts executing a compaction job asynchronously.
    fn start_compaction_job(&self, compaction: StartCompactionJobArgs);

    /// Stops the executor and cancels any in-flight tasks, waiting for them to finish.
    fn stop(&self);

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool;
}

pub(crate) struct TokioCompactionExecutor {
    inner: Arc<TokioCompactionExecutorInner>,
}

impl TokioCompactionExecutor {
    pub(crate) fn new(
        handle: tokio::runtime::Handle,
        options: Arc<CompactorOptions>,
        worker_tx: tokio::sync::mpsc::UnboundedSender<CompactorMessage>,
        table_store: Arc<TableStore>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        clock: Arc<dyn SystemClock>,
        manifest_store: Arc<ManifestStore>,
        merge_operator: Option<MergeOperatorType>,
    ) -> Self {
        Self {
            inner: Arc::new(TokioCompactionExecutorInner {
                options,
                handle,
                worker_tx,
                table_store,
                rand,
                tasks: Arc::new(Mutex::new(HashMap::new())),
                stats,
                clock,
                is_stopped: AtomicBool::new(false),
                manifest_store,
                merge_operator,
            }),
        }
    }
}

impl CompactionExecutor for TokioCompactionExecutor {
    fn start_compaction_job(&self, compaction: StartCompactionJobArgs) {
        self.inner.start_compaction_job(compaction);
    }

    fn stop(&self) {
        self.inner.stop()
    }

    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }
}

struct TokioCompactionTask {
    task: JoinHandle<Result<SortedRun, SlateDBError>>,
}

pub(crate) struct TokioCompactionExecutorInner {
    options: Arc<CompactorOptions>,
    handle: tokio::runtime::Handle,
    worker_tx: tokio::sync::mpsc::UnboundedSender<CompactorMessage>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    clock: Arc<dyn SystemClock>,
    is_stopped: AtomicBool,
    manifest_store: Arc<ManifestStore>,
    merge_operator: Option<MergeOperatorType>,
}

impl TokioCompactionExecutorInner {
    /// Builds input iterators for all sources (L0 and SR) and wraps them with optional
    /// merge and retention logic.
    async fn load_iterators<'a>(
        &self,
        job_args: &'a StartCompactionJobArgs,
    ) -> Result<PeekingIterator<RetentionIterator<Box<dyn KeyValueIterator + 'a>>>, SlateDBError>
    {
        let resume_cursor = match job_args.output_ssts.last() {
            Some(output_sst) => {
                last_written_key_and_seq(self.table_store.clone(), output_sst).await?
            }
            None => None,
        };
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 4,
            blocks_to_fetch: 256,
            cache_blocks: false, // don't clobber the cache
            eager_spawn: true,
        };

        let max_parallel = compute_max_parallel(job_args.ssts.len(), &job_args.sorted_runs, 4);
        // L0 (borrowed)
        let l0_iters_futures = build_concurrent(job_args.ssts.iter(), max_parallel, |h| {
            SstIterator::new_borrowed_initialized(.., h, self.table_store.clone(), sst_iter_options)
        });

        // SR (borrowed)
        let sr_iters_futures =
            build_concurrent(job_args.sorted_runs.iter(), max_parallel, |sr| async {
                SortedRunIterator::new_borrowed(.., sr, self.table_store.clone(), sst_iter_options)
                    .await
                    .map(Some)
            });

        let (l0_iters_res, sr_iters_res) = join(l0_iters_futures, sr_iters_futures).await;
        let l0_iters = l0_iters_res?;
        let sr_iters = sr_iters_res?;

        let l0_merge_iter = MergeIterator::new(l0_iters)?.with_dedup(false);
        let sr_merge_iter = MergeIterator::new(sr_iters)?.with_dedup(false);

        let merge_iter = MergeIterator::new([l0_merge_iter, sr_merge_iter])?.with_dedup(false);
        let merge_iter = if let Some(merge_operator) = self.merge_operator.clone() {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                merge_iter,
                false,
                job_args.compaction_logical_clock_tick,
                job_args.retention_min_seq,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(merge_iter)) as Box<dyn KeyValueIterator>
        };

        let stored_manifest =
            StoredManifest::load(self.manifest_store.clone(), self.clock.clone()).await?;
        let mut retention_iter = RetentionIterator::new(
            merge_iter,
            None,
            job_args.retention_min_seq,
            job_args.is_dest_last_run,
            job_args.compaction_logical_clock_tick,
            self.clock.clone(),
            Arc::new(stored_manifest.db_state().sequence_tracker.clone()),
        )
        .await?;
        retention_iter.init().await?;
        // When resuming, seek to the last written key and then drain entries for that key until
        // we reach the last written seq.
        let mut peeking_iter = PeekingIterator::new(retention_iter);
        if let Some((last_key, last_seq)) = resume_cursor {
            peeking_iter.seek(last_key.as_ref()).await?;
            loop {
                // break on empty
                let Some(entry) = peeking_iter.peek().await? else {
                    break;
                };
                // break on new key
                if entry.key.as_ref() != last_key.as_ref() {
                    break;
                }
                // break on lower seq (because entries are sorted descending by seq for the same key)
                if entry.seq < last_seq {
                    break;
                }
                peeking_iter.next_entry().await?;
            }
        }
        Ok(peeking_iter)
    }

    /// Executes a single compaction job and returns the resulting [`SortedRun`].
    ///
    /// ## Steps
    /// - Streams and merges input keys across all sources
    /// - Applies merge and retention policies
    /// - Writes output SSTs up to `max_sst_size`, reporting periodic progress
    ///
    /// ## Returns
    /// - The destination [`SortedRun`] with all output SST handles.
    #[instrument(level = "debug", skip_all, fields(id = %args.id))]
    async fn execute_compaction_job(
        &self,
        args: StartCompactionJobArgs,
    ) -> Result<SortedRun, SlateDBError> {
        debug!("executing compaction [job_args={:?}]", args);
        let mut all_iter = self.load_iterators(&args).await?;
        let mut output_ssts = args.output_ssts.clone();
        let mut current_writer = self.table_store.table_writer(SsTableId::Compacted(
            self.rand.rng().gen_ulid(self.clock.as_ref()),
        ));
        let mut bytes_written = 0usize;
        let mut last_progress_report = self.clock.now();

        while let Some(kv) = all_iter.next_entry().await? {
            let duration_since_last_report =
                self.clock.now().signed_duration_since(last_progress_report);
            if duration_since_last_report > TimeDelta::seconds(1) {
                // Allow send() because we are treating the executor like an external
                // component. They can do what they want. If the send fails (e.g., during
                // DB shutdown), we log it and continue with the compaction work.
                #[allow(clippy::disallowed_methods)]
                if let Err(e) = self
                    .worker_tx
                    .send(CompactorMessage::CompactionJobProgress {
                        id: args.id,
                        bytes_processed: all_iter.inner().total_bytes_processed(),
                        output_ssts: output_ssts.clone(),
                    })
                {
                    debug!(
                        "failed to send compaction progress (likely DB shutdown) [error={:?}]",
                        e
                    );
                }
                last_progress_report = self.clock.now();
            }

            if let Some(block_size) = current_writer.add(kv).await? {
                bytes_written += block_size;
            }

            if bytes_written > self.options.max_sst_size {
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store.table_writer(SsTableId::Compacted(
                        self.rand.rng().gen_ulid(self.clock.as_ref()),
                    )),
                );
                let sst = finished_writer.close().await?;

                self.stats.bytes_compacted.add(sst.info.filter_offset);
                output_ssts.push(sst);
                bytes_written = 0;
            }
        }

        if !current_writer.is_drained() {
            let sst = current_writer.close().await?;

            self.stats.bytes_compacted.add(sst.info.filter_offset);
            output_ssts.push(sst);
        }

        Ok(SortedRun {
            id: args.destination,
            ssts: output_ssts,
        })
    }

    /// Starts a background task to run the compaction job.
    fn start_compaction_job(self: &Arc<Self>, args: StartCompactionJobArgs) {
        let mut tasks = self.tasks.lock();
        if self.is_stopped.load(atomic::Ordering::SeqCst) {
            return;
        }
        let dst = args.destination;
        self.stats.running_compactions.inc();
        assert!(!tasks.contains_key(&dst));

        let id = args.id;

        // TODO(sujeetsawala): Add compaction plan to object store with InProgress status

        let this = self.clone();
        let this_cleanup = self.clone();
        let task = spawn_bg_task(
            "compactor_executor".to_string(),
            &self.handle,
            move |result| {
                let result = result.clone();
                {
                    let mut tasks = this_cleanup.tasks.lock();
                    tasks.remove(&dst);
                }
                // Allow send() because we are treating the executor like an external
                // component. They can do what they want. If the send fails (e.g., during
                // DB shutdown), we log it and continue with cleanup.
                #[allow(clippy::disallowed_methods)]
                if let Err(e) = this_cleanup
                    .worker_tx
                    .send(CompactionJobFinished { id, result })
                {
                    debug!(
                        "failed to send compaction finished msg (likely DB shutdown) [error={:?}]",
                        e
                    );
                }
                this_cleanup.stats.running_compactions.dec();
            },
            async move { this.execute_compaction_job(args).await },
        );
        tasks.insert(dst, TokioCompactionTask { task });
    }

    /// Cancels all active compaction tasks and waits for their termination.
    fn stop(&self) {
        // Drain all tasks and abort them, then release tasks lock so
        // the cleanup function in spawn_bg_task (above) can take the
        // lock and remove the task from the map.
        let task_handles = {
            let mut tasks = self.tasks.lock();
            for task in tasks.values() {
                task.task.abort();
            }
            tasks.drain().map(|(_, task)| task.task).collect::<Vec<_>>()
        };

        self.handle.block_on(async {
            let results = join_all(task_handles).await;
            for result in results {
                match result {
                    Err(e) if !e.is_cancelled() => {
                        error!("shutdown error in compaction task [error={:?}]", e);
                    }
                    _ => {}
                }
            }
        });

        self.is_stopped.store(true, atomic::Ordering::SeqCst);
    }

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool {
        self.is_stopped.load(atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::db_state::ManifestCore;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstView;
    use crate::stats::StatRegistry;
    use crate::test_utils::StringConcatMergeOperator;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::Db;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use slatedb_common::clock::DefaultSystemClock;
    use std::cmp::Ordering;
    use std::time::Duration;

    async fn write_sst(
        table_store: &Arc<TableStore>,
        entries: &[RowEntry],
        max_sst_size: usize,
    ) -> Vec<SsTableHandle> {
        if entries.is_empty() {
            return Vec::new();
        }

        // Write entries into one or more SSTs, splitting on the same block-size
        // accounting used by the compactor's output writer.
        let mut output_ssts = Vec::new();
        let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
        let mut bytes_written = 0usize;

        for (index, entry) in entries.iter().cloned().enumerate() {
            if let Some(block_size) = writer.add(entry).await.unwrap() {
                bytes_written += block_size;
            }

            if bytes_written > max_sst_size {
                output_ssts.push(writer.close().await.unwrap());
                bytes_written = 0;

                if index + 1 < entries.len() {
                    writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
                } else {
                    return output_ssts;
                }
            }
        }

        output_ssts.push(writer.close().await.unwrap());
        output_ssts
    }

    #[rstest]
    #[case::l0_only(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_value(b"k02", b"k02-102", 102),
                RowEntry::new_value(b"k03", b"k03-30", 30),
                RowEntry::new_value(b"k03", b"k03-20", 20),
                RowEntry::new_value(b"k04", b"k04-104", 104),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k05", b"k05-105", 105),
                RowEntry::new_value(b"k06", b"k06-106", 106),
                RowEntry::new_value(b"k07", b"k07-107", 107),
                RowEntry::new_value(b"k08", b"k08-108", 108),
                RowEntry::new_value(b"k09", b"k09-109", 109),
                RowEntry::new_value(b"k10", b"k10-110", 110),
            ],
        ],
        Vec::new(),
        4096,
        67_108_864,
        Some(3),
        None,
        vec![
            RowEntry::new_value(b"k03", b"k03-20", 20),
            RowEntry::new_value(b"k04", b"k04-104", 104),
            RowEntry::new_value(b"k05", b"k05-105", 105),
            RowEntry::new_value(b"k06", b"k06-106", 106),
            RowEntry::new_value(b"k07", b"k07-107", 107),
            RowEntry::new_value(b"k08", b"k08-108", 108),
            RowEntry::new_value(b"k09", b"k09-109", 109),
            RowEntry::new_value(b"k10", b"k10-110", 110),
        ]
    )]
    #[case::l0_only_multi_block(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_value(b"k02", b"k02-102", 102),
                RowEntry::new_value(b"k03", b"k03-103", 103),
                RowEntry::new_value(b"k04", b"k04-104", 104),
                RowEntry::new_value(b"k05", b"k05-105", 105),
                RowEntry::new_value(b"k06", b"k06-106", 106),
                RowEntry::new_value(b"k07", b"k07-107", 107),
                RowEntry::new_value(b"k08", b"k08-80", 80),
                RowEntry::new_value(b"k08", b"k08-70", 70),
                RowEntry::new_value(b"k09", b"k09-109", 109),
                RowEntry::new_value(b"k10", b"k10-110", 110),
                RowEntry::new_value(b"k11", b"k11-111", 111),
                RowEntry::new_value(b"k12", b"k12-112", 112),
            ],
        ],
        Vec::new(),
        128,
        1,
        Some(9),
        None,
        vec![
            RowEntry::new_value(b"k09", b"k09-109", 109),
            RowEntry::new_value(b"k10", b"k10-110", 110),
            RowEntry::new_value(b"k11", b"k11-111", 111),
            RowEntry::new_value(b"k12", b"k12-112", 112),
        ]
    )]
    #[case::sr_only(
        Vec::new(),
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k00", b"k00-300", 300),
                    RowEntry::new_value(b"k01", b"k01-301", 301),
                    RowEntry::new_value(b"k02", b"k02-302", 302),
                    RowEntry::new_value(b"k03", b"k03-303", 303),
                    RowEntry::new_value(b"k03", b"k03-33", 33),
                    RowEntry::new_value(b"k04", b"k04-304", 304),
                    RowEntry::new_value(b"k05", b"k05-305", 305),
                ],
            ],
            vec![ // sr(1)
                vec![  // sr(1) sst(0)
                    RowEntry::new_value(b"k06", b"k06-406", 406),
                    RowEntry::new_value(b"k07", b"k07-407", 407),
                    RowEntry::new_value(b"k08", b"k08-408", 408),
                    RowEntry::new_value(b"k09", b"k09-409", 409),
                    RowEntry::new_value(b"k10", b"k10-410", 410),
                    RowEntry::new_value(b"k11", b"k11-411", 411),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(4),
        None,
        vec![
            RowEntry::new_value(b"k04", b"k04-304", 304),
            RowEntry::new_value(b"k05", b"k05-305", 305),
            RowEntry::new_value(b"k06", b"k06-406", 406),
            RowEntry::new_value(b"k07", b"k07-407", 407),
            RowEntry::new_value(b"k08", b"k08-408", 408),
            RowEntry::new_value(b"k09", b"k09-409", 409),
            RowEntry::new_value(b"k10", b"k10-410", 410),
            RowEntry::new_value(b"k11", b"k11-411", 411),
        ]
    )]
    #[case::l0_and_sr(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-500", 500),
                RowEntry::new_value(b"k01", b"k01-501", 501),
                RowEntry::new_value(b"k02", b"k02-502", 502),
                RowEntry::new_value(b"k03", b"k03-503", 503),
                RowEntry::new_value(b"k04", b"k04-504", 504),
                RowEntry::new_value(b"k05", b"k05-90", 90),
                RowEntry::new_value(b"k05", b"k05-80", 80),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k06", b"k06-606", 606),
                RowEntry::new_value(b"k07", b"k07-607", 607),
                RowEntry::new_value(b"k08", b"k08-608", 608),
                RowEntry::new_value(b"k09", b"k09-609", 609),
                RowEntry::new_value(b"k10", b"k10-610", 610),
                RowEntry::new_value(b"k11", b"k11-611", 611),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k12", b"k12-720", 720),
                    RowEntry::new_value(b"k13", b"k13-721", 721),
                    RowEntry::new_value(b"k14", b"k14-722", 722),
                    RowEntry::new_value(b"k15", b"k15-120", 120),
                    RowEntry::new_value(b"k15", b"k15-110", 110),
                    RowEntry::new_value(b"k16", b"k16-726", 726),
                    RowEntry::new_value(b"k17", b"k17-727", 727),
                ],
            ],
            vec![ // sr(1)
                vec![ // sr(1) sst(0)
                    RowEntry::new_value(b"k18", b"k18-830", 830),
                    RowEntry::new_value(b"k19", b"k19-831", 831),
                    RowEntry::new_value(b"k20", b"k20-832", 832),
                    RowEntry::new_value(b"k21", b"k21-833", 833),
                    RowEntry::new_value(b"k22", b"k22-834", 834),
                    RowEntry::new_value(b"k23", b"k23-835", 835),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(5),
        None,
        vec![
            RowEntry::new_value(b"k05", b"k05-80", 80),
            RowEntry::new_value(b"k06", b"k06-606", 606),
            RowEntry::new_value(b"k07", b"k07-607", 607),
            RowEntry::new_value(b"k08", b"k08-608", 608),
            RowEntry::new_value(b"k09", b"k09-609", 609),
            RowEntry::new_value(b"k10", b"k10-610", 610),
            RowEntry::new_value(b"k11", b"k11-611", 611),
            RowEntry::new_value(b"k12", b"k12-720", 720),
            RowEntry::new_value(b"k13", b"k13-721", 721),
            RowEntry::new_value(b"k14", b"k14-722", 722),
            RowEntry::new_value(b"k15", b"k15-120", 120),
            RowEntry::new_value(b"k15", b"k15-110", 110),
            RowEntry::new_value(b"k16", b"k16-726", 726),
            RowEntry::new_value(b"k17", b"k17-727", 727),
            RowEntry::new_value(b"k18", b"k18-830", 830),
            RowEntry::new_value(b"k19", b"k19-831", 831),
            RowEntry::new_value(b"k20", b"k20-832", 832),
            RowEntry::new_value(b"k21", b"k21-833", 833),
            RowEntry::new_value(b"k22", b"k22-834", 834),
            RowEntry::new_value(b"k23", b"k23-835", 835),
        ]
    )]
    #[case::l0_and_sr_overlap(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-900", 900),
                RowEntry::new_value(b"k01", b"k01-901", 901),
                RowEntry::new_value(b"k02", b"k02-902", 902),
                RowEntry::new_value(b"k03", b"k03-903", 903),
                RowEntry::new_value(b"k04", b"k04-904", 904),
                RowEntry::new_value(b"k05", b"k05-905", 905),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k02", b"k02-920", 920),
                RowEntry::new_value(b"k03", b"k03-921", 921),
                RowEntry::new_value(b"k04", b"k04-922", 922),
                RowEntry::new_value(b"k06", b"k06-923", 923),
                RowEntry::new_value(b"k07", b"k07-924", 924),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-800", 800),
                    RowEntry::new_value(b"k03", b"k03-801", 801),
                    RowEntry::new_value(b"k05", b"k05-802", 802),
                    RowEntry::new_value(b"k08", b"k08-803", 803),
                    RowEntry::new_value(b"k09", b"k09-804", 804),
                ],
            ],
            vec![ // sr(1)
                vec![ // sr(1) sst(0)
                    RowEntry::new_value(b"k02", b"k02-850", 850),
                    RowEntry::new_value(b"k04", b"k04-851", 851),
                    RowEntry::new_value(b"k05", b"k05-852", 852),
                    RowEntry::new_value(b"k07", b"k07-853", 853),
                    RowEntry::new_value(b"k10", b"k10-854", 854),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(7),
        None,
        vec![
            RowEntry::new_value(b"k03", b"k03-801", 801),
            RowEntry::new_value(b"k04", b"k04-922", 922),
            RowEntry::new_value(b"k04", b"k04-904", 904),
            RowEntry::new_value(b"k04", b"k04-851", 851),
            RowEntry::new_value(b"k05", b"k05-905", 905),
            RowEntry::new_value(b"k05", b"k05-852", 852),
            RowEntry::new_value(b"k05", b"k05-802", 802),
            RowEntry::new_value(b"k06", b"k06-923", 923),
            RowEntry::new_value(b"k07", b"k07-924", 924),
            RowEntry::new_value(b"k07", b"k07-853", 853),
            RowEntry::new_value(b"k08", b"k08-803", 803),
            RowEntry::new_value(b"k09", b"k09-804", 804),
            RowEntry::new_value(b"k10", b"k10-854", 854),
        ]
    )]
    #[case::tombstones_resume(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_tombstone(b"k02", 105),
                RowEntry::new_value(b"k02", b"k02-103", 103),
                RowEntry::new_value(b"k03", b"k03-106", 106),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-90", 90),
                    RowEntry::new_value(b"k02", b"k02-95", 95),
                    RowEntry::new_value(b"k04", b"k04-107", 107),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(3),
        None,
        vec![
            RowEntry::new_value(b"k02", b"k02-103", 103),
            RowEntry::new_value(b"k02", b"k02-95", 95),
            RowEntry::new_value(b"k03", b"k03-106", 106),
            RowEntry::new_value(b"k04", b"k04-107", 107),
        ]
    )]
    #[case::no_output_ssts(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-10", 10),
                RowEntry::new_value(b"k02", b"k02-25", 25),
                RowEntry::new_value(b"k02", b"k02-20", 20),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k01", b"k01-30", 30),
                RowEntry::new_value(b"k03", b"k03-40", 40),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-15", 15),
                    RowEntry::new_value(b"k02", b"k02-22", 22),
                    RowEntry::new_value(b"k04", b"k04-50", 50),
                ],
            ],
        ],
        4096,
        67_108_864,
        None,
        None,
        vec![
            RowEntry::new_value(b"k00", b"k00-10", 10),
            RowEntry::new_value(b"k01", b"k01-30", 30),
            RowEntry::new_value(b"k01", b"k01-15", 15),
            RowEntry::new_value(b"k02", b"k02-25", 25),
            RowEntry::new_value(b"k02", b"k02-22", 22),
            RowEntry::new_value(b"k02", b"k02-20", 20),
            RowEntry::new_value(b"k03", b"k03-40", 40),
            RowEntry::new_value(b"k04", b"k04-50", 50),
        ]
    )]
    #[case::resume_at_end(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-10", 10),
                RowEntry::new_value(b"k01", b"k01-11", 11),
            ],
        ],
        Vec::new(),
        4096,
        67_108_864,
        Some(1),
        None,
        vec![]
    )]
    #[case::merge_operator_resume(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_merge(b"k01", b"b", 5),
                RowEntry::new_merge(b"k02", b"y", 6),
                RowEntry::new_value(b"k03", b"k03-103", 103),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_merge(b"k01", b"a", 4),
                    RowEntry::new_value(b"k01", b"base", 3),
                    RowEntry::new_merge(b"k02", b"x", 4),
                    RowEntry::new_value(b"k02", b"base2", 2),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(0),
        Some(Arc::new(StringConcatMergeOperator {}) as MergeOperatorType),
        vec![
            RowEntry::new_value(b"k01", b"baseab", 5),
            RowEntry::new_value(b"k02", b"base2xy", 6),
            RowEntry::new_value(b"k03", b"k03-103", 103),
        ]
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_iterators_resume(
        #[case] l0_entry_sets: Vec<Vec<RowEntry>>,
        #[case] sr_entry_sets: Vec<Vec<Vec<RowEntry>>>,
        #[case] block_size: usize,
        #[case] output_max_sst_size: usize,
        #[case] resume_index: Option<usize>,
        #[case] merge_operator: Option<MergeOperatorType>,
        #[case] expected_rows: Vec<RowEntry>,
    ) {
        let handle = tokio::runtime::Handle::current();
        let options = Arc::new(CompactorOptions::default());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root_path = Path::from("testdb-load-iterators");
        let clock = Arc::new(DefaultSystemClock::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat {
                block_size,
                ..SsTableFormat::default()
            },
            root_path.clone(),
            None,
        ));
        let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
        StoredManifest::create_new_db(manifest_store.clone(), ManifestCore::new(), clock.clone())
            .await
            .unwrap();
        let retention_min_seq = if merge_operator.is_some() {
            None
        } else {
            Some(0)
        };
        let executor = TokioCompactionExecutor::new(
            handle,
            options,
            tx,
            table_store.clone(),
            Arc::new(DbRand::new(100u64)),
            Arc::new(CompactionStats::new(Arc::new(StatRegistry::new()))),
            clock,
            manifest_store,
            merge_operator,
        );

        // Materialize L0 SSTs from the provided entry sets. Use a huge max size so
        // each entry set stays in a single SST, keeping the inputs predictable.
        let mut l0_ssts = Vec::new();
        let mut sorted_runs = Vec::new();
        let mut all_entries = Vec::new();

        for entries in &l0_entry_sets {
            let ssts = write_sst(&table_store, entries, usize::MAX).await;
            l0_ssts.extend(ssts);
            all_entries.extend(entries.iter().cloned());
        }

        // Materialize sorted-run SSTs and group them into multiple SortedRun inputs,
        // again keeping each entry set as a single SST by using a huge max size.
        if !sr_entry_sets.is_empty() {
            for (sr_id, sr_sst_sets) in sr_entry_sets.iter().enumerate() {
                let mut sr_ssts = Vec::new();
                for entries in sr_sst_sets {
                    let ssts = write_sst(&table_store, entries, usize::MAX).await;
                    sr_ssts.extend(ssts);
                    all_entries.extend(entries.iter().cloned());
                }
                sorted_runs.push(SortedRun {
                    id: sr_id as u32,
                    ssts: sr_ssts,
                });
            }
        }

        // Sort the merged inputs to match the iterator order (key asc, seq desc).
        let mut sorted_entries = all_entries.clone();
        sorted_entries.sort_by(|left, right| match left.key.cmp(&right.key) {
            Ordering::Equal => right.seq.cmp(&left.seq),
            other => other,
        });
        // Persist the "already-written" output SSTs when resuming from a prefix.
        let output_ssts = if let Some(resume_index) = resume_index {
            assert!(resume_index < sorted_entries.len());
            write_sst(
                &table_store,
                &sorted_entries[..=resume_index],
                output_max_sst_size,
            )
            .await
        } else {
            Vec::new()
        };

        // Build compaction args that resume from the output SST starting point and expect
        // the iterator to continue at the correct next row.
        let job_args = StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            ssts: l0_ssts,
            sorted_runs,
            output_ssts,
            compaction_logical_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq,
            estimated_source_bytes: 0,
        };

        // Verify the resumed iterator yields all remaining rows, starting immediately
        // after the persisted prefix and continuing in sorted order.
        let mut iter = executor.inner.load_iterators(&job_args).await.unwrap();
        let mut resumed_entries = Vec::new();
        while let Some(entry) = iter.next_entry().await.unwrap() {
            resumed_entries.push(entry);
        }
        assert_eq!(resumed_entries, expected_rows);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_should_retain_merges_newer_than_retention_min_seq_num() {
        let handle = tokio::runtime::Handle::current();
        let options = Arc::new(CompactorOptions::default());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let os = Arc::new(InMemory::new());
        let path = "testdb".to_string();
        let clock = Arc::new(DefaultSystemClock::new());
        let db = Db::builder(path.clone(), os.clone())
            .with_system_clock(clock.clone())
            .build()
            .await
            .unwrap();
        let table_store = db.inner.table_store.clone();
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path.as_str()), os.clone()));
        let executor = TokioCompactionExecutor::new(
            handle,
            options,
            tx,
            table_store.clone(),
            Arc::new(DbRand::new(100u64)),
            Arc::new(CompactionStats::new(Arc::new(StatRegistry::new()))),
            clock,
            manifest_store.clone(),
            Some(Arc::new(StringConcatMergeOperator {})),
        );

        // write some merges
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"3", 4))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"2", 3))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"1", 2))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"0", 1))
            .await
            .unwrap();
        let encoded_sst = sst_builder.build().await.unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, encoded_sst, false)
            .await
            .unwrap();
        let retention_min_seq_num = 2;

        // start a compaction of a single sst
        let compaction = StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            ssts: vec![l0],
            sorted_runs: vec![],
            output_ssts: vec![],
            compaction_logical_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq: Some(retention_min_seq_num),
            estimated_source_bytes: 0,
        };
        executor.start_compaction_job(compaction);

        // wait for it to finish
        let result = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let msg = rx.recv().await.unwrap();
                if let CompactorMessage::CompactionJobFinished { id: _, result } = msg {
                    return result;
                }
            }
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(1, result.ssts.len());
        let sst = result.ssts[0].clone();
        let mut iter = SstIterator::new(
            SstView::Borrowed(&sst, BytesRange::from(..)),
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .unwrap();
        iter.init().await.unwrap();
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"3".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 2);
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"2".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 1);
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"01".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num);
        assert!(iter.next_entry().await.unwrap().is_none());
    }
}
