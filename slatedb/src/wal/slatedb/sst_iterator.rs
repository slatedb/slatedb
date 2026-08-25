use std::collections::VecDeque;
use std::sync::Arc;

use log::error;
use tokio::task::{JoinError, JoinHandle};

use super::resource_limiter::{ResourceGuard, ResourceLimiter};
use super::store::{WalFileHandle, WalTableStore};
use crate::block_iterator::DataBlockIterator;
use crate::config::SstBlockSize;
use crate::error::SlateDBError;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::types::RowEntry;
use crate::utils::panic_string;

enum FetchTask {
    InFlight {
        join_handle: JoinHandle<Result<VecDeque<Arc<Block>>, SlateDBError>>,
        _fetch_guard: ResourceGuard,
        _buffer_guard: ResourceGuard,
    },
    Finished {
        blocks: VecDeque<Arc<Block>>,
        _buffer_guard: ResourceGuard,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct WalSstIteratorOptions {
    /// Target encoded bytes per block-fetch request.
    pub(crate) target_bytes_to_fetch: usize,
    /// Shared limit on in-flight WAL block fetches.
    pub(crate) fetch_limiter: ResourceLimiter,
    /// Shared limit on bytes reserved by queued WAL block fetches.
    pub(crate) buffer_limiter: ResourceLimiter,
}

impl Default for WalSstIteratorOptions {
    fn default() -> Self {
        Self {
            target_bytes_to_fetch: 1,
            fetch_limiter: ResourceLimiter::new(1),
            buffer_limiter: ResourceLimiter::new(SstBlockSize::default().as_bytes()),
        }
    }
}

enum IteratorState {
    Uninitialized,
    Active(DataBlockIterator<Arc<Block>>),
    Finished,
}

/// Iterates all rows in one WAL SST in ascending sequence order.
///
/// WAL replay only performs whole-file scans, so this iterator deliberately has
/// no range/view abstraction, filters, cache controls, descending mode, or seek
/// operation. Metadata loading and speculative fetch spawning remain separate
/// so replay can preload several WAL files under shared resource limits.
pub(crate) struct WalSstIterator {
    table: WalFileHandle,
    index: Option<Arc<SsTableIndexOwned>>,
    state: IteratorState,
    next_block_idx_to_fetch: usize,
    fetch_tasks: VecDeque<FetchTask>,
    table_store: Arc<WalTableStore>,
    options: WalSstIteratorOptions,
}

impl WalSstIterator {
    pub(crate) fn new(
        table: WalFileHandle,
        table_store: Arc<WalTableStore>,
        options: WalSstIteratorOptions,
    ) -> Self {
        assert!(options.target_bytes_to_fetch > 0);
        Self {
            table,
            index: None,
            state: IteratorState::Uninitialized,
            next_block_idx_to_fetch: 0,
            fetch_tasks: VecDeque::new(),
            table_store,
            options,
        }
    }

    /// Loads the WAL SST index without initializing row iteration or fetching
    /// data blocks.
    pub(crate) async fn load_metadata(&mut self) -> Result<(), SlateDBError> {
        if self.index.is_none() {
            self.index = Some(self.table_store.read_index(&self.table).await?);
            self.next_block_idx_to_fetch = 0;
        }
        Ok(())
    }

    /// Starts as many speculative block fetches as the shared allocators permit.
    pub(crate) fn spawn_fetches(&mut self) -> usize {
        self.do_spawn_fetches(false)
    }

    /// Initializes row iteration. This is idempotent and forces one fetch over
    /// its resource limits when that is necessary to make progress.
    pub(crate) async fn init(&mut self) -> Result<(), SlateDBError> {
        if matches!(self.state, IteratorState::Uninitialized) {
            self.advance_block().await?;
        }
        Ok(())
    }

    /// Returns the next WAL row in ascending sequence order.
    pub(crate) async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if matches!(self.state, IteratorState::Uninitialized) {
            return Err(SlateDBError::IteratorNotInitialized);
        }

        loop {
            match &mut self.state {
                IteratorState::Uninitialized => unreachable!("initialization checked above"),
                IteratorState::Finished => return Ok(None),
                IteratorState::Active(iter) => {
                    if let Some(row) = iter.next().await? {
                        return Ok(Some(row));
                    }
                }
            }
            self.advance_block().await?;
        }
    }

    fn allocate_fetch_resources(
        &self,
        buffer_usage: usize,
        force: bool,
    ) -> Option<(ResourceGuard, ResourceGuard)> {
        let fetch_guard = self.options.fetch_limiter.allocate(1, force)?;
        let buffer_guard = self.options.buffer_limiter.allocate(buffer_usage, force)?;
        Some((fetch_guard, buffer_guard))
    }

    fn do_spawn_fetches(&mut self, force_progress: bool) -> usize {
        let Some(index) = self.index.as_ref() else {
            return 0;
        };
        let num_blocks = index.borrow().block_meta().len();
        let mut spawned = 0;

        while self.next_block_idx_to_fetch < num_blocks {
            let blocks = self.table_store.block_range_for_target_bytes(
                &self.table,
                index,
                self.next_block_idx_to_fetch,
                self.options.target_bytes_to_fetch,
            );
            let buffer_usage =
                self.table_store
                    .block_range_size(&self.table, index, blocks.clone());
            let resources = match self.allocate_fetch_resources(buffer_usage, false) {
                Some(resources) => Some(resources),
                None if force_progress && self.fetch_tasks.is_empty() => {
                    let resources = self.allocate_fetch_resources(buffer_usage, true);
                    assert!(resources.is_some(), "forced allocation must make progress");
                    resources
                }
                None => None,
            };
            let Some((fetch_guard, buffer_guard)) = resources else {
                break;
            };

            let table_store = Arc::clone(&self.table_store);
            let table = self.table.clone();
            let index = Arc::clone(index);
            let next_block_idx_to_fetch = blocks.end;
            let join_handle = tokio::spawn(async move {
                table_store
                    .read_blocks_using_index(&table, index, blocks)
                    .await
            });
            self.fetch_tasks.push_back(FetchTask::InFlight {
                join_handle,
                _fetch_guard: fetch_guard,
                _buffer_guard: buffer_guard,
            });
            self.next_block_idx_to_fetch = next_block_idx_to_fetch;
            spawned += 1;
        }

        spawned
    }

    async fn next_block_iter(
        &mut self,
        spawn_fetches: bool,
    ) -> Result<Option<DataBlockIterator<Arc<Block>>>, SlateDBError> {
        let Some(index) = self.index.as_ref() else {
            return Ok(None);
        };
        let num_blocks = index.borrow().block_meta().len();

        loop {
            if spawn_fetches {
                self.do_spawn_fetches(true);
            }

            let fetched_blocks = match self.fetch_tasks.front_mut() {
                Some(FetchTask::InFlight { join_handle, .. }) => Some(
                    join_handle
                        .await
                        .map_err(|error| block_fetch_join_error(error, self.table.id.value()))??,
                ),
                _ => None,
            };
            if let Some(blocks) = fetched_blocks {
                let buffer_guard = match self.fetch_tasks.pop_front().expect("task must exist") {
                    FetchTask::InFlight {
                        _fetch_guard: fetch_guard,
                        _buffer_guard,
                        ..
                    } => {
                        drop(fetch_guard);
                        _buffer_guard
                    }
                    FetchTask::Finished { .. } => unreachable!("task state changed unexpectedly"),
                };
                self.fetch_tasks.push_front(FetchTask::Finished {
                    blocks,
                    _buffer_guard: buffer_guard,
                });
                continue;
            }

            if let Some(FetchTask::Finished { blocks, .. }) = self.fetch_tasks.front_mut() {
                if let Some(block) = blocks.pop_front() {
                    return Ok(Some(DataBlockIterator::new(
                        block,
                        self.table.format_version,
                        IterationOrder::Ascending,
                    )?));
                }
                self.fetch_tasks.pop_front();
                continue;
            }

            assert!(self.fetch_tasks.is_empty());
            if spawn_fetches {
                assert_eq!(self.next_block_idx_to_fetch, num_blocks);
            }
            return Ok(None);
        }
    }

    async fn advance_block(&mut self) -> Result<(), SlateDBError> {
        self.load_metadata().await?;
        self.state = match self.next_block_iter(true).await? {
            Some(iter) => IteratorState::Active(iter),
            None => IteratorState::Finished,
        };
        Ok(())
    }
}

fn block_fetch_join_error(error: JoinError, wal_id: u64) -> SlateDBError {
    let task_name = format!("wal_sst_block_fetch[{wal_id}]");
    match error.try_into_panic() {
        Ok(panic_error) => {
            error!(
                "WAL SST block fetch task panicked unexpectedly. [task_name={}, panic={}]",
                task_name,
                panic_string(&panic_error),
            );
            SlateDBError::BackgroundTaskPanic(task_name)
        }
        Err(_) => SlateDBError::BackgroundTaskCancelled(task_name),
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::*;
    use crate::flatbuffer_types::FlatBufferSsTableInfoCodec;
    use crate::format::sst::SsTableFormat;
    use crate::object_store_tag::TableStoreKind;
    use crate::wal::slatedb::sst_builder::EncodedWalSsTableBuilder;

    fn test_store() -> Arc<WalTableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Arc::new(WalTableStore::new(
            object_store,
            SsTableFormat::default(),
            Path::from("test-db"),
            TableStoreKind::Main,
        ))
    }

    fn test_rows() -> Vec<RowEntry> {
        (1..=6)
            .map(|seq| {
                RowEntry::new_value(
                    format!("key-{seq}").as_bytes(),
                    format!("value-{seq}").as_bytes(),
                    seq,
                )
            })
            .collect()
    }

    async fn write_test_wal(
        store: &WalTableStore,
        wal_id: u64,
        rows: &[RowEntry],
    ) -> WalFileHandle {
        let mut builder =
            EncodedWalSsTableBuilder::new(32, Box::new(FlatBufferSsTableInfoCodec {}));
        for row in rows.iter().cloned() {
            builder.add(row).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        assert!(encoded.index.borrow().block_meta().len() > 1);
        store.write_sst(wal_id.into(), &encoded).await.unwrap()
    }

    #[tokio::test]
    async fn preloads_metadata_and_iterates_the_whole_wal() {
        let store = test_store();
        let rows = test_rows();
        let table = write_test_wal(&store, 1, &rows).await;
        let mut iter = WalSstIterator::new(
            table,
            Arc::clone(&store),
            WalSstIteratorOptions {
                target_bytes_to_fetch: 1,
                fetch_limiter: ResourceLimiter::new(1),
                buffer_limiter: ResourceLimiter::new(usize::MAX),
            },
        );

        assert!(matches!(
            iter.next().await,
            Err(SlateDBError::IteratorNotInitialized)
        ));
        assert_eq!(iter.spawn_fetches(), 0);

        iter.load_metadata().await.unwrap();
        assert!(iter.index.is_some());
        assert!(matches!(iter.state, IteratorState::Uninitialized));
        assert!(iter.fetch_tasks.is_empty());
        assert_eq!(iter.spawn_fetches(), 1);
        assert_eq!(iter.spawn_fetches(), 0);

        iter.init().await.unwrap();
        iter.init().await.unwrap();
        let mut actual = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            actual.push(row);
        }
        assert_eq!(actual, rows);
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn forces_a_fetch_when_required_for_progress() {
        let store = test_store();
        let rows = test_rows();
        let table = write_test_wal(&store, 1, &rows).await;
        let mut iter = WalSstIterator::new(
            table,
            store,
            WalSstIteratorOptions {
                target_bytes_to_fetch: 1,
                fetch_limiter: ResourceLimiter::new(0),
                buffer_limiter: ResourceLimiter::new(0),
            },
        );

        iter.load_metadata().await.unwrap();
        assert_eq!(iter.spawn_fetches(), 0);
        iter.init().await.unwrap();

        let mut actual = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            actual.push(row);
        }
        assert_eq!(actual, rows);
    }

    #[tokio::test]
    async fn releases_fetch_capacity_before_buffer_capacity() {
        let store = test_store();
        let rows = test_rows();
        let table = write_test_wal(&store, 1, &rows).await;
        let index = store.read_index(&table).await.unwrap();
        let blocks = store.block_range_for_target_bytes(&table, &index, 0, 1);
        let buffer_usage = store.block_range_size(&table, &index, blocks);
        let fetch_limiter = ResourceLimiter::new(1);
        let buffer_limiter = ResourceLimiter::new(buffer_usage);
        let mut iter = WalSstIterator::new(
            table,
            store,
            WalSstIteratorOptions {
                target_bytes_to_fetch: 1,
                fetch_limiter: fetch_limiter.clone(),
                buffer_limiter: buffer_limiter.clone(),
            },
        );

        iter.load_metadata().await.unwrap();
        assert_eq!(iter.spawn_fetches(), 1);
        iter.init().await.unwrap();

        let fetch_probe = fetch_limiter
            .allocate(1, false)
            .expect("fetch guard should be released once the task finishes");
        assert!(buffer_limiter.allocate(1, false).is_none());
        drop(fetch_probe);

        drop(iter);
        assert!(buffer_limiter.allocate(buffer_usage, false).is_some());
    }
}
