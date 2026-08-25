use std::collections::VecDeque;
use std::sync::Arc;

use super::store::{WalFileHandle, WalTableStore};
use crate::block_iterator::DataBlockIterator;
use crate::config::SstBlockSize;
use crate::error::SlateDBError;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::types::RowEntry;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalSstIteratorOptions {
    /// Target encoded bytes per block-fetch request.
    pub(crate) target_bytes_to_fetch: usize,
}

impl Default for WalSstIteratorOptions {
    fn default() -> Self {
        Self {
            target_bytes_to_fetch: SstBlockSize::default().as_bytes(),
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
/// no range/view abstraction, filters, cache controls, descending mode, seek,
/// or speculative fetch scheduling.
pub(crate) struct WalSstIterator {
    table: WalFileHandle,
    index: Option<Arc<SsTableIndexOwned>>,
    state: IteratorState,
    next_block_idx_to_fetch: usize,
    fetched_blocks: VecDeque<Arc<Block>>,
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
            fetched_blocks: VecDeque::new(),
            table_store,
            options,
        }
    }

    /// Initializes row iteration. This is idempotent.
    pub(crate) async fn init(&mut self) -> Result<(), SlateDBError> {
        if matches!(self.state, IteratorState::Uninitialized) {
            self.load_metadata().await?;
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

    async fn load_metadata(&mut self) -> Result<(), SlateDBError> {
        if self.index.is_none() {
            self.index = Some(self.table_store.read_index(&self.table).await?);
        }
        Ok(())
    }

    async fn advance_block(&mut self) -> Result<(), SlateDBError> {
        loop {
            if let Some(block) = self.fetched_blocks.pop_front() {
                self.state = IteratorState::Active(DataBlockIterator::new(
                    block,
                    self.table.format_version,
                    IterationOrder::Ascending,
                )?);
                return Ok(());
            }

            let index = self.index.as_ref().expect("metadata must be loaded");
            let num_blocks = index.borrow().block_meta().len();
            if self.next_block_idx_to_fetch == num_blocks {
                self.state = IteratorState::Finished;
                return Ok(());
            }

            let blocks = self.table_store.block_range_for_target_bytes(
                &self.table,
                index,
                self.next_block_idx_to_fetch,
                self.options.target_bytes_to_fetch,
            );
            self.next_block_idx_to_fetch = blocks.end;
            self.fetched_blocks = self
                .table_store
                .read_blocks_using_index(&self.table, Arc::clone(index), blocks)
                .await?;
        }
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

    #[tokio::test]
    async fn iterates_the_whole_wal_sequentially() {
        let store = test_store();
        let rows: Vec<_> = (1..=6)
            .map(|seq| {
                RowEntry::new_value(
                    format!("key-{seq}").as_bytes(),
                    format!("value-{seq}").as_bytes(),
                    seq,
                )
            })
            .collect();
        let mut builder =
            EncodedWalSsTableBuilder::new(32, Box::new(FlatBufferSsTableInfoCodec {}));
        for row in rows.iter().cloned() {
            builder.add(row).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let table = store.write_sst(1.into(), &encoded).await.unwrap();
        let mut iter = WalSstIterator::new(
            table,
            store,
            WalSstIteratorOptions {
                target_bytes_to_fetch: 1,
            },
        );

        assert!(matches!(
            iter.next().await,
            Err(SlateDBError::IteratorNotInitialized)
        ));
        iter.init().await.unwrap();
        iter.init().await.unwrap();

        let mut actual = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            actual.push(row);
        }
        assert_eq!(actual, rows);
        assert!(iter.next().await.unwrap().is_none());
    }
}
