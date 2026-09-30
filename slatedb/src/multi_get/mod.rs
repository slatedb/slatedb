//! The `multi_get` read path: the layer walk of RFC 0035.
//!
//! Walk 0 answers keys from the write batch and the memtables. Then each layer
//! of the LSM tree, an L0 SST or a sorted run, is walked for the keys that are
//! still open, with `lookahead` walks in flight. One SST serves all of its keys
//! with one filter probe, one index and one read per distinct block.

mod keys;
mod layer;
mod sst;

use futures::{future, stream, StreamExt};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;
use tracing::Instrument;

use self::keys::Batch;
use self::layer::Layers;
use self::sst::ReadContext;
use crate::batch::WriteBatch;
use crate::config::MultiGetOptions;
use crate::error::SlateDBError;
use crate::reader::{DbStateReader, ReadTrace, Reader};
use crate::types::KeyValue;

impl Reader {
    /// Reads the keys with one state view and one sequence bound for the batch.
    pub(crate) async fn multi_get_with_options<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&RwLock<WriteBatch>>,
        max_seq: Option<u64>,
    ) -> Result<Vec<Option<KeyValue>>, SlateDBError> {
        let read_trace = ReadTrace::new_multi_get(options.tracing_options.clone(), keys.len());
        let read = self.walk_layers(keys, options, db_state, write_batch, max_seq, &read_trace);
        read.instrument(read_trace.read_span()).await
    }

    async fn walk_layers<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&RwLock<WriteBatch>>,
        max_seq: Option<u64>,
        read_trace: &ReadTrace,
    ) -> Result<Vec<Option<KeyValue>>, SlateDBError> {
        self.db_stats.multi_get_requests.increment(1);
        self.db_stats.multi_get_keys.increment(keys.len() as u64);
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);

        let mut batch = Batch::new(keys, max_seq);
        batch.read_write_batch(write_batch).await;
        batch.read_memtables(db_state, read_trace);
        let layers = Layers::new(db_state.core(), &mut batch);

        let permits = Semaphore::new(options.max_fetch_tasks.max(1));
        let ctx = ReadContext {
            table_store: &self.table_store,
            db_stats: &self.db_stats,
            read_trace,
            options,
            permits: &permits,
        };
        // The stream and the loop take turns on the batch, so the lock never waits.
        let batch = Mutex::new(batch);
        let mut walked: u64 = 0;
        {
            // A walk starts when the window has room, after the walks before it were applied.
            let mut walks = stream::iter(layers)
                .filter_map(|layer| {
                    let keys = batch.lock().open_keys(layer.segment());
                    future::ready((!keys.is_empty()).then_some((layer, keys)))
                })
                .map(|(layer, keys)| {
                    walked += 1;
                    self.db_stats.multi_get_layers.increment(1);
                    layer.walk(keys, &ctx)
                })
                .buffered(options.lookahead.max(1));
            while let Some(entries) = walks.next().await {
                let mut batch = batch.lock();
                batch.apply(entries?);
                if batch.is_done() {
                    break;
                }
            }
        }
        read_trace.read_span().record("layers", walked);

        batch
            .into_inner()
            .resolve(self.read_merge_operator.as_ref())
            .await
    }
}
