//! The `multi_get` read path: a loop of `get` over one state view, see RFC 0035.

use std::collections::HashMap;

use parking_lot::RwLock;
use tracing::Instrument;

use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::bytes_range::BytesRange;
use crate::config::MultiGetOptions;
use crate::error::SlateDBError;
use crate::iter::IterationOrder;
use crate::reader::{DbStateReader, ReadTrace, Reader};
use crate::types::KeyValue;

impl Reader {
    /// Reads each key as `get` does, with one state view and one sequence bound for the batch.
    pub(crate) async fn multi_get_with_options<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&RwLock<WriteBatch>>,
        max_seq: Option<u64>,
    ) -> Result<Vec<Option<KeyValue>>, SlateDBError> {
        let read_trace = ReadTrace::new_multi_get(options.tracing_options.clone(), keys.len());
        let read = self.multi_get_with_options_inner(
            keys,
            options,
            db_state,
            write_batch,
            max_seq,
            read_trace.clone(),
        );
        read.instrument(read_trace.read_span()).await
    }

    async fn multi_get_with_options_inner<K: AsRef<[u8]> + Sync>(
        &self,
        keys: &[K],
        options: &MultiGetOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&RwLock<WriteBatch>>,
        max_seq: Option<u64>,
        read_trace: ReadTrace,
    ) -> Result<Vec<Option<KeyValue>>, SlateDBError> {
        self.db_stats.multi_get_requests.increment(1);
        self.db_stats.multi_get_keys.increment(keys.len() as u64);
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let read_options = options.read_options();

        let mut results: Vec<Option<KeyValue>> = Vec::with_capacity(keys.len());
        // A duplicate key copies the result of its first slot.
        let mut first_slot: HashMap<&[u8], usize> = HashMap::new();
        for key in keys {
            let key = key.as_ref();
            if let Some(&slot) = first_slot.get(key) {
                results.push(results[slot].clone());
                continue;
            }
            first_slot.insert(key, results.len());
            // The write batch lookup holds the read guard for one key, as `get` does.
            let write_batch_iter = write_batch.map(|write_batch| {
                let guard = write_batch.read();
                WriteBatchIterator::new(
                    &guard,
                    BytesRange::from_slice(key..=key),
                    IterationOrder::Ascending,
                    u64::MAX,
                    None,
                    None,
                )
            });
            let result = self
                .get_key_value_with_options_inner(
                    key,
                    &read_options,
                    db_state,
                    write_batch_iter,
                    max_seq,
                    read_trace.clone(),
                )
                .await?;
            results.push(result);
        }
        Ok(results)
    }
}
