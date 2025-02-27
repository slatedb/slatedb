use std::mem;
use std::ops::Range;
use crate::db_state::SsTableId;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::SlateDBError;
use std::sync::Arc;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableMemtable, WritableKVTable};

pub(crate) async fn load_wal_iter(
    table_store: Arc<TableStore>,
    wal_id: u64,
) -> Result<SstIterator<'static>, SlateDBError> {
    let sst = table_store.open_sst(&SsTableId::Wal(wal_id)).await?;
    let sst_iter_options = SstIteratorOptions {
        max_fetch_tasks: 1,
        blocks_to_fetch: 256,
        cache_blocks: true,
        eager_spawn: true,
    };
    SstIterator::new_owned(.., sst, table_store.clone(), sst_iter_options).await
}

pub(crate) async fn replay(
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    max_memtable_bytes: u64,
) -> Result<Vec<Arc<ImmutableMemtable>>, SlateDBError> {
    let mut tables = Vec::new();
    let mut curr_memtable = WritableKVTable::new();
    let mut last_wal_id= 0;

    for wal_id in wal_id_range {
        last_wal_id = wal_id;

        let mut sst_iter = load_wal_iter(Arc::clone(&table_store), wal_id).await?;
        while let Some(kv) = sst_iter.next_entry().await? {
            curr_memtable.put(kv.clone());

            // TODO: We are allowing the memtable to exceed the limit
            //  Maybe we can drop the last inserted key and insert
            //  it into the next table instead
            if curr_memtable.size() as u64 > max_memtable_bytes {
                let completed_memtable =
                    mem::replace(&mut curr_memtable, WritableKVTable::new());
                tables.push(Arc::new(ImmutableMemtable::new(completed_memtable, last_wal_id)));
            }
        }
    }

    if !curr_memtable.is_empty() {
        tables.push(Arc::new(ImmutableMemtable::new(curr_memtable, last_wal_id)));
    }

    Ok(tables)
}
