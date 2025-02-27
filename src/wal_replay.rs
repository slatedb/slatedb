use crate::db_state::SsTableId;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::SlateDBError;
use std::sync::Arc;

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
