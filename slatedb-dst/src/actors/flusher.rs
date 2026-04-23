use log::info;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

use super::PROGRESS_LOG_INTERVAL;

/// Forces explicit memtable flushes on the shared database.
///
/// The actor runs until the shared shutdown token is cancelled.
///
/// Each step calls [`slatedb::Db::flush_with_options`] with
/// [`FlushType::MemTable`], which makes the actor useful in scenarios that need
/// real SST creation and downstream compaction work rather than relying solely
/// on background intervals.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn flusher(ctx: ActorCtx) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        ctx.db()
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
        step += 1;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("flusher step complete [step={}]", step);
        }
    }

    Ok(())
}
