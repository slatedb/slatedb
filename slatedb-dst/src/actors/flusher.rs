use log::info;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

use super::{PROGRESS_LOG_INTERVAL, WORKLOAD_STEPS};

/// Forces deterministic memtable flushes on the shared database for a fixed
/// number of workload steps.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn flusher(ctx: ActorCtx) -> Result<(), Error> {
    let db = ctx.db();

    for step in 0..WORKLOAD_STEPS {
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await?;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("flusher step complete [step={}]", step);
        }
    }

    Ok(())
}
