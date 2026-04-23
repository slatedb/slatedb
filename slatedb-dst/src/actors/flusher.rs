use std::ops::RangeInclusive;
use std::time::Duration;

use log::info;
use rand::Rng;
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
/// on background intervals. Register it with [`crate::Harness::actor_with_state`]
/// and pass a `RangeInclusive<u64>` containing the inclusive minimum and
/// maximum sleep bounds in milliseconds. After each flush, it samples a
/// deterministic sleep duration from that range on the shared system clock so
/// it applies steady flush pressure without tight-spinning.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn flusher(
    ctx: ActorCtx,
    sleep_interval_range_ms: RangeInclusive<u64>,
) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let system_clock = ctx.system_clock();
    if sleep_interval_range_ms.is_empty() {
        return Err(Error::invalid(
            "flusher actor sleep interval range must be non-empty".to_string(),
        ));
    }
    let min_sleep_ms = *sleep_interval_range_ms.start();
    let max_sleep_ms = *sleep_interval_range_ms.end();
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

        let sleep_interval =
            Duration::from_millis(ctx.rand().rng().random_range(min_sleep_ms..=max_sleep_ms));

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => break,
            _ = system_clock.sleep(sleep_interval) => {}
        }
    }

    Ok(())
}
