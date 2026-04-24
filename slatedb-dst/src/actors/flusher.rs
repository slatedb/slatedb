use std::ops::RangeInclusive;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use rand::Rng;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::Error;
use tracing::instrument;

use crate::{Actor, ActorCtx};

use super::PROGRESS_LOG_INTERVAL;

/// Forces explicit memtable flushes on the shared database.
#[derive(Debug)]
pub struct FlusherActor {
    sleep_interval_range_ms: RangeInclusive<u64>,
    step: u64,
}

impl FlusherActor {
    pub fn new(sleep_interval_range_ms: RangeInclusive<u64>) -> Result<Self, Error> {
        if sleep_interval_range_ms.is_empty() {
            return Err(Error::invalid(
                "flusher actor sleep interval range must be non-empty".to_string(),
            ));
        }

        Ok(Self {
            sleep_interval_range_ms,
            step: 0,
        })
    }
}

#[async_trait]
impl Actor for FlusherActor {
    /// Executes one explicit memtable flush and one deterministic sleep.
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();

        ctx.db()
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;

        self.step += 1;
        if self.step % PROGRESS_LOG_INTERVAL == 0 {
            info!(
                "flusher step complete [name={}, step={}]",
                ctx.name(),
                self.step
            );
        }

        let sleep_interval = Duration::from_millis(
            ctx.rand()
                .rng()
                .random_range(self.sleep_interval_range_ms.clone()),
        );

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(sleep_interval) => {}
        }

        Ok(())
    }
}
