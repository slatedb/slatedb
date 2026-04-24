use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use rand::RngCore;
use slatedb::compactor::stats::COMPACTOR_EPOCH;
use slatedb::config::CompactorOptions;
use slatedb::{CloseReason, CompactorBuilder, Error, ErrorKind};
use slatedb_common::metrics::{lookup_metric, DefaultMetricsRecorder};
use tracing::instrument;

use crate::{Actor, ActorCtx};

/// Configuration for the standalone compactor DST actor.
#[derive(Clone, Debug)]
pub struct CompactorActorOptions {
    /// How long the actor waits before spawning a replacement compactor and
    /// asserting that the previous one was fenced.
    pub restart_interval: Duration,
    /// The options to use when constructing each standalone compactor.
    pub compactor_options: CompactorOptions,
}

pub struct CompactorActor {
    actor_options: CompactorActorOptions,
}

impl CompactorActor {
    pub fn new(actor_options: CompactorActorOptions) -> Result<Self, Error> {
        if actor_options.restart_interval.is_zero() {
            return Err(Error::invalid(
                "compactor actor restart_interval must be greater than zero".to_string(),
            ));
        }

        Ok(Self { actor_options })
    }
}

#[async_trait]
impl Actor for CompactorActor {
    /// Spawns the next compactor generation, fences the previous generation if
    /// present, then waits for one restart interval.
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let next = compactor_builder(ctx, &self.actor_options)
            .with_metrics_recorder(recorder.clone())
            .build();
        let next_cloned = next.clone();
        let mut new_task = tokio::spawn(async move { next_cloned.run().await });

        // Wait for the new compactor to start and claim a new epoch.
        while lookup_metric(recorder.as_ref(), COMPACTOR_EPOCH).is_none_or(|epoch| epoch == 0) {
            tokio::select! {
                result = &mut new_task => panic!("compactor exited unexpectedly: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
        }

        // Verify the previous compactor is fenced.
        if let Some(old) = ctx.swap_compactor(next.clone()) {
            info!("spawned replacement compactor [name={}]", ctx.name());
            match old.stop().await {
                Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => (),
                result => panic!("compactor was not fenced as expected [result={result:?}]"),
            }
        }

        // Wait for the restart interval before allowing the next generation to start.
        tokio::select! {
            biased;
            result = &mut new_task => panic!("compactor exited unexpectedly: {result:?}"),
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(self.actor_options.restart_interval) => {}
        }

        Ok(())
    }
}

fn compactor_builder(
    ctx: &ActorCtx,
    actor_options: &CompactorActorOptions,
) -> CompactorBuilder<object_store::path::Path> {
    CompactorBuilder::new(ctx.path().clone(), ctx.main_object_store())
        .with_options(actor_options.compactor_options.clone())
        .with_system_clock(ctx.system_clock())
        .with_seed(ctx.rand().rng().next_u64())
}
