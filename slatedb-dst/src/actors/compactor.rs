use std::sync::Arc;
use std::time::Duration;

use log::info;
use rand::RngCore;
use slatedb::compactor::stats::COMPACTOR_EPOCH;
use slatedb::compactor::Compactor;
use slatedb::config::CompactorOptions;
use slatedb::{CloseReason, CompactorBuilder, Error, ErrorKind};
use slatedb_common::metrics::{lookup_metric, DefaultMetricsRecorder};
use tracing::instrument;

use crate::ActorCtx;

/// Configuration for the standalone compactor DST actor.
#[derive(Clone, Debug)]
pub struct CompactorActorOptions {
    /// How long the actor waits before spawning a replacement compactor and
    /// asserting that the previous one was fenced.
    pub restart_interval: Duration,
    /// The options to use when constructing each standalone compactor.
    pub compactor_options: CompactorOptions,
}

#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn compactor(ctx: ActorCtx, actor_options: CompactorActorOptions) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let system_clock = ctx.system_clock();
    let mut current: Option<Compactor> = None;

    while !shutdown_token.is_cancelled() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let old = current;
        current = Some(
            compactor_builder(ctx.clone(), actor_options.clone())
                .with_metrics_recorder(recorder.clone())
                .build(),
        );

        let current_cloned = current.clone().expect("compactor should always exist");
        let current_spawned = tokio::spawn(async move { current_cloned.run().await });

        // Wait for the current compactor to start and publish its epoch before fencing it.
        while lookup_metric(recorder.as_ref(), COMPACTOR_EPOCH).is_none_or(|epoch| epoch == 0) {
            tokio::task::yield_now().await;
        }

        // If there was a previous compactor, assert that it was fenced and log the replacement.
        if let Some(old) = old {
            info!("spawned replacement compactor");
            match old.stop().await {
                Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => (),
                r => panic!("compactor was not fenced as expected [result={:?}]", r),
            }
        }

        tokio::select! {
            biased;
            result = current_spawned => panic!("compactor exited unexpectedly: {result:?}"),
            _ = shutdown_token.cancelled() => break,
            _ = system_clock.sleep(actor_options.restart_interval) => {}
        }
    }

    if let Some(current) = current {
        current.stop().await?;
    }

    Ok(())
}

fn compactor_builder(
    ctx: ActorCtx,
    actor_options: CompactorActorOptions,
) -> CompactorBuilder<object_store::path::Path> {
    CompactorBuilder::new(ctx.path().clone(), ctx.main_object_store())
        .with_options(actor_options.compactor_options.clone())
        .with_system_clock(ctx.system_clock())
        .with_seed(ctx.rand().rng().next_u64())
}
