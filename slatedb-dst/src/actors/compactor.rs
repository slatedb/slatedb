use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use rand::RngCore;
use slatedb::compactor::stats::COMPACTOR_EPOCH;
use slatedb::config::CompactorOptions;
use slatedb::{CloseReason, CompactorBuilder, Error, ErrorKind};
use slatedb_common::metrics::{lookup_metric, DefaultMetricsRecorder};
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::{Actor, ActorCtx};

/// Configuration for the standalone compactor DST actor.
#[derive(Clone, Debug)]
pub struct CompactorActorOptions {
    /// How long the actor waits before spawning a replacement compactor and
    /// asserting that the previous one was fenced.
    ///
    /// If the restart interval is shorter than the time it takes to complete
    /// a compaction, the compactor might never make progress. In such a case,
    /// the test will likely deadlock due to backpressure.
    pub restart_interval: Duration,
    /// The options to use when constructing each standalone compactor.
    pub compactor_options: CompactorOptions,
}

pub struct CompactorActor {
    actor_options: CompactorActorOptions,
    current_task: Option<JoinHandle<Result<(), Error>>>,
}

impl CompactorActor {
    pub fn new(actor_options: CompactorActorOptions) -> Result<Self, Error> {
        info!("compactor actor created [options={:?}]", actor_options);
        Ok(Self {
            actor_options,
            current_task: None,
        })
    }
}

/// Spawns a standalone compactor in a loop, fencing any existing ones.
///
/// - Create new compactor
/// - Wait for new compactor to claim an epoch (i.e. fence old)
/// - Assert old compactor is fenced
/// - Wait for restart interval before returning.
///
/// If the new compactor fails to start with `Unavailable`, it is retried indefinitely
/// until it succeeds (disregarding the shutdown token). This is because the compactor
/// can fail due to a timeout (outside of RetryingObjectStore) on startup. With toxics
/// enabled, this can cause the compactor to fail to start without retrying. In such a
/// case, we need to keep retrying or we might end up with no compactor at all, which
/// can cause deadlocks in tests (due to backpressure).
#[async_trait]
impl Actor for CompactorActor {
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        loop {
            match self.run_inner(ctx).await {
                Err(err) if matches!(err.kind(), ErrorKind::Unavailable) => {}
                result => return result,
            }
        }
    }
}

impl CompactorActor {
    async fn run_inner(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let next = self
            .compactor_builder(ctx)
            .with_metrics_recorder(recorder.clone())
            .build();
        let next_cloned = next.clone();
        let mut new_task = tokio::spawn(async move { next_cloned.run().await });

        // Wait for the new compactor to start and claim a new epoch.
        while lookup_metric(recorder.as_ref(), COMPACTOR_EPOCH).is_none_or(|epoch| epoch == 0) {
            tokio::select! {
                result = &mut new_task => match result {
                    Ok(Ok(())) => panic!("compactor suceeded unexpectedly during startup"),
                    Ok(Err(err)) => return Err(err),
                    Err(err) => panic!("compactor task panicked during startup: {err:?}"),
                },
                _ = tokio::task::yield_now() => {}
            }
        }

        // Swap in the new compactor and take the old task to verify it was fenced.
        let _ = ctx.swap_compactor(next.clone());
        let old_task = self.current_task.take();

        // Verify the previous compactor is fenced.
        if let Some(old_task) = old_task {
            info!("spawned replacement compactor [name={}]", ctx.name());
            match old_task.await {
                Ok(Err(err)) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => (),
                result => panic!("compactor was not fenced as expected [result={result:?}]"),
            }
        }

        // Install the new compactor task so we can verify it's fenced in the next iteration.
        self.current_task = Some(new_task);
        let current_task = self
            .current_task
            .as_mut()
            .expect("compactor task must be installed");

        // Wait for the restart interval before allowing the next generation to start.
        tokio::select! {
            biased;
            result = current_task => panic!("compactor exited unexpectedly: {result:?}"),
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(self.actor_options.restart_interval) => {}
        }

        Ok(())
    }

    fn compactor_builder(&self, ctx: &ActorCtx) -> CompactorBuilder<object_store::path::Path> {
        CompactorBuilder::new(ctx.path().clone(), ctx.main_object_store())
            .with_options(self.actor_options.compactor_options.clone())
            .with_system_clock(ctx.system_clock())
            .with_seed(ctx.rand().rng().next_u64())
    }
}
