use std::time::Duration;

use log::info;
use rand::RngCore;
use slatedb::config::CompactorOptions;
use slatedb::{CloseReason, CompactorBuilder, Error, ErrorKind};
use tokio::task::JoinHandle;
use tracing::{instrument, Instrument, Span};

use crate::ActorCtx;

struct CompactorTask(Option<JoinHandle<Result<(), Error>>>);

impl CompactorTask {
    async fn join(mut self) -> Result<Result<(), Error>, tokio::task::JoinError> {
        self.0
            .take()
            .expect("compactor task missing join handle")
            .await
    }
}

impl Drop for CompactorTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// Configuration for the standalone compactor DST actor.
#[derive(Clone, Debug)]
pub struct CompactorActorOptions {
    /// How long the actor waits before spawning a replacement compactor and
    /// asserting that the previous one was fenced.
    pub restart_interval: Duration,
    /// The options to use when constructing each standalone compactor.
    pub compactor_options: CompactorOptions,
}

/// Exercises standalone compactor fencing with deterministic restarts.
///
/// This actor repeatedly starts a standalone SlateDB compactor against the
/// shared harness database path. After each configured restart interval, it
/// starts a replacement compactor and verifies that the previous one exits with
/// a fencing error.
///
/// Register the actor with [`crate::Harness::actor_with_state`] and pass
/// [`CompactorActorOptions`] to control the restart cadence and standalone
/// compactor configuration. Scenarios that use this actor should disable the
/// main database client's embedded compactor so the actor is the only
/// compactor intentionally competing for the compactor epoch. Because the
/// cadence is driven by the shared mock clock, the harness must own logical
/// time for the full lifetime of the scenario.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn compactor(ctx: ActorCtx, actor_options: CompactorActorOptions) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let system_clock = ctx.system_clock();
    let mut current = spawn_compactor(&ctx, &actor_options.compactor_options);

    while !shutdown_token.is_cancelled() {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => break,
            _ = system_clock.sleep(actor_options.restart_interval) => {}
        }

        let old = current;
        current = spawn_compactor(&ctx, &actor_options.compactor_options);
        info!("spawned replacement compactor");

        match old.join().await {
            Ok(Err(err)) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => {
                // The old compactor was fenced as expected
                continue;
            }
            r => panic!(
                "expected previous compactor to be fenced after spawning replacement, but got: {:?}",
                r
            ),
        };
    }

    Ok(())
}

fn spawn_compactor(ctx: &ActorCtx, compactor_options: &CompactorOptions) -> CompactorTask {
    let compactor_seed = ctx.rand().rng().next_u64();
    let compactor = CompactorBuilder::new(ctx.path().clone(), ctx.main_object_store())
        .with_options(compactor_options.clone())
        .with_system_clock(ctx.system_clock())
        .with_seed(compactor_seed)
        .build();
    CompactorTask(Some(tokio::spawn(
        async move { compactor.run().await }.instrument(Span::current()),
    )))
}
