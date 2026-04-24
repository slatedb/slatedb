use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::info;
use slatedb::Error;
use tracing::instrument;

use crate::{Actor, ActorCtx};

/// Cancels the harness once the shared mock clock reaches the configured
/// shutdown deadline.
#[derive(Debug)]
pub struct ShutdownActor {
    shutdown_at: DateTime<Utc>,
}

impl ShutdownActor {
    pub fn new(shutdown_at_ms: i64) -> Result<Self, Error> {
        let shutdown_at = DateTime::from_timestamp_millis(shutdown_at_ms)
            .ok_or_else(|| Error::invalid("shutdown timestamp must be valid".to_string()))?;
        Ok(Self { shutdown_at })
    }
}

#[async_trait]
impl Actor for ShutdownActor {
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        let now = system_clock.now();

        if now >= self.shutdown_at {
            shutdown_token.cancel();
            return Ok(());
        }

        let remaining = self
            .shutdown_at
            .signed_duration_since(now)
            .to_std()
            .expect("shutdown deadline must not be earlier than the current mock clock");

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(remaining) => {
                if system_clock.now() >= self.shutdown_at {
                    info!("shutdown deadline reached, cancelling shutdown token");
                    shutdown_token.cancel();
                }
            }
        }

        Ok(())
    }
}
