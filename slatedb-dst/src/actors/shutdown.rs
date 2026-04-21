use std::sync::Arc;

use chrono::{DateTime, Utc};
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

/// Cancels the harness once the shared mock clock reaches `shutdown_at`.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn shutdown(ctx: ActorCtx, shutdown_at: Arc<DateTime<Utc>>) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let system_clock = ctx.system_clock();
    let shutdown_at = shutdown_at.as_ref();

    while !shutdown_token.is_cancelled() {
        let now = system_clock.now();
        if now >= shutdown_at.to_owned() {
            shutdown_token.cancel();
            return Ok(());
        }

        let remaining = shutdown_at
            .to_owned()
            .signed_duration_since(now)
            .to_std()
            .expect("shutdown deadline must not be earlier than the current mock clock");

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return Ok(()),
            _ = system_clock.sleep(remaining) => {}
        }
    }

    Ok(())
}
