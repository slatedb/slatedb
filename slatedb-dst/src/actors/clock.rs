use std::time::Duration;

use rand::RngCore;
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

/// Advances the shared mock clock forever using actor-local deterministic
/// randomness.
///
/// The actor runs until the shared shutdown token is cancelled.
///
/// On each loop iteration it:
/// - consumes one `u64` from the actor-local seeded RNG
/// - derives a delay in the inclusive range `1..=5` milliseconds
/// - advances the shared harness clock by that amount
///
/// Because the harness clock is shared by all actors and the wrapped object
/// stores, this actor provides deterministic passage of logical time for
/// features such as polling loops, age-based behavior, and fault-injection
/// latency.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn clock(ctx: ActorCtx) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();

    while !shutdown_token.is_cancelled() {
        let rand_value = ctx.rand().rng().next_u64();
        ctx.advance_time(Duration::from_millis(1 + (rand_value % 5)))
            .await;
    }

    Ok(())
}
