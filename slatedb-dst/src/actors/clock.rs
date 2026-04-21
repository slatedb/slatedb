use std::time::Duration;

use rand::RngCore;
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

/// Advances the shared mock clock forever using actor-local deterministic
/// randomness.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn clock(ctx: ActorCtx) -> Result<(), Error> {
    loop {
        let rand_value = ctx.rand().rng().next_u64();
        ctx.advance_time(Duration::from_millis(1 + (rand_value % 5)))
            .await;
    }
}
