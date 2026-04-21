use log::info;
use rand::RngCore;
use slatedb::Error;
use tracing::instrument;

use crate::utils::{nondurable_write_options, workload_key};
use crate::ActorCtx;

use super::{PROGRESS_LOG_INTERVAL, WORKLOAD_STEPS};

/// Deletes deterministic keys from the shared database.
///
/// The actor clones the current shared database handle once at startup, then
/// performs exactly [`super::WORKLOAD_STEPS`] delete attempts before
/// returning.
///
/// On each step it:
/// - consumes one `u64` from the actor-local seeded RNG
/// - maps that value into the same workload keyspace used by the writer actor
/// - issues a non-durable delete for that key
///
/// Deletes are intentionally best-effort workload operations. A sampled key may
/// already be absent because another deleter removed it first or no writer has
/// produced it yet. Those no-op deletes are part of the intended deterministic
/// interleaving.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn deleter(ctx: ActorCtx) -> Result<(), Error> {
    let db = ctx.db();
    let write_options = nondurable_write_options();

    for step in 0..WORKLOAD_STEPS {
        let rand_value = ctx.rand().rng().next_u64();
        let key = workload_key(rand_value);
        db.delete_with_options(key.as_bytes(), &write_options)
            .await?;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("deleter step complete [step={}]", step);
        }
    }

    Ok(())
}
