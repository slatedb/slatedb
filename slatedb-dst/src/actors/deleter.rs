use log::info;
use rand::RngCore;
use slatedb::Error;
use tracing::instrument;

use crate::utils::{nondurable_write_options, workload_key};
use crate::ActorCtx;

use super::{PROGRESS_LOG_INTERVAL, WORKLOAD_STEPS};

/// Deletes deterministic keys from the shared database for a fixed number of
/// workload steps.
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
