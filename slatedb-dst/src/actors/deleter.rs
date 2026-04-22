use log::info;
use rand::RngCore;
use slatedb::config::WriteOptions;
use slatedb::Error;
use tracing::instrument;

use crate::utils::workload_key_with_prefix;
use crate::ActorCtx;

use super::{WorkloadKeyspace, PROGRESS_LOG_INTERVAL};

/// Deletes deterministic keys from the shared database.
///
/// The actor runs until the shared shutdown token is cancelled.
///
/// On each step it:
/// - consumes one `u64` from the actor-local seeded RNG
/// - maps that value into the same [`WorkloadKeyspace`] used by the writer
///   actor
/// - issues a non-durable delete for that key
///
/// Deletes are intentionally best-effort workload operations. A sampled key may
/// already be absent because another deleter removed it first or no writer has
/// produced it yet. Those no-op deletes are part of the intended deterministic
/// interleaving.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn deleter(ctx: ActorCtx, keyspace: WorkloadKeyspace) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    let key_prefix = keyspace.prefix;
    let key_count = keyspace.key_count;
    if key_count == 0 {
        return Err(Error::invalid(
            "deleter actor key_count must be greater than zero".to_string(),
        ));
    }
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let rand_value = ctx.rand().rng().next_u64();
        let key = workload_key_with_prefix(&key_prefix, rand_value, key_count);
        ctx.db()
            .delete_with_options(key.as_bytes(), &write_options)
            .await?;
        step += 1;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("deleter step complete [step={}]", step);
        }
    }

    Ok(())
}
