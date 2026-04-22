use log::info;
use rand::RngCore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::Error;
use tracing::instrument;

use crate::utils::workload_key_with_prefix;
use crate::ActorCtx;

use super::{WorkloadKeyspace, PROGRESS_LOG_INTERVAL};

/// Writes deterministic key/value updates against the shared database.
///
/// The actor runs until the shared shutdown token is cancelled.
///
/// On each step it:
/// - consumes one `u64` from the actor-local seeded RNG
/// - maps that value into the fixed-size workload keyspace described by
///   [`super::WorkloadKeyspace`] via [`crate::utils::workload_key_with_prefix`]
/// - encodes the step number and sampled random value into the stored bytes
/// - issues the write with default [`PutOptions`] and non-durable
///   [`slatedb::config::WriteOptions`]
///
/// The fixed keyspace means multiple writer instances with the same
/// [`WorkloadKeyspace`] intentionally overwrite the same logical keys. That
/// creates contention and update churn while remaining fully deterministic for
/// a given seed, actor count, and keyspace.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn writer(ctx: ActorCtx, keyspace: WorkloadKeyspace) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    let key_prefix = keyspace.prefix;
    let key_count = keyspace.key_count;
    if key_count == 0 {
        return Err(Error::invalid(
            "writer actor key_count must be greater than zero".to_string(),
        ));
    }
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let rand_value = ctx.rand().rng().next_u64();
        let key = workload_key_with_prefix(&key_prefix, rand_value, key_count);
        let value = format!("{step:04}-{rand_value:016x}").into_bytes();
        ctx.db()
            .put_with_options(key.as_bytes(), &value, &put_options, &write_options)
            .await?;
        step += 1;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("writer step complete [step={}]", step);
        }
    }

    Ok(())
}
