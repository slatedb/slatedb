use std::collections::BTreeMap;

use bytes::Bytes;
use log::info;
use rand::RngCore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Error, WriteBatch};
use tracing::instrument;

use crate::ActorCtx;

use super::PROGRESS_LOG_INTERVAL;

/// Configuration for the mixed DST workload actor.
#[derive(Clone, Debug)]
pub struct WorkloadActorOptions {
    /// Number of logical keys each actor maps random samples onto under its
    /// `{role}/{instance}/` prefix.
    pub key_count: usize,
    /// Minimum random value size generated for `put` and `write_batch`.
    pub min_value_size: usize,
    /// Maximum random value size generated for `put` and `write_batch`.
    pub max_value_size: usize,
}

impl Default for WorkloadActorOptions {
    fn default() -> Self {
        Self {
            key_count: 1024,
            min_value_size: 32,
            max_value_size: 256,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum WorkloadOperation {
    Get,
    Scan,
    Delete,
    Put,
    WriteBatch,
}

impl WorkloadOperation {
    fn sample(rand_value: u64) -> Self {
        match rand_value % 5 {
            0 => Self::Get,
            1 => Self::Scan,
            2 => Self::Delete,
            3 => Self::Put,
            _ => Self::WriteBatch,
        }
    }
}

/// Runs a mixed deterministic workload against the shared database while
/// verifying reads against an actor-local oracle.
///
/// The actor runs until the shared shutdown token is cancelled.
///
/// Each actor owns a disjoint key prefix derived from its role and instance id:
/// `{role}/{instance}/`. The actor maps random samples onto a fixed logical key
/// range under that prefix, which keeps its local oracle authoritative for all
/// keys it reads back.
///
/// On each step it:
/// - samples one top-level operation uniformly from `get`, `scan`, `delete`,
///   `put`, and `write_batch`
/// - issues the corresponding SlateDB operation with non-durable
///   [`slatedb::config::WriteOptions`] for writes
/// - verifies `get` and `scan` results with `assert!`/`assert_eq!` against the
///   actor-local oracle
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn workload(ctx: ActorCtx, options: WorkloadActorOptions) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    if options.key_count == 0 {
        return Err(Error::invalid(
            "workload actor key_count must be greater than zero".to_string(),
        ));
    }
    if options.min_value_size > options.max_value_size {
        return Err(Error::invalid(
            "workload actor min_value_size must be less than or equal to max_value_size"
                .to_string(),
        ));
    }

    let key_count = options.key_count;
    let min_value_size = options.min_value_size;
    let max_value_size = options.max_value_size;
    let key_prefix = format!("{}/{}/", ctx.role(), ctx.instance());
    let mut oracle = BTreeMap::<Bytes, Bytes>::new();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let operation = WorkloadOperation::sample(ctx.rand().rng().next_u64());
        match operation {
            WorkloadOperation::Get => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64(), key_count);
                verify_get(&ctx, step, &key, &oracle).await?;
            }
            WorkloadOperation::Scan => {
                verify_scan(&ctx, step, &key_prefix, &oracle).await?;
            }
            WorkloadOperation::Delete => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64(), key_count);
                ctx.db()
                    .delete_with_options(key.clone(), &write_options)
                    .await?;
                oracle.remove(&key);
            }
            WorkloadOperation::Put => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64(), key_count);
                let value = workload_value(&mut *ctx.rand().rng(), min_value_size, max_value_size);
                ctx.db()
                    .put_with_options(key.clone(), value.clone(), &put_options, &write_options)
                    .await?;
                oracle.insert(key, value);
            }
            WorkloadOperation::WriteBatch => {
                let mut batch = WriteBatch::new();
                let mut next_oracle = oracle.clone();
                let batch_len = 2 + (ctx.rand().rng().next_u64() % 3) as usize;

                for _ in 0..batch_len {
                    let key = workload_key(&key_prefix, ctx.rand().rng().next_u64(), key_count);
                    if ctx.rand().rng().next_u64() & 1 == 0 {
                        let value =
                            workload_value(&mut *ctx.rand().rng(), min_value_size, max_value_size);
                        batch.put_bytes(key.clone(), value.clone());
                        next_oracle.insert(key, value);
                    } else {
                        batch.delete(key.clone());
                        next_oracle.remove(&key);
                    }
                }

                ctx.db().write_with_options(batch, &write_options).await?;
                oracle = next_oracle;
            }
        }
        step += 1;

        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!(
                "workload step complete [step={}, oracle_keys={}]",
                step,
                oracle.len()
            );
        }
    }

    Ok(())
}

fn workload_key(prefix: &str, rand_value: u64, key_count: usize) -> Bytes {
    let key_index = ((rand_value >> 8) as usize) % key_count;
    Bytes::from(format!("{prefix}{key_index}"))
}

fn workload_value(rng: &mut impl RngCore, min_value_size: usize, max_value_size: usize) -> Bytes {
    let value_len =
        min_value_size + (rng.next_u64() as usize % (max_value_size - min_value_size + 1));
    let mut value = vec![0; value_len];
    rng.fill_bytes(&mut value);
    Bytes::from(value)
}

async fn verify_get(
    ctx: &ActorCtx,
    step: u64,
    key: &Bytes,
    oracle: &BTreeMap<Bytes, Bytes>,
) -> Result<(), Error> {
    let actual = ctx.db().get(key.clone()).await?;
    let expected = oracle.get(key).cloned();
    assert_eq!(
        actual,
        expected,
        "workload get mismatch [role={}, instance={}, step={}, key={}]",
        ctx.role(),
        ctx.instance(),
        step,
        String::from_utf8_lossy(key.as_ref()).into_owned(),
    );
    Ok(())
}

async fn verify_scan(
    ctx: &ActorCtx,
    step: u64,
    key_prefix: &str,
    oracle: &BTreeMap<Bytes, Bytes>,
) -> Result<(), Error> {
    let mut iter = ctx.db().scan_prefix(key_prefix.as_bytes()).await?;
    let mut actual = BTreeMap::new();

    while let Some(key_value) = iter.next().await? {
        assert!(
            actual
                .insert(key_value.key.clone(), key_value.value.clone())
                .is_none(),
            "workload scan returned duplicate key [role={}, instance={}, step={}, key_prefix={}, key={}]",
            ctx.role(),
            ctx.instance(),
            step,
            key_prefix,
            String::from_utf8_lossy(key_value.key.as_ref()).into_owned(),
        );
    }

    assert_eq!(
        actual,
        *oracle,
        "workload scan mismatch [role={}, instance={}, step={}, key_prefix={}]",
        ctx.role(),
        ctx.instance(),
        step,
        key_prefix,
    );
    Ok(())
}
