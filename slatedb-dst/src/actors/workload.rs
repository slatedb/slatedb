use std::collections::BTreeMap;

use bytes::Bytes;
use log::info;
use rand::RngCore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Error, WriteBatch};
use tracing::instrument;

use crate::ActorCtx;

use super::PROGRESS_LOG_INTERVAL;

const WORKLOAD_KEY_COUNT: usize = 1024;
const WORKLOAD_MIN_VALUE_SIZE: usize = 32;
const WORKLOAD_MAX_VALUE_SIZE: usize = 256;

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
pub async fn workload(ctx: ActorCtx) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    let key_prefix = format!("{}/{}/", ctx.role(), ctx.instance());
    let mut oracle = BTreeMap::<Bytes, Bytes>::new();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let operation = WorkloadOperation::sample(ctx.rand().rng().next_u64());
        match operation {
            WorkloadOperation::Get => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64());
                verify_get(&ctx, step, &key, &oracle).await?;
            }
            WorkloadOperation::Scan => {
                verify_scan(&ctx, step, &key_prefix, &oracle).await?;
            }
            WorkloadOperation::Delete => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64());
                ctx.db()
                    .delete_with_options(key.clone(), &write_options)
                    .await?;
                oracle.remove(&key);
            }
            WorkloadOperation::Put => {
                let key = workload_key(&key_prefix, ctx.rand().rng().next_u64());
                let value = workload_value(&mut *ctx.rand().rng());
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
                    let key = workload_key(&key_prefix, ctx.rand().rng().next_u64());
                    if ctx.rand().rng().next_u64() & 1 == 0 {
                        let value = workload_value(&mut *ctx.rand().rng());
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

fn workload_key(prefix: &str, rand_value: u64) -> Bytes {
    let key_index = ((rand_value >> 8) as usize) % WORKLOAD_KEY_COUNT;
    Bytes::from(format!("{prefix}{key_index}"))
}

fn workload_value(rng: &mut impl RngCore) -> Bytes {
    let value_len = WORKLOAD_MIN_VALUE_SIZE
        + (rng.next_u64() as usize % (WORKLOAD_MAX_VALUE_SIZE - WORKLOAD_MIN_VALUE_SIZE + 1));
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
