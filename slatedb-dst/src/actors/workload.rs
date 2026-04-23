use std::collections::BTreeMap;

use bytes::Bytes;
use log::info;
use rand::RngCore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Error, WriteBatch};
use tracing::instrument;

use crate::utils::workload_key_with_prefix;
use crate::ActorCtx;

use super::PROGRESS_LOG_INTERVAL;

/// Shared keyspace configuration for workload actors that map random samples
/// onto a fixed logical key range.
#[derive(Clone, Debug)]
pub struct WorkloadKeyspace {
    /// Prefix applied to each generated workload actor namespace.
    pub prefix: String,
    /// Number of logical keys to map random samples onto for each actor.
    pub key_count: usize,
}

impl Default for WorkloadKeyspace {
    fn default() -> Self {
        Self {
            prefix: "key".to_string(),
            key_count: 32,
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
/// Each actor owns a disjoint prefix namespace derived from the configured
/// [`WorkloadKeyspace`] plus its role and instance id. That keeps its local
/// oracle authoritative for all keys it reads back.
///
/// On each step it:
/// - samples one top-level operation uniformly from `get`, `scan`, `delete`,
///   `put`, and `write_batch`
/// - issues the corresponding SlateDB operation with non-durable
///   [`slatedb::config::WriteOptions`] for writes
/// - verifies `get` and `scan` results with `assert!`/`assert_eq!` against the
///   actor-local oracle
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn workload(ctx: ActorCtx, keyspace: WorkloadKeyspace) -> Result<(), Error> {
    let shutdown_token = ctx.shutdown_token();
    let put_options = PutOptions::default();
    let write_options = WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    };
    let key_count = keyspace.key_count;
    if key_count == 0 {
        return Err(Error::invalid(
            "workload actor key_count must be greater than zero".to_string(),
        ));
    }

    let actor_prefix = workload_actor_prefix(&keyspace.prefix, ctx.role(), ctx.instance());
    let mut oracle = BTreeMap::<Bytes, Bytes>::new();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let operation = WorkloadOperation::sample(ctx.rand().rng().next_u64());
        match operation {
            WorkloadOperation::Get => {
                let key = workload_key(&actor_prefix, ctx.rand().rng().next_u64(), key_count);
                verify_get(&ctx, step, &key, &oracle).await?;
            }
            WorkloadOperation::Scan => {
                verify_scan(&ctx, step, &actor_prefix, &oracle).await?;
            }
            WorkloadOperation::Delete => {
                let key = workload_key(&actor_prefix, ctx.rand().rng().next_u64(), key_count);
                ctx.db()
                    .delete_with_options(key.clone(), &write_options)
                    .await?;
                oracle.remove(&key);
            }
            WorkloadOperation::Put => {
                let key = workload_key(&actor_prefix, ctx.rand().rng().next_u64(), key_count);
                let value = workload_value(
                    ctx.role(),
                    ctx.instance(),
                    step,
                    None,
                    ctx.rand().rng().next_u64(),
                );
                ctx.db()
                    .put_with_options(key.clone(), value.clone(), &put_options, &write_options)
                    .await?;
                oracle.insert(key, value);
            }
            WorkloadOperation::WriteBatch => {
                let mut batch = WriteBatch::new();
                let mut next_oracle = oracle.clone();
                let batch_len = 2 + (ctx.rand().rng().next_u64() % 3) as usize;

                for entry_idx in 0..batch_len {
                    let key = workload_key(&actor_prefix, ctx.rand().rng().next_u64(), key_count);
                    if ctx.rand().rng().next_u64() & 1 == 0 {
                        let value = workload_value(
                            ctx.role(),
                            ctx.instance(),
                            step,
                            Some(entry_idx),
                            ctx.rand().rng().next_u64(),
                        );
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

fn workload_actor_prefix(prefix: &str, role: &str, instance: usize) -> String {
    format!("{prefix}/{role}/{instance}/")
}

fn workload_key(prefix: &str, rand_value: u64, key_count: usize) -> Bytes {
    Bytes::from(workload_key_with_prefix(prefix, rand_value, key_count))
}

fn workload_value(
    role: &str,
    instance: usize,
    step: u64,
    batch_entry: Option<usize>,
    rand_value: u64,
) -> Bytes {
    let batch_entry = batch_entry
        .map(|entry| format!("{entry:02}"))
        .unwrap_or_else(|| "na".to_string());
    Bytes::from(format!(
        "{role}:{instance}:{step:08}:{batch_entry}:{rand_value:016x}"
    ))
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
        display_bytes(key),
    );
    Ok(())
}

async fn verify_scan(
    ctx: &ActorCtx,
    step: u64,
    prefix: &str,
    oracle: &BTreeMap<Bytes, Bytes>,
) -> Result<(), Error> {
    let mut iter = ctx.db().scan_prefix(prefix.as_bytes()).await?;
    let mut actual = BTreeMap::new();

    while let Some(key_value) = iter.next().await? {
        assert!(
            actual
                .insert(key_value.key.clone(), key_value.value.clone())
                .is_none(),
            "workload scan returned duplicate key [role={}, instance={}, step={}, prefix={}, key={}]",
            ctx.role(),
            ctx.instance(),
            step,
            prefix,
            display_bytes(&key_value.key),
        );
    }

    assert_eq!(
        actual,
        *oracle,
        "workload scan mismatch [role={}, instance={}, step={}, prefix={}]",
        ctx.role(),
        ctx.instance(),
        step,
        prefix,
    );
    Ok(())
}

fn display_bytes(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes.as_ref()).into_owned()
}
