use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use rand::RngCore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Error, WriteBatch};
use tracing::instrument;

use crate::{Actor, ActorCtx};

use super::PROGRESS_LOG_INTERVAL;

/// Configuration for the mixed DST workload actor.
#[derive(Clone, Debug)]
pub struct WorkloadActorOptions {
    /// Number of logical keys each actor maps random samples onto under its
    /// `{name}/` prefix.
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

#[derive(Debug)]
pub struct WorkloadActor {
    options: WorkloadActorOptions,
    oracle: BTreeMap<Bytes, Bytes>,
    step: u64,
    key_prefix: Option<String>,
}

impl WorkloadActor {
    pub fn new(options: WorkloadActorOptions) -> Result<Self, Error> {
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

        Ok(Self {
            options,
            oracle: BTreeMap::new(),
            step: 0,
            key_prefix: None,
        })
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

#[async_trait]
impl Actor for WorkloadActor {
    /// Executes one mixed deterministic workload step against the shared
    /// database while verifying reads against an actor-local oracle.
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let put_options = PutOptions::default();
        let write_options = WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        };
        let key_prefix = self
            .key_prefix
            .get_or_insert_with(|| format!("{}/", ctx.name()))
            .clone();
        let operation = WorkloadOperation::sample(ctx.rand().rng().next_u64());

        match operation {
            WorkloadOperation::Get => {
                let key = workload_key(
                    &key_prefix,
                    ctx.rand().rng().next_u64(),
                    self.options.key_count,
                );
                verify_get(ctx, self.step, &key, &self.oracle).await?;
            }
            WorkloadOperation::Scan => {
                verify_scan(ctx, self.step, &key_prefix, &self.oracle).await?;
            }
            WorkloadOperation::Delete => {
                let key = workload_key(
                    &key_prefix,
                    ctx.rand().rng().next_u64(),
                    self.options.key_count,
                );
                ctx.db()
                    .delete_with_options(key.clone(), &write_options)
                    .await?;
                self.oracle.remove(&key);
            }
            WorkloadOperation::Put => {
                let key = workload_key(
                    &key_prefix,
                    ctx.rand().rng().next_u64(),
                    self.options.key_count,
                );
                let value = workload_value(
                    &mut *ctx.rand().rng(),
                    self.options.min_value_size,
                    self.options.max_value_size,
                );
                ctx.db()
                    .put_with_options(key.clone(), value.clone(), &put_options, &write_options)
                    .await?;
                self.oracle.insert(key, value);
            }
            WorkloadOperation::WriteBatch => {
                let mut batch = WriteBatch::new();
                let mut next_oracle = self.oracle.clone();
                let batch_len = 2 + (ctx.rand().rng().next_u64() % 3) as usize;

                for _ in 0..batch_len {
                    let key = workload_key(
                        &key_prefix,
                        ctx.rand().rng().next_u64(),
                        self.options.key_count,
                    );
                    if ctx.rand().rng().next_u64() & 1 == 0 {
                        let value = workload_value(
                            &mut *ctx.rand().rng(),
                            self.options.min_value_size,
                            self.options.max_value_size,
                        );
                        batch.put_bytes(key.clone(), value.clone());
                        next_oracle.insert(key, value);
                    } else {
                        batch.delete(key.clone());
                        next_oracle.remove(&key);
                    }
                }

                ctx.db().write_with_options(batch, &write_options).await?;
                self.oracle = next_oracle;
            }
        }

        self.step += 1;
        if self.step % PROGRESS_LOG_INTERVAL == 0 {
            info!(
                "workload step complete [name={}, step={}, oracle_keys={}]",
                ctx.name(),
                self.step,
                self.oracle.len()
            );
        }

        Ok(())
    }
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
        "workload get mismatch [name={}, step={}, key={}]",
        ctx.name(),
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
            "workload scan returned duplicate key [name={}, step={}, key_prefix={}, key={}]",
            ctx.name(),
            step,
            key_prefix,
            String::from_utf8_lossy(key_value.key.as_ref()).into_owned(),
        );
    }

    assert_eq!(
        actual,
        *oracle,
        "workload scan mismatch [name={}, step={}, key_prefix={}]",
        ctx.name(),
        step,
        key_prefix,
    );
    Ok(())
}
