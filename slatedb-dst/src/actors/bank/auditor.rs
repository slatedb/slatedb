use std::time::Duration;

use async_trait::async_trait;
use log::info;
use slatedb::Error;

use crate::{Actor, ActorCtx};

use super::{account_prefix, decode_balance, parse_account_id, BankOptions};

/// Repeatedly audits the bank by summing one scan view over all account rows.
#[derive(Debug)]
pub struct AuditorActor {
    options: BankOptions,
    audit_interval: Duration,
    scan_prefix: String,
    expected_total: u128,
    step: u64,
}

impl AuditorActor {
    pub fn new(options: BankOptions, audit_interval: Duration) -> Result<Self, Error> {
        options.validate()?;

        let scan_prefix = account_prefix(&options.prefix);
        let expected_total = options.expected_total();

        Ok(Self {
            options,
            audit_interval,
            scan_prefix,
            expected_total,
            step: 0,
        })
    }
}

#[async_trait]
impl Actor for AuditorActor {
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        let mut total = 0u128;
        let mut seen = vec![false; self.options.account_count];
        let mut iter = ctx.db().scan_prefix(self.scan_prefix.as_bytes()).await?;

        while let Some(kv) = iter.next().await? {
            let account_id = parse_account_id(
                kv.key.as_ref(),
                &self.options.prefix,
                self.options.account_count,
            )?;
            if seen[account_id] {
                return Err(Error::invalid(format!(
                    "duplicate bank account key observed during audit: {}",
                    String::from_utf8_lossy(kv.key.as_ref()),
                )));
            }

            seen[account_id] = true;
            total += u128::from(decode_balance(
                kv.value.as_ref(),
                self.options.value_size_bytes,
            )?);
        }

        let observed_count = seen.iter().filter(|present| **present).count();
        if observed_count != self.options.account_count {
            return Err(Error::invalid(format!(
                "bank audit observed {} accounts but expected {}",
                observed_count, self.options.account_count,
            )));
        }
        if total != self.expected_total {
            return Err(Error::invalid(format!(
                "bank audit total mismatch: observed {} expected {}",
                total, self.expected_total,
            )));
        }

        self.step += 1;
        info!(
            "bank auditor step complete [name={}, step={}]",
            ctx.name(),
            self.step
        );

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(self.audit_interval) => {}
        }

        Ok(())
    }
}
