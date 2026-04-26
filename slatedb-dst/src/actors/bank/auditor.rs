use std::time::Duration;

use async_trait::async_trait;
use log::info;
use slatedb::Error;

use crate::{Actor, ActorCtx};

use super::{BankAccounts, BankOptions};

/// Repeatedly audits the bank by summing one scan view over all account rows.
#[derive(Debug)]
pub struct AuditorActor {
    bank: BankAccounts,
    audit_interval: Duration,
    step: u64,
}

impl AuditorActor {
    pub fn new(options: BankOptions, audit_interval: Duration) -> Result<Self, Error> {
        Ok(Self {
            bank: BankAccounts::new(options)?,
            audit_interval,
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
        let mut seen = vec![false; self.bank.account_count()];
        let mut iter = ctx
            .db()
            .scan_prefix(self.bank.scan_prefix().as_bytes())
            .await?;

        while let Some(kv) = iter.next().await? {
            let account_id = self.bank.parse_account_id(kv.key.as_ref())?;
            if seen[account_id] {
                panic!(
                    "duplicate bank account key observed during audit: {}",
                    String::from_utf8_lossy(kv.key.as_ref()),
                );
            }

            seen[account_id] = true;
            total += u128::from(self.bank.decode_balance(kv.value.as_ref())?);
        }

        let observed_count = seen.iter().filter(|present| **present).count();
        assert_eq!(
            observed_count,
            self.bank.account_count(),
            "bank audit observed {} accounts but expected {}",
            observed_count,
            self.bank.account_count(),
        );
        assert_eq!(
            total,
            self.bank.expected_total(),
            "bank audit total mismatch: observed {} expected {}",
            total,
            self.bank.expected_total(),
        );

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
