use std::time::Duration;

use async_trait::async_trait;
use rand::RngCore;
use slatedb::config::DbReaderOptions;
use slatedb::{DbReadOps, DbReader, Error};
use tracing::{info, instrument};

use crate::{Actor, ActorCtx};

use super::{BankAccounts, BankOptions};

/// Read view used by the bank auditor.
#[derive(Clone)]
pub enum BankAuditView {
    /// Audit the current shared `Db` handle.
    Regular,
    /// Audit a new point-in-time `DbSnapshot` from the current shared `Db`.
    Snapshot,
    /// Audit a long-lived read-only `DbReader`.
    Reader { options: DbReaderOptions },
}

impl BankAuditView {
    fn name(&self) -> &'static str {
        match self {
            Self::Regular => "db",
            Self::Snapshot => "db_snapshot",
            Self::Reader { .. } => "db_reader",
        }
    }
}

impl Default for BankAuditView {
    fn default() -> Self {
        Self::Regular
    }
}

/// Repeatedly audits the bank by summing one read view over all account rows.
pub struct AuditorActor {
    bank: BankAccounts,
    audit_interval: Duration,
    view: BankAuditView,
    reader: Option<DbReader>,
    step: u64,
}

impl AuditorActor {
    pub fn new(options: BankOptions, audit_interval: Duration) -> Result<Self, Error> {
        Self::new_with_view(options, audit_interval, BankAuditView::default())
    }

    pub fn new_with_view(
        options: BankOptions,
        audit_interval: Duration,
        view: BankAuditView,
    ) -> Result<Self, Error> {
        Ok(Self {
            bank: BankAccounts::new(options)?,
            audit_interval,
            view,
            reader: None,
            step: 0,
        })
    }
}

#[async_trait]
impl Actor for AuditorActor {
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name(), view = %self.view.name(), step = self.step))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let view = self.view.clone();
        match view {
            BankAuditView::Regular => {
                audit_bank_view(ctx.db().as_ref(), &self.bank, self.step).await?;
            }
            BankAuditView::Snapshot => {
                let snapshot = ctx.db().snapshot().await?;
                audit_bank_view(snapshot.as_ref(), &self.bank, self.step).await?;
            }
            BankAuditView::Reader { options } => {
                if self.reader.is_none() {
                    self.reader = Some(open_bank_reader(ctx, options).await?);
                }
                let reader = self
                    .reader
                    .as_ref()
                    .expect("bank reader auditor should have opened a reader");
                audit_bank_view(reader, &self.bank, self.step).await?;
            }
        };

        self.step += 1;
        info!(
            "bank auditor step complete [name={}, step={}]",
            ctx.name(),
            self.step
        );

        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(self.audit_interval) => {}
        }

        Ok(())
    }

    async fn finish(&mut self, _ctx: &ActorCtx) -> Result<(), Error> {
        if let Some(reader) = self.reader.take() {
            reader.close().await?;
        }
        Ok(())
    }
}

async fn open_bank_reader(ctx: &ActorCtx, options: DbReaderOptions) -> Result<DbReader, Error> {
    let mut builder = DbReader::builder(ctx.path().clone(), ctx.main_object_store())
        .with_options(options)
        .with_system_clock(ctx.system_clock())
        .with_seed(ctx.rand().rng().next_u64());

    if let Some(wal_object_store) = ctx.wal_object_store() {
        builder = builder.with_wal_object_store(wal_object_store);
    }

    if let Some(merge_operator) = ctx.merge_operator() {
        builder = builder.with_merge_operator(merge_operator);
    }

    builder.build().await
}

async fn audit_bank_view<R>(reader: &R, bank: &BankAccounts, step: u64) -> Result<(), Error>
where
    R: DbReadOps + Sync,
{
    let mut total = 0u128;
    let mut seen = vec![false; bank.account_count()];
    let mut iter = reader.scan_prefix(bank.scan_prefix().as_bytes()).await?;

    while let Some(kv) = iter.next().await? {
        let account_id = bank.parse_account_id(kv.key.as_ref())?;
        assert!(
            !seen[account_id],
            "duplicate bank account key observed during audit [step={step}, key={}]",
            String::from_utf8_lossy(kv.key.as_ref()),
        );

        seen[account_id] = true;
        total += u128::from(bank.decode_balance(kv.value.as_ref())?);
    }

    let observed_count = seen.iter().filter(|present| **present).count();
    assert_eq!(
        observed_count,
        bank.account_count(),
        "bank audit observed {observed_count} accounts but expected {} [step={step}]",
        bank.account_count(),
    );
    assert_eq!(
        total,
        bank.expected_total(),
        "bank audit total mismatch: observed {total} expected {} [step={step}]",
        bank.expected_total(),
    );

    Ok(())
}
