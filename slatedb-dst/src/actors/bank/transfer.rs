use async_trait::async_trait;
use log::info;
use rand::RngCore;
use slatedb::config::WriteOptions;
use slatedb::{Error, ErrorKind, IsolationLevel};

use crate::{Actor, ActorCtx};

use super::{BankAccounts, BankOptions};

/// Repeatedly transfers funds between deterministic account pairs.
#[derive(Debug)]
pub struct TransferActor {
    bank: BankAccounts,
    step: u64,
}

impl TransferActor {
    pub fn new(options: BankOptions) -> Result<Self, Error> {
        Ok(Self {
            bank: BankAccounts::new(options)?,
            step: 0,
        })
    }
}

#[async_trait]
impl Actor for TransferActor {
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let write_options = WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        };
        let from_rand = ctx.rand().rng().next_u64();
        let to_rand = ctx.rand().rng().next_u64();
        let amount_rand = ctx.rand().rng().next_u64();

        let from = self.bank.sample_account_index(from_rand);
        let to = self.bank.sample_account_index(to_rand);
        let sampled_amount = 1 + (amount_rand % self.bank.max_transfer());

        if from != to {
            let from_key = self.bank.account_key(from);
            let to_key = self.bank.account_key(to);

            loop {
                let txn = ctx.db().begin(IsolationLevel::Snapshot).await?;
                let from_balance = self.bank.load_balance(&txn, from_key.as_bytes()).await?;
                let to_balance = self.bank.load_balance(&txn, to_key.as_bytes()).await?;
                let transfer_amount = sampled_amount.min(from_balance);

                if transfer_amount == 0 {
                    break;
                }
                let updated_to_balance = to_balance
                    .checked_add(transfer_amount)
                    .expect("bank transfer overflowed destination account balance");

                let updated_from_value = self.bank.encode_balance(from_balance - transfer_amount);
                let updated_to_value = self.bank.encode_balance(updated_to_balance);

                txn.put(from_key.as_bytes(), updated_from_value)?;
                txn.put(to_key.as_bytes(), updated_to_value)?;

                match txn.commit_with_options(&write_options).await {
                    Ok(_) => break,
                    Err(error) if error.kind() == ErrorKind::Transaction => continue,
                    Err(error) => return Err(error),
                }
            }
        }

        self.step += 1;
        if self.step % 10_000 == 0 {
            info!(
                "bank transfer step complete [name={}, step={}]",
                ctx.name(),
                self.step
            );
        }

        Ok(())
    }
}
