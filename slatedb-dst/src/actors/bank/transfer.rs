use async_trait::async_trait;
use log::info;
use rand::RngCore;
use slatedb::config::WriteOptions;
use slatedb::{Error, ErrorKind, IsolationLevel};

use crate::{Actor, ActorCtx};

use super::{account_key, encode_balance, load_balance, sample_account_index, BankOptions};

/// Repeatedly transfers funds between deterministic account pairs.
#[derive(Debug)]
pub struct TransferActor {
    options: BankOptions,
    step: u64,
}

impl TransferActor {
    pub fn new(options: BankOptions) -> Result<Self, Error> {
        options.validate()?;
        Ok(Self { options, step: 0 })
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

        let from = sample_account_index(from_rand, self.options.account_count);
        let to = sample_account_index(to_rand, self.options.account_count);
        let sampled_amount = 1 + (amount_rand % self.options.max_transfer);

        if from != to {
            let from_key = account_key(&self.options.prefix, from);
            let to_key = account_key(&self.options.prefix, to);

            loop {
                let txn = ctx.db().begin(IsolationLevel::Snapshot).await?;
                let from_balance =
                    load_balance(&txn, from_key.as_bytes(), self.options.value_size_bytes).await?;
                let to_balance =
                    load_balance(&txn, to_key.as_bytes(), self.options.value_size_bytes).await?;
                let transfer_amount = sampled_amount.min(from_balance);

                if transfer_amount == 0 {
                    break;
                }
                let updated_to_balance = to_balance
                    .checked_add(transfer_amount)
                    .expect("bank transfer overflowed destination account balance");

                let updated_from_value = encode_balance(
                    from_balance - transfer_amount,
                    self.options.value_size_bytes,
                );
                let updated_to_value =
                    encode_balance(updated_to_balance, self.options.value_size_bytes);

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
