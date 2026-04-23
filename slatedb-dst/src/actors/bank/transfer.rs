use log::info;
use rand::RngCore;
use slatedb::{Error, ErrorKind, IsolationLevel};
use tracing::instrument;

use crate::ActorCtx;

use super::super::PROGRESS_LOG_INTERVAL;
use super::{account_key, load_balance, sample_account_index, BankOptions};

/// Repeatedly transfers funds between deterministic account pairs.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn transfer(ctx: ActorCtx, options: BankOptions) -> Result<(), Error> {
    options.validate()?;

    let shutdown_token = ctx.shutdown_token();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let from_rand = ctx.rand().rng().next_u64();
        let to_rand = ctx.rand().rng().next_u64();
        let amount_rand = ctx.rand().rng().next_u64();

        let from = sample_account_index(from_rand, options.account_count);
        let to = sample_account_index(to_rand, options.account_count);
        let sampled_amount = 1 + (amount_rand % options.max_transfer);

        if from != to {
            let from_key = account_key(&options.prefix, from);
            let to_key = account_key(&options.prefix, to);

            loop {
                let txn = ctx.db().begin(IsolationLevel::Snapshot).await?;
                let from_balance = load_balance(&txn, from_key.as_bytes()).await?;
                let to_balance = load_balance(&txn, to_key.as_bytes()).await?;
                let transfer_amount = sampled_amount.min(from_balance);

                if transfer_amount == 0 {
                    break;
                }
                let updated_to_balance =
                    to_balance.checked_add(transfer_amount).ok_or_else(|| {
                        Error::invalid(
                            "bank transfer overflowed destination account balance".to_string(),
                        )
                    })?;

                txn.put(
                    from_key.as_bytes(),
                    (from_balance - transfer_amount).to_le_bytes(),
                )?;
                txn.put(to_key.as_bytes(), updated_to_balance.to_le_bytes())?;

                match txn.commit().await {
                    Ok(_) => break,
                    Err(error) if error.kind() == ErrorKind::Transaction => continue,
                    Err(error) => return Err(error),
                }
            }
        }

        step += 1;
        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("bank transfer step complete [step={}]", step);
        }
    }

    Ok(())
}
