use log::info;
use slatedb::Error;
use tracing::instrument;

use crate::ActorCtx;

use super::{account_prefix, decode_balance, parse_account_id, BankOptions};

/// Repeatedly audits the bank by summing one scan view over all account rows.
#[instrument(level = "debug", skip_all, fields(role = %ctx.role(), instance = ctx.instance()))]
pub async fn auditor(ctx: ActorCtx, options: BankOptions) -> Result<(), Error> {
    options.validate()?;

    let shutdown_token = ctx.shutdown_token();
    let scan_prefix = account_prefix(&options.prefix);
    let expected_total = options.expected_total();
    let mut step = 0u64;

    while !shutdown_token.is_cancelled() {
        let mut total = 0u128;
        let mut seen = vec![false; options.account_count];
        let mut iter = ctx.db().scan_prefix(scan_prefix.as_bytes()).await?;

        while let Some(kv) = iter.next().await? {
            let account_id =
                parse_account_id(kv.key.as_ref(), &options.prefix, options.account_count)?;
            if seen[account_id] {
                return Err(Error::invalid(format!(
                    "duplicate bank account key observed during audit: {}",
                    String::from_utf8_lossy(kv.key.as_ref()),
                )));
            }

            seen[account_id] = true;
            total += u128::from(decode_balance(kv.value.as_ref())?);
        }

        let observed_count = seen.iter().filter(|present| **present).count();
        if observed_count != options.account_count {
            return Err(Error::invalid(format!(
                "bank audit observed {} accounts but expected {}",
                observed_count, options.account_count,
            )));
        }
        if total != expected_total {
            return Err(Error::invalid(format!(
                "bank audit total mismatch: observed {} expected {}",
                total, expected_total,
            )));
        }

        step += 1;
        if step % 1000 == 0 {
            info!("bank auditor step complete [step={}]", step);
        }
    }

    Ok(())
}
