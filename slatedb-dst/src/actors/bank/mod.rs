use log::info;
use rand::RngCore;
use slatedb::{Db, DbTransaction, Error, ErrorKind, IsolationLevel};
use tracing::instrument;

use crate::ActorCtx;

use super::PROGRESS_LOG_INTERVAL;

/// Configuration for the deterministic bank workload.
#[derive(Clone, Debug)]
pub struct BankOptions {
    /// Root key prefix used for all account rows.
    pub prefix: String,
    /// Number of logical accounts in the bank.
    pub account_count: usize,
    /// Starting balance for every account before actors begin.
    pub initial_balance: u64,
    /// Maximum amount sampled for a transfer step.
    pub max_transfer: u64,
}

impl BankOptions {
    /// Returns the invariant total across all accounts.
    pub fn expected_total(&self) -> u128 {
        u128::from(self.initial_balance) * self.account_count as u128
    }

    fn validate(&self) -> Result<(), Error> {
        if self.account_count < 2 {
            return Err(Error::invalid(
                "bank workload account_count must be at least two".to_string(),
            ));
        }
        if self.max_transfer == 0 {
            return Err(Error::invalid(
                "bank workload max_transfer must be greater than zero".to_string(),
            ));
        }
        if self.expected_total() > u128::from(u64::MAX) {
            return Err(Error::invalid(
                "bank workload expected_total must fit within u64 balances".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for BankOptions {
    fn default() -> Self {
        Self {
            prefix: "acct".to_string(),
            account_count: 32,
            initial_balance: 10_000,
            max_transfer: 100,
        }
    }
}

/// Seeds the database with the configured bank accounts and flushes once so
/// actors begin from a fully materialized initial state.
pub async fn initialize_accounts(db: &Db, options: &BankOptions) -> Result<(), Error> {
    options.validate()?;

    let starting_balance = options.initial_balance.to_le_bytes();
    for account_id in 0..options.account_count {
        let key = account_key(&options.prefix, account_id);
        db.put(key.as_bytes(), &starting_balance).await?;
    }
    db.flush().await?;

    Ok(())
}

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
        if step % PROGRESS_LOG_INTERVAL == 0 {
            info!("bank auditor step complete [step={}]", step);
        }
    }

    Ok(())
}

fn account_prefix(prefix: &str) -> String {
    format!("{prefix}/")
}

fn account_key(prefix: &str, account_id: usize) -> String {
    format!("{}{account_id}", account_prefix(prefix))
}

fn sample_account_index(rand_value: u64, account_count: usize) -> usize {
    ((rand_value >> 8) as usize) % account_count
}

fn parse_account_id(key: &[u8], prefix: &str, account_count: usize) -> Result<usize, Error> {
    let key = std::str::from_utf8(key)
        .map_err(|_| Error::invalid("bank account key is not valid utf-8".to_string()))?;
    let key_prefix = account_prefix(prefix);
    let Some(account_id) = key.strip_prefix(&key_prefix) else {
        return Err(Error::invalid(format!(
            "bank audit observed unexpected key outside account namespace: {}",
            key,
        )));
    };

    let account_id = account_id.parse::<usize>().map_err(|_| {
        Error::invalid(format!(
            "bank account key does not end in a numeric id: {}",
            key
        ))
    })?;
    if account_id >= account_count {
        return Err(Error::invalid(format!(
            "bank account id {} exceeds configured account_count {}",
            account_id, account_count,
        )));
    }

    Ok(account_id)
}

fn decode_balance(bytes: &[u8]) -> Result<u64, Error> {
    let balance: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::invalid(format!(
            "bank balance value must be exactly 8 bytes, got {}",
            bytes.len(),
        ))
    })?;
    Ok(u64::from_le_bytes(balance))
}

async fn load_balance(txn: &DbTransaction, key: &[u8]) -> Result<u64, Error> {
    let Some(balance) = txn.get(key).await? else {
        return Err(Error::invalid(format!(
            "bank account missing during transfer: {}",
            String::from_utf8_lossy(key),
        )));
    };

    decode_balance(balance.as_ref())
}

#[cfg(test)]
mod tests {
    use super::{account_key, decode_balance, parse_account_id, BankOptions};
    use slatedb::ErrorKind;

    #[test]
    fn should_compute_expected_total_in_u128() {
        let options = BankOptions {
            prefix: "acct".to_string(),
            account_count: 3,
            initial_balance: u64::MAX,
            max_transfer: 1,
        };

        assert_eq!(options.expected_total(), u128::from(u64::MAX) * 3);
    }

    #[test]
    fn should_round_trip_account_keys() {
        let key = account_key("acct", 17);

        assert_eq!(key, "acct/17");
        assert_eq!(parse_account_id(key.as_bytes(), "acct", 32).unwrap(), 17);
    }

    #[test]
    fn should_reject_invalid_balance_width() {
        let error = decode_balance(&[1, 2, 3]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Invalid);
        assert_eq!(
            error.to_string(),
            "Invalid error: bank balance value must be exactly 8 bytes, got 3",
        );
    }
}
