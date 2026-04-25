mod auditor;
mod transfer;

use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, DbTransaction, Error};

pub use self::auditor::AuditorActor;
pub use self::transfer::TransferActor;

const BALANCE_BYTES: usize = std::mem::size_of::<u64>();

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
    /// Size in bytes for each account value. The first eight bytes encode the balance.
    pub value_size_bytes: usize,
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
        if self.value_size_bytes < BALANCE_BYTES {
            return Err(Error::invalid(format!(
                "bank workload value_size_bytes must be at least {}",
                BALANCE_BYTES,
            )));
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
            value_size_bytes: BALANCE_BYTES,
        }
    }
}

/// Seeds the database with the configured bank accounts and flushes once so
/// actors begin from a fully materialized initial state.
pub async fn initialize_accounts(db: &Db, options: &BankOptions) -> Result<(), Error> {
    options.validate()?;

    let starting_balance = encode_balance(options.initial_balance, options.value_size_bytes);
    for account_id in 0..options.account_count {
        let key = account_key(&options.prefix, account_id);
        db.put_with_options(
            key.as_bytes(),
            &starting_balance,
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await?;
    }
    db.flush().await?;

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

fn encode_balance(balance: u64, value_size_bytes: usize) -> Vec<u8> {
    debug_assert!(value_size_bytes >= BALANCE_BYTES);

    let mut value = vec![0; value_size_bytes];
    value[..BALANCE_BYTES].copy_from_slice(&balance.to_le_bytes());
    value
}

fn decode_balance(bytes: &[u8], value_size_bytes: usize) -> Result<u64, Error> {
    if bytes.len() != value_size_bytes {
        return Err(Error::invalid(format!(
            "bank balance value must be exactly {} bytes, got {}",
            value_size_bytes,
            bytes.len(),
        )));
    }
    if value_size_bytes < BALANCE_BYTES {
        return Err(Error::invalid(format!(
            "bank balance value_size_bytes must be at least {}",
            BALANCE_BYTES,
        )));
    }

    let balance: [u8; BALANCE_BYTES] = bytes[..BALANCE_BYTES].try_into().map_err(|_| {
        Error::invalid(format!(
            "bank balance value must include an {} byte balance prefix, got {}",
            BALANCE_BYTES,
            bytes.len(),
        ))
    })?;
    Ok(u64::from_le_bytes(balance))
}

async fn load_balance(
    txn: &DbTransaction,
    key: &[u8],
    value_size_bytes: usize,
) -> Result<u64, Error> {
    let Some(balance) = txn.get(key).await? else {
        return Err(Error::invalid(format!(
            "bank account missing during transfer: {}",
            String::from_utf8_lossy(key),
        )));
    };

    decode_balance(balance.as_ref(), value_size_bytes)
}

#[cfg(test)]
mod tests {
    use super::{account_key, decode_balance, encode_balance, parse_account_id, BankOptions};
    use slatedb::ErrorKind;

    #[test]
    fn should_compute_expected_total_in_u128() {
        let options = BankOptions {
            prefix: "acct".to_string(),
            account_count: 3,
            initial_balance: u64::MAX,
            max_transfer: 1,
            value_size_bytes: 8,
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
        let error = decode_balance(&[1, 2, 3], 8).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Invalid);
        assert_eq!(
            error.to_string(),
            "Invalid error: bank balance value must be exactly 8 bytes, got 3",
        );
    }

    #[test]
    fn should_round_trip_padded_balance_values() {
        let value = encode_balance(42, 32);

        assert_eq!(value.len(), 32);
        assert_eq!(decode_balance(&value, 32).unwrap(), 42);
        assert!(value[8..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn should_reject_too_small_value_size() {
        let options = BankOptions {
            value_size_bytes: 7,
            ..BankOptions::default()
        };
        let error = options.validate().unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Invalid);
        assert_eq!(
            error.to_string(),
            "Invalid error: bank workload value_size_bytes must be at least 8",
        );
    }
}
