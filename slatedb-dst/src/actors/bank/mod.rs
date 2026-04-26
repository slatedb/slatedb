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
        assert!(
            self.account_count >= 2,
            "bank workload account_count must be at least two"
        );
        assert!(
            self.max_transfer > 0,
            "bank workload max_transfer must be greater than zero"
        );
        assert!(
            self.expected_total() <= u128::from(u64::MAX),
            "bank workload expected_total must fit within u64 balances"
        );
        assert!(
            self.value_size_bytes >= BALANCE_BYTES,
            "bank workload value_size_bytes must be at least {}",
            BALANCE_BYTES
        );
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
    let key = std::str::from_utf8(key).expect("bank account key is not valid utf-8");
    let key_prefix = account_prefix(prefix);
    let Some(account_id) = key.strip_prefix(&key_prefix) else {
        panic!(
            "bank audit observed unexpected key outside account namespace: {}",
            key,
        );
    };

    let account_id = account_id
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("bank account key does not end in a numeric id: {}", key));
    assert!(
        account_id < account_count,
        "bank account id {} exceeds configured account_count {}",
        account_id,
        account_count,
    );

    Ok(account_id)
}

fn encode_balance(balance: u64, value_size_bytes: usize) -> Vec<u8> {
    debug_assert!(value_size_bytes >= BALANCE_BYTES);

    let mut value = vec![0; value_size_bytes];
    value[..BALANCE_BYTES].copy_from_slice(&balance.to_le_bytes());
    value
}

fn decode_balance(bytes: &[u8], value_size_bytes: usize) -> Result<u64, Error> {
    assert_eq!(
        bytes.len(),
        value_size_bytes,
        "bank balance value must be exactly {} bytes, got {}",
        value_size_bytes,
        bytes.len(),
    );
    assert!(
        value_size_bytes >= BALANCE_BYTES,
        "bank balance value_size_bytes must be at least {}",
        BALANCE_BYTES,
    );

    let balance: [u8; BALANCE_BYTES] = bytes[..BALANCE_BYTES].try_into().unwrap_or_else(|_| {
        panic!(
            "bank balance value must include an {} byte balance prefix, got {}",
            BALANCE_BYTES,
            bytes.len(),
        )
    });
    Ok(u64::from_le_bytes(balance))
}

async fn load_balance(
    txn: &DbTransaction,
    key: &[u8],
    value_size_bytes: usize,
) -> Result<u64, Error> {
    let balance = txn
        .get(key)
        .await?
        .expect("bank account missing during transfer");

    decode_balance(balance.as_ref(), value_size_bytes)
}
