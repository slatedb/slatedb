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
    let bank = BankAccounts::new(options.clone())?;

    let starting_balance = bank.encode_balance(bank.initial_balance());
    for account_id in 0..bank.account_count() {
        let key = bank.account_key(account_id);
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

#[derive(Clone, Debug)]
struct BankAccounts {
    options: BankOptions,
    account_prefix: String,
    expected_total: u128,
}

impl BankAccounts {
    fn new(options: BankOptions) -> Result<Self, Error> {
        options.validate()?;

        let account_prefix = format!("{}/", options.prefix);
        let expected_total = options.expected_total();

        Ok(Self {
            options,
            account_prefix,
            expected_total,
        })
    }

    fn account_count(&self) -> usize {
        self.options.account_count
    }

    fn initial_balance(&self) -> u64 {
        self.options.initial_balance
    }

    fn max_transfer(&self) -> u64 {
        self.options.max_transfer
    }

    fn expected_total(&self) -> u128 {
        self.expected_total
    }

    fn scan_prefix(&self) -> &str {
        &self.account_prefix
    }

    fn account_key(&self, account_id: usize) -> String {
        format!("{}{account_id}", self.account_prefix)
    }

    fn sample_account_index(&self, rand_value: u64) -> usize {
        ((rand_value >> 8) as usize) % self.options.account_count
    }

    fn parse_account_id(&self, key: &[u8]) -> Result<usize, Error> {
        let key = std::str::from_utf8(key).expect("bank account key is not valid utf-8");
        let Some(account_id) = key.strip_prefix(&self.account_prefix) else {
            panic!(
                "bank audit observed unexpected key outside account namespace: {}",
                key,
            );
        };

        let account_id = account_id
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("bank account key does not end in a numeric id: {}", key));
        assert!(
            account_id < self.options.account_count,
            "bank account id {} exceeds configured account_count {}",
            account_id,
            self.options.account_count,
        );

        Ok(account_id)
    }

    fn encode_balance(&self, balance: u64) -> Vec<u8> {
        debug_assert!(self.options.value_size_bytes >= BALANCE_BYTES);

        let mut value = vec![0; self.options.value_size_bytes];
        value[..BALANCE_BYTES].copy_from_slice(&balance.to_le_bytes());
        value
    }

    fn decode_balance(&self, bytes: &[u8]) -> Result<u64, Error> {
        assert_eq!(
            bytes.len(),
            self.options.value_size_bytes,
            "bank balance value must be exactly {} bytes, got {}",
            self.options.value_size_bytes,
            bytes.len(),
        );
        assert!(
            self.options.value_size_bytes >= BALANCE_BYTES,
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

    async fn load_balance(&self, txn: &DbTransaction, key: &[u8]) -> Result<u64, Error> {
        let balance = txn
            .get(key)
            .await?
            .expect("bank account missing during transfer");

        self.decode_balance(balance.as_ref())
    }
}
