mod auditor;
mod transfer;

use bytes::Bytes;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, DbTransaction, Error, MergeOperator, MergeOperatorError};

pub use self::auditor::{AuditorActor, BankAuditView};
pub use self::transfer::{TransferActor, TransferMode};

const ACCUMULATOR_BYTES: usize = std::mem::size_of::<i64>();

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
    /// Size in bytes for each account value. The first eight bytes encode the signed accumulator.
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
            self.initial_balance <= i64::MAX as u64,
            "bank workload initial_balance must fit within i64 balances"
        );
        assert!(
            self.max_transfer <= i64::MAX as u64,
            "bank workload max_transfer must fit within i64 deltas"
        );
        assert!(
            self.expected_total() <= i64::MAX as u128,
            "bank workload expected_total must fit within i64 balances"
        );
        assert!(
            self.value_size_bytes >= ACCUMULATOR_BYTES,
            "bank workload value_size_bytes must be at least {}",
            ACCUMULATOR_BYTES
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
            value_size_bytes: ACCUMULATOR_BYTES,
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

/// Bank merge operator for fixed-size signed account accumulator payloads.
#[derive(Debug)]
pub struct BankMergeOperator {
    value_size_bytes: usize,
}

impl BankMergeOperator {
    /// Creates a merge operator for payloads encoded with the provided bank options.
    pub fn new(options: &BankOptions) -> Result<Self, Error> {
        options.validate()?;
        Ok(Self {
            value_size_bytes: options.value_size_bytes,
        })
    }
}

impl MergeOperator for BankMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut total = existing_value
            .as_ref()
            .map(|value| decode_accumulator(value.as_ref(), self.value_size_bytes))
            .unwrap_or(0);

        for operand in operands {
            total = total
                .checked_add(decode_accumulator(operand.as_ref(), self.value_size_bytes))
                .expect("bank merge overflowed signed accumulator");
        }

        Ok(Bytes::from(encode_accumulator(
            total,
            self.value_size_bytes,
        )))
    }
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
        let balance = i64::try_from(balance).expect("bank balance must fit within i64");
        encode_accumulator(balance, self.options.value_size_bytes)
    }

    fn encode_delta(&self, delta: i64) -> Vec<u8> {
        encode_accumulator(delta, self.options.value_size_bytes)
    }

    fn decode_balance(&self, bytes: &[u8]) -> Result<u64, Error> {
        let balance = decode_accumulator(bytes, self.options.value_size_bytes);
        assert!(
            balance >= 0,
            "bank balance must be non-negative, got {}",
            balance
        );
        Ok(balance as u64)
    }

    async fn load_balance(&self, txn: &DbTransaction, key: &[u8]) -> Result<u64, Error> {
        let balance = txn
            .get(key)
            .await?
            .expect("bank account missing during transfer");

        self.decode_balance(balance.as_ref())
    }
}

fn encode_accumulator(accumulator: i64, value_size_bytes: usize) -> Vec<u8> {
    debug_assert!(value_size_bytes >= ACCUMULATOR_BYTES);

    let mut value = vec![0; value_size_bytes];
    value[..ACCUMULATOR_BYTES].copy_from_slice(&accumulator.to_le_bytes());
    value
}

fn decode_accumulator(bytes: &[u8], value_size_bytes: usize) -> i64 {
    assert_eq!(
        bytes.len(),
        value_size_bytes,
        "bank account value must be exactly {} bytes, got {}",
        value_size_bytes,
        bytes.len(),
    );
    assert!(
        value_size_bytes >= ACCUMULATOR_BYTES,
        "bank account value_size_bytes must be at least {}",
        ACCUMULATOR_BYTES,
    );
    assert!(
        bytes[ACCUMULATOR_BYTES..].iter().all(|byte| *byte == 0),
        "bank account value padding must be zero",
    );

    let accumulator: [u8; ACCUMULATOR_BYTES] =
        bytes[..ACCUMULATOR_BYTES].try_into().unwrap_or_else(|_| {
            panic!(
                "bank account value must include an {} byte accumulator prefix, got {}",
                ACCUMULATOR_BYTES,
                bytes.len(),
            )
        });
    i64::from_le_bytes(accumulator)
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;

    fn test_options(value_size_bytes: usize) -> BankOptions {
        BankOptions {
            prefix: "test-acct".to_string(),
            account_count: 4,
            initial_balance: 100,
            max_transfer: 10,
            value_size_bytes,
        }
    }

    fn test_bank(value_size_bytes: usize) -> BankAccounts {
        BankAccounts::new(test_options(value_size_bytes)).unwrap()
    }

    fn test_operator(value_size_bytes: usize) -> BankMergeOperator {
        BankMergeOperator::new(&test_options(value_size_bytes)).unwrap()
    }

    #[test]
    fn should_encode_and_decode_fixed_size_balance() {
        let bank = test_bank(16);

        let encoded = bank.encode_balance(42);

        assert_eq!(encoded.len(), 16);
        assert_eq!(&encoded[..ACCUMULATOR_BYTES], &42i64.to_le_bytes());
        assert!(encoded[ACCUMULATOR_BYTES..].iter().all(|byte| *byte == 0));
        assert_eq!(bank.decode_balance(&encoded).unwrap(), 42);
    }

    #[test]
    fn should_encode_positive_and_negative_deltas() {
        let bank = test_bank(16);

        let credit = bank.encode_delta(17);
        let debit = bank.encode_delta(-9);

        assert_eq!(decode_accumulator(&credit, 16), 17);
        assert_eq!(decode_accumulator(&debit, 16), -9);
    }

    #[test]
    fn should_merge_deltas_without_base() {
        let operator = test_operator(16);
        let bank = test_bank(16);
        let operands = [
            Bytes::from(bank.encode_delta(10)),
            Bytes::from(bank.encode_delta(-3)),
        ];

        let result = operator
            .merge_batch(&Bytes::from_static(b"acct/0"), None, &operands)
            .unwrap();

        assert_eq!(decode_accumulator(result.as_ref(), 16), 7);
    }

    #[test]
    fn should_merge_deltas_with_base_or_partial_value() {
        let operator = test_operator(16);
        let bank = test_bank(16);
        let base = Bytes::from(bank.encode_balance(100));
        let operands = [
            Bytes::from(bank.encode_delta(-20)),
            Bytes::from(bank.encode_delta(5)),
        ];

        let result = operator
            .merge_batch(&Bytes::from_static(b"acct/0"), Some(base), &operands)
            .unwrap();

        assert_eq!(decode_accumulator(result.as_ref(), 16), 85);
        assert_eq!(bank.decode_balance(result.as_ref()).unwrap(), 85);
    }

    #[test]
    fn should_preserve_regrouping_equivalence() {
        let operator = test_operator(16);
        let bank = test_bank(16);
        let key = Bytes::from_static(b"acct/0");
        let base = Bytes::from(bank.encode_balance(100));
        let a = Bytes::from(bank.encode_delta(10));
        let b = Bytes::from(bank.encode_delta(-4));
        let c = Bytes::from(bank.encode_delta(7));

        let direct = operator
            .merge_batch(&key, Some(base.clone()), &[a.clone(), b.clone(), c.clone()])
            .unwrap();
        let partial = operator
            .merge_batch(&key, None, &[a.clone(), b.clone()])
            .unwrap();
        let regrouped = operator
            .merge_batch(&key, Some(base), &[partial, c])
            .unwrap();

        assert_eq!(direct, regrouped);
        assert_eq!(decode_accumulator(regrouped.as_ref(), 16), 113);
    }

    #[test]
    fn should_panic_on_malformed_payload_size() {
        let operator = test_operator(16);

        let result = catch_unwind(AssertUnwindSafe(|| {
            operator
                .merge_batch(
                    &Bytes::from_static(b"acct/0"),
                    None,
                    &[Bytes::from_static(b"short")],
                )
                .unwrap();
        }));

        assert!(result.is_err());
    }

    #[test]
    fn should_panic_on_arithmetic_overflow() {
        let operator = test_operator(16);
        let operands = [
            Bytes::from(encode_accumulator(i64::MAX, 16)),
            Bytes::from(encode_accumulator(1, 16)),
        ];

        let result = catch_unwind(AssertUnwindSafe(|| {
            operator
                .merge_batch(&Bytes::from_static(b"acct/0"), None, &operands)
                .unwrap();
        }));

        assert!(result.is_err());
    }

    #[test]
    fn should_panic_when_decoding_negative_balance() {
        let bank = test_bank(16);
        let negative = bank.encode_delta(-1);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = bank.decode_balance(&negative);
        }));

        assert!(result.is_err());
    }
}
