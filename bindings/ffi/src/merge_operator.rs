//! Merge-operator callback interfaces and adapters.

use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::{MergeOperator as CoreMergeOperator, MergeOperatorError as CoreMergeOperatorError};

use crate::error::MergeOperatorCallbackError;

/// Callback interface for SlateDB merge operators.
///
/// Merge operators are configured on [`crate::DbBuilder`] and are used by merge
/// reads and writes to combine an existing value with a new operand.
#[uniffi::export(callback_interface)]
pub trait MergeOperator: Send + Sync {
    /// Merge a new operand into the existing value for a key.
    ///
    /// ## Arguments
    /// - `key`: the key being merged.
    /// - `existing_value`: the current value, if one exists.
    /// - `operand`: the new merge operand.
    ///
    /// ## Returns
    /// - `Result<Vec<u8>, MergeOperatorCallbackError>`: the merged value that
    ///   should become visible for the key.
    fn merge(
        &self,
        key: Vec<u8>,
        existing_value: Option<Vec<u8>>,
        operand: Vec<u8>,
    ) -> Result<Vec<u8>, MergeOperatorCallbackError>;
}

struct MergeOperatorAdapter {
    inner: Arc<dyn MergeOperator>,
}

impl CoreMergeOperator for MergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, CoreMergeOperatorError> {
        self.inner
            .merge(
                key.to_vec(),
                existing_value.map(|value| value.to_vec()),
                operand.to_vec(),
            )
            .map(Bytes::from)
            .map_err(|error| CoreMergeOperatorError::Callback {
                message: error.to_string(),
            })
    }
}

pub(crate) fn adapt_merge_operator(
    merge_operator: Box<dyn MergeOperator>,
) -> Arc<dyn CoreMergeOperator + Send + Sync> {
    Arc::new(MergeOperatorAdapter {
        inner: merge_operator.into(),
    })
}
