use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::MergeOperatorError;

use crate::error::MergeOperatorCallbackError;

/// Application-provided merge operator used by merge-enabled databases.
#[uniffi::export(with_foreign)]
pub trait MergeOperator: Send + Sync {
    /// Combines an existing value and a new merge operand into the next value.
    ///
    /// `existing_value` is `None` when the key has no visible base value.
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

impl slatedb::MergeOperator for MergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.inner
            .merge(
                key.to_vec(),
                existing_value.map(|value| value.to_vec()),
                operand.to_vec(),
            )
            .map(Bytes::from)
            .map_err(|error| MergeOperatorError::Callback {
                message: error.to_string(),
            })
    }
}

pub(crate) fn adapt_merge_operator(
    merge_operator: Arc<dyn MergeOperator>,
) -> Arc<dyn slatedb::MergeOperator + Send + Sync> {
    Arc::new(MergeOperatorAdapter {
        inner: merge_operator,
    })
}
