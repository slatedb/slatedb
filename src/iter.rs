use bytes::Bytes;

use crate::error::SlateDBError;

#[derive(Debug)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12
pub trait KeyValueIterator {
    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError>;
}
