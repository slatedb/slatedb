pub(crate) mod store;

pub(in crate::record) mod path_scoped_object_store;

use crate::error::SlateDBError;
use bytes::Bytes;

// Generic codec to serialize/deserialize versioned records stored as files
pub(crate) trait RecordCodec<T>: Send + Sync {
    fn encode(&self, value: &T) -> Bytes;
    fn decode(&self, bytes: &Bytes) -> Result<T, SlateDBError>;
}
