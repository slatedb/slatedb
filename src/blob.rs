use std::ops::Range;

use bytes::Bytes;

use crate::error::SlateDBError;

pub(crate) trait ReadOnlyBlob {
    async fn len(&self) -> Result<usize, SlateDBError>;

    async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError>;

    #[allow(dead_code)]
    async fn read(&self) -> Result<Bytes, SlateDBError>;
}
