//! Benchmark helpers for comparing flat vs partitioned SST index read performance.
//!
//! Exposed under the `bencher` feature so that Criterion benchmarks in `benches/`
//! can drive internal read paths without making them part of the public API.

use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};

use crate::blob::ReadOnlyBlob;
use crate::bytes_generator::OrderedBytesGenerator;
use crate::db_state::SsTableInfo;
use crate::error::SlateDBError;
use crate::flatbuffer_types::FlatBufferSsTableInfoCodec;
use crate::format::sst::SsTableFormat;
use crate::types::{RowEntry, ValueDeletable};

/// A simple in-memory blob used to satisfy `read_index`'s `ReadOnlyBlob` bound.
struct BytesBlob(Bytes);

impl ReadOnlyBlob for BytesBlob {
    async fn len(&self) -> Result<u64, SlateDBError> {
        Ok(self.0.len() as u64)
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError> {
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        Ok(self.0.clone())
    }
}

/// A pre-built SST (bytes + info) ready to be fed into `read_index`.
///
/// Construct via [`PreparedSst::build_flat`] or [`PreparedSst::build_partitioned`],
/// then call [`PreparedSst::read_index`] inside the Criterion loop.
pub struct PreparedSst {
    bytes: Bytes,
    info: SsTableInfo,
    format: SsTableFormat,
}

impl PreparedSst {
    /// Builds a WAL SST with a **flat** `SsTableIndex`.
    ///
    /// WAL SSTs always use `SstType::Wal` which routes through
    /// `build_flat_index` in `EncodedSsTableFooterBuilder`.
    pub async fn build_flat(num_blocks: usize) -> Self {
        let format = SsTableFormat {
            sst_codec: Box::new(FlatBufferSsTableInfoCodec {}),
            ..SsTableFormat::default()
        };
        let sst = build_wal_sst(&format, num_blocks).await;
        let bytes = sst.remaining_as_bytes();
        Self {
            bytes,
            info: sst.info,
            format,
        }
    }

    /// Builds a compacted SST with a **partitioned** `SsTableIndexV2`.
    ///
    /// Compacted SSTs use `SstType::Compacted` which routes through
    /// `build_partitioned_index` in `EncodedSsTableFooterBuilder`.
    pub async fn build_partitioned(num_blocks: usize) -> Self {
        let format = SsTableFormat {
            sst_codec: Box::new(FlatBufferSsTableInfoCodec {}),
            ..SsTableFormat::default()
        };
        let sst = build_compacted_sst(&format, num_blocks).await;
        let bytes = sst.remaining_as_bytes();
        Self {
            bytes,
            info: sst.info,
            format,
        }
    }

    /// Reads and decodes the SST index, returning the number of data blocks
    /// found in the stitched flat index.  The return value prevents the
    /// compiler from optimising the call away.
    pub async fn read_index(&self) -> usize {
        let index = self
            .format
            .read_index(&self.info, &BytesBlob(self.bytes.clone()))
            .await
            .expect("read_index failed");
        index.borrow().block_meta().len()
    }
}

/// Builds a WAL SST with exactly `num_blocks` complete data blocks.
///
/// Each entry uses an 8-byte big-endian sequence number as its "key"
/// (the WAL index key is the first seq in each block).
async fn build_wal_sst(
    format: &SsTableFormat,
    num_blocks: usize,
) -> crate::format::sst::EncodedSsTable {
    let mut builder = format.wal_table_builder();
    let mut seq: u64 = 0;
    let mut blocks_finished = 0;

    loop {
        let key = Bytes::copy_from_slice(&seq.to_be_bytes());
        let mut val = BytesMut::with_capacity(32);
        val.put_bytes(0u8, 32);
        let entry = RowEntry::new(key, ValueDeletable::Value(val.freeze()), seq, None, None);

        let finished = builder
            .add(entry)
            .await
            .expect("WAL table builder should be adding entries");
        if finished.is_some() {
            blocks_finished += 1;
            if blocks_finished >= num_blocks {
                break;
            }
        }
        seq += 1;
    }

    builder
        .build()
        .await
        .expect("WAL table builder should have succesfully built EncodedSsTable")
}

/// Builds a compacted SST with exactly `num_blocks` complete data blocks.
///
/// Keys are generated via `OrderedBytesGenerator` with a 16-byte suffix,
/// matching what `test_utils::build_test_sst` produces.
async fn build_compacted_sst(
    format: &SsTableFormat,
    num_blocks: usize,
) -> crate::format::sst::EncodedSsTable {
    let mut builder = format.table_builder();
    let mut keygen = OrderedBytesGenerator::new_with_suffix(&[], &[0u8; 16]);
    let mut blocks_finished = 0;

    loop {
        let key = keygen.next();
        let mut val = BytesMut::with_capacity(32);
        val.put_bytes(0u8, 32);
        let entry = RowEntry::new(key, ValueDeletable::Value(val.freeze()), 0, None, None);

        let finished = builder
            .add(entry)
            .await
            .expect("Table builder should be adding entries.");
        if finished.is_some() {
            blocks_finished += 1;
            if blocks_finished >= num_blocks {
                break;
            }
        }
    }

    builder
        .build()
        .await
        .expect("Table builder wshould have built succesfully")
}
